// manager.go 是策略子系统的主控入口。
// 它负责启动或连接 Python 策略进程，维护 HTTP 连接与健康检查，并对外提供策略定义、实例和信号管理能力。
package strategy

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/klinequery"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/searchindex"
)

type strategyServiceProcessInfo struct {
	ProcessID    int    `json:"ProcessId"`
	CreationDate string `json:"CreationDate"`
	CommandLine  string `json:"CommandLine"`
}

const minRuntimeStartTimeout = 30 * time.Second
const zigzagATR26FeatureID = "zigzag_atr26"

type strategyFeatureKey struct {
	Mode      string
	Symbol    string
	Timeframe string
	Indicator string
}

type Manager struct {
	cfg   config.StrategyConfig
	store *Store
	exec  *ExecutionEngine

	featureMu sync.RWMutex
	features  map[strategyFeatureKey][]map[string]any

	mu          sync.RWMutex
	restoreMu   sync.Mutex
	cmd         *exec.Cmd
	connReady   bool
	lastError   string
	lastHealth  time.Time
	client      *StrategyServiceClient
	connClose   func() error
	events      map[chan EventEnvelope]struct{}
	instances   map[string]StrategyInstance
	queueHandle *queuewatch.QueueHandle
	queueCap    int
	lastRestart time.Time
	reportMu    sync.Mutex
	reports     map[string]*ReplayReport

	backtestMarketDSN string
	marketRealtimeDSN string
	marketReplayDSN   string
	sharedMetaDSN     string
}

func NewManager(cfg config.StrategyConfig, dsn string, registry *queuewatch.Registry) (*Manager, error) {
	store, err := NewStore(dsn)
	if err != nil {
		return nil, err
	}
	queueCfg := queuewatch.DefaultConfig("")
	if registry != nil {
		queueCfg = registry.Config()
	}
	m := &Manager{
		cfg:       cfg,
		store:     store,
		exec:      NewExecutionEngine(),
		features:  make(map[strategyFeatureKey][]map[string]any),
		events:    make(map[chan EventEnvelope]struct{}),
		instances: make(map[string]StrategyInstance),
		reports:   make(map[string]*ReplayReport),
		queueCap:  queueCfg.StrategyEventCapacity,
	}
	if registry != nil {
		m.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        "strategy_event_subscribers",
			Category:    "strategy",
			Criticality: "best_effort",
			Capacity:    queueCfg.StrategyEventCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "strategy Web event subscribers",
		})
	}
	if items, err := store.ListInstances(); err == nil {
		for _, item := range items {
			m.instances[item.InstanceID] = item
		}
	}
	SetDefaultSink(m)
	return m, nil
}

func (m *Manager) SetBacktestMarketDSN(dsn string) {
	m.mu.Lock()
	m.backtestMarketDSN = strings.TrimSpace(dsn)
	m.mu.Unlock()
}

func (m *Manager) SetMarketDataDSNs(realtimeDSN string, replayDSN string, sharedMetaDSN string) {
	m.mu.Lock()
	m.marketRealtimeDSN = strings.TrimSpace(realtimeDSN)
	m.marketReplayDSN = strings.TrimSpace(replayDSN)
	m.sharedMetaDSN = strings.TrimSpace(sharedMetaDSN)
	m.mu.Unlock()
}

func (m *Manager) Close() error {
	SetDefaultSink(nil)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connClose != nil {
		_ = m.connClose()
		m.connClose = nil
	}
	if m.cmd != nil && m.cmd.Process != nil {
		_ = m.cmd.Process.Kill()
		m.cmd = nil
	}
	return m.store.Close()
}

func (m *Manager) Start() error {
	if m == nil || !m.cfg.IsEnabled() {
		return nil
	}
	if m.cfg.IsAutoStart() {
		if err := m.ensureServiceStarted(false); err != nil {
			m.setError(err)
			return err
		}
	} else if err := m.connectWithRetry(0); err != nil {
		// 外部调试模式下 Python 进程可能晚于 Go 启动。
		// 这里不能直接让 Start 失败，否则 forwardStrategyEvents 不会启动，后续即使重连成功也无法把 trace 推到页面。
		m.setError(err)
		logger.Warn("strategy http unavailable on startup; health loop will keep reconnecting", "error", err, "http_addr", m.strategyHTTPAddr())
	}
	go m.healthLoop()
	return nil
}

func (m *Manager) RestartService() error {
	if m == nil || !m.cfg.IsEnabled() {
		return nil
	}
	if !m.cfg.IsAutoStart() {
		return fmt.Errorf("strategy auto_start is disabled")
	}
	runningIDs := make([]string, 0, 8)
	if items, err := m.store.ListInstances(); err == nil {
		for _, item := range items {
			if item.Status == InstanceStatusRunning {
				runningIDs = append(runningIDs, item.InstanceID)
			}
		}
	}
	if err := m.restartServiceLocked(true); err != nil {
		m.setError(err)
		return err
	}
	for _, instanceID := range runningIDs {
		if err := m.StartInstance(instanceID); err != nil {
			logger.Warn("restart strategy instance after service restart failed", "instance_id", instanceID, "error", err)
		}
	}
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) StartService() error {
	if m == nil || !m.cfg.IsEnabled() {
		return nil
	}
	if !m.cfg.IsAutoStart() {
		return fmt.Errorf("strategy auto_start is disabled")
	}
	m.mu.RLock()
	alreadyRunning := m.cmd != nil && m.cmd.Process != nil && m.connReady
	m.mu.RUnlock()
	if alreadyRunning {
		return nil
	}
	if err := m.ensureServiceStarted(true); err != nil {
		m.setError(err)
		return err
	}
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) StopService() error {
	if m == nil || !m.cfg.IsEnabled() {
		return nil
	}
	if !m.cfg.IsAutoStart() {
		return fmt.Errorf("strategy auto_start is disabled")
	}
	items, err := m.store.ListInstances()
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.Status != InstanceStatusRunning {
			continue
		}
		item.Status = InstanceStatusStopped
		item.LastError = ""
		if saveErr := m.store.SaveInstance(item); saveErr != nil {
			return saveErr
		}
		m.mu.Lock()
		m.instances[item.InstanceID] = item
		m.mu.Unlock()
	}
	if err := m.stopManagedProcess(); err != nil {
		m.setError(err)
		return err
	}
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) connectWithRetry(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := m.connect(); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (m *Manager) healthLoop() {
	ticker := time.NewTicker(time.Duration(m.cfg.HealthcheckIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if m == nil || !m.cfg.IsEnabled() {
			return
		}
		m.mu.RLock()
		ready := m.connReady
		m.mu.RUnlock()
		if ready {
			continue
		}
		if err := m.connect(); err != nil {
			m.setError(err)
		}
	}
}

func (m *Manager) strategyHTTPAddr() string {
	addr := strings.TrimSpace(m.cfg.HTTPAddr)
	if addr == "" {
		addr = "127.0.0.1:50051"
	}
	return addr
}

func (m *Manager) startProcessLocked() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cmd != nil && m.cmd.Process != nil {
		return nil
	}
	entry := strings.TrimSpace(m.cfg.PythonEntry)
	if entry == "" {
		entry = filepath.Join("python", "strategy_service.py")
	}
	workdir := strings.TrimSpace(m.cfg.PythonWorkdir)
	if workdir == "" {
		workdir = "."
	}
	logPath := filepath.Join(workdir, "flow", "strategy_logs", "strategy_service.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create strategy log directory failed: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open strategy log file failed: %w", err)
	}
	pythonExe, cmdEnv := m.resolvePythonRuntime()
	httpAddr := m.strategyHTTPAddr()
	cmd := exec.Command(pythonExe, entry, "--addr", httpAddr, "--log-file", logPath)
	cmd.Dir = workdir
	cmd.Env = cmdEnv
	cmd.Stdout = io.MultiWriter(os.Stdout, logFile)
	cmd.Stderr = io.MultiWriter(os.Stderr, logFile)
	logger.Info("starting python strategy service",
		"python_executable", pythonExe,
		"python_conda_env_path", strings.TrimSpace(m.cfg.PythonCondaEnvPath),
		"entry", entry,
		"workdir", workdir,
		"http_addr", httpAddr,
		"log_file", logPath,
	)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start python strategy service failed: %w", err)
	}
	m.cmd = cmd
	m.lastRestart = time.Now()
	logger.Info("python strategy service process started", "pid", cmd.Process.Pid)
	go func() {
		defer logFile.Close()
		err := cmd.Wait()
		if err != nil {
			m.setError(fmt.Errorf("python strategy process exited: %w", err))
		}
		m.mu.Lock()
		if m.cmd == cmd {
			m.cmd = nil
		}
		m.mu.Unlock()
	}()
	return nil
}

func (m *Manager) resolvePythonRuntime() (string, []string) {
	pythonExe := strings.TrimSpace(m.cfg.PythonExecutable)
	condaEnvPath := strings.TrimSpace(m.cfg.PythonCondaEnvPath)
	if pythonExe == "" && condaEnvPath != "" {
		pythonExe = filepath.Join(condaEnvPath, "python.exe")
	}
	if pythonExe == "" {
		pythonExe = "python"
	}
	env := os.Environ()
	if condaEnvPath == "" {
		return pythonExe, env
	}
	env = setEnvValue(env, "CONDA_PREFIX", condaEnvPath)
	env = setEnvValue(env, "CONDA_DEFAULT_ENV", filepath.Base(filepath.Clean(condaEnvPath)))
	env = setEnvValue(env, "PYTHONNOUSERSITE", "1")
	env = prependEnvPath(env, "PATH",
		condaEnvPath,
		filepath.Join(condaEnvPath, "Scripts"),
		filepath.Join(condaEnvPath, "Library", "bin"),
		filepath.Join(condaEnvPath, "Library", "usr", "bin"),
		filepath.Join(condaEnvPath, "Library", "mingw-w64", "bin"),
	)
	return pythonExe, env
}

func setEnvValue(env []string, key string, value string) []string {
	prefix := key + "="
	upperPrefix := strings.ToUpper(prefix)
	for i, item := range env {
		if strings.HasPrefix(strings.ToUpper(item), upperPrefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

func prependEnvPath(env []string, key string, entries ...string) []string {
	filtered := make([]string, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		filtered = append(filtered, entry)
	}
	if len(filtered) == 0 {
		return env
	}
	current := ""
	prefix := key + "="
	upperPrefix := strings.ToUpper(prefix)
	for _, item := range env {
		if strings.HasPrefix(strings.ToUpper(item), upperPrefix) {
			current = item[len(prefix):]
			break
		}
	}
	if current != "" {
		filtered = append(filtered, current)
	}
	return setEnvValue(env, key, strings.Join(filtered, string(os.PathListSeparator)))
}

func (m *Manager) restartServiceLocked(explicit bool) error {
	if err := m.stopManagedProcess(); err != nil {
		return err
	}
	if explicit {
		logger.Info("restarting python strategy service", "http_addr", m.strategyHTTPAddr())
	}
	if err := m.startProcessLocked(); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	m.mu.RLock()
	processRunning := m.cmd != nil && m.cmd.Process != nil
	m.mu.RUnlock()
	if !processRunning {
		return fmt.Errorf("python strategy service exited immediately; check http_addr=%s and strategy log", m.strategyHTTPAddr())
	}
	if err := m.connectWithRetry(5 * time.Second); err != nil {
		return err
	}
	return nil
}

func (m *Manager) ensureServiceStarted(logAlreadyRunning bool) error {
	entry := strings.TrimSpace(m.cfg.PythonEntry)
	if entry == "" {
		entry = filepath.Join("python", "strategy_service.py")
	}
	httpAddr := m.strategyHTTPAddr()
	if info, ok, err := findExistingPythonStrategyService(entry, httpAddr); err != nil {
		logger.Warn("probe existing python strategy service failed", "error", err, "http_addr", httpAddr)
	} else if ok {
		fields := []any{"pid", info.ProcessID, "http_addr", httpAddr}
		if startedAt := parseWMICreationTime(info.CreationDate); !startedAt.IsZero() {
			fields = append(fields, "started_at", startedAt.Format(time.RFC3339))
		}
		if logAlreadyRunning {
			logger.Info("python strategy service already running", fields...)
		} else {
			logger.Info("python strategy service already running on startup", fields...)
		}
		return m.connectWithRetry(5 * time.Second)
	}
	if err := m.startProcessLocked(); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	m.mu.RLock()
	processRunning := m.cmd != nil && m.cmd.Process != nil
	m.mu.RUnlock()
	if !processRunning {
		return fmt.Errorf("python strategy service exited immediately; check http_addr=%s and strategy log", httpAddr)
	}
	return m.connectWithRetry(5 * time.Second)
}

func (m *Manager) stopManagedProcess() error {
	m.mu.Lock()
	connClose := m.connClose
	cmd := m.cmd
	m.connClose = nil
	m.client = nil
	m.connReady = false
	m.mu.Unlock()
	if connClose != nil {
		_ = connClose()
	}
	if cmd != nil && cmd.Process != nil {
		logger.Info("stopping managed python strategy service", "pid", cmd.Process.Pid)
		if err := cmd.Process.Kill(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "process has already exited") {
			return fmt.Errorf("stop managed python strategy service failed: %w", err)
		}
	}
	entry := strings.TrimSpace(m.cfg.PythonEntry)
	if entry == "" {
		entry = filepath.Join("python", "strategy_service.py")
	}
	if err := stopExistingPythonStrategyService(entry, m.strategyHTTPAddr()); err != nil {
		return err
	}
	return nil
}

func findExistingPythonStrategyService(entry string, httpAddr string) (strategyServiceProcessInfo, bool, error) {
	scriptName := strings.ToLower(strings.TrimSpace(filepath.Base(entry)))
	if scriptName == "" {
		scriptName = "strategy_service.py"
	}
	addrText := strings.TrimSpace(httpAddr)
	ps := fmt.Sprintf(`$item = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -like '*%s*' -and $_.CommandLine -like '*--addr %s*' } | Sort-Object CreationDate | Select-Object -First 1 ProcessId,CreationDate,CommandLine; if ($null -eq $item) { '' } else { $item | ConvertTo-Json -Compress }`, scriptName, addrText)
	cmd := exec.Command("powershell", "-NoProfile", "-Command", ps)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return strategyServiceProcessInfo{}, false, fmt.Errorf("probe existing python strategy service failed: %w", err)
	}
	raw := strings.TrimSpace(string(out))
	if raw == "" {
		return strategyServiceProcessInfo{}, false, nil
	}
	var info strategyServiceProcessInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		return strategyServiceProcessInfo{}, false, fmt.Errorf("decode existing python strategy service info failed: %w", err)
	}
	return info, info.ProcessID > 0, nil
}

func parseWMICreationTime(value string) time.Time {
	raw := strings.TrimSpace(value)
	if len(raw) < 14 {
		return time.Time{}
	}
	ts, err := time.Parse("20060102150405", raw[:14])
	if err != nil {
		return time.Time{}
	}
	return ts
}

func stopExistingPythonStrategyService(entry string, httpAddr string) error {
	scriptName := strings.ToLower(strings.TrimSpace(filepath.Base(entry)))
	if scriptName == "" {
		scriptName = "strategy_service.py"
	}
	addrText := strings.TrimSpace(httpAddr)
	ps := fmt.Sprintf(`$items = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -like '*%s*' -and $_.CommandLine -like '*--addr %s*' }; if (-not $items) { 'no existing python strategy service'; exit 0 }; foreach ($item in $items) { 'stopping pid=' + $item.ProcessId + ' cmd=' + $item.CommandLine; Stop-Process -Id $item.ProcessId -Force; 'stopped pid=' + $item.ProcessId }`, scriptName, addrText)
	logger.Info("stop existing python strategy service command", "shell", "powershell", "command", ps)
	cmd := exec.Command("powershell", "-NoProfile", "-Command", ps)
	out, err := cmd.CombinedOutput()
	result := strings.TrimSpace(string(out))
	if result == "" {
		result = "<empty>"
	}
	logger.Info("stop existing python strategy service result", "result", result)
	if err != nil {
		return fmt.Errorf("stop existing python strategy service failed: %w", err)
	}
	return nil
}

func (m *Manager) connect() error {
	addr := m.strategyHTTPAddr()
	ctx, cancel := context.WithTimeout(context.Background(), m.requestTimeout())
	client := NewStrategyServiceClient(addr, nil)
	_, err := client.Ping(ctx)
	cancel()
	if err != nil {
		// logger.Warn("strategy http ping failed", "http_addr", addr, "error", err)
		return err
	}
	m.mu.Lock()
	becameReady := !m.connReady
	if m.connClose != nil {
		_ = m.connClose()
	}
	m.client = client
	m.connReady = true
	m.connClose = nil
	m.lastError = ""
	m.lastHealth = time.Now()
	m.mu.Unlock()
	m.broadcast("strategy_status_update", m.Status())
	if becameReady {
		m.afterStrategyServiceConnected("http_connect")
	}
	return nil
}

func (m *Manager) afterStrategyServiceConnected(reason string) {
	if _, err := m.syncDefinitions(); err != nil {
		logger.Warn("strategy definition sync failed after python connection", "reason", reason, "error", err)
	}
	if err := m.restoreRunningInstances(reason); err != nil {
		logger.Warn("restore running strategy instances failed after python connection", "reason", reason, "error", err)
	}
}

func (m *Manager) ping() (HealthResponse, error) {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return HealthResponse{}, fmt.Errorf("strategy http client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	resp, err := client.Ping(ctx)
	if err == nil {
		m.mu.Lock()
		m.lastHealth = time.Now()
		m.connReady = true
		m.lastError = ""
		m.mu.Unlock()
	}
	return resp, err
}

func (m *Manager) syncDefinitions() ([]StrategyDefinition, error) {
	if m.store == nil {
		return nil, fmt.Errorf("strategy store not configured")
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("strategy http client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	resp, err := client.ListStrategies(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]StrategyDefinition, 0, len(resp.Strategies))
	now := time.Now()
	for _, item := range resp.Strategies {
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = now
		}
		items = append(items, item)
	}
	if err := m.store.ReplaceDefinitions(items); err != nil {
		return nil, err
	}
	return items, nil
}

func (m *Manager) Status() ManagerStatus {
	defs, insts := 0, 0
	var sigs, audits, runs int64
	if m.store != nil {
		defs, insts, sigs, audits, runs, _ = m.store.Counts()
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	running := 0
	httpAddr := m.strategyHTTPAddr()
	for _, inst := range m.instances {
		if inst.Status == InstanceStatusRunning {
			running++
		}
	}
	return ManagerStatus{
		Enabled:            m.cfg.IsEnabled(),
		ProcessRunning:     m.cmd != nil || m.connReady,
		Connected:          m.connReady,
		HTTPAddr:           httpAddr,
		PythonExecutable:   m.cfg.PythonExecutable,
		PythonCondaEnvPath: m.cfg.PythonCondaEnvPath,
		PythonEntry:        m.cfg.PythonEntry,
		LastError:          m.lastError,
		LastHealthAt:       m.lastHealth,
		LastRestartAt:      m.lastRestart,
		UpdatedAt:          time.Now(),
		Definitions:        defs,
		Instances:          insts,
		RunningCount:       running,
		SignalCount:        sigs,
		AuditCount:         audits,
		BacktestRunCount:   runs,
	}
}

func (m *Manager) ListDefinitions() ([]StrategyDefinition, error) {
	if m.store == nil {
		return []StrategyDefinition{}, nil
	}
	items, err := m.store.ListDefinitions()
	if err == nil && len(items) > 0 {
		return items, nil
	}
	items, err = m.syncDefinitions()
	if err != nil {
		logger.Warn("strategy definitions unavailable; reconnecting before returning list", "error", err)
		if connectErr := m.connect(); connectErr == nil {
			items, err = m.syncDefinitions()
		} else {
			err = connectErr
		}
		if err != nil {
			// 策略列表必须反映 Python ListStrategies 当前导出的算法，而不是 Go 侧兜底定义。
			// Python 未连接时返回空列表，避免数据库旧行继续展示当前 Python 服务未必提供的策略。
			logger.Warn("strategy definitions unavailable from python; returning empty list", "error", err)
			return []StrategyDefinition{}, nil
		}
	}
	return items, nil
}

func (m *Manager) ListInstances() ([]StrategyInstance, error) {
	items, err := m.store.ListInstances()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances = make(map[string]StrategyInstance, len(items))
	for _, item := range items {
		m.instances[item.InstanceID] = item
	}
	out := make([]StrategyInstance, len(items))
	for i, item := range items {
		out[i] = sanitizeStrategyInstanceForList(item)
	}
	return out, nil
}

func sanitizeStrategyInstanceForList(inst StrategyInstance) StrategyInstance {
	if len(inst.Params) == 0 {
		return inst
	}
	if _, ok := inst.Params["warmup_bars"]; !ok {
		return inst
	}
	params := cloneStrategyParams(inst.Params)
	// warmup_bars 是启动 Python runtime 时的内部大对象，页面只需要看到实例和状态。
	// 历史实例若把它持久化，列表接口会变成 MB 级，拖慢启动/停止后的刷新并让按钮看似无响应。
	delete(params, "warmup_bars")
	params["warmup_bars_omitted"] = true
	inst.Params = params
	return inst
}

func (m *Manager) SaveInstance(inst StrategyInstance) error {
	if strings.TrimSpace(inst.InstanceID) != "" {
		if existing, err := m.store.GetInstance(inst.InstanceID); err == nil {
			if inst.CreatedAt.IsZero() {
				inst.CreatedAt = existing.CreatedAt
			}
			if inst.LastSignalAt == nil {
				inst.LastSignalAt = existing.LastSignalAt
			}
			if inst.LastStartedAt == nil {
				inst.LastStartedAt = existing.LastStartedAt
			}
			if inst.LastTargetPosition == 0 {
				inst.LastTargetPosition = existing.LastTargetPosition
			}
			if strings.TrimSpace(inst.Status) == "" {
				inst.Status = existing.Status
			}
			if strings.TrimSpace(inst.LastError) == "" {
				inst.LastError = existing.LastError
			}
		}
	}
	if strings.TrimSpace(inst.InstanceID) == "" {
		inst.InstanceID = mustRunID("inst")
	}
	if strings.TrimSpace(inst.DisplayName) == "" {
		inst.DisplayName = inst.InstanceID
	}
	if strings.TrimSpace(inst.Status) == "" {
		inst.Status = InstanceStatusStopped
	}
	if err := m.store.SaveInstance(inst); err != nil {
		return err
	}
	m.mu.Lock()
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) StartInstance(instanceID string) error {
	inst, err := m.store.GetInstance(instanceID)
	if err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(inst.Mode), RunTypeReplay) {
		if _, err := m.clearReplaySignalsForInstanceStart(inst); err != nil {
			return err
		}
	}
	if err := m.callStartInstance(inst); err != nil {
		inst.Status = InstanceStatusError
		inst.LastError = err.Error()
		_ = m.store.SaveInstance(inst)
		return err
	}
	inst.Status = InstanceStatusRunning
	inst.LastError = ""
	now := time.Now()
	inst.LastStartedAt = &now
	if err := m.store.SaveInstance(inst); err != nil {
		return err
	}
	m.mu.Lock()
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
	m.persistInstanceStartTrace(inst)
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) callStartInstance(inst StrategyInstance) error {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return fmt.Errorf("strategy http client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.runtimeStartTimeout())
	defer cancel()
	_, err := m.startRuntimeInstance(ctx, client, inst)
	return err
}

func (m *Manager) requestTimeout() time.Duration {
	timeout := time.Duration(m.cfg.RequestTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		return 3 * time.Second
	}
	return timeout
}

func (m *Manager) runtimeStartTimeout() time.Duration {
	timeout := m.requestTimeout()
	// StartInstance 比 Ping/ListStrategies 重：它会先问 Python warmup 需求、从数据库补齐 K 线，
	// 再把 warmup_bars 发送给 Python。调试模式下 Python 首次构造 runtime 还会被 debugpy 放大耗时。
	// 不能沿用 3 秒普通请求超时，否则 Python 已经启动完成时 Go 可能先判 DeadlineExceeded，页面就显示启动失败。
	if timeout < minRuntimeStartTimeout {
		return minRuntimeStartTimeout
	}
	return timeout
}

func (m *Manager) StopInstance(instanceID string) error {
	inst, err := m.store.GetInstance(instanceID)
	if err != nil {
		return err
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
		_ = client.StopInstance(ctx, StopInstanceRequest{InstanceID: instanceID})
		cancel()
	}
	inst.Status = InstanceStatusStopped
	inst.LastError = ""
	if err := m.store.SaveInstance(inst); err != nil {
		return err
	}
	m.mu.Lock()
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
	if strings.ToLower(strings.TrimSpace(inst.Mode)) == RunTypeReplay {
		m.FinalizeReplayReportsForInstance(inst.InstanceID, InstanceStatusStopped)
	}
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) ListSignals(instanceID string, symbol string, limit int) ([]SignalRecord, error) {
	return m.store.ListSignals(strings.TrimSpace(instanceID), strings.TrimSpace(symbol), limit)
}

func (m *Manager) DeleteSignals(instanceID string) (int64, error) {
	return m.store.DeleteSignals(strings.TrimSpace(instanceID))
}

func (m *Manager) clearReplaySignalsForInstanceStart(inst StrategyInstance) (int64, error) {
	deleted, err := m.store.DeleteSignals(inst.InstanceID)
	if err != nil {
		return deleted, err
	}
	for _, symbol := range inst.Symbols {
		n, err := m.store.DeleteReplaySignalsForScope(inst.StrategyID, symbol, inst.Timeframe)
		deleted += n
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (m *Manager) ListRuns(limit int) ([]StrategyRun, error) {
	return m.store.ListRuns(limit)
}

func (m *Manager) GetRun(runID string) (StrategyRun, error) {
	return m.store.GetRun(runID)
}

func (m *Manager) ListOrderAudits(limit int) ([]OrderAuditRecord, error) {
	return m.store.ListOrderAudits(limit)
}

func (m *Manager) ListTraces(instanceID string, symbol string, limit int) ([]StrategyTraceRecord, error) {
	return m.store.ListTraces(strings.TrimSpace(instanceID), strings.TrimSpace(symbol), limit)
}

func (m *Manager) restoreRunningInstances(reason string) error {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return fmt.Errorf("strategy http client not connected")
	}
	return m.restoreRunningInstancesWithClient(context.Background(), client, reason)
}

func (m *Manager) restoreRunningInstancesWithClient(ctx context.Context, client runtimeStartClient, reason string) error {
	m.restoreMu.Lock()
	defer m.restoreMu.Unlock()
	if m == nil || m.store == nil {
		return nil
	}
	items, err := m.store.ListInstances()
	if err != nil {
		return err
	}
	var firstErr error
	restored := 0
	for _, inst := range items {
		if inst.Status != InstanceStatusRunning {
			continue
		}
		// Python runtime 是进程内状态，Go 重启或 Python 重启后不会自动拥有数据库里的 running 实例。
		// 因此每次 HTTP 从断开变为可用时，都要把这些实例重新 StartInstance 一遍；否则页面能看到运行实例，
		// 但 Python 收到 OnBar 时会报 “runtime instance not started”，最终没有策略过程 trace。
		startCtx := ctx
		cancel := func() {}
		if _, ok := startCtx.Deadline(); !ok {
			startCtx, cancel = context.WithTimeout(startCtx, m.runtimeStartTimeout())
		}
		_, startErr := m.startRuntimeInstance(startCtx, client, inst)
		cancel()
		if startErr != nil {
			logger.Warn("restore running strategy instance failed", "instance_id", inst.InstanceID, "strategy_id", inst.StrategyID, "reason", reason, "error", startErr)
			if firstErr == nil {
				firstErr = startErr
			}
			m.setInstanceError(inst.InstanceID, startErr)
			continue
		}
		now := time.Now()
		inst.LastStartedAt = &now
		inst.LastError = ""
		if saveErr := m.store.SaveInstance(inst); saveErr != nil {
			if firstErr == nil {
				firstErr = saveErr
			}
			m.setError(saveErr)
			continue
		}
		m.mu.Lock()
		m.instances[inst.InstanceID] = inst
		m.mu.Unlock()
		m.persistInstanceRestoreTrace(inst, reason)
		restored++
	}
	if restored > 0 {
		logger.Info("restored running strategy instances after python connection", "count", restored, "reason", reason)
		m.broadcast("strategy_status_update", m.Status())
	}
	return firstErr
}

func (m *Manager) OrdersStatus() OrdersStatus {
	return m.exec.Status()
}

func (m *Manager) RunBacktest(req BacktestRequest) (StrategyRun, error) {
	if shouldRunLocalMA20Backtest(req) {
		return m.runLocalMA20Backtest(req)
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return StrategyRun{}, fmt.Errorf("strategy http client not connected")
	}
	if strings.TrimSpace(req.RunID) == "" {
		req.RunID = mustRunID("backtest")
	}
	run := StrategyRun{
		RunID:      req.RunID,
		InstanceID: req.Instance.InstanceID,
		StrategyID: req.Instance.StrategyID,
		RunType:    RunTypeBacktest,
		Status:     "running",
		Symbol:     req.Symbol,
		Timeframe:  req.Timeframe,
		StartedAt:  time.Now(),
		Summary:    map[string]any{},
	}
	if err := m.store.SaveRun(run); err != nil {
		return StrategyRun{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := client.RunBacktest(ctx, req)
	if err != nil {
		run.Status = InstanceStatusError
		run.LastError = err.Error()
		finished := time.Now()
		run.FinishedAt = &finished
		_ = m.store.SaveRun(run)
		return run, err
	}
	outputPath, err := m.writeBacktestOutput(req.RunID, resp)
	if err != nil {
		return run, err
	}
	run.Status = resp.Status
	run.OutputPath = outputPath
	run.Summary = resp.Summary
	finished := time.Now()
	run.FinishedAt = &finished
	if err := m.store.SaveRun(run); err != nil {
		return StrategyRun{}, err
	}
	m.broadcast("strategy_backtest_done", run)
	return run, nil
}

func shouldRunLocalMA20Backtest(req BacktestRequest) bool {
	if IsMA20WeakStrategyID(req.Instance.StrategyID) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(ma20ParamString(req.Parameters, "engine", "")), "go_ma20") {
		return true
	}
	return false
}

func (m *Manager) runLocalMA20Backtest(req BacktestRequest) (StrategyRun, error) {
	if strings.TrimSpace(req.RunID) == "" {
		req.RunID = mustRunID("backtest")
	}
	run := StrategyRun{
		RunID:      req.RunID,
		InstanceID: req.Instance.InstanceID,
		StrategyID: firstNonEmpty(req.Instance.StrategyID, MA20WeakStrategyID),
		RunType:    RunTypeBacktest,
		Status:     "running",
		Symbol:     req.Symbol,
		Timeframe:  firstNonEmpty(req.Timeframe, "5m"),
		StartedAt:  time.Now(),
		Summary:    map[string]any{},
	}
	if err := m.store.SaveRun(run); err != nil {
		return StrategyRun{}, err
	}
	m.mu.RLock()
	marketDSN := strings.TrimSpace(m.backtestMarketDSN)
	m.mu.RUnlock()
	if marketDSN == "" {
		run.Status = InstanceStatusError
		run.LastError = "market realtime DSN is not configured"
		finished := time.Now()
		run.FinishedAt = &finished
		_ = m.store.SaveRun(run)
		return run, fmt.Errorf("%s", run.LastError)
	}
	db, err := dbx.Open(marketDSN)
	if err != nil {
		run.Status = InstanceStatusError
		run.LastError = err.Error()
		finished := time.Now()
		run.FinishedAt = &finished
		_ = m.store.SaveRun(run)
		return run, err
	}
	defer db.Close()
	resp, err := runLocalMA20BacktestWithDB(context.Background(), db, req)
	if err != nil {
		run.Status = InstanceStatusError
		run.LastError = err.Error()
		finished := time.Now()
		run.FinishedAt = &finished
		_ = m.store.SaveRun(run)
		return run, err
	}
	resp.RunID = req.RunID
	outputPath, err := m.writeBacktestOutput(req.RunID, resp)
	if err != nil {
		return run, err
	}
	run.Status = resp.Status
	run.OutputPath = outputPath
	run.Summary = resp.Summary
	finished := time.Now()
	run.FinishedAt = &finished
	if err := m.store.SaveRun(run); err != nil {
		return StrategyRun{}, err
	}
	m.broadcast("strategy_backtest_done", run)
	return run, nil
}

func runLocalMA20BacktestWithDB(ctx context.Context, db *sql.DB, req BacktestRequest) (BacktestResponse, error) {
	cfg := DefaultMA20BacktestConfig()
	cfg.Period = firstNonEmpty(req.Timeframe, cfg.Period)
	cfg.Tables = ma20BacktestTablesFromRequest(req)
	cfg.Algorithms = ma20ParamStringList(req.Parameters, "algorithms", cfg.Algorithms)
	if _, ok := req.Parameters["algorithms"]; !ok {
		if algo := MA20AlgorithmForStrategyID(req.Instance.StrategyID); algo != "" {
			cfg.Algorithms = []string{algo}
		}
	}
	cfg.ObservationBars = ma20ParamInt(req.Parameters, "observation_bars", cfg.ObservationBars)
	cfg.ProfitATRMultiple = ma20ParamFloat(req.Parameters, "profit_atr_multiple", cfg.ProfitATRMultiple)
	cfg.AdverseATRMultiple = ma20ParamFloat(req.Parameters, "adverse_atr_multiple", cfg.AdverseATRMultiple)
	cfg.StrengthExitBars = ma20ParamInt(req.Parameters, "strength_exit_bars", cfg.StrengthExitBars)
	cfg.ProfitReboundATR = ma20ParamFloat(req.Parameters, "profit_rebound_atr_multiple", cfg.ProfitReboundATR)
	cfg.ProfitReboundATR = ma20ParamFloat(req.Parameters, "profit_rebound_atr", cfg.ProfitReboundATR)
	cfg.ProfitRisingLowBars = ma20ParamInt(req.Parameters, "profit_rising_low_bars", cfg.ProfitRisingLowBars)
	cfg.StrongBullATR = ma20ParamFloat(req.Parameters, "strong_bull_atr_multiple", cfg.StrongBullATR)
	cfg.StrongBullATR = ma20ParamFloat(req.Parameters, "strong_bull_atr", cfg.StrongBullATR)
	cfg.StructureWaitBars = ma20ParamInt(req.Parameters, "structure_wait_bars", cfg.StructureWaitBars)
	cfg.TouchWaitBars = ma20ParamInt(req.Parameters, "touch_wait_bars", cfg.TouchWaitBars)
	cfg.TriggerWaitBars = ma20ParamInt(req.Parameters, "trigger_wait_bars", cfg.TriggerWaitBars)
	cfg.SwingLookbackBars = ma20ParamInt(req.Parameters, "swing_lookback_bars", cfg.SwingLookbackBars)
	cfg.SlopeLookbackBars = ma20ParamInt(req.Parameters, "slope_lookback_bars", cfg.SlopeLookbackBars)
	cfg.ReportAttemptLimit = ma20ParamInt(req.Parameters, "report_attempt_limit", cfg.ReportAttemptLimit)
	if start, ok, err := parseBacktestOptionalTime(req.StartTime); err != nil {
		return BacktestResponse{}, err
	} else if ok {
		cfg.StartTime = start
	}
	if end, ok, err := parseBacktestOptionalTime(req.EndTime); err != nil {
		return BacktestResponse{}, err
	} else if ok {
		cfg.EndTime = end
	}
	return RunMA20Backtest(ctx, db, cfg)
}

func ma20BacktestTablesFromRequest(req BacktestRequest) []string {
	if items := ma20ParamStringList(req.Parameters, "tables", nil); len(items) > 0 {
		return items
	}
	symbol := strings.ToLower(strings.TrimSpace(req.Symbol))
	if symbol == "" || symbol == "all" || symbol == "*" {
		return append([]string(nil), DefaultMA20BacktestTables...)
	}
	variety := symbol
	if strings.HasPrefix(variety, "future_kline_l9_mm_") {
		return []string{variety}
	}
	if strings.HasSuffix(variety, "l9") {
		variety = strings.TrimSuffix(variety, "l9")
	} else if extracted := leadingLetters(variety); extracted != "" {
		variety = extracted
	}
	if variety == "" {
		return append([]string(nil), DefaultMA20BacktestTables...)
	}
	return []string{"future_kline_l9_mm_" + variety}
}

func leadingLetters(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' {
			b.WriteRune(r)
			continue
		}
		break
	}
	return b.String()
}

func ma20ParamStringList(params map[string]any, key string, fallback []string) []string {
	if params == nil {
		return fallback
	}
	raw, ok := params[key]
	if !ok {
		return fallback
	}
	switch v := raw.(type) {
	case []string:
		return sanitizeMA20List(v)
	case []any:
		items := make([]string, 0, len(v))
		for _, item := range v {
			items = append(items, fmt.Sprint(item))
		}
		return sanitizeMA20List(items)
	case string:
		return sanitizeMA20List(strings.Split(v, ","))
	default:
		return fallback
	}
}

func ma20ParamInt(params map[string]any, key string, fallback int) int {
	if params == nil {
		return fallback
	}
	raw, ok := params[key]
	if !ok {
		return fallback
	}
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return int(n)
		}
	case string:
		var out int
		if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &out); err == nil {
			return out
		}
	}
	return fallback
}

func ma20ParamString(params map[string]any, key string, fallback string) string {
	if params == nil {
		return fallback
	}
	raw, ok := params[key]
	if !ok {
		return fallback
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func ma20ParamFloat(params map[string]any, key string, fallback float64) float64 {
	if params == nil {
		return fallback
	}
	raw, ok := params[key]
	if !ok {
		return fallback
	}
	switch v := raw.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		n, err := v.Float64()
		if err == nil {
			return n
		}
	case string:
		var out float64
		if _, err := fmt.Sscanf(strings.TrimSpace(v), "%f", &out); err == nil {
			return out
		}
	}
	return fallback
}

func parseBacktestOptionalTime(raw string) (time.Time, bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, false, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	}
	value = strings.ReplaceAll(value, "T", " ")
	for _, layout := range layouts {
		candidate := value
		if layout == time.RFC3339Nano || layout == time.RFC3339 {
			candidate = raw
		}
		if ts, err := time.ParseInLocation(layout, strings.TrimSpace(candidate), time.Local); err == nil {
			return ts, true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid backtest time: %s", raw)
}

func (m *Manager) RunParameterSweep(req ParameterSweepRequest) (StrategyRun, error) {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return StrategyRun{}, fmt.Errorf("strategy http client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := client.RunParameterSweep(ctx, req)
	if err != nil {
		return StrategyRun{}, err
	}
	run := StrategyRun{
		RunID:      resp.RunID,
		StrategyID: req.StrategyID,
		RunType:    "optimize",
		Status:     resp.Status,
		Symbol:     req.Symbol,
		Timeframe:  req.Timeframe,
		Summary:    resp.Summary,
		StartedAt:  time.Now(),
	}
	if err := m.store.SaveRun(run); err != nil {
		return StrategyRun{}, err
	}
	m.broadcast("strategy_backtest_done", run)
	return run, nil
}

func (m *Manager) writeBacktestOutput(runID string, resp BacktestResponse) (string, error) {
	base := m.cfg.BacktestOutputDir
	if base == "" {
		base = filepath.Join("flow", "strategy_backtests")
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(base, runID+".json")
	body, err := json.MarshalIndent(resp.Result, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func (m *Manager) HandleRealtimeTick(ev TickEvent) { m.handleTick(ev, RunTypeRealtime) }
func (m *Manager) HandleReplayTick(ev TickEvent)   { m.handleTick(ev, RunTypeReplay) }
func (m *Manager) HandleRealtimeBar(ev BarEvent)   { m.handleBar(ev, RunTypeRealtime) }
func (m *Manager) HandleReplayBar(ev BarEvent)     { m.handleBar(ev, RunTypeReplay) }

func (m *Manager) handleTick(ev TickEvent, mode string) {
	m.forEachMatchingInstance(ev.InstrumentID, "", mode, func(inst StrategyInstance) {
		m.callDecision(inst, ev.InstrumentID, mode, ev.ReplayTaskID, ev.ReceivedAt, &ev, nil)
	})
}

func (m *Manager) handleBar(ev BarEvent, mode string) {
	// logger.Info("strategy bar event received",
	// 	"symbol", ev.InstrumentID,
	// 	"timeframe", ev.Period,
	// 	"mode", mode,
	// 	"event_time", strategyBarEventTime(ev),
	// )
	indicators, trading := splitMatchingInstances(m.matchingInstances(ev.InstrumentID, ev.Period, mode))
	for _, inst := range indicators {
		m.callDecision(inst, ev.InstrumentID, mode, ev.ReplayTaskID, strategyBarEventTime(ev), nil, &ev)
	}
	for _, inst := range trading {
		m.callDecision(inst, ev.InstrumentID, mode, ev.ReplayTaskID, strategyBarEventTime(ev), nil, &ev)
	}
}

func (m *Manager) forEachMatchingInstance(symbol string, timeframe string, mode string, fn func(StrategyInstance)) {
	for _, item := range m.matchingInstances(symbol, timeframe, mode) {
		fn(item)
	}
}

func (m *Manager) matchingInstances(symbol string, timeframe string, mode string) []StrategyInstance {
	m.mu.RLock()
	items := make([]StrategyInstance, 0, len(m.instances))
	for _, item := range m.instances {
		items = append(items, item)
	}
	m.mu.RUnlock()
	out := make([]StrategyInstance, 0, len(items))
	for _, item := range items {
		if item.Status != InstanceStatusRunning {
			continue
		}
		if item.Mode != mode && !(mode == RunTypeRealtime && item.Mode == "paper") {
			continue
		}
		if timeframe != "" && item.Timeframe != "" && item.Timeframe != timeframe {
			continue
		}
		if len(item.Symbols) > 0 && !containsFold(item.Symbols, symbol) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func splitMatchingInstances(items []StrategyInstance) ([]StrategyInstance, []StrategyInstance) {
	indicators := make([]StrategyInstance, 0, len(items))
	trading := make([]StrategyInstance, 0, len(items))
	for _, item := range items {
		if isIndicatorStrategyID(item.StrategyID) {
			indicators = append(indicators, item)
			continue
		}
		trading = append(trading, item)
	}
	return indicators, trading
}

func isIndicatorStrategyID(strategyID string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(strategyID)), "indicator.")
}

func (m *Manager) callDecision(inst StrategyInstance, symbol string, mode string, replayTaskID string, eventTime time.Time, tick *TickEvent, bar *BarEvent) {
	m.touchReplayReport(replayTaskID, inst)
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return
	}
	if bar != nil {
		eventTime = strategyBarEventTime(*bar)
	}
	req := DecisionRequest{
		Instance:        runtimeStrategyInstance(inst),
		Symbol:          symbol,
		EventTime:       eventTime.Format(time.RFC3339Nano),
		Mode:            mode,
		CurrentPosition: m.exec.CurrentPosition(symbol),
		Account:         map[string]any{"account_id": inst.AccountID},
		Features:        m.featuresFor(inst, symbol, mode),
		Tick:            tick,
		Bar:             bar,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	var decision SignalDecision
	var err error
	switch {
	case tick != nil:
		decision, err = client.OnTick(ctx, req)
	case mode == RunTypeReplay:
		// logger.Info("strategy OnReplayBar call",
		// 	"instance_id", inst.InstanceID,
		// 	"strategy_id", inst.StrategyID,
		// 	"symbol", symbol,
		// 	"timeframe", inst.Timeframe,
		// 	"event_time", eventTime,
		// )
		decision, err = client.OnReplayBar(ctx, req)
	default:
		logger.Info("strategy OnBar call",
			"instance_id", inst.InstanceID,
			"strategy_id", inst.StrategyID,
			"symbol", symbol,
			"timeframe", inst.Timeframe,
			"event_time", eventTime,
		)
		decision, err = client.OnBar(ctx, req)
	}
	if err != nil {
		m.setInstanceError(inst.InstanceID, err)
		return
	}
	if decision.Trace != nil {
		trace := normalizeStrategyTrace(inst, symbol, mode, eventTime, *decision.Trace)
		m.persistTrace(inst, symbol, mode, eventTime, trace)
		m.updateFeatureCacheFromTrace(trace)
	}
	if decision.NoSignal {
		if decision.Trace != nil {
			m.persistSignalResultPoint(inst, symbol, mode, eventTime, *decision.Trace)
		}
		return
	}
	m.persistDecision(inst, symbol, mode, replayTaskID, eventTime, decision)
}

func featureKey(mode string, symbol string, timeframe string, indicator string) strategyFeatureKey {
	return strategyFeatureKey{
		Mode:      strings.ToLower(strings.TrimSpace(mode)),
		Symbol:    strings.ToLower(strings.TrimSpace(symbol)),
		Timeframe: strings.ToLower(strings.TrimSpace(timeframe)),
		Indicator: strings.ToLower(strings.TrimSpace(indicator)),
	}
}

func (m *Manager) updateFeatureCacheFromTrace(trace StrategyTraceRecord) {
	if m == nil || !isIndicatorStrategyID(trace.StrategyID) || trace.Metrics == nil {
		return
	}
	indicator := strings.ToLower(strings.TrimSpace(fmt.Sprint(trace.Metrics["indicator"])))
	if indicator != zigzagATR26FeatureID {
		return
	}
	typ := strings.ToUpper(strings.TrimSpace(fmt.Sprint(trace.Metrics["zigzag_type"])))
	if typ != "PEAK" {
		return
	}
	pivotIndex, ok := metricInt(trace.Metrics["pivot_index"])
	if !ok {
		return
	}
	confirmedTime := strings.TrimSpace(fmt.Sprint(trace.Metrics["confirmed_time"]))
	if confirmedTime == "" || confirmedTime == "<nil>" {
		confirmedTime = trace.EventTime.Format(time.RFC3339Nano)
	}
	pivot := map[string]any{
		"pivot_index":    pivotIndex,
		"pivot_time":     trace.Metrics["pivot_time"],
		"pivot_price":    trace.Metrics["pivot_price"],
		"confirmed_time": confirmedTime,
	}
	if confirmedIndex, ok := metricInt(trace.Metrics["confirmed_index"]); ok {
		pivot["confirmed_index"] = confirmedIndex
	}
	key := featureKey(trace.Mode, trace.Symbol, trace.Timeframe, indicator)
	m.featureMu.Lock()
	if m.features == nil {
		m.features = make(map[strategyFeatureKey][]map[string]any)
	}
	items := append(m.features[key], pivot)
	const keep = 64
	if len(items) > keep {
		items = items[len(items)-keep:]
	}
	m.features[key] = items
	m.featureMu.Unlock()
}

func (m *Manager) featuresFor(inst StrategyInstance, symbol string, mode string) map[string]any {
	if m == nil {
		return nil
	}
	key := featureKey(mode, symbol, inst.Timeframe, zigzagATR26FeatureID)
	m.featureMu.RLock()
	items := m.features[key]
	m.featureMu.RUnlock()
	if len(items) == 0 {
		return nil
	}
	limit := 3
	if len(items) < limit {
		limit = len(items)
	}
	peaks := make([]map[string]any, 0, limit)
	for i := len(items) - 1; i >= 0 && len(peaks) < limit; i-- {
		item := make(map[string]any, len(items[i]))
		for k, v := range items[i] {
			item[k] = v
		}
		peaks = append(peaks, item)
	}
	return map[string]any{
		zigzagATR26FeatureID: map[string]any{
			"peaks": peaks,
		},
	}
}

func metricInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case int32:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			return int(i), true
		}
		f, err := v.Float64()
		if err == nil {
			return int(f), true
		}
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

func (m *Manager) persistTrace(inst StrategyInstance, symbol string, mode string, eventTime time.Time, trace StrategyTraceRecord) {
	if m == nil || m.store == nil {
		return
	}
	trace = normalizeStrategyTrace(inst, symbol, mode, eventTime, trace)
	if !isPersistableTraceEventType(trace.EventType) {
		return
	}
	id, err := m.store.AppendTrace(trace)
	if err != nil {
		m.setError(err)
		return
	}
	trace.TraceID = id
	m.broadcast("strategy_trace_update", trace)
}

func normalizeStrategyTrace(inst StrategyInstance, symbol string, mode string, eventTime time.Time, trace StrategyTraceRecord) StrategyTraceRecord {
	if strings.TrimSpace(trace.EventType) == "" {
		trace.EventType = "bar"
	}
	trace.InstanceID = firstNonEmpty(trace.InstanceID, inst.InstanceID)
	trace.StrategyID = firstNonEmpty(trace.StrategyID, inst.StrategyID)
	trace.Symbol = firstNonEmpty(trace.Symbol, symbol)
	trace.Timeframe = firstNonEmpty(trace.Timeframe, inst.Timeframe)
	trace.Mode = firstNonEmpty(trace.Mode, mode)
	if trace.EventTime.IsZero() {
		trace.EventTime = eventTime
	}
	if trace.CreatedAt.IsZero() {
		trace.CreatedAt = time.Now()
	}
	if trace.Metrics == nil {
		trace.Metrics = map[string]any{}
	}
	if trace.SignalPreview == nil {
		trace.SignalPreview = map[string]any{}
	}
	return trace
}

func isPersistableTraceEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "bar", "key_tick", "signal", "order_plan", "order_result":
		return true
	default:
		return false
	}
}

func (m *Manager) persistSignalResultPoint(inst StrategyInstance, symbol string, mode string, eventTime time.Time, trace StrategyTraceRecord) {
	if m == nil || m.store == nil {
		return
	}
	switch strings.TrimSpace(trace.StepKey) {
	case "SIGNAL_RESULT", "TAKE_PROFIT", "STOP_LOSS":
	default:
		return
	}
	result := signalResultKind(trace)
	if result == "" {
		return
	}
	metrics := map[string]any{}
	for k, v := range trace.Metrics {
		metrics[k] = v
	}
	metrics["signal_point_type"] = "exit"
	metrics["signal_result"] = result
	if trace.TraceID > 0 {
		metrics["source_trace_id"] = trace.TraceID
	}
	sig := SignalRecord{
		InstanceID:     inst.InstanceID,
		StrategyID:     inst.StrategyID,
		Symbol:         symbol,
		Timeframe:      inst.Timeframe,
		Mode:           mode,
		EventTime:      firstTime(trace.EventTime, eventTime),
		TargetPosition: 0,
		Confidence:     1,
		Reason:         firstNonEmpty(trace.Reason, trace.StepLabel, result),
		Metrics:        metrics,
		CreatedAt:      time.Now(),
	}
	id, err := m.store.AppendSignal(sig)
	if err != nil {
		m.setError(err)
		return
	}
	sig.ID = id
	m.broadcast("strategy_signal", sig)
}

func signalResultKind(trace StrategyTraceRecord) string {
	if trace.Metrics != nil {
		if raw := strings.ToLower(strings.TrimSpace(fmt.Sprint(trace.Metrics["signal_result"]))); raw == "success" || raw == "failure" || raw == "unresolved" {
			return raw
		}
	}
	switch strings.ToLower(strings.TrimSpace(trace.Status)) {
	case "passed":
		return "success"
	case "failed":
		return "failure"
	case "done":
		return "unresolved"
	default:
		return ""
	}
}

func firstTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now()
}

func (m *Manager) persistDecision(inst StrategyInstance, symbol string, mode string, replayTaskID string, eventTime time.Time, decision SignalDecision) {
	sig := SignalRecord{
		InstanceID:     inst.InstanceID,
		StrategyID:     inst.StrategyID,
		Symbol:         symbol,
		Timeframe:      inst.Timeframe,
		Mode:           mode,
		EventTime:      eventTime,
		TargetPosition: decision.TargetPosition,
		Confidence:     decision.Confidence,
		Reason:         decision.Reason,
		Metrics:        decision.Metrics,
		CreatedAt:      time.Now(),
	}
	id, err := m.store.AppendSignal(sig)
	if err != nil {
		m.setError(err)
		return
	}
	sig.ID = id
	signalStepIndex := 5
	signalStepTotal := 5
	if decision.Trace != nil {
		if decision.Trace.StepIndex > 0 {
			signalStepIndex = decision.Trace.StepIndex
		}
		if decision.Trace.StepTotal > 0 {
			signalStepTotal = decision.Trace.StepTotal
		}
	}
	m.persistTrace(inst, symbol, mode, eventTime, StrategyTraceRecord{
		EventType: "signal",
		StepKey:   "signal",
		StepLabel: "发出策略信号",
		StepIndex: signalStepIndex,
		StepTotal: signalStepTotal,
		Status:    "passed",
		Reason:    decision.Reason,
		Metrics:   decision.Metrics,
		SignalPreview: map[string]any{
			"target_position": decision.TargetPosition,
			"confidence":      decision.Confidence,
			"signal_id":       id,
		},
	})
	plan := m.exec.Plan(inst, symbol, decision.TargetPosition, mode)
	m.persistTrace(inst, symbol, mode, eventTime, StrategyTraceRecord{
		EventType: "order_plan",
		StepKey:   "order_plan",
		StepLabel: "生成订单计划",
		StepIndex: 5,
		StepTotal: 5,
		Status:    plan.RiskStatus,
		Reason:    plan.RiskReason,
		Metrics: map[string]any{
			"current_position": plan.CurrentPosition,
			"target_position":  plan.TargetPosition,
			"planned_delta":    plan.PlannedDelta,
			"order_status":     plan.OrderStatus,
		},
	})
	m.exec.Apply(symbol, plan)
	audit := OrderAuditRecord{
		InstanceID:      inst.InstanceID,
		StrategyID:      inst.StrategyID,
		Symbol:          symbol,
		Mode:            mode,
		EventTime:       eventTime,
		TargetPosition:  decision.TargetPosition,
		CurrentPosition: plan.CurrentPosition,
		PlannedDelta:    plan.PlannedDelta,
		RiskStatus:      plan.RiskStatus,
		RiskReason:      plan.RiskReason,
		OrderStatus:     plan.OrderStatus,
		Audit: map[string]any{
			"reason":     decision.Reason,
			"confidence": decision.Confidence,
			"metrics":    decision.Metrics,
		},
		CreatedAt: time.Now(),
	}
	auditID, _ := m.store.AppendOrderAudit(audit)
	audit.ID = auditID
	m.persistTrace(inst, symbol, mode, eventTime, StrategyTraceRecord{
		EventType: "order_result",
		StepKey:   "order_result",
		StepLabel: "订单执行结果",
		StepIndex: 5,
		StepTotal: 5,
		Status:    plan.OrderStatus,
		Reason:    plan.RiskReason,
		Metrics: map[string]any{
			"risk_status":      plan.RiskStatus,
			"risk_reason":      plan.RiskReason,
			"order_status":     plan.OrderStatus,
			"current_position": plan.CurrentPosition,
			"target_position":  plan.TargetPosition,
			"planned_delta":    plan.PlannedDelta,
		},
	})
	now := time.Now()
	inst.LastSignalAt = &now
	inst.LastTargetPosition = decision.TargetPosition
	inst.LastError = ""
	_ = m.store.SaveInstance(inst)
	m.mu.Lock()
	if m.instances == nil {
		m.instances = make(map[string]StrategyInstance)
	}
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
	m.broadcast("strategy_signal", sig)
	m.broadcast("order_audit_update", audit)
	m.appendReplayReport(replayTaskID, inst, sig, audit)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (m *Manager) Subscribe() (<-chan EventEnvelope, func()) {
	cap := m.queueCap
	if cap <= 0 {
		cap = queuewatch.DefaultConfig("").StrategyEventCapacity
	}
	ch := make(chan EventEnvelope, cap)
	m.mu.Lock()
	m.events[ch] = struct{}{}
	m.mu.Unlock()
	if m.queueHandle != nil {
		m.queueHandle.ObserveDepth(m.maxEventDepth())
	}
	return ch, func() {
		m.mu.Lock()
		delete(m.events, ch)
		close(ch)
		m.mu.Unlock()
		if m.queueHandle != nil {
			m.queueHandle.ObserveDepth(m.maxEventDepth())
		}
	}
}

func strategyBarEventTime(ev BarEvent) time.Time {
	return ev.AdjustedTime
}

func (m *Manager) broadcast(typ string, data any) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for ch := range m.events {
		select {
		case ch <- EventEnvelope{Type: typ, Data: data}:
			if m.queueHandle != nil {
				m.queueHandle.MarkEnqueued(m.maxEventDepth())
			}
		default:
			if m.queueHandle != nil {
				m.queueHandle.MarkDropped(m.maxEventDepth())
			}
		}
	}
	if m.queueHandle != nil {
		m.queueHandle.ObserveDepth(m.maxEventDepth())
	}
}

func (m *Manager) maxEventDepth() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	maxDepth := 0
	for ch := range m.events {
		if depth := len(ch); depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func (m *Manager) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastError = err.Error()
	}
}

func (m *Manager) setInstanceError(instanceID string, err error) {
	if err == nil {
		return
	}
	inst, loadErr := m.store.GetInstance(instanceID)
	if loadErr != nil {
		m.setError(err)
		return
	}
	inst.Status = InstanceStatusError
	inst.LastError = err.Error()
	_ = m.store.SaveInstance(inst)
	m.mu.Lock()
	m.instances[inst.InstanceID] = inst
	m.lastError = err.Error()
	m.mu.Unlock()
	m.broadcast("strategy_status_update", m.Status())
}

func containsFold(items []string, target string) bool {
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}

func runtimeStrategyInstance(inst StrategyInstance) StrategyInstance {
	return inst
}

type runtimeStartPlanner interface {
	LoadStrategy(context.Context, LoadStrategyRequest) error
	GetStartRequirements(context.Context, StartRequirementsRequest) (StartRequirementsResponse, error)
}

type runtimeStartClient interface {
	runtimeStartPlanner
	StartInstance(context.Context, StartInstanceRequest) error
}

type RuntimeStartPlan struct {
	Instance     StrategyInstance
	Requirements StartRequirementsResponse
}

func (m *Manager) buildRuntimeStartPlan(ctx context.Context, client runtimeStartPlanner, inst StrategyInstance) (RuntimeStartPlan, error) {
	if client == nil {
		return RuntimeStartPlan{}, fmt.Errorf("strategy http client not connected")
	}
	if err := client.LoadStrategy(ctx, LoadStrategyRequest{StrategyID: inst.StrategyID}); err != nil {
		logger.Error("LoadStrategy err: ", err)
		return RuntimeStartPlan{}, err
	}
	requirements, err := client.GetStartRequirements(ctx, StartRequirementsRequest{Instance: runtimeStrategyInstance(inst)})
	if err != nil {
		logger.Error("GetStartRequirements err: ", err)
		return RuntimeStartPlan{}, err
	}
	runtimeInst, err := m.prepareRuntimeStartInstance(inst, requirements)
	if err != nil {
		return RuntimeStartPlan{}, err
	}
	return RuntimeStartPlan{Instance: runtimeInst, Requirements: requirements}, nil
}

func (m *Manager) startRuntimeInstance(ctx context.Context, client runtimeStartClient, inst StrategyInstance) (RuntimeStartPlan, error) {
	plan, err := m.buildRuntimeStartPlan(ctx, client, inst)
	if err != nil {
		return RuntimeStartPlan{}, err
	}
	logger.Info("strategy StartInstance call",
		"instance_id", plan.Instance.InstanceID,
		"strategy_id", plan.Instance.StrategyID,
		"mode", plan.Instance.Mode,
		"symbols", plan.Instance.Symbols,
		"timeframe", plan.Instance.Timeframe,
		"warmup_target", plan.Requirements.WarmupTarget,
		"requires_anchor_time", plan.Requirements.RequiresAnchorTime,
		// "params", plan.Instance.Params,
	)
	if err := client.StartInstance(ctx, StartInstanceRequest{Instance: plan.Instance}); err != nil {
		return RuntimeStartPlan{}, err
	}
	return plan, nil
}

func (m *Manager) prepareRuntimeStartInstance(inst StrategyInstance, requirements StartRequirementsResponse) (StrategyInstance, error) {
	out := runtimeStrategyInstance(inst)
	out.Params = cloneStrategyParams(inst.Params)
	warmupTarget := requirements.WarmupTarget
	if warmupTarget <= 0 {
		return out, nil
	}
	existing := warmupBarsFromParams(out.Params)
	if len(existing) >= warmupTarget {
		out.Params["warmup_count"] = len(existing)
		out.Params["warmup_target"] = warmupTarget
		return out, nil
	}
	anchorTime, ok := parseInstanceAnchorTime(out.Params)
	if !ok {
		return out, fmt.Errorf("strategy instance start_time/chart_start_time/chart_anchor is required: instance_id=%s strategy_id=%s timeframe=%s warmup_target=%d requires_anchor_time=%t", out.InstanceID, out.StrategyID, out.Timeframe, warmupTarget, requirements.RequiresAnchorTime)
	}
	warmupBars, err := m.fetchWarmupBars(out, anchorTime, warmupTarget)
	if err != nil {
		return out, err
	}
	out.Params["warmup_bars"] = warmupBars
	out.Params["warmup_count"] = len(warmupBars)
	out.Params["warmup_target"] = warmupTarget
	out.Params["warmup_anchor_time"] = anchorTime.Format("2006-01-02 15:04:05")
	return out, nil
}

func warmupBarsFromParams(params map[string]any) []klinequery.KlineBar {
	if len(params) == 0 {
		return nil
	}
	raw, ok := params["warmup_bars"]
	if !ok || raw == nil {
		return nil
	}
	var out []klinequery.KlineBar
	buf, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	if err := json.Unmarshal(buf, &out); err != nil {
		return nil
	}
	return out
}

func inferWarmupScope(inst StrategyInstance) (symbol string, kind string, variety string) {
	symbol = firstSymbol(inst.Symbols)
	params := inst.Params
	if anchor, ok := params["chart_anchor"].(map[string]any); ok {
		if v := strings.TrimSpace(fmt.Sprint(anchor["symbol"])); v != "" {
			symbol = strings.ToLower(v)
		}
		kind = strings.ToLower(strings.TrimSpace(fmt.Sprint(anchor["type"])))
		variety = strings.ToLower(strings.TrimSpace(fmt.Sprint(anchor["variety"])))
	}
	symbol = strings.ToLower(strings.TrimSpace(symbol))
	if kind == "" {
		if symbol == "l9" || strings.HasSuffix(symbol, "l9") {
			kind = "l9"
		} else {
			kind = "contract"
		}
	}
	if variety == "" {
		if kind == "l9" {
			if symbol != "l9" && strings.HasSuffix(symbol, "l9") {
				variety = strings.TrimSuffix(symbol, "l9")
			}
		} else {
			for i := 0; i < len(symbol); i++ {
				c := symbol[i]
				if c < 'a' || c > 'z' {
					variety = symbol[:i]
					break
				}
			}
			if variety == "" {
				variety = symbol
			}
		}
	}
	return symbol, kind, variety
}

func (m *Manager) fetchWarmupBars(inst StrategyInstance, anchorTime time.Time, target int) ([]klinequery.KlineBar, error) {
	if target <= 0 {
		return nil, nil
	}
	m.mu.RLock()
	realtimeDSN := strings.TrimSpace(m.marketRealtimeDSN)
	replayDSN := strings.TrimSpace(m.marketReplayDSN)
	sharedMetaDSN := strings.TrimSpace(m.sharedMetaDSN)
	m.mu.RUnlock()
	mode := strings.ToLower(strings.TrimSpace(inst.Mode))
	sources := warmupQuerySources(inst, realtimeDSN, replayDSN)
	if len(sources) == 0 {
		return nil, fmt.Errorf("strategy warmup market DSN is not configured for mode=%s", mode)
	}
	symbol, kind, variety := inferWarmupScope(inst)
	if symbol == "" {
		return nil, fmt.Errorf("strategy warmup symbol is empty for instance_id=%s", inst.InstanceID)
	}
	timeframe := strings.TrimSpace(inst.Timeframe)
	if timeframe == "" {
		timeframe = "1m"
	}
	var lastErr error
	for _, source := range sources {
		querySvc := klinequery.NewServiceWithSessionDB(source.dsn, sharedMetaDSN, searchindex.NewManager(source.dsn, 0))
		resp, err := querySvc.BarsByEnd(symbol, kind, variety, timeframe, anchorTime, target)
		if err != nil {
			lastErr = err
			logger.Warn("strategy warmup source failed",
				"instance_id", inst.InstanceID,
				"strategy_id", inst.StrategyID,
				"mode", mode,
				"source", source.name,
				"symbol", symbol,
				"type", kind,
				"variety", variety,
				"timeframe", timeframe,
				"anchor_time", anchorTime.Format("2006-01-02 15:04:05"),
				"warmup_target", target,
				"error", err,
			)
			continue
		}
		if len(resp.Bars) < target {
			lastErr = fmt.Errorf("warmup bars below target: source=%s warmup_count=%d warmup_target=%d", source.name, len(resp.Bars), target)
			logger.Warn("strategy warmup bars below target",
				"instance_id", inst.InstanceID,
				"strategy_id", inst.StrategyID,
				"mode", mode,
				"source", source.name,
				"symbol", symbol,
				"type", kind,
				"variety", variety,
				"timeframe", timeframe,
				"anchor_time", anchorTime.Format("2006-01-02 15:04:05"),
				"warmup_target", target,
				"warmup_count", len(resp.Bars),
			)
			continue
		}
		logger.Info("strategy warmup source selected",
			"instance_id", inst.InstanceID,
			"strategy_id", inst.StrategyID,
			"mode", mode,
			"source", source.name,
			"symbol", symbol,
			"timeframe", timeframe,
			"warmup_count", len(resp.Bars),
		)
		return resp.Bars, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("fetch warmup bars failed after trying %s: %w", warmupSourceNames(sources), lastErr)
	}
	return nil, fmt.Errorf("fetch warmup bars failed: no available market data source")
}

type warmupQuerySource struct {
	name string
	dsn  string
}

func warmupQuerySources(inst StrategyInstance, realtimeDSN string, replayDSN string) []warmupQuerySource {
	mode := strings.ToLower(strings.TrimSpace(inst.Mode))
	realtimeDSN = strings.TrimSpace(realtimeDSN)
	replayDSN = strings.TrimSpace(replayDSN)
	sources := make([]warmupQuerySource, 0, 2)
	appendSource := func(name string, dsn string) {
		dsn = strings.TrimSpace(dsn)
		if dsn == "" {
			return
		}
		for _, item := range sources {
			if item.dsn == dsn {
				return
			}
		}
		sources = append(sources, warmupQuerySource{name: name, dsn: dsn})
	}
	params := inst.Params
	if params == nil {
		params = map[string]any{}
	}
	switch strings.ToLower(strings.TrimSpace(fmt.Sprint(params["warmup_source"]))) {
	case "realtime", "live":
		appendSource("realtime", realtimeDSN)
		return sources
	case "replay":
		appendSource("replay", replayDSN)
		return sources
	}
	if mode == RunTypeReplay && strings.ToLower(strings.TrimSpace(fmt.Sprint(params["replay_mode"]))) == "kline" {
		appendSource("realtime", realtimeDSN)
		return sources
	}
	if mode == RunTypeReplay {
		appendSource("replay", replayDSN)
		appendSource("realtime", realtimeDSN)
		return sources
	}
	appendSource("realtime", realtimeDSN)
	appendSource("replay", replayDSN)
	return sources
}

func warmupSourceNames(items []warmupQuerySource) string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.name) == "" {
			continue
		}
		names = append(names, item.name)
	}
	if len(names) == 0 {
		return "no source"
	}
	return strings.Join(names, " -> ")
}

func cloneStrategyParams(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (m *Manager) persistInstanceStartTrace(inst StrategyInstance) {
	trace, ok := initialTraceForInstance(inst)
	if !ok {
		return
	}
	m.persistTrace(inst, firstSymbol(inst.Symbols), inst.Mode, trace.EventTime, trace)
}

func (m *Manager) persistInstanceRestoreTrace(inst StrategyInstance, reason string) {
	trace, ok := initialTraceForInstance(inst)
	if !ok {
		return
	}
	trace.EventTime = time.Now()
	trace.Reason = "python runtime restored after http reconnect"
	trace.Metrics["restore_reason"] = strings.TrimSpace(reason)
	trace.Metrics["restored_at"] = trace.EventTime.Format(time.RFC3339Nano)
	m.persistTrace(inst, firstSymbol(inst.Symbols), inst.Mode, trace.EventTime, trace)
}

func initialTraceForInstance(inst StrategyInstance) (StrategyTraceRecord, bool) {
	params := inst.Params
	if params == nil {
		params = map[string]any{}
	}
	stepKey := "WAIT_BREAK_BELOW_MA20"
	stepLabel := "等待跌破 MA20"
	stepIndex := 1
	stepTotal := 5
	switch strings.ToLower(strings.TrimSpace(inst.StrategyID)) {
	case MA20WeakBaselineStrategyID:
		stepTotal = 5
	case MA20WeakStrategyID, MA20WeakHardFilterStrategyID, MA20WeakScoreFilterStrategyID:
		stepTotal = 6
	}
	eventTime := time.Now()
	if ts, ok := parseInstanceAnchorTime(params); ok {
		eventTime = ts
	}
	return StrategyTraceRecord{
		InstanceID: inst.InstanceID,
		StrategyID: inst.StrategyID,
		Symbol:     firstSymbol(inst.Symbols),
		Timeframe:  inst.Timeframe,
		Mode:       inst.Mode,
		EventType:  "bar",
		EventTime:  eventTime,
		StepKey:    stepKey,
		StepLabel:  stepLabel,
		StepIndex:  stepIndex,
		StepTotal:  stepTotal,
		Status:     "waiting",
		Reason:     "instance started with warmup bars, waiting for break below MA20",
		Metrics: map[string]any{
			"state":        stepKey,
			"step_total":   stepTotal,
			"chart_anchor": params["chart_anchor"],
			"chart_start":  params["chart_start_time"],
		},
		CreatedAt: time.Now(),
	}, true
}

func parseInstanceAnchorTime(params map[string]any) (time.Time, bool) {
	values := []string{}
	if anchor, ok := params["chart_anchor"].(map[string]any); ok {
		values = append(values, fmt.Sprint(anchor["adjusted_time"]), fmt.Sprint(anchor["plot_time"]))
	}
	if raw, ok := params["chart_start_time"]; ok {
		values = append(values, fmt.Sprint(raw))
	}
	if raw, ok := params["start_time"]; ok {
		values = append(values, fmt.Sprint(raw))
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02 15:04"}
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || value == "<nil>" {
			continue
		}
		if ts, ok := parseUnixSecondsText(value); ok {
			return ts, true
		}
		for _, layout := range layouts {
			if ts, err := time.ParseInLocation(layout, strings.ReplaceAll(value, "T", " "), time.Local); err == nil {
				return ts, true
			}
			if ts, err := time.Parse(layout, value); err == nil {
				return ts, true
			}
		}
	}
	return time.Time{}, false
}

func parseUnixSecondsText(value string) (time.Time, bool) {
	if strings.ContainsAny(value, "-:TtZz ") {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, false
	}
	whole := int64(seconds)
	nanos := int64((seconds - float64(whole)) * 1e9)
	return time.Unix(whole, nanos).In(time.Local), true
}

func firstSymbol(symbols []string) string {
	if len(symbols) == 0 {
		return ""
	}
	return strings.TrimSpace(symbols[0])
}
