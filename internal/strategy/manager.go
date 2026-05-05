// manager.go 是策略子系统的主控入口。
// 它负责启动或连接 Python 策略进程，维护 gRPC 连接与健康检查，并对外提供策略定义、实例和信号管理能力。
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
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

type strategyServiceProcessInfo struct {
	ProcessID    int    `json:"ProcessId"`
	CreationDate string `json:"CreationDate"`
	CommandLine  string `json:"CommandLine"`
}

type Manager struct {
	cfg   config.StrategyConfig
	store *Store
	exec  *ExecutionEngine

	mu          sync.RWMutex
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

	backtestMarketDSN string
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
		events:    make(map[chan EventEnvelope]struct{}),
		instances: make(map[string]StrategyInstance),
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
	if err := m.ensureLocalDefinitions(); err != nil {
		logger.Warn("seed local strategy definitions failed", "error", err)
	}
	SetDefaultSink(m)
	return m, nil
}

func (m *Manager) SetBacktestMarketDSN(dsn string) {
	m.mu.Lock()
	m.backtestMarketDSN = strings.TrimSpace(dsn)
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
		m.setError(err)
		return err
	}
	go m.healthLoop()
	if err := m.syncDefinitions(); err != nil {
		logger.Warn("strategy definition sync failed", "error", err)
	}
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
		if _, err := m.ping(); err != nil {
			m.setError(err)
			m.mu.Lock()
			m.connReady = false
			m.mu.Unlock()
			_ = m.connect()
			continue
		}
	}
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
	cmd := exec.Command("python", entry, "--addr", m.cfg.GRPCAddr, "--log-file", logPath)
	cmd.Dir = workdir
	cmd.Stdout = io.MultiWriter(os.Stdout, logFile)
	cmd.Stderr = io.MultiWriter(os.Stderr, logFile)
	logger.Info("starting python strategy service",
		"entry", entry,
		"workdir", workdir,
		"grpc_addr", m.cfg.GRPCAddr,
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

func (m *Manager) restartServiceLocked(explicit bool) error {
	if err := m.stopManagedProcess(); err != nil {
		return err
	}
	if explicit {
		logger.Info("restarting python strategy service", "grpc_addr", m.cfg.GRPCAddr)
	}
	if err := m.startProcessLocked(); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	m.mu.RLock()
	processRunning := m.cmd != nil && m.cmd.Process != nil
	m.mu.RUnlock()
	if !processRunning {
		return fmt.Errorf("python strategy service exited immediately; check grpc_addr=%s and strategy log", m.cfg.GRPCAddr)
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
	if info, ok, err := findExistingPythonStrategyService(entry, m.cfg.GRPCAddr); err != nil {
		logger.Warn("probe existing python strategy service failed", "error", err, "grpc_addr", m.cfg.GRPCAddr)
	} else if ok {
		fields := []any{"pid", info.ProcessID, "grpc_addr", m.cfg.GRPCAddr}
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
		return fmt.Errorf("python strategy service exited immediately; check grpc_addr=%s and strategy log", m.cfg.GRPCAddr)
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
	if err := stopExistingPythonStrategyService(entry, m.cfg.GRPCAddr); err != nil {
		return err
	}
	return nil
}

func findExistingPythonStrategyService(entry string, grpcAddr string) (strategyServiceProcessInfo, bool, error) {
	scriptName := strings.ToLower(strings.TrimSpace(filepath.Base(entry)))
	if scriptName == "" {
		scriptName = "strategy_service.py"
	}
	addrText := strings.TrimSpace(grpcAddr)
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

func stopExistingPythonStrategyService(entry string, grpcAddr string) error {
	scriptName := strings.ToLower(strings.TrimSpace(filepath.Base(entry)))
	if scriptName == "" {
		scriptName = "strategy_service.py"
	}
	addrText := strings.TrimSpace(grpcAddr)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	conn, err := DialStrategyService(ctx, m.cfg.GRPCAddr)
	if err != nil {
		return err
	}
	client := NewStrategyServiceClient(conn)
	if _, err := client.Ping(ctx); err != nil {
		_ = conn.Close()
		return err
	}
	m.mu.Lock()
	if m.connClose != nil {
		_ = m.connClose()
	}
	m.client = client
	m.connReady = true
	m.connClose = conn.Close
	m.lastError = ""
	m.lastHealth = time.Now()
	m.mu.Unlock()
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) ping() (HealthResponse, error) {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return HealthResponse{}, fmt.Errorf("strategy grpc client not connected")
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

func (m *Manager) syncDefinitions() error {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return fmt.Errorf("strategy grpc client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	resp, err := client.ListStrategies(ctx)
	if err != nil {
		return err
	}
	for _, item := range resp.Strategies {
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now()
		}
		if err := m.store.UpsertDefinition(item); err != nil {
			return err
		}
	}
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) Status() ManagerStatus {
	defs, insts, sigs, audits, runs, _ := m.store.Counts()
	m.mu.RLock()
	defer m.mu.RUnlock()
	running := 0
	for _, inst := range m.instances {
		if inst.Status == InstanceStatusRunning {
			running++
		}
	}
	return ManagerStatus{
		Enabled:          m.cfg.IsEnabled(),
		ProcessRunning:   m.cmd != nil || m.connReady,
		Connected:        m.connReady,
		GRPCAddr:         m.cfg.GRPCAddr,
		PythonEntry:      m.cfg.PythonEntry,
		LastError:        m.lastError,
		LastHealthAt:     m.lastHealth,
		LastRestartAt:    m.lastRestart,
		UpdatedAt:        time.Now(),
		Definitions:      defs,
		Instances:        insts,
		RunningCount:     running,
		SignalCount:      sigs,
		AuditCount:       audits,
		BacktestRunCount: runs,
	}
}

func (m *Manager) ListDefinitions() ([]StrategyDefinition, error) {
	if err := m.ensureLocalDefinitions(); err != nil {
		logger.Warn("ensure local strategy definitions failed", "error", err)
	}
	if err := m.syncDefinitions(); err != nil {
		logger.Warn("strategy definition sync skipped", "error", err)
	}
	return m.store.ListDefinitions()
}

func (m *Manager) ensureLocalDefinitions() error {
	for _, item := range localStrategyDefinitions() {
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now()
		}
		if err := m.store.UpsertDefinition(item); err != nil {
			return err
		}
	}
	return nil
}

func localStrategyDefinitions() []StrategyDefinition {
	now := time.Now()
	return []StrategyDefinition{
		{
			StrategyID:  MA20WeakBaselineStrategyID,
			DisplayName: "MA20 Weak Pullback Baseline",
			EntryScript: "python/ma20_weak_pullback_baseline.py",
			Version:     "1.0.0",
			DefaultParams: map[string]any{
				"ma_period":            20,
				"max_wait_bars":        6,
				"observation_bars":     24,
				"profit_atr_multiple":  1.0,
				"adverse_atr_multiple": 0.8,
				"algorithm":            MA20AlgoBaseline,
			},
			UpdatedAt: now,
		},
		{
			StrategyID:  MA20WeakHardFilterStrategyID,
			DisplayName: "MA20 Weak Pullback Hard Filter",
			EntryScript: "python/ma20_weak_pullback_hard_filter.py",
			Version:     "1.0.0",
			DefaultParams: map[string]any{
				"ma20_period":          20,
				"ma60_period":          60,
				"ma120_period":         120,
				"atr_period":           14,
				"observation_bars":     24,
				"profit_atr_multiple":  1.0,
				"adverse_atr_multiple": 0.8,
				"swing_lookback_bars":  20,
				"slope_lookback_bars":  5,
				"structure_wait_bars":  3,
				"touch_wait_bars":      12,
				"trigger_wait_bars":    6,
				"use_score_filter":     false,
				"algorithm":            MA20AlgoHardFilter,
			},
			UpdatedAt: now,
		},
		{
			StrategyID:  MA20WeakScoreFilterStrategyID,
			DisplayName: "MA20 Weak Pullback Score Filter",
			EntryScript: "python/ma20_weak_pullback_score_filter.py",
			Version:     "1.0.0",
			DefaultParams: map[string]any{
				"ma20_period":          20,
				"ma60_period":          60,
				"ma120_period":         120,
				"atr_period":           14,
				"observation_bars":     24,
				"profit_atr_multiple":  1.0,
				"adverse_atr_multiple": 0.8,
				"swing_lookback_bars":  20,
				"slope_lookback_bars":  5,
				"structure_wait_bars":  3,
				"touch_wait_bars":      12,
				"trigger_wait_bars":    6,
				"use_score_filter":     true,
				"algorithm":            MA20AlgoScoreFilter,
			},
			UpdatedAt: now,
		},
	}
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
	return items, nil
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
		return fmt.Errorf("strategy grpc client not connected")
	}
	runtimeInst := runtimeStrategyInstance(inst)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	if err := client.LoadStrategy(ctx, LoadStrategyRequest{StrategyID: runtimeInst.StrategyID}); err != nil {
		return err
	}
	return client.StartInstance(ctx, StartInstanceRequest{Instance: runtimeInst})
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
	m.broadcast("strategy_status_update", m.Status())
	return nil
}

func (m *Manager) ListSignals(limit int) ([]SignalRecord, error) {
	return m.store.ListSignals(limit)
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
		return StrategyRun{}, fmt.Errorf("strategy grpc client not connected")
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
		return StrategyRun{}, fmt.Errorf("strategy grpc client not connected")
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
		m.callDecision(inst, ev.InstrumentID, mode, ev.ReceivedAt, &ev, nil)
	})
}

func (m *Manager) handleBar(ev BarEvent, mode string) {
	logger.Info("strategy bar event received",
		"symbol", ev.InstrumentID,
		"timeframe", ev.Period,
		"mode", mode,
		"event_time", ev.DataTime,
	)
	m.forEachMatchingInstance(ev.InstrumentID, ev.Period, mode, func(inst StrategyInstance) {
		m.callDecision(inst, ev.InstrumentID, mode, ev.DataTime, nil, &ev)
	})
}

func (m *Manager) forEachMatchingInstance(symbol string, timeframe string, mode string, fn func(StrategyInstance)) {
	m.mu.RLock()
	items := make([]StrategyInstance, 0, len(m.instances))
	for _, item := range m.instances {
		items = append(items, item)
	}
	m.mu.RUnlock()
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
		fn(item)
	}
}

func (m *Manager) callDecision(inst StrategyInstance, symbol string, mode string, eventTime time.Time, tick *TickEvent, bar *BarEvent) {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return
	}
	req := DecisionRequest{
		Instance:        runtimeStrategyInstance(inst),
		Symbol:          symbol,
		EventTime:       eventTime.Format(time.RFC3339Nano),
		Mode:            mode,
		CurrentPosition: m.exec.CurrentPosition(symbol),
		Account:         map[string]any{"account_id": inst.AccountID},
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
		logger.Info("strategy OnReplayBar call",
			"instance_id", inst.InstanceID,
			"strategy_id", inst.StrategyID,
			"symbol", symbol,
			"timeframe", inst.Timeframe,
			"event_time", eventTime,
		)
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
		m.persistTrace(inst, symbol, mode, eventTime, *decision.Trace)
	}
	if decision.NoSignal {
		return
	}
	m.persistDecision(inst, symbol, mode, eventTime, decision)
}

func (m *Manager) persistTrace(inst StrategyInstance, symbol string, mode string, eventTime time.Time, trace StrategyTraceRecord) {
	if strings.TrimSpace(trace.EventType) == "" {
		trace.EventType = "bar"
	}
	if trace.EventType != "bar" && trace.EventType != "key_tick" && trace.EventType != "signal" && trace.EventType != "order_plan" && trace.EventType != "order_result" {
		return
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
	id, err := m.store.AppendTrace(trace)
	if err != nil {
		m.setError(err)
		return
	}
	trace.TraceID = id
	m.broadcast("strategy_trace_update", trace)
}

func (m *Manager) persistDecision(inst StrategyInstance, symbol string, mode string, eventTime time.Time, decision SignalDecision) {
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
	_, _ = m.store.AppendOrderAudit(audit)
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
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
	m.broadcast("strategy_signal", sig)
	m.broadcast("order_audit_update", audit)
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
	if raw, ok := params["chart_start_time"]; ok {
		values = append(values, fmt.Sprint(raw))
	}
	if anchor, ok := params["chart_anchor"].(map[string]any); ok {
		values = append(values, fmt.Sprint(anchor["data_time"]), fmt.Sprint(anchor["adjusted_time"]), fmt.Sprint(anchor["plot_time"]))
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02 15:04"}
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || value == "<nil>" {
			continue
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

func firstSymbol(symbols []string) string {
	if len(symbols) == 0 {
		return ""
	}
	return strings.TrimSpace(symbols[0])
}
