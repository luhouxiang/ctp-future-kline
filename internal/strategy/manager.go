// manager.go 是策略子系统的主控入口。
// 它负责启动或连接 Python 策略进程，维护 gRPC 连接与健康检查，并对外提供策略定义、实例和信号管理能力。
package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

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
			BasisText:   "????/??????? Web???????????????",
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
		if err := m.connect(); err != nil {
			if err := m.startProcessLocked(); err != nil {
				m.setError(err)
				return err
			}
			if err := m.connectWithRetry(5 * time.Second); err != nil {
				m.setError(err)
				return err
			}
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
	cmd := exec.Command("python", entry, "--addr", m.cfg.GRPCAddr)
	cmd.Dir = workdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start python strategy service failed: %w", err)
	}
	m.cmd = cmd
	go func() {
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
		ProcessRunning:   m.cmd != nil,
		Connected:        m.connReady,
		GRPCAddr:         m.cfg.GRPCAddr,
		PythonEntry:      m.cfg.PythonEntry,
		LastError:        m.lastError,
		LastHealthAt:     m.lastHealth,
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
	if err := m.syncDefinitions(); err != nil {
		logger.Warn("strategy definition sync skipped", "error", err)
	}
	return m.store.ListDefinitions()
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
	if err := m.store.SaveInstance(inst); err != nil {
		return err
	}
	m.mu.Lock()
	m.instances[inst.InstanceID] = inst
	m.mu.Unlock()
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	if err := client.LoadStrategy(ctx, LoadStrategyRequest{StrategyID: inst.StrategyID}); err != nil {
		return err
	}
	return client.StartInstance(ctx, StartInstanceRequest{Instance: inst})
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
		Instance:        inst,
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
		decision, err = client.OnReplayBar(ctx, req)
	default:
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
	m.persistTrace(inst, symbol, mode, eventTime, StrategyTraceRecord{
		EventType: "signal",
		StepKey:   "signal",
		StepLabel: "发出策略信号",
		StepIndex: 5,
		StepTotal: 5,
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
