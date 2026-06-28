package strategy

import (
	"math"
	"strings"
	"sync"
	"time"
)

type ExecutionEngine struct {
	mu           sync.Mutex
	positions    map[string]float64
	subPositions map[strategySubPositionKey]float64
	lastAuditAt  *time.Time
	paused       bool
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{
		positions:    make(map[string]float64),
		subPositions: make(map[strategySubPositionKey]float64),
	}
}

type strategySubPositionKey struct {
	Mode       string
	AccountID  string
	Symbol     string
	Timeframe  string
	InstanceID string
}

type strategyNetPositionKey struct {
	Mode      string
	AccountID string
	Symbol    string
}

type InstanceExecutionPlan struct {
	Plan                    ExecutionPlan
	InstanceCurrentPosition float64
	InstanceTargetPosition  float64
	NetCurrentTarget        float64
	NetTargetPosition       float64
}

func (e *ExecutionEngine) CurrentPosition(symbol string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.positions[symbol]
}

func (e *ExecutionEngine) SetPaused(paused bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.paused = paused
}

func (e *ExecutionEngine) Paused() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.paused
}

func (e *ExecutionEngine) Plan(instance StrategyInstance, symbol string, target float64, mode string) ExecutionPlan {
	e.mu.Lock()
	current := e.positions[symbol]
	e.mu.Unlock()
	return e.PlanWithCurrent(instance, current, target, mode)
}

func (e *ExecutionEngine) PlanInstanceTarget(instance StrategyInstance, symbol string, target float64, mode string, currentPosition float64) InstanceExecutionPlan {
	currentInstance, currentNet := e.instancePositionSnapshot(instance, symbol, mode)
	netTarget := currentNet - currentInstance + target
	plan := e.PlanWithCurrent(instance, currentPosition, netTarget, mode)
	return InstanceExecutionPlan{
		Plan:                    plan,
		InstanceCurrentPosition: currentInstance,
		InstanceTargetPosition:  target,
		NetCurrentTarget:        currentNet,
		NetTargetPosition:       netTarget,
	}
}

func (e *ExecutionEngine) PlanWithCurrent(instance StrategyInstance, current float64, target float64, mode string) ExecutionPlan {
	e.mu.Lock()
	paused := e.paused
	e.mu.Unlock()
	delta := target - current
	plan := ExecutionPlan{
		CurrentPosition: current,
		TargetPosition:  target,
		PlannedDelta:    delta,
		RiskStatus:      RiskStatusAllowed,
		OrderStatus:     OrderStatusSimulated,
	}
	if mode == RunTypeReplay {
		plan.RiskStatus = RiskStatusBlocked
		plan.RiskReason = "replay mode blocks live execution"
		plan.OrderStatus = OrderStatusBlocked
		return plan
	}
	if paused {
		plan.RiskStatus = RiskStatusBlocked
		plan.RiskReason = "auto execution is paused"
		plan.OrderStatus = OrderStatusPaused
		return plan
	}
	if math.Abs(delta) < 1e-9 {
		plan.OrderStatus = OrderStatusNoop
		return plan
	}
	if math.Abs(target) > 100 {
		plan.RiskStatus = RiskStatusBlocked
		plan.RiskReason = "target position exceeds max limit 100"
		plan.OrderStatus = OrderStatusBlocked
	}
	return plan
}

func (e *ExecutionEngine) Apply(symbol string, plan ExecutionPlan) {
	if plan.RiskStatus != RiskStatusAllowed || plan.OrderStatus != OrderStatusSimulated {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.positions[symbol] = plan.TargetPosition
	now := time.Now()
	e.lastAuditAt = &now
}

func (e *ExecutionEngine) ApplyInstanceTarget(instance StrategyInstance, symbol string, target float64, mode string, plan ExecutionPlan) {
	if plan.RiskStatus != RiskStatusAllowed {
		return
	}
	if plan.OrderStatus != OrderStatusSimulated && plan.OrderStatus != OrderStatusNoop {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	key := subPositionKey(instance, symbol, mode)
	if math.Abs(target) < 1e-9 {
		delete(e.subPositions, key)
	} else {
		e.subPositions[key] = target
	}
	e.positions[symbol] = plan.TargetPosition
	now := time.Now()
	e.lastAuditAt = &now
}

func (e *ExecutionEngine) Status() OrdersStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	positions := make(map[string]float64, len(e.positions))
	for k, v := range e.positions {
		positions[k] = v
	}
	return OrdersStatus{
		Mode:                "simulated",
		AutoExecutionPaused: e.paused,
		Positions:           positions,
		LastAuditAt:         e.lastAuditAt,
		UpdatedAt:           time.Now(),
	}
}

func (e *ExecutionEngine) instancePositionSnapshot(instance StrategyInstance, symbol string, mode string) (float64, float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	subKey := subPositionKey(instance, symbol, mode)
	netKey := netPositionKey(instance, symbol, mode)
	currentInstance := e.subPositions[subKey]
	var currentNet float64
	for key, value := range e.subPositions {
		if subPositionNetKey(key) == netKey {
			currentNet += value
		}
	}
	return currentInstance, currentNet
}

func subPositionKey(instance StrategyInstance, symbol string, mode string) strategySubPositionKey {
	return strategySubPositionKey{
		Mode:       normalizePositionPart(firstNonEmpty(mode, instance.Mode)),
		AccountID:  normalizePositionPart(instance.AccountID),
		Symbol:     normalizePositionPart(symbol),
		Timeframe:  normalizePositionPart(instance.Timeframe),
		InstanceID: strings.TrimSpace(instance.InstanceID),
	}
}

func netPositionKey(instance StrategyInstance, symbol string, mode string) strategyNetPositionKey {
	return subPositionNetKey(subPositionKey(instance, symbol, mode))
}

func subPositionNetKey(key strategySubPositionKey) strategyNetPositionKey {
	return strategyNetPositionKey{
		Mode:      key.Mode,
		AccountID: key.AccountID,
		Symbol:    key.Symbol,
	}
}

func normalizePositionPart(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
