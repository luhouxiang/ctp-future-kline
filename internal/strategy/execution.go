package strategy

import (
	"math"
	"sync"
	"time"
)

type ExecutionEngine struct {
	mu          sync.Mutex
	positions   map[string]float64
	lastAuditAt *time.Time
	paused      bool
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{positions: make(map[string]float64)}
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
