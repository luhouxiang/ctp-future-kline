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
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{positions: make(map[string]float64)}
}

func (e *ExecutionEngine) CurrentPosition(symbol string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.positions[symbol]
}

func (e *ExecutionEngine) Plan(instance StrategyInstance, symbol string, target float64, mode string) ExecutionPlan {
	current := e.CurrentPosition(symbol)
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
		Mode:        "simulated",
		Positions:   positions,
		LastAuditAt: e.lastAuditAt,
		UpdatedAt:   time.Now(),
	}
}
