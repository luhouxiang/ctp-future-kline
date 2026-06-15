package strategy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecutionEngineAppliesRealtimePaperTarget(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	plan := engine.Plan(StrategyInstance{}, "rb2601", -1, RunTypeRealtime)

	if plan.RiskStatus != RiskStatusAllowed || plan.OrderStatus != OrderStatusSimulated {
		t.Fatalf("Plan() = %+v, want allowed simulated", plan)
	}
	engine.Apply("rb2601", plan)
	if got := engine.CurrentPosition("rb2601"); got != -1 {
		t.Fatalf("CurrentPosition() = %v, want -1", got)
	}
}

func TestExecutionEngineBlocksReplayTarget(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	plan := engine.Plan(StrategyInstance{}, "rb2601", -1, RunTypeReplay)

	if plan.RiskStatus != RiskStatusBlocked || plan.OrderStatus != OrderStatusBlocked {
		t.Fatalf("Plan() = %+v, want replay blocked", plan)
	}
	engine.Apply("rb2601", plan)
	if got := engine.CurrentPosition("rb2601"); got != 0 {
		t.Fatalf("CurrentPosition() = %v, want unchanged 0", got)
	}
}

func TestExecutionEnginePauseBlocksRealtimeTarget(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	engine.SetPaused(true)
	plan := engine.Plan(StrategyInstance{}, "rb2601", -1, RunTypeRealtime)

	if plan.RiskStatus != RiskStatusBlocked || plan.OrderStatus != OrderStatusPaused {
		t.Fatalf("Plan() = %+v, want paused blocked", plan)
	}
	engine.Apply("rb2601", plan)
	if got := engine.CurrentPosition("rb2601"); got != 0 {
		t.Fatalf("CurrentPosition() = %v, want unchanged 0", got)
	}
	if !engine.Status().AutoExecutionPaused {
		t.Fatalf("Status().AutoExecutionPaused = false, want true")
	}
}

func TestExecutionEngineResumeAllowsRealtimeTarget(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	engine.SetPaused(true)
	engine.SetPaused(false)
	plan := engine.Plan(StrategyInstance{}, "rb2601", -1, RunTypeRealtime)

	if plan.RiskStatus != RiskStatusAllowed || plan.OrderStatus != OrderStatusSimulated {
		t.Fatalf("Plan() = %+v, want allowed simulated", plan)
	}
	engine.Apply("rb2601", plan)
	if got := engine.CurrentPosition("rb2601"); got != -1 {
		t.Fatalf("CurrentPosition() = %v, want -1", got)
	}
	if engine.Status().AutoExecutionPaused {
		t.Fatalf("Status().AutoExecutionPaused = true, want false")
	}
}

func TestExecutionEnginePlanWithExternalCurrentPosition(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	engine.Apply("rb2601", ExecutionPlan{
		CurrentPosition: 0,
		TargetPosition:  -1,
		PlannedDelta:    -1,
		RiskStatus:      RiskStatusAllowed,
		OrderStatus:     OrderStatusSimulated,
	})

	plan := engine.PlanWithCurrent(StrategyInstance{}, 0, -1, RunTypeRealtime)
	if plan.CurrentPosition != 0 || plan.PlannedDelta != -1 {
		t.Fatalf("PlanWithCurrent() = %+v, want current 0 delta -1", plan)
	}
}

func TestSubmitExternalOrderFailureBlocksPlan(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	m := &Manager{
		exec:          engine,
		orderExecutor: failingStrategyOrderExecutor{},
	}
	plan := engine.Plan(StrategyInstance{}, "rb2601", -1, RunTypeRealtime)

	result := m.submitExternalOrderIfNeeded(StrategyInstance{InstanceID: "inst-1"}, "rb2601", RunTypeRealtime, time.Now(), SignalDecision{
		TargetPosition: -1,
		Reason:         "test signal",
	}, &plan)

	if plan.RiskStatus != RiskStatusBlocked || plan.OrderStatus != OrderStatusBlocked {
		t.Fatalf("plan after submit failure = %+v, want blocked", plan)
	}
	if result == nil || result["error"] == "" {
		t.Fatalf("external result = %+v, want error detail", result)
	}
	engine.Apply("rb2601", plan)
	if got := engine.CurrentPosition("rb2601"); got != 0 {
		t.Fatalf("CurrentPosition() = %v, want unchanged 0", got)
	}
}

type failingStrategyOrderExecutor struct{}

func (f failingStrategyOrderExecutor) CurrentPosition(string) (float64, error) {
	return 0, nil
}

func (f failingStrategyOrderExecutor) SubmitStrategyOrder(context.Context, StrategyOrderRequest) (StrategyOrderResult, error) {
	return StrategyOrderResult{Status: OrderStatusBlocked, Reason: "boom"}, errors.New("boom")
}
