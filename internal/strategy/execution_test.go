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

func TestExecutionEngineNetsMultiplePrimaryInstanceSubPositions(t *testing.T) {
	t.Parallel()

	engine := NewExecutionEngine()
	instA := StrategyInstance{InstanceID: "primary-a", AccountID: "paper", Timeframe: "1m"}
	instB := StrategyInstance{InstanceID: "primary-b", AccountID: "paper", Timeframe: "5m"}

	openA := engine.PlanInstanceTarget(instA, "rb2601", -1, RunTypeRealtime, engine.CurrentPosition("rb2601"))
	if openA.Plan.TargetPosition != -1 || openA.Plan.PlannedDelta != -1 {
		t.Fatalf("openA = %+v, want net target -1 delta -1", openA)
	}
	engine.ApplyInstanceTarget(instA, "rb2601", -1, RunTypeRealtime, openA.Plan)

	openB := engine.PlanInstanceTarget(instB, "rb2601", -1, RunTypeRealtime, engine.CurrentPosition("rb2601"))
	if openB.Plan.TargetPosition != -2 || openB.Plan.PlannedDelta != -1 {
		t.Fatalf("openB = %+v, want net target -2 delta -1", openB)
	}
	engine.ApplyInstanceTarget(instB, "rb2601", -1, RunTypeRealtime, openB.Plan)
	if got := engine.CurrentPosition("rb2601"); got != -2 {
		t.Fatalf("CurrentPosition after two opens = %v, want -2", got)
	}

	closeA := engine.PlanInstanceTarget(instA, "rb2601", 0, RunTypeRealtime, engine.CurrentPosition("rb2601"))
	if closeA.Plan.TargetPosition != -1 || closeA.Plan.PlannedDelta != 1 {
		t.Fatalf("closeA = %+v, want net target -1 delta +1", closeA)
	}
	engine.ApplyInstanceTarget(instA, "rb2601", 0, RunTypeRealtime, closeA.Plan)
	if got := engine.CurrentPosition("rb2601"); got != -1 {
		t.Fatalf("CurrentPosition after A close = %v, want -1", got)
	}

	closeB := engine.PlanInstanceTarget(instB, "rb2601", 0, RunTypeRealtime, engine.CurrentPosition("rb2601"))
	if closeB.Plan.TargetPosition != 0 || closeB.Plan.PlannedDelta != 1 {
		t.Fatalf("closeB = %+v, want net target 0 delta +1", closeB)
	}
	engine.ApplyInstanceTarget(instB, "rb2601", 0, RunTypeRealtime, closeB.Plan)
	if got := engine.CurrentPosition("rb2601"); got != 0 {
		t.Fatalf("CurrentPosition after all close = %v, want 0", got)
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
