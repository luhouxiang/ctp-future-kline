package strategy

import "testing"

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
