package strategy

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type testService struct{}

func (testService) Ping(context.Context, HealthRequest) (HealthResponse, error) {
	return HealthResponse{OK: true, Version: "test"}, nil
}

func (testService) ListStrategies(context.Context, ListStrategiesRequest) (ListStrategiesResponse, error) {
	return ListStrategiesResponse{Strategies: []StrategyDefinition{{StrategyID: "demo", DisplayName: "Demo"}}}, nil
}

func (testService) LoadStrategy(context.Context, LoadStrategyRequest) (HealthResponse, error) {
	return HealthResponse{OK: true}, nil
}

func (testService) StartInstance(context.Context, StartInstanceRequest) (HealthResponse, error) {
	return HealthResponse{OK: true}, nil
}

func (testService) StopInstance(context.Context, StopInstanceRequest) (HealthResponse, error) {
	return HealthResponse{OK: true}, nil
}

func (testService) OnTick(context.Context, DecisionRequest) (SignalDecision, error) {
	return SignalDecision{
		NoSignal:       true,
		TargetPosition: 0,
		Metrics:        map[string]any{"state": "WAIT_BREAK_BELOW_MA20"},
		Trace: &StrategyTraceRecord{
			EventType: "key_tick",
			StepKey:   "WAIT_BREAK_TOUCH_OPEN",
			StepLabel: "等待跌破触碰K开盘价",
			StepIndex: 4,
			StepTotal: 5,
			Status:    "waiting",
			Checks:    []TraceCheck{{Name: "最新价跌破触碰K开盘价", Passed: false, Current: 100.0, Target: 99.5}},
		},
	}, nil
}

func (testService) OnBar(context.Context, DecisionRequest) (SignalDecision, error) {
	return SignalDecision{
		TargetPosition: -1,
		Metrics:        map[string]any{"signal": "SHORT", "touch_open": 99.5},
		Trace: &StrategyTraceRecord{
			EventType: "bar",
			StepKey:   "BROKEN_BELOW_MA20",
			StepLabel: "已跌破，等待反抽触碰 MA20",
			StepIndex: 3,
			StepTotal: 5,
			Status:    "passed",
		},
	}, nil
}

func (testService) OnReplayBar(context.Context, DecisionRequest) (SignalDecision, error) {
	return SignalDecision{TargetPosition: -1}, nil
}

func (testService) RunBacktest(context.Context, BacktestRequest) (BacktestResponse, error) {
	return BacktestResponse{RunID: "run-1", Status: "done"}, nil
}

func (testService) GetBacktestResult(context.Context, BacktestResultRequest) (BacktestResponse, error) {
	return BacktestResponse{RunID: "run-1", Status: "done"}, nil
}

func (testService) RunParameterSweep(context.Context, ParameterSweepRequest) (ParameterSweepResponse, error) {
	return ParameterSweepResponse{RunID: "opt-1", Status: "done"}, nil
}

func TestJSONStrategyServiceRoundTrip(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	server := grpc.NewServer()
	RegisterStrategyServiceServer(server, testService{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := DialStrategyService(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("DialStrategyService() error = %v", err)
	}
	defer conn.Close()

	client := NewStrategyServiceClient(conn)
	health, err := client.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if !health.OK || health.Version != "test" {
		t.Fatalf("Ping() = %+v, want ok=true version=test", health)
	}
	defs, err := client.ListStrategies(ctx)
	if err != nil {
		t.Fatalf("ListStrategies() error = %v", err)
	}
	if len(defs.Strategies) != 1 || defs.Strategies[0].StrategyID != "demo" {
		t.Fatalf("ListStrategies() = %+v, want demo strategy", defs)
	}
	barDecision, err := client.OnBar(ctx, DecisionRequest{})
	if err != nil {
		t.Fatalf("OnBar() error = %v", err)
	}
	if barDecision.TargetPosition != -1 || barDecision.Metrics["signal"] != "SHORT" {
		t.Fatalf("OnBar() = %+v, want SHORT target -1", barDecision)
	}
	if barDecision.Trace == nil || barDecision.Trace.EventType != "bar" || barDecision.Trace.StepIndex != 3 {
		t.Fatalf("OnBar() trace = %+v, want bar step 3", barDecision.Trace)
	}
	tickDecision, err := client.OnTick(ctx, DecisionRequest{})
	if err != nil {
		t.Fatalf("OnTick() error = %v", err)
	}
	if !tickDecision.NoSignal || tickDecision.Metrics["state"] != "WAIT_BREAK_BELOW_MA20" {
		t.Fatalf("OnTick() = %+v, want no_signal waiting state", tickDecision)
	}
	if tickDecision.Trace == nil || tickDecision.Trace.EventType != "key_tick" || len(tickDecision.Trace.Checks) != 1 {
		t.Fatalf("OnTick() trace = %+v, want key_tick with checks", tickDecision.Trace)
	}
}
