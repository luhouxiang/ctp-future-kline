package strategy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStrategyDefinitionUnmarshalAcceptsPythonNaiveUpdatedAt(t *testing.T) {
	t.Parallel()

	var def StrategyDefinition
	err := json.Unmarshal([]byte(`{
		"strategy_id": "ma20.weak_pullback_short.baseline",
		"display_name": "MA20 Weak Pullback Baseline",
		"entry_script": "python/ma20_weak_pullback.py",
		"version": "1.0",
		"default_params": {},
		"updated_at": "2026-05-11T23:04:29"
	}`), &def)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if def.StrategyID != "ma20.weak_pullback_short.baseline" || def.UpdatedAt.IsZero() {
		t.Fatalf("StrategyDefinition = %+v, want decoded strategy with non-zero updated_at", def)
	}
}

func TestHTTPStrategyClientUsesShortIdleTimeout(t *testing.T) {
	t.Parallel()

	client := NewStrategyServiceClient("127.0.0.1:50051", nil)
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("http transport = %T, want *http.Transport", client.httpClient.Transport)
	}
	if transport.IdleConnTimeout != strategyHTTPIdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %s, want %s", transport.IdleConnTimeout, strategyHTTPIdleConnTimeout)
	}
}

func TestHTTPStrategyServiceRoundTrip(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health/ping":
			_ = json.NewEncoder(w).Encode(HealthResponse{OK: true, Version: "test"})
		case "/registry/list":
			_ = json.NewEncoder(w).Encode(ListStrategiesResponse{Strategies: []StrategyDefinition{{StrategyID: "demo", DisplayName: "Demo"}}})
		case "/runtime/load":
			_ = json.NewEncoder(w).Encode(HealthResponse{OK: true})
		case "/runtime/start-requirements":
			_ = json.NewEncoder(w).Encode(StartRequirementsResponse{WarmupTarget: 40, RequiresAnchorTime: true})
		case "/runtime/start", "/runtime/stop":
			_ = json.NewEncoder(w).Encode(HealthResponse{OK: true})
		case "/runtime/on_bar":
			_ = json.NewEncoder(w).Encode(asyncPushResponse{OK: true, PushID: "bar-1", Status: "queued"})
		case "/runtime/on_tick":
			_ = json.NewEncoder(w).Encode(asyncPushResponse{OK: true, PushID: "tick-1", Status: "queued"})
		case "/runtime/result":
			var req asyncResultRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			switch req.PushID {
			case "bar-1":
				raw, _ := json.Marshal(SignalDecision{
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
				})
				_ = json.NewEncoder(w).Encode(asyncResultResponse{PushID: req.PushID, Status: "done", Result: raw})
			case "tick-1":
				raw, _ := json.Marshal(SignalDecision{
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
				})
				_ = json.NewEncoder(w).Encode(asyncResultResponse{PushID: req.PushID, Status: "done", Result: raw})
			default:
				http.Error(w, "unknown push id", http.StatusNotFound)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := NewStrategyServiceClient(server.URL, server.Client())

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
	requirements, err := client.GetStartRequirements(ctx, StartRequirementsRequest{Instance: StrategyInstance{StrategyID: "demo"}})
	if err != nil {
		t.Fatalf("GetStartRequirements() error = %v", err)
	}
	if requirements.WarmupTarget != 40 || !requirements.RequiresAnchorTime {
		t.Fatalf("GetStartRequirements() = %+v, want warmup target 40 and anchor required", requirements)
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

func TestPushAndWaitHandlesAsyncErrorExpiredAndTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/runtime/on_bar":
			_ = json.NewEncoder(w).Encode(asyncPushResponse{OK: true, PushID: "error-1", Status: "queued"})
		case "/runtime/on_tick":
			_ = json.NewEncoder(w).Encode(asyncPushResponse{OK: true, PushID: "expired-1", Status: "queued"})
		case "/runtime/on_replay_bar":
			_ = json.NewEncoder(w).Encode(asyncPushResponse{OK: true, PushID: "running-1", Status: "queued"})
		case "/runtime/result":
			var req asyncResultRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			switch req.PushID {
			case "error-1":
				_ = json.NewEncoder(w).Encode(asyncResultResponse{PushID: req.PushID, Status: "error", Error: "boom"})
			case "expired-1":
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(asyncResultResponse{PushID: req.PushID, Status: "expired", Error: "expired"})
			case "running-1":
				time.Sleep(5 * time.Millisecond)
				_ = json.NewEncoder(w).Encode(asyncResultResponse{PushID: req.PushID, Status: "running"})
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewStrategyServiceClient(server.URL, server.Client())

	_, err := client.OnBar(context.Background(), DecisionRequest{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("OnBar() error = %v, want async boom error", err)
	}

	_, err = client.OnTick(context.Background(), DecisionRequest{})
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("OnTick() error = %v, want expired error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = client.OnReplayBar(ctx, DecisionRequest{})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("OnReplayBar() error = %v, want context deadline error", err)
	}
}
