package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/klinequery"
	"ctp-future-kline/internal/testmysql"
)

type fakeRuntimeStartPlanner struct {
	loadedStrategyID string
	requirementReq   StrategyInstance
	requirements     StartRequirementsResponse
}

func (f *fakeRuntimeStartPlanner) LoadStrategy(_ context.Context, req LoadStrategyRequest) error {
	f.loadedStrategyID = req.StrategyID
	return nil
}

func (f *fakeRuntimeStartPlanner) GetStartRequirements(_ context.Context, req StartRequirementsRequest) (StartRequirementsResponse, error) {
	f.requirementReq = req.Instance
	return f.requirements, nil
}

type fakeRuntimeStartClient struct {
	fakeRuntimeStartPlanner
	started []StrategyInstance
}

func (f *fakeRuntimeStartClient) StartInstance(_ context.Context, req StartInstanceRequest) error {
	f.started = append(f.started, req.Instance)
	return nil
}

func TestRuntimeStartTimeoutUsesLongerDebugFloor(t *testing.T) {
	m := &Manager{cfg: config.StrategyConfig{RequestTimeoutMS: 3000}}

	if got := m.runtimeStartTimeout(); got != minRuntimeStartTimeout {
		t.Fatalf("runtimeStartTimeout() = %v, want %v", got, minRuntimeStartTimeout)
	}
}

func TestSanitizeStrategyInstanceForListOmitsWarmupBars(t *testing.T) {
	inst := StrategyInstance{
		InstanceID: "inst-1",
		Params: map[string]any{
			"warmup_bars":   []map[string]any{{"close": 1.0}},
			"warmup_count":  1,
			"warmup_target": 20,
		},
	}

	out := sanitizeStrategyInstanceForList(inst)
	if _, ok := out.Params["warmup_bars"]; ok {
		t.Fatalf("sanitizeStrategyInstanceForList() kept warmup_bars: %+v", out.Params)
	}
	if out.Params["warmup_bars_omitted"] != true {
		t.Fatalf("sanitizeStrategyInstanceForList() warmup_bars_omitted = %v, want true", out.Params["warmup_bars_omitted"])
	}
	if _, ok := inst.Params["warmup_bars"]; !ok {
		t.Fatalf("sanitizeStrategyInstanceForList() mutated original params")
	}
}

func TestParseInstanceAnchorTimeSupportsStartTime(t *testing.T) {
	ts, ok := parseInstanceAnchorTime(map[string]any{
		"start_time": "2026-05-06T09:31:00",
	})
	if !ok {
		t.Fatalf("parseInstanceAnchorTime() ok = false, want true")
	}
	want := time.Date(2026, 5, 6, 9, 31, 0, 0, time.Local)
	if !ts.Equal(want) {
		t.Fatalf("parseInstanceAnchorTime() = %v, want %v", ts, want)
	}
}

func TestParseInstanceAnchorTimePrefersAdjustedAnchor(t *testing.T) {
	adjusted := time.Date(2026, 5, 6, 21, 5, 0, 0, time.Local)
	dataTime := time.Date(2026, 5, 7, 21, 5, 0, 0, time.Local)
	ts, ok := parseInstanceAnchorTime(map[string]any{
		"chart_start_time": dataTime.Format("2006-01-02 15:04:05"),
		"chart_anchor": map[string]any{
			"adjusted_time": adjusted.Unix(),
			"data_time":     dataTime.Unix(),
		},
	})
	if !ok {
		t.Fatalf("parseInstanceAnchorTime() ok = false, want true")
	}
	if !ts.Equal(adjusted) {
		t.Fatalf("parseInstanceAnchorTime() = %v, want adjusted %v", ts, adjusted)
	}
}

func TestParseInstanceAnchorTimeDoesNotUseDataTimeFallback(t *testing.T) {
	dataTime := time.Date(2026, 5, 7, 21, 5, 0, 0, time.Local)
	_, ok := parseInstanceAnchorTime(map[string]any{
		"chart_anchor": map[string]any{
			"data_time": dataTime.Unix(),
		},
	})
	if ok {
		t.Fatalf("parseInstanceAnchorTime() ok = true, want false when only data_time is present")
	}
}

func TestStrategyBarEventTimeUsesAdjustedTime(t *testing.T) {
	dataTime := time.Date(2026, 5, 7, 21, 5, 0, 0, time.Local)
	adjusted := time.Date(2026, 5, 6, 21, 5, 0, 0, time.Local)
	got := strategyBarEventTime(BarEvent{DataTime: dataTime, AdjustedTime: adjusted})
	if !got.Equal(adjusted) {
		t.Fatalf("strategyBarEventTime() = %v, want adjusted %v", got, adjusted)
	}
}

func TestStrategyBarEventTimeDoesNotUseDataTimeFallback(t *testing.T) {
	dataTime := time.Date(2026, 5, 7, 21, 5, 0, 0, time.Local)
	got := strategyBarEventTime(BarEvent{DataTime: dataTime})
	if !got.IsZero() {
		t.Fatalf("strategyBarEventTime() = %v, want zero when adjusted_time is missing", got)
	}
}

func TestSplitMatchingInstancesRunsIndicatorsFirst(t *testing.T) {
	items := []StrategyInstance{
		{InstanceID: "trade-1", StrategyID: "ma20.state_diagram_short"},
		{InstanceID: "indicator-1", StrategyID: "indicator.zigzag_atr26"},
		{InstanceID: "trade-2", StrategyID: "ma20.weak_pullback_short.baseline"},
	}

	indicators, trading := splitMatchingInstances(items)

	if len(indicators) != 1 || indicators[0].InstanceID != "indicator-1" {
		t.Fatalf("indicators = %+v, want indicator-1 only", indicators)
	}
	if len(trading) != 2 || trading[0].InstanceID != "trade-1" || trading[1].InstanceID != "trade-2" {
		t.Fatalf("trading = %+v, want non-indicators in original order", trading)
	}
}

func TestZigZagFeatureCacheInjectsRecentPeaksByScope(t *testing.T) {
	m := &Manager{features: make(map[strategyFeatureKey][]map[string]any)}
	baseTrace := StrategyTraceRecord{
		StrategyID: "indicator.zigzag_atr26",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeReplay,
		EventTime:  time.Date(2026, 1, 2, 9, 40, 0, 0, time.Local),
		Metrics: map[string]any{
			"indicator":       "zigzag_atr26",
			"zigzag_type":     "PEAK",
			"pivot_index":     12,
			"pivot_time":      "2026-01-02T09:32:00+08:00",
			"pivot_price":     3520.5,
			"confirmed_time":  "2026-01-02T09:40:00+08:00",
			"confirmed_index": 20,
		},
	}
	m.updateFeatureCacheFromTrace(baseTrace)
	trough := baseTrace
	trough.Metrics = map[string]any{"indicator": "zigzag_atr26", "zigzag_type": "TROUGH", "pivot_index": 15}
	m.updateFeatureCacheFromTrace(trough)
	otherMode := baseTrace
	otherMode.Mode = RunTypeRealtime
	otherMode.Metrics = map[string]any{"indicator": "zigzag_atr26", "zigzag_type": "PEAK", "pivot_index": 99}
	m.updateFeatureCacheFromTrace(otherMode)

	features := m.featuresFor(StrategyInstance{Timeframe: "1m"}, "rb2601", RunTypeReplay)
	zigzag, ok := features[zigzagATR26FeatureID].(map[string]any)
	if !ok {
		t.Fatalf("features missing zigzag payload: %+v", features)
	}
	peaks, ok := zigzag["peaks"].([]map[string]any)
	if !ok || len(peaks) != 1 {
		t.Fatalf("peaks = %#v, want one replay peak", zigzag["peaks"])
	}
	if peaks[0]["pivot_index"] != 12 || peaks[0]["confirmed_index"] != 20 {
		t.Fatalf("peak = %+v, want cached pivot and confirmed index", peaks[0])
	}
}

func TestFeatureCacheInjectsDeclaredGenericFeatureOnly(t *testing.T) {
	m := &Manager{features: make(map[strategyFeatureKey][]map[string]any)}
	m.updateFeatureCacheFromTrace(StrategyTraceRecord{
		StrategyID: "indicator.custom",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeRealtime,
		Metrics: map[string]any{
			"feature_key":     "custom_feature",
			"feature_payload": map[string]any{"score": 0.7, "state": "ready"},
		},
	})
	m.updateFeatureCacheFromTrace(StrategyTraceRecord{
		StrategyID: "indicator.other",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeRealtime,
		Metrics: map[string]any{
			"feature_key":     "other_feature",
			"feature_payload": map[string]any{"score": 0.1},
		},
	})

	features := m.featuresFor(StrategyInstance{
		Timeframe: "1m",
		Params: map[string]any{
			"feature_dependencies": []any{"custom_feature"},
		},
	}, "rb2601", RunTypeRealtime)

	custom, ok := features["custom_feature"].(map[string]any)
	if !ok {
		t.Fatalf("custom feature missing: %+v", features)
	}
	if custom["score"] != 0.7 || custom["state"] != "ready" {
		t.Fatalf("custom feature = %+v, want latest payload", custom)
	}
	if _, exists := features["other_feature"]; exists {
		t.Fatalf("other_feature injected despite dependencies: %+v", features)
	}
}

func TestFeatureScoreCacheInjectsScoresAndMissingAsZero(t *testing.T) {
	m := &Manager{features: make(map[strategyFeatureKey][]map[string]any)}
	m.updateFeatureCacheFromTrace(StrategyTraceRecord{
		StrategyID: "indicator.score",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeRealtime,
		Metrics: map[string]any{
			"feature_key":    "score_a",
			"feature_schema": featureScoreSchema,
			"feature_payload": map[string]any{
				"score":     12.5,
				"reference": "too high clamps to ten",
			},
		},
	})
	m.updateFeatureCacheFromTrace(StrategyTraceRecord{
		StrategyID: "indicator.score",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeRealtime,
		Metrics: map[string]any{
			"feature_key":    "score_b",
			"feature_schema": featureScoreSchema,
			"feature_payload": map[string]any{
				"score": -2,
			},
		},
	})

	features := m.featuresFor(StrategyInstance{
		Timeframe: "1m",
		Params: map[string]any{
			"feature_score_dependencies": []any{
				map[string]any{"key": "score_a", "weight": 0.6},
				map[string]any{"key": "score_b", "weight": 0.4},
				map[string]any{"key": "missing_score", "weight": 1.0},
			},
		},
	}, "rb2601", RunTypeRealtime)

	scores, ok := features[featureScoresKey].(map[string]any)
	if !ok {
		t.Fatalf("scores feature missing: %+v", features)
	}
	scoreA, ok := scores["score_a"].(map[string]any)
	if !ok || scoreA["score"] != 10.0 {
		t.Fatalf("score_a = %#v, want clamped score 10", scores["score_a"])
	}
	scoreB, ok := scores["score_b"].(map[string]any)
	if !ok || scoreB["score"] != 0.0 {
		t.Fatalf("score_b = %#v, want clamped score 0", scores["score_b"])
	}
	missing, ok := scores["missing_score"].(map[string]any)
	if !ok || missing["score"] != 0.0 || missing["missing"] != true {
		t.Fatalf("missing_score = %#v, want missing score 0", scores["missing_score"])
	}
}

func TestFeatureDependenciesFallbackKeepsLegacyZigZag(t *testing.T) {
	m := &Manager{features: make(map[strategyFeatureKey][]map[string]any)}
	m.updateFeatureCacheFromTrace(StrategyTraceRecord{
		StrategyID: "indicator.zigzag_atr26",
		Symbol:     "rb2601",
		Timeframe:  "1m",
		Mode:       RunTypeRealtime,
		EventTime:  time.Date(2026, 1, 2, 9, 40, 0, 0, time.Local),
		Metrics: map[string]any{
			"indicator":       "zigzag_atr26",
			"feature_key":     "zigzag_atr26",
			"feature_payload": map[string]any{"pivot_index": 12},
			"zigzag_type":     "PEAK",
			"pivot_index":     12,
			"confirmed_index": 20,
		},
	})

	features := m.featuresFor(StrategyInstance{Timeframe: "1m"}, "rb2601", RunTypeRealtime)
	zigzag, ok := features[zigzagATR26FeatureID].(map[string]any)
	if !ok {
		t.Fatalf("legacy zigzag missing: %+v", features)
	}
	peaks, ok := zigzag["peaks"].([]map[string]any)
	if !ok || len(peaks) != 1 {
		t.Fatalf("peaks = %#v, want one legacy peak", zigzag["peaks"])
	}
}

func TestHandleBarRunsIndicatorBeforeTradingAndInjectsFeature(t *testing.T) {
	type capturedRequest struct {
		StrategyID string
		Features   map[string]any
	}
	var captured []capturedRequest
	results := make(map[string]SignalDecision)
	nextPushID := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/runtime/result" {
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode result request: %v", err)
			}
			pushID := strings.TrimSpace(req["push_id"].(string))
			result, ok := results[pushID]
			if !ok {
				t.Fatalf("missing result for push_id=%s", pushID)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"status": "done", "result": json.RawMessage(raw)}); err != nil {
				t.Fatalf("encode result response: %v", err)
			}
			return
		}
		var req DecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		captured = append(captured, capturedRequest{StrategyID: req.Instance.StrategyID, Features: req.Features})
		resp := SignalDecision{
			NoSignal:       true,
			InstanceID:     req.Instance.InstanceID,
			Symbol:         req.Symbol,
			EventTime:      req.EventTime,
			TargetPosition: req.CurrentPosition,
			Metrics:        map[string]any{},
		}
		if req.Instance.StrategyID == "indicator.zigzag_atr26" {
			resp.Trace = &StrategyTraceRecord{
				StrategyID: req.Instance.StrategyID,
				Symbol:     req.Symbol,
				Timeframe:  req.Instance.Timeframe,
				Mode:       req.Mode,
				EventType:  "bar",
				EventTime:  time.Date(2026, 1, 2, 9, 35, 0, 0, time.Local),
				StepKey:    "ZIGZAG_PEAK",
				StepLabel:  "确认波峰",
				StepIndex:  1,
				StepTotal:  1,
				Status:     "passed",
				Metrics: map[string]any{
					"indicator":       "zigzag_atr26",
					"zigzag_type":     "PEAK",
					"pivot_index":     7,
					"pivot_time":      "2026-01-02T09:32:00+08:00",
					"pivot_price":     3510.0,
					"confirmed_time":  "2026-01-02T09:35:00+08:00",
					"confirmed_index": 10,
				},
			}
		}
		nextPushID++
		pushID := "push-" + strconv.Itoa(nextPushID)
		results[pushID] = resp
		if err := json.NewEncoder(w).Encode(map[string]any{"ok": true, "push_id": pushID}); err != nil {
			t.Fatalf("encode push response: %v", err)
		}
	}))
	defer server.Close()

	m := &Manager{
		cfg:       config.StrategyConfig{RequestTimeoutMS: 3000},
		exec:      NewExecutionEngine(),
		client:    NewStrategyServiceClient(server.URL, server.Client()),
		connReady: true,
		features:  make(map[strategyFeatureKey][]map[string]any),
		instances: map[string]StrategyInstance{
			"trade-1": {
				InstanceID: "trade-1",
				StrategyID: "ma20.state_diagram_short",
				Mode:       RunTypeRealtime,
				Status:     InstanceStatusRunning,
				Symbols:    []string{"rb2601"},
				Timeframe:  "1m",
				Params:     map[string]any{},
			},
			"indicator-1": {
				InstanceID: "indicator-1",
				StrategyID: "indicator.zigzag_atr26",
				Mode:       RunTypeRealtime,
				Status:     InstanceStatusRunning,
				Symbols:    []string{"rb2601"},
				Timeframe:  "1m",
				Params:     map[string]any{},
			},
		},
	}

	m.HandleRealtimeBar(BarEvent{
		InstrumentID: "rb2601",
		AdjustedTime: time.Date(2026, 1, 2, 9, 35, 0, 0, time.Local),
		Period:       "1m",
		Open:         3500,
		High:         3512,
		Low:          3498,
		Close:        3508,
	})

	if len(captured) != 2 {
		t.Fatalf("captured requests = %d, want 2", len(captured))
	}
	if captured[0].StrategyID != "indicator.zigzag_atr26" || captured[1].StrategyID != "ma20.state_diagram_short" {
		t.Fatalf("strategy call order = %+v, want indicator then trading", captured)
	}
	if captured[1].Features == nil {
		t.Fatalf("trading request features nil, want zigzag peak")
	}
	zigzag, ok := captured[1].Features[zigzagATR26FeatureID].(map[string]any)
	if !ok {
		t.Fatalf("trading features missing zigzag payload: %+v", captured[1].Features)
	}
	peaks, ok := zigzag["peaks"].([]any)
	if !ok || len(peaks) != 1 {
		t.Fatalf("trading peaks = %#v, want one peak", zigzag["peaks"])
	}
	peak, ok := peaks[0].(map[string]any)
	if !ok || peak["pivot_index"] != float64(7) || peak["confirmed_index"] != float64(10) {
		t.Fatalf("trading peak = %#v, want injected zigzag pivot", peaks[0])
	}
}

func TestIndicatorSignalDoesNotExecutePositionChange(t *testing.T) {
	result := SignalDecision{
		NoSignal:       false,
		InstanceID:     "indicator-1",
		Symbol:         "rb2601",
		EventTime:      "2026-01-02T09:35:00+08:00",
		TargetPosition: -1,
		Confidence:     1,
		Reason:         "bad indicator signal",
		Metrics:        map[string]any{},
	}
	nextPushID := 0
	results := map[string]SignalDecision{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/runtime/result" {
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode result request: %v", err)
			}
			pushID := strings.TrimSpace(fmt.Sprint(req["push_id"]))
			raw, err := json.Marshal(results[pushID])
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"status": "done", "result": json.RawMessage(raw)}); err != nil {
				t.Fatalf("encode result response: %v", err)
			}
			return
		}
		nextPushID++
		pushID := "push-" + strconv.Itoa(nextPushID)
		results[pushID] = result
		if err := json.NewEncoder(w).Encode(map[string]any{"ok": true, "push_id": pushID}); err != nil {
			t.Fatalf("encode push response: %v", err)
		}
	}))
	defer server.Close()

	m := &Manager{
		cfg:       config.StrategyConfig{RequestTimeoutMS: 3000},
		exec:      NewExecutionEngine(),
		client:    NewStrategyServiceClient(server.URL, server.Client()),
		connReady: true,
		features:  make(map[strategyFeatureKey][]map[string]any),
	}
	inst := StrategyInstance{
		InstanceID: "indicator-1",
		StrategyID: "indicator.custom",
		Mode:       RunTypeRealtime,
		Status:     InstanceStatusRunning,
		Symbols:    []string{"rb2601"},
		Timeframe:  "1m",
	}

	m.callDecision(inst, "rb2601", RunTypeRealtime, "", time.Date(2026, 1, 2, 9, 35, 0, 0, time.Local), nil, &BarEvent{
		InstrumentID: "rb2601",
		AdjustedTime: time.Date(2026, 1, 2, 9, 35, 0, 0, time.Local),
		Period:       "1m",
		Open:         3500,
		High:         3512,
		Low:          3498,
		Close:        3508,
	})

	if got := m.exec.CurrentPosition("rb2601"); got != 0 {
		t.Fatalf("indicator changed position to %v, want unchanged 0", got)
	}
}

func TestWarmupBarsFromParams(t *testing.T) {
	bars := warmupBarsFromParams(map[string]any{
		"warmup_bars": []klinequery.KlineBar{
			{AdjustedTime: 1, DataTime: 1, Open: 1, High: 2, Low: 0.5, Close: 1.5},
			{AdjustedTime: 2, DataTime: 2, Open: 1.5, High: 2.5, Low: 1, Close: 2},
		},
	})
	if len(bars) != 2 {
		t.Fatalf("warmupBarsFromParams() len = %d, want 2", len(bars))
	}
	if bars[1].Close != 2 {
		t.Fatalf("warmupBarsFromParams()[1].Close = %v, want 2", bars[1].Close)
	}
}

func TestBuildRuntimeStartPlanUsesPythonFactoryRequirements(t *testing.T) {
	m := &Manager{}
	planner := &fakeRuntimeStartPlanner{
		requirements: StartRequirementsResponse{WarmupTarget: 2, RequiresAnchorTime: true},
	}
	plan, err := m.buildRuntimeStartPlan(context.Background(), planner, StrategyInstance{
		InstanceID: "inst-1",
		StrategyID: "ma20.pullback_short",
		Timeframe:  "1m",
		Params: map[string]any{
			"warmup_bars": []klinequery.KlineBar{
				{AdjustedTime: 1, DataTime: 1, Open: 1, High: 2, Low: 0.5, Close: 1.5},
				{AdjustedTime: 2, DataTime: 2, Open: 1.5, High: 2.5, Low: 1, Close: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("buildRuntimeStartPlan() error = %v", err)
	}
	if planner.loadedStrategyID != "ma20.pullback_short" {
		t.Fatalf("loaded strategy id = %q, want ma20.pullback_short", planner.loadedStrategyID)
	}
	if planner.requirementReq.InstanceID != "inst-1" {
		t.Fatalf("requirement instance id = %q, want inst-1", planner.requirementReq.InstanceID)
	}
	if plan.Requirements.WarmupTarget != 2 {
		t.Fatalf("plan warmup target = %d, want 2", plan.Requirements.WarmupTarget)
	}
	if got := plan.Instance.Params["warmup_count"]; got != 2 {
		t.Fatalf("plan warmup_count = %v, want 2", got)
	}
}

func TestListDefinitionsReturnsEmptyWhenPythonUnavailable(t *testing.T) {
	m := &Manager{}
	items, err := m.ListDefinitions()
	if err != nil {
		t.Fatalf("ListDefinitions() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("ListDefinitions() len = %d, want 0 when python ListStrategies is unavailable", len(items))
	}
}

func TestRestoreRunningInstancesStartsPythonRuntimeAndWritesTrace(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		store:     store,
		instances: make(map[string]StrategyInstance),
	}
	running := StrategyInstance{
		InstanceID:  "inst-running",
		StrategyID:  "sample.momentum",
		DisplayName: "running sample",
		Mode:        "paper",
		Status:      InstanceStatusRunning,
		AccountID:   "paper",
		Symbols:     []string{"rb2601"},
		Timeframe:   "1m",
		Params:      map[string]any{},
	}
	stopped := running
	stopped.InstanceID = "inst-stopped"
	stopped.Status = InstanceStatusStopped
	if err := store.SaveInstance(running); err != nil {
		t.Fatalf("SaveInstance(running) error = %v", err)
	}
	if err := store.SaveInstance(stopped); err != nil {
		t.Fatalf("SaveInstance(stopped) error = %v", err)
	}

	client := &fakeRuntimeStartClient{
		fakeRuntimeStartPlanner: fakeRuntimeStartPlanner{
			requirements: StartRequirementsResponse{},
		},
	}
	if err := m.restoreRunningInstancesWithClient(context.Background(), client, "test reconnect"); err != nil {
		t.Fatalf("restoreRunningInstancesWithClient() error = %v", err)
	}
	if len(client.started) != 1 {
		t.Fatalf("started runtimes = %d, want 1", len(client.started))
	}
	if client.started[0].InstanceID != running.InstanceID {
		t.Fatalf("started instance = %q, want %q", client.started[0].InstanceID, running.InstanceID)
	}
	if _, ok := m.instances[running.InstanceID]; !ok {
		t.Fatalf("manager instances missing restored instance %q", running.InstanceID)
	}
	traces, err := store.ListTraces(running.InstanceID, "rb2601", 10)
	if err != nil {
		t.Fatalf("ListTraces() error = %v", err)
	}
	if len(traces) != 1 {
		t.Fatalf("trace count = %d, want 1", len(traces))
	}
	if traces[0].StepKey != "WAIT_BREAK_BELOW_MA20" || !strings.Contains(traces[0].Reason, "restored") {
		t.Fatalf("restore trace = %+v, want baseline start trace with restore reason", traces[0])
	}
}

func TestPersistSignalResultPointSupportsStateDiagramTerminals(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{store: store}
	inst := StrategyInstance{
		InstanceID: "state-1",
		StrategyID: "ma20.state_diagram_short",
		Timeframe:  "1m",
	}
	eventTime := time.Date(2026, 1, 2, 9, 30, 0, 0, time.Local)
	m.persistSignalResultPoint(inst, "rb2601", RunTypeReplay, eventTime, StrategyTraceRecord{
		StepKey:   "TAKE_PROFIT",
		StepLabel: "止盈",
		Status:    "passed",
		Reason:    "take profit",
		Metrics:   map[string]any{"signal_result": "success"},
		EventTime: eventTime,
	})

	items, err := store.ListSignals("state-1", "rb2601", 10)
	if err != nil {
		t.Fatalf("ListSignals() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("signals len = %d, want 1", len(items))
	}
	if items[0].TargetPosition != 0 || items[0].Metrics["signal_result"] != "success" {
		t.Fatalf("signal = %+v, want target 0 success exit", items[0])
	}
}

func TestPrepareRuntimeStartInstanceUsesStartRequirements(t *testing.T) {
	m := &Manager{}
	out, err := m.prepareRuntimeStartInstance(StrategyInstance{
		InstanceID: "inst-1",
		StrategyID: "ma20.pullback_short",
		Timeframe:  "1m",
		Params: map[string]any{
			"warmup_bars": []klinequery.KlineBar{
				{AdjustedTime: 1, DataTime: 1, Open: 1, High: 2, Low: 0.5, Close: 1.5},
				{AdjustedTime: 2, DataTime: 2, Open: 1.5, High: 2.5, Low: 1, Close: 2},
			},
		},
	}, StartRequirementsResponse{WarmupTarget: 2, RequiresAnchorTime: true})
	if err != nil {
		t.Fatalf("prepareRuntimeStartInstance() error = %v", err)
	}
	if got := out.Params["warmup_target"]; got != 2 {
		t.Fatalf("warmup_target = %v, want 2", got)
	}
	if got := out.Params["warmup_count"]; got != 2 {
		t.Fatalf("warmup_count = %v, want 2", got)
	}
}

func TestInferWarmupScope(t *testing.T) {
	symbol, kind, variety := inferWarmupScope(StrategyInstance{
		Symbols: []string{"rb2601"},
	})
	if symbol != "rb2601" || kind != "contract" || variety != "rb" {
		t.Fatalf("inferWarmupScope(contract) = (%q,%q,%q)", symbol, kind, variety)
	}
	symbol, kind, variety = inferWarmupScope(StrategyInstance{
		Symbols: []string{"agl9"},
	})
	if symbol != "agl9" || kind != "l9" || variety != "ag" {
		t.Fatalf("inferWarmupScope(l9) = (%q,%q,%q)", symbol, kind, variety)
	}
}

func TestWarmupQuerySourcesReplayFallsBackToRealtime(t *testing.T) {
	items := warmupQuerySources(StrategyInstance{Mode: RunTypeReplay}, "realtime-dsn", "replay-dsn")
	if len(items) != 2 {
		t.Fatalf("warmupQuerySources() len = %d, want 2", len(items))
	}
	if items[0].name != "replay" || items[0].dsn != "replay-dsn" {
		t.Fatalf("warmupQuerySources()[0] = %+v, want replay source first", items[0])
	}
	if items[1].name != "realtime" || items[1].dsn != "realtime-dsn" {
		t.Fatalf("warmupQuerySources()[1] = %+v, want realtime source second", items[1])
	}
}

func TestWarmupQuerySourcesDedupesSameDSN(t *testing.T) {
	items := warmupQuerySources(StrategyInstance{Mode: RunTypeReplay}, "same-dsn", "same-dsn")
	if len(items) != 1 {
		t.Fatalf("warmupQuerySources() len = %d, want 1", len(items))
	}
	if items[0].name != "replay" || items[0].dsn != "same-dsn" {
		t.Fatalf("warmupQuerySources()[0] = %+v, want single replay item", items[0])
	}
}

func TestWarmupQuerySourcesRealtimeOverrideWinsInReplayMode(t *testing.T) {
	items := warmupQuerySources(StrategyInstance{
		Mode: RunTypeReplay,
		Params: map[string]any{
			"warmup_source": "realtime",
			"replay_mode":   "kline",
		},
	}, "realtime-dsn", "replay-dsn")
	if len(items) != 1 {
		t.Fatalf("warmupQuerySources() len = %d, want 1", len(items))
	}
	if items[0].name != "realtime" || items[0].dsn != "realtime-dsn" {
		t.Fatalf("warmupQuerySources()[0] = %+v, want single realtime source", items[0])
	}
}

func TestWarmupQuerySourcesKlineReplayUsesRealtimeOnly(t *testing.T) {
	items := warmupQuerySources(StrategyInstance{
		Mode: RunTypeReplay,
		Params: map[string]any{
			"replay_mode": "kline",
		},
	}, "realtime-dsn", "replay-dsn")
	if len(items) != 1 {
		t.Fatalf("warmupQuerySources() len = %d, want 1", len(items))
	}
	if items[0].name != "realtime" || items[0].dsn != "realtime-dsn" {
		t.Fatalf("warmupQuerySources()[0] = %+v, want single realtime source", items[0])
	}
}

func TestPrepareRuntimeStartInstanceRequiresAnchorTimeWhenWarmupNeeded(t *testing.T) {
	m := &Manager{}
	_, err := m.prepareRuntimeStartInstance(StrategyInstance{
		InstanceID: "inst-1",
		StrategyID: "ma20.pullback_short",
		Timeframe:  "1m",
		Params:     map[string]any{},
	}, StartRequirementsResponse{WarmupTarget: 40, RequiresAnchorTime: true})
	if err == nil {
		t.Fatalf("prepareRuntimeStartInstance() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "start_time/chart_start_time/chart_anchor is required") {
		t.Fatalf("prepareRuntimeStartInstance() error = %q, want required anchor time message", err.Error())
	}
}
