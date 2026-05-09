package strategy

import (
	"context"
	"strings"
	"testing"
	"time"

	"ctp-future-kline/internal/klinequery"
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
