package strategy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/testmysql"
)

func TestReplayReportIncrementalSignalOrderAndFinalize(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		cfg:     config.StrategyConfig{BacktestOutputDir: filepath.Join(t.TempDir(), "reports")},
		store:   store,
		exec:    NewExecutionEngine(),
		events:  make(map[chan EventEnvelope]struct{}),
		reports: make(map[string]*ReplayReport),
	}
	inst := StrategyInstance{
		InstanceID:  "inst-1",
		StrategyID:  "strategy.demo",
		DisplayName: "Demo",
		Mode:        RunTypeReplay,
		Symbols:     []string{"rb2601"},
		Timeframe:   "1m",
	}
	eventTime := time.Date(2026, 5, 19, 21, 0, 0, 0, time.Local)

	m.persistDecision(inst, "rb2601", RunTypeReplay, "task-1", eventTime, SignalDecision{
		TargetPosition: -1,
		Confidence:     0.8,
		Reason:         "short signal",
		Metrics:        map[string]any{"ma20": 100.5},
	})

	runs, err := store.ListRuns(10)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs len=%d, want 1", len(runs))
	}
	run := runs[0]
	if run.RunType != RunTypeReplayReport || run.Status != "running" {
		t.Fatalf("run type/status = %s/%s, want replay_report/running", run.RunType, run.Status)
	}
	if got := run.Summary["signal_count"]; got != float64(1) && got != 1 {
		t.Fatalf("signal_count=%v, want 1", got)
	}

	result := readReplayReportResult(t, run.OutputPath)
	if rows, ok := result["signal_table"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("signal_table=%#v, want one row", result["signal_table"])
	}
	if rows, ok := result["order_table"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("order_table=%#v, want one row", result["order_table"])
	}
	if rows, ok := result["strategy_analysis_table"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("strategy_analysis_table=%#v, want one row", result["strategy_analysis_table"])
	}

	m.FinalizeReplayReports("task-1", "done")
	finalRun, err := store.GetRun(run.RunID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if finalRun.Status != "done" || finalRun.FinishedAt == nil {
		t.Fatalf("final run status/finished = %s/%v, want done/non-nil", finalRun.Status, finalRun.FinishedAt)
	}
}

func TestReplayReportCreatedForNoSignalReplayDecision(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		cfg:     config.StrategyConfig{BacktestOutputDir: filepath.Join(t.TempDir(), "reports")},
		store:   store,
		events:  make(map[chan EventEnvelope]struct{}),
		reports: make(map[string]*ReplayReport),
	}
	inst := StrategyInstance{
		InstanceID: "inst-no-signal",
		StrategyID: "strategy.demo",
		Mode:       RunTypeReplay,
		Symbols:    []string{"rb2601"},
		Timeframe:  "1m",
	}

	m.touchReplayReport("task-no-signal", inst)

	runs, err := store.ListRuns(10)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs len=%d, want 1", len(runs))
	}
	run := runs[0]
	if run.RunType != RunTypeReplayReport || run.Status != "running" {
		t.Fatalf("run type/status = %s/%s, want replay_report/running", run.RunType, run.Status)
	}
	result := readReplayReportResult(t, run.OutputPath)
	if rows, ok := result["signal_table"].([]any); !ok || len(rows) != 0 {
		t.Fatalf("signal_table=%#v, want empty table", result["signal_table"])
	}
	if rows, ok := result["order_table"].([]any); !ok || len(rows) != 0 {
		t.Fatalf("order_table=%#v, want empty table", result["order_table"])
	}
	analysis, ok := result["strategy_analysis_table"].([]any)
	if !ok || len(analysis) != 1 {
		t.Fatalf("strategy_analysis_table=%#v, want one row", result["strategy_analysis_table"])
	}
	row, ok := analysis[0].(map[string]any)
	if !ok || row["signal_count"] != float64(0) || row["order_plan_count"] != float64(0) {
		t.Fatalf("analysis row=%#v, want zero counts", analysis[0])
	}
}

func TestReplayReportCreatedWithoutReplayTaskIDAndFinalizedOnStop(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		cfg:     config.StrategyConfig{BacktestOutputDir: filepath.Join(t.TempDir(), "reports")},
		store:   store,
		events:  make(map[chan EventEnvelope]struct{}),
		reports: make(map[string]*ReplayReport),
	}
	inst := StrategyInstance{
		InstanceID: "inst-local-replay",
		StrategyID: "strategy.demo",
		Mode:       RunTypeReplay,
		Symbols:    []string{"rb2601"},
		Timeframe:  "1m",
	}

	m.touchReplayReport("", inst)
	m.FinalizeReplayReportsForInstance(inst.InstanceID, InstanceStatusStopped)

	run, err := store.GetRun("replay-report-instance-inst-local-replay-inst-local-replay")
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if run.Status != "stopped" || run.FinishedAt == nil {
		t.Fatalf("run status/finished = %s/%v, want stopped/non-nil", run.Status, run.FinishedAt)
	}
	result := readReplayReportResult(t, run.OutputPath)
	if result["replay_task_id"] != "instance-inst-local-replay" {
		t.Fatalf("replay_task_id=%v, want synthetic instance scope", result["replay_task_id"])
	}
}

func TestReplayReportSeparatesInstances(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		cfg:     config.StrategyConfig{BacktestOutputDir: filepath.Join(t.TempDir(), "reports")},
		store:   store,
		exec:    NewExecutionEngine(),
		events:  make(map[chan EventEnvelope]struct{}),
		reports: make(map[string]*ReplayReport),
	}
	for _, instanceID := range []string{"inst-a", "inst-b"} {
		inst := StrategyInstance{
			InstanceID: instanceID,
			StrategyID: "strategy.demo",
			Mode:       RunTypeReplay,
			Symbols:    []string{"rb2601"},
			Timeframe:  "1m",
		}
		m.persistDecision(inst, "rb2601", RunTypeReplay, "task-1", time.Now(), SignalDecision{
			TargetPosition: -1,
			Confidence:     0.8,
			Reason:         instanceID,
			Metrics:        map[string]any{},
		})
	}
	runs, err := store.ListRuns(10)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs len=%d, want 2", len(runs))
	}
	seen := map[string]bool{}
	for _, run := range runs {
		seen[run.InstanceID] = true
	}
	if !seen["inst-a"] || !seen["inst-b"] {
		t.Fatalf("runs by instance=%v, want inst-a and inst-b", seen)
	}
}

func readReplayReportResult(t *testing.T, path string) map[string]any {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("Unmarshal result error = %v", err)
	}
	return out
}
