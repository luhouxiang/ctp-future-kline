package strategy

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAppendSignalEventLogWritesExcelCSVPerInstance(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir(temp) error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	startedAt := time.Date(2026, 4, 2, 23, 0, 0, 0, time.Local)
	inst := StrategyInstance{
		InstanceID:    "inst:demo/1",
		StrategyID:    "ma20.weak_pullback_short",
		DisplayName:   "MA20 回调空",
		Mode:          RunTypeReplay,
		Symbols:       []string{"rb2601"},
		Timeframe:     "1m",
		LastStartedAt: &startedAt,
	}
	m := &Manager{}
	ma20 := 3521.5
	m.appendSignalEventLog(inst, "rb2601", RunTypeReplay, startedAt, SignalDecision{
		TargetPosition: -1,
		Confidence:     0.9,
		Reason:         "开仓测试",
		Metrics:        map[string]any{"ma20": ma20, "ma60": 3540},
	}, ExecutionPlan{
		CurrentPosition: 0,
		TargetPosition:  -1,
		PlannedDelta:    -1,
	}, &BarEvent{
		Open:         3510,
		High:         3520,
		Low:          3500,
		Close:        3505,
		AdjustedTime: startedAt,
	})
	m.appendSignalEventLog(inst, "rb2601", RunTypeReplay, startedAt.Add(time.Minute), SignalDecision{
		TargetPosition: 0,
		Confidence:     0.9,
		Reason:         "平仓测试",
		Metrics:        map[string]any{"ma20": ma20, "ma60": 3540},
	}, ExecutionPlan{
		CurrentPosition: -1,
		TargetPosition:  0,
		PlannedDelta:    1,
	}, &BarEvent{
		Open:         3500,
		High:         3502,
		Low:          3490,
		Close:        3495,
		AdjustedTime: startedAt.Add(time.Minute),
	})

	files, err := filepath.Glob(filepath.Join("logs", "strategy_signal_events", "*.csv"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("csv files=%v, want one file", files)
	}
	name := filepath.Base(files[0])
	for _, want := range []string{"MA20", "回调空", "inst_demo_1", "rb2601", "1m", "20260402_230000"} {
		if !strings.Contains(name, want) {
			t.Fatalf("filename=%q, want to contain %q", name, want)
		}
	}

	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.HasPrefix(string(body), "\ufeff") {
		t.Fatalf("csv missing UTF-8 BOM for Excel")
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(body), "\ufeff")))
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows len=%d, want header + two data rows", len(rows))
	}
	if rows[0][0] != "记录时间" || rows[0][7] != "策略名" || rows[0][20] != "盈亏点数" || rows[0][21] != "原因" {
		t.Fatalf("header=%v, want excel-friendly Chinese columns", rows[0])
	}
	if rows[1][1] != "open" || rows[1][2] != "开仓" || rows[1][7] != "MA20 回调空" || rows[1][15] != "3521.5" || rows[1][20] != "0" || rows[1][21] != "开仓测试" {
		t.Fatalf("data row=%v, want exported signal event values", rows[1])
	}
	if rows[2][1] != "close" || rows[2][2] != "平仓" || rows[2][20] != "10" || rows[2][21] != "平仓测试" {
		t.Fatalf("close row=%v, want short close profit points", rows[2])
	}
}
