package strategy

import (
	"testing"
	"time"

	"ctp-future-kline/internal/testmysql"
)

func TestStoreAppendAndListTraces(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	eventTime := time.Date(2026, 1, 1, 9, 30, 0, 0, time.Local)
	id, err := store.AppendTrace(StrategyTraceRecord{
		InstanceID:    "inst-1",
		StrategyID:    "ma20.pullback_short",
		Symbol:        "rb2601",
		Timeframe:     "1m",
		Mode:          "realtime",
		EventType:     "bar",
		EventTime:     eventTime,
		StepKey:       "WAIT_BREAK_BELOW_MA20",
		StepLabel:     "等待跌破 MA20",
		StepIndex:     2,
		StepTotal:     5,
		Status:        "waiting",
		Reason:        "no trade signal",
		Checks:        []TraceCheck{{Name: "收盘低于MA20", Passed: false, Current: 100.0, Target: 99.5}},
		Metrics:       map[string]any{"ma20": 99.5},
		SignalPreview: map[string]any{},
	})
	if err != nil {
		t.Fatalf("AppendTrace() error = %v", err)
	}
	if id <= 0 {
		t.Fatalf("AppendTrace() id=%d, want >0", id)
	}

	items, err := store.ListTraces("inst-1", "rb2601", 10)
	if err != nil {
		t.Fatalf("ListTraces() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("ListTraces() len=%d, want 1", len(items))
	}
	got := items[0]
	if got.TraceID != id || got.StepKey != "WAIT_BREAK_BELOW_MA20" || len(got.Checks) != 1 {
		t.Fatalf("ListTraces() item=%+v, want saved trace", got)
	}
}
