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

func TestStoreListAndDeleteSignalsByInstance(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	eventTime := time.Date(2026, 1, 1, 9, 30, 0, 0, time.Local)
	_, err = store.AppendSignal(SignalRecord{
		InstanceID:     "inst-1",
		StrategyID:     "ma20.pullback_short",
		Symbol:         "rb2601",
		Timeframe:      "1m",
		Mode:           RunTypeReplay,
		EventTime:      eventTime,
		TargetPosition: -1,
		Confidence:     0.9,
		Reason:         "short signal",
		Metrics:        map[string]any{"entry_price": 100.5},
	})
	if err != nil {
		t.Fatalf("AppendSignal(inst-1) error = %v", err)
	}
	_, err = store.AppendSignal(SignalRecord{
		InstanceID:     "inst-2",
		StrategyID:     "ma20.pullback_short",
		Symbol:         "rb2601",
		Timeframe:      "1m",
		Mode:           RunTypeReplay,
		EventTime:      eventTime.Add(time.Minute),
		TargetPosition: -1,
		Confidence:     0.8,
		Reason:         "other signal",
		Metrics:        map[string]any{"entry_price": 99.5},
	})
	if err != nil {
		t.Fatalf("AppendSignal(inst-2) error = %v", err)
	}

	items, err := store.ListSignals("inst-1", "rb2601", 10)
	if err != nil {
		t.Fatalf("ListSignals() error = %v", err)
	}
	if len(items) != 1 || items[0].InstanceID != "inst-1" || items[0].TargetPosition != -1 {
		t.Fatalf("ListSignals() items=%+v, want only inst-1 short signal", items)
	}

	deleted, err := store.DeleteSignals("inst-1")
	if err != nil {
		t.Fatalf("DeleteSignals() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteSignals() deleted=%d, want 1", deleted)
	}
	items, err = store.ListSignals("", "rb2601", 10)
	if err != nil {
		t.Fatalf("ListSignals(all) error = %v", err)
	}
	if len(items) != 1 || items[0].InstanceID != "inst-2" {
		t.Fatalf("ListSignals(all) items=%+v, want only inst-2 remaining", items)
	}
	deleted, err = store.DeleteReplaySignalsForScope("ma20.pullback_short", "rb2601", "1m")
	if err != nil {
		t.Fatalf("DeleteReplaySignalsForScope() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteReplaySignalsForScope() deleted=%d, want 1", deleted)
	}
	items, err = store.ListSignals("", "rb2601", 10)
	if err != nil {
		t.Fatalf("ListSignals(after scope delete) error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("ListSignals(after scope delete) items=%+v, want empty", items)
	}
}
