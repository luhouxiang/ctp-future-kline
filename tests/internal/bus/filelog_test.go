package bus_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"ctp-go-demo/internal/bus"
)

func TestFileLogAppendAndIterate(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "bus")
	log := bus.NewFileLog(dir, 0)
	defer func() { _ = log.Close() }()

	ev1 := bus.BusEvent{
		EventID:    bus.NewEventID(),
		Topic:      bus.TopicTick,
		Source:     "test",
		OccurredAt: time.Date(2026, 2, 15, 9, 0, 0, 0, time.Local),
		Payload:    mustJSON(t, map[string]any{"i": 1}),
	}
	ev2 := bus.BusEvent{
		EventID:    bus.NewEventID(),
		Topic:      bus.TopicBar,
		Source:     "test",
		OccurredAt: time.Date(2026, 2, 15, 9, 1, 0, 0, time.Local),
		Payload:    mustJSON(t, map[string]any{"i": 2}),
	}
	cur1, err := log.Append(ev1)
	if err != nil {
		t.Fatalf("append ev1 failed: %v", err)
	}
	_, err = log.Append(ev2)
	if err != nil {
		t.Fatalf("append ev2 failed: %v", err)
	}

	var got []string
	err = log.Iterate(context.Background(), bus.ReadOptions{}, func(_ context.Context, ev bus.BusEvent, _ bus.FileCursor) error {
		got = append(got, ev.EventID)
		return nil
	})
	if err != nil {
		t.Fatalf("iterate failed: %v", err)
	}
	if len(got) != 2 || got[0] != ev1.EventID || got[1] != ev2.EventID {
		t.Fatalf("iterate ids = %v", got)
	}

	var gotFromOffset []string
	err = log.Iterate(context.Background(), bus.ReadOptions{FromCursor: &cur1}, func(_ context.Context, ev bus.BusEvent, _ bus.FileCursor) error {
		gotFromOffset = append(gotFromOffset, ev.EventID)
		return nil
	})
	if err != nil {
		t.Fatalf("iterate from cursor failed: %v", err)
	}
	if len(gotFromOffset) != 2 {
		t.Fatalf("iterate from cursor got %d, want 2", len(gotFromOffset))
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
