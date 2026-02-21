package replay_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/replay"
	_ "modernc.org/sqlite"
)

func TestReplayServiceFastDispatchAndDedup(t *testing.T) {
	t.Parallel()

	svc := newReplayService(t)
	var hits atomic.Int64
	svc.RegisterConsumer("consumer_1", func(_ context.Context, _ bus.BusEvent) error {
		hits.Add(1)
		return nil
	})

	task, err := svc.Start(replay.StartRequest{Mode: "fast"})
	if err != nil {
		t.Fatalf("start replay failed: %v", err)
	}
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	if got := hits.Load(); got != 2 {
		t.Fatalf("dispatch count = %d, want 2", got)
	}

	task, err = svc.Start(replay.StartRequest{Mode: "fast"})
	if err != nil {
		t.Fatalf("start replay second run failed: %v", err)
	}
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	if got := hits.Load(); got != 2 {
		t.Fatalf("dispatch count after second replay = %d, want still 2 due dedup", got)
	}
}

func TestReplayServicePauseResumeStop(t *testing.T) {
	t.Parallel()

	svc := newReplayService(t)
	task, err := svc.Start(replay.StartRequest{
		Mode:  "realtime",
		Speed: 0.01,
	})
	if err != nil {
		t.Fatalf("start replay failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if _, err := svc.Pause(); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	if snap := svc.Status(); snap.Status != replay.StatusPaused {
		t.Fatalf("status after pause = %s, want paused", snap.Status)
	}
	if _, err := svc.Resume(); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if _, err := svc.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	waitTaskFinished(t, svc, task.TaskID, 3*time.Second)
}

func newReplayService(t *testing.T) *replay.Service {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "bus")
	log := bus.NewFileLog(dir, 0)
	t.Cleanup(func() { _ = log.Close() })

	now := time.Now()
	_, _ = log.Append(bus.BusEvent{
		EventID:    "ev-1",
		Topic:      bus.TopicTick,
		Source:     "test",
		OccurredAt: now,
		Payload:    mustJSON(t, map[string]any{"n": 1}),
	})
	_, _ = log.Append(bus.BusEvent{
		EventID:    "ev-2",
		Topic:      bus.TopicBar,
		Source:     "test",
		OccurredAt: now.Add(1 * time.Second),
		Payload:    mustJSON(t, map[string]any{"n": 2}),
	})

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "dedup.db"))
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := bus.NewConsumerStore(db)
	if err != nil {
		t.Fatalf("new consumer store failed: %v", err)
	}
	return replay.NewService(log, store, true)
}

func waitTaskDone(t *testing.T, svc *replay.Service, taskID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snap := svc.Status()
		if snap.TaskID == taskID && (snap.Status == replay.StatusDone || snap.Status == replay.StatusError || snap.Status == replay.StatusStopped) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("task %s not done in %s", taskID, timeout)
}

func waitTaskFinished(t *testing.T, svc *replay.Service, taskID string, timeout time.Duration) {
	t.Helper()
	waitTaskDone(t, svc, taskID, timeout)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
