package replay_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/replay"
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

func TestReplayServiceTickDirRealtimeDispatchAndCursor(t *testing.T) {
	t.Parallel()

	svc := newReplayService(t)
	tickDir := filepath.Join(t.TempDir(), "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeTickCSV(t, filepath.Join(tickDir, "au2506.csv"), []string{
		"2026-03-01 09:00:00.000,au2506,SHFE,20260303,20260301,09:00:00,100.1,1,10,99,100,100.2,0",
		"2026-03-01 09:00:02.000,au2506,SHFE,20260303,20260301,09:00:02,100.3,2,11,99,100.2,100.4,0",
	})
	writeTickCSV(t, filepath.Join(tickDir, "ag2506.csv"), []string{
		"2026-03-01 09:00:01.000,ag2506,SHFE,20260303,20260301,09:00:01,80.1,3,12,79,80,80.2,0",
	})

	var mu sync.Mutex
	got := make([]string, 0, 3)
	svc.RegisterConsumer("tick_consumer", func(_ context.Context, ev bus.BusEvent) error {
		if ev.Topic != bus.TopicTick {
			return nil
		}
		var payload map[string]any
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			return err
		}
		mu.Lock()
		got = append(got, strings.ToLower(payload["InstrumentID"].(string)))
		mu.Unlock()
		return nil
	})

	task, err := svc.Start(replay.StartRequest{
		Mode:    "realtime",
		Speed:   1000,
		TickDir: tickDir,
	})
	if err != nil {
		t.Fatalf("start tick dir replay failed: %v", err)
	}
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	mu.Lock()
	if !reflect.DeepEqual(got, []string{"au2506", "ag2506", "au2506"}) {
		mu.Unlock()
		t.Fatalf("tick replay order = %v, want [au2506 ag2506 au2506]", got)
	}
	mu.Unlock()
	snap := svc.Status()
	if snap.TickDir != tickDir {
		t.Fatalf("TickDir = %q, want %q", snap.TickDir, tickDir)
	}
	if snap.TickFiles != 2 {
		t.Fatalf("TickFiles = %d, want 2", snap.TickFiles)
	}
	if snap.Instruments != 2 {
		t.Fatalf("Instruments = %d, want 2", snap.Instruments)
	}
	if snap.TotalTicks != 3 {
		t.Fatalf("TotalTicks = %d, want 3", snap.TotalTicks)
	}
	if snap.ProcessedTicks != 3 {
		t.Fatalf("ProcessedTicks = %d, want 3", snap.ProcessedTicks)
	}
	if snap.CurrentInstrumentID != "au2506" {
		t.Fatalf("CurrentInstrumentID = %q, want au2506", snap.CurrentInstrumentID)
	}
	if snap.CurrentSimTime == nil || snap.CurrentSimTime.Format("2006-01-02 15:04:05") != "2026-03-01 09:00:02" {
		t.Fatalf("CurrentSimTime = %v, want 2026-03-01 09:00:02", snap.CurrentSimTime)
	}
	if snap.FirstSimTime == nil || snap.FirstSimTime.Format("2006-01-02 15:04:05") != "2026-03-01 09:00:00" {
		t.Fatalf("FirstSimTime = %v, want 2026-03-01 09:00:00", snap.FirstSimTime)
	}
	if snap.LastSimTime == nil || snap.LastSimTime.Format("2006-01-02 15:04:05") != "2026-03-01 09:00:02" {
		t.Fatalf("LastSimTime = %v, want 2026-03-01 09:00:02", snap.LastSimTime)
	}
	if snap.LastCursor == nil || snap.LastCursor.File != "au2506.csv" || snap.LastCursor.Offset != 3 {
		t.Fatalf("LastCursor = %+v, want au2506.csv@3", snap.LastCursor)
	}

	mu.Lock()
	got = got[:0]
	mu.Unlock()
	task, err = svc.Start(replay.StartRequest{
		Mode:    "fast",
		TickDir: tickDir,
		FromCursor: &bus.FileCursor{
			File:   "au2506.csv",
			Offset: 3,
		},
	})
	if err != nil {
		t.Fatalf("start tick dir replay with cursor failed: %v", err)
	}
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	mu.Lock()
	if len(got) != 0 {
		mu.Unlock()
		t.Fatalf("dispatch count after second replay = %d, want 0 due dedup", len(got))
	}
	mu.Unlock()
	snap = svc.Status()
	if snap.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", snap.Skipped)
	}
}

func TestReplayServiceTickDirIgnoresWrongSourcesByAutoIncludingTickCSV(t *testing.T) {
	t.Parallel()

	svc := newReplayService(t)
	tickDir := filepath.Join(t.TempDir(), "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeTickCSV(t, filepath.Join(tickDir, "rb2505.csv"), []string{
		"2026-03-01 09:00:00.000,rb2505,SHFE,20260303,20260301,09:00:00,100.1,1,10,99,100,100.2,0",
	})

	var hits atomic.Int64
	svc.RegisterConsumer("tick_consumer", func(_ context.Context, ev bus.BusEvent) error {
		if ev.Topic == bus.TopicTick {
			hits.Add(1)
		}
		return nil
	})

	task, err := svc.Start(replay.StartRequest{
		Mode:    "fast",
		TickDir: tickDir,
		Sources: []string{"quotes.md"},
	})
	if err != nil {
		t.Fatalf("start tick dir replay failed: %v", err)
	}
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	if got := hits.Load(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1", got)
	}
	if snap := svc.Status(); len(snap.Sources) == 0 || snap.Sources[len(snap.Sources)-1] != "replay.tickcsv" {
		t.Fatalf("Sources = %v, want replay.tickcsv included", snap.Sources)
	}
}

func TestReplayServiceStartWithPrepareReturnsBeforePrepareCompletes(t *testing.T) {
	t.Parallel()

	svc := newReplayService(t)
	prepareStarted := make(chan struct{})
	allowPrepare := make(chan struct{})
	var hits atomic.Int64
	svc.RegisterConsumer("consumer_1", func(_ context.Context, _ bus.BusEvent) error {
		hits.Add(1)
		return nil
	})

	task, err := svc.StartWithPrepare(replay.StartRequest{Mode: "fast"}, []replay.StartPrepareFunc{
		func(ctx context.Context, _ replay.StartRequest) error {
			close(prepareStarted)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-allowPrepare:
				return nil
			}
		},
	})
	if err != nil {
		t.Fatalf("start replay failed: %v", err)
	}
	if task.Status != replay.StatusRunning {
		t.Fatalf("status after start = %s, want running", task.Status)
	}
	select {
	case <-prepareStarted:
	case <-time.After(time.Second):
		t.Fatal("prepare did not start")
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("dispatch before prepare completed = %d, want 0", got)
	}

	close(allowPrepare)
	waitTaskDone(t, svc, task.TaskID, 3*time.Second)
	if got := hits.Load(); got != 2 {
		t.Fatalf("dispatch after prepare completed = %d, want 2", got)
	}
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

func writeTickCSV(t *testing.T, path string, lines []string) {
	t.Helper()
	content := "received_at,instrument_id,exchange_id,trading_day,action_day,update_time,last_price,volume,open_interest,settlement_price,bid_price1,ask_price1,update_millisec\n" + strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write tick csv failed: %v", err)
	}
}
