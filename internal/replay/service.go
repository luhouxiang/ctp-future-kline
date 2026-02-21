package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/order"
)

const (
	StatusIdle    = "idle"
	StatusRunning = "running"
	StatusPaused  = "paused"
	StatusStopped = "stopped"
	StatusDone    = "done"
	StatusError   = "error"
)

type StartRequest struct {
	Topics     []string        `json:"topics"`
	Sources    []string        `json:"sources"`
	StartTime  *time.Time      `json:"start_time"`
	EndTime    *time.Time      `json:"end_time"`
	Mode       string          `json:"mode"`
	Speed      float64         `json:"speed"`
	FromCursor *bus.FileCursor `json:"from_cursor"`
}

type TaskSnapshot struct {
	TaskID     string          `json:"task_id"`
	Status     string          `json:"status"`
	Mode       string          `json:"mode"`
	Speed      float64         `json:"speed"`
	Topics     []string        `json:"topics"`
	Sources    []string        `json:"sources"`
	StartTime  *time.Time      `json:"start_time"`
	EndTime    *time.Time      `json:"end_time"`
	FromCursor *bus.FileCursor `json:"from_cursor"`
	LastCursor *bus.FileCursor `json:"last_cursor"`
	Dispatched int64           `json:"dispatched"`
	Skipped    int64           `json:"skipped"`
	Errors     int64           `json:"errors"`
	LastError  string          `json:"last_error"`
	CreatedAt  time.Time       `json:"created_at"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
}

type ConsumerFunc func(ctx context.Context, ev bus.BusEvent) error

type Service struct {
	reader                  *bus.FileLog
	dedup                   *bus.ConsumerStore
	replayAllowOrderCommand bool

	mu        sync.Mutex
	activeID  string
	cancel    context.CancelFunc
	snapshot  TaskSnapshot
	consumers map[string]ConsumerFunc
}

func NewService(reader *bus.FileLog, dedup *bus.ConsumerStore, replayAllowOrderCommand bool) *Service {
	s := &Service{
		reader:                  reader,
		dedup:                   dedup,
		replayAllowOrderCommand: replayAllowOrderCommand,
		consumers:               make(map[string]ConsumerFunc),
		snapshot:                TaskSnapshot{Status: StatusIdle},
	}
	return s
}

func (s *Service) RegisterConsumer(id string, fn ConsumerFunc) {
	if id == "" || fn == nil {
		return
	}
	s.mu.Lock()
	s.consumers[id] = fn
	s.mu.Unlock()
}

func (s *Service) Start(req StartRequest) (TaskSnapshot, error) {
	mode := req.Mode
	if mode == "" {
		mode = "fast"
	}
	if mode != "fast" && mode != "realtime" {
		return TaskSnapshot{}, fmt.Errorf("invalid replay mode: %s", mode)
	}
	speed := req.Speed
	if speed == 0 {
		speed = 1.0
	}
	if speed <= 0 {
		return TaskSnapshot{}, fmt.Errorf("invalid replay speed: %v", speed)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeID != "" && (s.snapshot.Status == StatusRunning || s.snapshot.Status == StatusPaused) {
		return TaskSnapshot{}, fmt.Errorf("replay task already running")
	}
	taskID := bus.NewEventID()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.activeID = taskID
	s.snapshot = TaskSnapshot{
		TaskID:     taskID,
		Status:     StatusRunning,
		Mode:       mode,
		Speed:      speed,
		Topics:     append([]string(nil), req.Topics...),
		Sources:    append([]string(nil), req.Sources...),
		StartTime:  req.StartTime,
		EndTime:    req.EndTime,
		FromCursor: req.FromCursor,
		CreatedAt:  time.Now(),
		StartedAt:  time.Now(),
	}
	go s.run(ctx, taskID, req, mode, speed)
	return s.snapshot, nil
}

func (s *Service) Pause() (TaskSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Status != StatusRunning {
		return s.snapshot, fmt.Errorf("replay task not running")
	}
	s.snapshot.Status = StatusPaused
	return s.snapshot, nil
}

func (s *Service) Resume() (TaskSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Status != StatusPaused {
		return s.snapshot, fmt.Errorf("replay task not paused")
	}
	s.snapshot.Status = StatusRunning
	return s.snapshot, nil
}

func (s *Service) Stop() (TaskSnapshot, error) {
	s.mu.Lock()
	cancel := s.cancel
	if s.snapshot.Status != StatusRunning && s.snapshot.Status != StatusPaused {
		out := s.snapshot
		s.mu.Unlock()
		return out, fmt.Errorf("replay task not active")
	}
	s.snapshot.Status = StatusStopped
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return s.Status(), nil
}

func (s *Service) Status() TaskSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *Service) run(ctx context.Context, taskID string, req StartRequest, mode string, speed float64) {
	defer func() {
		s.mu.Lock()
		if s.snapshot.TaskID == taskID && s.snapshot.Status == StatusRunning {
			s.snapshot.Status = StatusDone
			s.snapshot.FinishedAt = time.Now()
		}
		s.activeID = ""
		s.cancel = nil
		s.mu.Unlock()
	}()

	opts := bus.ReadOptions{
		StartTime:  req.StartTime,
		EndTime:    req.EndTime,
		Topics:     bus.BuildSet(req.Topics),
		Sources:    bus.BuildSet(req.Sources),
		FromCursor: req.FromCursor,
	}
	var prevOccurred time.Time
	err := s.reader.Iterate(ctx, opts, func(iterCtx context.Context, ev bus.BusEvent, cursor bus.FileCursor) error {
		if err := s.waitIfPaused(iterCtx, taskID); err != nil {
			return err
		}
		if mode == "realtime" {
			if !prevOccurred.IsZero() && !ev.OccurredAt.IsZero() {
				delta := ev.OccurredAt.Sub(prevOccurred)
				if delta > 0 {
					wait := time.Duration(float64(delta) / speed)
					timer := time.NewTimer(wait)
					select {
					case <-iterCtx.Done():
						timer.Stop()
						return iterCtx.Err()
					case <-timer.C:
					}
				}
			}
			if !ev.OccurredAt.IsZero() {
				prevOccurred = ev.OccurredAt
			}
		}

		replayEvent := ev
		replayEvent.Replay = true
		replayEvent.ReplayTaskID = taskID

		dispatched, skipped, err := s.dispatch(iterCtx, replayEvent)
		s.mu.Lock()
		if s.snapshot.TaskID == taskID {
			s.snapshot.Dispatched += dispatched
			s.snapshot.Skipped += skipped
			cur := cursor
			s.snapshot.LastCursor = &cur
		}
		s.mu.Unlock()
		return err
	})
	if err != nil && err != context.Canceled {
		s.mu.Lock()
		if s.snapshot.TaskID == taskID {
			s.snapshot.Status = StatusError
			s.snapshot.LastError = err.Error()
			s.snapshot.Errors++
			s.snapshot.FinishedAt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	if s.snapshot.TaskID == taskID && s.snapshot.Status == StatusStopped {
		s.snapshot.FinishedAt = time.Now()
	}
	s.mu.Unlock()
}

func (s *Service) waitIfPaused(ctx context.Context, taskID string) error {
	for {
		s.mu.Lock()
		active := s.snapshot.TaskID == taskID
		status := s.snapshot.Status
		s.mu.Unlock()

		if !active || status == StatusStopped {
			return context.Canceled
		}
		if status != StatusPaused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Service) dispatch(ctx context.Context, ev bus.BusEvent) (int64, int64, error) {
	if ev.Topic == bus.TopicOrderCommand && !s.replayAllowOrderCommand {
		return 0, 1, nil
	}
	if ev.Topic == bus.TopicOrderCommand {
		ctx = order.WithReplayMeta(ctx, order.ReplayMeta{
			EventID:      ev.EventID,
			ReplayTaskID: ev.ReplayTaskID,
		})
	}

	s.mu.Lock()
	consumers := make(map[string]ConsumerFunc, len(s.consumers))
	for id, fn := range s.consumers {
		consumers[id] = fn
	}
	s.mu.Unlock()

	var dispatched int64
	var skipped int64
	for consumerID, fn := range consumers {
		if s.dedup != nil {
			first, err := s.dedup.MarkIfFirst(consumerID, ev.EventID)
			if err != nil {
				return dispatched, skipped, err
			}
			if !first {
				skipped++
				continue
			}
		}
		if err := fn(ctx, ev); err != nil {
			return dispatched, skipped, err
		}
		dispatched++
	}
	return dispatched, skipped, nil
}
