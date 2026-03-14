package replay

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/logger"
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

// StartRequest 描述一次 replay 任务的输入范围与推进方式。
// 它既支持从 bus 日志读取，也支持从 tick_dir 直接读取 CSV。
type StartRequest struct {
	Topics     []string        `json:"topics"`
	Sources    []string        `json:"sources"`
	StartTime  *time.Time      `json:"start_time"`
	EndTime    *time.Time      `json:"end_time"`
	Mode       string          `json:"mode"`
	Speed      float64         `json:"speed"`
	FromCursor *bus.FileCursor `json:"from_cursor"`
	TickDir    string          `json:"tick_dir"`
	FullReplay bool            `json:"full_replay"`
}

// TaskSnapshot 是 replay 任务的可观测状态快照。
// Web 接口和调试日志都依赖它向外暴露当前推进位置与统计信息。
type TaskSnapshot struct {
	TaskID              string          `json:"task_id"`
	Status              string          `json:"status"`
	Mode                string          `json:"mode"`
	Speed               float64         `json:"speed"`
	Topics              []string        `json:"topics"`
	Sources             []string        `json:"sources"`
	StartTime           *time.Time      `json:"start_time"`
	EndTime             *time.Time      `json:"end_time"`
	FromCursor          *bus.FileCursor `json:"from_cursor"`
	LastCursor          *bus.FileCursor `json:"last_cursor"`
	TickDir             string          `json:"tick_dir"`
	TickFiles           int             `json:"tick_files"`
	Instruments         int             `json:"instruments"`
	TotalTicks          int64           `json:"total_ticks"`
	ProcessedTicks      int64           `json:"processed_ticks"`
	CurrentInstrumentID string          `json:"current_instrument_id"`
	CurrentSimTime      *time.Time      `json:"current_sim_time"`
	FirstSimTime        *time.Time      `json:"first_sim_time"`
	LastSimTime         *time.Time      `json:"last_sim_time"`
	Dispatched          int64           `json:"dispatched"`
	Skipped             int64           `json:"skipped"`
	Errors              int64           `json:"errors"`
	LastError           string          `json:"last_error"`
	CreatedAt           time.Time       `json:"created_at"`
	StartedAt           time.Time       `json:"started_at"`
	FinishedAt          time.Time       `json:"finished_at"`
}

type ConsumerFunc func(ctx context.Context, ev bus.BusEvent) error
type TaskLifecycle interface {
	OnTaskFinished(ctx context.Context, snap TaskSnapshot) error
}

// Service 是回放调度中心。
//
// 它管理：
// 1. 事件来源：bus 文件日志或 tick CSV 目录
// 2. 任务生命周期：启动、暂停、恢复、停止
// 3. consumer 分发：把每条事件投递给注册的消费方
// 4. 去重存储：避免同一 consumer 重复消费同一事件
type Service struct {
	reader                  *bus.FileLog
	dedup                   *bus.ConsumerStore
	replayAllowOrderCommand bool

	mu        sync.Mutex
	activeID  string
	cancel    context.CancelFunc
	snapshot  TaskSnapshot
	consumers map[string]ConsumerFunc
	hooks     map[string]TaskLifecycle
}

// NewService 初始化 replay service。
func NewService(reader *bus.FileLog, dedup *bus.ConsumerStore, replayAllowOrderCommand bool) *Service {
	s := &Service{
		reader:                  reader,
		dedup:                   dedup,
		replayAllowOrderCommand: replayAllowOrderCommand,
		consumers:               make(map[string]ConsumerFunc),
		hooks:                   make(map[string]TaskLifecycle),
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

func (s *Service) RegisterTaskLifecycle(id string, hook TaskLifecycle) {
	if id == "" || hook == nil {
		return
	}
	s.mu.Lock()
	s.hooks[id] = hook
	s.mu.Unlock()
}

// Start 启动一次新的 replay 任务。
// 如果当前已有运行中的任务，会直接拒绝。
func (s *Service) Start(req StartRequest) (TaskSnapshot, error) {
	req = normalizeStartRequest(req)
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
	if req.FullReplay && s.dedup != nil {
		if err := s.dedup.ClearAll(); err != nil {
			return TaskSnapshot{}, err
		}
		logger.Info("replay dedup records cleared before full replay")
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
		TickDir:    req.TickDir,
		CreatedAt:  time.Now(),
		StartedAt:  time.Now(),
	}
	go s.run(ctx, taskID, req, mode, speed)
	return s.snapshot, nil
}

// normalizeStartRequest 在启动前补齐 sources/topics 等默认值。
func normalizeStartRequest(req StartRequest) StartRequest {
	if strings.TrimSpace(req.TickDir) == "" {
		return req
	}
	req.Topics = normalizeList(req.Topics)
	req.Sources = ensureReplayTickCSVSource(req.Sources)
	return req
}

func normalizeList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, item := range values {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func ensureReplayTickCSVSource(values []string) []string {
	values = normalizeList(values)
	for _, item := range values {
		if item == tickCSVSource {
			return values
		}
	}
	return append(values, tickCSVSource)
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

// run 是 replay 主循环入口。
// 当配置了 tick_dir 时，实际回放逻辑会转交给 runTickDir；否则从 bus 日志迭代。
func (s *Service) run(ctx context.Context, taskID string, req StartRequest, mode string, speed float64) {
	defer func() {
		s.mu.Lock()
		if s.snapshot.TaskID == taskID && s.snapshot.Status == StatusRunning {
			s.snapshot.Status = StatusDone
			s.snapshot.FinishedAt = time.Now()
		}
		snapshot := s.snapshot
		hooks := make([]TaskLifecycle, 0, len(s.hooks))
		for _, hook := range s.hooks {
			hooks = append(hooks, hook)
		}
		s.activeID = ""
		s.cancel = nil
		s.mu.Unlock()
		for _, hook := range hooks {
			_ = hook.OnTaskFinished(context.Background(), snapshot)
		}
	}()

	if req.TickDir != "" {
		s.runTickDir(ctx, taskID, req, mode, speed)
		return
	}

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

// waitIfPaused 在每条事件处理前检查任务是否暂停或停止。
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
	// dispatch 负责把“当前回放到的一条事件”投递给所有已注册的 consumer。
	//
	// 返回值语义：
	// 1. dispatched: 本次实际成功调用了多少个 consumer
	// 2. skipped: 因为回放策略或 consumer 去重而被跳过了多少次投递
	// 3. error: 任意一个 consumer 执行失败，或去重存储失败时返回错误并中断本次分发
	//
	// 注意这里的统计口径是“按 consumer 次数”而不是“按 event 次数”：
	// 如果同一条 event 注册了 3 个 consumer，并且都成功收到，那么 dispatched=3。

	// 对回放中的下单指令做单独保护。
	// 当配置禁止 replay 发送订单命令时，直接把该事件视为一次“跳过”，
	// 既不投递给任何 consumer，也不报错。
	if ev.Topic == bus.TopicOrderCommand && !s.replayAllowOrderCommand {
		logger.Debug(
			"replay dispatch skipped by order command guard",
			"task_id", ev.ReplayTaskID,
			"event_id", ev.EventID,
			"topic", ev.Topic,
			"source", ev.Source,
		)
		return 0, 1, nil
	}

	// 允许回放订单命令时，把 replay 元信息挂进 context。
	// 后续 consumer 如果需要知道“这是一笔回放订单”以及它来自哪个 replay task，
	// 就可以从 context 中读取，而不必额外修改 consumer 函数签名。
	if ev.Topic == bus.TopicOrderCommand {
		ctx = order.WithReplayMeta(ctx, order.ReplayMeta{
			EventID:      ev.EventID,
			ReplayTaskID: ev.ReplayTaskID,
		})
	}

	// 先在锁内拷贝一份当前 consumer 快照，再在锁外执行真正的分发。
	// 这样做有两个目的：
	// 1. 避免分发过程中长期持锁，影响 RegisterConsumer/RegisterTaskLifecycle 等操作
	// 2. 避免某个 consumer 执行缓慢时阻塞整个 replay service 的其它状态操作
	s.mu.Lock()
	consumers := make(map[string]ConsumerFunc, len(s.consumers))
	for id, fn := range s.consumers {
		consumers[id] = fn
	}
	s.mu.Unlock()

	var dispatched int64
	var skipped int64
	for consumerID, fn := range consumers {
		// 逐个 consumer 投递，并在 debug 日志里保留完整定位信息，
		// 方便排查“事件有没有发出去、发给了谁、在哪一层被跳过或失败”。
		logger.Debug(
			"replay dispatch begin",
			"task_id", ev.ReplayTaskID,
			"event_id", ev.EventID,
			"topic", ev.Topic,
			"source", ev.Source,
			"consumer_id", consumerID,
			"occurred_at", ev.OccurredAt.Format(time.RFC3339Nano),
			"produced_at", ev.ProducedAt.Format(time.RFC3339Nano),
		)
		if s.dedup != nil {
			// 去重粒度是“consumerID + eventID”。
			// 也就是说：
			// 1. 同一条 event 对同一个 consumer 只会成功投递一次
			// 2. 同一条 event 仍然可以分别投递给不同 consumer
			//
			// 这能保证 replay 重试、断点续播或重复读取日志时，不会让同一个 consumer
			// 对同一 event 重复产生副作用（例如重复写库、重复下单、重复更新状态）。
			first, err := s.dedup.MarkIfFirst(consumerID, ev.EventID)
			if err != nil {
				logger.Error(
					"replay dispatch dedup failed",
					"task_id", ev.ReplayTaskID,
					"event_id", ev.EventID,
					"consumer_id", consumerID,
					"error", err,
				)
				return dispatched, skipped, err
			}
			if !first {
				// 已经投递过的 consumer 不再重复执行，记入 skipped。
				logger.Debug(
					"replay dispatch skipped by consumer dedup",
					"task_id", ev.ReplayTaskID,
					"event_id", ev.EventID,
					"topic", ev.Topic,
					"consumer_id", consumerID,
				)
				skipped++
				continue
			}
		}

		// 真正调用 consumer。
		// 这里只要有一个 consumer 返回错误，就立即中止本次事件的后续分发，
		// 并把错误上传给 replay 主循环，由上层把任务状态标记为 error。
		if err := fn(ctx, ev); err != nil {
			logger.Error(
				"replay dispatch consumer failed",
				"task_id", ev.ReplayTaskID,
				"event_id", ev.EventID,
				"topic", ev.Topic,
				"consumer_id", consumerID,
				"error", err,
			)
			return dispatched, skipped, err
		}

		// 只有 consumer 实际执行成功，才计入 dispatched。
		logger.Debug(
			"replay dispatch delivered",
			"task_id", ev.ReplayTaskID,
			"event_id", ev.EventID,
			"topic", ev.Topic,
			"consumer_id", consumerID,
		)
		dispatched++
	}
	return dispatched, skipped, nil
}
