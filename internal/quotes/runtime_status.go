// runtime_status.go 提供行情运行时的统一状态中心。
// 所有连接状态、订阅状态、tick 延迟、队列深度、去重计数等指标都汇总到这里，
// 再由 /api/status 和 WebSocket 对外暴露。
package quotes

import (
	"sync"
	"time"
)

const (
	RuntimeStateIdle     = "idle"
	RuntimeStateStarting = "starting"
	RuntimeStateRunning  = "running"
	RuntimeStateError    = "error"
)

type RuntimeSnapshot struct {
	// State 描述行情主链路的整体状态，如 idle、starting、running、error。
	State string `json:"state"`
	// QueryFront 表示查询前置是否已连通。
	QueryFront bool `json:"query_front"`
	// QueryLogin 表示查询会话是否已登录成功。
	QueryLogin bool `json:"query_login"`
	// MdFront 表示行情前置是否已连通。
	MdFront bool `json:"md_front"`
	// MdLogin 表示行情会话是否已登录成功。
	MdLogin bool `json:"md_login"`
	// MdSubscribed 表示行情订阅是否已发起且处于有效状态。
	MdSubscribed bool `json:"md_subscribed"`
	// MdFrontDown 表示最近一次状态是否为行情前置断开。
	MdFrontDown bool `json:"md_front_disconnected"`
	// MdDownReason 保存行情断开时的 CTP reason code。
	MdDownReason int `json:"md_disconnect_reason"`
	// MdReconnectTry 是当前或最近一次自动重连尝试次数。
	MdReconnectTry int `json:"md_reconnect_attempt"`
	// MdNextRetryAt 是预计下一次重连时间。
	MdNextRetryAt time.Time `json:"md_next_retry_at"`
	// NetworkSuspect 表示虽然连接还在，但系统怀疑链路已卡住或无行情。
	NetworkSuspect bool `json:"network_suspect"`
	// TickDedupDrop 是重复 tick 被丢弃的累计次数。
	TickDedupDrop int64 `json:"tick_dedup_dropped"`
	// DriftSeconds 是最近一次 tick 业务时间与本地时间的偏移秒数。
	DriftSeconds float64 `json:"drift_seconds"`
	// DriftPaused 表示系统当前是否处于时间漂移暂停告警状态。
	DriftPaused bool `json:"drift_paused"`
	// DriftPauseCnt 是累计进入漂移暂停状态的次数。
	DriftPauseCnt int64 `json:"drift_pause_count"`
	// LastDriftInstrument 是最近一次产生漂移指标的合约。
	LastDriftInstrument string `json:"last_drift_instrument"`
	// LastDriftAt 是最近一次刷新漂移指标的时间。
	LastDriftAt time.Time `json:"last_drift_at"`
	// UpstreamLagMS 是最近一次 tick 从业务时间到本地接收时间的延迟。
	UpstreamLagMS float64 `json:"upstream_lag_ms"`
	// CallbackToProcMS 是 CTP 回调到进入处理逻辑之间的延迟。
	CallbackToProcMS float64 `json:"callback_to_process_ms"`
	// LockWaitMS 是处理链路等待关键锁的耗时。
	LockWaitMS float64 `json:"lock_wait_ms"`
	// SideEffectTickQueueMS 是 tick 旁路事件排队耗时。
	SideEffectTickQueueMS float64 `json:"side_effect_tick_queue_ms"`
	// SideEffectBarQueueMS 是 bar 旁路事件排队耗时。
	SideEffectBarQueueMS float64 `json:"side_effect_bar_queue_ms"`
	// MinuteStoreMS 是分钟线单次写入存储耗时。
	MinuteStoreMS float64 `json:"minute_store_ms"`
	// MMQueueMS 是多周期聚合任务进入队列前的等待耗时。
	MMQueueMS float64 `json:"mm_queue_ms"`
	// MMRunMS 是多周期聚合实际执行耗时。
	MMRunMS float64 `json:"mm_run_ms"`
	// RouterQueueMS 是 tick 从入口到分片路由入队的耗时。
	RouterQueueMS float64 `json:"router_queue_ms"`
	// ShardQueueMS 是 tick 在 shard 内排队等待处理的耗时。
	ShardQueueMS float64 `json:"shard_queue_ms"`
	// PersistQueueMS 是持久化任务在 DB writer 中排队的耗时。
	PersistQueueMS float64 `json:"persist_queue_ms"`
	// EndToEndMS 是 tick 从接收到最终落库的总耗时。
	EndToEndMS float64 `json:"end_to_end_ms"`
	// DBFlushMS 是最近一批数据库 flush 的耗时。
	DBFlushMS float64 `json:"db_flush_ms"`
	// DBFlushRows 是最近一批数据库 flush 的行数。
	DBFlushRows int `json:"db_flush_rows"`
	// DBQueueDepth 是当前 DB writer 队列深度。
	DBQueueDepth int `json:"db_queue_depth"`
	// FileFlushMS 是最近一次文件刷盘耗时。
	FileFlushMS float64 `json:"file_flush_ms"`
	// FileQueueDepth 是当前文件写队列深度。
	FileQueueDepth int `json:"file_queue_depth"`
	// ShardBacklog 展示每个行情 shard 当前积压量。
	ShardBacklog []int `json:"shard_backlog"`
	// DroppedTicks 是因为队列打满等原因被直接丢弃的 tick 数。
	DroppedTicks int64 `json:"dropped_ticks"`
	// LateTicks 是时间漂移过大的 tick 计数。
	LateTicks int64 `json:"late_ticks"`
	// Goroutines 是当前进程 goroutine 数，用于观察运行压力。
	Goroutines int `json:"goroutines"`
	// LastLatencyInstrument 是最近一次刷新延迟指标时对应的合约。
	LastLatencyInstrument string `json:"last_latency_instrument"`
	// LastLatencyStage 是最近一次刷新延迟指标的阶段名称。
	LastLatencyStage string `json:"last_latency_stage"`
	// ServerTime 是对前端展示的服务端参考时间。
	ServerTime string `json:"server_time"`
	// LastTickTime 是最近一次成功进入处理链路的 tick 接收时间。
	LastTickTime time.Time `json:"last_tick_time"`
	// IsMarketOpen 是根据最近 tick 活跃度推断出的市场是否开市。
	IsMarketOpen bool `json:"is_market_open"`
	// LastError 保存主链路最近一次错误信息。
	LastError string `json:"last_error"`
	// UpdatedAt 是快照最后更新时间。
	UpdatedAt time.Time `json:"updated_at"`
	// TradingDay 是当前识别到的交易日。
	TradingDay string `json:"trading_day"`
	// SubscribeCount 是当前成功订阅的合约数量。
	SubscribeCount int `json:"subscribe_count"`
}

type RuntimeStatusCenter struct {
	// mu 保护 snapshot 和订阅者集合的并发读写。
	mu sync.RWMutex
	// marketOpenStale 是最近 tick 超过多久后判定市场不再活跃的阈值。
	marketOpenStale time.Duration
	// snapshot 保存当前最新的行情运行状态快照。
	snapshot RuntimeSnapshot
	// subscribers 保存订阅状态更新的监听者。
	subscribers map[chan RuntimeSnapshot]struct{}
	// subscriberBufSize 是状态订阅通道的缓冲大小。
	subscriberBufSize int
}

func NewRuntimeStatusCenter(marketOpenStale time.Duration) *RuntimeStatusCenter {
	return &RuntimeStatusCenter{
		marketOpenStale:   marketOpenStale,
		snapshot:          RuntimeSnapshot{State: RuntimeStateIdle, UpdatedAt: time.Now()},
		subscribers:       make(map[chan RuntimeSnapshot]struct{}),
		subscriberBufSize: 16,
	}
}

func (c *RuntimeStatusCenter) Snapshot(now time.Time) RuntimeSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.snapshot
	if out.LastTickTime.IsZero() {
		out.IsMarketOpen = false
		return out
	}
	out.IsMarketOpen = now.Sub(out.LastTickTime) <= c.marketOpenStale
	return out
}

func (c *RuntimeStatusCenter) TradingDay() string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshot.TradingDay
}

func (c *RuntimeStatusCenter) Subscribe() (<-chan RuntimeSnapshot, func()) {
	ch := make(chan RuntimeSnapshot, c.subscriberBufSize)

	c.mu.Lock()
	c.subscribers[ch] = struct{}{}
	current := c.snapshot
	c.mu.Unlock()

	ch <- current

	cancel := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.subscribers[ch]; ok {
			delete(c.subscribers, ch)
			close(ch)
		}
	}
	return ch, cancel
}

func (c *RuntimeStatusCenter) SetStarting() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateStarting
		s.LastError = ""
	})
}

func (c *RuntimeStatusCenter) SetRunning() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateRunning
		s.LastError = ""
	})
}

func (c *RuntimeStatusCenter) SetError(err error) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateError
		if err != nil {
			s.LastError = err.Error()
		}
	})
}

func (c *RuntimeStatusCenter) MarkQueryFrontConnected() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.QueryFront = true
	})
}

func (c *RuntimeStatusCenter) MarkQueryLogin(loginTime string, tradingDay string) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.QueryLogin = true
		if loginTime != "" {
			s.ServerTime = loginTime
		}
		if tradingDay != "" {
			s.TradingDay = tradingDay
		}
	})
}

func (c *RuntimeStatusCenter) MarkMdFrontConnected() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdFront = true
		s.MdFrontDown = false
		s.MdDownReason = 0
	})
}

func (c *RuntimeStatusCenter) MarkMdLogin(loginTime string, tradingDay string) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdLogin = true
		s.MdFrontDown = false
		s.MdDownReason = 0
		s.MdReconnectTry = 0
		s.MdNextRetryAt = time.Time{}
		if loginTime != "" {
			s.ServerTime = loginTime
		}
		if tradingDay != "" {
			s.TradingDay = tradingDay
		}
	})
}

func (c *RuntimeStatusCenter) MarkSubscribed(count int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdSubscribed = true
		s.SubscribeCount = count
		s.NetworkSuspect = false
	})
}

func (c *RuntimeStatusCenter) MarkTick(ts time.Time) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.LastTickTime = ts
		s.ServerTime = ts.Format("2006-01-02 15:04:05")
		s.NetworkSuspect = false
	})
}

func (c *RuntimeStatusCenter) MarkMdFrontDisconnected(reason int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdFront = false
		s.MdLogin = false
		s.MdSubscribed = false
		s.MdFrontDown = true
		s.MdDownReason = reason
	})
}

func (c *RuntimeStatusCenter) MarkMdReconnectAttempt(attempt int, nextRetryAt time.Time) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdReconnectTry = attempt
		s.MdNextRetryAt = nextRetryAt
	})
}

func (c *RuntimeStatusCenter) MarkNetworkSuspect(v bool) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.NetworkSuspect = v
	})
}

func (c *RuntimeStatusCenter) MarkTickDedupDropped() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TickDedupDrop++
	})
}

func (c *RuntimeStatusCenter) MarkDrift(instrumentID string, driftSec float64, paused bool) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DriftSeconds = driftSec
		s.LastDriftInstrument = instrumentID
		s.LastDriftAt = time.Now()
		wasPaused := s.DriftPaused
		s.DriftPaused = paused
		if paused && !wasPaused {
			s.DriftPauseCnt++
		}
	})
}

func (c *RuntimeStatusCenter) MarkDriftRecovered() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DriftPaused = false
		s.DriftSeconds = 0
	})
}

func (c *RuntimeStatusCenter) MarkTickPipelineLatency(instrumentID string, upstreamLagMS float64, callbackToProcMS float64, lockWaitMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.UpstreamLagMS = upstreamLagMS
		s.CallbackToProcMS = callbackToProcMS
		s.LockWaitMS = lockWaitMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "tick_pipeline"
	})
}

func (c *RuntimeStatusCenter) MarkSideEffectLatency(kind string, instrumentID string, queueMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		if kind == "tick" {
			s.SideEffectTickQueueMS = queueMS
		} else if kind == "bar" {
			s.SideEffectBarQueueMS = queueMS
		}
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "side_effect_" + kind
	})
}

func (c *RuntimeStatusCenter) MarkMinuteStoreLatency(instrumentID string, storeMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MinuteStoreMS = storeMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "minute_store"
	})
}

func (c *RuntimeStatusCenter) MarkMMRebuildLatency(instrumentID string, queueMS float64, runMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MMQueueMS = queueMS
		s.MMRunMS = runMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "mm_rebuild"
	})
}

func (c *RuntimeStatusCenter) MarkRuntimeQueues(shardBacklog []int, dbQueueDepth int, _ int64, goroutines int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.ShardBacklog = append([]int(nil), shardBacklog...)
		s.DBQueueDepth = dbQueueDepth
		s.Goroutines = goroutines
	})
}

func (c *RuntimeStatusCenter) MarkRouterLatency(instrumentID string, queueMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.RouterQueueMS = queueMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "router_queue"
	})
}

func (c *RuntimeStatusCenter) MarkShardLatency(instrumentID string, shardID int, queueMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.ShardQueueMS = queueMS
		if shardID >= 0 && shardID < len(s.ShardBacklog) {
			_ = shardID
		}
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "shard_queue"
	})
}

func (c *RuntimeStatusCenter) MarkUpstreamLag(instrumentID string, upstreamLagMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.UpstreamLagMS = upstreamLagMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "upstream_lag"
	})
}

func (c *RuntimeStatusCenter) MarkPersistLatency(instrumentID string, queueMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.PersistQueueMS = queueMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "persist_queue"
	})
}

func (c *RuntimeStatusCenter) MarkEndToEndLatency(instrumentID string, totalMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.EndToEndMS = totalMS
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "end_to_end"
	})
}

func (c *RuntimeStatusCenter) MarkDBFlush(rows int, flushMS float64, queueDepth int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DBFlushRows = rows
		s.DBFlushMS = flushMS
		s.DBQueueDepth = queueDepth
		s.LastLatencyStage = "db_flush"
	})
}

func (c *RuntimeStatusCenter) MarkFileFlush(flushMS int, queueDepth int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.FileFlushMS = float64(flushMS)
		s.FileQueueDepth = queueDepth
		s.LastLatencyStage = "file_flush"
	})
}

func (c *RuntimeStatusCenter) MarkTickDropped() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DroppedTicks++
	})
}

func (c *RuntimeStatusCenter) MarkLateTick() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.LateTicks++
	})
}

func (c *RuntimeStatusCenter) mutate(fn func(*RuntimeSnapshot)) {
	c.mu.Lock()
	fn(&c.snapshot)
	c.snapshot.UpdatedAt = time.Now()
	out := c.snapshot
	if !out.LastTickTime.IsZero() {
		out.IsMarketOpen = time.Since(out.LastTickTime) <= c.marketOpenStale
	} else {
		out.IsMarketOpen = false
	}
	subs := make([]chan RuntimeSnapshot, 0, len(c.subscribers))
	for ch := range c.subscribers {
		subs = append(subs, ch)
	}
	c.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- out:
		default:
		}
	}
}
