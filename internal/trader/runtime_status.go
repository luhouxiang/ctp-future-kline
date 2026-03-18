package trader

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
	State                 string    `json:"state"`
	TraderFront           bool      `json:"trader_front"`
	TraderLogin           bool      `json:"trader_login"`
	MdFront               bool      `json:"md_front"`
	MdLogin               bool      `json:"md_login"`
	MdSubscribed          bool      `json:"md_subscribed"`
	MdFrontDown           bool      `json:"md_front_disconnected"`
	MdDownReason          int       `json:"md_disconnect_reason"`
	MdReconnectTry        int       `json:"md_reconnect_attempt"`
	MdNextRetryAt         time.Time `json:"md_next_retry_at"`
	NetworkSuspect        bool      `json:"network_suspect"`
	TickDedupDrop         int64     `json:"tick_dedup_dropped"`
	DriftSeconds          float64   `json:"drift_seconds"`
	DriftPaused           bool      `json:"drift_paused"`
	DriftPauseCnt         int64     `json:"drift_pause_count"`
	UpstreamLagMS         float64   `json:"upstream_lag_ms"`
	CallbackToProcMS      float64   `json:"callback_to_process_ms"`
	LockWaitMS            float64   `json:"lock_wait_ms"`
	SideEffectTickQueueMS float64   `json:"side_effect_tick_queue_ms"`
	SideEffectBarQueueMS  float64   `json:"side_effect_bar_queue_ms"`
	MinuteStoreMS         float64   `json:"minute_store_ms"`
	MMQueueMS             float64   `json:"mm_queue_ms"`
	MMRunMS               float64   `json:"mm_run_ms"`
	RouterQueueMS         float64   `json:"router_queue_ms"`
	ShardQueueMS          float64   `json:"shard_queue_ms"`
	PersistQueueMS        float64   `json:"persist_queue_ms"`
	EndToEndMS            float64   `json:"end_to_end_ms"`
	DBFlushMS             float64   `json:"db_flush_ms"`
	DBFlushRows           int       `json:"db_flush_rows"`
	DBQueueDepth          int       `json:"db_queue_depth"`
	FileFlushMS           float64   `json:"file_flush_ms"`
	FileQueueDepth        int       `json:"file_queue_depth"`
	ShardBacklog          []int     `json:"shard_backlog"`
	DroppedTicks          int64     `json:"dropped_ticks"`
	LateTicks             int64     `json:"late_ticks"`
	Goroutines            int       `json:"goroutines"`
	LastLatencyInstrument string    `json:"last_latency_instrument"`
	LastLatencyStage      string    `json:"last_latency_stage"`
	ServerTime            string    `json:"server_time"`
	LastTickTime          time.Time `json:"last_tick_time"`
	IsMarketOpen          bool      `json:"is_market_open"`
	LastError             string    `json:"last_error"`
	UpdatedAt             time.Time `json:"updated_at"`
	TradingDay            string    `json:"trading_day"`
	SubscribeCount        int       `json:"subscribe_count"`
}

type RuntimeStatusCenter struct {
	mu                sync.RWMutex
	marketOpenStale   time.Duration
	snapshot          RuntimeSnapshot
	subscribers       map[chan RuntimeSnapshot]struct{}
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

func (c *RuntimeStatusCenter) MarkTraderFrontConnected() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TraderFront = true
	})
}

func (c *RuntimeStatusCenter) MarkTraderLogin(loginTime string, tradingDay string) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TraderLogin = true
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

func (c *RuntimeStatusCenter) MarkDrift(driftSec float64, paused bool) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DriftSeconds = driftSec
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
