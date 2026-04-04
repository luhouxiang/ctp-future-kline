// runtime_status.go 提供行情运行时的统一状态中心。
// 所有连接状态、订阅状态、tick 延迟、队列深度、去重计数等指标都汇总到这里，
// 再由 /api/status 和 WebSocket 对外暴露。
package quotes

import (
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/queuewatch"
)

const (
	RuntimeStateIdle     = "idle"
	RuntimeStateStarting = "starting"
	RuntimeStateRunning  = "running"
	RuntimeStateError    = "error"
)

type RuntimeSnapshot struct {
	// State ?????????????? idle?starting?running?error?
	State string `json:"state"`
	// QueryFront ????????????
	QueryFront bool `json:"query_front"`
	// QueryLogin ??????????????
	QueryLogin bool `json:"query_login"`
	// MdFront ????????????
	MdFront bool `json:"md_front"`
	// MdLogin ??????????????
	MdLogin bool `json:"md_login"`
	// MdSubscribed ???????????????????
	MdSubscribed bool `json:"md_subscribed"`
	// MdFrontDown ??????????????????
	MdFrontDown bool `json:"md_front_disconnected"`
	// MdDownReason ???????? CTP reason code?
	MdDownReason int `json:"md_disconnect_reason"`
	// MdReconnectTry ?????????????????
	MdReconnectTry int `json:"md_reconnect_attempt"`
	// MdNextRetryAt ???????????
	MdNextRetryAt time.Time `json:"md_next_retry_at"`
	// NetworkSuspect ????????????????????????
	NetworkSuspect bool `json:"network_suspect"`
	// TickDedupDrop ??? tick ?????????
	TickDedupDrop int64 `json:"tick_dedup_dropped"`
	// DriftSeconds ????? tick ???????????????
	DriftSeconds float64 `json:"drift_seconds"`
	// DriftPaused ?????????????????????
	DriftPaused bool `json:"drift_paused"`
	// DriftPauseCnt ???????????????
	DriftPauseCnt int64 `json:"drift_pause_count"`
	// LastDriftInstrument ???????????????
	LastDriftInstrument string `json:"last_drift_instrument"`
	// LastDriftAt ???????????????
	LastDriftAt time.Time `json:"last_drift_at"`
	// UpstreamLagMS ????? tick ????????????????
	UpstreamLagMS float64 `json:"upstream_lag_ms"`
	// CallbackToProcMS ? CTP ???????????????
	CallbackToProcMS float64 `json:"callback_to_process_ms"`
	// LockWaitMS ??????????????
	LockWaitMS float64 `json:"lock_wait_ms"`
	// SideEffectTickQueueMS ? tick ?????????
	SideEffectTickQueueMS float64 `json:"side_effect_tick_queue_ms"`
	// SideEffectBarQueueMS ? bar ?????????
	SideEffectBarQueueMS float64 `json:"side_effect_bar_queue_ms"`
	// MinuteStoreMS ?????????????
	MinuteStoreMS float64 `json:"minute_store_ms"`
	// MMQueueMS ???????????????????
	MMQueueMS float64 `json:"mm_queue_ms"`
	// MMRunMS ?????????????
	MMRunMS float64 `json:"mm_run_ms"`
	// RouterQueueMS ? tick ??????????????
	RouterQueueMS float64 `json:"router_queue_ms"`
	// ShardQueueMS ? tick ? shard ???????????
	ShardQueueMS float64 `json:"shard_queue_ms"`
	// PersistQueueMS ??????? DB writer ???????
	PersistQueueMS float64 `json:"persist_queue_ms"`
	// EndToEndMS ? tick ?????????????
	EndToEndMS float64 `json:"end_to_end_ms"`
	// DBFlushMS ???????? flush ????
	DBFlushMS float64 `json:"db_flush_ms"`
	// DBFlushRows ???????? flush ????
	DBFlushRows int `json:"db_flush_rows"`
	// DBQueueDepth ??? DB writer ?????
	DBQueueDepth int `json:"db_queue_depth"`
	// FileFlushMS ????????????
	FileFlushMS float64 `json:"file_flush_ms"`
	// FileQueueDepth ???????????
	FileQueueDepth int `json:"file_queue_depth"`
	// ShardBacklog ?????? shard ??????
	ShardBacklog []int `json:"shard_backlog"`
	// DroppedTicks ???????????????? tick ??
	DroppedTicks int64 `json:"dropped_ticks"`
	// LateTicks ???????? tick ???
	LateTicks int64 `json:"late_ticks"`
	// Goroutines ????? goroutine ???????????
	Goroutines int `json:"goroutines"`
	// LastLatencyInstrument ??????????????????
	LastLatencyInstrument string `json:"last_latency_instrument"`
	// LastLatencyStage ?????????????????
	LastLatencyStage string `json:"last_latency_stage"`
	// ServerTime ???????????????
	ServerTime string `json:"server_time"`
	// LastTickTime ?????????????? tick ?????
	LastTickTime time.Time `json:"last_tick_time"`
	// IsMarketOpen ????? tick ??????????????
	IsMarketOpen bool `json:"is_market_open"`
	// LastError ??????????????
	LastError string `json:"last_error"`
	// UpdatedAt ??????????
	UpdatedAt time.Time `json:"updated_at"`
	// TradingDay ???????????
	TradingDay string `json:"trading_day"`
	// SubscribeCount ?????????????
	SubscribeCount int `json:"subscribe_count"`
	// QueueAlertCount ???????????????
	QueueAlertCount int `json:"queue_alert_count"`
	// QueueCriticalCount ??? critical/emergency ?????
	QueueCriticalCount int `json:"queue_critical_count"`
	// QueueSpillingCount ?????????????????
	QueueSpillingCount int `json:"queue_spilling_count"`
}

type RuntimeStatusCenter struct {
	// mu ?? snapshot ????????????
	mu sync.RWMutex
	// marketOpenStale ??? tick ?????????????????
	marketOpenStale time.Duration
	// snapshot ????????????????
	snapshot RuntimeSnapshot
	// subscribers ?????????????
	subscribers map[chan RuntimeSnapshot]struct{}
	// subscriberBufSize ?????????????
	subscriberBufSize int
	// queueRegistry ??????????????????
	queueRegistry *queuewatch.Registry
	// statusQueue ?? RuntimeStatusCenter ????????????
	statusQueue *queuewatch.QueueHandle
}

func NewRuntimeStatusCenter(marketOpenStale time.Duration) *RuntimeStatusCenter {
	cfg := queuewatch.DefaultConfig("")
	registry := queuewatch.NewRegistry(cfg)
	center := &RuntimeStatusCenter{
		marketOpenStale:   marketOpenStale,
		snapshot:          RuntimeSnapshot{State: RuntimeStateIdle, UpdatedAt: time.Now()},
		subscribers:       make(map[chan RuntimeSnapshot]struct{}),
		subscriberBufSize: cfg.StatusSubscriberCapacity,
		queueRegistry:     registry,
	}
	center.statusQueue = registry.Register(queuewatch.QueueSpec{
		Name:        "runtime_status_subscribers",
		Category:    "status",
		Criticality: "best_effort",
		Capacity:    cfg.StatusSubscriberCapacity,
		LossPolicy:  "latest_only",
		BasisText:   "每个状态订阅者一个缓冲通道，容量等于 status_subscriber_capacity。",
	})
	return center
}

func (c *RuntimeStatusCenter) ConfigureQueueMonitoring(ctpCfg config.CTPConfig) {
	if c == nil || c.queueRegistry == nil {
		return
	}
	cfg := queuewatch.DefaultConfig(ctpCfg.FlowPath)
	if strings.TrimSpace(ctpCfg.QueueSpoolDir) != "" {
		cfg.SpoolDir = strings.TrimSpace(ctpCfg.QueueSpoolDir)
	}
	cfg.WarnPercent = ctpCfg.QueueAlertWarnPercent
	cfg.CriticalPercent = ctpCfg.QueueAlertCriticalPercent
	cfg.EmergencyPercent = ctpCfg.QueueAlertEmergencyPercent
	cfg.RecoverPercent = ctpCfg.QueueAlertRecoverPercent
	cfg.ShardCapacity = ctpCfg.ShardCapacity
	cfg.PersistCapacity = ctpCfg.PersistCapacity
	cfg.MMDeferredCapacity = ctpCfg.MMDeferredCapacity
	cfg.L9TaskCapacity = ctpCfg.L9TaskCapacity
	cfg.FilePerShardCapacity = ctpCfg.FilePerShardCapacity
	cfg.SideEffectTickCapacity = ctpCfg.SideEffectTickCapacity
	cfg.SideEffectBarCapacity = ctpCfg.SideEffectBarCapacity
	cfg.ChartSubscriberCapacity = ctpCfg.ChartSubscriberCapacity
	cfg.StatusSubscriberCapacity = ctpCfg.StatusSubscriberCapacity
	cfg.StrategyEventCapacity = ctpCfg.StrategyEventCapacity
	cfg.TradeEventCapacity = ctpCfg.TradeEventCapacity
	cfg.TradeGatewayEventCapacity = ctpCfg.TradeGatewayEventCapacity
	cfg.MDDisconnectCapacity = ctpCfg.MDDisconnectCapacity
	c.queueRegistry.Configure(cfg)
	c.mu.Lock()
	c.subscriberBufSize = cfg.StatusSubscriberCapacity
	c.mu.Unlock()
	if c.statusQueue != nil {
		c.statusQueue.SetBasisText("每个状态订阅者一个缓冲通道，容量等于 status_subscriber_capacity。")
		c.statusQueue.ObserveDepth(c.maxStatusSubscriberDepth())
	}
}

func (c *RuntimeStatusCenter) QueueRegistry() *queuewatch.Registry {
	if c == nil {
		return nil
	}
	return c.queueRegistry
}

func (c *RuntimeStatusCenter) Snapshot(now time.Time) RuntimeSnapshot {
	c.mu.RLock()
	out := c.snapshot
	marketOpenStale := c.marketOpenStale
	c.mu.RUnlock()
	if out.LastTickTime.IsZero() {
		out.IsMarketOpen = false
	} else {
		out.IsMarketOpen = now.Sub(out.LastTickTime) <= marketOpenStale
	}
	if c.queueRegistry != nil {
		summary := c.queueRegistry.Snapshot().Summary
		out.QueueAlertCount = summary.ActiveAlerts
		out.QueueCriticalCount = summary.CriticalQueues
		out.QueueSpillingCount = summary.SpillingQueues
	}
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
	c.mu.RLock()
	bufSize := c.subscriberBufSize
	current := c.snapshot
	c.mu.RUnlock()
	ch := make(chan RuntimeSnapshot, bufSize)

	c.mu.Lock()
	c.subscribers[ch] = struct{}{}
	c.mu.Unlock()
	if c.statusQueue != nil {
		c.statusQueue.ObserveDepth(c.maxStatusSubscriberDepth())
	}
	ch <- current

	cancel := func() {
		c.mu.Lock()
		if _, ok := c.subscribers[ch]; ok {
			delete(c.subscribers, ch)
			close(ch)
		}
		c.mu.Unlock()
		if c.statusQueue != nil {
			c.statusQueue.ObserveDepth(c.maxStatusSubscriberDepth())
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
			if c.statusQueue != nil {
				c.statusQueue.MarkDropped(c.maxStatusSubscriberDepth())
			}
		}
	}
	if c.statusQueue != nil {
		c.statusQueue.ObserveDepth(c.maxStatusSubscriberDepth())
	}
}

func (c *RuntimeStatusCenter) maxStatusSubscriberDepth() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	maxDepth := 0
	for ch := range c.subscribers {
		if depth := len(ch); depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}
