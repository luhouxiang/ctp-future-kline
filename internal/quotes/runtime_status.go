// runtime_status.go 提供行情运行时的统一状态中心。
// 所有连接状态、订阅状态、tick 延迟、队列深度、去重计数等指标都汇总到这里，
// 再由 /api/status 和 WebSocket 对外暴露。
package quotes

import (
	"math"
	"sort"
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
	// PersistQueueMSMax1m 保存最近 1 分钟 DB 排队时间最大值。
	PersistQueueMSMax1m float64 `json:"persist_queue_ms_max_1m"`
	// EndToEndMS ? tick ?????????????
	EndToEndMS float64 `json:"end_to_end_ms"`
	// EndToEndMSAvg1m 保存最近 1 分钟端到端耗时均值。
	EndToEndMSAvg1m float64 `json:"end_to_end_ms_avg_1m"`
	// EndToEndMSP951m 保存最近 1 分钟端到端耗时 P95。
	EndToEndMSP951m float64 `json:"end_to_end_ms_p95_1m"`
	// EndToEndMSMax1m 保存最近 1 分钟端到端耗时最大值。
	EndToEndMSMax1m float64 `json:"end_to_end_ms_max_1m"`
	// DBFlushMS ???????? flush ????
	DBFlushMS float64 `json:"db_flush_ms"`
	// DBFlushRows ???????? flush ????
	DBFlushRows int `json:"db_flush_rows"`
	// DBFlushRowsLast 保存最近一次 DB flush 成功写入行数。
	DBFlushRowsLast int `json:"db_flush_rows_last"`
	// DBFlushMSLast 保存最近一次 DB flush 执行耗时。
	DBFlushMSLast float64 `json:"db_flush_ms_last"`
	// DBFlushRowsAvg1m 保存最近 1 分钟 DB flush 行数均值。
	DBFlushRowsAvg1m float64 `json:"db_flush_rows_avg_1m"`
	// DBFlushMSAvg1m 保存最近 1 分钟 DB flush 耗时均值。
	DBFlushMSAvg1m float64 `json:"db_flush_ms_avg_1m"`
	// DBFlushMSP951m 保存最近 1 分钟 DB flush 耗时 P95。
	DBFlushMSP951m float64 `json:"db_flush_ms_p95_1m"`
	// DBFlushRowsP951m 保存最近 1 分钟 DB flush 行数 P95。
	DBFlushRowsP951m int `json:"db_flush_rows_p95_1m"`
	// PersistQueueMSAvg1m 保存最近 1 分钟 DB 排队时间均值。
	PersistQueueMSAvg1m float64 `json:"persist_queue_ms_avg_1m"`
	// PersistQueueMSP951m 保存最近 1 分钟 DB 排队时间 P95。
	PersistQueueMSP951m float64 `json:"persist_queue_ms_p95_1m"`
	// DBQueueDepth ??? DB writer ?????
	DBQueueDepth int `json:"db_queue_depth"`
	// DBQueueDepthTotal 保存 DB writer 总队列深度。
	DBQueueDepthTotal int `json:"db_queue_depth_total"`
	// DBQueueDepthInflight 保存 DB writer 当前 in-flight 任务数。
	DBQueueDepthInflight int `json:"db_queue_depth_inflight"`
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
	// persistQueueSamples 保存最近 1 分钟排队时间样本。
	persistQueueSamples []timedFloatSample
	// endToEndSamples 保存最近 1 分钟端到端耗时样本。
	endToEndSamples []timedFloatSample
	// dbFlushMSSamples 保存最近 1 分钟 DB flush 耗时样本。
	dbFlushMSSamples []timedFloatSample
	// dbFlushRowSamples 保存最近 1 分钟 DB flush 行数样本。
	dbFlushRowSamples []timedIntSample
}

type timedFloatSample struct {
	at    time.Time
	value float64
}

type timedIntSample struct {
	at    time.Time
	value int
}

const runtimeStatusWindow = time.Minute

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

func (c *RuntimeStatusCenter) MarkRuntimeQueues(shardBacklog []int, dbQueueDepthTotal int, dbQueueDepthInflight int, _ int64, goroutines int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.ShardBacklog = append([]int(nil), shardBacklog...)
		s.DBQueueDepth = dbQueueDepthTotal
		s.DBQueueDepthTotal = dbQueueDepthTotal
		s.DBQueueDepthInflight = dbQueueDepthInflight
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
		now := time.Now()
		c.recordPersistQueueSampleLocked(now, queueMS)
		s.PersistQueueMS = queueMS
		s.PersistQueueMSAvg1m = avgFloatSamples(c.persistQueueSamples)
		s.PersistQueueMSP951m = p95FloatSamples(c.persistQueueSamples)
		s.PersistQueueMSMax1m = maxFloatSamples(c.persistQueueSamples)
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "persist_queue"
	})
}

func (c *RuntimeStatusCenter) MarkEndToEndLatency(instrumentID string, totalMS float64) {
	c.mutate(func(s *RuntimeSnapshot) {
		now := time.Now()
		c.recordEndToEndSampleLocked(now, totalMS)
		s.EndToEndMS = totalMS
		s.EndToEndMSAvg1m = avgFloatSamples(c.endToEndSamples)
		s.EndToEndMSP951m = p95FloatSamples(c.endToEndSamples)
		s.EndToEndMSMax1m = maxFloatSamples(c.endToEndSamples)
		s.LastLatencyInstrument = instrumentID
		s.LastLatencyStage = "end_to_end"
	})
}

func (c *RuntimeStatusCenter) MarkDBFlush(rows int, flushMS float64, queueDepthTotal int, queueDepthInflight int) {
	c.mutate(func(s *RuntimeSnapshot) {
		now := time.Now()
		c.recordDBFlushSamplesLocked(now, rows, flushMS)
		s.DBFlushRows = rows
		s.DBFlushMS = flushMS
		s.DBFlushRowsLast = rows
		s.DBFlushMSLast = flushMS
		s.DBFlushRowsAvg1m = avgIntSamples(c.dbFlushRowSamples)
		s.DBFlushMSAvg1m = avgFloatSamples(c.dbFlushMSSamples)
		s.DBFlushMSP951m = p95FloatSamples(c.dbFlushMSSamples)
		s.DBFlushRowsP951m = p95IntSamples(c.dbFlushRowSamples)
		s.DBQueueDepth = queueDepthTotal
		s.DBQueueDepthTotal = queueDepthTotal
		s.DBQueueDepthInflight = queueDepthInflight
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

func (c *RuntimeStatusCenter) recordPersistQueueSampleLocked(now time.Time, value float64) {
	c.persistQueueSamples = append(c.persistQueueSamples, timedFloatSample{at: now, value: value})
	c.persistQueueSamples = pruneFloatSamples(c.persistQueueSamples, now.Add(-runtimeStatusWindow))
}

func (c *RuntimeStatusCenter) recordEndToEndSampleLocked(now time.Time, value float64) {
	c.endToEndSamples = append(c.endToEndSamples, timedFloatSample{at: now, value: value})
	c.endToEndSamples = pruneFloatSamples(c.endToEndSamples, now.Add(-runtimeStatusWindow))
}

func (c *RuntimeStatusCenter) recordDBFlushSamplesLocked(now time.Time, rows int, flushMS float64) {
	c.dbFlushRowSamples = append(c.dbFlushRowSamples, timedIntSample{at: now, value: rows})
	c.dbFlushMSSamples = append(c.dbFlushMSSamples, timedFloatSample{at: now, value: flushMS})
	c.dbFlushRowSamples = pruneIntSamples(c.dbFlushRowSamples, now.Add(-runtimeStatusWindow))
	c.dbFlushMSSamples = pruneFloatSamples(c.dbFlushMSSamples, now.Add(-runtimeStatusWindow))
}

func pruneFloatSamples(in []timedFloatSample, cutoff time.Time) []timedFloatSample {
	idx := 0
	for idx < len(in) && in[idx].at.Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return in
	}
	out := make([]timedFloatSample, len(in)-idx)
	copy(out, in[idx:])
	return out
}

func pruneIntSamples(in []timedIntSample, cutoff time.Time) []timedIntSample {
	idx := 0
	for idx < len(in) && in[idx].at.Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return in
	}
	out := make([]timedIntSample, len(in)-idx)
	copy(out, in[idx:])
	return out
}

func avgFloatSamples(in []timedFloatSample) float64 {
	if len(in) == 0 {
		return 0
	}
	total := 0.0
	for _, item := range in {
		total += item.value
	}
	return total / float64(len(in))
}

func avgIntSamples(in []timedIntSample) float64 {
	if len(in) == 0 {
		return 0
	}
	total := 0
	for _, item := range in {
		total += item.value
	}
	return float64(total) / float64(len(in))
}

func p95FloatSamples(in []timedFloatSample) float64 {
	if len(in) == 0 {
		return 0
	}
	vals := make([]float64, 0, len(in))
	for _, item := range in {
		vals = append(vals, item.value)
	}
	sort.Float64s(vals)
	return vals[p95Index(len(vals))]
}

func p95IntSamples(in []timedIntSample) int {
	if len(in) == 0 {
		return 0
	}
	vals := make([]int, 0, len(in))
	for _, item := range in {
		vals = append(vals, item.value)
	}
	sort.Ints(vals)
	return vals[p95Index(len(vals))]
}

func maxFloatSamples(in []timedFloatSample) float64 {
	if len(in) == 0 {
		return 0
	}
	maxVal := in[0].value
	for _, item := range in[1:] {
		if item.value > maxVal {
			maxVal = item.value
		}
	}
	return maxVal
}

func p95Index(n int) int {
	if n <= 1 {
		return 0
	}
	idx := int(math.Ceil(float64(n)*0.95)) - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
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
