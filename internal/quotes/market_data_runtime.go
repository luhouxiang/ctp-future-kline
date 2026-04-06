// market_data_runtime.go 是实时行情处理核心。
// 它将 tick 按合约分片串行处理，在 shard 内完成时间规整、去重、漂移检测、1m/mm 聚合，
// 并把结果交给落库、文件输出、L9 计算和旁路分发模块。
package quotes

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ctp-future-kline/internal/klineagg"
	"ctp-future-kline/internal/klineclock"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/sessiontime"
)

const (
	defaultMarketDataShardCount   = 16
	defaultShardChannelCapacity   = 8192
	defaultPersistQueueCapacity   = 16384
	defaultFileQueueCapacity      = 16384
	defaultDBWriterCount          = 12
	defaultDBFlushBatch           = 128
	defaultDBFlushInterval        = 100 * time.Millisecond
	defaultFileFlushInterval      = 200 * time.Millisecond
	defaultFileFsyncInterval      = time.Second
	defaultSlowLogInterval        = 5 * time.Second
	defaultTickHistoryRetention   = 2048
	defaultL9TaskQueueCapacity    = 4096
	defaultRuntimeMetricsInterval = 5 * time.Second
	invalidTickSessionGapMinutes  = 3
)

type runtimeOptions struct {
	// tickDedupWindow 是重复 tick 的判定窗口。
	tickDedupWindow time.Duration
	// driftThreshold 是允许的 tick 时间漂移阈值。
	driftThreshold time.Duration
	// driftResumeTicks 是恢复正常漂移状态所需的连续正常 tick 数。
	driftResumeTicks int
	// enableMultiMinute 控制是否继续生成 mm 周期 bar。
	enableMultiMinute bool
	// flowPath 指向 flow 根目录，用于初始化文件输出。
	flowPath string
	// onTick 是 tick 旁路回调。
	onTick func(tickEvent)
	// onBar 是 bar 旁路回调。
	onBar func(minuteBar)
	// onPartialBar 在当前分钟 bar 被更新后立即触发。
	onPartialBar func(minuteBar)
	// onPersistTask 在 bar 进入落库队列时触发。
	onPersistTask func(persistTask)
}

type runtimeTick struct {
	// tickEvent 是统一后的 tick 数据体。
	tickEvent
	// replay 标记该 tick 是否来自回放，而非实时 CTP。
	replay bool
}

type runtimeTrace struct {
	// ReceivedAt 是 tick 被接收的时间。
	ReceivedAt time.Time
	// RouteEnqueuedAt 是路由到 shard 前的入队时间。
	RouteEnqueuedAt time.Time
	// ShardDequeuedAt 是从 shard 队列取出开始处理的时间。
	ShardDequeuedAt time.Time
	// StateUpdatedAt 是完成 shard 内状态更新的时间。
	StateUpdatedAt time.Time
	// MinuteClosedAt 是关闭前一根分钟线的时间。
	MinuteClosedAt time.Time
	// PersistEnqueuedAt 是持久化任务入队时间。
	PersistEnqueuedAt time.Time
	// DBDequeuedAt 是 DB writer 取出该任务的时间。
	DBDequeuedAt time.Time
	// DBFlushedAt 是该任务随批次成功写入数据库的时间。
	DBFlushedAt time.Time
}

type persistTask struct {
	// Bar 是待落库的分钟线数据。
	Bar minuteBar
	// TableName 是目标表名。
	TableName string
	// Trace 保存该 bar 从 tick 到落库的时序跟踪信息。
	Trace runtimeTrace
	// InstrumentID 是该任务所属合约。
	InstrumentID string
	// ShardID 是生成该任务的行情分片编号。
	ShardID int
	// IsL9 标记该任务是否属于 L9/主连产物。
	IsL9 bool
	// Replay 标记该任务是否来自回放链路。
	Replay bool
}

type flushRequest struct {
	// done 用于把 flush 结果回传给调用方。
	done chan error
}

type stopRequest struct {
	// done 用于通知 shard 已完成停止流程。
	done chan struct{}
}

type instrumentRuntimeState struct {
	// bar 保存当前正在构建中的分钟线。
	bar minuteBar
	// lastTick 保存最近一个有效 tick 的快照。
	lastTick minuteTickSnapshot
	// hasBar 表示当前是否已经为该合约初始化过 bar。
	hasBar bool
	// currentBarBaseVol 是当前正在构建中的分钟线成交量基线。
	currentBarBaseVol int
	// prevGeneratedBarCloseVol 是上一根已生成分钟线结束时的累计成交量。
	prevGeneratedBarCloseVol int
	// hasPrevGeneratedBarVolume 表示是否已有上一根已生成分钟线的累计成交量基准。
	hasPrevGeneratedBarVolume bool
	// currentTradingDay 记录当前状态对应的业务交易日。
	currentTradingDay string
	// lastVolumes 是最近一次看到的累计成交量。
	lastVolumes int
	// tracker 负责把 1m final + 当前 1m partial 增量推进到更高周期。
	tracker *timeframeTracker
	// restoredTradingDay 记录高周期状态最近一次回灌完成的交易日。
	restoredTradingDay string
}

type closedBarAggregator struct {
	// instrumentID 是当前聚合器所属合约。
	instrumentID string
	// exchange 是当前合约所在交易所。
	exchange string
	// history 保存最近一段时间的已关闭分钟线历史。
	history []minuteBar
	// lastEmitted 记录各周期最近一次已输出的 bar 时间，避免重复发射。
	lastEmitted map[string]time.Time
}

func newClosedBarAggregator(instrumentID string, exchange string) *closedBarAggregator {
	return &closedBarAggregator{
		instrumentID: instrumentID,
		exchange:     exchange,
		lastEmitted:  make(map[string]time.Time),
	}
}

func (a *closedBarAggregator) Consume(closedBar minuteBar, sessions []sessiontime.Range, flush bool) []minuteBar {
	if a == nil {
		return nil
	}
	if closedBar.Exchange != "" {
		a.exchange = closedBar.Exchange
	}
	a.instrumentID = closedBar.InstrumentID
	a.history = append(a.history, closedBar)
	if len(a.history) > defaultTickHistoryRetention {
		a.history = append([]minuteBar(nil), a.history[len(a.history)-defaultTickHistoryRetention:]...)
	}

	out := make([]minuteBar, 0, 6)
	out = append(out, a.consumeIntraday(closedBar, sessions)...)
	if daily, ok := a.consumeDaily(closedBar, sessions, flush); ok {
		out = append(out, daily)
	}
	return out
}

func (a *closedBarAggregator) consumeIntraday(closedBar minuteBar, sessions []sessiontime.Range) []minuteBar {
	if len(sessions) == 0 || len(a.history) == 0 {
		return nil
	}
	src := make([]klineagg.MinuteBar, 0, len(a.history))
	for _, bar := range a.history {
		src = append(src, klineagg.MinuteBar{
			InstrumentID: bar.InstrumentID,
			Exchange:     bar.Exchange,
			DataTime:     bar.MinuteTime,
			AdjustedTime: chooseAdjustedTime(bar),
			Open:         bar.Open,
			High:         bar.High,
			Low:          bar.Low,
			Close:        bar.Close,
			Volume:       bar.Volume,
			OpenInterest: bar.OpenInterest,
		})
	}

	periods := []struct {
		label   string
		minutes int
	}{
		{label: "5m", minutes: 5},
		{label: "15m", minutes: 15},
		{label: "30m", minutes: 30},
		{label: "1h", minutes: 60},
		{label: "120m", minutes: 120},
	}

	out := make([]minuteBar, 0, len(periods))
	for _, period := range periods {
		aggBars, _ := klineagg.Aggregate(src, toKlineAggSessions(sessions), period.label, period.minutes, klineagg.Options{
			CrossSessionFor30m1h: true,
			ClampToSessionEnd:    true,
		})
		if len(aggBars) == 0 {
			continue
		}
		last := aggBars[len(aggBars)-1]
		if last.ExpectedMinutes <= 0 || last.ActualMinutes < last.ExpectedMinutes {
			continue
		}
		lastEmittedAt := a.lastEmitted[period.label]
		if !last.AdjustedTime.After(lastEmittedAt) {
			continue
		}
		out = append(out, minuteBar{
			Variety:          closedBar.Variety,
			InstrumentID:     last.InstrumentID,
			Exchange:         last.Exchange,
			MinuteTime:       last.DataTime,
			AdjustedTime:     last.AdjustedTime,
			SourceReceivedAt: closedBar.SourceReceivedAt,
			Period:           period.label,
			Open:             last.Open,
			High:             last.High,
			Low:              last.Low,
			Close:            last.Close,
			Volume:           last.Volume,
			OpenInterest:     last.OpenInterest,
			SettlementPrice:  closedBar.SettlementPrice,
		})
		a.lastEmitted[period.label] = last.AdjustedTime
	}
	return out
}

func (a *closedBarAggregator) consumeDaily(closedBar minuteBar, sessions []sessiontime.Range, flush bool) (minuteBar, bool) {
	if len(a.history) == 0 {
		return minuteBar{}, false
	}
	currentDay := tradingDayKey(closedBar)
	if currentDay == "" {
		return minuteBar{}, false
	}
	targetDay := currentDay
	if !flush && len(a.history) >= 2 {
		prev := tradingDayKey(a.history[len(a.history)-2])
		if prev == currentDay {
			return minuteBar{}, false
		}
		targetDay = prev
	}

	lastEmittedAt := a.lastEmitted["1d"]
	var dayBars []minuteBar
	for _, bar := range a.history {
		if tradingDayKey(bar) != targetDay {
			continue
		}
		dayBars = append(dayBars, bar)
	}
	if len(dayBars) == 0 {
		return minuteBar{}, false
	}
	lastBar := dayBars[len(dayBars)-1]
	if !flush && tradingDayKey(lastBar) == currentDay && targetDay == currentDay {
		return minuteBar{}, false
	}
	dataLabelTime, adjustedLabelTime, ok := dailyLabelTimes(dayBars[0], sessions)
	if !ok {
		return minuteBar{}, false
	}
	if !adjustedLabelTime.After(lastEmittedAt) {
		return minuteBar{}, false
	}
	daily := minuteBar{
		Variety:          closedBar.Variety,
		InstrumentID:     a.instrumentID,
		Exchange:         a.exchange,
		MinuteTime:       dataLabelTime,
		AdjustedTime:     adjustedLabelTime,
		SourceReceivedAt: closedBar.SourceReceivedAt,
		Period:           "1d",
		Open:             dayBars[0].Open,
		High:             dayBars[0].High,
		Low:              dayBars[0].Low,
		Close:            dayBars[len(dayBars)-1].Close,
		Volume:           0,
		OpenInterest:     dayBars[len(dayBars)-1].OpenInterest,
		SettlementPrice:  dayBars[len(dayBars)-1].SettlementPrice,
	}
	for _, bar := range dayBars {
		if bar.High > daily.High {
			daily.High = bar.High
		}
		if bar.Low < daily.Low {
			daily.Low = bar.Low
		}
		daily.Volume += bar.Volume
	}
	a.lastEmitted["1d"] = adjustedLabelTime
	return daily, true
}

type marketDataRuntime struct {
	// store 是 K 线存储层，负责分钟线落库与查询。
	store *klineStore
	// status 是全局行情状态中心。
	status *RuntimeStatusCenter
	// clock 用于解析交易日和业务时间。
	clock *klineclock.CalendarResolver
	// sessionResolver 提供品种交易时段信息。
	sessionResolver *sessionResolver
	// l9Async 用于异步计算主连/L9。
	l9Async *l9AsyncCalculator
	// opts 保存运行时行为参数和旁路回调。
	opts runtimeOptions

	// shards 是按合约 hash 切分出的处理分片。
	shards []*marketDataShard
	// dbWriter 负责持久化任务批量写库。
	dbWriter *dbBatchWriter
	// closed 标记 runtime 是否已关闭。
	closed atomic.Bool
	// logMu 保护 lastLogAt。
	logMu sync.Mutex
	// lastLogAt 用于限频某些告警日志。
	lastLogAt map[string]time.Time

	// replayMu 保护 replay 首条日志状态。
	replayMu sync.Mutex
	// replayProcessSeen 记录哪些合约已经打过首次进入 runtime 的回放日志。
	replayProcessSeen map[string]struct{}
	// replayPipelineSeen 记录哪些合约已经打过首次进入 shard 的回放日志。
	replayPipelineSeen map[string]struct{}
	// shardQueueDepthGauge 记录每个 shard 当前队列长度。
	shardQueueDepthGauge []int64
}

type marketDataShard struct {
	// runtime 指回所属的全局运行时。
	runtime *marketDataRuntime
	// id 是 shard 编号。
	id int
	// in 是该 shard 的输入队列。
	in chan any
	// queueHandle 汇总该 shard 输入队列的深度、告警和溢写状态。
	queueHandle *queuewatch.QueueHandle
	// tickSpool 在内存队列写满时把 tick 顺序溢写到磁盘。
	tickSpool *queuewatch.JSONSpool[runtimeTick]

	// state 保存该 shard 内各合约的聚合状态。
	state map[string]*instrumentRuntimeState
	// fileWriter 负责该 shard 的文件侧异步输出。
	fileWriter *shardFileWriter
}

func newMarketDataRuntime(store *klineStore, metaDB *sql.DB, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter, opts runtimeOptions) *marketDataRuntime {
	if opts.tickDedupWindow <= 0 {
		opts.tickDedupWindow = 2 * time.Second
	}
	if opts.driftThreshold <= 0 {
		opts.driftThreshold = 5 * time.Second
	}
	if opts.driftResumeTicks <= 0 {
		opts.driftResumeTicks = 3
	}

	rt := &marketDataRuntime{
		store:              store,
		status:             status,
		clock:              klineclock.NewCalendarResolver(preferMetaDB(metaDB, store.DB())),
		sessionResolver:    newSessionResolver(preferMetaDB(metaDB, store.DB())),
		l9Async:            l9Async,
		opts:               opts,
		lastLogAt:          make(map[string]time.Time),
		replayProcessSeen:  make(map[string]struct{}),
		replayPipelineSeen: make(map[string]struct{}),
	}
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	shardCount := defaultMarketDataShardCount
	rt.shardQueueDepthGauge = make([]int64, shardCount)
	rt.dbWriter = newDBBatchWriter(store, status, defaultDBWriterCount, queueCfg.PersistCapacity, defaultDBFlushBatch, defaultDBFlushInterval)
	if l9Async != nil {
		l9Async.SetPersistSink(rt.enqueuePersistTasks)
	}
	rt.shards = make([]*marketDataShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shard := &marketDataShard{
			runtime: rt,
			id:      i,
			in:      make(chan any, queueCfg.ShardCapacity),
			state:   make(map[string]*instrumentRuntimeState),
		}
		if registry != nil {
			shard.queueHandle = registry.Register(queuewatch.QueueSpec{
				Name:        fmt.Sprintf("market_data_shard_%02d", i),
				Category:    "quotes_primary",
				Criticality: "critical",
				Capacity:    queueCfg.ShardCapacity,
				LossPolicy:  "spill_to_disk",
				BasisText:   fmt.Sprintf("按合约哈希后每个 shard 一个内存队列，容量等于 shard_capacity=%d。", queueCfg.ShardCapacity),
			})
			spool, err := queuewatch.NewJSONSpool[runtimeTick](queueCfg.SpoolDir, shard.queueHandle.Name())
			if err != nil {
				logger.Error("init shard queue spool failed", "shard_id", i, "error", err)
			} else {
				shard.tickSpool = spool
				if shard.queueHandle != nil {
					shard.queueHandle.ObserveDepth(len(shard.in) + shard.tickSpool.Pending())
				}
			}
		}
		if opts.flowPath != "" {
			fileWriter, err := newShardFileWriter(opts.flowPath, i, status)
			if err != nil {
				logger.Error("init shard file writer failed", "shard_id", i, "error", err)
			} else {
				shard.fileWriter = fileWriter
			}
		}
		rt.shards = append(rt.shards, shard)
		go shard.run()
	}
	go rt.publishQueueMetrics()
	return rt
}

func (r *marketDataRuntime) close() error {
	if r == nil {
		return nil
	}
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := r.flush(); err != nil {
		return err
	}
	if r.l9Async != nil {
		r.l9Async.Close()
	}
	return r.dbWriter.Close()
}

func (r *marketDataRuntime) publishQueueMetrics() {
	ticker := time.NewTicker(defaultRuntimeMetricsInterval)
	defer ticker.Stop()
	for range ticker.C {
		if r.closed.Load() {
			return
		}
		if r.status != nil {
			backlog := make([]int, 0, len(r.shards))
			for i := range r.shards {
				backlog = append(backlog, int(atomic.LoadInt64(&r.shardQueueDepthGauge[i])))
			}
			r.status.MarkRuntimeQueues(backlog, r.dbWriter.QueueDepth(), r.dbWriter.DropCount(), runtime.NumGoroutine())
		}
	}
}

func (r *marketDataRuntime) currentTradingDay() string {
	if r == nil || r.status == nil {
		return ""
	}
	return strings.TrimSpace(r.status.TradingDay())
}

func (r *marketDataRuntime) onLiveTick(in tickInputData) error {
	tradingDay := r.currentTradingDay()
	if tradingDay == "" {
		tradingDay = strings.TrimSpace(in.TradingDay)
	}
	return r.enqueue(runtimeTick{
		tickEvent: tickEvent{
			InstrumentID:       strings.TrimSpace(in.InstrumentID),
			ExchangeID:         strings.TrimSpace(in.ExchangeID),
			ExchangeInstID:     strings.TrimSpace(in.ExchangeInstID),
			RawActionDay:       strings.TrimSpace(in.ActionDay),
			RawTradingDay:      strings.TrimSpace(in.TradingDay),
			ActionDay:          strings.TrimSpace(in.ActionDay),
			TradingDay:         tradingDay,
			UpdateTime:         strings.TrimSpace(in.UpdateTime),
			UpdateMillisec:     in.UpdateMillisec,
			ReceivedAt:         in.ReceivedAt,
			CallbackAt:         in.CallbackAt,
			LastPrice:          in.LastPrice,
			PreSettlementPrice: in.PreSettlementPrice,
			PreClosePrice:      in.PreClosePrice,
			PreOpenInterest:    in.PreOpenInterest,
			OpenPrice:          in.OpenPrice,
			HighestPrice:       in.HighestPrice,
			LowestPrice:        in.LowestPrice,
			Volume:             in.Volume,
			Turnover:           in.Turnover,
			OpenInterest:       in.OpenInterest,
			ClosePrice:         in.ClosePrice,
			SettlementPrice:    in.SettlementPrice,
			UpperLimitPrice:    in.UpperLimitPrice,
			LowerLimitPrice:    in.LowerLimitPrice,
			AveragePrice:       in.AveragePrice,
			PreDelta:           in.PreDelta,
			CurrDelta:          in.CurrDelta,
			BidPrice1:          in.BidPrice1,
			AskPrice1:          in.AskPrice1,
			BidVolume1:         in.BidVolume1,
			AskVolume1:         in.AskVolume1,
		},
	})
}

func (r *marketDataRuntime) onReplayTick(ev tickEvent) error {
	r.logFirstReplayProcess(ev)
	if strings.TrimSpace(ev.RawActionDay) == "" {
		ev.RawActionDay = ev.ActionDay
	}
	if strings.TrimSpace(ev.RawTradingDay) == "" {
		ev.RawTradingDay = ev.TradingDay
	}
	tradingDay := r.currentTradingDay()
	if tradingDay != "" {
		ev.TradingDay = tradingDay
	}
	return r.enqueue(runtimeTick{tickEvent: ev, replay: true})
}

func (r *marketDataRuntime) enqueue(t runtimeTick) error {
	if r == nil {
		return nil
	}
	instrumentID := strings.TrimSpace(t.InstrumentID)
	if instrumentID == "" {
		return nil
	}
	if t.ReceivedAt.IsZero() {
		t.ReceivedAt = time.Now()
	}
	if t.CallbackAt.IsZero() {
		t.CallbackAt = t.ReceivedAt
	}
	t.ProcessStartedAt = time.Now()
	routeAt := time.Now()
	shardID := r.shardForInstrument(instrumentID)
	t.SideEffectEnqueuedAt = routeAt
	if !t.replay && shardID >= 0 && shardID < len(r.shards) && r.shards[shardID].fileWriter != nil {
		r.shards[shardID].fileWriter.Enqueue(t.tickEvent)
	}
	shard := r.shards[shardID]
	select {
	case shard.in <- t:
		depth := len(shard.in)
		if shard.tickSpool != nil {
			depth += shard.tickSpool.Pending()
		}
		atomic.StoreInt64(&r.shardQueueDepthGauge[shardID], int64(depth))
		if shard.queueHandle != nil {
			shard.queueHandle.MarkEnqueued(depth)
		}
		if r.status != nil {
			routerQueueMS := routeAt.Sub(runtimeMetricTime(t.CallbackAt, t.replay, routeAt)).Seconds() * 1000
			r.status.MarkRouterLatency(instrumentID, routerQueueMS)
		}
		return nil
	default:
		if shard.tickSpool != nil {
			if size, err := shard.tickSpool.Enqueue(t); err == nil {
				depth := len(shard.in) + shard.tickSpool.Pending()
				atomic.StoreInt64(&r.shardQueueDepthGauge[shardID], int64(depth))
				if shard.queueHandle != nil {
					shard.queueHandle.MarkSpilled(size, depth)
				}
				if r.status != nil {
					routerQueueMS := routeAt.Sub(runtimeMetricTime(t.CallbackAt, t.replay, routeAt)).Seconds() * 1000
					r.status.MarkRouterLatency(instrumentID, routerQueueMS)
				}
				return nil
			}
		}
		shard.in <- t
		depth := len(shard.in)
		if shard.tickSpool != nil {
			depth += shard.tickSpool.Pending()
		}
		atomic.StoreInt64(&r.shardQueueDepthGauge[shardID], int64(depth))
		if shard.queueHandle != nil {
			shard.queueHandle.MarkEnqueued(depth)
		}
		if r.status != nil {
			routerQueueMS := routeAt.Sub(runtimeMetricTime(t.CallbackAt, t.replay, routeAt)).Seconds() * 1000
			r.status.MarkRouterLatency(instrumentID, routerQueueMS)
		}
		r.maybeWarn("shard_queue_spill_fail:"+instrumentID, "market data shard queue full and spill unavailable, blocking enqueue",
			"instrument_id", instrumentID,
			"shard_id", shardID,
			"queue_depth", depth,
		)
		return nil
	}
}

func (r *marketDataRuntime) flush() error {
	if r == nil {
		return nil
	}
	var firstErr error
	for _, shard := range r.shards {
		done := make(chan error, 1)
		shard.in <- flushRequest{done: done}
		if err := <-done; err != nil && firstErr == nil {
			firstErr = err
		}
		atomic.StoreInt64(&r.shardQueueDepthGauge[shard.id], int64(len(shard.in)))
	}
	if err := r.dbWriter.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (r *marketDataRuntime) resetReplayStageLogState() {
	r.replayMu.Lock()
	r.replayProcessSeen = make(map[string]struct{})
	r.replayPipelineSeen = make(map[string]struct{})
	r.replayMu.Unlock()
}

func (r *marketDataRuntime) logFirstReplayProcess(ev tickEvent) {
	instrumentID := strings.TrimSpace(ev.InstrumentID)
	if instrumentID == "" {
		return
	}
	r.replayMu.Lock()
	if _, ok := r.replayProcessSeen[instrumentID]; ok {
		r.replayMu.Unlock()
		return
	}
	r.replayProcessSeen[instrumentID] = struct{}{}
	r.replayMu.Unlock()
	logger.Info(
		"replay tick first entered runtime",
		"stage", "marketDataRuntime.onReplayTick",
		"instrument_id", instrumentID,
		"update_time", ev.UpdateTime,
		"update_millisec", ev.UpdateMillisec,
	)
}

func (r *marketDataRuntime) logFirstReplayPipeline(ev tickEvent) {
	instrumentID := strings.TrimSpace(ev.InstrumentID)
	if instrumentID == "" {
		return
	}
	r.replayMu.Lock()
	if _, ok := r.replayPipelineSeen[instrumentID]; ok {
		r.replayMu.Unlock()
		return
	}
	r.replayPipelineSeen[instrumentID] = struct{}{}
	r.replayMu.Unlock()
	logger.Info(
		"replay tick first entered shard pipeline",
		"stage", "marketDataRuntime.processTick",
		"instrument_id", instrumentID,
		"update_time", ev.UpdateTime,
		"update_millisec", ev.UpdateMillisec,
	)
}

func (r *marketDataRuntime) shardForInstrument(instrumentID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(instrumentID))))
	return int(h.Sum32() % uint32(len(r.shards)))
}

func (r *marketDataRuntime) maybeWarn(key string, msg string, args ...any) {
	r.logMu.Lock()
	defer r.logMu.Unlock()
	now := time.Now()
	lastAt := r.lastLogAt[key]
	if now.Sub(lastAt) < defaultSlowLogInterval {
		return
	}
	r.lastLogAt[key] = now
	logger.Warn(msg, args...)
}

func runtimeMetricTime(ts time.Time, replay bool, now time.Time) time.Time {
	if replay || ts.IsZero() {
		return now
	}
	return ts
}

func (r *marketDataRuntime) enqueuePersistTasks(tasks []persistTask) {
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		if r.opts.onPersistTask != nil {
			r.opts.onPersistTask(task)
		}
		r.dbWriter.Enqueue(task)
	}
}

func (s *marketDataShard) run() {
	for {
		select {
		case msg, ok := <-s.in:
			if !ok {
				return
			}
			if s.handleMessage(msg) {
				return
			}
			continue
		default:
		}
		if s.tickSpool != nil {
			if tick, ok, _, err := s.tickSpool.Dequeue(); err == nil && ok {
				depth := len(s.in) + s.tickSpool.Pending()
				atomic.StoreInt64(&s.runtime.shardQueueDepthGauge[s.id], int64(depth))
				if s.queueHandle != nil {
					s.queueHandle.MarkSpillRecovered(depth)
					s.queueHandle.MarkDequeued(depth)
				}
				s.processTick(tick)
				continue
			} else if err != nil {
				logger.Error("dequeue shard spool failed", "shard_id", s.id, "error", err)
			}
		}
		msg, ok := <-s.in
		if !ok {
			return
		}
		if s.handleMessage(msg) {
			return
		}
	}
}

func (s *marketDataShard) handleMessage(msg any) bool {
	switch m := msg.(type) {
	case runtimeTick:
		depth := len(s.in)
		if s.tickSpool != nil {
			depth += s.tickSpool.Pending()
		}
		atomic.StoreInt64(&s.runtime.shardQueueDepthGauge[s.id], int64(depth))
		if s.queueHandle != nil {
			s.queueHandle.MarkDequeued(depth)
		}
		s.processTick(m)
	case flushRequest:
		m.done <- s.flush()
	case stopRequest:
		close(m.done)
		return true
	}
	return false
}

func (s *marketDataShard) processTick(t runtimeTick) {
	if t.replay {
		s.runtime.logFirstReplayPipeline(t.tickEvent)
	}
	dequeuedAt := time.Now()
	now := t.ReceivedAt
	if now.IsZero() {
		now = dequeuedAt
	}
	instrumentID := strings.TrimSpace(t.InstrumentID)
	exchangeID := strings.TrimSpace(t.ExchangeID)
	variety := normalizeVariety(instrumentID)
	if instrumentID == "" || variety == "" {
		return
	}

	sessions, err := s.runtime.sessionResolver.Sessions(variety)
	if err != nil {
		logger.Error("load trading sessions failed", "instrument_id", instrumentID, "error", err)
		return
	}
	sessionDistance, err := tickSessionDistanceMinutes(t.UpdateTime, sessions)
	if err != nil {
		logger.Error("parse tick update_time failed", "instrument_id", instrumentID, "update_time", strings.TrimSpace(t.UpdateTime), "error", err)
		return
	}
	if sessionDistance > invalidTickSessionGapMinutes {
		s.runtime.maybeWarn("tick_out_of_session:"+instrumentID+":"+strings.TrimSpace(t.UpdateTime), "tick too far from trading session, dropping",
			"instrument_id", instrumentID,
			"exchange_id", exchangeID,
			"update_time", strings.TrimSpace(t.UpdateTime),
			"trading_day", strings.TrimSpace(t.TradingDay),
			"action_day", strings.TrimSpace(t.ActionDay),
			"distance_minutes", sessionDistance,
			"threshold_minutes", invalidTickSessionGapMinutes,
			"replay", t.replay,
		)
		return
	}
	minuteTime, adjustedTime, adjustedTickTime, err := parseTickTimesWithMillis(t.ActionDay, t.TradingDay, t.UpdateTime, t.UpdateMillisec, sessions, s.runtime.clock)
	if err != nil {
		logger.Error("parse tick time failed", "instrument_id", instrumentID, "error", err)
		return
	}
	if s.runtime.status != nil {
		s.runtime.status.MarkTick(now)
	}

	price := t.LastPrice
	if !isValidTradePrice(price) {
		return
	}
	settlement := t.SettlementPrice
	if !isFinitePrice(settlement) {
		settlement = 0
	}
	openInterest := t.OpenInterest
	currentVol := t.Volume
	currentTradingDay := strings.TrimSpace(t.TradingDay)

	state := s.state[instrumentID]
	if state == nil {
		state = &instrumentRuntimeState{
			tracker: newTimeframeTracker(),
		}
		s.state[instrumentID] = state
	}
	if state.currentTradingDay != "" && currentTradingDay != "" && state.currentTradingDay != currentTradingDay {
		state.hasBar = false
		state.currentBarBaseVol = 0
		state.prevGeneratedBarCloseVol = 0
		state.hasPrevGeneratedBarVolume = false
		state.restoredTradingDay = ""
		if state.tracker != nil {
			state.tracker.Reset()
		}
	}
	if currentTradingDay != "" {
		state.currentTradingDay = currentTradingDay
	}
	if state.tracker == nil {
		state.tracker = newTimeframeTracker()
	}
	if s.runtime.opts.enableMultiMinute {
		if err := s.restoreTimeframeState(state, instrumentID, variety, currentTradingDay); err != nil {
			logger.Error("restore higher timeframe state failed", "instrument_id", instrumentID, "trading_day", currentTradingDay, "error", err)
		}
	}

	upstreamLagMS := now.Sub(adjustedTickTime).Seconds() * 1000
	shardQueueMS := dequeuedAt.Sub(t.ProcessStartedAt).Seconds() * 1000
	if shardQueueMS < 0 {
		shardQueueMS = 0
	}
	if s.runtime.status != nil {
		s.runtime.status.MarkShardLatency(instrumentID, s.id, shardQueueMS)
	}
	if !t.replay && shouldCheckTickDrift(now, adjustedTickTime) {
		driftSec := math.Abs(now.Sub(adjustedTickTime).Seconds())
		if s.runtime.status != nil {
			s.runtime.status.MarkDrift(instrumentID, driftSec, false)
			s.runtime.status.MarkUpstreamLag(instrumentID, upstreamLagMS)
		}
		if driftSec > s.runtime.opts.driftThreshold.Seconds() {
			if s.runtime.status != nil {
				s.runtime.status.MarkLateTick()
			}
			s.runtime.maybeWarn("tick_drift:"+instrumentID, "tick upstream lag too large",
				"instrument_id", instrumentID,
				"drift_seconds", driftSec,
				"threshold_seconds", s.runtime.opts.driftThreshold.Seconds(),
				"shard_id", s.id,
			)
		}
	}

	if s.runtime.opts.onTick != nil {
		t.SideEffectEnqueuedAt = dequeuedAt
		s.runtime.opts.onTick(t.tickEvent)
	}

	volumeDelta := int64(0)
	if state.hasBar && currentVol >= state.lastVolumes {
		volumeDelta = int64(currentVol - state.lastVolumes)
	}
	state.lastVolumes = currentVol
	lastTick := minuteTickSnapshot{
		TickTime:       adjustedTickTime,
		ReceivedAt:     now,
		UpdateTime:     t.UpdateTime,
		UpdateMillisec: t.UpdateMillisec,
		Price:          price,
		CurrentVolume:  currentVol,
		VolumeDelta:    volumeDelta,
		OpenInterest:   openInterest,
	}

	var persisted []persistTask
	if !state.hasBar {
		baseVol := previousGeneratedBarBase(state, currentVol)
		state.bar = minuteBar{
			Variety:          variety,
			InstrumentID:     instrumentID,
			Exchange:         exchangeID,
			Replay:           t.replay,
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: now,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           computeBucketVolume(currentVol, baseVol, state.hasPrevGeneratedBarVolume),
			OpenInterest:     openInterest,
			SettlementPrice:  settlement,
		}
		state.currentBarBaseVol = baseVol
		state.lastTick = lastTick
		state.hasBar = true
		if s.runtime.opts.onPartialBar != nil {
			onPartialBarRateProbe.Inc()
			s.runtime.opts.onPartialBar(state.bar)
		}
		s.emitHigherTimeframePartials(state, sessions)
		return
	}

	if !state.bar.MinuteTime.Equal(minuteTime) {
		closed := state.bar
		closed.SourceReceivedAt = state.lastTick.ReceivedAt
		trace := runtimeTrace{
			ReceivedAt:        runtimeMetricTime(t.ReceivedAt, t.replay, now),
			RouteEnqueuedAt:   t.ProcessStartedAt,
			ShardDequeuedAt:   dequeuedAt,
			StateUpdatedAt:    time.Now(),
			MinuteClosedAt:    time.Now(),
			PersistEnqueuedAt: time.Now(),
		}
		persisted = append(persisted, s.buildPersistTasks(closed, trace, false)...)
		state.prevGeneratedBarCloseVol = state.lastTick.CurrentVolume
		state.hasPrevGeneratedBarVolume = true

		baseVol := previousGeneratedBarBase(state, currentVol)
		started := minuteBar{
			Variety:          variety,
			InstrumentID:     instrumentID,
			Exchange:         exchangeID,
			Replay:           t.replay,
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: now,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           computeBucketVolume(currentVol, baseVol, state.hasPrevGeneratedBarVolume),
			OpenInterest:     openInterest,
			SettlementPrice:  settlement,
		}
		state.currentBarBaseVol = baseVol
		state.bar = started
		state.lastTick = lastTick
	} else {
		if price > state.bar.High {
			state.bar.High = price
		}
		if price < state.bar.Low {
			state.bar.Low = price
		}
		state.bar.Close = price
		state.bar.Volume = computeBucketVolume(currentVol, state.currentBarBaseVol, state.hasPrevGeneratedBarVolume)
		state.bar.OpenInterest = openInterest
		state.bar.SettlementPrice = settlement
		state.bar.AdjustedTime = adjustedTime
		state.bar.SourceReceivedAt = now
		state.lastTick = lastTick
	}
	if s.runtime.opts.onPartialBar != nil {
		onPartialBarRateProbe.Inc()
		s.runtime.opts.onPartialBar(state.bar)
	}
	s.emitHigherTimeframePartials(state, sessions)

	if len(persisted) > 0 {
		s.runtime.enqueuePersistTasks(persisted)
		if s.runtime.opts.onBar != nil {
			for _, task := range persisted {
				if task.Bar.Period == "1m" && !task.IsL9 {
					task.Bar.SideEffectEnqueuedAt = time.Now()
					s.runtime.opts.onBar(task.Bar)
				}
			}
		}
	}
}

func sameRawMarketData(a tickEvent, b tickEvent) bool {
	return strings.TrimSpace(a.InstrumentID) == strings.TrimSpace(b.InstrumentID) &&
		strings.TrimSpace(a.ExchangeID) == strings.TrimSpace(b.ExchangeID) &&
		strings.TrimSpace(a.RawTradingDay) == strings.TrimSpace(b.RawTradingDay) &&
		strings.TrimSpace(a.RawActionDay) == strings.TrimSpace(b.RawActionDay) &&
		strings.TrimSpace(a.UpdateTime) == strings.TrimSpace(b.UpdateTime) &&
		a.UpdateMillisec == b.UpdateMillisec &&
		a.LastPrice == b.LastPrice &&
		a.Volume == b.Volume &&
		a.OpenInterest == b.OpenInterest &&
		a.SettlementPrice == b.SettlementPrice &&
		a.BidPrice1 == b.BidPrice1 &&
		a.AskPrice1 == b.AskPrice1
}

func tickSessionDistanceMinutes(updateTime string, sessions []sessiontime.Range) (int, error) {
	clockTime, err := time.ParseInLocation("15:04:05", strings.TrimSpace(updateTime), time.Local)
	if err != nil {
		return 0, err
	}
	rawMinute := clockTime.Hour()*60 + clockTime.Minute()
	return sessiontime.DistanceToTradingWindow(rawMinute, sessions), nil
}

func formatTickForLog(ev tickEvent, raw bool) string {
	actionDay := strings.TrimSpace(ev.ActionDay)
	tradingDay := strings.TrimSpace(ev.TradingDay)
	if raw {
		if strings.TrimSpace(ev.RawActionDay) != "" {
			actionDay = strings.TrimSpace(ev.RawActionDay)
		}
		if strings.TrimSpace(ev.RawTradingDay) != "" {
			tradingDay = strings.TrimSpace(ev.RawTradingDay)
		}
	}
	return fmt.Sprintf(
		"received_at=%s instrument_id=%s exchange_id=%s trading_day=%s action_day=%s update_time=%s update_millisec=%d last_price=%.8f volume=%d open_interest=%.8f settlement_price=%.8f bid_price1=%.8f ask_price1=%.8f",
		ev.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		strings.TrimSpace(ev.InstrumentID),
		strings.TrimSpace(ev.ExchangeID),
		tradingDay,
		actionDay,
		strings.TrimSpace(ev.UpdateTime),
		ev.UpdateMillisec,
		ev.LastPrice,
		ev.Volume,
		ev.OpenInterest,
		ev.SettlementPrice,
		ev.BidPrice1,
		ev.AskPrice1,
	)
}

func previousGeneratedBarBase(state *instrumentRuntimeState, currentVol int) int {
	if state == nil || !state.hasPrevGeneratedBarVolume {
		return 0
	}
	if currentVol < state.prevGeneratedBarCloseVol {
		return 0
	}
	return state.prevGeneratedBarCloseVol
}

func (s *marketDataShard) buildPersistTasks(closedBar minuteBar, trace runtimeTrace, flush bool) []persistTask {
	tasks := make([]persistTask, 0, 8)

	// 第一步：先把刚封口的合约 1m bar 变成落库任务。
	tableName, err := tableNameForVariety(closedBar.Variety)
	if err == nil {
		tasks = append(tasks, persistTask{
			Bar:          closedBar,
			TableName:    tableName,
			Trace:        trace,
			InstrumentID: closedBar.InstrumentID,
			ShardID:      s.id,
			Replay:       closedBar.Replay,
		})
	}

	sessions, err := s.runtime.sessionResolver.Sessions(closedBar.Variety)
	if err == nil && s.runtime.opts.enableMultiMinute {
		state := s.state[closedBar.InstrumentID]
		if state != nil && state.tracker != nil {
			aggBars, _ := state.tracker.ConsumeFinal(closedBar, sessions)
			if flush {
				aggBars = append(aggBars, state.tracker.Flush()...)
			}
			sortTimeframeBars(aggBars)
			if mmTable, mmErr := instrumentMMTableName(closedBar.Variety); mmErr == nil {
				for _, aggBar := range aggBars {
					tasks = append(tasks, persistTask{
						Bar:          aggBar,
						TableName:    mmTable,
						Trace:        trace,
						InstrumentID: aggBar.InstrumentID,
						ShardID:      s.id,
						Replay:       closedBar.Replay,
					})
				}
			}
		}
	}

	if s.runtime.l9Async != nil {
		// 第三步：把这根 1m 记入品种分钟快照，并异步触发一次 L9 计算。
		// L9 的 1m/mm 不在当前 goroutine 里同步生成，避免拖慢主 shard。
		s.runtime.l9Async.ObserveMinuteBar(closedBar)
		s.runtime.l9Async.Submit(closedBar.Variety, closedBar.MinuteTime)
	}
	return tasks
}

func (s *marketDataShard) restoreTimeframeState(state *instrumentRuntimeState, instrumentID string, variety string, tradingDay string) error {
	if s == nil || state == nil || state.tracker == nil {
		return nil
	}
	if strings.TrimSpace(tradingDay) == "" || state.restoredTradingDay == tradingDay {
		return nil
	}
	bars, err := s.runtime.store.QueryMinuteBarsForTradingDay(variety, instrumentID, false, tradingDay)
	if err != nil {
		return err
	}
	state.tracker.Reset()
	state.tracker.RestoreFinals(bars, mustSessionsCopy(s.runtime.sessionResolver, variety))
	state.restoredTradingDay = tradingDay
	return nil
}

func (s *marketDataShard) emitHigherTimeframePartials(state *instrumentRuntimeState, sessions []sessiontime.Range) {
	if s == nil || state == nil || state.tracker == nil || s.runtime.opts.onPartialBar == nil || !state.hasBar {
		return
	}
	partials := state.tracker.BuildPartials(state.bar, sessions)
	sortTimeframeBars(partials)
	for _, bar := range partials {
		onPartialBarRateProbe.Inc()
		s.runtime.opts.onPartialBar(bar)
	}
}

func (s *marketDataShard) flush() error {
	var tasks []persistTask
	for _, state := range s.state {
		if state == nil || !state.hasBar {
			continue
		}
		trace := runtimeTrace{
			ReceivedAt:        runtimeMetricTime(state.lastTick.ReceivedAt, state.bar.Replay, time.Now()),
			StateUpdatedAt:    time.Now(),
			MinuteClosedAt:    time.Now(),
			PersistEnqueuedAt: time.Now(),
		}
		tasks = append(tasks, s.buildPersistTasks(state.bar, trace, true)...)
	}
	if len(tasks) > 0 {
		s.runtime.enqueuePersistTasks(tasks)
	}
	if s.fileWriter != nil {
		if err := s.fileWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func toKlineAggSessions(in []sessiontime.Range) []klineagg.SessionRange {
	if len(in) == 0 {
		return nil
	}
	out := make([]klineagg.SessionRange, 0, len(in))
	for _, item := range in {
		out = append(out, klineagg.SessionRange{
			Start: item.Start,
			End:   item.End,
		})
	}
	return out
}

func mustSessionsCopy(resolver *sessionResolver, variety string) []sessiontime.Range {
	if resolver == nil {
		return nil
	}
	sessions, err := resolver.Sessions(variety)
	if err != nil {
		return nil
	}
	out := make([]sessiontime.Range, len(sessions))
	copy(out, sessions)
	return out
}

func instrumentMMTableName(variety string) (string, error) {
	return fmt.Sprintf("%s%s", instrumentMMTablePrefix, normalizeVariety(variety)), nil
}
