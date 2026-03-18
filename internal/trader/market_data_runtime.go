package trader

import (
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ctp-go-demo/internal/klineagg"
	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/sessiontime"
)

const (
	defaultMarketDataShardCount   = 16
	defaultShardChannelCapacity   = 8192
	defaultPersistQueueCapacity   = 16384
	defaultFileQueueCapacity      = 16384
	defaultDBWriterCount          = 4
	defaultDBFlushBatch           = 200
	defaultDBFlushInterval        = 100 * time.Millisecond
	defaultFileFlushInterval      = 200 * time.Millisecond
	defaultFileFsyncInterval      = time.Second
	defaultSlowLogInterval        = 5 * time.Second
	defaultTickHistoryRetention   = 2048
	defaultL9TaskQueueCapacity    = 4096
	defaultRuntimeMetricsInterval = 5 * time.Second
)

type runtimeOptions struct {
	tickDedupWindow  time.Duration
	driftThreshold   time.Duration
	driftResumeTicks int
	flowPath         string
	onTick           func(tickEvent)
	onBar            func(minuteBar)
}

type runtimeTick struct {
	tickEvent
	replay bool
}

type runtimeTrace struct {
	ReceivedAt        time.Time
	RouteEnqueuedAt   time.Time
	ShardDequeuedAt   time.Time
	StateUpdatedAt    time.Time
	MinuteClosedAt    time.Time
	PersistEnqueuedAt time.Time
	DBDequeuedAt      time.Time
	DBFlushedAt       time.Time
}

type persistTask struct {
	Bar          minuteBar
	TableName    string
	Trace        runtimeTrace
	InstrumentID string
	ShardID      int
	IsL9         bool
}

type flushRequest struct {
	done chan error
}

type stopRequest struct {
	done chan struct{}
}

type instrumentRuntimeState struct {
	bar                 minuteBar
	lastTick            minuteTickSnapshot
	hasBar              bool
	prevBucketCloseVol  int
	hasPrevBucketVolume bool
	lastFingerprint     tickFingerprintState
	lastVolumes         int
	aggregator          *closedBarAggregator
}

type closedBarAggregator struct {
	instrumentID string
	exchange     string
	history      []minuteBar
	lastEmitted  map[string]time.Time
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
	if daily, ok := a.consumeDaily(closedBar, flush); ok {
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

func (a *closedBarAggregator) consumeDaily(closedBar minuteBar, flush bool) (minuteBar, bool) {
	if len(a.history) == 0 {
		return minuteBar{}, false
	}
	currentDay := chooseAdjustedTime(closedBar).Format("2006-01-02")
	targetDay := currentDay
	if !flush && len(a.history) >= 2 {
		prev := chooseAdjustedTime(a.history[len(a.history)-2]).Format("2006-01-02")
		if prev == currentDay {
			return minuteBar{}, false
		}
		targetDay = prev
	}

	lastEmittedAt := a.lastEmitted["1d"]
	var dayBars []minuteBar
	for _, bar := range a.history {
		if chooseAdjustedTime(bar).Format("2006-01-02") != targetDay {
			continue
		}
		dayBars = append(dayBars, bar)
	}
	if len(dayBars) == 0 {
		return minuteBar{}, false
	}
	lastBar := dayBars[len(dayBars)-1]
	if !flush && chooseAdjustedTime(lastBar).Format("2006-01-02") == currentDay && targetDay == currentDay {
		return minuteBar{}, false
	}
	labelTime := time.Date(
		chooseAdjustedTime(dayBars[0]).Year(),
		chooseAdjustedTime(dayBars[0]).Month(),
		chooseAdjustedTime(dayBars[0]).Day(),
		0, 0, 0, 0,
		time.Local,
	)
	if !labelTime.After(lastEmittedAt) {
		return minuteBar{}, false
	}
	daily := minuteBar{
		Variety:          closedBar.Variety,
		InstrumentID:     a.instrumentID,
		Exchange:         a.exchange,
		MinuteTime:       time.Date(dayBars[0].MinuteTime.Year(), dayBars[0].MinuteTime.Month(), dayBars[0].MinuteTime.Day(), 0, 0, 0, 0, time.Local),
		AdjustedTime:     labelTime,
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
	a.lastEmitted["1d"] = labelTime
	return daily, true
}

type marketDataRuntime struct {
	store           *klineStore
	status          *RuntimeStatusCenter
	clock           *klineclock.CalendarResolver
	sessionResolver *sessionResolver
	l9Async         *l9AsyncCalculator
	opts            runtimeOptions

	shards    []*marketDataShard
	dbWriter  *dbBatchWriter
	closed    atomic.Bool
	logMu     sync.Mutex
	lastLogAt map[string]time.Time

	replayMu             sync.Mutex
	replayProcessSeen    map[string]struct{}
	replayPipelineSeen   map[string]struct{}
	shardQueueDepthGauge []int64
}

type marketDataShard struct {
	runtime *marketDataRuntime
	id      int
	in      chan any

	state      map[string]*instrumentRuntimeState
	fileWriter *shardFileWriter
}

func newMarketDataRuntime(store *klineStore, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter, opts runtimeOptions) *marketDataRuntime {
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
		clock:              klineclock.NewCalendarResolver(store.DB()),
		sessionResolver:    newSessionResolver(store.DB()),
		l9Async:            l9Async,
		opts:               opts,
		lastLogAt:          make(map[string]time.Time),
		replayProcessSeen:  make(map[string]struct{}),
		replayPipelineSeen: make(map[string]struct{}),
	}
	shardCount := defaultMarketDataShardCount
	rt.shardQueueDepthGauge = make([]int64, shardCount)
	rt.dbWriter = newDBBatchWriter(store, status, defaultDBWriterCount, defaultPersistQueueCapacity, defaultDBFlushBatch, defaultDBFlushInterval)
	if l9Async != nil {
		l9Async.SetPersistSink(rt.enqueuePersistTasks)
	}
	rt.shards = make([]*marketDataShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shard := &marketDataShard{
			runtime: rt,
			id:      i,
			in:      make(chan any, defaultShardChannelCapacity),
			state:   make(map[string]*instrumentRuntimeState),
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

func (r *marketDataRuntime) onLiveTick(in tickInputData) error {
	return r.enqueue(runtimeTick{
		tickEvent: tickEvent{
			InstrumentID:    strings.TrimSpace(in.InstrumentID),
			ExchangeID:      strings.TrimSpace(in.ExchangeID),
			ActionDay:       strings.TrimSpace(in.ActionDay),
			TradingDay:      strings.TrimSpace(in.TradingDay),
			UpdateTime:      strings.TrimSpace(in.UpdateTime),
			UpdateMillisec:  in.UpdateMillisec,
			ReceivedAt:      in.ReceivedAt,
			CallbackAt:      in.CallbackAt,
			LastPrice:       in.LastPrice,
			Volume:          in.Volume,
			OpenInterest:    in.OpenInterest,
			SettlementPrice: in.SettlementPrice,
			BidPrice1:       in.BidPrice1,
			AskPrice1:       in.AskPrice1,
		},
	})
}

func (r *marketDataRuntime) onReplayTick(ev tickEvent) error {
	r.logFirstReplayProcess(ev)
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
	select {
	case r.shards[shardID].in <- t:
		atomic.StoreInt64(&r.shardQueueDepthGauge[shardID], int64(len(r.shards[shardID].in)))
		if r.status != nil {
			routerQueueMS := routeAt.Sub(t.CallbackAt).Seconds() * 1000
			r.status.MarkRouterLatency(instrumentID, routerQueueMS)
		}
		return nil
	default:
		if r.status != nil {
			r.status.MarkTickDropped()
		}
		r.maybeWarn("shard_queue_full:"+instrumentID, "market data shard queue full, dropping tick",
			"instrument_id", instrumentID,
			"shard_id", shardID,
			"queue_depth", len(r.shards[shardID].in),
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

func (r *marketDataRuntime) enqueuePersistTasks(tasks []persistTask) {
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		r.dbWriter.Enqueue(task)
	}
}

func (s *marketDataShard) run() {
	for msg := range s.in {
		switch m := msg.(type) {
		case runtimeTick:
			s.processTick(m)
		case flushRequest:
			m.done <- s.flush()
		case stopRequest:
			close(m.done)
			return
		}
	}
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

	state := s.state[instrumentID]
	if state == nil {
		state = &instrumentRuntimeState{
			aggregator: newClosedBarAggregator(instrumentID, exchangeID),
		}
		s.state[instrumentID] = state
	}

	fingerprint := buildTickDedupFingerprint(tickInputData{
		InstrumentID:   instrumentID,
		ExchangeID:     exchangeID,
		ActionDay:      t.ActionDay,
		TradingDay:     t.TradingDay,
		UpdateTime:     t.UpdateTime,
		UpdateMillisec: t.UpdateMillisec,
	}, price, currentVol, openInterest)
	if s.runtime.opts.tickDedupWindow > 0 && fingerprint == state.lastFingerprint.fingerprint && now.Sub(state.lastFingerprint.at) <= s.runtime.opts.tickDedupWindow {
		if s.runtime.status != nil {
			s.runtime.status.MarkTickDedupDropped()
		}
		return
	}
	state.lastFingerprint = tickFingerprintState{fingerprint: fingerprint, at: now}

	upstreamLagMS := now.Sub(adjustedTickTime).Seconds() * 1000
	shardQueueMS := dequeuedAt.Sub(t.ProcessStartedAt).Seconds() * 1000
	if shardQueueMS < 0 {
		shardQueueMS = 0
	}
	if s.runtime.status != nil {
		s.runtime.status.MarkShardLatency(instrumentID, s.id, shardQueueMS)
	}
	if shouldCheckTickDrift(now, adjustedTickTime) {
		driftSec := math.Abs(now.Sub(adjustedTickTime).Seconds())
		if s.runtime.status != nil {
			s.runtime.status.MarkDrift(driftSec, false)
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
	if s.fileWriter != nil {
		s.fileWriter.Enqueue(t.tickEvent)
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
		state.bar = minuteBar{
			Variety:          variety,
			InstrumentID:     instrumentID,
			Exchange:         exchangeID,
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: now,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           0,
			OpenInterest:     openInterest,
			SettlementPrice:  settlement,
		}
		state.lastTick = lastTick
		state.hasBar = true
		return
	}

	if !state.bar.MinuteTime.Equal(minuteTime) {
		closed := state.bar
		closed.SourceReceivedAt = state.lastTick.ReceivedAt
		trace := runtimeTrace{
			ReceivedAt:        t.ReceivedAt,
			RouteEnqueuedAt:   t.ProcessStartedAt,
			ShardDequeuedAt:   dequeuedAt,
			StateUpdatedAt:    time.Now(),
			MinuteClosedAt:    time.Now(),
			PersistEnqueuedAt: time.Now(),
		}
		persisted = append(persisted, s.buildPersistTasks(closed, trace, false)...)

		started := minuteBar{
			Variety:          variety,
			InstrumentID:     instrumentID,
			Exchange:         exchangeID,
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: now,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           computeBucketVolume(currentVol, state.lastTick.CurrentVolume, true),
			OpenInterest:     openInterest,
			SettlementPrice:  settlement,
		}
		if started.Volume < 0 {
			started.Volume = 0
		}
		state.prevBucketCloseVol = state.lastTick.CurrentVolume
		state.hasPrevBucketVolume = true
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
		state.bar.Volume = computeBucketVolume(currentVol, state.prevBucketCloseVol, state.hasPrevBucketVolume)
		if state.bar.Volume < 0 {
			state.bar.Volume = 0
		}
		state.bar.OpenInterest = openInterest
		state.bar.SettlementPrice = settlement
		state.bar.AdjustedTime = adjustedTime
		state.bar.SourceReceivedAt = now
		state.lastTick = lastTick
	}

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

func (s *marketDataShard) buildPersistTasks(closedBar minuteBar, trace runtimeTrace, flush bool) []persistTask {
	tasks := make([]persistTask, 0, 8)
	tableName, err := tableNameForVariety(closedBar.Variety)
	if err == nil {
		tasks = append(tasks, persistTask{
			Bar:          closedBar,
			TableName:    tableName,
			Trace:        trace,
			InstrumentID: closedBar.InstrumentID,
			ShardID:      s.id,
		})
	}

	sessions, err := s.runtime.sessionResolver.Sessions(closedBar.Variety)
	if err == nil {
		state := s.state[closedBar.InstrumentID]
		if state != nil {
			aggBars := state.aggregator.Consume(closedBar, sessions, flush)
			if mmTable, mmErr := instrumentMMTableName(closedBar.Variety); mmErr == nil {
				for _, aggBar := range aggBars {
					tasks = append(tasks, persistTask{
						Bar:          aggBar,
						TableName:    mmTable,
						Trace:        trace,
						InstrumentID: aggBar.InstrumentID,
						ShardID:      s.id,
					})
				}
			}
		}
	}
	if s.runtime.l9Async != nil {
		s.runtime.l9Async.ObserveMinuteBar(closedBar)
		s.runtime.l9Async.Submit(closedBar.Variety, closedBar.MinuteTime)
	}
	return tasks
}

func (s *marketDataShard) flush() error {
	var tasks []persistTask
	for _, state := range s.state {
		if state == nil || !state.hasBar {
			continue
		}
		trace := runtimeTrace{
			ReceivedAt:        state.lastTick.ReceivedAt,
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

func instrumentMMTableName(variety string) (string, error) {
	return fmt.Sprintf("%s%s", instrumentMMTablePrefix, normalizeVariety(variety)), nil
}
