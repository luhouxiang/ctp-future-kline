// l9_async.go 负责主连/L9 的异步计算。
// 它在不阻塞实时 1m 聚合主路径的前提下，根据品种内多合约分钟线计算加权结果并回写存储层。
package quotes

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"ctp-future-kline/internal/klineclock"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/sessiontime"
)

type l9Task struct {
	variety    string
	minuteTime time.Time
}

type l9AsyncCalculator struct {
	store  *klineStore
	clock  *klineclock.CalendarResolver
	metaDB *sql.DB

	enabled     atomic.Bool
	tasks       chan l9Task
	queueHandle *queuewatch.QueueHandle
	spool       *queuewatch.JSONSpool[l9Task]
	wg          sync.WaitGroup

	mu                sync.RWMutex
	expected          map[string]map[string]struct{}
	latest            map[string]minuteBar
	minuteBars        map[string]map[int64]map[string]minuteBar
	instrumentVariety map[string]string
	trackers          map[string]*timeframeTracker
	restoredDays      map[string]string
	persistSink       func([]persistTask)
}

func newL9AsyncCalculator(store *klineStore, metaDB *sql.DB, status *RuntimeStatusCenter, enabled bool, workers int, expectedByVariety map[string][]string) *l9AsyncCalculator {
	if workers <= 0 {
		workers = 1
	}
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	c := &l9AsyncCalculator{
		store:             store,
		clock:             klineclock.NewCalendarResolver(preferMetaDB(metaDB, store.DB())),
		metaDB:            preferMetaDB(metaDB, store.DB()),
		tasks:             make(chan l9Task, queueCfg.L9TaskCapacity),
		expected:          make(map[string]map[string]struct{}),
		latest:            make(map[string]minuteBar),
		minuteBars:        make(map[string]map[int64]map[string]minuteBar),
		instrumentVariety: make(map[string]string),
		trackers:          make(map[string]*timeframeTracker),
		restoredDays:      make(map[string]string),
	}
	c.enabled.Store(enabled)
	if registry != nil {
		c.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        "l9_async_tasks",
			Category:    "quotes_primary",
			Criticality: "critical",
			Capacity:    queueCfg.L9TaskCapacity,
			LossPolicy:  "spill_to_disk",
			BasisText:   "L9 ???????????????????????????",
		})
		spool, err := queuewatch.NewJSONSpool[l9Task](queueCfg.SpoolDir, c.queueHandle.Name())
		if err != nil {
			logger.Error("init l9 task spool failed", "error", err)
		} else {
			c.spool = spool
			c.queueHandle.ObserveDepth(len(c.tasks) + spool.Pending())
		}
	}
	for variety, instruments := range expectedByVariety {
		nv := normalizeVariety(variety)
		if nv == "" {
			continue
		}
		set := c.expected[nv]
		if set == nil {
			set = make(map[string]struct{}, len(instruments))
			c.expected[nv] = set
		}
		for _, instrumentID := range instruments {
			if instrumentID == "" {
				continue
			}
			set[instrumentID] = struct{}{}
			c.instrumentVariety[instrumentID] = nv
		}
	}
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	return c
}

func (c *l9AsyncCalculator) SetPersistSink(fn func([]persistTask)) {
	c.mu.Lock()
	c.persistSink = fn
	c.mu.Unlock()
}

func (c *l9AsyncCalculator) Enable() {
	c.enabled.Store(true)
}

func (c *l9AsyncCalculator) Disable() {
	c.enabled.Store(false)
}

func (c *l9AsyncCalculator) ObserveMinuteBar(bar minuteBar) {
	if bar.InstrumentID == "" || bar.MinuteTime.IsZero() {
		return
	}
	bar.Variety = normalizeVariety(bar.Variety)
	if bar.Variety == "" {
		bar.Variety = normalizeVariety(bar.InstrumentID)
	}
	if bar.Variety == "" {
		return
	}
	if bar.AdjustedTime.IsZero() {
		bar.AdjustedTime = c.adjustedMinuteTime(bar.MinuteTime)
	}
	minuteKey := bar.MinuteTime.Unix()

	c.mu.Lock()
	defer c.mu.Unlock()

	// 同时维护：
	// 1) latest：该合约最近一根 1m，供后续分钟继续更新参考；
	// 2) minuteBars[variety][minute][instrument]：该品种该分钟真实生成的各合约 1m。
	c.latest[bar.InstrumentID] = bar
	c.instrumentVariety[bar.InstrumentID] = bar.Variety
	byMinute := c.minuteBars[bar.Variety]
	if byMinute == nil {
		byMinute = make(map[int64]map[string]minuteBar)
		c.minuteBars[bar.Variety] = byMinute
	}
	byInstrument := byMinute[minuteKey]
	if byInstrument == nil {
		byInstrument = make(map[string]minuteBar)
		byMinute[minuteKey] = byInstrument
	}
	byInstrument[bar.InstrumentID] = bar
	c.pruneOldMinutesLocked(bar.Variety, minuteKey)
}

func (c *l9AsyncCalculator) Submit(variety string, minuteTime time.Time) {
	if !c.enabled.Load() {
		return
	}
	task := l9Task{variety: normalizeVariety(variety), minuteTime: minuteTime}
	if task.variety == "" || task.minuteTime.IsZero() {
		return
	}
	// 这里只负责投递异步任务；真正的加权和落库在 worker 线程中完成。
	select {
	case c.tasks <- task:
	default:
		logger.Warn("l9 task queue full, dropping task", "variety", task.variety, "minute", task.minuteTime.Format("2006-01-02 15:04:00"))
	}
}

func (c *l9AsyncCalculator) Close() {
	if c.spool != nil {
		for {
			task, ok, _, err := c.spool.Dequeue()
			if err != nil {
				logger.Error("drain l9 spool before close failed", "error", err)
				break
			}
			if !ok {
				break
			}
			c.tasks <- task
			depth := len(c.tasks) + c.spool.Pending()
			if c.queueHandle != nil {
				c.queueHandle.MarkSpillRecovered(depth)
				c.queueHandle.MarkEnqueued(depth)
			}
		}
	}
	close(c.tasks)
	c.wg.Wait()
	c.flushTrackers()
}

func (c *l9AsyncCalculator) worker() {
	defer c.wg.Done()
	for {
		select {
		case task, ok := <-c.tasks:
			if !ok {
				return
			}
			depth := len(c.tasks)
			if c.spool != nil {
				depth += c.spool.Pending()
			}
			if c.queueHandle != nil {
				c.queueHandle.MarkDequeued(depth)
			}
			if err := c.computeAndStore(task.variety, task.minuteTime); err != nil {
				logger.Error("compute l9 failed", "variety", task.variety, "minute", task.minuteTime.Format("2006-01-02 15:04:00"), "error", err)
			}
		default:
			if c.spool != nil {
				if task, ok, _, err := c.spool.Dequeue(); err == nil && ok {
					depth := len(c.tasks) + c.spool.Pending()
					if c.queueHandle != nil {
						c.queueHandle.MarkSpillRecovered(depth)
						c.queueHandle.MarkDequeued(depth)
					}
					if err := c.computeAndStore(task.variety, task.minuteTime); err != nil {
						logger.Error("compute l9 failed", "variety", task.variety, "minute", task.minuteTime.Format("2006-01-02 15:04:00"), "error", err)
					}
					continue
				} else if err != nil {
					logger.Error("dequeue l9 spool failed", "error", err)
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *l9AsyncCalculator) computeAndStore(variety string, minuteTime time.Time) error {
	bars := c.snapshotBarsForMinute(variety, minuteTime)
	if len(bars) == 0 {
		return nil
	}

	// 按持仓量 OpenInterest 做加权，得到该品种当前分钟的 L9 1m bar。
	totalOI := 0.0
	weightedOpen := 0.0
	weightedHigh := 0.0
	weightedLow := 0.0
	weightedClose := 0.0
	weightedSettlement := 0.0
	totalVolume := int64(0)
	sourceReceivedAt := time.Time{}
	for _, bar := range bars {
		if bar.OpenInterest <= 0 {
			continue
		}
		if sourceReceivedAt.IsZero() || bar.SourceReceivedAt.After(sourceReceivedAt) {
			sourceReceivedAt = bar.SourceReceivedAt
		}
		w := bar.OpenInterest
		totalOI += w
		weightedOpen += bar.Open * w
		weightedHigh += bar.High * w
		weightedLow += bar.Low * w
		weightedClose += bar.Close * w
		weightedSettlement += bar.SettlementPrice * w
		totalVolume += bar.Volume
	}
	if totalOI <= 0 {
		return nil
	}

	// L9 不是交易所原生合约，统一落成 <variety>l9 / Exchange=L9。
	l9Bar := minuteBar{
		Variety:          variety,
		InstrumentID:     variety + "l9",
		Exchange:         "L9",
		Replay:           len(bars) > 0 && bars[0].Replay,
		MinuteTime:       minuteTime,
		AdjustedTime:     c.adjustedMinuteTime(minuteTime),
		SourceReceivedAt: sourceReceivedAt,
		Period:           "1m",
		Open:             weightedOpen / totalOI,
		High:             weightedHigh / totalOI,
		Low:              weightedLow / totalOI,
		Close:            weightedClose / totalOI,
		Volume:           totalVolume,
		OpenInterest:     totalOI,
		SettlementPrice:  weightedSettlement / totalOI,
	}

	c.mu.Lock()
	tracker := c.trackers[variety]
	if tracker == nil {
		tracker = newTimeframeTracker()
		c.trackers[variety] = tracker
	}
	restoredDay := c.restoredDays[variety]
	sink := c.persistSink
	c.mu.Unlock()

	tasks := make([]persistTask, 0, 8)
	if sink == nil {
		if err := c.store.UpsertL9MinuteBar(l9Bar); err != nil {
			return err
		}
	} else {
		// runtime 模式下不直接写库，而是复用统一的 persistTask -> dbBatchWriter 链路。
		tableName, err := tableNameForL9Variety(variety)
		if err != nil {
			return err
		}
		trace := runtimeTrace{
			ReceivedAt:        runtimeMetricTime(sourceReceivedAt, l9Bar.Replay, time.Now()),
			MinuteClosedAt:    time.Now(),
			PersistEnqueuedAt: time.Now(),
		}
		tasks = append(tasks, persistTask{
			Bar:          l9Bar,
			TableName:    tableName,
			Trace:        trace,
			InstrumentID: l9Bar.InstrumentID,
			IsL9:         true,
			Replay:       l9Bar.Replay,
		})
	}

	sessions, err := c.loadSessions(variety)
	if err == nil {
		if err := c.restoreTrackerForTradingDay(variety, tracker, restoredDay, l9Bar, sessions); err != nil {
			return err
		}
		for _, bar := range trackerConsumeFinals(tracker, l9Bar, sessions) {
			if sink == nil {
				tableName, tableErr := l9MMTableName(variety)
				if tableErr != nil {
					return tableErr
				}
				if err := c.store.upsertMinuteBarToTable(tableName, bar); err != nil {
					return err
				}
			} else {
				tableName, tableErr := l9MMTableName(variety)
				if tableErr != nil {
					return tableErr
				}
				tasks = append(tasks, persistTask{
					Bar:          bar,
					TableName:    tableName,
					Trace:        runtimeTrace{ReceivedAt: runtimeMetricTime(sourceReceivedAt, l9Bar.Replay, time.Now()), PersistEnqueuedAt: time.Now()},
					InstrumentID: bar.InstrumentID,
					IsL9:         true,
					Replay:       l9Bar.Replay,
				})
			}
		}
	}
	if sink != nil && len(tasks) > 0 {
		sink(tasks)
	}
	return nil
}

func trackerConsumeFinals(tracker *timeframeTracker, bar minuteBar, sessions []sessiontime.Range) []minuteBar {
	if tracker == nil {
		return nil
	}
	out, _ := tracker.ConsumeFinal(bar, sessions)
	sortTimeframeBars(out)
	return out
}

func (c *l9AsyncCalculator) restoreTrackerForTradingDay(variety string, tracker *timeframeTracker, restoredDay string, current minuteBar, sessions []sessiontime.Range) error {
	if c == nil || tracker == nil {
		return nil
	}
	tradingDay := tradingDayKey(current)
	if tradingDay == "" || tradingDay == restoredDay {
		return nil
	}
	bars, err := c.store.QueryMinuteBarsForTradingDay(variety, current.InstrumentID, true, tradingDay)
	if err != nil {
		return err
	}
	restoreBars := bars[:0]
	for _, bar := range bars {
		if !current.MinuteTime.IsZero() && !bar.MinuteTime.Before(current.MinuteTime) {
			continue
		}
		restoreBars = append(restoreBars, bar)
	}
	tracker.Reset()
	tracker.RestoreFinals(restoreBars, sessions)
	c.mu.Lock()
	c.restoredDays[variety] = tradingDay
	c.mu.Unlock()
	return nil
}

func (c *l9AsyncCalculator) flushTrackers() {
	if c == nil {
		return
	}
	c.mu.Lock()
	trackers := make(map[string]*timeframeTracker, len(c.trackers))
	sink := c.persistSink
	for variety, tracker := range c.trackers {
		trackers[variety] = tracker
	}
	c.mu.Unlock()

	for variety, tracker := range trackers {
		if tracker == nil {
			continue
		}
		bars := tracker.Flush()
		if len(bars) == 0 {
			continue
		}
		sortTimeframeBars(bars)
		if sink == nil {
			tableName, err := l9MMTableName(variety)
			if err != nil {
				continue
			}
			for _, bar := range bars {
				_ = c.store.upsertMinuteBarToTable(tableName, bar)
			}
			continue
		}
		tableName, err := l9MMTableName(variety)
		if err != nil {
			continue
		}
		tasks := make([]persistTask, 0, len(bars))
		for _, bar := range bars {
			tasks = append(tasks, persistTask{
				Bar:          bar,
				TableName:    tableName,
				Trace:        runtimeTrace{ReceivedAt: runtimeMetricTime(bar.SourceReceivedAt, bar.Replay, time.Now()), PersistEnqueuedAt: time.Now()},
				InstrumentID: bar.InstrumentID,
				IsL9:         true,
				Replay:       bar.Replay,
			})
		}
		sink(tasks)
	}
}

func (c *l9AsyncCalculator) snapshotBarsForMinute(variety string, minuteTime time.Time) []minuteBar {
	variety = normalizeVariety(variety)
	if variety == "" {
		return nil
	}
	minuteKey := minuteTime.Unix()

	c.mu.RLock()
	defer c.mu.RUnlock()

	instrumentIDs := c.expectedInstrumentsLocked(variety)
	if len(instrumentIDs) == 0 {
		for instrumentID, v := range c.instrumentVariety {
			if v == variety {
				instrumentIDs = append(instrumentIDs, instrumentID)
			}
		}
	}
	if len(instrumentIDs) == 0 {
		return nil
	}

	out := make([]minuteBar, 0, len(instrumentIDs))
	for _, instrumentID := range instrumentIDs {
		if bar, ok := c.minuteBarLocked(variety, minuteKey, instrumentID); ok {
			out = append(out, bar)
		}
	}
	return out
}

func (c *l9AsyncCalculator) expectedInstrumentsLocked(variety string) []string {
	set := c.expected[variety]
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for instrumentID := range set {
		out = append(out, instrumentID)
	}
	return out
}

func (c *l9AsyncCalculator) minuteBarLocked(variety string, minuteKey int64, instrumentID string) (minuteBar, bool) {
	byMinute := c.minuteBars[variety]
	if byMinute == nil {
		return minuteBar{}, false
	}
	byInstrument := byMinute[minuteKey]
	if byInstrument == nil {
		return minuteBar{}, false
	}
	bar, ok := byInstrument[instrumentID]
	return bar, ok
}

func (c *l9AsyncCalculator) pruneOldMinutesLocked(variety string, currentMinuteKey int64) {
	byMinute := c.minuteBars[variety]
	if byMinute == nil {
		return
	}
	keepFrom := currentMinuteKey - 15*60
	for minuteKey := range byMinute {
		if minuteKey < keepFrom {
			delete(byMinute, minuteKey)
		}
	}
}

func (c *l9AsyncCalculator) adjustedMinuteTime(minuteTime time.Time) time.Time {
	day := time.Date(minuteTime.Year(), minuteTime.Month(), minuteTime.Day(), 0, 0, 0, 0, time.Local)
	_, adjusted, err := klineclock.BuildBarTimes(day, klineclock.HHMMFromTime(minuteTime), c.clock)
	if err != nil {
		return minuteTime
	}
	return adjusted
}

func (c *l9AsyncCalculator) loadSessions(variety string) ([]sessiontime.Range, error) {
	resolver := newSessionResolver(preferMetaDB(c.metaDB, c.store.DB()))
	return resolver.Sessions(variety)
}

func l9MMTableName(variety string) (string, error) {
	return TableNameForL9MMVariety(variety)
}
