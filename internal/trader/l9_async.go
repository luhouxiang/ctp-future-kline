package trader

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
)

type l9Task struct {
	variety    string
	minuteTime time.Time
}

type l9AsyncCalculator struct {
	store *klineStore
	clock *klineclock.CalendarResolver

	enabled atomic.Bool
	tasks   chan l9Task
	wg      sync.WaitGroup

	mu                sync.RWMutex
	expected          map[string]map[string]struct{}
	latest            map[string]minuteBar
	minuteBars        map[string]map[int64]map[string]minuteBar
	instrumentVariety map[string]string
}

func newL9AsyncCalculator(store *klineStore, enabled bool, workers int, expectedByVariety map[string][]string) *l9AsyncCalculator {
	if workers <= 0 {
		workers = 1
	}
	c := &l9AsyncCalculator{
		store:             store,
		clock:             klineclock.NewCalendarResolver(store.DB()),
		tasks:             make(chan l9Task, 1024),
		expected:          make(map[string]map[string]struct{}),
		latest:            make(map[string]minuteBar),
		minuteBars:        make(map[string]map[int64]map[string]minuteBar),
		instrumentVariety: make(map[string]string),
	}
	c.enabled.Store(enabled)

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
	select {
	case c.tasks <- task:
	default:
		logger.Error("l9 task queue full, dropping task", "variety", task.variety, "minute", task.minuteTime.Format("2006-01-02 15:04:00"))
	}
}

func (c *l9AsyncCalculator) Close() {
	close(c.tasks)
	c.wg.Wait()
}

func (c *l9AsyncCalculator) worker() {
	defer c.wg.Done()
	for task := range c.tasks {
		if err := c.computeAndStore(task.variety, task.minuteTime); err != nil {
			logger.Error("compute l9 failed", "variety", task.variety, "minute", task.minuteTime.Format("2006-01-02 15:04:00"), "error", err)
		}
	}
}

func (c *l9AsyncCalculator) computeAndStore(variety string, minuteTime time.Time) error {
	bars := c.snapshotBarsForMinute(variety, minuteTime)
	if len(bars) == 0 {
		return nil
	}

	totalOI := 0.0
	weightedOpen := 0.0
	weightedHigh := 0.0
	weightedLow := 0.0
	weightedClose := 0.0
	weightedSettlement := 0.0
	totalVolume := int64(0)

	for _, bar := range bars {
		if bar.OpenInterest <= 0 {
			continue
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

	l9Bar := minuteBar{
		Variety:         variety,
		InstrumentID:    variety + "l9",
		Exchange:        "L9",
		MinuteTime:      minuteTime,
		AdjustedTime:    c.adjustedMinuteTime(minuteTime),
		Period:          "1m",
		Open:            weightedOpen / totalOI,
		High:            weightedHigh / totalOI,
		Low:             weightedLow / totalOI,
		Close:           weightedClose / totalOI,
		Volume:          totalVolume,
		OpenInterest:    totalOI,
		SettlementPrice: weightedSettlement / totalOI,
	}

	if err := c.store.UpsertL9MinuteBar(l9Bar); err != nil {
		return fmt.Errorf("upsert l9 bar failed: %w", err)
	}
	return nil
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
		sort.Strings(instrumentIDs)
	}
	if len(instrumentIDs) == 0 {
		return nil
	}

	var bars []minuteBar
	for _, instrumentID := range instrumentIDs {
		if bar, ok := c.minuteBarLocked(variety, minuteKey, instrumentID); ok {
			bars = append(bars, bar)
			continue
		}
		latest, ok := c.latest[instrumentID]
		if !ok || latest.Variety != variety {
			continue
		}
		price := latest.Close
		bars = append(bars, minuteBar{
			Variety:         variety,
			InstrumentID:    instrumentID,
			Exchange:        latest.Exchange,
			MinuteTime:      minuteTime,
			AdjustedTime:    c.adjustedMinuteTime(minuteTime),
			Period:          "1m",
			Open:            price,
			High:            price,
			Low:             price,
			Close:           price,
			Volume:          0,
			OpenInterest:    latest.OpenInterest,
			SettlementPrice: latest.SettlementPrice,
		})
	}
	return bars
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
	sort.Strings(out)
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
