package quotes

import (
	"sort"

	"ctp-future-kline/internal/klineagg"
	"ctp-future-kline/internal/klinesettings"
	"ctp-future-kline/internal/sessiontime"
)

var trackedRealtimeTimeframes = []struct {
	label   string
	minutes int
}{
	{label: "5m", minutes: 5},
	{label: "15m", minutes: 15},
	{label: "30m", minutes: 30},
	{label: "1h", minutes: 60},
	{label: "1d", minutes: 1440},
}

type timeframeTracker struct {
	states map[string]*timeframeAggregateState
}

type timeframeAggregateState struct {
	timeframe string
	minutes   int

	key             string
	tradingDay      string
	expectedMinutes int
	actualMinutes   int
	bar             minuteBar
	hasBar          bool
}

func newTimeframeTracker() *timeframeTracker {
	return newTimeframeTrackerWithFrames(trackedRealtimeTimeframes)
}

func newTimeframeTrackerForKind(kind string, settings klinesettings.Settings) *timeframeTracker {
	normalized := klinesettings.Normalize(settings)
	frames := make([]struct {
		label   string
		minutes int
	}, 0, len(trackedRealtimeTimeframes))
	for _, item := range trackedRealtimeTimeframes {
		if normalized.Enabled(kind, item.label) {
			frames = append(frames, item)
		}
	}
	return newTimeframeTrackerWithFrames(frames)
}

func newTimeframeTrackerWithFrames(frames []struct {
	label   string
	minutes int
}) *timeframeTracker {
	states := make(map[string]*timeframeAggregateState, len(frames))
	for _, item := range frames {
		states[item.label] = &timeframeAggregateState{
			timeframe: item.label,
			minutes:   item.minutes,
		}
	}
	return &timeframeTracker{states: states}
}

func (t *timeframeTracker) Reset() {
	if t == nil {
		return
	}
	for _, state := range t.states {
		state.reset()
	}
}

func (t *timeframeTracker) RestoreFinals(bars []minuteBar, sessions []sessiontime.Range) {
	if t == nil {
		return
	}
	for _, bar := range bars {
		_, _ = t.ConsumeFinal(bar, sessions)
	}
}

func (t *timeframeTracker) ConsumeFinal(bar minuteBar, sessions []sessiontime.Range) ([]minuteBar, bool) {
	if t == nil {
		return nil, false
	}
	out := make([]minuteBar, 0, len(t.states))
	changed := false
	for _, state := range t.states {
		finalized, ok := state.consumeFinal(bar, sessions)
		if !ok {
			continue
		}
		out = append(out, finalized)
		changed = true
	}
	return out, changed
}

func (t *timeframeTracker) BuildPartials(current minuteBar, sessions []sessiontime.Range) []minuteBar {
	if t == nil {
		return nil
	}
	out := make([]minuteBar, 0, len(t.states))
	for _, state := range t.states {
		partial, ok := state.buildPartial(current, sessions)
		if !ok {
			continue
		}
		out = append(out, partial)
	}
	return out
}

func (t *timeframeTracker) Flush() []minuteBar {
	if t == nil {
		return nil
	}
	out := make([]minuteBar, 0, len(t.states))
	for _, state := range t.states {
		if finalized, ok := state.flush(); ok {
			out = append(out, finalized)
		}
	}
	return out
}

func (s *timeframeAggregateState) consumeFinal(bar minuteBar, sessions []sessiontime.Range) (minuteBar, bool) {
	plan, ok := planTimeframeBucket(bar, s.timeframe, s.minutes, sessions)
	if !ok {
		return minuteBar{}, false
	}
	if !s.hasBar || s.key != plan.Key {
		s.key = plan.Key
		s.tradingDay = plan.TradingDay
		s.expectedMinutes = plan.ExpectedMinutes
		s.actualMinutes = 0
		s.bar = minuteBar{}
		s.hasBar = false
	}
	if !s.hasBar {
		s.bar = newTimeframeBarFromSource(bar, s.timeframe, plan)
		s.actualMinutes = 1
		s.hasBar = true
	} else {
		s.bar = mergeIntoTrackedBar(s.bar, bar, plan)
		s.actualMinutes += 1
	}
	if s.expectedMinutes > 0 && s.actualMinutes >= s.expectedMinutes {
		out := s.bar
		s.reset()
		return out, true
	}
	return minuteBar{}, false
}

func (s *timeframeAggregateState) buildPartial(current minuteBar, sessions []sessiontime.Range) (minuteBar, bool) {
	plan, ok := planTimeframeBucket(current, s.timeframe, s.minutes, sessions)
	if !ok {
		return minuteBar{}, false
	}
	if !s.hasBar || s.key != plan.Key {
		return newTimeframeBarFromSource(current, s.timeframe, plan), true
	}
	return mergeIntoTrackedBar(s.bar, current, plan), true
}

func (s *timeframeAggregateState) flush() (minuteBar, bool) {
	if s == nil || !s.hasBar {
		return minuteBar{}, false
	}
	out := s.bar
	s.reset()
	return out, true
}

func (s *timeframeAggregateState) reset() {
	if s == nil {
		return
	}
	s.key = ""
	s.tradingDay = ""
	s.expectedMinutes = 0
	s.actualMinutes = 0
	s.bar = minuteBar{}
	s.hasBar = false
}

func planTimeframeBucket(bar minuteBar, timeframe string, minutes int, sessions []sessiontime.Range) (klineagg.BucketPlan, bool) {
	if bar.MinuteTime.IsZero() {
		return klineagg.BucketPlan{}, false
	}
	return klineagg.PlanBucket(klineagg.MinuteBar{
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
	}, toKlineAggSessions(sessions), timeframe, minutes)
}

func newTimeframeBarFromSource(source minuteBar, timeframe string, plan klineagg.BucketPlan) minuteBar {
	return minuteBar{
		Variety:          source.Variety,
		InstrumentID:     source.InstrumentID,
		Exchange:         source.Exchange,
		Replay:           source.Replay,
		MinuteTime:       plan.DataTime,
		AdjustedTime:     plan.AdjustedTime,
		SourceReceivedAt: source.SourceReceivedAt,
		Period:           timeframe,
		Open:             source.Open,
		High:             source.High,
		Low:              source.Low,
		Close:            source.Close,
		Volume:           source.Volume,
		OpenInterest:     source.OpenInterest,
		SettlementPrice:  source.SettlementPrice,
	}
}

func mergeIntoTrackedBar(base minuteBar, source minuteBar, plan klineagg.BucketPlan) minuteBar {
	out := base
	if !source.SourceReceivedAt.IsZero() && (out.SourceReceivedAt.IsZero() || source.SourceReceivedAt.After(out.SourceReceivedAt)) {
		out.SourceReceivedAt = source.SourceReceivedAt
	}
	if source.High > out.High {
		out.High = source.High
	}
	if source.Low < out.Low {
		out.Low = source.Low
	}
	if out.Open == 0 && out.Close == 0 && out.High == 0 && out.Low == 0 {
		out.Open = source.Open
	}
	out.Close = source.Close
	out.Volume += source.Volume
	out.OpenInterest = source.OpenInterest
	out.SettlementPrice = source.SettlementPrice
	return out
}

func sortTimeframeBars(bars []minuteBar) {
	if len(bars) <= 1 {
		return
	}
	timeframeOrder := map[string]int{
		"1m":  0,
		"5m":  1,
		"15m": 2,
		"30m": 3,
		"1h":  4,
		"1d":  5,
	}
	sort.Slice(bars, func(i, j int) bool {
		oi := timeframeOrder[bars[i].Period]
		oj := timeframeOrder[bars[j].Period]
		if oi == oj {
			ti := chooseAdjustedTime(bars[i])
			tj := chooseAdjustedTime(bars[j])
			if ti.Equal(tj) {
				return bars[i].InstrumentID < bars[j].InstrumentID
			}
			return ti.Before(tj)
		}
		return oi < oj
	})
}
