package quotes

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/klineagg"
	"ctp-future-kline/internal/klineclock"
	"ctp-future-kline/internal/sessiontime"
)

type ChartSubscription struct {
	Symbol    string `json:"symbol"`
	Type      string `json:"type"`
	Variety   string `json:"variety"`
	Timeframe string `json:"timeframe"`
}

type ChartBar struct {
	AdjustedTime int64   `json:"adjusted_time"`
	DataTime     int64   `json:"data_time"`
	Open         float64 `json:"open"`
	High         float64 `json:"high"`
	Low          float64 `json:"low"`
	Close        float64 `json:"close"`
	Volume       int64   `json:"volume"`
	OpenInterest float64 `json:"open_interest"`
}

type ChartBarUpdate struct {
	Subscription ChartSubscription `json:"subscription"`
	Phase        string            `json:"phase"`
	Source       string            `json:"source"`
	Bar          ChartBar          `json:"bar"`
}

type chartTickSnapshot struct {
	instrumentID    string
	exchange        string
	variety         string
	minuteTime      time.Time
	adjustedTime    time.Time
	adjustedTick    time.Time
	receivedAt      time.Time
	price           float64
	openInterest    float64
	settlementPrice float64
}

type chartRootState struct {
	symbol         string
	kind           string
	variety        string
	exchange       string
	history1m      []minuteBar
	currentPartial *minuteBar
	latestTicks    map[string]chartTickSnapshot
}

type ChartStream struct {
	db              *sql.DB
	clock           *klineclock.CalendarResolver
	sessionResolver *sessionResolver

	mu          sync.RWMutex
	interests   map[string]int
	subscribers map[chan ChartBarUpdate]struct{}
	roots       map[string]*chartRootState
}

var (
	defaultChartStream   *ChartStream
	defaultChartStreamMu sync.RWMutex
)

func NewChartStream(dsn string) (*ChartStream, error) {
	db, err := dbx.Open(strings.TrimSpace(dsn))
	if err != nil {
		return nil, err
	}
	return &ChartStream{
		db:              db,
		clock:           klineclock.NewCalendarResolver(db),
		sessionResolver: newSessionResolver(db),
		interests:       make(map[string]int),
		subscribers:     make(map[chan ChartBarUpdate]struct{}),
		roots:           make(map[string]*chartRootState),
	}, nil
}

func (s *ChartStream) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func SetDefaultChartStream(stream *ChartStream) {
	defaultChartStreamMu.Lock()
	defaultChartStream = stream
	defaultChartStreamMu.Unlock()
}

func PublishRealtimeChartTick(ev tickEvent) {
	defaultChartStreamMu.RLock()
	stream := defaultChartStream
	defaultChartStreamMu.RUnlock()
	if stream != nil {
		stream.HandleTick(ev, false)
	}
}

func PublishReplayChartTick(ev tickEvent) {
	defaultChartStreamMu.RLock()
	stream := defaultChartStream
	defaultChartStreamMu.RUnlock()
	if stream != nil {
		stream.HandleTick(ev, true)
	}
}

func PublishChartFinalBar(bar minuteBar, replay bool) {
	defaultChartStreamMu.RLock()
	stream := defaultChartStream
	defaultChartStreamMu.RUnlock()
	if stream != nil {
		stream.HandleFinalBar(bar, replay)
	}
}

func NormalizeChartSubscription(raw ChartSubscription) (ChartSubscription, error) {
	sub := ChartSubscription{
		Symbol:    strings.ToLower(strings.TrimSpace(raw.Symbol)),
		Type:      strings.ToLower(strings.TrimSpace(raw.Type)),
		Variety:   strings.ToLower(strings.TrimSpace(raw.Variety)),
		Timeframe: strings.ToLower(strings.TrimSpace(raw.Timeframe)),
	}
	if sub.Symbol == "" {
		return ChartSubscription{}, fmt.Errorf("symbol is required")
	}
	if sub.Type == "" {
		if strings.HasSuffix(sub.Symbol, "l9") {
			sub.Type = "l9"
		} else {
			sub.Type = "contract"
		}
	}
	if sub.Type != "contract" && sub.Type != "l9" {
		return ChartSubscription{}, fmt.Errorf("invalid type: %s", sub.Type)
	}
	if sub.Variety == "" {
		sub.Variety = normalizeVariety(sub.Symbol)
		if sub.Type == "l9" && strings.HasSuffix(sub.Symbol, "l9") {
			sub.Variety = strings.TrimSuffix(sub.Symbol, "l9")
		}
	}
	switch sub.Timeframe {
	case "1m", "5m", "15m", "30m", "1h", "1d":
	default:
		return ChartSubscription{}, fmt.Errorf("invalid timeframe: %s", sub.Timeframe)
	}
	if sub.Variety == "" {
		return ChartSubscription{}, fmt.Errorf("variety is required")
	}
	return sub, nil
}

func ChartSubscriptionKey(sub ChartSubscription) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(sub.Symbol)),
		strings.ToLower(strings.TrimSpace(sub.Type)),
		strings.ToLower(strings.TrimSpace(sub.Variety)),
		strings.ToLower(strings.TrimSpace(sub.Timeframe)),
	}, "|")
}

func (s *ChartStream) AddInterest(raw ChartSubscription) (ChartSubscription, error) {
	sub, err := NormalizeChartSubscription(raw)
	if err != nil {
		return ChartSubscription{}, err
	}
	s.mu.Lock()
	s.interests[ChartSubscriptionKey(sub)] += 1
	s.mu.Unlock()
	return sub, nil
}

func (s *ChartStream) RemoveInterest(raw ChartSubscription) {
	sub, err := NormalizeChartSubscription(raw)
	if err != nil {
		return
	}
	key := ChartSubscriptionKey(sub)
	s.mu.Lock()
	if n := s.interests[key]; n > 1 {
		s.interests[key] = n - 1
	} else {
		delete(s.interests, key)
	}
	s.mu.Unlock()
}

func (s *ChartStream) Subscribe() (<-chan ChartBarUpdate, func()) {
	ch := make(chan ChartBarUpdate, 256)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		if _, ok := s.subscribers[ch]; ok {
			delete(s.subscribers, ch)
			close(ch)
		}
		s.mu.Unlock()
	}
}

func (s *ChartStream) HandleTick(ev tickEvent, replay bool) {
	if s == nil {
		return
	}
	instrumentID := strings.ToLower(strings.TrimSpace(ev.InstrumentID))
	variety := normalizeVariety(instrumentID)
	if instrumentID == "" || variety == "" {
		return
	}
	sessions, err := s.sessionResolver.Sessions(variety)
	if err != nil || len(sessions) == 0 {
		return
	}
	minuteTime, adjustedTime, adjustedTickTime, err := parseTickTimesWithMillis(ev.ActionDay, ev.TradingDay, ev.UpdateTime, ev.UpdateMillisec, sessions, s.clock)
	if err != nil {
		return
	}
	updates := make([]ChartBarUpdate, 0, 8)
	s.mu.Lock()
	contractFrames := s.interestedTimeframesLocked(instrumentID, "contract", variety)
	if len(contractFrames) > 0 {
		root := s.ensureRootLocked(instrumentID, "contract", variety)
		root.exchange = strings.TrimSpace(ev.ExchangeID)
		s.updateContractPartialLocked(root, ev, minuteTime, adjustedTime)
		updates = append(updates, s.buildPartialUpdatesLocked(root, contractFrames, sessions, replay)...)
	}
	l9Frames := s.interestedTimeframesLocked(variety+"l9", "l9", variety)
	if len(l9Frames) > 0 {
		root := s.ensureRootLocked(variety+"l9", "l9", variety)
		if root.latestTicks == nil {
			root.latestTicks = make(map[string]chartTickSnapshot)
		}
		root.latestTicks[instrumentID] = chartTickSnapshot{
			instrumentID:    instrumentID,
			exchange:        strings.TrimSpace(ev.ExchangeID),
			variety:         variety,
			minuteTime:      minuteTime,
			adjustedTime:    adjustedTime,
			adjustedTick:    adjustedTickTime,
			receivedAt:      ev.ReceivedAt,
			price:           ev.LastPrice,
			openInterest:    ev.OpenInterest,
			settlementPrice: ev.SettlementPrice,
		}
		s.updateL9PartialLocked(root, minuteTime, adjustedTime)
		updates = append(updates, s.buildPartialUpdatesLocked(root, l9Frames, sessions, replay)...)
	}
	s.mu.Unlock()
	s.broadcast(updates)
}

func (s *ChartStream) HandleFinalBar(bar minuteBar, replay bool) {
	if s == nil || bar.MinuteTime.IsZero() {
		return
	}
	symbol := strings.ToLower(strings.TrimSpace(bar.InstrumentID))
	kind := "contract"
	if strings.HasSuffix(symbol, "l9") {
		kind = "l9"
	}
	variety := normalizeVariety(bar.Variety)
	if variety == "" {
		variety = normalizeVariety(symbol)
		if kind == "l9" {
			variety = strings.TrimSuffix(symbol, "l9")
		}
	}
	if symbol == "" || variety == "" {
		return
	}
	updates := make([]ChartBarUpdate, 0, 4)
	s.mu.Lock()
	root := s.ensureRootLocked(symbol, kind, variety)
	root.exchange = bar.Exchange
	if bar.Period == "1m" {
		root.history1m = append(root.history1m, bar)
		if len(root.history1m) > defaultTickHistoryRetention {
			root.history1m = append([]minuteBar(nil), root.history1m[len(root.history1m)-defaultTickHistoryRetention:]...)
		}
		if root.currentPartial != nil && sameBarMinute(*root.currentPartial, bar) {
			root.currentPartial = nil
		}
	}
	frames := s.interestedTimeframesLocked(symbol, kind, variety)
	for _, timeframe := range frames {
		if timeframe != strings.ToLower(strings.TrimSpace(bar.Period)) {
			continue
		}
		updates = append(updates, ChartBarUpdate{
			Subscription: ChartSubscription{
				Symbol:    symbol,
				Type:      kind,
				Variety:   variety,
				Timeframe: timeframe,
			},
			Phase:  "final",
			Source: chartSourceLabel(replay),
			Bar:    chartBarFromMinuteBar(bar),
		})
	}
	if bar.Period == "1m" {
		sessions, err := s.sessionResolver.Sessions(variety)
		if err == nil && len(sessions) > 0 {
			for _, update := range s.buildPartialUpdatesLocked(root, filterNon1m(frames), sessions, replay) {
				updates = append(updates, update)
			}
		}
	}
	s.mu.Unlock()
	s.broadcast(updates)
}

func (s *ChartStream) broadcast(updates []ChartBarUpdate) {
	if len(updates) == 0 {
		return
	}
	s.mu.RLock()
	subs := make([]chan ChartBarUpdate, 0, len(s.subscribers))
	for ch := range s.subscribers {
		subs = append(subs, ch)
	}
	s.mu.RUnlock()
	for _, update := range updates {
		for _, ch := range subs {
			select {
			case ch <- update:
			default:
			}
		}
	}
}

func (s *ChartStream) interestedTimeframesLocked(symbol string, kind string, variety string) []string {
	out := make([]string, 0, 6)
	for _, tf := range []string{"1m", "5m", "15m", "30m", "1h", "1d"} {
		key := ChartSubscriptionKey(ChartSubscription{Symbol: symbol, Type: kind, Variety: variety, Timeframe: tf})
		if s.interests[key] > 0 {
			out = append(out, tf)
		}
	}
	return out
}

func (s *ChartStream) ensureRootLocked(symbol string, kind string, variety string) *chartRootState {
	key := kind + "|" + symbol + "|" + variety
	root := s.roots[key]
	if root == nil {
		root = &chartRootState{
			symbol:      symbol,
			kind:        kind,
			variety:     variety,
			latestTicks: make(map[string]chartTickSnapshot),
		}
		s.roots[key] = root
	}
	return root
}

func (s *ChartStream) updateContractPartialLocked(root *chartRootState, ev tickEvent, minuteTime time.Time, adjustedTime time.Time) {
	price := ev.LastPrice
	if root.currentPartial == nil || !root.currentPartial.MinuteTime.Equal(minuteTime) {
		root.currentPartial = &minuteBar{
			Variety:          root.variety,
			InstrumentID:     root.symbol,
			Exchange:         strings.TrimSpace(ev.ExchangeID),
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: ev.ReceivedAt,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           0,
			OpenInterest:     ev.OpenInterest,
			SettlementPrice:  ev.SettlementPrice,
		}
		return
	}
	if price > root.currentPartial.High {
		root.currentPartial.High = price
	}
	if price < root.currentPartial.Low {
		root.currentPartial.Low = price
	}
	root.currentPartial.Close = price
	root.currentPartial.OpenInterest = ev.OpenInterest
	root.currentPartial.SettlementPrice = ev.SettlementPrice
	root.currentPartial.SourceReceivedAt = ev.ReceivedAt
}

func (s *ChartStream) updateL9PartialLocked(root *chartRootState, minuteTime time.Time, adjustedTime time.Time) {
	totalOI := 0.0
	weightedPrice := 0.0
	weightedSettlement := 0.0
	latestAt := time.Time{}
	for _, item := range root.latestTicks {
		if item.openInterest <= 0 {
			continue
		}
		totalOI += item.openInterest
		weightedPrice += item.price * item.openInterest
		weightedSettlement += item.settlementPrice * item.openInterest
		if latestAt.IsZero() || item.receivedAt.After(latestAt) {
			latestAt = item.receivedAt
		}
	}
	if totalOI <= 0 {
		return
	}
	price := weightedPrice / totalOI
	settlement := weightedSettlement / totalOI
	if root.currentPartial == nil || !root.currentPartial.MinuteTime.Equal(minuteTime) {
		root.currentPartial = &minuteBar{
			Variety:          root.variety,
			InstrumentID:     root.symbol,
			Exchange:         "L9",
			MinuteTime:       minuteTime,
			AdjustedTime:     adjustedTime,
			SourceReceivedAt: latestAt,
			Period:           "1m",
			Open:             price,
			High:             price,
			Low:              price,
			Close:            price,
			Volume:           0,
			OpenInterest:     totalOI,
			SettlementPrice:  settlement,
		}
		return
	}
	if price > root.currentPartial.High {
		root.currentPartial.High = price
	}
	if price < root.currentPartial.Low {
		root.currentPartial.Low = price
	}
	root.currentPartial.Close = price
	root.currentPartial.OpenInterest = totalOI
	root.currentPartial.SettlementPrice = settlement
	root.currentPartial.SourceReceivedAt = latestAt
}

func (s *ChartStream) buildPartialUpdatesLocked(root *chartRootState, timeframes []string, sessions []sessiontime.Range, replay bool) []ChartBarUpdate {
	out := make([]ChartBarUpdate, 0, len(timeframes))
	for _, timeframe := range timeframes {
		bar, ok := buildPartialBarForTimeframe(root, timeframe, sessions)
		if !ok {
			continue
		}
		out = append(out, ChartBarUpdate{
			Subscription: ChartSubscription{
				Symbol:    root.symbol,
				Type:      root.kind,
				Variety:   root.variety,
				Timeframe: timeframe,
			},
			Phase:  "partial",
			Source: chartSourceLabel(replay),
			Bar:    chartBarFromMinuteBar(bar),
		})
	}
	return out
}

func buildPartialBarForTimeframe(root *chartRootState, timeframe string, sessions []sessiontime.Range) (minuteBar, bool) {
	if root == nil {
		return minuteBar{}, false
	}
	if timeframe == "1m" {
		if root.currentPartial == nil {
			return minuteBar{}, false
		}
		return *root.currentPartial, true
	}
	if timeframe == "1d" {
		return buildDailyPartial(root)
	}
	src := make([]klineagg.MinuteBar, 0, len(root.history1m)+1)
	for _, bar := range root.history1m {
		src = append(src, toAggMinuteBar(bar))
	}
	if root.currentPartial != nil {
		src = append(src, toAggMinuteBar(*root.currentPartial))
	}
	period, minutes := timeframeConfig(timeframe)
	if period == "" || minutes <= 1 || len(src) == 0 {
		return minuteBar{}, false
	}
	aggBars, _ := klineagg.Aggregate(src, toKlineAggSessions(sessions), period, minutes, klineagg.Options{
		CrossSessionFor30m1h: true,
		ClampToSessionEnd:    true,
	})
	if len(aggBars) == 0 {
		return minuteBar{}, false
	}
	last := aggBars[len(aggBars)-1]
	return minuteBar{
		Variety:          root.variety,
		InstrumentID:     root.symbol,
		Exchange:         root.exchange,
		MinuteTime:       last.DataTime,
		AdjustedTime:     last.AdjustedTime,
		SourceReceivedAt: time.Now(),
		Period:           timeframe,
		Open:             last.Open,
		High:             last.High,
		Low:              last.Low,
		Close:            last.Close,
		Volume:           last.Volume,
		OpenInterest:     last.OpenInterest,
	}, true
}

func buildDailyPartial(root *chartRootState) (minuteBar, bool) {
	var dayBars []minuteBar
	for _, bar := range root.history1m {
		dayBars = append(dayBars, bar)
	}
	if root.currentPartial != nil {
		dayBars = append(dayBars, *root.currentPartial)
	}
	if len(dayBars) == 0 {
		return minuteBar{}, false
	}
	targetDay := chooseAdjustedTime(dayBars[len(dayBars)-1]).Format("2006-01-02")
	filtered := make([]minuteBar, 0, len(dayBars))
	for _, bar := range dayBars {
		if chooseAdjustedTime(bar).Format("2006-01-02") == targetDay {
			filtered = append(filtered, bar)
		}
	}
	if len(filtered) == 0 {
		return minuteBar{}, false
	}
	out := minuteBar{
		Variety:          root.variety,
		InstrumentID:     root.symbol,
		Exchange:         root.exchange,
		MinuteTime:       time.Date(filtered[0].MinuteTime.Year(), filtered[0].MinuteTime.Month(), filtered[0].MinuteTime.Day(), 0, 0, 0, 0, time.Local),
		AdjustedTime:     time.Date(chooseAdjustedTime(filtered[0]).Year(), chooseAdjustedTime(filtered[0]).Month(), chooseAdjustedTime(filtered[0]).Day(), 0, 0, 0, 0, time.Local),
		SourceReceivedAt: filtered[len(filtered)-1].SourceReceivedAt,
		Period:           "1d",
		Open:             filtered[0].Open,
		High:             filtered[0].High,
		Low:              filtered[0].Low,
		Close:            filtered[len(filtered)-1].Close,
		OpenInterest:     filtered[len(filtered)-1].OpenInterest,
		SettlementPrice:  filtered[len(filtered)-1].SettlementPrice,
	}
	for _, bar := range filtered {
		if bar.High > out.High {
			out.High = bar.High
		}
		if bar.Low < out.Low {
			out.Low = bar.Low
		}
		out.Volume += bar.Volume
	}
	return out, true
}

func toAggMinuteBar(bar minuteBar) klineagg.MinuteBar {
	return klineagg.MinuteBar{
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
	}
}

func timeframeConfig(timeframe string) (string, int) {
	switch timeframe {
	case "5m":
		return "5m", 5
	case "15m":
		return "15m", 15
	case "30m":
		return "30m", 30
	case "1h":
		return "1h", 60
	default:
		return "", 0
	}
}

func chartBarFromMinuteBar(bar minuteBar) ChartBar {
	return ChartBar{
		AdjustedTime: chooseAdjustedTime(bar).Unix(),
		DataTime:     bar.MinuteTime.Unix(),
		Open:         bar.Open,
		High:         bar.High,
		Low:          bar.Low,
		Close:        bar.Close,
		Volume:       bar.Volume,
		OpenInterest: bar.OpenInterest,
	}
}

func sameBarMinute(a minuteBar, b minuteBar) bool {
	return a.MinuteTime.Equal(b.MinuteTime) || chooseAdjustedTime(a).Equal(chooseAdjustedTime(b))
}

func filterNon1m(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item != "1m" {
			out = append(out, item)
		}
	}
	return out
}

func chartSourceLabel(replay bool) string {
	if replay {
		return "replay"
	}
	return "realtime"
}
