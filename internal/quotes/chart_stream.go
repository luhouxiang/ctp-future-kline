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
	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/sessiontime"
)

type ChartSubscription struct {
	Symbol    string `json:"symbol"`
	Type      string `json:"type"`
	Variety   string `json:"variety"`
	Timeframe string `json:"timeframe"`
	DataMode  string `json:"data_mode"`
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

type ChartQuoteSnapshot struct {
	Symbol             string   `json:"symbol"`
	Type               string   `json:"type"`
	Variety            string   `json:"variety"`
	DataMode           string   `json:"data_mode"`
	LatestPrice        *float64 `json:"latest_price,omitempty"`
	BidPrice1          *float64 `json:"bid_price1,omitempty"`
	BidVolume1         *int64   `json:"bid_volume1,omitempty"`
	AskPrice1          *float64 `json:"ask_price1,omitempty"`
	AskVolume1         *int64   `json:"ask_volume1,omitempty"`
	PreSettlementPrice *float64 `json:"pre_settlement_price,omitempty"`
	PreClosePrice      *float64 `json:"pre_close_price,omitempty"`
	PreOpenInterest    *float64 `json:"pre_open_interest,omitempty"`
	SettlementPrice    *float64 `json:"settlement_price,omitempty"`
	Volume             *int64   `json:"volume,omitempty"`
	CurrentVolume      *int64   `json:"current_volume,omitempty"`
	Turnover           *float64 `json:"turnover,omitempty"`
	OpenInterest       *float64 `json:"open_interest,omitempty"`
	OIDelta            *float64 `json:"oi_delta,omitempty"`
	ReferencePrice     *float64 `json:"reference_price,omitempty"`
	Change             *float64 `json:"change,omitempty"`
	ChangePct          *float64 `json:"change_pct,omitempty"`
	Open               *float64 `json:"open,omitempty"`
	High               *float64 `json:"high,omitempty"`
	Low                *float64 `json:"low,omitempty"`
	Close              *float64 `json:"close,omitempty"`
	UpperLimitPrice    *float64 `json:"upper_limit_price,omitempty"`
	LowerLimitPrice    *float64 `json:"lower_limit_price,omitempty"`
	AveragePrice       *float64 `json:"average_price,omitempty"`
	Time               string   `json:"time,omitempty"`
}

type ChartQuoteTickRow struct {
	Time     string   `json:"time"`
	Price    *float64 `json:"price,omitempty"`
	Volume   *int64   `json:"volume,omitempty"`
	OIDelta  *float64 `json:"oi_delta,omitempty"`
	Nature   string   `json:"nature,omitempty"`
	DataMode string   `json:"data_mode,omitempty"`
}

type ChartQuoteUpdate struct {
	Subscription ChartSubscription   `json:"subscription"`
	Source       string              `json:"source"`
	Snapshot     ChartQuoteSnapshot  `json:"snapshot"`
	Ticks        []ChartQuoteTickRow `json:"ticks"`
}

type chartTickSnapshot struct {
	instrumentID       string
	exchange           string
	exchangeInstID     string
	variety            string
	minuteTime         time.Time
	adjustedTime       time.Time
	adjustedTick       time.Time
	receivedAt         time.Time
	price              float64
	preSettlementPrice float64
	preClosePrice      float64
	preOpenInterest    float64
	openPrice          float64
	highestPrice       float64
	lowestPrice        float64
	volume             int64
	turnover           float64
	openInterest       float64
	closePrice         float64
	settlementPrice    float64
	upperLimitPrice    float64
	lowerLimitPrice    float64
	averagePrice       float64
	preDelta           float64
	currDelta          float64
	bidPrice1          float64
	bidVolume1         int64
	askPrice1          float64
	askVolume1         int64
	updateTime         string
	updateMillisec     int
}

type chartQuoteTickRow struct {
	at           time.Time
	price        float64
	volume       int64
	openInterest float64
	bidPrice1    float64
	askPrice1    float64
	dataMode     string
}

type chartRootState struct {
	symbol         string
	kind           string
	variety        string
	exchange       string
	history1m      []minuteBar
	currentPartial *minuteBar
	latestTicks    map[string]chartTickSnapshot
	recentTicks    []chartQuoteTickRow
}

type ChartStream struct {
	db              *sql.DB
	clock           *klineclock.CalendarResolver
	sessionResolver *sessionResolver

	mu          sync.RWMutex
	interests   map[string]int
	quoteKeys   map[string]int
	subscribers map[chan ChartBarUpdate]struct{}
	quoteSubs   map[chan ChartQuoteUpdate]struct{}
	roots       map[string]*chartRootState
	queueHandle *queuewatch.QueueHandle
	queueCap    int
}

var (
	defaultChartStream   *ChartStream
	defaultChartStreamMu sync.RWMutex
)

func NewChartStream(dsn string, registry *queuewatch.Registry) (*ChartStream, error) {
	db, err := dbx.Open(strings.TrimSpace(dsn))
	if err != nil {
		return nil, err
	}
	queueCfg := queuewatch.DefaultConfig("")
	if registry != nil {
		queueCfg = registry.Config()
	}
	stream := &ChartStream{
		db:              db,
		clock:           klineclock.NewCalendarResolver(db),
		sessionResolver: newSessionResolver(db),
		interests:       make(map[string]int),
		quoteKeys:       make(map[string]int),
		subscribers:     make(map[chan ChartBarUpdate]struct{}),
		quoteSubs:       make(map[chan ChartQuoteUpdate]struct{}),
		roots:           make(map[string]*chartRootState),
		queueCap:        queueCfg.ChartSubscriberCapacity,
	}
	if registry != nil {
		stream.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        "chart_stream_subscribers",
			Category:    "web_realtime",
			Criticality: "best_effort",
			Capacity:    queueCfg.ChartSubscriberCapacity,
			LossPolicy:  "latest_only",
			BasisText:   "?????????????????????????",
		})
	}
	return stream, nil
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

func PublishChartPartialBar(bar minuteBar, replay bool) {
	defaultChartStreamMu.RLock()
	stream := defaultChartStream
	defaultChartStreamMu.RUnlock()
	if stream != nil {
		stream.HandlePartialBar(bar, replay)
	}
}

func NormalizeChartSubscription(raw ChartSubscription) (ChartSubscription, error) {
	sub := ChartSubscription{
		Symbol:    strings.ToLower(strings.TrimSpace(raw.Symbol)),
		Type:      strings.ToLower(strings.TrimSpace(raw.Type)),
		Variety:   strings.ToLower(strings.TrimSpace(raw.Variety)),
		Timeframe: strings.ToLower(strings.TrimSpace(raw.Timeframe)),
		DataMode:  strings.ToLower(strings.TrimSpace(raw.DataMode)),
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
	if sub.DataMode == "" {
		sub.DataMode = "realtime"
	}
	if sub.DataMode != "realtime" && sub.DataMode != "replay" {
		return ChartSubscription{}, fmt.Errorf("invalid data_mode: %s", sub.DataMode)
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
		strings.ToLower(strings.TrimSpace(sub.DataMode)),
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

func (s *ChartStream) AddQuoteInterest(raw ChartSubscription) (ChartSubscription, error) {
	sub, err := NormalizeChartSubscription(raw)
	if err != nil {
		return ChartSubscription{}, err
	}
	s.mu.Lock()
	s.quoteKeys[ChartSubscriptionKey(sub)] += 1
	s.mu.Unlock()
	return sub, nil
}

func (s *ChartStream) RemoveQuoteInterest(raw ChartSubscription) {
	sub, err := NormalizeChartSubscription(raw)
	if err != nil {
		return
	}
	key := ChartSubscriptionKey(sub)
	s.mu.Lock()
	if n := s.quoteKeys[key]; n > 1 {
		s.quoteKeys[key] = n - 1
	} else {
		delete(s.quoteKeys, key)
	}
	s.mu.Unlock()
}

func (s *ChartStream) Subscribe() (<-chan ChartBarUpdate, func()) {
	cap := s.queueCap
	if cap <= 0 {
		cap = queuewatch.DefaultConfig("").ChartSubscriberCapacity
	}
	ch := make(chan ChartBarUpdate, cap)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	if s.queueHandle != nil {
		s.queueHandle.ObserveDepth(s.maxSubscriberDepth())
	}
	return ch, func() {
		s.mu.Lock()
		if _, ok := s.subscribers[ch]; ok {
			delete(s.subscribers, ch)
			close(ch)
		}
		s.mu.Unlock()
	}
}

func (s *ChartStream) SubscribeQuotes() (<-chan ChartQuoteUpdate, func()) {
	cap := s.queueCap
	if cap <= 0 {
		cap = queuewatch.DefaultConfig("").ChartSubscriberCapacity
	}
	ch := make(chan ChartQuoteUpdate, cap)
	s.mu.Lock()
	s.quoteSubs[ch] = struct{}{}
	s.mu.Unlock()
	if s.queueHandle != nil {
		s.queueHandle.ObserveDepth(s.maxSubscriberDepth())
	}
	return ch, func() {
		s.mu.Lock()
		if _, ok := s.quoteSubs[ch]; ok {
			delete(s.quoteSubs, ch)
			close(ch)
		}
		s.mu.Unlock()
	}
}

func (s *ChartStream) SnapshotQuote(raw ChartSubscription) (ChartQuoteUpdate, bool) {
	if s == nil {
		return ChartQuoteUpdate{}, false
	}
	sub, err := NormalizeChartSubscription(raw)
	if err != nil {
		return ChartQuoteUpdate{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	root := s.roots[rootKeyForSubscription(sub)]
	if root == nil {
		return ChartQuoteUpdate{}, false
	}
	update, ok := s.buildQuoteUpdateLocked(root, sub, sub.DataMode)
	return update, ok
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
	quoteUpdates := make([]ChartQuoteUpdate, 0, 2)
	dataMode := chartDataModeLabel(replay)
	s.mu.Lock()
	contractFrames := s.interestedTimeframesLocked(instrumentID, "contract", variety, dataMode)
	contractQuoteFrames := s.interestedQuoteTimeframesLocked(instrumentID, "contract", variety, dataMode)
	if len(contractFrames) > 0 || len(contractQuoteFrames) > 0 {
		root := s.ensureRootLocked(instrumentID, "contract", variety)
		root.exchange = strings.TrimSpace(ev.ExchangeID)
		s.updateContractPartialLocked(root, ev, minuteTime, adjustedTime, dataMode)
		if len(contractFrames) > 0 {
			updates = append(updates, s.buildPartialUpdatesLocked(root, contractFrames, sessions, replay)...)
		}
		for _, timeframe := range contractQuoteFrames {
			if quoteUpdate, ok := s.buildQuoteUpdateLocked(root, ChartSubscription{
				Symbol:    instrumentID,
				Type:      "contract",
				Variety:   variety,
				Timeframe: timeframe,
				DataMode:  dataMode,
			}, dataMode); ok {
				quoteUpdates = append(quoteUpdates, quoteUpdate)
			}
		}
	}
	l9Frames := s.interestedTimeframesLocked(variety+"l9", "l9", variety, dataMode)
	l9QuoteFrames := s.interestedQuoteTimeframesLocked(variety+"l9", "l9", variety, dataMode)
	if len(l9Frames) > 0 || len(l9QuoteFrames) > 0 {
		root := s.ensureRootLocked(variety+"l9", "l9", variety)
		if root.latestTicks == nil {
			root.latestTicks = make(map[string]chartTickSnapshot)
		}
		root.latestTicks[instrumentID] = chartTickSnapshot{
			instrumentID:       instrumentID,
			exchange:           strings.TrimSpace(ev.ExchangeID),
			exchangeInstID:     strings.TrimSpace(ev.ExchangeInstID),
			variety:            variety,
			minuteTime:         minuteTime,
			adjustedTime:       adjustedTime,
			adjustedTick:       adjustedTickTime,
			receivedAt:         ev.ReceivedAt,
			price:              ev.LastPrice,
			preSettlementPrice: ev.PreSettlementPrice,
			preClosePrice:      ev.PreClosePrice,
			preOpenInterest:    ev.PreOpenInterest,
			openPrice:          ev.OpenPrice,
			highestPrice:       ev.HighestPrice,
			lowestPrice:        ev.LowestPrice,
			volume:             int64(ev.Volume),
			turnover:           ev.Turnover,
			openInterest:       ev.OpenInterest,
			closePrice:         ev.ClosePrice,
			settlementPrice:    ev.SettlementPrice,
			upperLimitPrice:    ev.UpperLimitPrice,
			lowerLimitPrice:    ev.LowerLimitPrice,
			averagePrice:       ev.AveragePrice,
			preDelta:           ev.PreDelta,
			currDelta:          ev.CurrDelta,
			bidPrice1:          ev.BidPrice1,
			bidVolume1:         int64(ev.BidVolume1),
			askPrice1:          ev.AskPrice1,
			askVolume1:         int64(ev.AskVolume1),
			updateTime:         strings.TrimSpace(ev.UpdateTime),
			updateMillisec:     ev.UpdateMillisec,
		}
		s.updateL9PartialLocked(root, minuteTime, adjustedTime)
		if len(l9Frames) > 0 {
			updates = append(updates, s.buildPartialUpdatesLocked(root, l9Frames, sessions, replay)...)
		}
		for _, timeframe := range l9QuoteFrames {
			if quoteUpdate, ok := s.buildQuoteUpdateLocked(root, ChartSubscription{
				Symbol:    variety + "l9",
				Type:      "l9",
				Variety:   variety,
				Timeframe: timeframe,
				DataMode:  dataMode,
			}, dataMode); ok {
				quoteUpdates = append(quoteUpdates, quoteUpdate)
			}
		}
	}
	s.mu.Unlock()
	for _, update := range updates {
		chartPartialKeyRateProbe.Inc(ChartSubscriptionKey(update.Subscription))
	}
	s.broadcast(updates)
	s.broadcastQuotes(quoteUpdates)
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
	dataMode := chartDataModeLabel(replay)
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
	frames := s.interestedTimeframesLocked(symbol, kind, variety, dataMode)
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
				DataMode:  dataMode,
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

func (s *ChartStream) HandlePartialBar(bar minuteBar, replay bool) {
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
	updates := make([]ChartBarUpdate, 0, 8)
	dataMode := chartDataModeLabel(replay)
	s.mu.Lock()
	root := s.ensureRootLocked(symbol, kind, variety)
	root.exchange = bar.Exchange
	root.currentPartial = cloneMinuteBarPtr(bar)
	frames := s.interestedTimeframesLocked(symbol, kind, variety, dataMode)
	for _, timeframe := range frames {
		if timeframe == "1m" {
			updates = append(updates, ChartBarUpdate{
				Subscription: ChartSubscription{
					Symbol:    symbol,
					Type:      kind,
					Variety:   variety,
					Timeframe: timeframe,
					DataMode:  dataMode,
				},
				Phase:  "partial",
				Source: chartSourceLabel(replay),
				Bar:    chartBarFromMinuteBar(bar),
			})
		}
	}
	if len(frames) > 0 {
		sessions, err := s.sessionResolver.Sessions(variety)
		if err == nil && len(sessions) > 0 {
			updates = append(updates, s.buildPartialUpdatesLocked(root, filterNon1m(frames), sessions, replay)...)
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
			if dropped := enqueueChartUpdateLatest(ch, update); dropped > 0 && s.queueHandle != nil {
				s.queueHandle.MarkDropped(s.maxSubscriberDepth())
			} else if s.queueHandle != nil {
				s.queueHandle.MarkEnqueued(s.maxSubscriberDepth())
			}
		}
	}
	if s.queueHandle != nil {
		s.queueHandle.ObserveDepth(s.maxSubscriberDepth())
	}
}

func (s *ChartStream) broadcastQuotes(updates []ChartQuoteUpdate) {
	if len(updates) == 0 {
		return
	}
	s.mu.RLock()
	subs := make([]chan ChartQuoteUpdate, 0, len(s.quoteSubs))
	for ch := range s.quoteSubs {
		subs = append(subs, ch)
	}
	s.mu.RUnlock()
	for _, update := range updates {
		for _, ch := range subs {
			if dropped := enqueueChartQuoteLatest(ch, update); dropped > 0 && s.queueHandle != nil {
				s.queueHandle.MarkDropped(s.maxSubscriberDepth())
			} else if s.queueHandle != nil {
				s.queueHandle.MarkEnqueued(s.maxSubscriberDepth())
			}
		}
	}
	if s.queueHandle != nil {
		s.queueHandle.ObserveDepth(s.maxSubscriberDepth())
	}
}

func enqueueChartUpdateLatest(ch chan ChartBarUpdate, update ChartBarUpdate) int {
	select {
	case ch <- update:
		return 0
	default:
	}
	dropped := 0
	select {
	case <-ch:
		dropped = 1
	default:
	}
	select {
	case ch <- update:
	default:
	}
	return dropped
}

func enqueueChartQuoteLatest(ch chan ChartQuoteUpdate, update ChartQuoteUpdate) int {
	select {
	case ch <- update:
		return 0
	default:
	}
	dropped := 0
	select {
	case <-ch:
		dropped = 1
	default:
	}
	select {
	case ch <- update:
	default:
	}
	return dropped
}

func (s *ChartStream) interestedTimeframesLocked(symbol string, kind string, variety string, dataMode string) []string {
	out := make([]string, 0, 6)
	for _, tf := range []string{"1m", "5m", "15m", "30m", "1h", "1d"} {
		key := ChartSubscriptionKey(ChartSubscription{Symbol: symbol, Type: kind, Variety: variety, Timeframe: tf, DataMode: dataMode})
		if s.interests[key] > 0 {
			out = append(out, tf)
		}
	}
	return out
}

func (s *ChartStream) interestedQuoteTimeframesLocked(symbol string, kind string, variety string, dataMode string) []string {
	out := make([]string, 0, 6)
	for _, tf := range []string{"1m", "5m", "15m", "30m", "1h", "1d"} {
		key := ChartSubscriptionKey(ChartSubscription{Symbol: symbol, Type: kind, Variety: variety, Timeframe: tf, DataMode: dataMode})
		if s.quoteKeys[key] > 0 {
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

func (s *ChartStream) updateContractPartialLocked(root *chartRootState, ev tickEvent, minuteTime time.Time, adjustedTime time.Time, dataMode string) {
	price := ev.LastPrice
	root.pushRecentTickLocked(chartQuoteTickRow{
		at:           combineTickTimestamp(adjustedTime, ev.UpdateTime, ev.UpdateMillisec),
		price:        price,
		volume:       int64(ev.Volume),
		openInterest: ev.OpenInterest,
		bidPrice1:    ev.BidPrice1,
		askPrice1:    ev.AskPrice1,
		dataMode:     dataMode,
	})
	if root.latestTicks == nil {
		root.latestTicks = make(map[string]chartTickSnapshot)
	}
	root.latestTicks[root.symbol] = chartTickSnapshot{
		instrumentID:       root.symbol,
		exchange:           strings.TrimSpace(ev.ExchangeID),
		exchangeInstID:     strings.TrimSpace(ev.ExchangeInstID),
		variety:            root.variety,
		minuteTime:         minuteTime,
		adjustedTime:       adjustedTime,
		adjustedTick:       combineTickTimestamp(adjustedTime, ev.UpdateTime, ev.UpdateMillisec),
		receivedAt:         ev.ReceivedAt,
		price:              price,
		preSettlementPrice: ev.PreSettlementPrice,
		preClosePrice:      ev.PreClosePrice,
		preOpenInterest:    ev.PreOpenInterest,
		openPrice:          ev.OpenPrice,
		highestPrice:       ev.HighestPrice,
		lowestPrice:        ev.LowestPrice,
		volume:             int64(ev.Volume),
		turnover:           ev.Turnover,
		openInterest:       ev.OpenInterest,
		closePrice:         ev.ClosePrice,
		settlementPrice:    ev.SettlementPrice,
		upperLimitPrice:    ev.UpperLimitPrice,
		lowerLimitPrice:    ev.LowerLimitPrice,
		averagePrice:       ev.AveragePrice,
		preDelta:           ev.PreDelta,
		currDelta:          ev.CurrDelta,
		bidPrice1:          ev.BidPrice1,
		bidVolume1:         int64(ev.BidVolume1),
		askPrice1:          ev.AskPrice1,
		askVolume1:         int64(ev.AskVolume1),
		updateTime:         strings.TrimSpace(ev.UpdateTime),
		updateMillisec:     ev.UpdateMillisec,
	}
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

func (root *chartRootState) pushRecentTickLocked(row chartQuoteTickRow) {
	const recentQuoteTickLimit = 32
	if root == nil || row.at.IsZero() {
		return
	}
	root.recentTicks = append([]chartQuoteTickRow{row}, root.recentTicks...)
	if len(root.recentTicks) > recentQuoteTickLimit {
		root.recentTicks = root.recentTicks[:recentQuoteTickLimit]
	}
}

func quoteReferencePrice(tick chartTickSnapshot) float64 {
	if tick.preSettlementPrice > 0 {
		return tick.preSettlementPrice
	}
	if tick.preClosePrice > 0 {
		return tick.preClosePrice
	}
	return 0
}

func quoteCurrentVolume(rows []chartQuoteTickRow) int64 {
	if len(rows) == 0 {
		return 0
	}
	if len(rows) == 1 {
		return rows[0].volume
	}
	delta := rows[0].volume - rows[1].volume
	if delta < 0 {
		return 0
	}
	return delta
}

func quoteOIDelta(rows []chartQuoteTickRow) float64 {
	if len(rows) < 2 {
		return 0
	}
	return rows[0].openInterest - rows[1].openInterest
}

func quoteTickNature(row chartQuoteTickRow) string {
	switch {
	case row.askPrice1 > 0 && row.price >= row.askPrice1:
		return "主动买"
	case row.bidPrice1 > 0 && row.price <= row.bidPrice1:
		return "主动卖"
	default:
		return "中性"
	}
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
				DataMode:  chartDataModeLabel(replay),
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
		return buildDailyPartial(root, sessions)
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

func (s *ChartStream) buildQuoteUpdateLocked(root *chartRootState, sub ChartSubscription, dataMode string) (ChartQuoteUpdate, bool) {
	if root == nil {
		return ChartQuoteUpdate{}, false
	}
	snapshot := ChartQuoteSnapshot{
		Symbol:   root.symbol,
		Type:     root.kind,
		Variety:  root.variety,
		DataMode: dataMode,
	}
	var ticks []ChartQuoteTickRow
	if root.kind == "contract" {
		tick, ok := root.latestTicks[root.symbol]
		if !ok {
			return ChartQuoteUpdate{}, false
		}
		snapshot.LatestPrice = floatPtr(tick.price)
		if tick.bidPrice1 > 0 {
			snapshot.BidPrice1 = floatPtr(tick.bidPrice1)
		}
		if tick.bidVolume1 > 0 {
			snapshot.BidVolume1 = int64Ptr(tick.bidVolume1)
		}
		if tick.askPrice1 > 0 {
			snapshot.AskPrice1 = floatPtr(tick.askPrice1)
		}
		if tick.askVolume1 > 0 {
			snapshot.AskVolume1 = int64Ptr(tick.askVolume1)
		}
		if tick.preSettlementPrice > 0 {
			snapshot.PreSettlementPrice = floatPtr(tick.preSettlementPrice)
		}
		if tick.preClosePrice > 0 {
			snapshot.PreClosePrice = floatPtr(tick.preClosePrice)
		}
		if tick.preOpenInterest > 0 {
			snapshot.PreOpenInterest = floatPtr(tick.preOpenInterest)
		}
		if tick.settlementPrice > 0 {
			snapshot.SettlementPrice = floatPtr(tick.settlementPrice)
		}
		if tick.turnover > 0 {
			snapshot.Turnover = floatPtr(tick.turnover)
		}
		if tick.upperLimitPrice > 0 {
			snapshot.UpperLimitPrice = floatPtr(tick.upperLimitPrice)
		}
		if tick.lowerLimitPrice > 0 {
			snapshot.LowerLimitPrice = floatPtr(tick.lowerLimitPrice)
		}
		if tick.averagePrice > 0 {
			snapshot.AveragePrice = floatPtr(tick.averagePrice)
		}
		snapshot.OpenInterest = floatPtr(tick.openInterest)
		if currentVolume := quoteCurrentVolume(root.recentTicks); currentVolume > 0 {
			snapshot.CurrentVolume = int64Ptr(currentVolume)
		}
		if oiDelta := quoteOIDelta(root.recentTicks); oiDelta != 0 {
			snapshot.OIDelta = floatPtr(oiDelta)
		}
		if referencePrice := quoteReferencePrice(tick); referencePrice > 0 {
			snapshot.ReferencePrice = floatPtr(referencePrice)
			change := tick.price - referencePrice
			snapshot.Change = floatPtr(change)
			snapshot.ChangePct = floatPtr(change / referencePrice)
		}
		snapshot.Time = formatQuoteTickTime(tick.adjustedTick, tick.updateTime, tick.updateMillisec)
	} else if tick, ok := latestL9Tick(root); ok {
		snapshot.LatestPrice = floatPtr(tick.price)
		if tick.preSettlementPrice > 0 {
			snapshot.PreSettlementPrice = floatPtr(tick.preSettlementPrice)
		}
		if tick.preClosePrice > 0 {
			snapshot.PreClosePrice = floatPtr(tick.preClosePrice)
		}
		if tick.settlementPrice > 0 {
			snapshot.SettlementPrice = floatPtr(tick.settlementPrice)
		}
		if tick.turnover > 0 {
			snapshot.Turnover = floatPtr(tick.turnover)
		}
		if tick.upperLimitPrice > 0 {
			snapshot.UpperLimitPrice = floatPtr(tick.upperLimitPrice)
		}
		if tick.lowerLimitPrice > 0 {
			snapshot.LowerLimitPrice = floatPtr(tick.lowerLimitPrice)
		}
		if tick.averagePrice > 0 {
			snapshot.AveragePrice = floatPtr(tick.averagePrice)
		}
		snapshot.OpenInterest = floatPtr(tick.openInterest)
		if referencePrice := quoteReferencePrice(tick); referencePrice > 0 {
			snapshot.ReferencePrice = floatPtr(referencePrice)
			change := tick.price - referencePrice
			snapshot.Change = floatPtr(change)
			snapshot.ChangePct = floatPtr(change / referencePrice)
		}
		snapshot.Time = formatQuoteTickTime(tick.adjustedTick, tick.updateTime, tick.updateMillisec)
	} else if root.currentPartial == nil && len(root.history1m) == 0 {
		return ChartQuoteUpdate{}, false
	}
	bar := latestQuoteBar(root)
	if bar != nil {
		snapshot.Open = floatPtr(bar.Open)
		snapshot.High = floatPtr(bar.High)
		snapshot.Low = floatPtr(bar.Low)
		snapshot.Close = floatPtr(bar.Close)
		snapshot.Volume = int64Ptr(bar.Volume)
		if snapshot.OpenInterest == nil {
			snapshot.OpenInterest = floatPtr(bar.OpenInterest)
		}
		if snapshot.SettlementPrice == nil && bar.SettlementPrice > 0 {
			snapshot.SettlementPrice = floatPtr(bar.SettlementPrice)
		}
		if snapshot.LatestPrice == nil {
			snapshot.LatestPrice = floatPtr(bar.Close)
		}
		if snapshot.Time == "" {
			snapshot.Time = formatQuoteBarTime(*bar)
		}
	}
	if root.kind == "contract" {
		ticks = make([]ChartQuoteTickRow, 0, len(root.recentTicks))
		for idx, item := range root.recentTicks {
			var volume *int64
			if idx+1 < len(root.recentTicks) {
				delta := item.volume - root.recentTicks[idx+1].volume
				if delta < 0 {
					delta = 0
				}
				volume = int64Ptr(delta)
			} else {
				volume = int64Ptr(item.volume)
			}
			var oiDelta *float64
			if idx+1 < len(root.recentTicks) {
				delta := item.openInterest - root.recentTicks[idx+1].openInterest
				oiDelta = floatPtr(delta)
			}
			ticks = append(ticks, ChartQuoteTickRow{
				Time:     item.at.Format("15:04:05"),
				Price:    floatPtr(item.price),
				Volume:   volume,
				OIDelta:  oiDelta,
				Nature:   quoteTickNature(item),
				DataMode: item.dataMode,
			})
		}
	}
	return ChartQuoteUpdate{
		Subscription: sub,
		Source:       dataMode,
		Snapshot:     snapshot,
		Ticks:        ticks,
	}, true
}

func latestQuoteBar(root *chartRootState) *minuteBar {
	if root == nil {
		return nil
	}
	if root.currentPartial != nil {
		return root.currentPartial
	}
	if len(root.history1m) == 0 {
		return nil
	}
	return &root.history1m[len(root.history1m)-1]
}

func latestL9Tick(root *chartRootState) (chartTickSnapshot, bool) {
	if root == nil {
		return chartTickSnapshot{}, false
	}
	var out chartTickSnapshot
	var ok bool
	for _, item := range root.latestTicks {
		if !ok || item.adjustedTick.After(out.adjustedTick) {
			out = item
			ok = true
		}
	}
	return out, ok
}

func combineTickTimestamp(adjustedTime time.Time, updateTime string, updateMillisec int) time.Time {
	if adjustedTime.IsZero() {
		return time.Time{}
	}
	parts := strings.Split(strings.TrimSpace(updateTime), ":")
	if len(parts) != 3 {
		return adjustedTime
	}
	hour := parseIntOrZero(parts[0])
	minute := parseIntOrZero(parts[1])
	second := parseIntOrZero(parts[2])
	millisec := updateMillisec
	if millisec < 0 {
		millisec = 0
	}
	return time.Date(adjustedTime.Year(), adjustedTime.Month(), adjustedTime.Day(), hour, minute, second, millisec*int(time.Millisecond), adjustedTime.Location())
}

func parseIntOrZero(raw string) int {
	var out int
	for _, ch := range strings.TrimSpace(raw) {
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

func formatQuoteTickTime(adjustedTick time.Time, updateTime string, updateMillisec int) string {
	if !adjustedTick.IsZero() {
		return adjustedTick.Format("15:04:05")
	}
	if strings.TrimSpace(updateTime) != "" {
		return strings.TrimSpace(updateTime)
	}
	return "-"
}

func formatQuoteBarTime(bar minuteBar) string {
	return chooseAdjustedTime(bar).Format("15:04:05")
}

func floatPtr(v float64) *float64 {
	out := v
	return &out
}

func int64Ptr(v int64) *int64 {
	out := v
	return &out
}

func rootKeyForSubscription(sub ChartSubscription) string {
	return strings.ToLower(strings.TrimSpace(sub.Type)) + "|" + strings.ToLower(strings.TrimSpace(sub.Symbol)) + "|" + strings.ToLower(strings.TrimSpace(sub.Variety))
}

func buildDailyPartial(root *chartRootState, sessions []sessiontime.Range) (minuteBar, bool) {
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
	targetDay := tradingDayKey(dayBars[len(dayBars)-1])
	if targetDay == "" {
		return minuteBar{}, false
	}
	filtered := make([]minuteBar, 0, len(dayBars))
	for _, bar := range dayBars {
		if tradingDayKey(bar) == targetDay {
			filtered = append(filtered, bar)
		}
	}
	if len(filtered) == 0 {
		return minuteBar{}, false
	}
	dataLabelTime, adjustedLabelTime, ok := dailyLabelTimes(filtered[0], sessions)
	if !ok {
		return minuteBar{}, false
	}
	out := minuteBar{
		Variety:          root.variety,
		InstrumentID:     root.symbol,
		Exchange:         root.exchange,
		MinuteTime:       dataLabelTime,
		AdjustedTime:     adjustedLabelTime,
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

func cloneMinuteBarPtr(bar minuteBar) *minuteBar {
	copied := bar
	return &copied
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

func chartDataModeLabel(replay bool) string {
	if replay {
		return "replay"
	}
	return "realtime"
}

func (s *ChartStream) maxSubscriberDepth() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	maxDepth := 0
	for ch := range s.subscribers {
		if depth := len(ch); depth > maxDepth {
			maxDepth = depth
		}
	}
	for ch := range s.quoteSubs {
		if depth := len(ch); depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}
