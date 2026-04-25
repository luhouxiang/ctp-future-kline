package web

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/appmode"
	"ctp-future-kline/internal/chartlayout"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/trade"
)

const (
	lineOrderStatusArmed     = "armed"
	lineOrderStatusTriggered = "triggered"
	lineOrderStatusDisabled  = "disabled"
	lineOrderStatusRejected  = "rejected"
)

type LineOrderRule struct {
	ID              string                    `json:"id"`
	DrawingID       string                    `json:"drawing_id"`
	Owner           string                    `json:"owner"`
	Symbol          string                    `json:"symbol"`
	Type            string                    `json:"type"`
	Variety         string                    `json:"variety"`
	Timeframe       string                    `json:"timeframe"`
	DataMode        string                    `json:"data_mode"`
	Trigger         string                    `json:"trigger"`
	Direction       string                    `json:"direction"`
	OffsetFlag      string                    `json:"offset_flag"`
	Volume          int                       `json:"volume"`
	PriceOffsetTick int                       `json:"price_offset_tick"`
	MaxPriceTicks   int                       `json:"max_price_ticks"`
	Once            bool                      `json:"once"`
	Status          string                    `json:"status"`
	LastPrice       float64                   `json:"last_price,omitempty"`
	LastLinePrice   float64                   `json:"last_line_price,omitempty"`
	LastError       string                    `json:"last_error,omitempty"`
	LastCommandID   string                    `json:"last_command_id,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
	Drawing         chartlayout.DrawingObject `json:"drawing"`
}

type lineOrderEngine struct {
	mu           sync.Mutex
	rules        map[string]*LineOrderRule
	lastSubmitAt time.Time
	minuteStart  time.Time
	minuteCount  int
}

func newLineOrderEngine() *lineOrderEngine {
	return &lineOrderEngine{rules: make(map[string]*LineOrderRule)}
}

func (e *lineOrderEngine) list() []LineOrderRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]LineOrderRule, 0, len(e.rules))
	for _, rule := range e.rules {
		out = append(out, *rule)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

func (e *lineOrderEngine) upsert(req LineOrderRule) (LineOrderRule, error) {
	if strings.TrimSpace(req.DrawingID) == "" {
		return LineOrderRule{}, fmt.Errorf("drawing_id is required")
	}
	if strings.TrimSpace(req.Symbol) == "" {
		return LineOrderRule{}, fmt.Errorf("symbol is required")
	}
	if req.Drawing.Type != "hline" && req.Drawing.Type != "trendline" {
		return LineOrderRule{}, fmt.Errorf("only hline or trendline can be armed")
	}
	trigger := strings.TrimSpace(req.Trigger)
	if trigger == "" {
		trigger = "touch"
	}
	if trigger != "touch" && trigger != "cross_up" && trigger != "cross_down" {
		return LineOrderRule{}, fmt.Errorf("trigger must be touch/cross_up/cross_down")
	}
	direction := strings.TrimSpace(req.Direction)
	if direction != "buy" && direction != "sell" {
		return LineOrderRule{}, fmt.Errorf("direction must be buy or sell")
	}
	offsetFlag := strings.TrimSpace(req.OffsetFlag)
	if offsetFlag == "" {
		offsetFlag = "open"
	}
	if req.Volume <= 0 {
		req.Volume = 1
	}
	if req.MaxPriceTicks <= 0 {
		req.MaxPriceTicks = 10
	}
	now := time.Now()
	req.ID = lineOrderRuleID(req.Owner, req.Symbol, req.Timeframe, req.DataMode, req.DrawingID)
	req.Trigger = trigger
	req.Direction = direction
	req.OffsetFlag = offsetFlag
	req.Once = true
	req.Status = lineOrderStatusArmed
	req.LastError = ""
	req.LastCommandID = ""
	req.CreatedAt = now
	req.UpdatedAt = now
	e.mu.Lock()
	e.rules[req.ID] = &req
	e.mu.Unlock()
	return req, nil
}

func (e *lineOrderEngine) disable(id string) (LineOrderRule, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rule, ok := e.rules[strings.TrimSpace(id)]
	if !ok {
		return LineOrderRule{}, false
	}
	rule.Status = lineOrderStatusDisabled
	rule.UpdatedAt = time.Now()
	return *rule, true
}

func (e *lineOrderEngine) disableAll() []LineOrderRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	out := make([]LineOrderRule, 0, len(e.rules))
	for _, rule := range e.rules {
		if rule.Status == lineOrderStatusArmed {
			rule.Status = lineOrderStatusDisabled
			rule.UpdatedAt = now
		}
		out = append(out, *rule)
	}
	return out
}

func (e *lineOrderEngine) evaluate(update quotes.ChartQuoteUpdate, mode string, svc *trade.Service) []LineOrderRule {
	if svc == nil || appmode.Normalize(mode) == appmode.LiveReal {
		return nil
	}
	price := quoteLatestPrice(update.Snapshot)
	if price <= 0 {
		return nil
	}
	ts := quoteUnix(update.Snapshot)
	tickSize := quotePriceTick(update.Snapshot)
	if tickSize <= 0 {
		tickSize = 1
	}
	sub := update.Subscription
	if sub.Symbol == "" {
		sub.Symbol = update.Snapshot.Symbol
	}
	if sub.DataMode == "" {
		sub.DataMode = update.Snapshot.DataMode
	}

	var fired []LineOrderRule
	e.mu.Lock()
	candidates := make([]*LineOrderRule, 0, len(e.rules))
	for _, rule := range e.rules {
		if rule.Status != lineOrderStatusArmed {
			continue
		}
		if !lineOrderMatchesSubscription(*rule, sub) {
			continue
		}
		linePrice, ok := drawingLinePrice(rule.Drawing, ts)
		if !ok || linePrice <= 0 {
			rule.Status = lineOrderStatusRejected
			rule.LastError = "line price unavailable"
			rule.UpdatedAt = time.Now()
			fired = append(fired, *rule)
			continue
		}
		shouldFire := shouldTrigger(rule.Trigger, rule.LastPrice, price, rule.LastLinePrice, linePrice, tickSize)
		rule.LastPrice = price
		rule.LastLinePrice = linePrice
		rule.UpdatedAt = time.Now()
		if shouldFire {
			candidates = append(candidates, rule)
		}
	}
	e.mu.Unlock()

	for _, rule := range candidates {
		limitPrice := guardedLineOrderPrice(rule.Direction, price, tickSize, rule.PriceOffsetTick)
		if math.Abs(limitPrice-price) > float64(rule.MaxPriceTicks)*tickSize+1e-9 {
			e.markRuleError(rule.ID, "line order price exceeds max price ticks")
			continue
		}
		if err := e.reserveSubmitSlot(rule.ID); err != nil {
			e.markRuleError(rule.ID, err.Error())
			continue
		}
		rec, err := svc.SubmitOrder(context.Background(), trade.SubmitOrderRequest{
			AccountID:  "",
			Symbol:     rule.Symbol,
			ExchangeID: strings.TrimSpace(update.Snapshot.ExchangeID),
			Direction:  rule.Direction,
			OffsetFlag: rule.OffsetFlag,
			LimitPrice: limitPrice,
			Volume:     rule.Volume,
			ClientTag:  "chart-line-order:" + rule.ID,
			Reason:     "line_order",
		})
		e.mu.Lock()
		current := e.rules[rule.ID]
		if current != nil {
			if err != nil {
				current.Status = lineOrderStatusRejected
				current.LastError = err.Error()
			} else {
				current.Status = lineOrderStatusTriggered
				current.LastCommandID = rec.CommandID
				current.LastError = ""
			}
			current.UpdatedAt = time.Now()
			fired = append(fired, *current)
		}
		e.mu.Unlock()
	}
	return fired
}

func (e *lineOrderEngine) reserveSubmitSlot(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if !e.lastSubmitAt.IsZero() && now.Sub(e.lastSubmitAt) < time.Second {
		return fmt.Errorf("line order rate limit: at most 1 order per second")
	}
	minute := now.Truncate(time.Minute)
	if e.minuteStart.IsZero() || !e.minuteStart.Equal(minute) {
		e.minuteStart = minute
		e.minuteCount = 0
	}
	if e.minuteCount >= 10 {
		return fmt.Errorf("line order rate limit: at most 10 orders per minute")
	}
	if rule := e.rules[id]; rule == nil || rule.Status != lineOrderStatusArmed {
		return fmt.Errorf("line order is not armed")
	}
	e.lastSubmitAt = now
	e.minuteCount++
	return nil
}

func (e *lineOrderEngine) markRuleError(id string, msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if rule := e.rules[id]; rule != nil {
		rule.Status = lineOrderStatusRejected
		rule.LastError = msg
		rule.UpdatedAt = time.Now()
	}
}

func lineOrderRuleID(owner, symbol, timeframe, dataMode, drawingID string) string {
	parts := []string{strings.TrimSpace(owner), strings.TrimSpace(symbol), strings.TrimSpace(timeframe), strings.TrimSpace(dataMode), strings.TrimSpace(drawingID)}
	return strings.Join(parts, "|")
}

func lineOrderMatchesSubscription(rule LineOrderRule, sub quotes.ChartSubscription) bool {
	return strings.EqualFold(rule.Symbol, sub.Symbol) &&
		strings.EqualFold(firstNonEmpty(rule.Type, "contract"), firstNonEmpty(sub.Type, "contract")) &&
		strings.EqualFold(firstNonEmpty(rule.Timeframe, "1m"), firstNonEmpty(sub.Timeframe, "1m")) &&
		strings.EqualFold(firstNonEmpty(rule.DataMode, "realtime"), firstNonEmpty(sub.DataMode, "realtime"))
}

func drawingLinePrice(d chartlayout.DrawingObject, ts int64) (float64, bool) {
	if d.Type == "hline" {
		for _, pt := range d.Points {
			if pt.Price != nil && *pt.Price > 0 {
				return *pt.Price, true
			}
		}
		if d.StartPrice != nil && *d.StartPrice > 0 {
			return *d.StartPrice, true
		}
		return 0, false
	}
	if d.Type != "trendline" {
		return 0, false
	}
	var t1, t2 int64
	var p1, p2 float64
	if len(d.Points) >= 2 && d.Points[0].Price != nil && d.Points[1].Price != nil {
		t1, t2 = d.Points[0].Time, d.Points[1].Time
		p1, p2 = *d.Points[0].Price, *d.Points[1].Price
	} else if d.StartPrice != nil && d.EndPrice != nil {
		t1, t2 = d.StartTime, d.EndTime
		p1, p2 = *d.StartPrice, *d.EndPrice
	}
	if t1 == t2 || p1 <= 0 || p2 <= 0 {
		return 0, false
	}
	return p1 + (p2-p1)*float64(ts-t1)/float64(t2-t1), true
}

func shouldTrigger(trigger string, prevPrice, price, prevLine, line float64, tickSize float64) bool {
	if prevPrice <= 0 || prevLine <= 0 {
		return false
	}
	prevDelta := prevPrice - prevLine
	delta := price - line
	switch trigger {
	case "cross_up":
		return prevDelta < 0 && delta >= 0
	case "cross_down":
		return prevDelta > 0 && delta <= 0
	default:
		return (prevDelta < 0 && delta >= 0) || (prevDelta > 0 && delta <= 0) || math.Abs(delta) <= tickSize/2
	}
}

func guardedLineOrderPrice(direction string, price float64, tickSize float64, offsetTick int) float64 {
	offset := float64(offsetTick) * tickSize
	if direction == "sell" {
		return price - offset
	}
	return price + offset
}

func quoteLatestPrice(snapshot quotes.ChartQuoteSnapshot) float64 {
	if snapshot.LatestPrice != nil && *snapshot.LatestPrice > 0 {
		return *snapshot.LatestPrice
	}
	if snapshot.AskPrice1 != nil && *snapshot.AskPrice1 > 0 {
		return *snapshot.AskPrice1
	}
	if snapshot.BidPrice1 != nil && *snapshot.BidPrice1 > 0 {
		return *snapshot.BidPrice1
	}
	return 0
}

func quotePriceTick(snapshot quotes.ChartQuoteSnapshot) float64 {
	if snapshot.PriceTick != nil && *snapshot.PriceTick > 0 {
		return *snapshot.PriceTick
	}
	return 0
}

func quoteUnix(snapshot quotes.ChartQuoteSnapshot) int64 {
	day := strings.TrimSpace(firstNonEmpty(snapshot.ActionDay, snapshot.TradingDay))
	clock := strings.TrimSpace(snapshot.UpdateTime)
	if len(day) == 8 && clock != "" {
		if ts, err := time.ParseInLocation("20060102 15:04:05", day+" "+clock, time.Local); err == nil {
			return ts.Unix()
		}
	}
	return time.Now().Unix()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
