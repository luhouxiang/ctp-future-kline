package trader

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
	ctp "github.com/kkqy/ctp-go"
)

type mdSpi struct {
	ctp.MarketDataSpi

	store   *klineStore
	l9Async *l9AsyncCalculator
	status  *RuntimeStatusCenter
	clock   *klineclock.CalendarResolver

	mu       sync.Mutex
	states   map[string]*instrumentMinuteState
	lastVols map[string]int

	onDisconnected       func(int)
	tickDedupWindow      time.Duration
	lastTickFingerprints map[string]tickFingerprintState
	driftThreshold       time.Duration
	driftResumeTicks     int
	driftPaused          bool
	driftResumeCount     int
	onTick               func(tickEvent)
	onBar                func(minuteBar)
}

type instrumentMinuteState struct {
	bar minuteBar
}

type tickFingerprintState struct {
	fingerprint string
	at          time.Time
}

func newMdSpi(store *klineStore, l9Async *l9AsyncCalculator) *mdSpi {
	return newMdSpiWithOptions(store, l9Async, mdSpiOptions{})
}

type mdSpiOptions struct {
	onDisconnected   func(int)
	tickDedupWindow  time.Duration
	driftThreshold   time.Duration
	driftResumeTicks int
	onTick           func(tickEvent)
	onBar            func(minuteBar)
}

type tickEvent struct {
	InstrumentID    string
	ExchangeID      string
	ActionDay       string
	TradingDay      string
	UpdateTime      string
	LastPrice       float64
	Volume          int
	OpenInterest    float64
	SettlementPrice float64
}

func newMdSpiWithOptions(store *klineStore, l9Async *l9AsyncCalculator, opts mdSpiOptions) *mdSpi {
	var clock *klineclock.CalendarResolver
	if store != nil {
		clock = klineclock.NewCalendarResolver(store.DB())
	}
	tickDedupWindow := opts.tickDedupWindow
	if tickDedupWindow <= 0 {
		tickDedupWindow = 2 * time.Second
	}
	driftThreshold := opts.driftThreshold
	if driftThreshold <= 0 {
		driftThreshold = 5 * time.Second
	}
	driftResumeTicks := opts.driftResumeTicks
	if driftResumeTicks <= 0 {
		driftResumeTicks = 3
	}
	return &mdSpi{
		store:                store,
		l9Async:              l9Async,
		states:               make(map[string]*instrumentMinuteState),
		lastVols:             make(map[string]int),
		clock:                clock,
		onDisconnected:       opts.onDisconnected,
		tickDedupWindow:      tickDedupWindow,
		lastTickFingerprints: make(map[string]tickFingerprintState),
		driftThreshold:       driftThreshold,
		driftResumeTicks:     driftResumeTicks,
		onTick:               opts.onTick,
		onBar:                opts.onBar,
	}
}

func newMdSpiWithStatus(store *klineStore, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter) *mdSpi {
	s := newMdSpiWithOptions(store, l9Async, mdSpiOptions{})
	s.status = status
	return s
}

func newMdSpiWithStatusAndOptions(store *klineStore, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter, opts mdSpiOptions) *mdSpi {
	s := newMdSpiWithOptions(store, l9Async, opts)
	s.status = status
	return s
}

func (p *mdSpi) OnFrontConnected() {
	logger.Info("md front connected")
	if p.status != nil {
		p.status.MarkMdFrontConnected()
	}
}

func (p *mdSpi) OnFrontDisconnected(nReason int) {
	logger.Error("md front disconnected", "reason", nReason)
	if p.status != nil {
		p.status.MarkMdFrontDisconnected(nReason)
	}
	if p.onDisconnected != nil {
		p.onDisconnected(nReason)
	}
}

func (p *mdSpi) OnHeartBeatWarning(nTimeLapse int) {
	logger.Error("md heartbeat warning", "time_lapse", nTimeLapse)
}

func (p *mdSpi) OnRspError(
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Error(
		"md response error",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", safeRspErrorID(pRspInfo),
	)
}

func (p *mdSpi) OnRspUserLogin(
	loginField ctp.CThostFtdcRspUserLoginField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "md",
		"callback", "OnRspUserLogin",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", pRspInfo.GetErrorID(),
		"login_time", loginField.GetLoginTime(),
		"trading_day", loginField.GetTradingDay(),
	)
	if pRspInfo.GetErrorID() == 0 {
		logger.Info("md user login success")
		if p.status != nil {
			p.status.MarkMdLogin(loginField.GetLoginTime(), loginField.GetTradingDay())
		}
		return
	}
	logger.Error("md user login failed", "error_id", pRspInfo.GetErrorID())
}

func (p *mdSpi) OnRspSubMarketData(
	pSpecificInstrument ctp.CThostFtdcSpecificInstrumentField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "md",
		"callback", "OnRspSubMarketData",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", pRspInfo.GetErrorID(),
		"instrument_id", pSpecificInstrument.GetInstrumentID(),
	)
	if pRspInfo.GetErrorID() != 0 {
		logger.Error(
			"subscribe market data failed",
			"instrument_id", pSpecificInstrument.GetInstrumentID(),
			"error_id", pRspInfo.GetErrorID(),
		)
		return
	}
	logger.Info("subscribed instrument", "instrument_id", pSpecificInstrument.GetInstrumentID(), "is_last", bIsLast)
}

func (p *mdSpi) OnRspUnSubMarketData(
	pSpecificInstrument ctp.CThostFtdcSpecificInstrumentField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "md",
		"callback", "OnRspUnSubMarketData",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", pRspInfo.GetErrorID(),
		"instrument_id", pSpecificInstrument.GetInstrumentID(),
	)
	if pRspInfo.GetErrorID() != 0 {
		logger.Error(
			"unsubscribe market data failed",
			"instrument_id", pSpecificInstrument.GetInstrumentID(),
			"error_id", pRspInfo.GetErrorID(),
		)
	}
}

func (p *mdSpi) OnRtnDepthMarketData(pDepthMarketData ctp.CThostFtdcDepthMarketDataField) {
	instrumentID := strings.TrimSpace(pDepthMarketData.GetInstrumentID())
	if instrumentID == "" {
		return
	}

	exchangeID := strings.TrimSpace(pDepthMarketData.GetExchangeID())
	minuteTime, adjustedTime, adjustedTickTime, err := parseTickTimes(
		pDepthMarketData.GetActionDay(),
		pDepthMarketData.GetTradingDay(),
		pDepthMarketData.GetUpdateTime(),
		p.clock,
	)
	if err != nil {
		logger.Error("parse tick time failed", "instrument_id", instrumentID, "error", err)
		return
	}
	now := time.Now()
	if p.status != nil {
		p.status.MarkTick(now)
	}

	price := pDepthMarketData.GetLastPrice()
	if !isFinitePositivePrice(price) {
		return
	}

	settlement := pDepthMarketData.GetSettlementPrice()
	if !isFinitePrice(settlement) {
		settlement = 0
	}

	variety := normalizeVariety(instrumentID)
	if variety == "" {
		logger.Error("invalid instrument variety", "instrument_id", instrumentID)
		return
	}

	currentVol := pDepthMarketData.GetVolume()
	openInterest := pDepthMarketData.GetOpenInterest()

	fingerprint := fmt.Sprintf(
		"%s|%s|%s|%.8f|%d|%.8f",
		strings.TrimSpace(pDepthMarketData.GetTradingDay()),
		strings.TrimSpace(pDepthMarketData.GetActionDay()),
		strings.TrimSpace(pDepthMarketData.GetUpdateTime()),
		price,
		currentVol,
		openInterest,
	)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shouldDropDuplicateTick(instrumentID, fingerprint, now) {
		if p.status != nil {
			p.status.MarkTickDedupDropped()
		}
		return
	}
	if p.onTick != nil {
		p.onTick(tickEvent{
			InstrumentID:    instrumentID,
			ExchangeID:      exchangeID,
			ActionDay:       strings.TrimSpace(pDepthMarketData.GetActionDay()),
			TradingDay:      strings.TrimSpace(pDepthMarketData.GetTradingDay()),
			UpdateTime:      strings.TrimSpace(pDepthMarketData.GetUpdateTime()),
			LastPrice:       price,
			Volume:          currentVol,
			OpenInterest:    openInterest,
			SettlementPrice: settlement,
		})
	}

	if shouldCheckTickDrift(now, adjustedTickTime) {
		driftSec := math.Abs(now.Sub(adjustedTickTime).Seconds())
		if p.status != nil {
			p.status.MarkDrift(driftSec, p.driftPaused)
		}
		if time.Duration(driftSec*float64(time.Second)) > p.driftThreshold {
			p.driftPaused = true
			p.driftResumeCount = 0
			if p.status != nil {
				p.status.MarkDrift(driftSec, true)
			}
			logger.Error(
				"tick drift too large, pause writing",
				"instrument_id", instrumentID,
				"drift_seconds", driftSec,
				"threshold_seconds", p.driftThreshold.Seconds(),
			)
			return
		}
		if p.driftPaused {
			p.driftResumeCount++
			if p.driftResumeCount < p.driftResumeTicks {
				if p.status != nil {
					p.status.MarkDrift(driftSec, true)
				}
				return
			}
			p.driftPaused = false
			p.driftResumeCount = 0
			if p.status != nil {
				p.status.MarkDriftRecovered()
			}
			logger.Info("tick drift recovered, resume writing", "instrument_id", instrumentID)
		}
	}

	volumeDelta := int64(0)
	if last, ok := p.lastVols[instrumentID]; ok {
		if currentVol >= last {
			volumeDelta = int64(currentVol - last)
		} else {
			volumeDelta = int64(currentVol)
		}
	}
	p.lastVols[instrumentID] = currentVol

	state := p.states[instrumentID]
	if state == nil {
		p.states[instrumentID] = &instrumentMinuteState{
			bar: minuteBar{
				Variety:         variety,
				InstrumentID:    instrumentID,
				Exchange:        exchangeID,
				MinuteTime:      minuteTime,
				AdjustedTime:    adjustedTime,
				Period:          "1m",
				Open:            price,
				High:            price,
				Low:             price,
				Close:           price,
				Volume:          volumeDelta,
				OpenInterest:    openInterest,
				SettlementPrice: settlement,
			},
		}
	} else {
		if !state.bar.MinuteTime.Equal(minuteTime) {
			endedBar := state.bar
			if err := p.store.UpsertMinuteBar(endedBar); err != nil {
				logger.Error("flush minute bar failed", "instrument_id", instrumentID, "error", err)
			} else if p.l9Async != nil {
				p.l9Async.ObserveMinuteBar(endedBar)
				p.l9Async.Submit(endedBar.Variety, endedBar.MinuteTime)
			}
			if p.onBar != nil {
				p.onBar(endedBar)
			}
			state.bar = minuteBar{
				Variety:         variety,
				InstrumentID:    instrumentID,
				Exchange:        exchangeID,
				MinuteTime:      minuteTime,
				AdjustedTime:    adjustedTime,
				Period:          "1m",
				Open:            price,
				High:            price,
				Low:             price,
				Close:           price,
				Volume:          volumeDelta,
				OpenInterest:    openInterest,
				SettlementPrice: settlement,
			}
		} else {
			if price > state.bar.High {
				state.bar.High = price
			}
			if price < state.bar.Low {
				state.bar.Low = price
			}
			if exchangeID != "" {
				state.bar.Exchange = exchangeID
			}
			state.bar.Close = price
			state.bar.Volume += volumeDelta
			state.bar.OpenInterest = openInterest
			state.bar.SettlementPrice = settlement
			state.bar.AdjustedTime = adjustedTime
		}
	}

	logger.Info(
		"market data push",
		"instrument_id", instrumentID,
		"exchange_id", exchangeID,
		"last_price", price,
		"bid1", pDepthMarketData.GetBidPrice1(),
		"ask1", pDepthMarketData.GetAskPrice1(),
	)
}

func (p *mdSpi) shouldDropDuplicateTick(instrumentID string, fingerprint string, now time.Time) bool {
	if p.tickDedupWindow <= 0 {
		return false
	}
	last, ok := p.lastTickFingerprints[instrumentID]
	p.lastTickFingerprints[instrumentID] = tickFingerprintState{
		fingerprint: fingerprint,
		at:          now,
	}
	if !ok {
		return false
	}
	if fingerprint != last.fingerprint {
		return false
	}
	return now.Sub(last.at) <= p.tickDedupWindow
}

func shouldCheckTickDrift(now time.Time, adjustedTickTime time.Time) bool {
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	tickDay := time.Date(adjustedTickTime.Year(), adjustedTickTime.Month(), adjustedTickTime.Day(), 0, 0, 0, 0, time.Local)
	deltaDays := nowDay.Sub(tickDay).Hours() / 24
	if deltaDays < 0 {
		deltaDays = -deltaDays
	}
	return deltaDays <= 1
}

func (p *mdSpi) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for instrumentID, state := range p.states {
		if state == nil {
			continue
		}
		endedBar := state.bar
		if err := p.store.UpsertMinuteBar(endedBar); err != nil {
			return fmt.Errorf("flush minute bar for %s failed: %w", instrumentID, err)
		}
		if p.l9Async != nil {
			p.l9Async.ObserveMinuteBar(endedBar)
			p.l9Async.Submit(endedBar.Variety, endedBar.MinuteTime)
		}
		if p.onBar != nil {
			p.onBar(endedBar)
		}
	}
	return nil
}

func parseTickTimes(actionDay string, tradingDay string, updateTime string, clock *klineclock.CalendarResolver) (time.Time, time.Time, time.Time, error) {
	dayText := strings.TrimSpace(tradingDay)
	if len(dayText) != 8 {
		dayText = strings.TrimSpace(actionDay)
	}
	if len(dayText) != 8 {
		return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("invalid trading_day/action_day")
	}
	day, err := klineclock.ParseTradingDay(dayText)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	updateTime = strings.TrimSpace(updateTime)
	if updateTime == "" {
		return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("empty update_time")
	}
	clockTime, err := time.ParseInLocation("15:04:05", updateTime, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	hhmm := klineclock.HHMMFromTime(clockTime)
	base, adjusted, err := klineclock.BuildBarTimes(day, hhmm, clock)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	baseMinute := base.Truncate(time.Minute)
	adjustedMinute := adjusted.Truncate(time.Minute)
	adjustedTick := time.Date(adjusted.Year(), adjusted.Month(), adjusted.Day(), clockTime.Hour(), clockTime.Minute(), clockTime.Second(), 0, time.Local)
	return baseMinute, adjustedMinute, adjustedTick, nil
}

func isFinitePositivePrice(price float64) bool {
	return isFinitePrice(price) && price > 0
}

func isFinitePrice(price float64) bool {
	return !math.IsNaN(price) && !math.IsInf(price, 0) && math.Abs(price) < 1e20
}
