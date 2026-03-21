package trader

import (
	"fmt"
	"math"
	"strings"
	"time"

	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/sessiontime"

	ctp "github.com/kkqy/ctp-go"
)

const (
	latencyLogThreshold = 500 * time.Millisecond
	latencyLogInterval  = 5 * time.Second
)

type mdSpi struct {
	ctp.MarketDataSpi

	runtime        *marketDataRuntime
	status         *RuntimeStatusCenter
	onDisconnected func(int)
}

type mdSpiOptions struct {
	onDisconnected   func(int)
	tickDedupWindow  time.Duration
	driftThreshold   time.Duration
	driftResumeTicks int
	enableMultiMinute bool
	flowPath         string
	onTick           func(tickEvent)
	onBar            func(minuteBar)
}

type tickEvent struct {
	InstrumentID         string
	ExchangeID           string
	RawActionDay         string
	RawTradingDay        string
	ActionDay            string
	TradingDay           string
	UpdateTime           string
	UpdateMillisec       int
	ReceivedAt           time.Time
	CallbackAt           time.Time
	ProcessStartedAt     time.Time
	LockAcquiredAt       time.Time
	SideEffectEnqueuedAt time.Time
	SideEffectHandledAt  time.Time
	LastPrice            float64
	Volume               int
	OpenInterest         float64
	SettlementPrice      float64
	BidPrice1            float64
	AskPrice1            float64
}

type tickInputData struct {
	InstrumentID    string
	ExchangeID      string
	ActionDay       string
	TradingDay      string
	UpdateTime      string
	UpdateMillisec  int
	ReceivedAt      time.Time
	CallbackAt      time.Time
	LastPrice       float64
	Volume          int
	OpenInterest    float64
	SettlementPrice float64
	BidPrice1       float64
	AskPrice1       float64
}

type minuteTickSnapshot struct {
	TickTime       time.Time
	ReceivedAt     time.Time
	UpdateTime     string
	UpdateMillisec int
	Price          float64
	CurrentVolume  int
	VolumeDelta    int64
	OpenInterest   float64
}

type tickFingerprintState struct {
	fingerprint string
	at          time.Time
	tick        tickEvent
}

func newMdSpi(store *klineStore, l9Async *l9AsyncCalculator) *mdSpi {
	return newMdSpiWithOptions(store, l9Async, mdSpiOptions{})
}

func newMdSpiWithStatus(store *klineStore, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter) *mdSpi {
	return newMdSpiWithStatusAndOptions(store, l9Async, status, mdSpiOptions{})
}

func newMdSpiWithOptions(store *klineStore, l9Async *l9AsyncCalculator, opts mdSpiOptions) *mdSpi {
	return newMdSpiWithStatusAndOptions(store, l9Async, nil, opts)
}

func newMdSpiWithStatusAndOptions(store *klineStore, l9Async *l9AsyncCalculator, status *RuntimeStatusCenter, opts mdSpiOptions) *mdSpi {
	spi := &mdSpi{
		status:         status,
		onDisconnected: opts.onDisconnected,
	}
	spi.runtime = newMarketDataRuntime(store, l9Async, status, runtimeOptions{
		tickDedupWindow:  opts.tickDedupWindow,
		driftThreshold:   opts.driftThreshold,
		driftResumeTicks: opts.driftResumeTicks,
		enableMultiMinute: opts.enableMultiMinute,
		flowPath:         opts.flowPath,
		onTick:           opts.onTick,
		onBar:            opts.onBar,
	})
	return spi
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

func (p *mdSpi) OnRspError(pRspInfo ctp.CThostFtdcRspInfoField, nRequestID int, bIsLast bool) {
	logger.Error(
		"md response error",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", safeRspErrorID(pRspInfo),
	)
}

func (p *mdSpi) OnRspUserLogin(loginField ctp.CThostFtdcRspUserLoginField, pRspInfo ctp.CThostFtdcRspInfoField, nRequestID int, bIsLast bool) {
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
	if pRspInfo.GetErrorID() == 0 && p.status != nil {
		p.status.MarkMdLogin(loginField.GetLoginTime(), loginField.GetTradingDay())
	}
}

func (p *mdSpi) OnRspSubMarketData(pSpecificInstrument ctp.CThostFtdcSpecificInstrumentField, pRspInfo ctp.CThostFtdcRspInfoField, nRequestID int, bIsLast bool) {
	logger.Info(
		"subscribed instrument response",
		"instrument_id", pSpecificInstrument.GetInstrumentID(),
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", safeRspErrorID(pRspInfo),
	)
}

func (p *mdSpi) OnRspUnSubMarketData(pSpecificInstrument ctp.CThostFtdcSpecificInstrumentField, pRspInfo ctp.CThostFtdcRspInfoField, nRequestID int, bIsLast bool) {
	logger.Info(
		"unsubscribed instrument response",
		"instrument_id", pSpecificInstrument.GetInstrumentID(),
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", safeRspErrorID(pRspInfo),
	)
}

func (p *mdSpi) OnRtnDepthMarketData(pDepthMarketData ctp.CThostFtdcDepthMarketDataField) {
	receivedAt := time.Now()
	_ = p.runtime.onLiveTick(tickInputData{
		InstrumentID:    strings.TrimSpace(pDepthMarketData.GetInstrumentID()),
		ExchangeID:      strings.TrimSpace(pDepthMarketData.GetExchangeID()),
		ActionDay:       strings.TrimSpace(pDepthMarketData.GetActionDay()),
		TradingDay:      strings.TrimSpace(pDepthMarketData.GetTradingDay()),
		UpdateTime:      strings.TrimSpace(pDepthMarketData.GetUpdateTime()),
		UpdateMillisec:  pDepthMarketData.GetUpdateMillisec(),
		ReceivedAt:      receivedAt,
		CallbackAt:      receivedAt,
		LastPrice:       pDepthMarketData.GetLastPrice(),
		Volume:          pDepthMarketData.GetVolume(),
		OpenInterest:    pDepthMarketData.GetOpenInterest(),
		SettlementPrice: sanitizeSettlementPrice(pDepthMarketData.GetSettlementPrice()),
		BidPrice1:       pDepthMarketData.GetBidPrice1(),
		AskPrice1:       pDepthMarketData.GetAskPrice1(),
	})
}

func (p *mdSpi) ProcessReplayTick(ev tickEvent) error {
	return p.runtime.onReplayTick(ev)
}

func (p *mdSpi) Flush() error {
	return p.runtime.flush()
}

func (p *mdSpi) ResetReplayStageLogState() {
	p.runtime.resetReplayStageLogState()
}

func shouldLogLatency(args ...any) bool {
	for i := 0; i+1 < len(args); i += 2 {
		switch v := args[i+1].(type) {
		case float64:
			if v >= latencyLogThreshold.Seconds()*1000 {
				return true
			}
		case int:
			if time.Duration(v)*time.Millisecond >= latencyLogThreshold {
				return true
			}
		case int64:
			if time.Duration(v)*time.Millisecond >= latencyLogThreshold {
				return true
			}
		}
	}
	return false
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

func buildTickDedupFingerprint(in tickInputData, price float64, currentVol int, openInterest float64) string {
	return fmt.Sprintf(
		"%s|%s|%s|%03d|%.8f|%d|%.8f|%.8f|%.8f",
		strings.TrimSpace(in.TradingDay),
		strings.TrimSpace(in.ActionDay),
		strings.TrimSpace(in.UpdateTime),
		in.UpdateMillisec,
		price,
		currentVol,
		openInterest,
		in.BidPrice1,
		in.AskPrice1,
	)
}

func parseTickTimes(actionDay string, tradingDay string, updateTime string, updateMillisec int, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) (time.Time, time.Time, time.Time, error) {
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
	rawMinute := clockTime.Hour()*60 + clockTime.Minute()
	_, adjusted, err := klineclock.BuildBarTimes(day, hhmm, clock)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	labelMinute, ok := resolveLabelMinute(rawMinute, sessions)
	if !ok {
		labelMinute = rawMinute + 1
	}
	baseMinute := labelMinuteOnDay(day, labelMinute)
	adjustedMinute := labelMinuteOnDay(adjusted, labelMinute)
	adjustedTick := time.Date(adjusted.Year(), adjusted.Month(), adjusted.Day(), clockTime.Hour(), clockTime.Minute(), clockTime.Second(), 0, time.Local)
	return baseMinute, adjustedMinute, adjustedTick, nil
}

func parseTickTimesWithMillis(actionDay string, tradingDay string, updateTime string, updateMillisec int, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) (time.Time, time.Time, time.Time, error) {
	baseMinute, adjustedMinute, adjustedTick, err := parseTickTimes(actionDay, tradingDay, updateTime, updateMillisec, sessions, clock)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	if updateMillisec < 0 {
		updateMillisec = 0
	}
	if updateMillisec > 999 {
		updateMillisec = updateMillisec % 1000
	}
	adjustedTick = adjustedTick.Add(time.Duration(updateMillisec) * time.Millisecond)
	return baseMinute, adjustedMinute, adjustedTick, nil
}

func resolveLabelMinute(hhmm int, sessions []sessiontime.Range) (int, bool) {
	return sessiontime.LabelMinute(hhmm, sessions)
}

func labelMinuteOnDay(day time.Time, minuteOfDay int) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), minuteOfDay/60, minuteOfDay%60, 0, 0, time.Local)
}

func computeBucketVolume(currentVol int, prevBucketCloseVol int, hasPrevBucket bool) int64 {
	if !hasPrevBucket {
		return 0
	}
	delta := int64(currentVol - prevBucketCloseVol)
	if delta < 0 {
		return 0
	}
	return delta
}

func isValidTradePrice(price float64) bool {
	return isFinitePrice(price) && price > 0
}

func sanitizeSettlementPrice(price float64) float64 {
	if !isFinitePrice(price) {
		return 0
	}
	return price
}

func isFinitePrice(price float64) bool {
	return !math.IsNaN(price) && !math.IsInf(price, 0) && math.Abs(price) < 1e20
}
