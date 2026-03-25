// md_spi.go 实现 MD API 的回调入口。
// 它负责接收实时 tick、做入站字段清洗，然后把标准化后的 tick 送入 marketDataRuntime 继续处理。
package quotes

import (
	"fmt"
	"math"
	"strings"
	"time"

	"ctp-future-kline/internal/klineclock"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/sessiontime"

	ctp "github.com/kkqy/ctp-go"
)

const (
	latencyLogThreshold = 500 * time.Millisecond
	latencyLogInterval  = 5 * time.Second
)

type mdSpi struct {
	ctp.MarketDataSpi

	// runtime 是实时行情处理核心，所有 tick 最终都交给它处理。
	runtime *marketDataRuntime
	// status 指向全局运行状态中心，用于刷新前置连接和登录状态。
	status *RuntimeStatusCenter
	// onDisconnected 由 mdSession 注入，用于把断线事件转成重连流程。
	onDisconnected func(int)
}

type mdSpiOptions struct {
	// onDisconnected 是行情前置断开时的回调。
	onDisconnected func(int)
	// tickDedupWindow 是重复 tick 的判定窗口。
	tickDedupWindow time.Duration
	// driftThreshold 是可接受的 tick 时间漂移阈值。
	driftThreshold time.Duration
	// driftResumeTicks 是恢复漂移正常状态需要的连续正常 tick 数。
	driftResumeTicks int
	// enableMultiMinute 控制是否继续聚合 mm 周期 bar。
	enableMultiMinute bool
	// flowPath 用于初始化 flow 目录下的文件输出。
	flowPath string
	// onTick 是 tick 旁路回调，供策略和 bus 使用。
	onTick func(tickEvent)
	// onBar 是 bar 旁路回调，供策略和 bus 使用。
	onBar func(minuteBar)
	// onPartialBar 在当前分钟 bar 被 tick 更新后立即触发，供图表实时预览使用。
	onPartialBar func(minuteBar)
	// onPersistTask 在 bar 已进入落库任务队列时触发，供图表 final 事件使用。
	onPersistTask func(persistTask)
}

type tickEvent struct {
	// InstrumentID 是合约代码。
	InstrumentID string
	// ExchangeID 是交易所代码。
	ExchangeID string
	// RawActionDay 保留 CTP 原始 action_day 字段，便于排障。
	RawActionDay string
	// RawTradingDay 保留 CTP 原始 trading_day 字段，便于排障。
	RawTradingDay string
	// ActionDay 是经清洗后的自然日字段。
	ActionDay string
	// TradingDay 是经运行时修正后的业务交易日字段。
	TradingDay string
	// UpdateTime 是 tick 的 HH:MM:SS 时间部分。
	UpdateTime string
	// UpdateMillisec 是 tick 的毫秒部分。
	UpdateMillisec int
	// ReceivedAt 是 Go 进程接收到该 tick 的时间。
	ReceivedAt time.Time
	// CallbackAt 是进入 CTP 回调的时间点。
	CallbackAt time.Time
	// ProcessStartedAt 是运行时开始路由该 tick 的时间。
	ProcessStartedAt time.Time
	// LockAcquiredAt 是进入关键处理区后的时间，用于延迟分析。
	LockAcquiredAt time.Time
	// SideEffectEnqueuedAt 是旁路事件入队时间。
	SideEffectEnqueuedAt time.Time
	// SideEffectHandledAt 是旁路事件实际处理完成时间。
	SideEffectHandledAt time.Time
	// LastPrice 是最新成交价，也是分钟线价格主字段。
	LastPrice float64
	// Volume 是 CTP 返回的累计成交量。
	Volume int
	// OpenInterest 是当前持仓量。
	OpenInterest float64
	// SettlementPrice 是结算价；异常极值会在入站时清洗为 0。
	SettlementPrice float64
	// BidPrice1 是买一价。
	BidPrice1 float64
	// AskPrice1 是卖一价。
	AskPrice1 float64
}

type tickInputData struct {
	// InstrumentID 是从 CTP 回调直接取出的合约代码。
	InstrumentID string
	// ExchangeID 是从 CTP 回调直接取出的交易所代码。
	ExchangeID string
	// ActionDay 是原始自然日字段。
	ActionDay string
	// TradingDay 是原始交易日字段。
	TradingDay string
	// UpdateTime 是原始时间字符串。
	UpdateTime string
	// UpdateMillisec 是原始毫秒值。
	UpdateMillisec int
	// ReceivedAt 是接收时间。
	ReceivedAt time.Time
	// CallbackAt 是回调时间。
	CallbackAt time.Time
	// LastPrice 是原始最新价。
	LastPrice float64
	// Volume 是原始累计成交量。
	Volume int
	// OpenInterest 是原始持仓量。
	OpenInterest float64
	// SettlementPrice 是原始结算价，进入 runtime 前已允许做清洗。
	SettlementPrice float64
	// BidPrice1 是原始买一价。
	BidPrice1 float64
	// AskPrice1 是原始卖一价。
	AskPrice1 float64
}

type minuteTickSnapshot struct {
	// TickTime 是该 tick 对应到业务时间轴后的实际时刻。
	TickTime time.Time
	// ReceivedAt 是该 tick 的接收时间。
	ReceivedAt time.Time
	// UpdateTime 是原始 HH:MM:SS 字段。
	UpdateTime string
	// UpdateMillisec 是原始毫秒字段。
	UpdateMillisec int
	// Price 是该 tick 的有效价格。
	Price float64
	// CurrentVolume 是该 tick 对应的累计成交量。
	CurrentVolume int
	// VolumeDelta 是相对上一 tick 算出的增量成交量。
	VolumeDelta int64
	// OpenInterest 是该 tick 对应的持仓量。
	OpenInterest float64
}

type tickFingerprintState struct {
	// fingerprint 是用于去重比较的指纹字符串。
	fingerprint string
	// at 是记录该指纹被看到的时间。
	at time.Time
	// tick 保存生成该指纹时对应的完整 tick，便于日志比较。
	tick tickEvent
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
		tickDedupWindow:   opts.tickDedupWindow,
		driftThreshold:    opts.driftThreshold,
		driftResumeTicks:  opts.driftResumeTicks,
		enableMultiMinute: opts.enableMultiMinute,
		flowPath:          opts.flowPath,
		onTick:            opts.onTick,
		onBar:             opts.onBar,
		onPartialBar:      opts.onPartialBar,
		onPersistTask:     opts.onPersistTask,
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
	onRtnDepthMarketDataRateProbe.Inc()
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
