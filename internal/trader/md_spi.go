package trader

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/mmkline"
	"ctp-go-demo/internal/sessiontime"

	ctp "github.com/kkqy/ctp-go"
)

const (
	latencyLogThreshold = 500 * time.Millisecond
	latencyLogInterval  = 5 * time.Second
)

// mdSpi 是行情接收与分钟线聚合的核心实现。
//
// 它同时服务两条输入链路：
// 1. 实时 CTP 行情回调 OnRtnDepthMarketData
// 2. 历史回放入口 ProcessReplayTick
//
// 两条链路最终都会汇入 processTick，在那里完成：
// 1. 时间修正与字段清洗
// 2. 非法价格过滤
// 3. 重复 tick 去重
// 4. 增量成交量计算
// 5. 1 分钟 bar 聚合
// 6. 1m 落库、mm 重建、L9 更新
type mdSpi struct {
	ctp.MarketDataSpi

	// store 是分钟线的落库入口。
	// 实时 tick 经过聚合后先写入 1m K 线，再触发多周期重建。
	store *klineStore
	// l9Async 用于异步维护主连/L9 数据，避免在行情回调里同步做重计算。
	l9Async *l9AsyncCalculator
	// status 汇总运行态指标，供健康检查、监控或 UI 展示使用。
	status *RuntimeStatusCenter
	// clock 负责根据交易日历把交易所时间映射到实际归属的分钟。
	// 夜盘跨日、节假日前后等场景都依赖它完成修正。
	clock *klineclock.CalendarResolver
	// sessionResolver 负责按品种加载交易时段，用于标签分钟映射。
	sessionResolver *sessionResolver

	mu sync.Mutex
	// states 保存每个合约当前正在构建中的 1 分钟 bar。
	states map[string]*instrumentMinuteState
	// lastVols 记录每个合约上一笔累计成交量，用来计算当前 tick 对应的增量成交量。
	lastVols map[string]int

	onDisconnected func(int)
	// tickDedupWindow 定义重复 tick 的判定窗口。
	// CTP 在网络抖动或重连阶段可能重复推送完全相同的行情，这里做近时间窗口去重。
	tickDedupWindow      time.Duration
	lastTickFingerprints map[string]tickFingerprintState
	replayProcessSeen    map[string]struct{}
	replayPipelineSeen   map[string]struct{}
	// driftThreshold 用于记录“本机接收时间”和“tick 自身时间”之间的偏差阈值。
	// 当前实现只打日志和指标，不阻止写入，避免在时钟漂移时丢失行情。
	driftThreshold   time.Duration
	driftResumeTicks int
	driftPaused      bool
	driftResumeCount int
	// onTick/onBar 是可选观察钩子，通常给回放、测试或旁路采集逻辑使用。
	onTick         func(tickEvent)
	onBar          func(minuteBar)
	mmRebuildSem   chan struct{}
	mmRebuildWG    sync.WaitGroup
	mmRebuildErr   atomic.Pointer[error]
	lastDriftLog   map[string]time.Time
	lastLatencyLog map[string]time.Time
}

// instrumentMinuteState 保存某个合约当前分钟仍在累计中的 bar。
type instrumentMinuteState struct {
	// bar 表示当前分钟尚未封口的聚合结果。
	bar                 minuteBar
	lastTick            minuteTickSnapshot
	prevBucketCloseVol  int
	hasPrevBucketVolume bool
}

// tickFingerprintState 记录最近一次 tick 指纹和进入时间，用于短窗口去重。
type tickFingerprintState struct {
	// fingerprint 由交易日、业务日、更新时间、价格、成交量、持仓量等字段拼成，
	// 用来识别“内容完全相同”的重复 tick。
	fingerprint string
	// at 是该指纹被最近一次接收的本地时间。
	at time.Time
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

func newMdSpi(store *klineStore, l9Async *l9AsyncCalculator) *mdSpi {
	return newMdSpiWithOptions(store, l9Async, mdSpiOptions{})
}

// mdSpiOptions 统一承载可选回调和链路参数，便于实时与回放模式共用同一实现。
type mdSpiOptions struct {
	onDisconnected   func(int)
	tickDedupWindow  time.Duration
	driftThreshold   time.Duration
	driftResumeTicks int
	onTick           func(tickEvent)
	onBar            func(minuteBar)
}

// tickEvent 是系统内部统一使用的 tick 结构。
//
// 时间字段语义：
// 1. ReceivedAt: 当前这次处理链路使用的本地服务器接收时间
// 2. 业务时间在聚合阶段内部计算，不由输入 tick 直接携带
type tickEvent struct {
	// tickEvent 是内部统一的 tick 事件结构。
	// 它既可承接实时 CTP 回调，也可承接离线回放，因此同时保留原始字段和修正后的时间。
	InstrumentID         string
	ExchangeID           string
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

// tickInputData 是 processTick 的输入结构。
// 相比 tickEvent，它更强调“处理入参”语义。
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

// newMdSpiWithOptions 初始化行情处理器，并为去重、漂移检测等参数补默认值。
func newMdSpiWithOptions(store *klineStore, l9Async *l9AsyncCalculator, opts mdSpiOptions) *mdSpi {
	var clock *klineclock.CalendarResolver
	var sessions *sessionResolver
	if store != nil {
		// 时间归属修正依赖数据库中的交易日日历，因此只有在 store 可用时才初始化。
		clock = klineclock.NewCalendarResolver(store.DB())
		sessions = newSessionResolver(store.DB())
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
		sessionResolver:      sessions,
		onDisconnected:       opts.onDisconnected,
		tickDedupWindow:      tickDedupWindow,
		lastTickFingerprints: make(map[string]tickFingerprintState),
		replayProcessSeen:    make(map[string]struct{}),
		replayPipelineSeen:   make(map[string]struct{}),
		driftThreshold:       driftThreshold,
		driftResumeTicks:     driftResumeTicks,
		onTick:               opts.onTick,
		onBar:                opts.onBar,
		mmRebuildSem:         make(chan struct{}, 4),
		lastDriftLog:         make(map[string]time.Time),
		lastLatencyLog:       make(map[string]time.Time),
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

// OnFrontConnected 表示行情前置连接建立，但此时还不代表登录完成。
func (p *mdSpi) OnFrontConnected() {
	// 仅表示 TCP/前置连通，尚不等于登录成功。
	logger.Info("md front connected")
	if p.status != nil {
		p.status.MarkMdFrontConnected()
	}
}

// OnFrontDisconnected 只做状态更新和旁路通知，真正的重连由上层会话管理器负责。
func (p *mdSpi) OnFrontDisconnected(nReason int) {
	// 前置断开后通常会由上层连接管理器负责重连，这里只做状态更新和回调通知。
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

// OnRspUserLogin 记录行情登录回包，用于确认后续是否可以订阅行情。
func (p *mdSpi) OnRspUserLogin(
	loginField ctp.CThostFtdcRspUserLoginField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	// 登录回包成功后，后续才允许订阅行情。
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

// OnRspSubMarketData 记录订阅结果。
// CTP 会对每个订阅目标各返回一条响应，bIsLast 表示这一批订阅响应结束。
func (p *mdSpi) OnRspSubMarketData(
	pSpecificInstrument ctp.CThostFtdcSpecificInstrumentField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	// CTP 对每个订阅品种都会返回一条响应，bIsLast 表示本批次响应结束。
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

// OnRtnDepthMarketData 是实时行情主入口。
// 这里会立即记录本地接收时间并写入 ReceivedAt。
func (p *mdSpi) OnRtnDepthMarketData(pDepthMarketData ctp.CThostFtdcDepthMarketDataField) {
	receivedAt := time.Now()
	// 实时行情主入口：
	// 1. 从 CTP 原始结构体抽取字段
	// 2. 记录本地接收时间
	// 3. 交给统一的 processTick 完成清洗、聚合和落库
	_ = p.processTick(tickInputData{
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
		SettlementPrice: pDepthMarketData.GetSettlementPrice(),
		BidPrice1:       pDepthMarketData.GetBidPrice1(),
		AskPrice1:       pDepthMarketData.GetAskPrice1(),
	}, true, false)
}

// ProcessReplayTick 是历史回放入口。
// 回放时直接使用 tick 数据中的 ReceivedAt，语义与实时链路中的 time.Now() 一致。
func (p *mdSpi) ProcessReplayTick(ev tickEvent) error {
	p.logFirstReplayProcess(ev)
	return p.processTick(tickInputData{
		InstrumentID:    strings.TrimSpace(ev.InstrumentID),
		ExchangeID:      strings.TrimSpace(ev.ExchangeID),
		ActionDay:       strings.TrimSpace(ev.ActionDay),
		TradingDay:      strings.TrimSpace(ev.TradingDay),
		UpdateTime:      strings.TrimSpace(ev.UpdateTime),
		UpdateMillisec:  ev.UpdateMillisec,
		ReceivedAt:      ev.ReceivedAt,
		CallbackAt:      ev.CallbackAt,
		LastPrice:       ev.LastPrice,
		Volume:          ev.Volume,
		OpenInterest:    ev.OpenInterest,
		SettlementPrice: ev.SettlementPrice,
		BidPrice1:       ev.BidPrice1,
		AskPrice1:       ev.AskPrice1,
	}, false, true)

}

// processTick 是 tick 处理链路的核心。
//
// 无论 tick 来自实时行情还是历史回放，都会在这里收敛成统一流程：
// 1. 时间修正
// 2. 非法价格过滤
// 3. 去重与漂移检测
// 4. 增量成交量推导
// 5. 1 分钟 bar 聚合与封口
// 6. 1m 落库、mm 重建、L9 更新
func (p *mdSpi) processTick(in tickInputData, allowNowFallback bool, replay bool) error {
	instrumentID := strings.TrimSpace(in.InstrumentID)
	if instrumentID == "" {
		return nil
	}
	processStartedAt := time.Now()
	if replay {
		p.logFirstReplayPipeline(in)
	}

	exchangeID := strings.TrimSpace(in.ExchangeID)
	variety := normalizeVariety(instrumentID)
	if variety == "" {
		logger.Error("invalid instrument variety", "instrument_id", instrumentID)
		return nil
	}
	minuteTime, adjustedTime, adjustedTickTime, err := p.resolveTickTimes(variety, in)
	if err != nil {
		logger.Error("parse tick time failed", "instrument_id", instrumentID, "error", err)
		return err
	}

	now := in.ReceivedAt
	if now.IsZero() && allowNowFallback {
		now = time.Now()
	}
	if now.IsZero() {
		now = adjustedTickTime
	}
	if now.IsZero() {
		now = time.Now()
	}
	callbackAt := in.CallbackAt
	if callbackAt.IsZero() {
		callbackAt = now
	}
	if p.status != nil {
		p.status.MarkTick(now)
	}

	price := in.LastPrice
	if !isFinitePositivePrice(price) {
		logger.Warn(
			"drop tick with invalid price",
			"instrument_id", instrumentID,
			"exchange_id", exchangeID,
			"last_price", price,
			"trading_day", strings.TrimSpace(in.TradingDay),
			"action_day", strings.TrimSpace(in.ActionDay),
			"update_time", strings.TrimSpace(in.UpdateTime),
			"update_millisec", in.UpdateMillisec,
		)
		return nil
	}

	settlement := in.SettlementPrice
	if !isFinitePrice(settlement) {
		settlement = 0
	}

	currentVol := in.Volume
	openInterest := in.OpenInterest
	fingerprint := buildTickDedupFingerprint(in, price, currentVol, openInterest)
	var sideEffectEnqueuedAt time.Time
	if p.onTick != nil {
		sideEffectEnqueuedAt = time.Now()
		p.onTick(tickEvent{
			InstrumentID:         instrumentID,
			ExchangeID:           exchangeID,
			ActionDay:            strings.TrimSpace(in.ActionDay),
			TradingDay:           strings.TrimSpace(in.TradingDay),
			UpdateTime:           strings.TrimSpace(in.UpdateTime),
			UpdateMillisec:       in.UpdateMillisec,
			ReceivedAt:           now,
			CallbackAt:           callbackAt,
			ProcessStartedAt:     processStartedAt,
			SideEffectEnqueuedAt: sideEffectEnqueuedAt,
			LastPrice:            price,
			Volume:               currentVol,
			OpenInterest:         openInterest,
			SettlementPrice:      settlement,
			BidPrice1:            in.BidPrice1,
			AskPrice1:            in.AskPrice1,
		})
	}

	lockRequestedAt := time.Now()
	p.mu.Lock()
	lockAcquiredAt := time.Now()
	upstreamLagMS := now.Sub(adjustedTickTime).Seconds() * 1000
	callbackToProcMS := processStartedAt.Sub(callbackAt).Seconds() * 1000
	lockWaitMS := lockAcquiredAt.Sub(lockRequestedAt).Seconds() * 1000
	if p.status != nil {
		p.status.MarkTickPipelineLatency(instrumentID, upstreamLagMS, callbackToProcMS, lockWaitMS)
	}
	sideEffectEnqueueMS := 0.0
	if !sideEffectEnqueuedAt.IsZero() {
		sideEffectEnqueueMS = sideEffectEnqueuedAt.Sub(processStartedAt).Seconds() * 1000
	}
	p.maybeLogLatency("tick_pipeline:"+instrumentID, lockAcquiredAt, "tick pipeline latency",
		"instrument_id", instrumentID,
		"upstream_lag_ms", upstreamLagMS,
		"callback_to_process_ms", callbackToProcMS,
		"lock_wait_ms", lockWaitMS,
		"side_effect_enqueue_ms", sideEffectEnqueueMS,
	)
	if p.shouldDropDuplicateTick(instrumentID, fingerprint, now) {
		p.mu.Unlock()
		logger.Warn(
			"drop duplicate tick in dedup window",
			"instrument_id", instrumentID,
			"exchange_id", exchangeID,
			"tick_time", adjustedTickTime.Format("2006-01-02 15:04:05"),
			"received_at", now.Format("2006-01-02 15:04:05.000"),
			"window_ms", p.tickDedupWindow.Milliseconds(),
		)
		if p.status != nil {
			p.status.MarkTickDedupDropped()
		}
		return nil
	}

	if shouldCheckTickDrift(now, adjustedTickTime) {
		driftSec := math.Abs(now.Sub(adjustedTickTime).Seconds())
		if p.status != nil {
			p.status.MarkDrift(driftSec, p.driftPaused)
		}
		if time.Duration(driftSec*float64(time.Second)) > p.driftThreshold {
			if p.status != nil {
				p.status.MarkDrift(driftSec, false)
			}
			lastLoggedAt := p.lastDriftLog[instrumentID]
			if now.Sub(lastLoggedAt) >= 5*time.Second {
				p.lastDriftLog[instrumentID] = now
				logger.Error(
					"tick drift too large, continue writing",
					"instrument_id", instrumentID,
					"drift_seconds", driftSec,
					"threshold_seconds", p.driftThreshold.Seconds(),
				)
			}
		}
	}

	volumeDelta := int64(0)
	if last, ok := p.lastVols[instrumentID]; ok && currentVol >= last {
		volumeDelta = int64(currentVol - last)
	}
	p.lastVols[instrumentID] = currentVol
	lastTick := minuteTickSnapshot{
		TickTime:       adjustedTickTime,
		ReceivedAt:     now,
		UpdateTime:     strings.TrimSpace(in.UpdateTime),
		UpdateMillisec: in.UpdateMillisec,
		Price:          price,
		CurrentVolume:  currentVol,
		VolumeDelta:    volumeDelta,
		OpenInterest:   openInterest,
	}

	var endedBar *minuteBar
	var endedLastTick minuteTickSnapshot

	state := p.states[instrumentID]
	if state == nil {
		startedBar := minuteBar{
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
		p.states[instrumentID] = &instrumentMinuteState{bar: startedBar, lastTick: lastTick}
		p.logMinuteFirstTick(startedBar, adjustedTickTime, price, currentVol, volumeDelta)
	} else {
		if !state.bar.MinuteTime.Equal(minuteTime) {
			barCopy := state.bar
			endedBar = &barCopy
			endedLastTick = state.lastTick
			startedBar := minuteBar{
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
			if startedBar.Volume < 0 {
				logger.Warn("negative bucket volume detected, clamped", "instrument_id", instrumentID, "current_volume", currentVol, "prev_bucket_close_volume", state.lastTick.CurrentVolume)
				startedBar.Volume = 0
			}
			state.prevBucketCloseVol = state.lastTick.CurrentVolume
			state.hasPrevBucketVolume = true
			state.bar = startedBar
			state.lastTick = lastTick
			p.logMinuteFirstTick(startedBar, adjustedTickTime, price, currentVol, volumeDelta)
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
			state.bar.Volume = computeBucketVolume(currentVol, state.prevBucketCloseVol, state.hasPrevBucketVolume)
			if state.bar.Volume < 0 {
				logger.Warn("negative bucket volume detected, clamped", "instrument_id", instrumentID, "current_volume", currentVol, "prev_bucket_close_volume", state.prevBucketCloseVol)
				state.bar.Volume = 0
			}
			state.bar.OpenInterest = openInterest
			state.bar.SettlementPrice = settlement
			state.bar.AdjustedTime = adjustedTime
			state.bar.SourceReceivedAt = now
			state.lastTick = lastTick
		}
	}
	p.mu.Unlock()

	if endedBar != nil {
		p.logMinuteLastTickConfirmed(*endedBar, endedLastTick, "minute_rollover")
		if err := p.flushEndedBar(*endedBar, adjustedTickTime, "minute_rollover"); err != nil {
			logger.Error("flush minute bar failed", "instrument_id", instrumentID, "error", err)
		} else {
			p.logMinuteBarPersisted(*endedBar, endedLastTick, adjustedTickTime, "minute_rollover")
		}
		if p.onBar != nil {
			endedBar.SideEffectEnqueuedAt = time.Now()
			p.onBar(*endedBar)
		}
	}
	return nil
}

func (p *mdSpi) resolveTickTimes(variety string, in tickInputData) (time.Time, time.Time, time.Time, error) {
	sessions, err := p.loadSessions(variety)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	return parseTickTimesWithMillis(in.ActionDay, in.TradingDay, in.UpdateTime, in.UpdateMillisec, sessions, p.clock)
}

// shouldDropDuplicateTick 判断是否丢弃短窗口内的重复 tick。
// 判定标准不是只看时间，而是“同合约 + 同指纹 + 落在 dedup window 内”。
func (p *mdSpi) shouldDropDuplicateTick(instrumentID string, fingerprint string, now time.Time) bool {
	// 先写入最新指纹，再判断是否重复，这样即使本次 tick 被丢弃也能刷新窗口基准。
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

// shouldCheckTickDrift 控制是否执行时间漂移检测。
// 只对日期接近的时间做比较，避免跨日夜盘天然产生巨大偏差。
// buildTickDedupFingerprint 统一定义 tick 短窗口去重所依据的唯一性字段。
// 这里显式包含 UpdateMillisec，避免同一秒内只差毫秒的多笔正常 tick 被错误折叠。
func buildTickDedupFingerprint(in tickInputData, price float64, currentVol int, openInterest float64) string {
	return fmt.Sprintf(
		"%s|%s|%s|%03d|%.8f|%d|%.8f",
		strings.TrimSpace(in.TradingDay),
		strings.TrimSpace(in.ActionDay),
		strings.TrimSpace(in.UpdateTime),
		in.UpdateMillisec,
		price,
		currentVol,
		openInterest,
	)
}

func (p *mdSpi) logFirstReplayProcess(ev tickEvent) {
	instrumentID := strings.TrimSpace(ev.InstrumentID)
	if instrumentID == "" {
		return
	}
	p.mu.Lock()
	if _, ok := p.replayProcessSeen[instrumentID]; ok {
		p.mu.Unlock()
		return
	}
	p.replayProcessSeen[instrumentID] = struct{}{}
	p.mu.Unlock()
	logger.Info(
		"replay tick first entered mdSpi.ProcessReplayTick",
		"stage", "mdSpi.ProcessReplayTick",
		"instrument_id", instrumentID,
		"update_time", ev.UpdateTime,
		"update_millisec", ev.UpdateMillisec,
	)
}

func (p *mdSpi) logFirstReplayPipeline(in tickInputData) {
	instrumentID := strings.TrimSpace(in.InstrumentID)
	if instrumentID == "" {
		return
	}
	p.mu.Lock()
	if _, ok := p.replayPipelineSeen[instrumentID]; ok {
		p.mu.Unlock()
		return
	}
	p.replayPipelineSeen[instrumentID] = struct{}{}
	p.mu.Unlock()
	logger.Info(
		"replay tick first entered processTick",
		"stage", "mdSpi.processTick",
		"instrument_id", instrumentID,
		"update_time", strings.TrimSpace(in.UpdateTime),
		"update_millisec", in.UpdateMillisec,
	)
}

func (p *mdSpi) ResetReplayStageLogState() {
	p.mu.Lock()
	p.replayProcessSeen = make(map[string]struct{})
	p.replayPipelineSeen = make(map[string]struct{})
	p.mu.Unlock()
}

func shouldCheckTickDrift(now time.Time, adjustedTickTime time.Time) bool {
	// 仅在日期接近时比较“接收时间”和“业务时间”，
	// 否则跨日夜盘会天然产生大偏差，日志价值不高。
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	tickDay := time.Date(adjustedTickTime.Year(), adjustedTickTime.Month(), adjustedTickTime.Day(), 0, 0, 0, 0, time.Local)
	deltaDays := nowDay.Sub(tickDay).Hours() / 24
	if deltaDays < 0 {
		deltaDays = -deltaDays
	}
	return deltaDays <= 1
}

// Flush 强制把各合约当前未封口的最后一根 1m bar 落库。
// 该函数通常在回放结束、服务退出或人工触发时调用。
func (p *mdSpi) Flush() error {
	p.mu.Lock()
	bars := make([]minuteBar, 0, len(p.states))
	lastTicks := make([]minuteTickSnapshot, 0, len(p.states))
	for _, state := range p.states {
		if state == nil {
			continue
		}
		bars = append(bars, state.bar)
		lastTicks = append(lastTicks, state.lastTick)
	}
	p.mu.Unlock()

	for i, endedBar := range bars {
		p.logMinuteLastTickConfirmed(endedBar, lastTicks[i], "flush")
		if err := p.flushEndedBar(endedBar, time.Time{}, "flush"); err != nil {
			return fmt.Errorf("flush minute bar for %s failed: %w", endedBar.InstrumentID, err)
		}
		p.logMinuteBarPersisted(endedBar, lastTicks[i], time.Time{}, "flush")
		if p.onBar != nil {
			endedBar.SideEffectEnqueuedAt = time.Now()
			p.onBar(endedBar)
		}
	}
	if err := p.waitForMMRebuilds(); err != nil {
		return err
	}
	return nil
}

func (p *mdSpi) flushEndedBar(endedBar minuteBar, nextTickTime time.Time, reason string) error {
	storeStartedAt := time.Now()
	endedBar.FlushStartedAt = storeStartedAt
	if err := p.store.UpsertMinuteBar(endedBar); err != nil {
		return err
	}
	storeMS := time.Since(storeStartedAt).Seconds() * 1000
	if p.status != nil {
		p.status.MarkMinuteStoreLatency(endedBar.InstrumentID, storeMS)
	}
	p.maybeLogLatency("minute_store:"+endedBar.InstrumentID, time.Now(), "minute bar store latency",
		"instrument_id", endedBar.InstrumentID,
		"store_ms", storeMS,
		"reason", reason,
	)
	p.scheduleMMRebuild(endedBar)
	if p.l9Async != nil {
		p.l9Async.ObserveMinuteBar(endedBar)
		p.l9Async.Submit(endedBar.Variety, endedBar.MinuteTime)
	}
	_ = nextTickTime
	_ = reason
	return nil
}

func (p *mdSpi) scheduleMMRebuild(bar minuteBar) {
	if p.store == nil {
		return
	}
	enqueuedAt := time.Now()
	p.mmRebuildWG.Add(1)
	go func() {
		defer p.mmRebuildWG.Done()
		p.mmRebuildSem <- struct{}{}
		defer func() { <-p.mmRebuildSem }()
		startedAt := time.Now()
		queueMS := startedAt.Sub(enqueuedAt).Seconds() * 1000

		if _, _, err := mmkline.RebuildAndUpsert(p.store.DB(), mmkline.RebuildRequest{
			Variety:      bar.Variety,
			InstrumentID: bar.InstrumentID,
			IsL9:         false,
		}); err != nil {
			wrapped := fmt.Errorf("rebuild mm bars for %s failed: %w", bar.InstrumentID, err)
			logger.Error("mm rebuild failed", "instrument_id", bar.InstrumentID, "error", err)
			p.mmRebuildErr.CompareAndSwap(nil, &wrapped)
		}
		runMS := time.Since(startedAt).Seconds() * 1000
		if p.status != nil {
			p.status.MarkMMRebuildLatency(bar.InstrumentID, queueMS, runMS)
		}
		p.maybeLogLatency("mm_rebuild:"+bar.InstrumentID, time.Now(), "mm rebuild latency",
			"instrument_id", bar.InstrumentID,
			"queue_ms", queueMS,
			"run_ms", runMS,
		)
	}()
}

func (p *mdSpi) waitForMMRebuilds() error {
	p.mmRebuildWG.Wait()
	if errPtr := p.mmRebuildErr.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

func (p *mdSpi) maybeLogLatency(key string, now time.Time, msg string, args ...any) {
	if !shouldLogLatency(args...) {
		return
	}
	lastLoggedAt := p.lastLatencyLog[key]
	if now.Sub(lastLoggedAt) < latencyLogInterval {
		return
	}
	p.lastLatencyLog[key] = now
	logger.Warn(msg, args...)
}

func shouldLogLatency(args ...any) bool {
	for i := 0; i+1 < len(args); i += 2 {
		switch v := args[i+1].(type) {
		case float64:
			if v >= latencyLogThreshold.Seconds()*1000 {
				return true
			}
		case int64:
			if time.Duration(v)*time.Millisecond >= latencyLogThreshold {
				return true
			}
		case int:
			if time.Duration(v)*time.Millisecond >= latencyLogThreshold {
				return true
			}
		}
	}
	return false
}

func (p *mdSpi) logMinuteFirstTick(bar minuteBar, tickTime time.Time, price float64, currentVol int, volumeDelta int64) {
	logger.Info(
		"minute first tick received",
		"marker", "first_tick",
		"instrument_id", bar.InstrumentID,
		"exchange_id", bar.Exchange,
		"minute_time", bar.MinuteTime.Format("2006-01-02 15:04:00"),
		"adjusted_time", bar.AdjustedTime.Format("2006-01-02 15:04:00"),
		"tick_time", tickTime.Format("2006-01-02 15:04:05"),
		"price", price,
		"volume", currentVol,
		"volume_delta", volumeDelta,
		"open_interest", bar.OpenInterest,
		"settlement_price", bar.SettlementPrice,
	)
}

func (p *mdSpi) logMinuteLastTickConfirmed(bar minuteBar, tick minuteTickSnapshot, reason string) {
	logger.Info(
		"minute last tick confirmed",
		"marker", "last_tick_confirmed",
		"reason", reason,
		"instrument_id", bar.InstrumentID,
		"exchange_id", bar.Exchange,
		"minute_time", bar.MinuteTime.Format("2006-01-02 15:04:00"),
		"adjusted_time", bar.AdjustedTime.Format("2006-01-02 15:04:00"),
		"tick_time", tick.TickTime.Format("2006-01-02 15:04:05.000"),
		"received_at", tick.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		"update_time", tick.UpdateTime,
		"update_millisec", tick.UpdateMillisec,
		"price", tick.Price,
		"current_volume", tick.CurrentVolume,
		"volume_delta", tick.VolumeDelta,
		"open_interest", tick.OpenInterest,
	)
}

func (p *mdSpi) logMinuteBarPersisted(bar minuteBar, tick minuteTickSnapshot, nextTickTime time.Time, reason string) {
	logger.Info(
		"minute bar persisted",
		"marker", "last_tick_persisted",
		"reason", reason,
		"instrument_id", bar.InstrumentID,
		"exchange_id", bar.Exchange,
		"minute_time", bar.MinuteTime.Format("2006-01-02 15:04:00"),
		"adjusted_time", bar.AdjustedTime.Format("2006-01-02 15:04:00"),
		"tick_time", tick.TickTime.Format("2006-01-02 15:04:05.000"),
		"update_time", tick.UpdateTime,
		"update_millisec", tick.UpdateMillisec,
		"price", tick.Price,
		"current_volume", tick.CurrentVolume,
		"volume_delta", tick.VolumeDelta,
		"open_interest", tick.OpenInterest,
		"open", bar.Open,
		"high", bar.High,
		"low", bar.Low,
		"close", bar.Close,
		"volume", bar.Volume,
		"settlement_price", bar.SettlementPrice,
		"next_tick_time", nextTickTime.Format("2006-01-02 15:04:05.000"),
	)
}

// parseTickTimes 根据 ActionDay、TradingDay 和 UpdateTime 生成分钟归属时间。
//
// 它的核心工作是：
// 1. 先确定交易日
// 2. 再结合时分秒得到业务时刻
// 3. 最后借助交易日历修正分钟归属，处理夜盘跨日问题
func parseTickTimes(actionDay string, tradingDay string, updateTime string, updateMillisec int, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) (time.Time, time.Time, time.Time, error) {
	// 时间解析规则：
	// 1. 优先使用 TradingDay，缺失时回退到 ActionDay
	// 2. 结合 UpdateTime 得到交易所报出的时分秒
	// 3. 再由交易日历修正出 bar 归属分钟，处理夜盘跨日问题
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
	_, adjusted, err := klineclock.BuildBarTimes(day, hhmm, clock)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	labelMinute, ok := resolveLabelMinute(hhmm, sessions)
	if !ok {
		labelMinute = hhmm
	}
	baseMinute := labelMinuteOnDay(day, labelMinute)
	adjustedMinute := labelMinuteOnDay(adjusted, labelMinute)
	// adjustedTick 保留秒级精度，供漂移检测和回放排序使用。
	adjustedTick := time.Date(adjusted.Year(), adjusted.Month(), adjusted.Day(), clockTime.Hour(), clockTime.Minute(), clockTime.Second(), 0, time.Local)
	return baseMinute, adjustedMinute, adjustedTick, nil
}

func parseBaseMinute(actionDay string, tradingDay string, updateTime string, sessions []sessiontime.Range) (time.Time, error) {
	dayText := strings.TrimSpace(tradingDay)
	if len(dayText) != 8 {
		dayText = strings.TrimSpace(actionDay)
	}
	if len(dayText) != 8 {
		return time.Time{}, fmt.Errorf("invalid trading_day/action_day")
	}
	day, err := klineclock.ParseTradingDay(dayText)
	if err != nil {
		return time.Time{}, err
	}
	updateTime = strings.TrimSpace(updateTime)
	if updateTime == "" {
		return time.Time{}, fmt.Errorf("empty update_time")
	}
	clockTime, err := time.ParseInLocation("15:04:05", updateTime, time.Local)
	if err != nil {
		return time.Time{}, err
	}
	labelMinute, ok := resolveLabelMinute(klineclock.HHMMFromTime(clockTime), sessions)
	if !ok {
		labelMinute = klineclock.HHMMFromTime(clockTime)
	}
	return labelMinuteOnDay(day, labelMinute), nil
}

func parseTickTimesWithMillis(actionDay string, tradingDay string, updateTime string, updateMillisec int, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) (time.Time, time.Time, time.Time, error) {
	baseMinute, adjustedMinute, adjustedTick, err := parseTickTimes(actionDay, tradingDay, updateTime, updateMillisec, sessions, clock)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	if adjustedTick.IsZero() {
		return baseMinute, adjustedMinute, adjustedTick, nil
	}
	if updateMillisec < 0 {
		updateMillisec = 0
	}
	if updateMillisec > 999 {
		updateMillisec = updateMillisec % 1000
	}
	adjustedTick = time.Date(
		adjustedTick.Year(),
		adjustedTick.Month(),
		adjustedTick.Day(),
		adjustedTick.Hour(),
		adjustedTick.Minute(),
		adjustedTick.Second(),
		updateMillisec*int(time.Millisecond),
		adjustedTick.Location(),
	)
	return baseMinute, adjustedMinute, adjustedTick, nil
}

func resolveLabelMinute(hhmm int, sessions []sessiontime.Range) (int, bool) {
	hour := hhmm / 100
	minute := hhmm % 100
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, false
	}
	if len(sessions) == 0 {
		total := hour*60 + minute
		if total < 23*60+59 {
			return total + 1, true
		}
		return total, true
	}
	return sessiontime.LabelMinute(hour*60+minute, sessions)
}

func labelMinuteOnDay(day time.Time, minuteOfDay int) time.Time {
	hour := (minuteOfDay / 60) % 24
	minute := minuteOfDay % 60
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.Local)
}

func computeBucketVolume(currentVol int, prevBucketCloseVol int, hasPrevBucket bool) int64 {
	if !hasPrevBucket {
		return 0
	}
	return int64(currentVol - prevBucketCloseVol)
}

func (p *mdSpi) loadSessions(variety string) ([]sessiontime.Range, error) {
	if p == nil || p.sessionResolver == nil {
		return nil, nil
	}
	return p.sessionResolver.Sessions(variety)
}

func isFinitePositivePrice(price float64) bool {
	return isFinitePrice(price) && price > 0
}

func isFinitePrice(price float64) bool {
	// 极端大值通常来自柜台脏数据或未初始化字段，也一并过滤。
	return !math.IsNaN(price) && !math.IsInf(price, 0) && math.Abs(price) < 1e20
}
