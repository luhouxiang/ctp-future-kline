package quotes

import (
	"sync"
	"sync/atomic"

	"ctp-future-kline/internal/logger"

	ctp "github.com/kkqy/ctp-go"
)

type querySpi struct {
	ctp.TraderSpi

	lastReqID     atomic.Int64
	queryFinished chan struct{}
	queryDoneOnce sync.Once

	instruments   []instrumentInfo
	snapshots     []instrumentSnapshot
	instrumentsMu sync.Mutex

	tradingDay   string
	tradingDayMu sync.Mutex
	status       *RuntimeStatusCenter
}

func newQuerySpi() *querySpi {
	return &querySpi{queryFinished: make(chan struct{})}
}

func newQuerySpiWithStatus(status *RuntimeStatusCenter) *querySpi {
	return &querySpi{
		queryFinished: make(chan struct{}),
		status:        status,
	}
}

func (p *querySpi) nextReqID() int {
	return int(p.lastReqID.Add(1))
}

func (p *querySpi) OnFrontConnected() {
	logger.Info("query front connected")
	if p.status != nil {
		p.status.MarkQueryFrontConnected()
	}
}

func (p *querySpi) OnRspAuthenticate(
	_ ctp.CThostFtdcRspAuthenticateField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "query",
		"callback", "OnRspAuthenticate",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", pRspInfo.GetErrorID(),
	)
	if pRspInfo.GetErrorID() == 0 {
		logger.Info("authenticate success")
		return
	}
	logger.Error("authenticate failed", "error_id", pRspInfo.GetErrorID())
}

func (p *querySpi) OnRspUserLogin(
	loginField ctp.CThostFtdcRspUserLoginField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "query",
		"callback", "OnRspUserLogin",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", pRspInfo.GetErrorID(),
		"login_time", loginField.GetLoginTime(),
		"trading_day", loginField.GetTradingDay(),
	)
	if pRspInfo.GetErrorID() == 0 {
		logger.Info("user login success")
		p.setTradingDay(loginField.GetTradingDay())
		if p.status != nil {
			p.status.MarkQueryLogin(loginField.GetLoginTime(), loginField.GetTradingDay())
		}
		return
	}
	logger.Error("user login failed", "error_id", pRspInfo.GetErrorID())
}

func (p *querySpi) OnRspQryInstrument(
	pInstrument ctp.CThostFtdcInstrumentField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	finishQuery := func() {
		if bIsLast {
			logger.Info("instrument query finished")
			p.queryDoneOnce.Do(func() { close(p.queryFinished) })
		}
	}
	defer func() {
		if r := recover(); r != nil {
			logger.Error("OnRspQryInstrument panic recovered", "req_id", nRequestID, "is_last", bIsLast, "panic", r)
			finishQuery()
		}
	}()
	if bIsLast {
		logger.Info("instrument query finished")
		finishQuery()
	}

	errorID := safeRspErrorID(pRspInfo)
	instrument := safeInstrumentSnapshot(pInstrument)
	if string([]byte{instrument.ProductClass}) != "1" {
		return
	}
	logger.Info(
		"ctp response",
		"api", "query",
		"callback", "OnRspQryInstrument",
		"req_id", nRequestID,
		"is_last", bIsLast,
		"error_id", errorID,
		"instrument_id", instrument.ID,
		"exchange_id", instrument.ExchangeID,
		"product_id", instrument.ProductID,
		"product_class", string([]byte{instrument.ProductClass}),
		"price_tick", instrument.PriceTick,
	)
	if errorID != 0 {
		logger.Error("query instrument failed", "error_id", errorID)
		finishQuery()
		return
	}

	if instrument.ID != "" {
		p.instrumentsMu.Lock()
		p.instruments = append(p.instruments, instrumentInfo{
			ID:           instrument.ID,
			ExchangeID:   instrument.ExchangeID,
			ProductID:    instrument.ProductID,
			ProductClass: instrument.ProductClass,
		})
		p.snapshots = append(p.snapshots, instrument)
		p.instrumentsMu.Unlock()
		logger.Info(
			"instrument received",
			"instrument_id", instrument.ID,
			"exchange_id", instrument.ExchangeID,
			"product_id", instrument.ProductID,
			"product_class", string([]byte{instrument.ProductClass}),
			"price_tick", instrument.PriceTick,
		)
	}

	finishQuery()
}

func (p *querySpi) instrumentInfos() []instrumentInfo {
	p.instrumentsMu.Lock()
	defer p.instrumentsMu.Unlock()

	out := make([]instrumentInfo, len(p.instruments))
	copy(out, p.instruments)
	return out
}

func (p *querySpi) instrumentSnapshots() []instrumentSnapshot {
	p.instrumentsMu.Lock()
	defer p.instrumentsMu.Unlock()

	out := make([]instrumentSnapshot, len(p.snapshots))
	copy(out, p.snapshots)
	return out
}

func (p *querySpi) setTradingDay(day string) {
	p.tradingDayMu.Lock()
	defer p.tradingDayMu.Unlock()
	p.tradingDay = day
}

func (p *querySpi) getTradingDay() string {
	p.tradingDayMu.Lock()
	defer p.tradingDayMu.Unlock()
	return p.tradingDay
}

type instrumentSnapshot struct {
	ID                     string
	ExchangeID             string
	ExchangeInstID         string
	InstrumentName         string
	ProductID              string
	ProductClass           byte
	DeliveryYear           int
	DeliveryMonth          int
	MaxMarketOrderVolume   int
	MinMarketOrderVolume   int
	MaxLimitOrderVolume    int
	MinLimitOrderVolume    int
	VolumeMultiple         int
	PriceTick              float64
	CreateDate             string
	OpenDate               string
	ExpireDate             string
	StartDelivDate         string
	EndDelivDate           string
	InstLifePhase          byte
	IsTrading              int
	PositionType           byte
	PositionDateType       byte
	LongMarginRatio        float64
	ShortMarginRatio       float64
	MaxMarginSideAlgorithm byte
	UnderlyingInstrID      string
	StrikePrice            float64
	OptionsType            byte
	UnderlyingMultiple     float64
	CombinationType        byte
}

func safeRspErrorID(pRspInfo ctp.CThostFtdcRspInfoField) (errorID int) {
	if pRspInfo == nil {
		return 0
	}
	if pRspInfo.Swigcptr() == 0 {
		return 0
	}
	defer func() {
		if recover() != nil {
			errorID = -1
		}
	}()
	return pRspInfo.GetErrorID()
}

func safeInstrumentSnapshot(pInstrument ctp.CThostFtdcInstrumentField) (out instrumentSnapshot) {
	if pInstrument == nil {
		return instrumentSnapshot{}
	}
	if pInstrument.Swigcptr() == 0 {
		return instrumentSnapshot{}
	}
	defer func() {
		if recover() != nil {
			out = instrumentSnapshot{}
		}
	}()
	out.ID = pInstrument.GetInstrumentID()
	out.ExchangeID = pInstrument.GetExchangeID()
	out.ExchangeInstID = pInstrument.GetExchangeInstID()
	out.InstrumentName = pInstrument.GetInstrumentName()
	out.ProductID = pInstrument.GetProductID()
	out.ProductClass = pInstrument.GetProductClass()
	out.DeliveryYear = pInstrument.GetDeliveryYear()
	out.DeliveryMonth = pInstrument.GetDeliveryMonth()
	out.MaxMarketOrderVolume = pInstrument.GetMaxMarketOrderVolume()
	out.MinMarketOrderVolume = pInstrument.GetMinMarketOrderVolume()
	out.MaxLimitOrderVolume = pInstrument.GetMaxLimitOrderVolume()
	out.MinLimitOrderVolume = pInstrument.GetMinLimitOrderVolume()
	out.VolumeMultiple = pInstrument.GetVolumeMultiple()
	out.PriceTick = pInstrument.GetPriceTick()
	out.CreateDate = pInstrument.GetCreateDate()
	out.OpenDate = pInstrument.GetOpenDate()
	out.ExpireDate = pInstrument.GetExpireDate()
	out.StartDelivDate = pInstrument.GetStartDelivDate()
	out.EndDelivDate = pInstrument.GetEndDelivDate()
	out.InstLifePhase = pInstrument.GetInstLifePhase()
	out.IsTrading = pInstrument.GetIsTrading()
	out.PositionType = pInstrument.GetPositionType()
	out.PositionDateType = pInstrument.GetPositionDateType()
	out.LongMarginRatio = pInstrument.GetLongMarginRatio()
	out.ShortMarginRatio = pInstrument.GetShortMarginRatio()
	out.MaxMarginSideAlgorithm = pInstrument.GetMaxMarginSideAlgorithm()
	out.UnderlyingInstrID = pInstrument.GetUnderlyingInstrID()
	out.StrikePrice = pInstrument.GetStrikePrice()
	out.OptionsType = pInstrument.GetOptionsType()
	out.UnderlyingMultiple = pInstrument.GetUnderlyingMultiple()
	out.CombinationType = pInstrument.GetCombinationType()
	return out
}
