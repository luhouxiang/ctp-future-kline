package quotes

import (
	"strings"
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

	commissionRates   map[string]commissionRateSnapshot
	commissionWaiters map[int]chan commissionQueryResult
	commissionReqRows map[int]int
	commissionMu      sync.Mutex

	tradingDay   string
	tradingDayMu sync.Mutex
	status       *RuntimeStatusCenter
}

func newQuerySpi() *querySpi {
	return &querySpi{
		queryFinished:     make(chan struct{}),
		commissionRates:   make(map[string]commissionRateSnapshot),
		commissionWaiters: make(map[int]chan commissionQueryResult),
		commissionReqRows: make(map[int]int),
	}
}

func newQuerySpiWithStatus(status *RuntimeStatusCenter) *querySpi {
	return &querySpi{
		queryFinished:     make(chan struct{}),
		status:            status,
		commissionRates:   make(map[string]commissionRateSnapshot),
		commissionWaiters: make(map[int]chan commissionQueryResult),
		commissionReqRows: make(map[int]int),
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

func (p *querySpi) OnRspQryInstrumentCommissionRate(
	pInstrumentCommissionRate ctp.CThostFtdcInstrumentCommissionRateField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	errID := safeRspErrorID(pRspInfo)
	errMsg := safeRspErrorMsg(pRspInfo)
	if errID == 0 {
		if item, ok := safeCommissionRateSnapshot(pInstrumentCommissionRate); ok {
			key := strings.ToLower(strings.TrimSpace(item.InstrumentID)) + "|" + strings.TrimSpace(item.ExchangeID)
			if key != "|" {
				p.commissionMu.Lock()
				p.commissionRates[key] = item
				p.commissionReqRows[nRequestID]++
				p.commissionMu.Unlock()
			}
		}
	}
	if !bIsLast {
		return
	}
	p.commissionMu.Lock()
	ch, ok := p.commissionWaiters[nRequestID]
	rows := p.commissionReqRows[nRequestID]
	if ok {
		delete(p.commissionWaiters, nRequestID)
	}
	delete(p.commissionReqRows, nRequestID)
	p.commissionMu.Unlock()
	if !ok {
		return
	}
	ch <- commissionQueryResult{
		ErrID:   errID,
		ErrMsg:  errMsg,
		ReqID:   nRequestID,
		Rows:    rows,
		IsLast:  bIsLast,
		Success: errID == 0,
	}
	close(ch)
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

func (p *querySpi) registerCommissionWaiter(reqID int) <-chan commissionQueryResult {
	ch := make(chan commissionQueryResult, 1)
	p.commissionMu.Lock()
	p.commissionWaiters[reqID] = ch
	p.commissionReqRows[reqID] = 0
	p.commissionMu.Unlock()
	return ch
}

func (p *querySpi) commissionRateSnapshots() []commissionRateSnapshot {
	p.commissionMu.Lock()
	defer p.commissionMu.Unlock()
	out := make([]commissionRateSnapshot, 0, len(p.commissionRates))
	for _, item := range p.commissionRates {
		out = append(out, item)
	}
	return out
}

func (p *querySpi) resetCommissionRateSnapshots() {
	p.commissionMu.Lock()
	p.commissionRates = make(map[string]commissionRateSnapshot)
	p.commissionMu.Unlock()
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

type commissionRateSnapshot struct {
	InstrumentID            string
	ExchangeID              string
	OpenRatioByMoney        float64
	OpenRatioByVolume       float64
	CloseRatioByMoney       float64
	CloseRatioByVolume      float64
	CloseTodayRatioByMoney  float64
	CloseTodayRatioByVolume float64
}

type commissionQueryResult struct {
	ErrID   int
	ErrMsg  string
	ReqID   int
	Rows    int
	IsLast  bool
	Success bool
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

func safeRspErrorMsg(pRspInfo ctp.CThostFtdcRspInfoField) (msg string) {
	if pRspInfo == nil {
		return ""
	}
	if pRspInfo.Swigcptr() == 0 {
		return ""
	}
	defer func() {
		if recover() != nil {
			msg = ""
		}
	}()
	return strings.TrimSpace(pRspInfo.GetErrorMsg())
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

func safeCommissionRateSnapshot(p ctp.CThostFtdcInstrumentCommissionRateField) (out commissionRateSnapshot, ok bool) {
	if p == nil || p.Swigcptr() == 0 {
		return commissionRateSnapshot{}, false
	}
	defer func() {
		if recover() != nil {
			out = commissionRateSnapshot{}
			ok = false
		}
	}()
	out.InstrumentID = strings.TrimSpace(p.GetInstrumentID())
	out.ExchangeID = strings.TrimSpace(p.GetExchangeID())
	out.OpenRatioByMoney = p.GetOpenRatioByMoney()
	out.OpenRatioByVolume = p.GetOpenRatioByVolume()
	out.CloseRatioByMoney = p.GetCloseRatioByMoney()
	out.CloseRatioByVolume = p.GetCloseRatioByVolume()
	out.CloseTodayRatioByMoney = p.GetCloseTodayRatioByMoney()
	out.CloseTodayRatioByVolume = p.GetCloseTodayRatioByVolume()
	if out.InstrumentID == "" {
		return commissionRateSnapshot{}, false
	}
	return out, true
}
