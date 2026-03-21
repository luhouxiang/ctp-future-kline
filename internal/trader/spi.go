// spi.go 实现 Trader API 的回调适配层。
// 它主要处理认证、登录和合约查询回调，并把交易日、连接状态、合约列表等信息反馈给上层服务。
package trader

import (
	"sync"
	"sync/atomic"

	"ctp-go-demo/internal/logger"

	ctp "github.com/kkqy/ctp-go"
)

type traderSpi struct {
	ctp.TraderSpi

	// lastReqID 生成递增请求号，供认证、登录和查合约请求复用。
	lastReqID atomic.Int64
	// queryFinished 在合约查询回调收到最后一条时关闭，用于通知查询阶段完成。
	queryFinished chan struct{}
	// queryDoneOnce 保证 queryFinished 只关闭一次。
	queryDoneOnce sync.Once
	// instruments 累积 Trader 查询回调返回的合约信息。
	instruments []instrumentInfo
	// instrumentsMu 保护 instruments 切片的并发访问。
	instrumentsMu sync.Mutex
	// tradingDay 保存登录回调返回的当前交易日。
	tradingDay string
	// tradingDayMu 保护 tradingDay 字段。
	tradingDayMu sync.Mutex
	// status 用于把前置连接、登录和交易日同步到全局状态中心。
	status *RuntimeStatusCenter
}

func newTraderSpi() *traderSpi {
	return &traderSpi{queryFinished: make(chan struct{})}
}

func newTraderSpiWithStatus(status *RuntimeStatusCenter) *traderSpi {
	return &traderSpi{
		queryFinished: make(chan struct{}),
		status:        status,
	}
}

func (p *traderSpi) nextReqID() int {
	return int(p.lastReqID.Add(1))
}

func (p *traderSpi) OnFrontConnected() {
	logger.Info("trader front connected")
	if p.status != nil {
		p.status.MarkTraderFrontConnected()
	}
}

func (p *traderSpi) OnRspAuthenticate(
	_ ctp.CThostFtdcRspAuthenticateField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "trader",
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

func (p *traderSpi) OnRspUserLogin(
	loginField ctp.CThostFtdcRspUserLoginField,
	pRspInfo ctp.CThostFtdcRspInfoField,
	nRequestID int,
	bIsLast bool,
) {
	logger.Info(
		"ctp response",
		"api", "trader",
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
			p.status.MarkTraderLogin(loginField.GetLoginTime(), loginField.GetTradingDay())
		}
		return
	}
	logger.Error("user login failed", "error_id", pRspInfo.GetErrorID())
}

func (p *traderSpi) OnRspQryInstrument(
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
		return // 不是期货的，直接略过
	}
	logger.Info(
		"ctp response",
		"api", "trader",
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

	instrumentID := instrument.ID
	if instrumentID != "" {
		p.instrumentsMu.Lock()
		p.instruments = append(p.instruments, instrumentInfo{
			ID:           instrumentID,
			ExchangeID:   instrument.ExchangeID,
			ProductID:    instrument.ProductID,
			ProductClass: instrument.ProductClass,
		})
		p.instrumentsMu.Unlock()
		logger.Info(
			"instrument received",
			"instrument_id", instrumentID,
			"exchange_id", instrument.ExchangeID,
			"product_id", instrument.ProductID,
			"product_class", string([]byte{instrument.ProductClass}),
			"price_tick", instrument.PriceTick,
		)
	}

	finishQuery()
}

func (p *traderSpi) instrumentInfos() []instrumentInfo {
	p.instrumentsMu.Lock()
	defer p.instrumentsMu.Unlock()

	out := make([]instrumentInfo, len(p.instruments))
	copy(out, p.instruments)
	return out
}

func (p *traderSpi) setTradingDay(day string) {
	p.tradingDayMu.Lock()
	defer p.tradingDayMu.Unlock()
	p.tradingDay = day
}

func (p *traderSpi) getTradingDay() string {
	p.tradingDayMu.Lock()
	defer p.tradingDayMu.Unlock()
	return p.tradingDay
}

type instrumentSnapshot struct {
	// ID 是回调里读取到的合约代码。
	ID string
	// ExchangeID 是交易所代码。
	ExchangeID string
	// ProductID 是品种代码。
	ProductID string
	// ProductClass 是 CTP 产品类别。
	ProductClass byte
	// PriceTick 是该合约最小变动价位。
	PriceTick float64
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
	out.ProductID = pInstrument.GetProductID()
	out.ProductClass = pInstrument.GetProductClass()
	out.PriceTick = pInstrument.GetPriceTick()
	return out
}
