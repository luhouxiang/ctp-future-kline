// ctp_gateway.go 封装交易侧 CTP API 的具体交互。
// 它负责前置连接、登录、结算确认、账户/持仓/委托/成交查询，以及下单和撤单请求的回调编排。
package trade

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ctp-go-demo/internal/config"
	"ctp-go-demo/internal/logger"

	ctp "github.com/kkqy/ctp-go"
)

type CTPGateway struct {
	cfg      config.CTPConfig
	tradeCfg config.TradeConfig

	api    ctp.CThostFtdcTraderApi
	spi    *ctpTradeSpi
	status atomic.Pointer[TradeStatus]

	mu      sync.Mutex
	started bool
}

func NewCTPGateway(cfg config.CTPConfig, tradeCfg config.TradeConfig) *CTPGateway {
	g := &CTPGateway{cfg: cfg, tradeCfg: tradeCfg}
	st := TradeStatus{Enabled: tradeCfg.IsEnabled(), AccountID: tradeCfg.AccountID, UpdatedAt: time.Now()}
	g.status.Store(&st)
	return g
}

func (g *CTPGateway) Start() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		return nil
	}
	if err := os.MkdirAll(filepath.Clean(g.cfg.FlowPath), 0o755); err != nil {
		return fmt.Errorf("create trade flow dir failed: %w", err)
	}

	spi := newCTPTradeSpi(g.tradeCfg.AccountID)
	api := ctp.CThostFtdcTraderApiCreateFtdcTraderApi(filepath.Clean(g.cfg.FlowPath))
	api.RegisterSpi(ctp.NewDirectorCThostFtdcTraderSpi(spi))
	api.RegisterFront(g.cfg.TraderFrontAddr)
	api.SubscribePrivateTopic(ctp.THOST_TERT_RESTART)
	api.SubscribePublicTopic(ctp.THOST_TERT_RESTART)
	api.Init()

	select {
	case <-spi.frontConnected:
	case <-time.After(time.Duration(g.cfg.ConnectWaitSeconds) * time.Second):
		return errors.New("trade gateway wait front connected timeout")
	}
	if err := g.authenticate(api, spi.nextReqID()); err != nil {
		return err
	}
	select {
	case err := <-spi.authResp:
		if err != nil {
			return err
		}
	case <-time.After(time.Duration(g.cfg.AuthenticateWaitSeconds) * time.Second):
		return errors.New("trade gateway wait authenticate timeout")
	}
	if err := g.login(api, spi.nextReqID()); err != nil {
		return err
	}
	var login loginResult
	select {
	case login = <-spi.loginResp:
		if login.Err != nil {
			return login.Err
		}
	case <-time.After(time.Duration(g.cfg.LoginWaitSeconds) * time.Second):
		return errors.New("trade gateway wait login timeout")
	}
	if g.tradeCfg.IsAutoConfirmSettlement() {
		if err := g.confirmSettlement(api, spi.nextReqID()); err != nil {
			return err
		}
		select {
		case err := <-spi.settlementResp:
			if err != nil {
				return err
			}
		case <-time.After(time.Duration(g.cfg.LoginWaitSeconds) * time.Second):
			return errors.New("trade gateway wait settlement confirm timeout")
		}
	}

	g.api = api
	g.spi = spi
	g.started = true
	g.setStatus(func(st *TradeStatus) {
		st.TraderFront = true
		st.TraderLogin = true
		st.SettlementConfirmed = g.tradeCfg.IsAutoConfirmSettlement()
		st.TradingDay = login.TradingDay
		st.FrontID = login.FrontID
		st.SessionID = login.SessionID
		st.LastError = ""
	})
	return nil
}

func (g *CTPGateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.api != nil {
		g.api.Release()
		g.api = nil
	}
	g.started = false
	return nil
}

func (g *CTPGateway) Status() TradeStatus {
	if g.spi != nil {
		g.setStatus(func(st *TradeStatus) {
			st.TraderFront = g.spi.isConnected()
			st.TraderLogin = g.spi.isLoggedIn()
			st.SettlementConfirmed = g.spi.isSettlementConfirmed()
			st.TradingDay = g.spi.tradingDay()
			st.FrontID = g.spi.frontID()
			st.SessionID = g.spi.sessionID()
			st.LastError = g.spi.lastError()
		})
	}
	cur := g.status.Load()
	if cur == nil {
		return TradeStatus{Enabled: g.tradeCfg.IsEnabled(), AccountID: g.tradeCfg.AccountID, UpdatedAt: time.Now()}
	}
	return *cur
}

func (g *CTPGateway) Subscribe() (<-chan GatewayEvent, func()) {
	if g.spi == nil {
		ch := make(chan GatewayEvent)
		close(ch)
		return ch, func() {}
	}
	ch := g.spi.Subscribe()
	return ch, func() {}
}

func (g *CTPGateway) RefreshAccount() (TradingAccountSnapshot, error) {
	field := ctp.NewCThostFtdcQryTradingAccountField()
	defer ctp.DeleteCThostFtdcQryTradingAccountField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	resCh := g.spi.beginQuery(g.spi.nextReqID())
	if ret := g.api.ReqQryTradingAccount(field, resCh.reqID); ret != 0 {
		return TradingAccountSnapshot{}, fmt.Errorf("ReqQryTradingAccount failed: %d", ret)
	}
	res, err := g.waitQueryResult(resCh, "account")
	if err != nil {
		return TradingAccountSnapshot{}, err
	}
	if res.err != nil {
		return TradingAccountSnapshot{}, res.err
	}
	if len(res.accounts) == 0 {
		return TradingAccountSnapshot{}, errors.New("empty trading account response")
	}
	g.touchQuery()
	return res.accounts[0], nil
}

func (g *CTPGateway) RefreshPositions() ([]PositionSnapshot, error) {
	field := ctp.NewCThostFtdcQryInvestorPositionField()
	defer ctp.DeleteCThostFtdcQryInvestorPositionField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	resCh := g.spi.beginQuery(g.spi.nextReqID())
	if ret := g.api.ReqQryInvestorPosition(field, resCh.reqID); ret != 0 {
		return nil, fmt.Errorf("ReqQryInvestorPosition failed: %d", ret)
	}
	res, err := g.waitQueryResult(resCh, "positions")
	if err != nil {
		return nil, err
	}
	if res.err != nil {
		return nil, res.err
	}
	g.touchQuery()
	return res.positions, nil
}

func (g *CTPGateway) RefreshOrders() ([]OrderRecord, error) {
	field := ctp.NewCThostFtdcQryOrderField()
	defer ctp.DeleteCThostFtdcQryOrderField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	resCh := g.spi.beginQuery(g.spi.nextReqID())
	if ret := g.api.ReqQryOrder(field, resCh.reqID); ret != 0 {
		return nil, fmt.Errorf("ReqQryOrder failed: %d", ret)
	}
	res, err := g.waitQueryResult(resCh, "orders")
	if err != nil {
		return nil, err
	}
	if res.err != nil {
		return nil, res.err
	}
	g.touchQuery()
	return res.orders, nil
}

func (g *CTPGateway) RefreshTrades() ([]TradeRecord, error) {
	field := ctp.NewCThostFtdcQryTradeField()
	defer ctp.DeleteCThostFtdcQryTradeField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	resCh := g.spi.beginQuery(g.spi.nextReqID())
	if ret := g.api.ReqQryTrade(field, resCh.reqID); ret != 0 {
		return nil, fmt.Errorf("ReqQryTrade failed: %d", ret)
	}
	res, err := g.waitQueryResult(resCh, "trades")
	if err != nil {
		return nil, err
	}
	if res.err != nil {
		return nil, res.err
	}
	g.touchQuery()
	return res.trades, nil
}

func (g *CTPGateway) waitQueryResult(resCh *queryResult, kind string) (queryResult, error) {
	timeout := time.Duration(g.cfg.LoginWaitSeconds) * time.Second
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	select {
	case res := <-resCh.done:
		return res, nil
	case <-time.After(timeout):
		if g.spi != nil {
			g.spi.cancelQuery(resCh.reqID)
		}
		return queryResult{}, fmt.Errorf("trade gateway wait %s query timeout", kind)
	}
}

func (g *CTPGateway) SubmitOrder(commandID string, req SubmitOrderRequest) (OrderRecord, error) {
	orderRef := g.spi.nextOrderRef()
	field := ctp.NewCThostFtdcInputOrderField()
	defer ctp.DeleteCThostFtdcInputOrderField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	field.SetUserID(g.cfg.UserID)
	field.SetInstrumentID(req.Symbol)
	field.SetExchangeID(req.ExchangeID)
	field.SetOrderRef(orderRef)
	field.SetOrderPriceType(ctp.THOST_FTDC_OPT_LimitPrice)
	field.SetDirection(mapDirection(req.Direction))
	field.SetCombOffsetFlag(string(mapOffsetFlag(req.OffsetFlag)))
	field.SetCombHedgeFlag(string(ctp.THOST_FTDC_HF_Speculation))
	field.SetLimitPrice(req.LimitPrice)
	field.SetVolumeTotalOriginal(req.Volume)
	field.SetTimeCondition(ctp.THOST_FTDC_TC_GFD)
	field.SetVolumeCondition(ctp.THOST_FTDC_VC_AV)
	field.SetContingentCondition(ctp.THOST_FTDC_CC_Immediately)
	field.SetForceCloseReason(ctp.THOST_FTDC_FCC_NotForceClose)
	reqID := g.spi.nextReqID()
	now := time.Now()
	record := OrderRecord{
		AccountID:           g.tradeCfg.AccountID,
		CommandID:           commandID,
		OrderRef:            orderRef,
		FrontID:             g.spi.frontID(),
		SessionID:           g.spi.sessionID(),
		ExchangeID:          req.ExchangeID,
		Symbol:              req.Symbol,
		Direction:           req.Direction,
		OffsetFlag:          req.OffsetFlag,
		LimitPrice:          req.LimitPrice,
		VolumeTotalOriginal: req.Volume,
		OrderStatus:         "submitted",
		SubmitStatus:        "submitted",
		InsertedAt:          now,
		UpdatedAt:           now,
	}
	g.spi.bindCommand(orderRef, commandID)
	if ret := g.api.ReqOrderInsert(field, reqID); ret != 0 {
		return record, fmt.Errorf("ReqOrderInsert failed: %d", ret)
	}
	return record, nil
}

func (g *CTPGateway) CancelOrder(req CancelOrderRequest) (OrderRecord, error) {
	field := ctp.NewCThostFtdcInputOrderActionField()
	defer ctp.DeleteCThostFtdcInputOrderActionField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	field.SetUserID(g.cfg.UserID)
	field.SetActionFlag(ctp.THOST_FTDC_AF_Delete)
	field.SetExchangeID(req.ExchangeID)
	field.SetOrderSysID(req.OrderSysID)
	field.SetFrontID(req.FrontID)
	field.SetSessionID(req.SessionID)
	field.SetOrderRef(req.OrderRef)
	reqID := g.spi.nextReqID()
	if ret := g.api.ReqOrderAction(field, reqID); ret != 0 {
		return OrderRecord{CommandID: req.CommandID, AccountID: req.AccountID, OrderRef: req.OrderRef, ExchangeID: req.ExchangeID, OrderSysID: req.OrderSysID, UpdatedAt: time.Now()}, fmt.Errorf("ReqOrderAction failed: %d", ret)
	}
	return OrderRecord{
		AccountID:    req.AccountID,
		CommandID:    req.CommandID,
		OrderRef:     req.OrderRef,
		ExchangeID:   req.ExchangeID,
		OrderSysID:   req.OrderSysID,
		OrderStatus:  "cancel_submitted",
		SubmitStatus: "accepted",
		UpdatedAt:    time.Now(),
	}, nil
}

func (g *CTPGateway) authenticate(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcReqAuthenticateField()
	defer ctp.DeleteCThostFtdcReqAuthenticateField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetUserID(g.cfg.UserID)
	field.SetAuthCode(g.cfg.AuthCode)
	field.SetAppID(g.cfg.AppID)
	if ret := api.ReqAuthenticate(field, reqID); ret != 0 {
		return fmt.Errorf("ReqAuthenticate failed: %d", ret)
	}
	return nil
}

func (g *CTPGateway) login(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcReqUserLoginField()
	defer ctp.DeleteCThostFtdcReqUserLoginField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetUserID(g.cfg.UserID)
	field.SetPassword(g.cfg.Password)
	field.SetProtocolInfo(g.cfg.UserProductInfo)
	if ret := api.ReqUserLogin(field, reqID); ret != 0 {
		return fmt.Errorf("ReqUserLogin failed: %d", ret)
	}
	return nil
}

func (g *CTPGateway) confirmSettlement(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcSettlementInfoConfirmField()
	defer ctp.DeleteCThostFtdcSettlementInfoConfirmField(field)
	field.SetBrokerID(g.cfg.BrokerID)
	field.SetInvestorID(g.cfg.UserID)
	if ret := api.ReqSettlementInfoConfirm(field, reqID); ret != 0 {
		return fmt.Errorf("ReqSettlementInfoConfirm failed: %d", ret)
	}
	return nil
}

func (g *CTPGateway) touchQuery() {
	g.setStatus(func(st *TradeStatus) {
		st.LastQueryAt = time.Now()
	})
}

func (g *CTPGateway) setStatus(fn func(*TradeStatus)) {
	cur := g.status.Load()
	var next TradeStatus
	if cur != nil {
		next = *cur
	}
	fn(&next)
	next.Enabled = g.tradeCfg.IsEnabled()
	next.AccountID = g.tradeCfg.AccountID
	next.UpdatedAt = time.Now()
	g.status.Store(&next)
}

type loginResult struct {
	TradingDay string
	FrontID    int
	SessionID  int
	Err        error
}

type queryResult struct {
	reqID     int
	done      chan queryResult
	err       error
	accounts  []TradingAccountSnapshot
	positions []PositionSnapshot
	orders    []OrderRecord
	trades    []TradeRecord
}

type ctpTradeSpi struct {
	ctp.TraderSpi

	accountID string

	reqIDSeq    atomic.Int64
	orderRefSeq atomic.Int64

	frontConnected chan struct{}
	authResp       chan error
	loginResp      chan loginResult
	settlementResp chan error

	mu                  sync.Mutex
	connected           bool
	loggedIn            bool
	settlementConfirmed bool
	tradingDayValue     string
	frontIDValue        int
	sessionIDValue      int
	lastErr             string
	queryWaiters        map[int]*queryResult
	commandByOrderRef   map[string]string
	subscribers         map[chan GatewayEvent]struct{}
}

func newCTPTradeSpi(accountID string) *ctpTradeSpi {
	spi := &ctpTradeSpi{
		accountID:         accountID,
		frontConnected:    make(chan struct{}, 1),
		authResp:          make(chan error, 1),
		loginResp:         make(chan loginResult, 1),
		settlementResp:    make(chan error, 1),
		queryWaiters:      make(map[int]*queryResult),
		commandByOrderRef: make(map[string]string),
		subscribers:       make(map[chan GatewayEvent]struct{}),
	}
	spi.orderRefSeq.Store(time.Now().Unix() % 1000000)
	return spi
}

func (p *ctpTradeSpi) nextReqID() int {
	return int(p.reqIDSeq.Add(1))
}

func (p *ctpTradeSpi) nextOrderRef() string {
	return fmt.Sprintf("%012d", p.orderRefSeq.Add(1))
}

func (p *ctpTradeSpi) beginQuery(reqID int) *queryResult {
	q := &queryResult{reqID: reqID, done: make(chan queryResult, 1)}
	p.mu.Lock()
	p.queryWaiters[reqID] = q
	p.mu.Unlock()
	return q
}

func (p *ctpTradeSpi) cancelQuery(reqID int) {
	p.mu.Lock()
	delete(p.queryWaiters, reqID)
	p.mu.Unlock()
}

func (p *ctpTradeSpi) Subscribe() chan GatewayEvent {
	ch := make(chan GatewayEvent, 32)
	p.mu.Lock()
	p.subscribers[ch] = struct{}{}
	p.mu.Unlock()
	return ch
}

func (p *ctpTradeSpi) bindCommand(orderRef string, commandID string) {
	p.mu.Lock()
	p.commandByOrderRef[orderRef] = commandID
	p.mu.Unlock()
}

func (p *ctpTradeSpi) OnFrontConnected() {
	p.mu.Lock()
	p.connected = true
	p.mu.Unlock()
	select {
	case p.frontConnected <- struct{}{}:
	default:
	}
	logger.Info("trade gateway front connected")
}

func (p *ctpTradeSpi) OnRspAuthenticate(_ ctp.CThostFtdcRspAuthenticateField, rsp ctp.CThostFtdcRspInfoField, _ int, _ bool) {
	err := rspError(rsp)
	p.setError(err)
	select {
	case p.authResp <- err:
	default:
	}
}

func (p *ctpTradeSpi) OnRspUserLogin(login ctp.CThostFtdcRspUserLoginField, rsp ctp.CThostFtdcRspInfoField, _ int, _ bool) {
	err := rspError(rsp)
	result := loginResult{Err: err}
	if err == nil && !isNilCTPObject(login) {
		result.TradingDay = login.GetTradingDay()
		result.FrontID = login.GetFrontID()
		result.SessionID = login.GetSessionID()
		p.mu.Lock()
		p.loggedIn = true
		p.tradingDayValue = result.TradingDay
		p.frontIDValue = result.FrontID
		p.sessionIDValue = result.SessionID
		p.mu.Unlock()
	}
	p.setError(err)
	select {
	case p.loginResp <- result:
	default:
	}
}

func (p *ctpTradeSpi) OnRspSettlementInfoConfirm(_ ctp.CThostFtdcSettlementInfoConfirmField, rsp ctp.CThostFtdcRspInfoField, _ int, _ bool) {
	err := rspError(rsp)
	if err == nil {
		p.mu.Lock()
		p.settlementConfirmed = true
		p.mu.Unlock()
	}
	p.setError(err)
	select {
	case p.settlementResp <- err:
	default:
	}
}

func (p *ctpTradeSpi) OnRspQryTradingAccount(field ctp.CThostFtdcTradingAccountField, rsp ctp.CThostFtdcRspInfoField, reqID int, last bool) {
	p.handleQuery(reqID, rspError(rsp), last, func(q *queryResult) {
		if isNilCTPObject(field) {
			return
		}
		q.accounts = append(q.accounts, TradingAccountSnapshot{
			AccountID:      p.accountID,
			Balance:        field.GetBalance(),
			Available:      field.GetAvailable(),
			Margin:         field.GetCurrMargin(),
			FrozenCash:     field.GetFrozenCash(),
			Commission:     field.GetCommission(),
			CloseProfit:    field.GetCloseProfit(),
			PositionProfit: field.GetPositionProfit(),
			UpdatedAt:      time.Now(),
		})
	})
}

func (p *ctpTradeSpi) OnRspQryInvestorPosition(field ctp.CThostFtdcInvestorPositionField, rsp ctp.CThostFtdcRspInfoField, reqID int, last bool) {
	p.handleQuery(reqID, rspError(rsp), last, func(q *queryResult) {
		if isNilCTPObject(field) {
			return
		}
		q.positions = append(q.positions, PositionSnapshot{
			AccountID:     p.accountID,
			Symbol:        field.GetInstrumentID(),
			Exchange:      field.GetExchangeID(),
			Direction:     mapPosDirection(field.GetPosiDirection()),
			HedgeFlag:     mapHedgeFlag(field.GetHedgeFlag()),
			YdPosition:    field.GetYdPosition(),
			TodayPosition: field.GetTodayPosition(),
			Position:      field.GetPosition(),
			OpenCost:      field.GetOpenCost(),
			PositionCost:  field.GetPositionCost(),
			UseMargin:     field.GetUseMargin(),
			UpdatedAt:     time.Now(),
		})
	})
}

func (p *ctpTradeSpi) OnRspQryOrder(field ctp.CThostFtdcOrderField, rsp ctp.CThostFtdcRspInfoField, reqID int, last bool) {
	p.handleQuery(reqID, rspError(rsp), last, func(q *queryResult) {
		if isNilCTPObject(field) {
			return
		}
		q.orders = append(q.orders, p.mapOrder(field))
	})
}

func (p *ctpTradeSpi) OnRspQryTrade(field ctp.CThostFtdcTradeField, rsp ctp.CThostFtdcRspInfoField, reqID int, last bool) {
	p.handleQuery(reqID, rspError(rsp), last, func(q *queryResult) {
		if isNilCTPObject(field) {
			return
		}
		q.trades = append(q.trades, p.mapTrade(field))
	})
}

func (p *ctpTradeSpi) OnRtnOrder(field ctp.CThostFtdcOrderField) {
	if isNilCTPObject(field) {
		return
	}
	rec := p.mapOrder(field)
	p.broadcast(GatewayEvent{Type: "trade_order_update", Order: &rec})
}

func (p *ctpTradeSpi) OnRtnTrade(field ctp.CThostFtdcTradeField) {
	if isNilCTPObject(field) {
		return
	}
	rec := p.mapTrade(field)
	p.broadcast(GatewayEvent{Type: "trade_trade_update", Trade: &rec})
}

func (p *ctpTradeSpi) OnRspOrderInsert(_ ctp.CThostFtdcInputOrderField, rsp ctp.CThostFtdcRspInfoField, _ int, _ bool) {
	p.setError(rspError(rsp))
}

func (p *ctpTradeSpi) OnErrRtnOrderInsert(field ctp.CThostFtdcInputOrderField, rsp ctp.CThostFtdcRspInfoField) {
	if isNilCTPObject(field) {
		return
	}
	rec := OrderRecord{
		AccountID:    p.accountID,
		CommandID:    p.commandForOrderRef(field.GetOrderRef()),
		OrderRef:     field.GetOrderRef(),
		ExchangeID:   field.GetExchangeID(),
		Symbol:       field.GetInstrumentID(),
		Direction:    mapDirectionText(field.GetDirection()),
		OffsetFlag:   mapOffsetFlagText(firstByte(field.GetCombOffsetFlag())),
		LimitPrice:   field.GetLimitPrice(),
		OrderStatus:  "rejected",
		SubmitStatus: "rejected",
		StatusMsg:    errString(rspError(rsp)),
		InsertedAt:   time.Now(),
		UpdatedAt:    time.Now(),
	}
	p.broadcast(GatewayEvent{Type: "trade_order_update", Order: &rec, Err: rspError(rsp)})
}

func (p *ctpTradeSpi) OnRspOrderAction(_ ctp.CThostFtdcInputOrderActionField, rsp ctp.CThostFtdcRspInfoField, _ int, _ bool) {
	p.setError(rspError(rsp))
}

func (p *ctpTradeSpi) OnErrRtnOrderAction(field ctp.CThostFtdcOrderActionField, rsp ctp.CThostFtdcRspInfoField) {
	if isNilCTPObject(field) {
		return
	}
	rec := OrderRecord{
		AccountID:    p.accountID,
		CommandID:    p.commandForOrderRef(field.GetOrderRef()),
		OrderRef:     field.GetOrderRef(),
		ExchangeID:   field.GetExchangeID(),
		OrderSysID:   field.GetOrderSysID(),
		OrderStatus:  "cancel_rejected",
		SubmitStatus: "cancel_rejected",
		StatusMsg:    errString(rspError(rsp)),
		UpdatedAt:    time.Now(),
	}
	p.broadcast(GatewayEvent{Type: "trade_order_update", Order: &rec, Err: rspError(rsp)})
}

func (p *ctpTradeSpi) mapOrder(field ctp.CThostFtdcOrderField) OrderRecord {
	return OrderRecord{
		AccountID:           p.accountID,
		CommandID:           p.commandForOrderRef(field.GetOrderRef()),
		OrderRef:            field.GetOrderRef(),
		FrontID:             field.GetFrontID(),
		SessionID:           field.GetSessionID(),
		ExchangeID:          field.GetExchangeID(),
		OrderSysID:          strings.TrimSpace(field.GetOrderSysID()),
		Symbol:              field.GetInstrumentID(),
		Direction:           mapDirectionText(field.GetDirection()),
		OffsetFlag:          mapOffsetFlagText(firstByte(field.GetCombOffsetFlag())),
		LimitPrice:          field.GetLimitPrice(),
		VolumeTotalOriginal: field.GetVolumeTotalOriginal(),
		VolumeTraded:        field.GetVolumeTraded(),
		VolumeCanceled:      maxInt(0, field.GetVolumeTotalOriginal()-field.GetVolumeTotal()-field.GetVolumeTraded()),
		OrderStatus:         mapOrderStatus(field.GetOrderStatus()),
		SubmitStatus:        mapSubmitStatus(field.GetOrderSubmitStatus()),
		StatusMsg:           field.GetStatusMsg(),
		InsertedAt:          parseDateTime(field.GetInsertDate(), field.GetInsertTime()),
		UpdatedAt:           parseDateTime(field.GetInsertDate(), field.GetUpdateTime()),
	}
}

func (p *ctpTradeSpi) mapTrade(field ctp.CThostFtdcTradeField) TradeRecord {
	return TradeRecord{
		AccountID:  p.accountID,
		TradeID:    field.GetTradeID(),
		OrderRef:   field.GetOrderRef(),
		OrderSysID: field.GetOrderSysID(),
		ExchangeID: field.GetExchangeID(),
		Symbol:     field.GetInstrumentID(),
		Direction:  mapDirectionText(field.GetDirection()),
		OffsetFlag: mapOffsetFlagText(field.GetOffsetFlag()),
		Price:      field.GetPrice(),
		Volume:     field.GetVolume(),
		TradeTime:  parseDateTime(field.GetTradingDay(), field.GetTradeTime()),
		TradingDay: field.GetTradingDay(),
		ReceivedAt: time.Now(),
	}
}

func (p *ctpTradeSpi) handleQuery(reqID int, err error, last bool, appendFn func(*queryResult)) {
	p.mu.Lock()
	q := p.queryWaiters[reqID]
	if q != nil {
		if appendFn != nil {
			appendFn(q)
		}
		if err != nil {
			q.err = err
		}
		if last {
			delete(p.queryWaiters, reqID)
		}
	}
	p.mu.Unlock()
	if q != nil && last {
		q.done <- *q
	}
}

func (p *ctpTradeSpi) broadcast(ev GatewayEvent) {
	p.mu.Lock()
	subs := make([]chan GatewayEvent, 0, len(p.subscribers))
	for ch := range p.subscribers {
		subs = append(subs, ch)
	}
	p.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (p *ctpTradeSpi) commandForOrderRef(orderRef string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commandByOrderRef[orderRef]
}

func (p *ctpTradeSpi) isConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connected
}

func (p *ctpTradeSpi) isLoggedIn() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.loggedIn
}

func (p *ctpTradeSpi) isSettlementConfirmed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.settlementConfirmed
}

func (p *ctpTradeSpi) tradingDay() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tradingDayValue
}

func (p *ctpTradeSpi) frontID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.frontIDValue
}

func (p *ctpTradeSpi) sessionID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessionIDValue
}

func (p *ctpTradeSpi) lastError() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

func (p *ctpTradeSpi) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.lastErr = err.Error()
	} else {
		p.lastErr = ""
	}
}

func rspError(rsp ctp.CThostFtdcRspInfoField) error {
	if isNilCTPObject(rsp) {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			logger.Error("trade rsp info access panic recovered", "panic", r)
		}
	}()
	errID := rsp.GetErrorID()
	if errID == 0 {
		return nil
	}
	return fmt.Errorf("ctp error %d: %s", errID, strings.TrimSpace(rsp.GetErrorMsg()))
}

func isNilCTPObject(v any) bool {
	if v == nil {
		return true
	}
	obj, ok := v.(interface{ Swigcptr() uintptr })
	if !ok {
		return false
	}
	return obj.Swigcptr() == 0
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func parseDateTime(datePart string, timePart string) time.Time {
	datePart = strings.TrimSpace(strings.ReplaceAll(datePart, "-", ""))
	timePart = strings.TrimSpace(timePart)
	if len(datePart) != 8 || len(timePart) < 8 {
		return time.Now()
	}
	ts, err := time.ParseInLocation("2006010215:04:05", datePart+timePart[:8], time.Local)
	if err != nil {
		return time.Now()
	}
	return ts
}

func mapDirection(v string) byte {
	if strings.EqualFold(strings.TrimSpace(v), "sell") {
		return ctp.THOST_FTDC_D_Sell
	}
	return ctp.THOST_FTDC_D_Buy
}

func mapDirectionText(v byte) string {
	if v == ctp.THOST_FTDC_D_Sell {
		return "sell"
	}
	return "buy"
}

func mapPosDirection(v byte) string {
	if v == '2' {
		return "short"
	}
	return "long"
}

func mapHedgeFlag(v byte) string {
	if v == ctp.THOST_FTDC_HF_Speculation {
		return "speculation"
	}
	return string(v)
}

func mapOffsetFlag(v string) byte {
	switch strings.TrimSpace(v) {
	case "close":
		return ctp.THOST_FTDC_OF_Close
	case "close_today":
		return ctp.THOST_FTDC_OF_CloseToday
	case "close_yesterday":
		return ctp.THOST_FTDC_OF_CloseYesterday
	default:
		return ctp.THOST_FTDC_OF_Open
	}
}

func mapOffsetFlagText(v byte) string {
	switch v {
	case ctp.THOST_FTDC_OF_Close:
		return "close"
	case ctp.THOST_FTDC_OF_CloseToday:
		return "close_today"
	case ctp.THOST_FTDC_OF_CloseYesterday:
		return "close_yesterday"
	default:
		return "open"
	}
}

func mapOrderStatus(v byte) string {
	switch v {
	case ctp.THOST_FTDC_OST_AllTraded:
		return "all_traded"
	case ctp.THOST_FTDC_OST_PartTradedQueueing:
		return "part_traded_queueing"
	case ctp.THOST_FTDC_OST_PartTradedNotQueueing:
		return "part_traded_not_queueing"
	case ctp.THOST_FTDC_OST_NoTradeQueueing:
		return "no_trade_queueing"
	case ctp.THOST_FTDC_OST_NoTradeNotQueueing:
		return "no_trade_not_queueing"
	case ctp.THOST_FTDC_OST_Canceled:
		return "canceled"
	default:
		return string(v)
	}
}

func mapSubmitStatus(v byte) string {
	switch v {
	case ctp.THOST_FTDC_OSS_InsertSubmitted:
		return "insert_submitted"
	case ctp.THOST_FTDC_OSS_CancelSubmitted:
		return "cancel_submitted"
	case ctp.THOST_FTDC_OSS_Accepted:
		return "accepted"
	case ctp.THOST_FTDC_OSS_InsertRejected:
		return "insert_rejected"
	case ctp.THOST_FTDC_OSS_CancelRejected:
		return "cancel_rejected"
	default:
		return string(v)
	}
}

func firstByte(v string) byte {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
