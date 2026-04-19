// service.go 是实盘交易子系统的服务层。
// 它对接 CTPGateway、维护账户/持仓/委托/成交快照、执行周期性查询，并通过事件把交易状态同步给 Web 控制面。
package trade

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/quotes"
)

type Service struct {
	// cfg 保存交易子系统本身的启用和风控配置。
	cfg config.TradeConfig
	// ctpCfg 保存交易侧复用的 CTP 接入参数。
	ctpCfg config.CTPConfig
	// store 是交易子系统的持久化存储。
	store *Store
	// gateway 是底层 CTP 交易网关抽象。
	gateway        Gateway
	tradeOpGateway *CTPGateway
	autoPosGateway *CTPGateway
	feeGateway     *CTPGateway
	marginGateway  *CTPGateway
	rateCatalog    *rateCatalog
	// accountID 是系统内部统一使用的账户标识。
	accountID string
	// paper 表示当前服务是否为模拟交易后端。
	paper bool
	// replayPaper 表示当前服务是否为回放模拟交易后端。
	replayPaper bool
	// livePaper 表示当前服务是否为实时模拟交易后端。
	livePaper bool

	// mu 保护 status、account、positions 这些内存快照。
	mu sync.RWMutex
	// status 是当前交易链路状态快照。
	status TradeStatus
	// account 是最近一次账户资金快照。
	account TradingAccountSnapshot
	// positions 是最近一次持仓快照。
	positions []PositionSnapshot

	// subs 保存订阅交易事件的监听者。
	subs map[chan EventEnvelope]struct{}
	// subsMu 保护 subs 集合。
	subsMu sync.Mutex
	// busLog 用于把订单指令等事件旁路写到 bus，总线可选。
	busLog             *bus.FileLog
	ctx                context.Context
	cancel             context.CancelFunc
	startMu            sync.Mutex
	started            bool
	closeOnce          sync.Once
	queueHandle        *queuewatch.QueueHandle
	queueCap           int
	paperMu            sync.Mutex
	pending            map[string]OrderRecord
	replayQuotes       map[string]replayQuote
	resolver           *quotes.ProductExchangeCache
	laneStateMu        sync.RWMutex
	feeOrdersByFeeLane bool
	tradesByMarginLane bool
	metaSyncMu         sync.Mutex
	metaSyncRequested  bool
	metaSyncRunning    bool
	metaSyncTradingDay string
	feeThrottle        laneThrottle
	marginThrottle     laneThrottle
}

const replayPaperInitialBalance = 100_000

type replayTick struct {
	InstrumentID string
	ExchangeID   string
	TradingDay   string
	ActionDay    string
	UpdateTime   string
	UpdateMS     int
	ReceivedAt   time.Time
	BidPrice1    float64
	AskPrice1    float64
}

type replayQuote struct {
	BidPrice1  float64
	AskPrice1  float64
	LastTickAt time.Time
}

func NewService(cfg config.TradeConfig, ctpCfg config.CTPConfig, dsn string, registry *queuewatch.Registry) (*Service, error) {
	if count, err := quotes.DefaultProductExchangeCache().EnsureLoadedFromDSN(ctpCfg.SharedMetaDSN); err != nil {
		logger.Error("load product exchange cache for trade service failed", "error", err)
	} else if count > 0 {
		logger.Info("product exchange cache ready", "source", "trade_service_init", "product_exchange_count", count)
	}
	store, err := NewStore(dsn)
	if err != nil {
		return nil, err
	}
	if err := store.UpsertTradeAccount(cfg.AccountID, ctpCfg.BrokerID, ctpCfg.UserID); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	queueCfg := queuewatch.DefaultConfig("")
	if registry != nil {
		queueCfg = registry.Config()
	}
	tradeOpGateway := NewCTPGateway(withTradeFlowSuffix(ctpCfg, "trade_op"), cfg, registry)
	autoPosGateway := NewCTPGateway(withTradeFlowSuffix(ctpCfg, "auto_pos"), cfg, registry)
	feeGateway := NewCTPGateway(withTradeFlowSuffix(ctpCfg, "fee_lane"), cfg, registry)
	marginGateway := NewCTPGateway(withTradeFlowSuffix(ctpCfg, "margin_lane"), cfg, registry)

	var catalog *rateCatalog
	if strings.TrimSpace(ctpCfg.SharedMetaDSN) != "" {
		catalog, err = newRateCatalog(ctpCfg.SharedMetaDSN)
		if err != nil {
			return nil, err
		}
	}
	s := &Service{
		cfg:            cfg,
		ctpCfg:         ctpCfg,
		store:          store,
		gateway:        tradeOpGateway,
		tradeOpGateway: tradeOpGateway,
		autoPosGateway: autoPosGateway,
		feeGateway:     feeGateway,
		marginGateway:  marginGateway,
		rateCatalog:    catalog,
		accountID:      cfg.AccountID,
		subs:           make(map[chan EventEnvelope]struct{}),
		queueCap:       queueCfg.TradeEventCapacity,
		status:         TradeStatus{Enabled: cfg.IsEnabled(), AccountID: cfg.AccountID, UpdatedAt: time.Now()},
		ctx:            ctx,
		cancel:         cancel,
		resolver:       quotes.DefaultProductExchangeCache(),
	}
	if registry != nil {
		s.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        "trade_event_subscribers",
			Category:    "trade",
			Criticality: "best_effort",
			Capacity:    queueCfg.TradeEventCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "??????? Web????????????????",
		})
	}
	if ctpCfg.IsBusEnabled() {
		busPath := strings.TrimSpace(ctpCfg.BusLogPath)
		if busPath == "" {
			busPath = filepath.Join(ctpCfg.FlowPath, "bus")
		}
		s.busLog = bus.NewFileLog(busPath, time.Duration(ctpCfg.BusFlushMS)*time.Millisecond)
	}
	if st, err := store.LoadSessionState(cfg.AccountID); err == nil {
		s.status.TraderFront = st.Connected
		s.status.TraderLogin = st.LoggedIn
		s.status.SettlementConfirmed = st.SettlementConfirmed
		s.status.TradingDay = st.TradingDay
		s.status.FrontID = st.FrontID
		s.status.SessionID = st.SessionID
	}
	if item, err := store.LatestAccountSnapshot(cfg.AccountID); err == nil {
		s.account = item
	}
	if items, err := store.ListPositions(cfg.AccountID); err == nil {
		s.positions = items
	}
	return s, nil
}

func NewPaperService(cfg config.TradeConfig, accountID string, dsn string, registry *queuewatch.Registry) (*Service, error) {
	store, err := NewStore(dsn)
	if err != nil {
		return nil, err
	}
	if accountID == "" {
		accountID = "paper"
	}
	ctx, cancel := context.WithCancel(context.Background())
	queueCfg := queuewatch.DefaultConfig("")
	if registry != nil {
		queueCfg = registry.Config()
	}
	cfg.Enabled = boolPtr(true)
	cfg.AccountID = accountID
	s := &Service{
		cfg:          cfg,
		store:        store,
		accountID:    accountID,
		paper:        true,
		replayPaper:  strings.EqualFold(accountID, "paper_replay"),
		livePaper:    strings.EqualFold(accountID, "paper_live"),
		subs:         make(map[chan EventEnvelope]struct{}),
		queueCap:     queueCfg.TradeEventCapacity,
		pending:      make(map[string]OrderRecord),
		replayQuotes: make(map[string]replayQuote),
		status: TradeStatus{
			Enabled:             true,
			AccountID:           accountID,
			TraderFront:         true,
			TraderLogin:         true,
			SettlementConfirmed: true,
			UpdatedAt:           time.Now(),
		},
		ctx:      ctx,
		cancel:   cancel,
		resolver: quotes.DefaultProductExchangeCache(),
	}
	if registry != nil {
		s.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        "trade_event_subscribers_" + accountID,
			Category:    "trade",
			Criticality: "best_effort",
			Capacity:    queueCfg.TradeEventCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "paper trade subscribers",
		})
	}
	if err := store.UpsertTradeAccount(accountID, "paper", accountID); err != nil {
		return nil, err
	}
	if err := s.ensurePaperAccount(); err != nil {
		return nil, err
	}
	if s.replayPaper {
		if err := s.loadPendingOrders(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func NewPaperServiceWithMeta(cfg config.TradeConfig, ctpCfg config.CTPConfig, accountID string, dsn string, registry *queuewatch.Registry) (*Service, error) {
	s, err := NewPaperService(cfg, accountID, dsn, registry)
	if err != nil {
		return nil, err
	}
	if !s.livePaper {
		return s, nil
	}
	if strings.TrimSpace(ctpCfg.TraderFrontAddr) == "" {
		return s, nil
	}
	s.ctpCfg = ctpCfg
	s.tradeOpGateway = NewCTPGateway(withTradeFlowSuffix(ctpCfg, "paper_live_meta_trade_op"), s.cfg, registry)
	s.feeGateway = NewCTPGateway(withTradeFlowSuffix(ctpCfg, "paper_live_meta_fee_lane"), s.cfg, registry)
	s.marginGateway = NewCTPGateway(withTradeFlowSuffix(ctpCfg, "paper_live_meta_margin_lane"), s.cfg, registry)
	if strings.TrimSpace(ctpCfg.SharedMetaDSN) != "" {
		catalog, catalogErr := newRateCatalog(ctpCfg.SharedMetaDSN)
		if catalogErr != nil {
			return nil, catalogErr
		}
		s.rateCatalog = catalog
	}
	s.feeThrottle = newLaneThrottle("fee_lane", "commission_rate", s.cfg)
	s.marginThrottle = newLaneThrottle("margin_lane", "margin_rate", s.cfg)
	return s, nil
}

func (s *Service) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.startMu.Lock()
		s.started = false
		s.startMu.Unlock()
		s.subsMu.Lock()
		for ch := range s.subs {
			close(ch)
			delete(s.subs, ch)
		}
		s.subsMu.Unlock()
		if s.gateway != nil {
			_ = s.gateway.Close()
		}
		if s.autoPosGateway != nil {
			_ = s.autoPosGateway.Close()
		}
		if s.feeGateway != nil {
			_ = s.feeGateway.Close()
		}
		if s.marginGateway != nil {
			_ = s.marginGateway.Close()
		}
		if s.rateCatalog != nil {
			_ = s.rateCatalog.Close()
		}
		if s.store != nil {
			err = s.store.Close()
		}
	})
	return err
}

func (s *Service) Start() error {
	if s.paper {
		s.startMu.Lock()
		s.started = true
		s.startMu.Unlock()
		s.broadcast("trade_status_update", s.Status())
		if s.livePaper {
			if err := s.startPaperMetaSync(); err != nil {
				s.setStatus(func(st *TradeStatus) { st.LastError = err.Error() })
				return err
			}
		}
		return nil
	}
	s.startMu.Lock()
	if s.started {
		s.startMu.Unlock()
		return nil
	}
	s.startMu.Unlock()
	if err := s.gateway.Start(); err != nil {
		s.setStatus(func(st *TradeStatus) { st.LastError = err.Error() })
		return err
	}
	if err := s.startQueryGateways(); err != nil {
		s.setStatus(func(st *TradeStatus) { st.LastError = err.Error() })
		return err
	}
	s.startMu.Lock()
	if s.started {
		s.startMu.Unlock()
		return nil
	}
	s.started = true
	s.startMu.Unlock()
	s.setStatusFromGateway()
	if err := s.saveSessionState(); err != nil {
		logger.Warn("save trade session state failed", "error", err)
	}
	ch, _ := s.gateway.Subscribe()
	go s.forwardGatewayEvents(ch)
	go s.pollQueries()
	return nil
}

func (s *Service) ResetPaperReplay() error {
	if !s.replayPaper {
		return fmt.Errorf("paper replay reset is only available for replay paper service")
	}
	s.paperMu.Lock()
	defer s.paperMu.Unlock()
	if err := s.store.ResetPaperAccount(s.accountID); err != nil {
		return err
	}
	s.pending = make(map[string]OrderRecord)
	s.replayQuotes = make(map[string]replayQuote)
	if err := s.ensurePaperAccount(); err != nil {
		return err
	}
	if err := s.RefreshAll(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Status() TradeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *Service) Account() (TradingAccountSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.account.AccountID == "" {
		return TradingAccountSnapshot{}, sql.ErrNoRows
	}
	return s.account, nil
}

func (s *Service) Positions() ([]PositionSnapshot, error) {
	return s.store.ListPositions(s.accountID)
}

func (s *Service) Orders(limit int) ([]OrderRecord, error) {
	return s.store.ListOrders(s.accountID, limit)
}

func (s *Service) Order(commandID string) (OrderRecord, error) {
	return s.store.GetOrder(commandID)
}

func (s *Service) Trades(limit int) ([]TradeRecord, error) {
	return s.store.ListTrades(s.accountID, limit)
}

func (s *Service) Audits(limit int) ([]OrderCommandAudit, error) {
	return s.store.ListCommandAudits(s.accountID, limit)
}

func (s *Service) RefreshAccount() (TradingAccountSnapshot, error) {
	if s.paper {
		item, err := s.store.LatestAccountSnapshot(s.accountID)
		if err != nil {
			return TradingAccountSnapshot{}, err
		}
		s.mu.Lock()
		s.account = item
		s.mu.Unlock()
		s.broadcast("trade_account_update", item)
		return item, nil
	}
	item, err := s.gateway.RefreshAccount()
	s.auditQuery("account", err)
	if err != nil {
		return TradingAccountSnapshot{}, err
	}
	if err := s.store.SaveAccountSnapshot(item); err != nil {
		return TradingAccountSnapshot{}, err
	}
	s.mu.Lock()
	s.account = item
	s.mu.Unlock()
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_account_update", item)
	return item, nil
}

func (s *Service) RefreshPositions() ([]PositionSnapshot, error) {
	if s.paper {
		items, err := s.store.ListPositions(s.accountID)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.positions = items
		s.mu.Unlock()
		s.broadcast("trade_position_update", map[string]any{"items": items})
		return items, nil
	}
	gw := s.gateway
	if s.autoPosGateway != nil {
		gw = s.autoPosGateway
	}
	items, err := gw.RefreshPositions()
	s.auditQuery("positions", err)
	if err != nil {
		return nil, err
	}
	if err := s.store.ReplacePositions(s.accountID, items); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.positions = items
	s.mu.Unlock()
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_position_update", map[string]any{"items": items})
	return items, nil
}

func (s *Service) RefreshOrders() ([]OrderRecord, error) {
	if s.paper {
		items, err := s.store.ListOrders(s.accountID, 200)
		if err != nil {
			return nil, err
		}
		s.broadcast("trade_order_update", map[string]any{"items": items})
		return items, nil
	}
	gw := s.gateway
	if s.feeGateway != nil {
		gw = s.feeGateway
	}
	items, err := gw.RefreshOrders()
	s.auditQuery("orders", err)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.CommandID == "" {
			item.CommandID = mustCommandID("qry")
		}
		if err := s.store.UpsertOrder(item); err != nil {
			return nil, err
		}
	}
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_order_update", map[string]any{"items": items})
	return items, nil
}

func (s *Service) RefreshTrades() ([]TradeRecord, error) {
	if s.paper {
		items, err := s.store.ListTrades(s.accountID, 200)
		if err != nil {
			return nil, err
		}
		s.broadcast("trade_trade_update", map[string]any{"items": items})
		return items, nil
	}
	gw := s.gateway
	if s.marginGateway != nil {
		gw = s.marginGateway
	}
	items, err := gw.RefreshTrades()
	s.auditQuery("trades", err)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if err := s.store.AppendTrade(item); err != nil {
			return nil, err
		}
	}
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_trade_update", map[string]any{"items": items})
	return items, nil
}

func (s *Service) RefreshAll() error {
	if s.livePaper {
		s.RequestMetaSync()
	}
	if _, err := s.RefreshAccount(); err != nil {
		return err
	}
	if _, err := s.RefreshPositions(); err != nil {
		return err
	}
	if _, err := s.RefreshOrders(); err != nil {
		return err
	}
	if _, err := s.RefreshTrades(); err != nil {
		return err
	}
	return nil
}

func (s *Service) RequestMetaSync() {
	if !s.livePaper {
		return
	}
	s.metaSyncMu.Lock()
	s.metaSyncRequested = true
	s.metaSyncMu.Unlock()
}

func (s *Service) SubmitOrder(ctx context.Context, req SubmitOrderRequest) (OrderRecord, error) {
	req, err := s.normalizeSubmitRequest(req)
	if err != nil {
		return OrderRecord{AccountID: s.accountID, Symbol: strings.TrimSpace(req.Symbol), ExchangeID: strings.TrimSpace(req.ExchangeID), UpdatedAt: time.Now()}, err
	}
	status := s.Status()
	account, _ := s.Account()
	positions, _ := s.Positions()
	if s.replayPaper || s.livePaper {
		if item, items, err := s.paperRiskState(); err == nil {
			account = item
			positions = items
		}
	}
	commandID, err := ValidateSubmit(ctx, status, s.cfg, account, positions, req)
	audit := OrderCommandAudit{
		AccountID:   s.accountID,
		CommandID:   commandID,
		CommandType: CommandTypeSubmit,
		Symbol:      req.Symbol,
		RiskStatus:  RiskStatusAllowed,
		Request:     map[string]any{"submit": req},
		Response:    map[string]any{},
		CreatedAt:   time.Now(),
	}
	if err != nil {
		audit.RiskStatus = RiskStatusBlocked
		audit.RiskReason = err.Error()
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return OrderRecord{AccountID: s.accountID, CommandID: commandID, Symbol: req.Symbol, UpdatedAt: time.Now()}, err
	}
	if s.paper {
		rec, err := s.submitPaperOrder(commandID, req)
		if err != nil {
			audit.RiskStatus = RiskStatusBlocked
			audit.RiskReason = err.Error()
			audit.Response["error"] = err.Error()
			_, _ = s.store.AppendCommandAudit(audit)
			s.broadcast("trade_command_audit", audit)
			return rec, err
		}
		audit.Response["order"] = rec
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return rec, nil
	}
	rec, err := s.gateway.SubmitOrder(commandID, req)
	if err != nil {
		audit.RiskStatus = RiskStatusBlocked
		audit.RiskReason = err.Error()
		audit.Response["error"] = err.Error()
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return rec, err
	}
	rec.CommandID = commandID
	rec.AccountID = s.accountID
	if err := s.store.UpsertOrder(rec); err != nil {
		return rec, err
	}
	audit.Response["order"] = rec
	_, _ = s.store.AppendCommandAudit(audit)
	s.appendBusEvent(bus.TopicOrderCommand, "trade.web", rec.UpdatedAt, rec)
	s.broadcast("trade_command_audit", audit)
	s.broadcast("trade_order_update", rec)
	return rec, nil
}

func (s *Service) CancelOrder(ctx context.Context, req CancelOrderRequest) (OrderRecord, error) {
	orderRec, err := s.store.GetOrder(req.CommandID)
	if err != nil {
		return OrderRecord{CommandID: req.CommandID, AccountID: s.accountID}, err
	}
	req, err = s.normalizeCancelRequest(req, orderRec)
	if err != nil {
		return orderRec, err
	}
	if isNoopCancel(req) {
		return orderRec, fmt.Errorf("cancel request missing order identifiers")
	}
	if err := ValidateCancel(ctx, req, orderRec); err != nil {
		audit := OrderCommandAudit{
			AccountID:   s.accountID,
			CommandID:   req.CommandID,
			CommandType: CommandTypeCancel,
			Symbol:      orderRec.Symbol,
			RiskStatus:  RiskStatusBlocked,
			RiskReason:  err.Error(),
			Request:     map[string]any{"cancel": req},
			Response:    map[string]any{"order": orderRec},
			CreatedAt:   time.Now(),
		}
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return orderRec, err
	}
	if s.paper {
		rec, err := s.cancelPaperOrder(req, orderRec)
		audit := OrderCommandAudit{
			AccountID:   s.accountID,
			CommandID:   req.CommandID,
			CommandType: CommandTypeCancel,
			Symbol:      orderRec.Symbol,
			RiskStatus:  RiskStatusAllowed,
			Request:     map[string]any{"cancel": req},
			Response:    map[string]any{"order": rec},
			CreatedAt:   time.Now(),
		}
		if err != nil {
			audit.RiskStatus = RiskStatusBlocked
			audit.RiskReason = err.Error()
			audit.Response["error"] = err.Error()
			_, _ = s.store.AppendCommandAudit(audit)
			s.broadcast("trade_command_audit", audit)
			return rec, err
		}
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return rec, nil
	}
	rec, err := s.gateway.CancelOrder(req)
	audit := OrderCommandAudit{
		AccountID:   s.accountID,
		CommandID:   req.CommandID,
		CommandType: CommandTypeCancel,
		Symbol:      orderRec.Symbol,
		RiskStatus:  RiskStatusAllowed,
		Request:     map[string]any{"cancel": req},
		Response:    map[string]any{"order": rec},
		CreatedAt:   time.Now(),
	}
	if err != nil {
		audit.RiskStatus = RiskStatusBlocked
		audit.RiskReason = err.Error()
		_, _ = s.store.AppendCommandAudit(audit)
		s.broadcast("trade_command_audit", audit)
		return rec, err
	}
	rec.CommandID = req.CommandID
	rec.Symbol = orderRec.Symbol
	rec.Direction = orderRec.Direction
	rec.OffsetFlag = orderRec.OffsetFlag
	rec.LimitPrice = orderRec.LimitPrice
	rec.VolumeTotalOriginal = orderRec.VolumeTotalOriginal
	if err := s.store.UpsertOrder(rec); err != nil {
		return rec, err
	}
	_, _ = s.store.AppendCommandAudit(audit)
	s.appendBusEvent(bus.TopicOrderCommand, "trade.web", rec.UpdatedAt, rec)
	s.broadcast("trade_command_audit", audit)
	s.broadcast("trade_order_update", rec)
	return rec, nil
}

func (s *Service) Subscribe() (<-chan EventEnvelope, func()) {
	cap := s.queueCap
	if cap <= 0 {
		cap = queuewatch.DefaultConfig("").TradeEventCapacity
	}
	ch := make(chan EventEnvelope, cap)
	s.subsMu.Lock()
	s.subs[ch] = struct{}{}
	s.subsMu.Unlock()
	if s.queueHandle != nil {
		s.queueHandle.ObserveDepth(s.maxSubDepth())
	}
	cancel := func() {
		s.subsMu.Lock()
		if _, ok := s.subs[ch]; ok {
			delete(s.subs, ch)
			close(ch)
		}
		s.subsMu.Unlock()
	}
	return ch, cancel
}

func (s *Service) forwardGatewayEvents(ch <-chan GatewayEvent) {
	for ev := range ch {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		if ev.Order != nil {
			if ev.Order.AccountID == "" {
				ev.Order.AccountID = s.accountID
			}
			if ev.Order.CommandID == "" {
				ev.Order.CommandID = mustCommandID("evt")
			}
			if ev.Order.UpdatedAt.IsZero() {
				ev.Order.UpdatedAt = time.Now()
			}
			if ev.Order.InsertedAt.IsZero() {
				ev.Order.InsertedAt = ev.Order.UpdatedAt
			}
			if err := s.store.UpsertOrder(*ev.Order); err == nil {
				s.appendBusEvent(bus.TopicOrderStatus, "trade.gateway", ev.Order.UpdatedAt, ev.Order)
			}
			s.broadcast("trade_order_update", ev.Order)
		}
		if ev.Trade != nil {
			if ev.Trade.AccountID == "" {
				ev.Trade.AccountID = s.accountID
			}
			if ev.Trade.ReceivedAt.IsZero() {
				ev.Trade.ReceivedAt = time.Now()
			}
			_ = s.store.AppendTrade(*ev.Trade)
			s.broadcast("trade_trade_update", ev.Trade)
		}
		if ev.Err != nil {
			s.setStatus(func(st *TradeStatus) { st.LastError = ev.Err.Error() })
			s.broadcast("trade_status_update", s.Status())
		}
	}
}

func (s *Service) pollQueries() {
	if s.paper {
		return
	}
	go s.runAutoPosLane()
	go s.runFeeLane()
	go s.runMarginLane()
	statusTicker := time.NewTicker(time.Duration(s.cfg.QueryPollIntervalMS) * time.Millisecond)
	defer statusTicker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-statusTicker.C:
			s.setStatusFromGateway()
			_ = s.saveSessionState()
			s.broadcast("trade_status_update", s.Status())
		}
	}
}

func (s *Service) setStatusFromGateway() {
	if s.paper {
		s.setStatus(func(st *TradeStatus) {
			st.TraderFront = true
			st.TraderLogin = true
			st.SettlementConfirmed = true
			st.LastError = ""
		})
		return
	}
	gw := s.gateway.Status()
	s.setStatus(func(st *TradeStatus) {
		*st = gw
	})
}

func (s *Service) setStatus(fn func(*TradeStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.status)
	s.status.Enabled = s.cfg.IsEnabled()
	s.status.AccountID = s.accountID
	s.status.UpdatedAt = time.Now()
}

func (s *Service) saveSessionState() error {
	st := s.Status()
	return s.store.SaveSessionState(SessionState{
		AccountID:           st.AccountID,
		FrontID:             st.FrontID,
		SessionID:           st.SessionID,
		NextOrderRef:        time.Now().Unix(),
		Connected:           st.TraderFront,
		Authenticated:       st.TraderFront,
		LoggedIn:            st.TraderLogin,
		SettlementConfirmed: st.SettlementConfirmed,
		TradingDay:          st.TradingDay,
		UpdatedAt:           time.Now(),
	})
}

func (s *Service) auditQuery(kind string, err error) {
	status := QueryStatusOK
	detail := "ok"
	if err != nil {
		status = QueryStatusError
		detail = err.Error()
	}
	_ = s.store.AppendQueryAudit(QueryAudit{
		AccountID: s.accountID,
		QueryType: kind,
		Status:    status,
		Detail:    detail,
		CreatedAt: time.Now(),
	})
}

func (s *Service) normalizeSubmitRequest(req SubmitOrderRequest) (SubmitOrderRequest, error) {
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.ExchangeID = strings.TrimSpace(req.ExchangeID)
	if s == nil || s.resolver == nil || s.resolver.Count() == 0 || req.Symbol == "" {
		return req, nil
	}
	resolved, err := s.resolver.Resolve(req.Symbol, req.ExchangeID)
	if err != nil {
		return req, err
	}
	req.Symbol = resolved.Symbol
	req.ExchangeID = resolved.ExchangeID
	return req, nil
}

func (s *Service) normalizeCancelRequest(req CancelOrderRequest, orderRec OrderRecord) (CancelOrderRequest, error) {
	req.OrderRef = firstNonEmpty(strings.TrimSpace(req.OrderRef), strings.TrimSpace(orderRec.OrderRef))
	req.OrderSysID = firstNonEmpty(strings.TrimSpace(req.OrderSysID), strings.TrimSpace(orderRec.OrderSysID))
	if req.FrontID == 0 {
		req.FrontID = orderRec.FrontID
	}
	if req.SessionID == 0 {
		req.SessionID = orderRec.SessionID
	}
	req.ExchangeID = strings.TrimSpace(req.ExchangeID)
	orderExchangeID := strings.TrimSpace(orderRec.ExchangeID)
	if req.ExchangeID == "" {
		req.ExchangeID = orderExchangeID
	} else if orderExchangeID != "" && !strings.EqualFold(req.ExchangeID, orderExchangeID) {
		return req, fmt.Errorf("cancel exchange_id mismatch: want %s", orderExchangeID)
	} else if orderExchangeID != "" {
		req.ExchangeID = orderExchangeID
	}
	if req.ExchangeID == "" {
		return req, fmt.Errorf("exchange_id is required")
	}
	return req, nil
}

func (s *Service) broadcast(eventType string, data any) {
	payload := EventEnvelope{Type: eventType, Data: data}
	s.subsMu.Lock()
	subs := make([]chan EventEnvelope, 0, len(s.subs))
	for ch := range s.subs {
		subs = append(subs, ch)
	}
	s.subsMu.Unlock()
	for _, ch := range subs {
		func(out chan EventEnvelope) {
			defer func() {
				_ = recover()
			}()
			select {
			case out <- payload:
				if s.queueHandle != nil {
					s.queueHandle.MarkEnqueued(s.maxSubDepth())
				}
			default:
				if s.queueHandle != nil {
					s.queueHandle.MarkDropped(s.maxSubDepth())
				}
			}
		}(ch)
	}
}

func (s *Service) maxSubDepth() int {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	maxDepth := 0
	for ch := range s.subs {
		if depth := len(ch); depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func (s *Service) appendBusEvent(topic string, source string, occurredAt time.Time, payload any) {
	if s.busLog == nil {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = s.busLog.Append(bus.BusEvent{
		EventID:    bus.NewEventID(),
		Topic:      topic,
		Source:     source,
		OccurredAt: occurredAt,
		Payload:    raw,
	})
}

func (s *Service) ensurePaperAccount() error {
	if _, err := s.store.LatestAccountSnapshot(s.accountID); err == nil {
		return nil
	}
	now := time.Now()
	initialBalance := 1_000_000.0
	if s.replayPaper {
		initialBalance = replayPaperInitialBalance
	}
	item := TradingAccountSnapshot{
		AccountID:      s.accountID,
		Balance:        initialBalance,
		Available:      initialBalance,
		Margin:         0,
		FrozenCash:     0,
		Commission:     0,
		CloseProfit:    0,
		PositionProfit: 0,
		UpdatedAt:      now,
	}
	if err := s.store.SaveAccountSnapshot(item); err != nil {
		return err
	}
	s.mu.Lock()
	s.account = item
	s.mu.Unlock()
	return s.store.SaveSessionState(SessionState{
		AccountID:           s.accountID,
		Connected:           true,
		Authenticated:       true,
		LoggedIn:            true,
		SettlementConfirmed: true,
		UpdatedAt:           now,
	})
}

func (s *Service) submitPaperOrder(commandID string, req SubmitOrderRequest) (OrderRecord, error) {
	if !s.replayPaper && !s.livePaper {
		return s.submitImmediatePaperOrder(commandID, req)
	}
	s.paperMu.Lock()
	now := s.replayNowLocked()
	rec := OrderRecord{
		AccountID:           s.accountID,
		CommandID:           commandID,
		OrderRef:            commandID,
		ExchangeID:          req.ExchangeID,
		OrderSysID:          commandID,
		Symbol:              req.Symbol,
		Direction:           req.Direction,
		OffsetFlag:          req.OffsetFlag,
		LimitPrice:          req.LimitPrice,
		VolumeTotalOriginal: req.Volume,
		VolumeTraded:        0,
		VolumeCanceled:      0,
		OrderStatus:         "queued",
		SubmitStatus:        "accepted",
		StatusMsg:           "paper order accepted and waiting for market",
		InsertedAt:          now,
		UpdatedAt:           now,
	}
	if err := s.store.UpsertOrder(rec); err != nil {
		s.paperMu.Unlock()
		return rec, err
	}
	s.pending[rec.CommandID] = rec
	if _, _, err := s.recalculateReplayPaperStateLocked(now); err != nil {
		s.paperMu.Unlock()
		return rec, err
	}
	s.paperMu.Unlock()
	s.broadcast("trade_order_update", rec)
	return rec, nil
}

func (s *Service) submitImmediatePaperOrder(commandID string, req SubmitOrderRequest) (OrderRecord, error) {
	now := time.Now()
	rec := OrderRecord{
		AccountID:           s.accountID,
		CommandID:           commandID,
		OrderRef:            commandID,
		ExchangeID:          req.ExchangeID,
		OrderSysID:          commandID,
		Symbol:              req.Symbol,
		Direction:           req.Direction,
		OffsetFlag:          req.OffsetFlag,
		LimitPrice:          req.LimitPrice,
		VolumeTotalOriginal: req.Volume,
		VolumeTraded:        req.Volume,
		VolumeCanceled:      0,
		OrderStatus:         "all_traded",
		SubmitStatus:        "paper_filled",
		StatusMsg:           "paper order filled immediately",
		InsertedAt:          now,
		UpdatedAt:           now,
	}
	if err := s.store.UpsertOrder(rec); err != nil {
		return rec, err
	}
	tradeRec := TradeRecord{
		AccountID:  s.accountID,
		TradeID:    commandID,
		OrderRef:   rec.OrderRef,
		OrderSysID: rec.OrderSysID,
		ExchangeID: req.ExchangeID,
		Symbol:     req.Symbol,
		Direction:  req.Direction,
		OffsetFlag: req.OffsetFlag,
		Price:      req.LimitPrice,
		Volume:     req.Volume,
		TradeTime:  now,
		TradingDay: now.Format("20060102"),
		ReceivedAt: now,
	}
	if err := s.store.AppendTrade(tradeRec); err != nil {
		return rec, err
	}
	if err := s.applyImmediatePaperFill(tradeRec); err != nil {
		return rec, err
	}
	s.broadcast("trade_order_update", rec)
	s.broadcast("trade_trade_update", tradeRec)
	return rec, nil
}

func (s *Service) applyImmediatePaperFill(tr TradeRecord) error {
	account, err := s.store.LatestAccountSnapshot(s.accountID)
	if err != nil {
		return err
	}
	positions, err := s.store.ListPositions(s.accountID)
	if err != nil {
		return err
	}
	commission := paperCommission(tr)
	account.Commission += commission
	account.Balance -= commission
	account.Available -= commission
	positions = applyFilledTradeToPositions(positions, tr)
	margin := 0.0
	for _, item := range positions {
		margin += item.UseMargin
	}
	account.Margin = margin
	account.UpdatedAt = time.Now()
	account.PositionProfit = 0
	if account.Available < 0 {
		account.Available = 0
	}
	if err := s.store.ReplacePositions(s.accountID, positions); err != nil {
		return err
	}
	if err := s.store.SaveAccountSnapshot(account); err != nil {
		return err
	}
	s.mu.Lock()
	s.account = account
	s.positions = positions
	s.mu.Unlock()
	s.broadcast("trade_account_update", account)
	s.broadcast("trade_position_update", map[string]any{"items": positions})
	return nil
}

func (s *Service) cancelPaperOrder(req CancelOrderRequest, orderRec OrderRecord) (OrderRecord, error) {
	if !s.replayPaper && !s.livePaper {
		return orderRec, fmt.Errorf("paper orders are filled immediately and cannot be canceled")
	}
	s.paperMu.Lock()
	current, err := s.store.GetOrder(req.CommandID)
	if err != nil {
		s.paperMu.Unlock()
		return orderRec, err
	}
	switch strings.TrimSpace(current.OrderStatus) {
	case "all_traded", "canceled", "rejected":
		s.paperMu.Unlock()
		return current, ErrOrderAlreadyFinal
	}
	now := s.replayNowLocked()
	current.VolumeCanceled = current.VolumeTotalOriginal - current.VolumeTraded
	if current.VolumeCanceled < 0 {
		current.VolumeCanceled = 0
	}
	current.OrderStatus = "canceled"
	current.SubmitStatus = "accepted"
	current.StatusMsg = "paper order canceled"
	current.UpdatedAt = now
	if err := s.store.UpsertOrder(current); err != nil {
		s.paperMu.Unlock()
		return current, err
	}
	delete(s.pending, current.CommandID)
	if _, _, err := s.recalculateReplayPaperStateLocked(now); err != nil {
		s.paperMu.Unlock()
		return current, err
	}
	s.paperMu.Unlock()
	s.broadcast("trade_order_update", current)
	return current, nil
}

func (s *Service) ConsumeBusEvent(_ context.Context, ev bus.BusEvent) error {
	if !s.replayPaper || ev.Topic != bus.TopicTick {
		return nil
	}
	var tick quotes.TickEvent
	if err := json.Unmarshal(ev.Payload, &tick); err != nil {
		return fmt.Errorf("decode replay tick for paper trade failed: %w", err)
	}
	return s.ConsumePaperMarketTick(PaperMarketTick{
		Symbol:         tick.InstrumentID,
		ExchangeID:     tick.ExchangeID,
		TradingDay:     tick.TradingDay,
		ActionDay:      tick.ActionDay,
		UpdateTime:     tick.UpdateTime,
		UpdateMillisec: tick.UpdateMillisec,
		BidPrice1:      tick.BidPrice1,
		AskPrice1:      tick.AskPrice1,
	})
}

func (s *Service) ConsumePaperMarketTick(tick PaperMarketTick) error {
	if !s.paper {
		return nil
	}
	marketTS := parseDateTime(strings.TrimSpace(tick.TradingDay), strings.TrimSpace(tick.UpdateTime))
	if marketTS.IsZero() || strings.TrimSpace(tick.TradingDay) == "" || strings.TrimSpace(tick.UpdateTime) == "" {
		marketTS = time.Now()
	}
	return s.matchReplayTick(replayTick{
		InstrumentID: strings.TrimSpace(tick.Symbol),
		ExchangeID:   strings.TrimSpace(tick.ExchangeID),
		TradingDay:   strings.TrimSpace(tick.TradingDay),
		ActionDay:    strings.TrimSpace(tick.ActionDay),
		UpdateTime:   strings.TrimSpace(tick.UpdateTime),
		UpdateMS:     tick.UpdateMillisec,
		ReceivedAt:   marketTS,
		BidPrice1:    tick.BidPrice1,
		AskPrice1:    tick.AskPrice1,
	})
}

func (s *Service) loadPendingOrders() error {
	items, err := s.store.ListOpenOrders(s.accountID)
	if err != nil {
		return err
	}
	s.pending = make(map[string]OrderRecord, len(items))
	for _, item := range items {
		s.pending[item.CommandID] = item
	}
	return nil
}

func (s *Service) paperRiskState() (TradingAccountSnapshot, []PositionSnapshot, error) {
	if !s.replayPaper && !s.livePaper {
		account, err := s.Account()
		if err != nil {
			return TradingAccountSnapshot{}, nil, err
		}
		positions, err := s.Positions()
		return account, positions, err
	}
	s.paperMu.Lock()
	defer s.paperMu.Unlock()
	account, positions, err := s.recalculateReplayPaperStateLocked(time.Now())
	if err != nil {
		return TradingAccountSnapshot{}, nil, err
	}
	return account, applyPendingCloseReservations(positions, pendingOrderSlice(s.pending)), nil
}

func (s *Service) matchReplayTick(tick replayTick) error {
	s.paperMu.Lock()
	defer s.paperMu.Unlock()
	s.rememberReplayQuoteLocked(tick)
	orders := pendingOrderSlice(s.pending)
	sort.SliceStable(orders, func(i, j int) bool {
		if orders[i].InsertedAt.Equal(orders[j].InsertedAt) {
			return orders[i].CommandID < orders[j].CommandID
		}
		return orders[i].InsertedAt.Before(orders[j].InsertedAt)
	})
	now := tick.ReceivedAt
	if now.IsZero() {
		now = time.Now()
	}
	if len(orders) == 0 {
		return s.markReplayPaperToMarketLocked(now)
	}
	var filledOrders []OrderRecord
	var trades []TradeRecord
	for _, order := range orders {
		if !strings.EqualFold(order.Symbol, tick.InstrumentID) {
			continue
		}
		price, ok := marketableReplayPrice(order, tick)
		if !ok {
			continue
		}
		order.VolumeTraded = order.VolumeTotalOriginal - order.VolumeCanceled
		if order.VolumeTraded < 0 {
			order.VolumeTraded = 0
		}
		order.OrderStatus = "all_traded"
		order.SubmitStatus = "accepted"
		order.StatusMsg = "paper replay order filled by replay market"
		order.UpdatedAt = now
		tradeRec := TradeRecord{
			AccountID:  s.accountID,
			TradeID:    order.CommandID + "-fill",
			OrderRef:   order.OrderRef,
			OrderSysID: order.OrderSysID,
			ExchangeID: firstNonEmpty(order.ExchangeID, tick.ExchangeID),
			Symbol:     order.Symbol,
			Direction:  order.Direction,
			OffsetFlag: order.OffsetFlag,
			Price:      price,
			Volume:     order.VolumeTraded,
			TradeTime:  now,
			TradingDay: firstNonEmpty(strings.TrimSpace(tick.TradingDay), now.Format("20060102")),
			ReceivedAt: now,
		}
		if err := s.store.UpsertOrder(order); err != nil {
			return err
		}
		if err := s.store.AppendTrade(tradeRec); err != nil {
			return err
		}
		delete(s.pending, order.CommandID)
		filledOrders = append(filledOrders, order)
		trades = append(trades, tradeRec)
	}
	if len(trades) == 0 {
		return s.markReplayPaperToMarketLocked(now)
	}
	if _, _, err := s.recalculateReplayPaperStateLocked(now); err != nil {
		return err
	}
	for i := range filledOrders {
		s.broadcast("trade_order_update", filledOrders[i])
		s.broadcast("trade_trade_update", trades[i])
	}
	return nil
}

func (s *Service) recalculateReplayPaperState(now time.Time) error {
	s.paperMu.Lock()
	defer s.paperMu.Unlock()
	_, _, err := s.recalculateReplayPaperStateLocked(now)
	return err
}

func (s *Service) recalculateReplayPaperStateLocked(now time.Time) (TradingAccountSnapshot, []PositionSnapshot, error) {
	if !s.replayPaper {
		return TradingAccountSnapshot{}, nil, nil
	}
	if s.pending == nil {
		s.pending = make(map[string]OrderRecord)
	}
	openOrders, err := s.store.ListOpenOrders(s.accountID)
	if err != nil {
		return TradingAccountSnapshot{}, nil, err
	}
	s.pending = make(map[string]OrderRecord, len(openOrders))
	for _, item := range openOrders {
		s.pending[item.CommandID] = item
	}
	trades, err := s.store.ListTrades(s.accountID, 100000)
	if err != nil {
		return TradingAccountSnapshot{}, nil, err
	}
	sort.SliceStable(trades, func(i, j int) bool {
		if trades[i].TradeTime.Equal(trades[j].TradeTime) {
			return trades[i].TradeID < trades[j].TradeID
		}
		return trades[i].TradeTime.Before(trades[j].TradeTime)
	})
	account := TradingAccountSnapshot{
		AccountID:      s.accountID,
		Balance:        replayPaperInitialBalance,
		Available:      replayPaperInitialBalance,
		Margin:         0,
		FrozenCash:     0,
		Commission:     0,
		CloseProfit:    0,
		PositionProfit: 0,
		UpdatedAt:      now,
	}
	positions := make([]PositionSnapshot, 0)
	for _, tr := range trades {
		positions, account.CloseProfit = applyFilledTradeToPositionsWithProfit(positions, tr, account.CloseProfit)
		commission := paperCommission(tr)
		account.Commission += commission
		account.Balance -= commission
	}
	for _, item := range positions {
		account.Margin += item.UseMargin
		account.PositionProfit += replayPositionProfit(item, s.replayQuotes[item.Symbol])
	}
	for _, item := range s.pending {
		if item.OffsetFlag == "open" {
			account.FrozenCash += openOrderReserve(item)
		}
	}
	account.Available = account.Balance - account.Margin - account.FrozenCash
	if account.Available < 0 {
		account.Available = 0
	}
	if err := s.store.ReplacePositions(s.accountID, positions); err != nil {
		return TradingAccountSnapshot{}, nil, err
	}
	if err := s.store.SaveAccountSnapshot(account); err != nil {
		return TradingAccountSnapshot{}, nil, err
	}
	s.mu.Lock()
	s.account = account
	s.positions = positions
	s.mu.Unlock()
	s.broadcast("trade_account_update", account)
	s.broadcast("trade_position_update", map[string]any{"items": positions})
	return account, positions, nil
}

func (s *Service) rememberReplayQuoteLocked(tick replayTick) {
	symbol := strings.TrimSpace(tick.InstrumentID)
	if symbol == "" {
		return
	}
	lastTickAt := tick.ReceivedAt
	if lastTickAt.IsZero() {
		lastTickAt = time.Now()
	}
	s.replayQuotes[symbol] = replayQuote{
		BidPrice1:  tick.BidPrice1,
		AskPrice1:  tick.AskPrice1,
		LastTickAt: lastTickAt,
	}
}

func (s *Service) replayNowLocked() time.Time {
	if len(s.replayQuotes) == 0 {
		return time.Now()
	}
	var latest time.Time
	for _, q := range s.replayQuotes {
		if q.LastTickAt.IsZero() {
			continue
		}
		if latest.IsZero() || q.LastTickAt.After(latest) {
			latest = q.LastTickAt
		}
	}
	if latest.IsZero() {
		return time.Now()
	}
	return latest
}

func (s *Service) markReplayPaperToMarketLocked(now time.Time) error {
	account := s.account
	if account.AccountID == "" {
		return nil
	}
	account.PositionProfit = 0
	account.UpdatedAt = now
	for _, item := range s.positions {
		account.PositionProfit += replayPositionProfit(item, s.replayQuotes[item.Symbol])
	}
	s.mu.Lock()
	s.account = account
	s.mu.Unlock()
	s.broadcast("trade_account_update", account)
	return nil
}

func applyFilledTradeToPositions(items []PositionSnapshot, tr TradeRecord) []PositionSnapshot {
	out, _ := applyFilledTradeToPositionsWithProfit(items, tr, 0)
	return out
}

func applyFilledTradeToPositionsWithProfit(items []PositionSnapshot, tr TradeRecord, closeProfit float64) ([]PositionSnapshot, float64) {
	out := make([]PositionSnapshot, 0, len(items)+1)
	now := time.Now()
	wantDir := "long"
	closeDir := "short"
	if tr.Direction == "sell" {
		wantDir = "short"
		closeDir = "long"
	}
	remainingClose := tr.Volume
	if tr.OffsetFlag != "open" {
		for _, item := range items {
			if !strings.EqualFold(item.Symbol, tr.Symbol) || item.Direction != closeDir || remainingClose <= 0 {
				out = append(out, item)
				continue
			}
			used := remainingClose
			if item.Position < used {
				used = item.Position
			}
			item.Position -= used
			if item.TodayPosition >= used {
				item.TodayPosition -= used
			} else {
				item.YdPosition = paperMaxInt(item.YdPosition-(used-item.TodayPosition), 0)
				item.TodayPosition = 0
			}
			avgOpenCost := 0.0
			avgPositionCost := 0.0
			avgMargin := 0.0
			if item.Position > 0 {
				avgOpenCost = item.OpenCost / float64(item.Position)
				avgPositionCost = item.PositionCost / float64(item.Position)
				avgMargin = item.UseMargin / float64(item.Position)
			}
			item.OpenCost = paperMaxFloat(item.OpenCost-avgOpenCost*float64(used), 0)
			item.PositionCost = paperMaxFloat(item.PositionCost-avgPositionCost*float64(used), 0)
			item.UseMargin = paperMaxFloat(item.UseMargin-avgMargin*float64(used), 0)
			if closeDir == "long" {
				closeProfit += (tr.Price - avgPositionCost) * float64(used)
			} else {
				closeProfit += (avgPositionCost - tr.Price) * float64(used)
			}
			item.UpdatedAt = now
			remainingClose -= used
			if item.Position > 0 {
				out = append(out, item)
			}
		}
		items = out
		out = make([]PositionSnapshot, 0, len(items)+1)
	}
	inserted := false
	for _, item := range items {
		if strings.EqualFold(item.Symbol, tr.Symbol) && item.Direction == wantDir {
			if tr.OffsetFlag == "open" {
				item.Position += tr.Volume
				item.TodayPosition += tr.Volume
				item.UseMargin += float64(tr.Volume) * tr.Price
				item.OpenCost += float64(tr.Volume) * tr.Price
				item.PositionCost += float64(tr.Volume) * tr.Price
				item.UpdatedAt = now
			}
			out = append(out, item)
			inserted = true
			continue
		}
		out = append(out, item)
	}
	if tr.OffsetFlag == "open" && !inserted {
		out = append(out, PositionSnapshot{
			AccountID:     tr.AccountID,
			Symbol:        tr.Symbol,
			Exchange:      tr.ExchangeID,
			Direction:     wantDir,
			HedgeFlag:     "speculation",
			TodayPosition: tr.Volume,
			Position:      tr.Volume,
			OpenCost:      float64(tr.Volume) * tr.Price,
			PositionCost:  float64(tr.Volume) * tr.Price,
			UseMargin:     float64(tr.Volume) * tr.Price,
			UpdatedAt:     now,
		})
	}
	return out, closeProfit
}

func pendingOrderSlice(items map[string]OrderRecord) []OrderRecord {
	out := make([]OrderRecord, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func applyPendingCloseReservations(items []PositionSnapshot, orders []OrderRecord) []PositionSnapshot {
	out := make([]PositionSnapshot, len(items))
	copy(out, items)
	for _, order := range orders {
		if order.OffsetFlag == "open" {
			continue
		}
		closeDir := "long"
		if order.Direction == "buy" {
			closeDir = "short"
		}
		remaining := order.VolumeTotalOriginal - order.VolumeTraded - order.VolumeCanceled
		if remaining <= 0 {
			continue
		}
		for i := range out {
			if !strings.EqualFold(out[i].Symbol, order.Symbol) || out[i].Direction != closeDir || remaining <= 0 {
				continue
			}
			used := remaining
			if out[i].Position < used {
				used = out[i].Position
			}
			out[i].Position -= used
			if out[i].TodayPosition >= used {
				out[i].TodayPosition -= used
			} else {
				out[i].YdPosition = paperMaxInt(out[i].YdPosition-(used-out[i].TodayPosition), 0)
				out[i].TodayPosition = 0
			}
			remaining -= used
		}
	}
	filtered := make([]PositionSnapshot, 0, len(out))
	for _, item := range out {
		if item.Position > 0 {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func replayPositionProfit(pos PositionSnapshot, quote replayQuote) float64 {
	if pos.Position <= 0 || pos.PositionCost <= 0 {
		return 0
	}
	cost := pos.PositionCost / float64(pos.Position)
	mark := replayMarkPrice(pos.Direction, quote, cost)
	switch pos.Direction {
	case "short":
		return (cost - mark) * float64(pos.Position)
	default:
		return (mark - cost) * float64(pos.Position)
	}
}

func replayMarkPrice(direction string, quote replayQuote, fallback float64) float64 {
	switch direction {
	case "short":
		if quote.AskPrice1 > 0 {
			return quote.AskPrice1
		}
		if quote.BidPrice1 > 0 {
			return quote.BidPrice1
		}
	default:
		if quote.BidPrice1 > 0 {
			return quote.BidPrice1
		}
		if quote.AskPrice1 > 0 {
			return quote.AskPrice1
		}
	}
	return fallback
}

func marketableReplayPrice(order OrderRecord, tick replayTick) (float64, bool) {
	switch order.Direction {
	case "buy":
		if tick.AskPrice1 > 0 && order.LimitPrice >= tick.AskPrice1 {
			return tick.AskPrice1, true
		}
	case "sell":
		if tick.BidPrice1 > 0 && order.LimitPrice <= tick.BidPrice1 {
			return tick.BidPrice1, true
		}
	}
	return 0, false
}

func openOrderReserve(order OrderRecord) float64 {
	remaining := order.VolumeTotalOriginal - order.VolumeTraded - order.VolumeCanceled
	if remaining <= 0 {
		return 0
	}
	return float64(remaining) * order.LimitPrice
}

func paperCommission(tr TradeRecord) float64 {
	return tr.Price * float64(tr.Volume) * 0.0001
}

func firstNonEmpty(values ...string) string {
	for _, item := range values {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}

func boolPtr(v bool) *bool { return &v }

func paperMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func paperMaxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
