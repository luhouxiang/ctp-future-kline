// service.go 是实盘交易子系统的服务层。
// 它对接 CTPGateway、维护账户/持仓/委托/成交快照、执行周期性查询，并通过事件把交易状态同步给 Web 控制面。
package trade

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

type Service struct {
	// cfg 保存交易子系统本身的启用和风控配置。
	cfg config.TradeConfig
	// ctpCfg 保存交易侧复用的 CTP 接入参数。
	ctpCfg config.CTPConfig
	// store 是交易子系统的持久化存储。
	store *Store
	// gateway 是底层 CTP 交易网关抽象。
	gateway Gateway
	// accountID 是系统内部统一使用的账户标识。
	accountID string
	// paper 表示当前服务是否为模拟交易后端。
	paper bool

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
	busLog      *bus.FileLog
	ctx         context.Context
	cancel      context.CancelFunc
	startMu     sync.Mutex
	started     bool
	closeOnce   sync.Once
	queueHandle *queuewatch.QueueHandle
	queueCap    int
}

func NewService(cfg config.TradeConfig, ctpCfg config.CTPConfig, dsn string, registry *queuewatch.Registry) (*Service, error) {
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
	s := &Service{
		cfg:       cfg,
		ctpCfg:    ctpCfg,
		store:     store,
		gateway:   NewCTPGateway(ctpCfg, cfg, registry),
		accountID: cfg.AccountID,
		subs:      make(map[chan EventEnvelope]struct{}),
		queueCap:  queueCfg.TradeEventCapacity,
		status:    TradeStatus{Enabled: cfg.IsEnabled(), AccountID: cfg.AccountID, UpdatedAt: time.Now()},
		ctx:       ctx,
		cancel:    cancel,
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
		cfg:       cfg,
		store:     store,
		accountID: accountID,
		paper:     true,
		subs:      make(map[chan EventEnvelope]struct{}),
		queueCap:  queueCfg.TradeEventCapacity,
		status: TradeStatus{
			Enabled:             true,
			AccountID:           accountID,
			TraderFront:         true,
			TraderLogin:         true,
			SettlementConfirmed: true,
			UpdatedAt:           time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
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
	items, err := s.gateway.RefreshPositions()
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
	items, err := s.gateway.RefreshOrders()
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
	items, err := s.gateway.RefreshTrades()
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

func (s *Service) SubmitOrder(ctx context.Context, req SubmitOrderRequest) (OrderRecord, error) {
	status := s.Status()
	account, _ := s.Account()
	positions, _ := s.Positions()
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
	if isNoopCancel(req) {
		return OrderRecord{CommandID: req.CommandID, AccountID: s.accountID}, fmt.Errorf("cancel request missing order identifiers")
	}
	orderRec, err := s.store.GetOrder(req.CommandID)
	if err != nil {
		return OrderRecord{CommandID: req.CommandID, AccountID: s.accountID}, err
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
		err := fmt.Errorf("paper orders are filled immediately and cannot be canceled")
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
	queryTicker := time.NewTicker(time.Duration(s.cfg.QueryPollIntervalMS) * time.Millisecond)
	defer queryTicker.Stop()
	positionTicker := time.NewTicker(time.Duration(s.cfg.PositionSyncIntervalMS) * time.Millisecond)
	defer positionTicker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-queryTicker.C:
			s.setStatusFromGateway()
			_, _ = s.RefreshAccount()
			_, _ = s.RefreshOrders()
			_, _ = s.RefreshTrades()
			_ = s.saveSessionState()
			s.broadcast("trade_status_update", s.Status())
		case <-positionTicker.C:
			_, _ = s.RefreshPositions()
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
	item := TradingAccountSnapshot{
		AccountID:      s.accountID,
		Balance:        1_000_000,
		Available:      1_000_000,
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
	if err := s.applyPaperFill(tradeRec); err != nil {
		return rec, err
	}
	s.broadcast("trade_order_update", rec)
	s.broadcast("trade_trade_update", tradeRec)
	return rec, nil
}

func (s *Service) applyPaperFill(tr TradeRecord) error {
	account, err := s.store.LatestAccountSnapshot(s.accountID)
	if err != nil {
		return err
	}
	positions, err := s.store.ListPositions(s.accountID)
	if err != nil {
		return err
	}
	commission := tr.Price * float64(tr.Volume) * 0.0001
	account.Commission += commission
	account.Balance -= commission
	account.Available -= commission
	positions = mergePaperPosition(positions, tr)
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

func mergePaperPosition(items []PositionSnapshot, tr TradeRecord) []PositionSnapshot {
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
			item.UseMargin = paperMaxFloat(item.UseMargin-float64(used)*tr.Price, 0)
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
	return out
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
