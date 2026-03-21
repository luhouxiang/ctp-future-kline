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

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/config"
	"ctp-go-demo/internal/logger"
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
	busLog *bus.FileLog
}

func NewService(cfg config.TradeConfig, ctpCfg config.CTPConfig, dsn string) (*Service, error) {
	store, err := NewStore(dsn)
	if err != nil {
		return nil, err
	}
	if err := store.UpsertTradeAccount(cfg.AccountID, ctpCfg.BrokerID, ctpCfg.UserID); err != nil {
		return nil, err
	}
	s := &Service{
		cfg:       cfg,
		ctpCfg:    ctpCfg,
		store:     store,
		gateway:   NewCTPGateway(ctpCfg, cfg),
		accountID: cfg.AccountID,
		subs:      make(map[chan EventEnvelope]struct{}),
		status:    TradeStatus{Enabled: cfg.IsEnabled(), AccountID: cfg.AccountID, UpdatedAt: time.Now()},
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

func (s *Service) Close() error {
	if s.gateway != nil {
		_ = s.gateway.Close()
	}
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *Service) Start() error {
	if err := s.gateway.Start(); err != nil {
		s.setStatus(func(st *TradeStatus) { st.LastError = err.Error() })
		return err
	}
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
	ch := make(chan EventEnvelope, 64)
	s.subsMu.Lock()
	s.subs[ch] = struct{}{}
	s.subsMu.Unlock()
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
	queryTicker := time.NewTicker(time.Duration(s.cfg.QueryPollIntervalMS) * time.Millisecond)
	defer queryTicker.Stop()
	positionTicker := time.NewTicker(time.Duration(s.cfg.PositionSyncIntervalMS) * time.Millisecond)
	defer positionTicker.Stop()
	for {
		select {
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
		select {
		case ch <- payload:
		default:
		}
	}
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
