package trade

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
)

type laneThrottle struct {
	name      string
	interval  time.Duration
	step      time.Duration
	max       time.Duration
	timeout   time.Duration
	queryType string
}

func newLaneThrottle(name string, queryType string, cfg config.TradeConfig) laneThrottle {
	base := time.Duration(cfg.QueryBaseIntervalMS) * time.Millisecond
	if base <= 0 {
		base = time.Second
	}
	step := time.Duration(cfg.QueryBackoffStepMS) * time.Millisecond
	if step <= 0 {
		step = 200 * time.Millisecond
	}
	max := time.Duration(cfg.QueryMaxIntervalMS) * time.Millisecond
	if max <= 0 {
		max = 3 * time.Second
	}
	timeout := time.Duration(cfg.QueryTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return laneThrottle{
		name:      name,
		interval:  base,
		step:      step,
		max:       max,
		timeout:   timeout,
		queryType: queryType,
	}
}

func (l *laneThrottle) onFlowControl(err error, instrumentID string, exchangeID string) {
	if err == nil {
		return
	}
	if !isFlowControlError(err) {
		return
	}
	old := l.interval
	l.interval += l.step
	if l.interval > l.max {
		l.interval = l.max
	}
	logger.Warn(
		"trade query lane throttle increased",
		"lane", l.name,
		"query_type", l.queryType,
		"instrument_id", strings.TrimSpace(instrumentID),
		"exchange_id", strings.TrimSpace(exchangeID),
		"old_throttle_ms", old.Milliseconds(),
		"new_throttle_ms", l.interval.Milliseconds(),
		"error", err.Error(),
	)
}

func (s *Service) startQueryGateways() error {
	if s.paper {
		return nil
	}
	for _, item := range []struct {
		name string
		gw   *CTPGateway
	}{
		{name: "auto_pos", gw: s.autoPosGateway},
		{name: "fee_lane", gw: s.feeGateway},
		{name: "margin_lane", gw: s.marginGateway},
	} {
		if item.gw == nil {
			return fmt.Errorf("%s gateway is nil", item.name)
		}
		if err := item.gw.Start(); err != nil {
			return fmt.Errorf("start %s gateway failed: %w", item.name, err)
		}
	}
	return nil
}

func (s *Service) runAutoPosLane() {
	lane := newLaneThrottle("auto_pos", "positions", s.cfg)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		items, err := s.autoPosGateway.RefreshPositions()
		s.auditQuery("positions_lane", err)
		if err != nil {
			s.logLaneError(lane, err, "", "")
			lane.onFlowControl(err, "", "")
		} else {
			s.applyPositions(items)
		}
		if s.shouldAutoPosQueryOrders() {
			orders, err := s.autoPosGateway.RefreshOrders()
			s.auditQuery("orders_bootstrap", err)
			if err != nil {
				s.logLaneError(lane, err, "", "")
				lane.onFlowControl(err, "", "")
			} else {
				s.applyOrders(orders)
			}
		}
		if s.shouldAutoPosQueryTrades() {
			trades, err := s.autoPosGateway.RefreshTrades()
			s.auditQuery("trades_bootstrap", err)
			if err != nil {
				s.logLaneError(lane, err, "", "")
				lane.onFlowControl(err, "", "")
			} else {
				s.applyTrades(trades)
			}
		}
		if !sleepOrDone(s.ctx, lane.interval) {
			return
		}
	}
}

func (s *Service) runFeeLane() {
	lane := newLaneThrottle("fee_lane", "commission_or_orders", s.cfg)
	lastTradingDay := ""
	pendingFill := make([]instrumentKey, 0, 256)
	nextRebuildAt := time.Time{}
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		tradingDay := s.currentTradingDay()
		now := time.Now()
		if tradingDay != "" && tradingDay != lastTradingDay {
			lastTradingDay = tradingDay
			items, err := s.buildMissingCommissionQueue(tradingDay)
			if err != nil {
				s.logLaneError(lane, err, "", "")
			} else {
				pendingFill = items
				nextRebuildAt = now.Add(60 * time.Second)
				logger.Info("commission fill queue prepared", "trading_day", tradingDay, "missing_count", len(pendingFill))
			}
		} else if tradingDay != "" && len(pendingFill) == 0 && (nextRebuildAt.IsZero() || now.After(nextRebuildAt)) {
			items, err := s.buildMissingCommissionQueue(tradingDay)
			if err != nil {
				s.logLaneError(lane, err, "", "")
			} else {
				pendingFill = items
				nextRebuildAt = now.Add(60 * time.Second)
				if len(pendingFill) > 0 {
					logger.Info("commission fill queue refreshed", "trading_day", tradingDay, "missing_count", len(pendingFill))
				}
			}
		}
		if len(pendingFill) > 0 {
			item := pendingFill[0]
			rates, err := s.feeGateway.QueryCommissionRate(item.InstrumentID, item.ExchangeID)
			s.auditQuery("commission_rate", err)
			if err != nil {
				s.logLaneError(lane, err, item.InstrumentID, item.ExchangeID)
				lane.onFlowControl(err, item.InstrumentID, item.ExchangeID)
			} else {
				if _, upErr := s.rateCatalog.upsertCommissionRates(lastTradingDay, rates); upErr != nil {
					s.logLaneError(lane, upErr, item.InstrumentID, item.ExchangeID)
				} else {
					pendingFill = pendingFill[1:]
				}
			}
		} else {
			s.markFeeOrdersTakenByFeeLane()
			items, err := s.feeGateway.RefreshOrders()
			s.auditQuery("orders_lane", err)
			if err != nil {
				s.logLaneError(lane, err, "", "")
				lane.onFlowControl(err, "", "")
			} else {
				s.applyOrders(items)
			}
		}
		if !sleepOrDone(s.ctx, lane.interval) {
			return
		}
	}
}

func (s *Service) runMarginLane() {
	lane := newLaneThrottle("margin_lane", "margin_or_trades", s.cfg)
	lastTradingDay := ""
	pendingFill := make([]instrumentKey, 0, 256)
	nextRebuildAt := time.Time{}
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		tradingDay := s.currentTradingDay()
		now := time.Now()
		if tradingDay != "" && tradingDay != lastTradingDay {
			lastTradingDay = tradingDay
			items, err := s.buildMissingMarginQueue(tradingDay)
			if err != nil {
				s.logLaneError(lane, err, "", "")
			} else {
				pendingFill = items
				nextRebuildAt = now.Add(60 * time.Second)
				logger.Info("margin fill queue prepared", "trading_day", tradingDay, "missing_count", len(pendingFill))
			}
		} else if tradingDay != "" && len(pendingFill) == 0 && (nextRebuildAt.IsZero() || now.After(nextRebuildAt)) {
			items, err := s.buildMissingMarginQueue(tradingDay)
			if err != nil {
				s.logLaneError(lane, err, "", "")
			} else {
				pendingFill = items
				nextRebuildAt = now.Add(60 * time.Second)
				if len(pendingFill) > 0 {
					logger.Info("margin fill queue refreshed", "trading_day", tradingDay, "missing_count", len(pendingFill))
				}
			}
		}
		if len(pendingFill) > 0 {
			item := pendingFill[0]
			rates, err := s.marginGateway.QueryMarginRate(item.InstrumentID, item.ExchangeID)
			s.auditQuery("margin_rate", err)
			if err != nil {
				s.logLaneError(lane, err, item.InstrumentID, item.ExchangeID)
				lane.onFlowControl(err, item.InstrumentID, item.ExchangeID)
			} else {
				if _, upErr := s.rateCatalog.upsertMarginRates(lastTradingDay, rates); upErr != nil {
					s.logLaneError(lane, upErr, item.InstrumentID, item.ExchangeID)
				} else {
					pendingFill = pendingFill[1:]
				}
			}
		} else {
			s.markTradesTakenByMarginLane()
			items, err := s.marginGateway.RefreshTrades()
			s.auditQuery("trades_lane", err)
			if err != nil {
				s.logLaneError(lane, err, "", "")
				lane.onFlowControl(err, "", "")
			} else {
				s.applyTrades(items)
			}
		}
		if !sleepOrDone(s.ctx, lane.interval) {
			return
		}
	}
}

func (s *Service) shouldAutoPosQueryOrders() bool {
	s.laneStateMu.RLock()
	defer s.laneStateMu.RUnlock()
	return !s.feeOrdersByFeeLane
}

func (s *Service) shouldAutoPosQueryTrades() bool {
	s.laneStateMu.RLock()
	defer s.laneStateMu.RUnlock()
	return !s.tradesByMarginLane
}

func (s *Service) markFeeOrdersTakenByFeeLane() {
	s.laneStateMu.Lock()
	s.feeOrdersByFeeLane = true
	s.laneStateMu.Unlock()
}

func (s *Service) markTradesTakenByMarginLane() {
	s.laneStateMu.Lock()
	s.tradesByMarginLane = true
	s.laneStateMu.Unlock()
}

func (s *Service) currentTradingDay() string {
	if s.tradeOpGateway != nil {
		day := strings.TrimSpace(s.tradeOpGateway.Status().TradingDay)
		if day != "" {
			return day
		}
	}
	if s.gateway != nil {
		return strings.TrimSpace(s.gateway.Status().TradingDay)
	}
	return ""
}

func (s *Service) buildMissingCommissionQueue(tradingDay string) ([]instrumentKey, error) {
	if s.rateCatalog == nil {
		return nil, nil
	}
	all, err := s.rateCatalog.listInstrumentsByTradingDay(tradingDay)
	if err != nil {
		return nil, err
	}
	missing, err := s.rateCatalog.missingCommissionInstruments(tradingDay, all)
	if err != nil {
		return nil, err
	}
	return missing, nil
}

func (s *Service) buildMissingMarginQueue(tradingDay string) ([]instrumentKey, error) {
	if s.rateCatalog == nil {
		return nil, nil
	}
	all, err := s.rateCatalog.listInstrumentsByTradingDay(tradingDay)
	if err != nil {
		return nil, err
	}
	missing, err := s.rateCatalog.missingMarginInstruments(tradingDay, all)
	if err != nil {
		return nil, err
	}
	return missing, nil
}

func prioritizeProbeSymbol(items []instrumentKey, probe string) []instrumentKey {
	probe = strings.ToLower(strings.TrimSpace(probe))
	if probe == "" || len(items) <= 1 {
		return items
	}
	idx := -1
	for i, item := range items {
		if strings.ToLower(strings.TrimSpace(item.InstrumentID)) == probe {
			idx = i
			break
		}
	}
	if idx <= 0 {
		return items
	}
	out := make([]instrumentKey, 0, len(items))
	out = append(out, items[idx])
	out = append(out, items[:idx]...)
	out = append(out, items[idx+1:]...)
	return out
}

func (s *Service) applyPositions(items []PositionSnapshot) {
	if err := s.store.ReplacePositions(s.accountID, items); err != nil {
		s.logLaneError(newLaneThrottle("auto_pos", "positions", s.cfg), err, "", "")
		return
	}
	s.mu.Lock()
	s.positions = items
	s.mu.Unlock()
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_position_update", map[string]any{"items": items})
}

func (s *Service) applyOrders(items []OrderRecord) {
	for _, item := range items {
		if item.CommandID == "" {
			item.CommandID = mustCommandID("qry")
		}
		if err := s.store.UpsertOrder(item); err != nil {
			s.logLaneError(newLaneThrottle("fee_lane", "orders", s.cfg), err, item.Symbol, item.ExchangeID)
			return
		}
	}
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_order_update", map[string]any{"items": items})
}

func (s *Service) applyTrades(items []TradeRecord) {
	for _, item := range items {
		if err := s.store.AppendTrade(item); err != nil {
			s.logLaneError(newLaneThrottle("margin_lane", "trades", s.cfg), err, item.Symbol, item.ExchangeID)
			return
		}
	}
	s.setStatus(func(st *TradeStatus) { st.LastQueryAt = time.Now() })
	s.broadcast("trade_trade_update", map[string]any{"items": items})
}

func (s *Service) logLaneError(lane laneThrottle, err error, instrumentID string, exchangeID string) {
	code := extractCTPErrorCode(err)
	reqID := extractReqID(err)
	logger.Error(
		"trade query lane error",
		"lane", lane.name,
		"query_type", lane.queryType,
		"req_id", reqID,
		"instrument_id", strings.TrimSpace(instrumentID),
		"exchange_id", strings.TrimSpace(exchangeID),
		"error_id", code,
		"error_msg", err.Error(),
		"throttle_ms", lane.interval.Milliseconds(),
	)
}

func extractReqID(err error) int {
	if err == nil {
		return 0
	}
	msg := err.Error()
	const marker = "req_id="
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return 0
	}
	tail := msg[idx+len(marker):]
	end := strings.IndexAny(tail, " ,:")
	if end < 0 {
		end = len(tail)
	}
	v, _ := strconv.Atoi(strings.TrimSpace(tail[:end]))
	return v
}

func extractCTPErrorCode(err error) int {
	if err == nil {
		return 0
	}
	msg := err.Error()
	if strings.Contains(msg, "failed: -2") {
		return -2
	}
	if strings.Contains(msg, "failed: -3") {
		return -3
	}
	const marker = "ctp error "
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return 0
	}
	tail := msg[idx+len(marker):]
	end := strings.Index(tail, ":")
	if end < 0 {
		end = len(tail)
	}
	code, _ := strconv.Atoi(strings.TrimSpace(tail[:end]))
	return code
}

func isFlowControlError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "failed: -2") || strings.Contains(msg, "failed: -3") {
		return true
	}
	if strings.Contains(msg, "查询未就绪") || strings.Contains(msg, "每秒发送请求数超过许可数") || strings.Contains(msg, "未处理请求超过许可数") {
		return true
	}
	if strings.Contains(msg, "too frequent") || strings.Contains(msg, "flow") {
		return true
	}
	return false
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = time.Second
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func withTradeFlowSuffix(cfg config.CTPConfig, suffix string) config.CTPConfig {
	out := cfg
	base := strings.TrimSpace(cfg.FlowPath)
	if base == "" {
		base = "."
	}
	out.FlowPath = filepath.Join(base, "trade_"+strings.TrimSpace(suffix))
	return out
}
