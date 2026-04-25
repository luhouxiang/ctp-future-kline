package trade

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/order"
)

var (
	ErrTradeServiceOffline = errors.New("trade service offline")
	ErrOrderAlreadyFinal   = errors.New("order already final")
)

func ValidateSubmit(ctx context.Context, status TradeStatus, cfg config.TradeConfig, account TradingAccountSnapshot, positions []PositionSnapshot, req SubmitOrderRequest) (string, error) {
	commandID := mustCommandID("cmd")
	if err := order.EnsureReplaySafe(ctx, commandID); err != nil {
		return commandID, err
	}
	if !status.TraderFront || !status.TraderLogin || !status.SettlementConfirmed {
		return commandID, ErrTradeServiceOffline
	}
	switch strings.TrimSpace(req.Reason) {
	case "manual", "line_order":
	default:
		return commandID, errors.New("only manual or line_order orders are allowed")
	}
	if !symbolAllowed(cfg.AllowedSymbols, req.Symbol) {
		return commandID, fmt.Errorf("symbol %s not allowed", req.Symbol)
	}
	if req.Volume <= 0 {
		return commandID, errors.New("volume must be > 0")
	}
	if req.Volume > cfg.MaxOrderVolume {
		return commandID, fmt.Errorf("volume exceeds max_order_volume %d", cfg.MaxOrderVolume)
	}
	if strings.TrimSpace(req.Symbol) == "" {
		return commandID, errors.New("symbol is required")
	}
	if strings.TrimSpace(req.ExchangeID) == "" {
		return commandID, errors.New("exchange_id is required")
	}
	if req.LimitPrice <= 0 {
		return commandID, errors.New("limit_price must be > 0")
	}
	if req.Direction != "buy" && req.Direction != "sell" {
		return commandID, errors.New("direction must be buy or sell")
	}
	switch req.OffsetFlag {
	case "open", "close", "close_today", "close_yesterday":
	default:
		return commandID, errors.New("offset_flag must be open/close/close_today/close_yesterday")
	}
	if req.OffsetFlag != "open" && closableVolume(positions, req.Symbol, req.Direction) < req.Volume {
		return commandID, errors.New("close volume exceeds available position")
	}
	if req.OffsetFlag == "open" && account.Available <= 0 {
		return commandID, errors.New("account available funds <= 0")
	}
	return commandID, nil
}

func ValidateCancel(ctx context.Context, req CancelOrderRequest, orderRec OrderRecord) error {
	if err := order.EnsureReplaySafe(ctx, req.CommandID); err != nil {
		return err
	}
	if strings.TrimSpace(req.Reason) != "manual_cancel" {
		return errors.New("only manual cancel is allowed")
	}
	switch strings.TrimSpace(orderRec.OrderStatus) {
	case "all_traded", "canceled", "rejected":
		return ErrOrderAlreadyFinal
	}
	return nil
}

func closableVolume(items []PositionSnapshot, symbol string, direction string) int {
	wantDir := "long"
	if direction == "buy" {
		wantDir = "short"
	}
	total := 0
	for _, item := range items {
		if !strings.EqualFold(item.Symbol, symbol) || item.Direction != wantDir {
			continue
		}
		total += item.Position
	}
	return total
}

func symbolAllowed(allowed []string, symbol string) bool {
	if len(allowed) == 0 {
		return true
	}
	symbol = strings.TrimSpace(strings.ToLower(symbol))
	for _, item := range allowed {
		if strings.TrimSpace(strings.ToLower(item)) == symbol {
			return true
		}
	}
	return false
}

func isNoopCancel(req CancelOrderRequest) bool {
	return strings.TrimSpace(req.OrderSysID) == "" && strings.TrimSpace(req.OrderRef) == "" && (req.FrontID == 0 || req.SessionID == 0)
}
