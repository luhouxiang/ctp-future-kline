package trade

import (
	"context"
	"testing"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/order"
)

func TestValidateSubmitAllowsManualOrder(t *testing.T) {
	t.Parallel()

	commandID, err := ValidateSubmit(
		context.Background(),
		TradeStatus{TraderFront: true, TraderLogin: true, SettlementConfirmed: true},
		config.TradeConfig{MaxOrderVolume: 10},
		TradingAccountSnapshot{Available: 100000},
		nil,
		SubmitOrderRequest{
			ExchangeID: "SHFE",
			Symbol:     "ag2605",
			Direction:  "buy",
			OffsetFlag: "open",
			LimitPrice: 22847,
			Volume:     2,
			Reason:     "manual",
		},
	)
	if err != nil {
		t.Fatalf("ValidateSubmit() error = %v", err)
	}
	if commandID == "" {
		t.Fatal("ValidateSubmit() commandID = empty")
	}
}

func TestValidateSubmitBlocksReplayContext(t *testing.T) {
	t.Parallel()

	ctx := order.WithReplayMeta(context.Background(), order.ReplayMeta{EventID: "ev-1", ReplayTaskID: "task-1"})
	_, err := ValidateSubmit(
		ctx,
		TradeStatus{TraderFront: true, TraderLogin: true, SettlementConfirmed: true},
		config.TradeConfig{MaxOrderVolume: 10},
		TradingAccountSnapshot{Available: 100000},
		nil,
		SubmitOrderRequest{
			Symbol:     "ag2605",
			Direction:  "buy",
			OffsetFlag: "open",
			LimitPrice: 22847,
			Volume:     1,
			Reason:     "manual",
		},
	)
	if err == nil {
		t.Fatal("ValidateSubmit() error = nil, want replay blocked")
	}
}

func TestValidateSubmitRequiresSymbol(t *testing.T) {
	t.Parallel()

	_, err := ValidateSubmit(
		context.Background(),
		TradeStatus{TraderFront: true, TraderLogin: true, SettlementConfirmed: true},
		config.TradeConfig{MaxOrderVolume: 10},
		TradingAccountSnapshot{Available: 100000},
		nil,
		SubmitOrderRequest{
			ExchangeID: "SHFE",
			Direction:  "sell",
			OffsetFlag: "open",
			LimitPrice: 17150,
			Volume:     1,
			Reason:     "manual",
		},
	)
	if err == nil || err.Error() != "symbol is required" {
		t.Fatalf("ValidateSubmit() error = %v, want symbol is required", err)
	}
}

func TestValidateSubmitRequiresExchangeID(t *testing.T) {
	t.Parallel()

	_, err := ValidateSubmit(
		context.Background(),
		TradeStatus{TraderFront: true, TraderLogin: true, SettlementConfirmed: true},
		config.TradeConfig{MaxOrderVolume: 10},
		TradingAccountSnapshot{Available: 100000},
		nil,
		SubmitOrderRequest{
			Symbol:     "ag2605",
			Direction:  "sell",
			OffsetFlag: "open",
			LimitPrice: 17150,
			Volume:     1,
			Reason:     "manual",
		},
	)
	if err == nil || err.Error() != "exchange_id is required" {
		t.Fatalf("ValidateSubmit() error = %v, want exchange_id is required", err)
	}
}

func TestValidateCancelRejectsFinalOrder(t *testing.T) {
	t.Parallel()

	err := ValidateCancel(
		context.Background(),
		CancelOrderRequest{CommandID: "cmd-1", Reason: "manual_cancel"},
		OrderRecord{OrderStatus: "canceled"},
	)
	if err != ErrOrderAlreadyFinal {
		t.Fatalf("ValidateCancel() error = %v, want %v", err, ErrOrderAlreadyFinal)
	}
}
