package trade

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/testmysql"
)

func TestResetPaperReplayClearsTradeStateAndRestoresInitialBalance(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc := newReplayPaperServiceForTest(t, dsn)

	now := time.Now()
	if err := svc.store.SaveAccountSnapshot(TradingAccountSnapshot{
		AccountID:      svc.accountID,
		Balance:        88888,
		Available:      77777,
		Margin:         1111,
		FrozenCash:     2222,
		Commission:     33,
		CloseProfit:    44,
		PositionProfit: 55,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("save old account snapshot failed: %v", err)
	}
	if err := svc.store.ReplacePositions(svc.accountID, []PositionSnapshot{{
		AccountID:     svc.accountID,
		Symbol:        "rb2505",
		Exchange:      "SHFE",
		Direction:     "long",
		HedgeFlag:     "speculation",
		TodayPosition: 2,
		Position:      2,
		OpenCost:      200,
		PositionCost:  200,
		UseMargin:     200,
		UpdatedAt:     now,
	}}); err != nil {
		t.Fatalf("replace positions failed: %v", err)
	}
	if err := svc.store.UpsertOrder(OrderRecord{
		AccountID:           svc.accountID,
		CommandID:           "cmd-old",
		OrderRef:            "cmd-old",
		OrderSysID:          "cmd-old",
		ExchangeID:          "SHFE",
		Symbol:              "rb2505",
		Direction:           "buy",
		OffsetFlag:          "open",
		LimitPrice:          100,
		VolumeTotalOriginal: 2,
		VolumeTraded:        1,
		OrderStatus:         "queued",
		SubmitStatus:        "accepted",
		StatusMsg:           "old",
		InsertedAt:          now,
		UpdatedAt:           now,
	}); err != nil {
		t.Fatalf("upsert old order failed: %v", err)
	}
	if err := svc.store.AppendTrade(TradeRecord{
		AccountID:  svc.accountID,
		TradeID:    "trade-old",
		OrderRef:   "cmd-old",
		OrderSysID: "cmd-old",
		ExchangeID: "SHFE",
		Symbol:     "rb2505",
		Direction:  "buy",
		OffsetFlag: "open",
		Price:      100,
		Volume:     1,
		TradeTime:  now,
		TradingDay: now.Format("20060102"),
		ReceivedAt: now,
	}); err != nil {
		t.Fatalf("append old trade failed: %v", err)
	}
	if _, err := svc.store.AppendCommandAudit(OrderCommandAudit{
		AccountID:   svc.accountID,
		CommandID:   "cmd-old",
		CommandType: CommandTypeSubmit,
		Symbol:      "rb2505",
		RiskStatus:  RiskStatusAllowed,
		Request:     map[string]any{"k": "v"},
		Response:    map[string]any{"ok": true},
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("append command audit failed: %v", err)
	}
	if err := svc.store.AppendQueryAudit(QueryAudit{
		AccountID: svc.accountID,
		QueryType: "account",
		Status:    QueryStatusOK,
		Detail:    "ok",
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("append query audit failed: %v", err)
	}
	if err := svc.store.SaveSessionState(SessionState{
		AccountID:           svc.accountID,
		FrontID:             1,
		SessionID:           2,
		NextOrderRef:        3,
		Connected:           true,
		Authenticated:       true,
		LoggedIn:            true,
		SettlementConfirmed: true,
		TradingDay:          "20260406",
		UpdatedAt:           now,
	}); err != nil {
		t.Fatalf("save session state failed: %v", err)
	}

	if err := svc.ResetPaperReplay(); err != nil {
		t.Fatalf("reset paper replay failed: %v", err)
	}

	account, err := svc.Account()
	if err != nil {
		t.Fatalf("load reset account failed: %v", err)
	}
	assertFloatEqual(t, account.Balance, replayPaperInitialBalance)
	assertFloatEqual(t, account.Available, replayPaperInitialBalance)
	assertFloatEqual(t, account.Margin, 0)
	assertFloatEqual(t, account.FrozenCash, 0)
	assertFloatEqual(t, account.Commission, 0)
	assertTableCount(t, svc, "trade_account_snapshots", 1)
	assertTableCount(t, svc, "trade_positions", 0)
	assertTableCount(t, svc, "trade_orders", 0)
	assertTableCount(t, svc, "trade_trades", 0)
	assertTableCount(t, svc, "trade_command_audits", 0)
	assertTableCount(t, svc, "trade_query_audits", 0)
	assertTableCount(t, svc, "trade_session_state", 1)
}

func TestLivePaperInitialBalanceIs100K(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc, err := NewPaperService(configForTradeTest(), "paper_live", dsn, nil)
	if err != nil {
		t.Fatalf("new live paper service failed: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	account, err := svc.Account()
	if err != nil {
		t.Fatalf("load live paper account failed: %v", err)
	}
	assertFloatEqual(t, account.Balance, replayPaperInitialBalance)
	assertFloatEqual(t, account.Available, replayPaperInitialBalance)
}

func TestReplayPaperMatchingAndCancelLifecycle(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc := newReplayPaperServiceForTest(t, dsn)

	order, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "buy",
		OffsetFlag: "open",
		LimitPrice: 100,
		Volume:     2,
		Reason:     "manual",
	})
	if err != nil {
		t.Fatalf("submit open order failed: %v", err)
	}
	if order.OrderStatus != "queued" || order.SubmitStatus != "accepted" {
		t.Fatalf("unexpected pending order status: %#v", order)
	}

	account, err := svc.Account()
	if err != nil {
		t.Fatalf("load pending account failed: %v", err)
	}
	assertFloatEqual(t, account.Balance, replayPaperInitialBalance)
	assertFloatEqual(t, account.Margin, 0)
	assertFloatEqual(t, account.FrozenCash, 200)
	assertFloatEqual(t, account.Available, replayPaperInitialBalance-200)

	if err := svc.ConsumeBusEvent(context.Background(), replayTickEvent(t, "rb2505", "SHFE", 99, 101)); err != nil {
		t.Fatalf("consume non-marketable tick failed: %v", err)
	}
	trades, err := svc.Trades(10)
	if err != nil {
		t.Fatalf("list trades failed: %v", err)
	}
	if len(trades) != 0 {
		t.Fatalf("trade count after non-marketable tick = %d, want 0", len(trades))
	}

	if err := svc.ConsumeBusEvent(context.Background(), replayTickEvent(t, "rb2505", "SHFE", 98, 99)); err != nil {
		t.Fatalf("consume marketable tick failed: %v", err)
	}
	order, err = svc.Order(order.CommandID)
	if err != nil {
		t.Fatalf("reload filled order failed: %v", err)
	}
	if order.OrderStatus != "all_traded" || order.VolumeTraded != 2 {
		t.Fatalf("filled order = %#v, want all_traded with traded volume 2", order)
	}
	trades, err = svc.Trades(10)
	if err != nil {
		t.Fatalf("list trades after fill failed: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("trade count after fill = %d, want 1", len(trades))
	}
	if trades[0].Price != 99 {
		t.Fatalf("trade price = %v, want 99", trades[0].Price)
	}
	account, err = svc.Account()
	if err != nil {
		t.Fatalf("load account after fill failed: %v", err)
	}
	assertFloatEqual(t, account.FrozenCash, 0)
	assertFloatEqual(t, account.Margin, 198)
	assertFloatEqual(t, account.Balance, replayPaperInitialBalance-0.0198)
	assertFloatEqual(t, account.Available, replayPaperInitialBalance-0.0198-198)
	assertFloatEqual(t, account.PositionProfit, 0)

	if err := svc.ConsumeBusEvent(context.Background(), replayTickEvent(t, "rb2505", "SHFE", 105, 106)); err != nil {
		t.Fatalf("consume mtm tick failed: %v", err)
	}
	account, err = svc.Account()
	if err != nil {
		t.Fatalf("load account after mtm tick failed: %v", err)
	}
	assertFloatEqual(t, account.PositionProfit, 12)

	cancelable, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "sell",
		OffsetFlag: "close",
		LimitPrice: 105,
		Volume:     1,
		Reason:     "manual",
	})
	if err != nil {
		t.Fatalf("submit close order failed: %v", err)
	}
	canceled, err := svc.CancelOrder(context.Background(), CancelOrderRequest{
		AccountID: svc.accountID,
		CommandID: cancelable.CommandID,
		Reason:    "manual_cancel",
	})
	if err != nil {
		t.Fatalf("cancel paper replay order failed: %v", err)
	}
	if canceled.OrderStatus != "canceled" {
		t.Fatalf("canceled order status = %q, want canceled", canceled.OrderStatus)
	}
	if err := svc.ConsumeBusEvent(context.Background(), replayTickEvent(t, "rb2505", "SHFE", 110, 111)); err != nil {
		t.Fatalf("consume tick after cancel failed: %v", err)
	}
	trades, err = svc.Trades(10)
	if err != nil {
		t.Fatalf("list trades after cancel tick failed: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("trade count after canceled order tick = %d, want still 1", len(trades))
	}
}

func TestReplayPaperPendingCloseReducesClosableVolume(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc := newReplayPaperServiceForTest(t, dsn)

	if _, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "buy",
		OffsetFlag: "open",
		LimitPrice: 100,
		Volume:     2,
		Reason:     "manual",
	}); err != nil {
		t.Fatalf("submit open order failed: %v", err)
	}
	if err := svc.ConsumeBusEvent(context.Background(), replayTickEvent(t, "rb2505", "SHFE", 99, 99)); err != nil {
		t.Fatalf("fill open order failed: %v", err)
	}
	if _, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "sell",
		OffsetFlag: "close",
		LimitPrice: 120,
		Volume:     2,
		Reason:     "manual",
	}); err != nil {
		t.Fatalf("submit pending close order failed: %v", err)
	}
	if _, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "sell",
		OffsetFlag: "close",
		LimitPrice: 121,
		Volume:     1,
		Reason:     "manual",
	}); err == nil {
		t.Fatal("expected second close order to be blocked by reserved closable volume")
	}
}

func TestSubmitOrderNormalizesProductCaseAndInfersExchange(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc := newReplayPaperServiceForTest(t, dsn)
	svc.resolver = quotes.NewProductExchangeResolver()
	svc.resolver.Replace([]quotes.ProductExchange{
		{ProductID: "SR", ProductIDNorm: "sr", ExchangeID: "CZCE"},
	})

	order, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "sr2509",
		Direction:  "buy",
		OffsetFlag: "open",
		LimitPrice: 100,
		Volume:     1,
		Reason:     "manual",
	})
	if err != nil {
		t.Fatalf("SubmitOrder() error = %v", err)
	}
	if order.Symbol != "SR2509" {
		t.Fatalf("order.Symbol = %q, want SR2509", order.Symbol)
	}
	if order.ExchangeID != "CZCE" {
		t.Fatalf("order.ExchangeID = %q, want CZCE", order.ExchangeID)
	}
}

func TestSubmitOrderRejectsAmbiguousExchange(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc := newReplayPaperServiceForTest(t, dsn)
	svc.resolver = quotes.NewProductExchangeResolver()
	svc.resolver.Replace([]quotes.ProductExchange{
		{ProductID: "AP", ProductIDNorm: "ap", ExchangeID: "CZCE"},
		{ProductID: "AP", ProductIDNorm: "ap", ExchangeID: "GFEX"},
	})

	if _, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		AccountID:  svc.accountID,
		Symbol:     "ap2509",
		Direction:  "buy",
		OffsetFlag: "open",
		LimitPrice: 100,
		Volume:     1,
		Reason:     "manual",
	}); err == nil {
		t.Fatal("SubmitOrder() error = nil, want ambiguity error")
	}
}

func newReplayPaperServiceForTest(t *testing.T, dsn string) *Service {
	t.Helper()
	svc, err := NewPaperService(configForTradeTest(), "paper_replay", dsn, nil)
	if err != nil {
		t.Fatalf("new replay paper service failed: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.ResetPaperReplay(); err != nil {
		t.Fatalf("reset replay paper service failed: %v", err)
	}
	return svc
}

func configForTradeTest() config.TradeConfig {
	cfg := config.TradeConfig{
		AccountID:              "paper_replay",
		MaxOrderVolume:         10,
		QueryPollIntervalMS:    1000,
		PositionSyncIntervalMS: 1000,
	}
	cfg.Enabled = boolPtr(true)
	return cfg
}

func replayTickEvent(t *testing.T, symbol string, exchange string, bid float64, ask float64) bus.BusEvent {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"InstrumentID": symbol,
		"ExchangeID":   exchange,
		"BidPrice1":    bid,
		"AskPrice1":    ask,
		"ReceivedAt":   time.Now().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("marshal tick payload failed: %v", err)
	}
	return bus.BusEvent{Topic: bus.TopicTick, Payload: payload}
}

func assertTableCount(t *testing.T, svc *Service, table string, want int) {
	t.Helper()
	var got int
	query := "SELECT COUNT(*) FROM " + table + " WHERE account_id=?"
	if err := svc.store.db.QueryRow(query, svc.accountID).Scan(&got); err != nil {
		t.Fatalf("count %s failed: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}

func assertFloatEqual(t *testing.T, got float64, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Fatalf("float mismatch: got %.10f want %.10f", got, want)
	}
}
