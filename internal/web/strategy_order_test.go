package web

import (
	"context"
	"strings"
	"testing"

	"ctp-future-kline/internal/appmode"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/strategy"
	"ctp-future-kline/internal/testmysql"
	"ctp-future-kline/internal/trade"
)

func TestBuildStrategySubmitOrderRequestOpensShortAtBid(t *testing.T) {
	t.Parallel()

	bid := 3510.0
	ask := 3511.0
	req, err := buildStrategySubmitOrderRequest(strategy.StrategyOrderRequest{
		Instance:        strategy.StrategyInstance{InstanceID: "inst-1"},
		Symbol:          "rb2601",
		CurrentPosition: 0,
		TargetPosition:  -1,
		PlannedDelta:    -1,
	}, quotes.ChartQuoteSnapshot{BidPrice1: &bid, AskPrice1: &ask}, "SHFE", "paper_live")
	if err != nil {
		t.Fatalf("buildStrategySubmitOrderRequest() error = %v", err)
	}
	if req.Direction != "sell" || req.OffsetFlag != "open" || req.Volume != 1 || req.LimitPrice != bid || req.Reason != "strategy" {
		t.Fatalf("request = %+v, want sell open volume 1 at bid", req)
	}
}

func TestBuildStrategySubmitOrderRequestClosesShortAtAsk(t *testing.T) {
	t.Parallel()

	bid := 3508.0
	ask := 3509.0
	req, err := buildStrategySubmitOrderRequest(strategy.StrategyOrderRequest{
		Instance:        strategy.StrategyInstance{InstanceID: "inst-1"},
		Symbol:          "rb2601",
		CurrentPosition: -1,
		TargetPosition:  0,
		PlannedDelta:    1,
	}, quotes.ChartQuoteSnapshot{BidPrice1: &bid, AskPrice1: &ask}, "SHFE", "paper_live")
	if err != nil {
		t.Fatalf("buildStrategySubmitOrderRequest() error = %v", err)
	}
	if req.Direction != "buy" || req.OffsetFlag != "close" || req.Volume != 1 || req.LimitPrice != ask {
		t.Fatalf("request = %+v, want buy close volume 1 at ask", req)
	}
}

func TestBuildStrategySubmitOrderRequestRejectsUnsafeDeltas(t *testing.T) {
	t.Parallel()

	latest := 3510.0
	_, err := buildStrategySubmitOrderRequest(strategy.StrategyOrderRequest{
		Instance:        strategy.StrategyInstance{InstanceID: "inst-1"},
		Symbol:          "rb2601",
		CurrentPosition: 0,
		TargetPosition:  -0.5,
		PlannedDelta:    -0.5,
	}, quotes.ChartQuoteSnapshot{LatestPrice: &latest}, "SHFE", "paper_live")
	if err == nil || !strings.Contains(err.Error(), "not an integer volume") {
		t.Fatalf("non-integer delta error = %v, want integer volume error", err)
	}

	_, err = buildStrategySubmitOrderRequest(strategy.StrategyOrderRequest{
		Instance:        strategy.StrategyInstance{InstanceID: "inst-1"},
		Symbol:          "rb2601",
		CurrentPosition: -1,
		TargetPosition:  1,
		PlannedDelta:    2,
	}, quotes.ChartQuoteSnapshot{LatestPrice: &latest}, "SHFE", "paper_live")
	if err == nil || !strings.Contains(err.Error(), "must be split") {
		t.Fatalf("reversal error = %v, want split reversal error", err)
	}
}

func TestSubmitStrategyOrderBlocksLiveRealByDefault(t *testing.T) {
	t.Parallel()

	srv := &Server{
		cfg: config.AppConfig{
			Trade: config.TradeConfig{},
		},
		currentMode: appmode.LiveReal,
	}

	_, err := srv.SubmitStrategyOrder(context.Background(), strategy.StrategyOrderRequest{
		Instance:        strategy.StrategyInstance{InstanceID: "inst-1"},
		Symbol:          "rb2601",
		CurrentPosition: 0,
		TargetPosition:  -1,
		PlannedDelta:    -1,
	})
	if err == nil || !strings.Contains(err.Error(), "block_strategy_live_order") {
		t.Fatalf("SubmitStrategyOrder() error = %v, want live order blocked", err)
	}
}

func TestValidateStrategyAccountGuardBlocksOpenBelowStaticBalance(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	svc, err := trade.NewPaperService(tradeTestConfig(), "paper_live", dsn, nil)
	if err != nil {
		t.Fatalf("new paper service failed: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	if _, err := svc.AdjustAccount(trade.AccountAdjustRequest{
		AccountID:     "paper_live",
		OtherFeeDelta: 500,
	}); err != nil {
		t.Fatalf("adjust account failed: %v", err)
	}
	srv := &Server{currentMode: appmode.LivePaper}
	err = srv.validateStrategyAccountGuard(svc, strategyGuardSubmitOrderRequest("open"))
	if err == nil || !strings.Contains(err.Error(), "below static balance") {
		t.Fatalf("validateStrategyAccountGuard(open) error = %v, want below static balance block", err)
	}

	err = srv.validateStrategyAccountGuard(svc, strategyGuardSubmitOrderRequest("close"))
	if err != nil {
		t.Fatalf("validateStrategyAccountGuard(close) error = %v, want nil", err)
	}
}

func strategyGuardSubmitOrderRequest(offsetFlag string) trade.SubmitOrderRequest {
	return trade.SubmitOrderRequest{
		AccountID:  "paper_live",
		Symbol:     "rb2601",
		ExchangeID: "SHFE",
		Direction:  "sell",
		OffsetFlag: offsetFlag,
		LimitPrice: 3500,
		Volume:     1,
		Reason:     "strategy",
		ClientTag:  "strategy-auto-test",
	}
}
