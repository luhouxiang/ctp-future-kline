package web

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ctp-future-kline/internal/appmode"
	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/replay"
	"ctp-future-kline/internal/testmysql"
	"ctp-future-kline/internal/trade"

	_ "modernc.org/sqlite"
)

func TestHandleReplayStartResetsReplayPaperTradeState(t *testing.T) {
	dsn := testmysql.NewDatabase(t)
	tradeSvc, err := trade.NewPaperService(tradeTestConfig(), "paper_replay", dsn, nil)
	if err != nil {
		t.Fatalf("new paper replay service failed: %v", err)
	}
	t.Cleanup(func() { _ = tradeSvc.Close() })

	if _, err := tradeSvc.SubmitOrder(httptest.NewRequest(http.MethodPost, "/", nil).Context(), trade.SubmitOrderRequest{
		AccountID:  "paper_replay",
		Symbol:     "rb2505",
		ExchangeID: "SHFE",
		Direction:  "buy",
		OffsetFlag: "open",
		LimitPrice: 100,
		Volume:     1,
		Reason:     "manual",
	}); err != nil {
		t.Fatalf("seed pending order failed: %v", err)
	}

	replaySvc := newReplayServiceForWebTest(t)
	replaySvc.RegisterConsumer("trade.paper_replay", tradeSvc.ConsumeBusEvent)
	srv := &Server{
		cfg: config.AppConfig{
			CTP: config.CTPConfig{
				FlowPath:          t.TempDir(),
				ReplayDefaultMode: "fast",
				ReplayDefaultSpeed: 1,
			},
		},
		replay:           replaySvc,
		tradePaperReplay: tradeSvc,
		currentMode:      appmode.ReplayPaper,
	}

	tickDir := filepath.Join(t.TempDir(), "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeReplayTickCSV(t, filepath.Join(tickDir, "rb2505.csv"), []string{
		"2026-03-01 09:00:00.000,rb2505,SHFE,20260303,20260301,09:00:00,100.1,1,10,99,100,100.2,0",
	})

	body := bytes.NewBufferString(`{"mode":"fast","tick_dir":"` + filepath.ToSlash(tickDir) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/replay/start", body)
	rr := httptest.NewRecorder()
	srv.handleReplayStart(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("handleReplayStart status = %d, body=%s", rr.Code, rr.Body.String())
	}

	account, err := tradeSvc.Account()
	if err != nil {
		t.Fatalf("account after replay start failed: %v", err)
	}
	if account.Balance != 100000 || account.Available != 100000 {
		t.Fatalf("account after replay start = %#v, want clean 100000 balance", account)
	}
	orders, err := tradeSvc.Orders(10)
	if err != nil {
		t.Fatalf("orders after replay start failed: %v", err)
	}
	if len(orders) != 0 {
		t.Fatalf("orders after replay start = %d, want 0", len(orders))
	}
	trades, err := tradeSvc.Trades(10)
	if err != nil {
		t.Fatalf("trades after replay start failed: %v", err)
	}
	if len(trades) != 0 {
		t.Fatalf("trades after replay start = %d, want 0", len(trades))
	}
}

func newReplayServiceForWebTest(t *testing.T) *replay.Service {
	t.Helper()
	log := bus.NewFileLog(filepath.Join(t.TempDir(), "bus"), 0)
	t.Cleanup(func() { _ = log.Close() })
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "replay_dedup.db"))
	if err != nil {
		t.Fatalf("open replay sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := bus.NewConsumerStore(db)
	if err != nil {
		t.Fatalf("new consumer store failed: %v", err)
	}
	return replay.NewService(log, store, true)
}

func writeReplayTickCSV(t *testing.T, path string, rows []string) {
	t.Helper()
	content := "received_at,instrument_id,exchange_id,trading_day,action_day,update_time,last_price,volume,open_interest,bid_price1,ask_price1,settlement_price,update_millisec\n" +
		strings.Join(rows, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write tick csv failed: %v", err)
	}
}

func tradeTestConfig() config.TradeConfig {
	cfg := config.TradeConfig{AccountID: "paper_replay", MaxOrderVolume: 10}
	cfg.Enabled = boolPtrWeb(true)
	return cfg
}

func boolPtrWeb(v bool) *bool { return &v }
