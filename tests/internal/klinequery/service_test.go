package klinequery_test

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ctp-go-demo/internal/klinequery"
	"ctp-go-demo/internal/searchindex"
	_ "modernc.org/sqlite"
)

func TestBarsAndSearch(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	prepareDB(t, dbPath)

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)

	start := mustTime("2026-01-19 09:00:00")
	end := mustTime("2026-01-19 09:30:00")

	searchResp, err := svc.Search("sr", start, end, 1, 100)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if searchResp.Total != 2 {
		t.Fatalf("Search total = %d, want 2", searchResp.Total)
	}

	barsResp, err := svc.BarsByEnd("SR2701", "contract", "sr", "1m", end, 2000)
	if err != nil {
		t.Fatalf("Bars() error = %v", err)
	}
	if barsResp.Meta.Symbol != "SR2701" {
		t.Fatalf("Meta.Symbol = %q, want SR2701", barsResp.Meta.Symbol)
	}
	if len(barsResp.Bars) != 2 {
		t.Fatalf("len(Bars) = %d, want 2", len(barsResp.Bars))
	}
	if len(barsResp.MACD) != 2 {
		t.Fatalf("len(MACD) = %d, want 2", len(barsResp.MACD))
	}
	if barsResp.MACD[0].DIF == nil || barsResp.MACD[1].DIF == nil {
		t.Fatal("MACD DIF should not be nil")
	}
}

func TestBarsByEndUsesAdjustedTime(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE "future_kline_instrument_1m_sr" (
  "InstrumentID" TEXT NOT NULL,
  "Exchange" TEXT NOT NULL,
  "DataTime" TEXT NOT NULL,
  "AdjustedTime" TEXT NOT NULL,
  "Period" TEXT NOT NULL,
  "Open" REAL NOT NULL,
  "High" REAL NOT NULL,
  "Low" REAL NOT NULL,
  "Close" REAL NOT NULL,
  "Volume" INTEGER NOT NULL,
  "OpenInterest" REAL NOT NULL,
  "SettlementPrice" REAL NOT NULL
)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('2701','CZCE','2026-01-20 21:01:00','2026-01-19 21:01:00','1m',1,2,1,2,10,100,0)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	}

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)
	end := mustTime("2026-01-19 23:00:00")

	resp, err := svc.BarsByEnd("SR2701", "contract", "sr", "1m", end, 2000)
	if err != nil {
		t.Fatalf("BarsByEnd() error = %v", err)
	}
	if len(resp.Bars) != 1 {
		t.Fatalf("len(resp.Bars)=%d, want 1", len(resp.Bars))
	}
	adj := mustTime("2026-01-19 21:01:00").Unix()
	dt := mustTime("2026-01-20 21:01:00").Unix()
	if resp.Bars[0].AdjustedTime != adj {
		t.Fatalf("resp.Bars[0].AdjustedTime=%d, want %d", resp.Bars[0].AdjustedTime, adj)
	}
	if resp.Bars[0].DataTime != dt {
		t.Fatalf("resp.Bars[0].DataTime=%d, want %d", resp.Bars[0].DataTime, dt)
	}
}

func TestListContractsOnlyReturnsContractKind(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	prepareDB(t, dbPath)

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)

	resp, err := svc.ListContracts(1, 100)
	if err != nil {
		t.Fatalf("ListContracts() error = %v", err)
	}
	if resp.Total != 1 {
		t.Fatalf("ListContracts total = %d, want 1", resp.Total)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("ListContracts items = %d, want 1", len(resp.Items))
	}
	if resp.Items[0].Type != "contract" {
		t.Fatalf("item type = %q, want contract", resp.Items[0].Type)
	}
	if resp.Items[0].Symbol != "SR2701" {
		t.Fatalf("item symbol = %q, want SR2701", resp.Items[0].Symbol)
	}
}

func TestBarsByEndAggregatesByHourWithCompletedSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	mustExec(t, db, `CREATE TABLE "future_kline_instrument_1m_sr" (
  "InstrumentID" TEXT NOT NULL,
  "Exchange" TEXT NOT NULL,
  "DataTime" TEXT NOT NULL,
  "AdjustedTime" TEXT NOT NULL,
  "Period" TEXT NOT NULL,
  "Open" REAL NOT NULL,
  "High" REAL NOT NULL,
  "Low" REAL NOT NULL,
  "Close" REAL NOT NULL,
  "Volume" INTEGER NOT NULL,
  "OpenInterest" REAL NOT NULL,
  "SettlementPrice" REAL NOT NULL
)`)
	ensureTradingSessionsTable(t, db)
	mustExec(t, db, `INSERT INTO trading_sessions(variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at)
VALUES('sr','21:00-23:00,09:00-10:15,10:30-11:30,13:30-15:00',
'[{"start":"21:00","end":"23:00"},{"start":"09:00","end":"10:15"},{"start":"10:30","end":"11:30"},{"start":"13:30","end":"15:00"}]',
1,'2026-01-19','2026-01-20',1.0,'2026-01-20 15:01:00')`)

	insertSessionBars(t, db, "2701", "2026-01-20", "2026-01-19", [][2]string{
		{"21:00", "23:00"},
		{"09:00", "10:15"},
		{"10:30", "11:30"},
		{"13:30", "15:00"},
	})

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)
	end := mustTime("2026-01-20 23:59:00")
	resp, err := svc.BarsByEnd("SR2701", "contract", "sr", "1h", end, 5000)
	if err != nil {
		t.Fatalf("BarsByEnd() error = %v", err)
	}

	got := make([]string, 0, len(resp.Bars))
	for _, b := range resp.Bars {
		got = append(got, time.Unix(b.AdjustedTime, 0).Format("2006-01-02 15:04"))
	}
	want := []string{
		"2026-01-19 22:00",
		"2026-01-19 23:00",
		"2026-01-20 10:00",
		"2026-01-20 11:15",
		"2026-01-20 14:15",
		"2026-01-20 15:00",
	}
	if len(got) != len(want) {
		t.Fatalf("len(bars)=%d, want %d, got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bars[%d]=%s, want %s; all=%v", i, got[i], want[i], got)
		}
	}
}

func TestBarsByEndSessionMissingReturnsTypedError(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	prepareDB(t, dbPath)

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)
	end := mustTime("2026-01-19 09:30:00")

	_, err := svc.BarsByEnd("SR2701", "contract", "sr", "1h", end, 2000)
	if err == nil {
		t.Fatal("expect error, got nil")
	}
	if !errors.Is(err, klinequery.ErrTradingSessionNotReady) {
		t.Fatalf("error=%v, want ErrTradingSessionNotReady", err)
	}
}

func TestBarsByEndL9InferTradingSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	mustExec(t, db, `CREATE TABLE "future_kline_l9_1m_sr" (
  "InstrumentID" TEXT NOT NULL,
  "Exchange" TEXT NOT NULL,
  "DataTime" TEXT NOT NULL,
  "AdjustedTime" TEXT NOT NULL,
  "Period" TEXT NOT NULL,
  "Open" REAL NOT NULL,
  "High" REAL NOT NULL,
  "Low" REAL NOT NULL,
  "Close" REAL NOT NULL,
  "Volume" INTEGER NOT NULL,
  "OpenInterest" REAL NOT NULL,
  "SettlementPrice" REAL NOT NULL
)`)
	ensureTradingSessionsTable(t, db)

	insertL9DayBars(t, db, "2026-01-20", []string{"21:00", "21:05", "21:10", "09:00", "15:01"})
	insertL9DayBars(t, db, "2026-01-21", []string{"21:00", "21:05", "21:10", "09:00", "15:00"})

	idx := searchindex.NewManager(dbPath, 0)
	svc := klinequery.NewService(dbPath, idx)
	end := mustTime("2026-01-21 23:59:00")

	resp, err := svc.BarsByEnd("srl9", "l9", "sr", "1m", end, 5000)
	if err != nil {
		t.Fatalf("BarsByEnd() error = %v", err)
	}
	if len(resp.Bars) == 0 {
		t.Fatalf("expected non-empty bars")
	}

	var (
		isCompleted bool
		matchRatio  float64
		sessionText string
	)
	if err := db.QueryRow(`SELECT is_completed,match_ratio,session_text FROM trading_sessions WHERE variety='sr'`).Scan(&isCompleted, &matchRatio, &sessionText); err != nil {
		t.Fatalf("query trading_sessions failed: %v", err)
	}
	if !isCompleted {
		t.Fatalf("is_completed=false, want true")
	}
	if matchRatio < 0.90 {
		t.Fatalf("match_ratio=%.4f, want >=0.90", matchRatio)
	}
	if !strings.Contains(sessionText, "15:00") || strings.Contains(sessionText, "15:01") {
		t.Fatalf("session_text=%q, want rounded to 15:00", sessionText)
	}
}

func prepareDB(t *testing.T, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE "future_kline_instrument_1m_sr" (
  "InstrumentID" TEXT NOT NULL,
  "Exchange" TEXT NOT NULL,
  "DataTime" TEXT NOT NULL,
  "AdjustedTime" TEXT NOT NULL,
  "Period" TEXT NOT NULL,
  "Open" REAL NOT NULL,
  "High" REAL NOT NULL,
  "Low" REAL NOT NULL,
  "Close" REAL NOT NULL,
  "Volume" INTEGER NOT NULL,
  "OpenInterest" REAL NOT NULL,
  "SettlementPrice" REAL NOT NULL
)`,
		`CREATE TABLE "future_kline_l9_1m_sr" (
  "InstrumentID" TEXT NOT NULL,
  "Exchange" TEXT NOT NULL,
  "DataTime" TEXT NOT NULL,
  "AdjustedTime" TEXT NOT NULL,
  "Period" TEXT NOT NULL,
  "Open" REAL NOT NULL,
  "High" REAL NOT NULL,
  "Low" REAL NOT NULL,
  "Close" REAL NOT NULL,
  "Volume" INTEGER NOT NULL,
  "OpenInterest" REAL NOT NULL,
  "SettlementPrice" REAL NOT NULL
)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('2701','CZCE','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('2701','CZCE','2026-01-19 09:02:00','2026-01-19 09:02:00','1m',2,3,2,3,11,101,0)`,
		`INSERT INTO "future_kline_l9_1m_sr" VALUES ('l9','L9','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,20,200,0)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	}
}

func ensureTradingSessionsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, `CREATE TABLE IF NOT EXISTS trading_sessions(
  variety TEXT PRIMARY KEY,
  session_text TEXT NOT NULL,
  session_json TEXT NOT NULL,
  is_completed INTEGER NOT NULL DEFAULT 0,
  sample_trade_date TEXT NULL,
  validated_trade_date TEXT NULL,
  match_ratio REAL NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
)`)
}

func insertSessionBars(t *testing.T, db *sql.DB, instrumentID string, dataDate string, nightAdjustedDate string, sessions [][2]string) {
	t.Helper()
	price := 100.0
	volume := int64(10)
	for _, s := range sessions {
		start := mustTime(fmt.Sprintf("%s %s:00", dataDate, s[0]))
		end := mustTime(fmt.Sprintf("%s %s:00", dataDate, s[1]))
		for ts := start; !ts.After(end); ts = ts.Add(time.Minute) {
			adj := ts
			if ts.Hour() >= 18 {
				adj = mustTime(fmt.Sprintf("%s %02d:%02d:00", nightAdjustedDate, ts.Hour(), ts.Minute()))
			}
			stmt := `INSERT INTO "future_kline_instrument_1m_sr" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
			mustExecArgs(t, db, stmt,
				instrumentID, "CZCE",
				ts.Format("2006-01-02 15:04:05"),
				adj.Format("2006-01-02 15:04:05"),
				"1m",
				price, price+1, price-1, price+0.5, volume, 100.0, 0.0,
			)
			price += 0.1
			volume += 1
		}
	}
}

func insertL9DayBars(t *testing.T, db *sql.DB, tradeDate string, hhmm []string) {
	t.Helper()
	price := 200.0
	for _, hm := range hhmm {
		ts := mustTime(fmt.Sprintf("%s %s:00", tradeDate, hm))
		stmt := `INSERT INTO "future_kline_l9_1m_sr" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
		mustExecArgs(t, db, stmt,
			"l9", "L9",
			ts.Format("2006-01-02 15:04:05"),
			ts.Format("2006-01-02 15:04:05"),
			"1m",
			price, price+1, price-1, price+0.5, 20, 200.0, 0.0,
		)
		price += 0.2
	}
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec failed: %v, stmt=%s", err, stmt)
	}
}

func mustExecArgs(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatalf("exec with args failed: %v, stmt=%s", err, stmt)
	}
}

func mustTime(v string) time.Time {
	out, _ := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
	return out
}
