package klinequery_test

import (
	"database/sql"
	"path/filepath"
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

	barsResp, err := svc.BarsByEnd("SR2701", "contract", "sr", end, 2000)
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

	resp, err := svc.BarsByEnd("SR2701", "contract", "sr", end, 2000)
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

func mustTime(v string) time.Time {
	out, _ := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
	return out
}
