package searchindex_test

import (
	"testing"
	"time"

	"ctp-future-kline/internal/searchindex"
	"ctp-future-kline/tests/internal/testmysql"
)

func TestSearchAndLookup(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	mgr := searchindex.NewManager(dsn, 0)
	start := mustTime("2026-01-19 00:00:00")
	end := mustTime("2026-01-20 23:59:00")

	items, total, err := mgr.Search("sr", start, end, 1, 100)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if total != 2 {
		t.Fatalf("total = %d, want 2", total)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}

	foundContract := false
	foundL9 := false
	for _, it := range items {
		if it.Kind == "contract" && it.SymbolNorm == "sr2701" {
			foundContract = true
		}
		if it.Kind == "l9" && it.SymbolNorm == "srl9" {
			foundL9 = true
		}
	}
	if !foundContract || !foundL9 {
		t.Fatalf("unexpected search items: %+v", items)
	}

	got, err := mgr.LookupBySymbol("SRL9", "l9", "sr")
	if err != nil {
		t.Fatalf("LookupBySymbol() error = %v", err)
	}
	if got == nil {
		t.Fatal("LookupBySymbol() = nil, want item")
	}
	if got.Variety != "sr" {
		t.Fatalf("variety = %q, want sr", got.Variety)
	}
}

func prepareDB(t *testing.T, dsn string) {
	t.Helper()

	db := testmysql.Open(t, dsn)
	defer db.Close()

	stmts := []string{
		`CREATE TABLE "future_kline_instrument_1m_sr" (
  "InstrumentID" VARCHAR(32) NOT NULL,
  "Exchange" VARCHAR(16) NOT NULL,
  "DataTime" DATETIME NOT NULL,
  "AdjustedTime" DATETIME NOT NULL,
  "Period" VARCHAR(8) NOT NULL,
  "Open" DOUBLE NOT NULL,
  "High" DOUBLE NOT NULL,
  "Low" DOUBLE NOT NULL,
  "Close" DOUBLE NOT NULL,
  "Volume" BIGINT NOT NULL,
  "OpenInterest" DOUBLE NOT NULL,
  "SettlementPrice" DOUBLE NOT NULL
)`,
		`CREATE TABLE "future_kline_l9_1m_sr" (
  "InstrumentID" VARCHAR(32) NOT NULL,
  "Exchange" VARCHAR(16) NOT NULL,
  "DataTime" DATETIME NOT NULL,
  "AdjustedTime" DATETIME NOT NULL,
  "Period" VARCHAR(8) NOT NULL,
  "Open" DOUBLE NOT NULL,
  "High" DOUBLE NOT NULL,
  "Low" DOUBLE NOT NULL,
  "Close" DOUBLE NOT NULL,
  "Volume" BIGINT NOT NULL,
  "OpenInterest" DOUBLE NOT NULL,
  "SettlementPrice" DOUBLE NOT NULL
)`,
		`CREATE TABLE "future_kline_instrument_mm_sr" (
  "InstrumentID" VARCHAR(32) NOT NULL,
  "Exchange" VARCHAR(16) NOT NULL,
  "DataTime" DATETIME NOT NULL,
  "AdjustedTime" DATETIME NOT NULL,
  "Period" VARCHAR(8) NOT NULL,
  "Open" DOUBLE NOT NULL,
  "High" DOUBLE NOT NULL,
  "Low" DOUBLE NOT NULL,
  "Close" DOUBLE NOT NULL,
  "Volume" BIGINT NOT NULL,
  "OpenInterest" DOUBLE NOT NULL,
  "SettlementPrice" DOUBLE NOT NULL
)`,
		`CREATE TABLE "future_kline_l9_mm_sr" (
  "InstrumentID" VARCHAR(32) NOT NULL,
  "Exchange" VARCHAR(16) NOT NULL,
  "DataTime" DATETIME NOT NULL,
  "AdjustedTime" DATETIME NOT NULL,
  "Period" VARCHAR(8) NOT NULL,
  "Open" DOUBLE NOT NULL,
  "High" DOUBLE NOT NULL,
  "Low" DOUBLE NOT NULL,
  "Close" DOUBLE NOT NULL,
  "Volume" BIGINT NOT NULL,
  "OpenInterest" DOUBLE NOT NULL,
  "SettlementPrice" DOUBLE NOT NULL
)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','CZCE','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','CZCE','2026-01-19 09:02:00','2026-01-19 09:02:00','1m',2,3,2,3,11,101,0)`,
		`INSERT INTO "future_kline_l9_1m_sr" VALUES ('srl9','L9','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,20,200,0)`,
		`INSERT INTO "future_kline_instrument_mm_sr" VALUES ('sr2701','CZCE','2026-01-19 09:05:00','2026-01-19 09:05:00','5m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_l9_mm_sr" VALUES ('srl9','L9','2026-01-19 09:05:00','2026-01-19 09:05:00','5m',1,2,1,2,20,200,0)`,
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
