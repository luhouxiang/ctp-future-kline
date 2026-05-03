package searchindex_test

import (
	"testing"

	"ctp-future-kline/internal/searchindex"
	"ctp-future-kline/tests/internal/testmysql"
)

func TestRebuildAllAndLookup(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	mgr := searchindex.NewManager(dsn, 0)
	if err := mgr.RebuildAll(); err != nil {
		t.Fatalf("RebuildAll() error = %v", err)
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
	if got.BarCount != 1 {
		t.Fatalf("bar_count = %d, want 1", got.BarCount)
	}
}

func TestRebuildItemsDeletesMissingRow(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	mgr := searchindex.NewManager(dsn, 0)
	if err := mgr.RebuildAll(); err != nil {
		t.Fatalf("RebuildAll() error = %v", err)
	}

	db := testmysql.Open(t, dsn)
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM "future_kline_instrument_1m_sr" WHERE lower("InstrumentID")='sr2701'`); err != nil {
		t.Fatalf("delete source row failed: %v", err)
	}

	if err := mgr.RebuildItems([]searchindex.Target{{
		TableName: "future_kline_instrument_1m_sr",
		Symbol:    "sr2701",
		Variety:   "sr",
		Kind:      "contract",
	}}); err != nil {
		t.Fatalf("RebuildItems() error = %v", err)
	}

	got, err := mgr.LookupBySymbol("sr2701", "contract", "sr")
	if err != nil {
		t.Fatalf("LookupBySymbol() error = %v", err)
	}
	if got != nil {
		t.Fatalf("LookupBySymbol() = %+v, want nil after delete", got)
	}
}

func prepareDB(t *testing.T, dsn string) {
	t.Helper()

	db := testmysql.Open(t, dsn)
	defer db.Close()

	stmts := []string{
		`CREATE TABLE "future_kline_instrument_1m_sr" (
  "InstrumentID" VARCHAR(32) NOT NULL,
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
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','2026-01-19 09:02:00','2026-01-19 09:02:00','1m',2,3,2,3,11,101,0)`,
		`INSERT INTO "future_kline_l9_1m_sr" VALUES ('srl9','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,20,200,0)`,
		`INSERT INTO "future_kline_instrument_mm_sr" VALUES ('sr2701','2026-01-19 09:05:00','2026-01-19 09:05:00','5m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_l9_mm_sr" VALUES ('srl9','2026-01-19 09:05:00','2026-01-19 09:05:00','5m',1,2,1,2,20,200,0)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	}
}
