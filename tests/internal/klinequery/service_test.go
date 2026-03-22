package klinequery_test

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"ctp-future-kline/internal/klinequery"
	"ctp-future-kline/internal/mmkline"
	"ctp-future-kline/internal/searchindex"
	"ctp-future-kline/tests/internal/testmysql"
)

func TestBarsAndSearch(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
	if err := idx.RebuildAll(); err != nil {
		t.Fatalf("RebuildAll() error = %v", err)
	}

	end := mustTime("2026-01-19 09:30:00")

	searchResp, err := svc.Search("sr", 1, 100)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if searchResp.Total != 2 {
		t.Fatalf("Search total = %d, want 2", searchResp.Total)
	}
	if searchResp.Items[0].BarCount == 0 {
		t.Fatalf("Search item bar_count = 0, want metadata backfilled")
	}

	barsResp, err := svc.BarsByEnd("SR2701", "contract", "sr", "1m", end, 2000)
	if err != nil {
		t.Fatalf("Bars() error = %v", err)
	}
	if barsResp.Meta.Symbol != "sr2701" {
		t.Fatalf("Meta.Symbol = %q, want sr2701", barsResp.Meta.Symbol)
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

func TestSearchEmptyKeywordPrefersIndexL9ThenContracts(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	db := testmysql.Open(t, dsn)
	defer db.Close()

	createSearchIndexTable(t, db)
	now := mustTime("2026-01-20 15:00:00")
	for i := 0; i < 12; i++ {
		variety := fmt.Sprintf("v%02d", i)
		insertSearchIndexRow(t, db, searchIndexRow{
			symbol:     variety + "l9",
			symbolNorm: variety + "l9",
			variety:    variety,
			exchange:   "L9",
			kind:       "l9",
			barCount:   int64(100 + i),
			updatedAt:  now.Add(time.Duration(i) * time.Minute),
			tableName:  "future_kline_l9_1m_" + variety,
		})
	}
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "rb2501",
		symbolNorm: "rb2501",
		variety:    "rb",
		exchange:   "SHFE",
		kind:       "contract",
		barCount:   88,
		updatedAt:  now.Add(20 * time.Minute),
		tableName:  "future_kline_instrument_1m_rb",
	})

	svc := klinequery.NewService(dsn, searchindex.NewManager(dsn, 0))
	resp, err := svc.Search("", 1, 20)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 10 {
		t.Fatalf("len(resp.Items)=%d, want 10", len(resp.Items))
	}
	for i, item := range resp.Items {
		if item.Type != "l9" {
			t.Fatalf("items[%d].Type=%q, want l9", i, item.Type)
		}
		if item.BarCount == 0 || item.TableName == "" || item.UpdatedAt == "" {
			t.Fatalf("items[%d] metadata not filled: %+v", i, item)
		}
	}
}

func TestSearchEmptyKeywordFallsBackWhenIndexHasNoRows(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)
	db := testmysql.Open(t, dsn)
	defer db.Close()
	createSearchIndexTable(t, db)

	svc := klinequery.NewService(dsn, searchindex.NewManager(dsn, 0))
	resp, err := svc.Search("", 1, 20)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("len(resp.Items)=%d, want 2", len(resp.Items))
	}
	got := map[string]bool{}
	for _, item := range resp.Items {
		got[item.Type] = true
	}
	if !got["l9"] || !got["contract"] {
		t.Fatalf("fallback items=%+v, want both l9 and contract from table scan", resp.Items)
	}
}

func TestSearchEmptyKeywordFillsContractsWhenIndexL9Insufficient(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	db := testmysql.Open(t, dsn)
	defer db.Close()

	createSearchIndexTable(t, db)
	now := mustTime("2026-01-20 15:00:00")
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "srl9",
		symbolNorm: "srl9",
		variety:    "sr",
		exchange:   "L9",
		kind:       "l9",
		barCount:   90,
		updatedAt:  now,
		tableName:  "future_kline_l9_1m_sr",
	})
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "cfl9",
		symbolNorm: "cfl9",
		variety:    "cf",
		exchange:   "L9",
		kind:       "l9",
		barCount:   91,
		updatedAt:  now.Add(time.Minute),
		tableName:  "future_kline_l9_1m_cf",
	})
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "rb2501",
		symbolNorm: "rb2501",
		variety:    "rb",
		exchange:   "SHFE",
		kind:       "contract",
		barCount:   92,
		updatedAt:  now.Add(2 * time.Minute),
		tableName:  "future_kline_instrument_1m_rb",
	})
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "ag2501",
		symbolNorm: "ag2501",
		variety:    "ag",
		exchange:   "SHFE",
		kind:       "contract",
		barCount:   93,
		updatedAt:  now.Add(3 * time.Minute),
		tableName:  "future_kline_instrument_1m_ag",
	})

	svc := klinequery.NewService(dsn, searchindex.NewManager(dsn, 0))
	resp, err := svc.Search("", 1, 20)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 4 {
		t.Fatalf("len(resp.Items)=%d, want 4", len(resp.Items))
	}
	if resp.Items[0].Type != "l9" || resp.Items[1].Type != "l9" {
		t.Fatalf("first two items=%+v, want l9 first", resp.Items[:2])
	}
	if resp.Items[2].Type != "contract" || resp.Items[3].Type != "contract" {
		t.Fatalf("last two items=%+v, want contracts appended after l9", resp.Items[2:])
	}
}

func TestSearchKeywordPrefersPrefixAndL9OverContainsAndContracts(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	db := testmysql.Open(t, dsn)
	defer db.Close()

	createSearchIndexTable(t, db)
	now := mustTime("2026-01-20 15:00:00")
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "srl9",
		symbolNorm: "srl9",
		variety:    "sr",
		exchange:   "L9",
		kind:       "l9",
		barCount:   101,
		updatedAt:  now.Add(3 * time.Minute),
		tableName:  "future_kline_l9_1m_sr",
	})
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "sr2501",
		symbolNorm: "sr2501",
		variety:    "sr",
		exchange:   "CZCE",
		kind:       "contract",
		barCount:   102,
		updatedAt:  now.Add(2 * time.Minute),
		tableName:  "future_kline_instrument_1m_sr",
	})
	insertSearchIndexRow(t, db, searchIndexRow{
		symbol:     "masr",
		symbolNorm: "masr",
		variety:    "ma",
		exchange:   "CZCE",
		kind:       "contract",
		barCount:   103,
		updatedAt:  now.Add(5 * time.Minute),
		tableName:  "future_kline_instrument_1m_ma",
	})

	svc := klinequery.NewService(dsn, searchindex.NewManager(dsn, 0))
	resp, err := svc.Search("sr", 1, 20)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) < 3 {
		t.Fatalf("len(resp.Items)=%d, want at least 3", len(resp.Items))
	}
	got := []string{
		resp.Items[0].Type + ":" + resp.Items[0].Symbol,
		resp.Items[1].Type + ":" + resp.Items[1].Symbol,
		resp.Items[2].Type + ":" + resp.Items[2].Symbol,
	}
	want := []string{"l9:srl9", "contract:sr2501", "contract:masr"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("items order=%v, want prefix-first and l9-first order %v", got, want)
		}
	}
}

func TestBarsByEndUsesAdjustedTime(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
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
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','CZCE','2026-01-20 21:01:00','2026-01-19 21:01:00','1m',1,2,1,2,10,100,0)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	}

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
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

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
	if err := idx.RebuildAll(); err != nil {
		t.Fatalf("RebuildAll() error = %v", err)
	}

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
	if resp.Items[0].Symbol != "sr2701" {
		t.Fatalf("item symbol = %q, want sr2701", resp.Items[0].Symbol)
	}
}

func TestBarsByEndAggregatesByHourWithCompletedSession(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	db := testmysql.Open(t, dsn)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE "future_kline_instrument_1m_sr" (
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
)`)
	ensureTradingSessionsTable(t, db)
	mustExec(t, db, `INSERT INTO trading_sessions(variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at)
VALUES('sr','21:00-23:00,09:00-10:15,10:30-11:30,13:30-15:00',
'[{"start":"21:00","end":"23:00"},{"start":"09:00","end":"10:15"},{"start":"10:30","end":"11:30"},{"start":"13:30","end":"15:00"}]',
1,'2026-01-19','2026-01-20',1.0,'2026-01-20 15:01:00')`)

	insertSessionBars(t, db, "sr2701", "2026-01-20", "2026-01-19", [][2]string{
		{"21:00", "23:00"},
		{"09:00", "10:15"},
		{"10:30", "11:30"},
		{"13:30", "15:00"},
	})
	if _, _, err := mmkline.RebuildAndUpsert(db, mmkline.RebuildRequest{
		Variety:      "sr",
		InstrumentID: "sr2701",
		IsL9:         false,
	}); err != nil {
		t.Fatalf("RebuildAndUpsert() error = %v", err)
	}

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
	end := mustTime("2026-01-20 23:59:00")
	resp, err := svc.BarsByEnd("SR2701", "contract", "sr", "1h", end, 5000)
	if err != nil {
		t.Fatalf("BarsByEnd() error = %v", err)
	}

	got := make([]string, 0, len(resp.Bars))
	gotData := make([]string, 0, len(resp.Bars))
	for _, b := range resp.Bars {
		got = append(got, time.Unix(b.AdjustedTime, 0).Format("2006-01-02 15:04"))
		gotData = append(gotData, time.Unix(b.DataTime, 0).Format("2006-01-02 15:04"))
	}
	want := []string{
		"2026-01-19 22:00",
		"2026-01-19 23:00",
		"2026-01-20 10:00",
		"2026-01-20 11:15",
		"2026-01-20 14:15",
		"2026-01-20 15:00",
	}
	wantData := []string{
		"2026-01-20 22:00",
		"2026-01-20 23:00",
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
		if gotData[i] != wantData[i] {
			t.Fatalf("bars[%d].data_time=%s, want %s; all=%v", i, gotData[i], wantData[i], gotData)
		}
	}
}

func TestBarsByEndSessionMissingReturnsTypedError(t *testing.T) {
	t.Parallel()

	dsn := testmysql.NewDatabase(t)
	prepareDB(t, dsn)

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
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

	dsn := testmysql.NewDatabase(t)
	db := testmysql.Open(t, dsn)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE "future_kline_l9_1m_sr" (
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
)`)
	insertL9DayBars(t, db, "2026-01-19", []string{"21:00", "21:05", "21:10", "09:00", "15:00"})
	insertL9DayBars(t, db, "2026-01-20", []string{"21:00", "21:05", "21:10", "09:00", "15:01"})
	insertL9DayBars(t, db, "2026-01-21", []string{"21:00", "21:05", "21:10", "09:00", "15:00"})
	if _, _, err := mmkline.RebuildAndUpsert(db, mmkline.RebuildRequest{
		Variety:      "sr",
		InstrumentID: "srl9",
		IsL9:         true,
	}); err != nil {
		t.Fatalf("RebuildAndUpsert() error = %v", err)
	}

	idx := searchindex.NewManager(dsn, 0)
	svc := klinequery.NewService(dsn, idx)
	end := mustTime("2026-01-21 23:59:00")

	resp, err := svc.BarsByEnd("srl9", "l9", "sr", "1h", end, 5000)
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
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','CZCE','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,10,100,0)`,
		`INSERT INTO "future_kline_instrument_1m_sr" VALUES ('sr2701','CZCE','2026-01-19 09:02:00','2026-01-19 09:02:00','1m',2,3,2,3,11,101,0)`,
		`INSERT INTO "future_kline_l9_1m_sr" VALUES ('srl9','L9','2026-01-19 09:01:00','2026-01-19 09:01:00','1m',1,2,1,2,20,200,0)`,
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
  variety VARCHAR(32) PRIMARY KEY,
  session_text VARCHAR(255) NOT NULL,
  session_json JSON NOT NULL,
  is_completed TINYINT NOT NULL DEFAULT 0,
  sample_trade_date DATE NULL,
  validated_trade_date DATE NULL,
  match_ratio DOUBLE NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL
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
			"srl9", "L9",
			ts.Format("2006-01-02 15:04:05"),
			ts.Format("2006-01-02 15:04:05"),
			"1m",
			price, price+1, price-1, price+0.5, 20, 200.0, 0.0,
		)
		price += 0.2
	}
}

type searchIndexRow struct {
	symbol     string
	symbolNorm string
	variety    string
	exchange   string
	kind       string
	barCount   int64
	updatedAt  time.Time
	tableName  string
}

func createSearchIndexTable(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, `CREATE TABLE IF NOT EXISTS kline_search_index(
  table_name VARCHAR(128) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  symbol_norm VARCHAR(64) NOT NULL,
  variety VARCHAR(32) NOT NULL,
  exchange VARCHAR(16) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  bar_count BIGINT NOT NULL,
  updated_at DATETIME NOT NULL
)`)
	mustExec(t, db, `DELETE FROM kline_search_index`)
}

func insertSearchIndexRow(t *testing.T, db *sql.DB, row searchIndexRow) {
	t.Helper()
	mustExecArgs(t, db, `INSERT INTO kline_search_index(table_name,symbol,symbol_norm,variety,exchange,kind,bar_count,updated_at)
VALUES(?,?,?,?,?,?,?,?)`,
		row.tableName,
		row.symbol,
		row.symbolNorm,
		row.variety,
		row.exchange,
		row.kind,
		row.barCount,
		row.updatedAt.Format("2006-01-02 15:04:05"),
	)
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
