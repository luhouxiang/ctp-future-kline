package importer_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"ctp-go-demo/internal/importer"
	"ctp-go-demo/internal/trader"
	"golang.org/x/text/encoding/simplifiedchinese"
	_ "modernc.org/sqlite"
)

func TestTDXImportSessionImportsContractAndL9(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	files := []importer.UploadFile{
		{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
			"2026/01/19,2101,5321,5322,5321,5322,3,3,0",
		}))},
		{Name: "28#SRL9.txt", Data: mustGBK(t, sampleTdxContent("SRL9", []string{
			"2026/01/19,2101,5000,5001,5000,5001,1,1,0",
		}))},
	}

	h := newCaptureHandler()
	s := importer.NewTDXImportSession("s1", dbPath, files, h)
	s.Start()

	done := h.waitDone(t)
	if done.InsertedRows != 2 {
		t.Fatalf("InsertedRows = %d, want 2", done.InsertedRows)
	}
	if done.SkippedFiles != 0 {
		t.Fatalf("SkippedFiles = %d, want 0", done.SkippedFiles)
	}

	store, err := trader.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("NewKlineStore() error = %v", err)
	}
	defer store.Close()

	row := store.DB().QueryRow(`SELECT COUNT(1) FROM "future_kline_instrument_1m_sr"`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}

	row = store.DB().QueryRow(`SELECT COUNT(1) FROM "future_kline_l9_1m_sr"`)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query l9 count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("l9 row count = %d, want 1", count)
	}
}

func TestTDXImportSessionImportsWhenHeaderMarkerMissing(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	files := []importer.UploadFile{
		{Name: "30#SPL9.txt", Data: mustGBK(t, sampleTdxContentNoMarker("SPL9", []string{
			"2026/01/19,2101,5000,5001,5000,5001,1,1,0",
		}))},
	}

	h := newCaptureHandler()
	s := importer.NewTDXImportSession("s-header", dbPath, files, h)
	s.Start()

	done := h.waitDone(t)
	if done.InsertedRows != 1 {
		t.Fatalf("InsertedRows = %d, want 1", done.InsertedRows)
	}

	store, err := trader.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("NewKlineStore() error = %v", err)
	}
	defer store.Close()

	row := store.DB().QueryRow(`SELECT COUNT(1) FROM "future_kline_l9_1m_sp" WHERE "InstrumentID"='spl9'`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query l9 count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("l9 row count = %d, want 1", count)
	}
}

func TestTDXImportSessionConflictAndOverwriteAll(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	baseFile := importer.UploadFile{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
		"2026/01/19,2101,5321,5322,5321,5322,3,3,0",
		"2026/01/19,2102,5322,5323,5322,5323,3,3,0",
	}))}

	h1 := newCaptureHandler()
	s1 := importer.NewTDXImportSession("s1", dbPath, []importer.UploadFile{baseFile}, h1)
	s1.Start()
	_ = h1.waitDone(t)

	overwriteFile := importer.UploadFile{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
		"2026/01/19,2101,6000,6001,6000,6001,9,9,0",
		"2026/01/19,2102,7000,7001,7000,7001,8,8,0",
	}))}

	h2 := newCaptureHandler()
	s2 := importer.NewTDXImportSession("s2", dbPath, []importer.UploadFile{overwriteFile}, h2)
	s2.Start()

	conflict := h2.waitConflict(t)
	if conflict.Incoming.Close != 6001 {
		t.Fatalf("first conflict close = %v, want 6001", conflict.Incoming.Close)
	}

	err := s2.ApplyDecision(importer.DecisionRequest{
		Action:                importer.ActionOverwrite,
		OverwriteAllContracts: true,
	})
	if err != nil {
		t.Fatalf("ApplyDecision() error = %v", err)
	}

	done := h2.waitDone(t)
	if done.OverwrittenRows != 2 {
		t.Fatalf("OverwrittenRows = %d, want 2", done.OverwrittenRows)
	}

	store, err := trader.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("NewKlineStore() error = %v", err)
	}
	defer store.Close()

	var closePrice float64
	err = store.DB().QueryRow(
		`SELECT "Close" FROM "future_kline_instrument_1m_sr" WHERE "DataTime"=? AND "InstrumentID"=? AND "Exchange"=? AND "Period"=?`,
		"2026-01-19 21:02:00", "sr2701", "CZCE", "1m",
	).Scan(&closePrice)
	if err != nil {
		t.Fatalf("query updated row failed: %v", err)
	}
	if closePrice != 7001 {
		t.Fatalf("close price = %v, want 7001", closePrice)
	}
}

func TestAdjustedTimePrefersPreviousTradingDayFromCurrentFile(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	mustPrepareTradingCalendar(t, dbPath, []string{"2026-01-17"})

	file := importer.UploadFile{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
		"2026/01/19,0901,5000,5001,5000,5001,1,1,0",
		"2026/01/20,2101,6000,6001,6000,6001,1,1,0",
	}))}

	h := newCaptureHandler()
	s := importer.NewTDXImportSession("s1", dbPath, []importer.UploadFile{file}, h)
	s.Start()
	_ = h.waitDone(t)

	adjusted := queryAdjustedTimeByDataTime(t, dbPath, "2026-01-20 21:01:00")
	if adjusted != "2026-01-19 21:01:00" {
		t.Fatalf("adjusted = %s, want 2026-01-19 21:01:00", adjusted)
	}
}

func TestAdjustedTimeFallsBackToCalendarWhenNoPreviousDayInFile(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	mustPrepareTradingCalendar(t, dbPath, []string{"2026-01-19"})

	file := importer.UploadFile{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
		"2026/01/20,2101,6000,6001,6000,6001,1,1,0",
	}))}

	h := newCaptureHandler()
	s := importer.NewTDXImportSession("s1", dbPath, []importer.UploadFile{file}, h)
	s.Start()
	_ = h.waitDone(t)

	adjusted := queryAdjustedTimeByDataTime(t, dbPath, "2026-01-20 21:01:00")
	if adjusted != "2026-01-19 21:01:00" {
		t.Fatalf("adjusted = %s, want 2026-01-19 21:01:00", adjusted)
	}
}

func TestAdjustedTimeRulesForEarlyAndBoundaryTimes(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "future_kline.db")
	mustPrepareTradingCalendar(t, dbPath, []string{"2026-01-19"})

	file := importer.UploadFile{Name: "28#SR2701.txt", Data: mustGBK(t, sampleTdxContent("SR2701", []string{
		"2026/01/20,0030,100,101,99,100,1,1,0",
		"2026/01/20,0800,110,111,109,110,1,1,0",
		"2026/01/20,1600,120,121,119,120,1,1,0",
		"2026/01/20,1601,130,131,129,130,1,1,0",
	}))}

	h := newCaptureHandler()
	s := importer.NewTDXImportSession("s1", dbPath, []importer.UploadFile{file}, h)
	s.Start()
	_ = h.waitDone(t)

	cases := []struct {
		dataTime string
		want     string
	}{
		{dataTime: "2026-01-20 00:30:00", want: "2026-01-20 00:30:00"},
		{dataTime: "2026-01-20 08:00:00", want: "2026-01-20 08:00:00"},
		{dataTime: "2026-01-20 16:00:00", want: "2026-01-20 16:00:00"},
		{dataTime: "2026-01-20 16:01:00", want: "2026-01-19 16:01:00"},
	}
	for _, tc := range cases {
		got := queryAdjustedTimeByDataTime(t, dbPath, tc.dataTime)
		if got != tc.want {
			t.Fatalf("adjusted for %s = %s, want %s", tc.dataTime, got, tc.want)
		}
	}
}

func sampleTdxContent(instrument string, rows []string) string {
	body := ""
	for _, row := range rows {
		body += row + "\n"
	}
	return fmt.Sprintf(
		"%s 示例 1分钟线 不复权\n      日期\t时间\t开盘\t最高\t最低\t收盘\t成交量\t持仓量\t结算价\n%s#数据来源:通达信\n",
		instrument,
		body,
	)
}

func sampleTdxContentNoMarker(instrument string, rows []string) string {
	body := ""
	for _, row := range rows {
		body += row + "\n"
	}
	return fmt.Sprintf(
		"%s sample header\nDate,Time,Open,High,Low,Close,Volume,OI,Settle\n%s#source:tdx\n",
		instrument,
		body,
	)
}

func mustGBK(t *testing.T, s string) []byte {
	t.Helper()
	out, err := simplifiedchinese.GBK.NewEncoder().Bytes([]byte(s))
	if err != nil {
		t.Fatalf("encode gbk failed: %v", err)
	}
	return out
}

func mustPrepareTradingCalendar(t *testing.T, dbPath string, openDays []string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS trading_calendar (trade_date TEXT PRIMARY KEY, is_open INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create trading_calendar failed: %v", err)
	}
	for _, day := range openDays {
		if _, err := db.Exec(`INSERT OR REPLACE INTO trading_calendar(trade_date, is_open) VALUES(?,1)`, day); err != nil {
			t.Fatalf("insert trading day failed: %v", err)
		}
	}
}

func queryAdjustedTimeByDataTime(t *testing.T, dbPath string, dataTime string) string {
	t.Helper()
	store, err := trader.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("NewKlineStore() error = %v", err)
	}
	defer store.Close()

	var adjusted string
	err = store.DB().QueryRow(
		`SELECT "AdjustedTime" FROM "future_kline_instrument_1m_sr" WHERE "DataTime"=? AND "InstrumentID"=? AND "Exchange"=? AND "Period"=?`,
		dataTime, "sr2701", "CZCE", "1m",
	).Scan(&adjusted)
	if err != nil {
		t.Fatalf("query adjusted row failed: %v", err)
	}
	return adjusted
}

type captureHandler struct {
	mu         sync.Mutex
	doneCh     chan importer.Progress
	conflictCh chan importer.ConflictRecord
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{
		doneCh:     make(chan importer.Progress, 1),
		conflictCh: make(chan importer.ConflictRecord, 4),
	}
}

func (h *captureHandler) OnProgress(importer.Progress) {}

func (h *captureHandler) OnConflict(c importer.ConflictRecord) {
	h.conflictCh <- c
}

func (h *captureHandler) OnDone(p importer.Progress) {
	h.doneCh <- p
}

func (h *captureHandler) OnError(error) {}

func (h *captureHandler) waitDone(t *testing.T) importer.Progress {
	t.Helper()
	select {
	case p := <-h.doneCh:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("wait done timeout")
		return importer.Progress{}
	}
}

func (h *captureHandler) waitConflict(t *testing.T) importer.ConflictRecord {
	t.Helper()
	select {
	case c := <-h.conflictCh:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("wait conflict timeout")
		return importer.ConflictRecord{}
	}
}
