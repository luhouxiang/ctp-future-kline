package calendar_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ctp-go-demo/internal/calendar"
	"golang.org/x/text/encoding/simplifiedchinese"
	_ "modernc.org/sqlite"
)

type mockSource struct {
	ann calendar.Announcement
	err error
}

func (m mockSource) FetchLatest(context.Context) (calendar.Announcement, error) {
	return m.ann, m.err
}

func TestNeedRefreshByHorizon(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})

	need, _, err := m.NeedRefreshByHorizon(60)
	if err != nil {
		t.Fatalf("NeedRefreshByHorizon error=%v", err)
	}
	if !need {
		t.Fatal("empty calendar should need refresh")
	}
}

func TestHorizonBoundary(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})
	today := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), 0, 0, 0, 0, time.Local)

	assertNeed := func(offset int, want bool) {
		day := today.AddDate(0, 0, offset).Format("2006-01-02")
		csv := fmt.Sprintf("trade_date,is_open\n%s,1\n", day)
		if err := m.ImportCSVBytes([]byte(csv), "test"); err != nil {
			t.Fatalf("ImportCSVBytes error=%v", err)
		}
		got, _, e := m.NeedRefreshByHorizon(60)
		if e != nil {
			t.Fatalf("NeedRefreshByHorizon error=%v", e)
		}
		if got != want {
			t.Fatalf("offset=%d got=%v want=%v", offset, got, want)
		}
	}
	assertNeed(59, true)
	assertNeed(60, false)
	assertNeed(61, false)
}

func TestRefreshIfNeededByHorizon(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "kline.db")
	year := time.Now().Year()
	closed := []time.Time{time.Date(year, 1, 1, 0, 0, 0, 0, time.Local)}
	m := calendar.NewManagerWithSource(dbPath, mockSource{ann: calendar.Announcement{Year: year, ClosedDays: closed, Meta: calendar.AnnouncementMeta{Title: "2026年休市安排", URL: "mock://shfe"}}})
	if err := m.RefreshIfNeeded(calendar.Config{AutoUpdateOnStart: true, MinFutureOpenDays: 60}); err != nil {
		t.Fatalf("RefreshIfNeeded error=%v", err)
	}
	st, err := m.Status(60)
	if err != nil {
		t.Fatalf("Status error=%v", err)
	}
	if st.LastSource != "shfe" {
		t.Fatalf("LastSource=%s", st.LastSource)
	}
	if st.LastAnnouncementYear != year {
		t.Fatalf("LastAnnouncementYear=%d want %d", st.LastAnnouncementYear, year)
	}
	if st.MaxOpenTradeDate == "" {
		t.Fatal("MaxOpenTradeDate should not be empty")
	}
}

func TestParseAnnouncementYearAndClosedDays(t *testing.T) {
	t.Parallel()
	html := `<html><head><title>关于2027年休市安排的公告</title></head><body>
2027年休市安排：1月1日（星期五）休市，1月20日至1月27日休市，2月4日起照常开市。
</body></html>`
	ann, err := calendar.ParseAnnouncementText(html, "https://example.com/a", "")
	if err != nil {
		t.Fatalf("ParseAnnouncementText error=%v", err)
	}
	if ann.Year != 2027 {
		t.Fatalf("year=%d", ann.Year)
	}
	if len(ann.ClosedDays) < 2 {
		t.Fatalf("closed days too few=%d", len(ann.ClosedDays))
	}
	for _, d := range ann.ClosedDays {
		if d.Format("2006-01-02") == "2027-02-04" {
			t.Fatalf("open day should not be in closed days: %s", d.Format("2006-01-02"))
		}
	}
}

func TestSHFESourceFetchLatest(t *testing.T) {
	t.Parallel()

	h := http.NewServeMux()
	h.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body><a href="/ann">关于2028年休市安排公告</a></body></html>`))
	})
	h.HandleFunc("/ann", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><head><title>关于2028年休市安排公告</title></head><body>2028年休市安排：2月10日至2月17日休市。</body></html>`))
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	source := calendar.NewSHFESource(srv.URL + "/index")
	ann, err := source.FetchLatest(context.Background())
	if err != nil {
		t.Fatalf("FetchLatest error=%v", err)
	}
	if ann.Year != 2028 {
		t.Fatalf("year=%d", ann.Year)
	}
	if len(ann.ClosedDays) == 0 {
		t.Fatal("closed days should not be empty")
	}
	if !strings.Contains(ann.Meta.URL, "/ann") {
		t.Fatalf("url=%s", ann.Meta.URL)
	}
}

func TestSHFESourceWAF(t *testing.T) {
	t.Parallel()
	h := http.NewServeMux()
	h.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><head><title>WEB 应用防火墙</title></head><body>challenge</body></html>`))
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	source := calendar.NewSHFESource(srv.URL + "/index")
	_, err := source.FetchLatest(context.Background())
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "waf") {
		t.Fatalf("expected waf error, got %v", err)
	}
}

func TestImportTDXDailyBytesValidDailyReplaceRange(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})
	if err := m.ImportCSVBytes([]byte(
		"trade_date,is_open\n2017-11-30,1\n2017-12-01,0\n2017-12-02,0\n2017-12-03,0\n2017-12-04,1\n",
	), "seed"); err != nil {
		t.Fatalf("seed import failed: %v", err)
	}

	input := "T001 通达信商品 日线 不复权\n" +
		"      日期\t开盘\t最高\t最低\t收盘\t成交量\t持仓量\t结算价\n" +
		"2017/12/01,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n" +
		"2017/12/03,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n" +
		"#数据来源:通达信\n"
	result, err := m.ImportTDXDailyBytes(mustGBK(t, input), "test:daily")
	if err != nil {
		t.Fatalf("ImportTDXDailyBytes error=%v", err)
	}
	if result.ImportedDays != 2 {
		t.Fatalf("ImportedDays=%d want 2", result.ImportedDays)
	}
	if result.DeletedRangeRows != 3 {
		t.Fatalf("DeletedRangeRows=%d want 3", result.DeletedRangeRows)
	}
	if result.MinDate != "2017-12-01" || result.MaxDate != "2017-12-03" {
		t.Fatalf("range=%s~%s", result.MinDate, result.MaxDate)
	}

	db := mustOpenDB(t, dbPath)
	defer db.Close()
	mustAssertIsOpen(t, db, "2017-11-30", 1)
	mustAssertIsOpen(t, db, "2017-12-01", 1)
	mustAssertNotExist(t, db, "2017-12-02")
	mustAssertIsOpen(t, db, "2017-12-03", 1)
	mustAssertIsOpen(t, db, "2017-12-04", 1)
}

func TestImportTDXDailyBytesNotDailyShouldFail(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})
	input := "T001 通达信商品 1分钟线 不复权\n" +
		"      日期\t时间\t开盘\t最高\t最低\t收盘\t成交量\t持仓量\t结算价\n" +
		"2017/12/01,0901,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n"
	if _, err := m.ImportTDXDailyBytes(mustGBK(t, input), "test:not_daily"); err == nil || !strings.Contains(err.Error(), "上传文件不是日线数据") {
		t.Fatalf("expected not daily error, got %v", err)
	}
}

func TestImportTDXDailyBytesDuplicateDatesDedup(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})
	input := "T001 通达信商品 日线 不复权\n" +
		"      日期\t开盘\t最高\t最低\t收盘\t成交量\t持仓量\t结算价\n" +
		"2017/12/01,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n" +
		"2017/12/01,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n" +
		"2017/12/02,162.09,163.56,162.09,163.49,20368338,23163746,0.00\n"
	result, err := m.ImportTDXDailyBytes(mustGBK(t, input), "test:dedup")
	if err != nil {
		t.Fatalf("ImportTDXDailyBytes error=%v", err)
	}
	if result.ImportedDays != 2 {
		t.Fatalf("ImportedDays=%d want 2", result.ImportedDays)
	}

	db := mustOpenDB(t, dbPath)
	defer db.Close()
	var c int
	if err := db.QueryRow(`SELECT COUNT(1) FROM trading_calendar WHERE trade_date >= ? AND trade_date <= ?`, "2017-12-01", "2017-12-02").Scan(&c); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if c != 2 {
		t.Fatalf("count=%d want 2", c)
	}
}

func TestImportTDXDailyBytesNoValidRowsShouldFail(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	m := calendar.NewManagerWithSource(dbPath, mockSource{})
	input := "T001 通达信商品 日线 不复权\n" +
		"      日期\t开盘\t最高\t最低\t收盘\t成交量\t持仓量\t结算价\n" +
		"#数据来源:通达信\n"
	if _, err := m.ImportTDXDailyBytes(mustGBK(t, input), "test:no_rows"); err == nil || !strings.Contains(err.Error(), "未解析到有效交易日") {
		t.Fatalf("expected no rows error, got %v", err)
	}

	db := mustOpenDB(t, dbPath)
	defer db.Close()
	var c int
	if err := db.QueryRow(`SELECT COUNT(1) FROM trading_calendar`).Scan(&c); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if c != 0 {
		t.Fatalf("count=%d want 0", c)
	}
}

func mustGBK(t *testing.T, s string) []byte {
	t.Helper()
	out, err := simplifiedchinese.GBK.NewEncoder().Bytes([]byte(s))
	if err != nil {
		t.Fatalf("encode gbk failed: %v", err)
	}
	return out
}

func mustOpenDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	return db
}

func mustAssertIsOpen(t *testing.T, db *sql.DB, day string, isOpen int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT is_open FROM trading_calendar WHERE trade_date = ?`, day).Scan(&got); err != nil {
		t.Fatalf("query day %s failed: %v", day, err)
	}
	if got != isOpen {
		t.Fatalf("day %s is_open=%d want %d", day, got, isOpen)
	}
}

func mustAssertNotExist(t *testing.T, db *sql.DB, day string) {
	t.Helper()
	var c int
	if err := db.QueryRow(`SELECT COUNT(1) FROM trading_calendar WHERE trade_date = ?`, day).Scan(&c); err != nil {
		t.Fatalf("query day %s failed: %v", day, err)
	}
	if c != 0 {
		t.Fatalf("day %s count=%d want 0", day, c)
	}
}
