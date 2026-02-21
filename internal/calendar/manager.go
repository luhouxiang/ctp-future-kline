package calendar

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/logger"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const dateLayout = "2006-01-02"

type Config struct {
	AutoUpdateOnStart  bool
	MinFutureOpenDays  int
	SourceURL          string
	SourceCSVPath      string
	CheckIntervalHours int
	BrowserFallback    bool
	BrowserPath        string
	BrowserHeadless    bool
}

type Status struct {
	TotalRows            int    `json:"total_rows"`
	OpenRows             int    `json:"open_rows"`
	MinDate              string `json:"min_date"`
	MaxDate              string `json:"max_date"`
	MaxOpenTradeDate     string `json:"max_open_trade_date"`
	LastCheckAt          string `json:"last_check_at"`
	LastSuccessAt        string `json:"last_success_at"`
	LastSource           string `json:"last_source"`
	LastError            string `json:"last_error"`
	NeedFutureDays       int    `json:"need_future_days"`
	DaysToHorizon        int    `json:"days_to_horizon"`
	NeedsAutoUpdate      bool   `json:"needs_auto_update"`
	LastAnnouncementYear int    `json:"last_announcement_year"`
}

type TDXDailyImportResult struct {
	ImportedDays     int    `json:"imported_days"`
	DeletedRangeRows int    `json:"deleted_range_rows"`
	MinDate          string `json:"min_date"`
	MaxDate          string `json:"max_date"`
	LastSource       string `json:"last_source"`
	TableName        string `json:"table_name"`
	WriteSuccess     bool   `json:"write_success"`
}

type Manager struct {
	dbPath string
	source SHFESource
}

func NewManager(dbPath string) *Manager {
	return &Manager{dbPath: dbPath, source: NewSHFESource("")}
}

func NewManagerWithSource(dbPath string, source SHFESource) *Manager {
	if source == nil {
		source = NewSHFESource("")
	}
	return &Manager{dbPath: dbPath, source: source}
}

func (m *Manager) EnsureOnStart(cfg Config) error {
	if !cfg.AutoUpdateOnStart {
		return nil
	}
	if err := m.RefreshIfNeeded(cfg); err != nil {
		// Startup must not fail calendar refresh; fetch error is already logged by refresh flow.
		logger.Info("calendar ensure on start skipped", "reason", err.Error())
		return nil
	}
	return nil
}

func (m *Manager) NeedRefreshByHorizon(horizonDays int) (bool, Status, error) {
	db, err := dbx.Open(m.dbPath)
	if err != nil {
		return false, Status{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	if err := ensureSchema(db); err != nil {
		return false, Status{}, err
	}
	st, err := m.statusFromDB(db, horizonDays)
	if err != nil {
		return false, Status{}, err
	}
	return st.NeedsAutoUpdate, *st, nil
}

func (m *Manager) RefreshIfNeeded(cfg Config) error {
	db, err := dbx.Open(m.dbPath)
	if err != nil {
		return fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	if err := ensureSchema(db); err != nil {
		return err
	}

	now := time.Now()
	_ = setMeta(db, "last_check_at", now.Format(time.RFC3339))

	horizon := cfg.MinFutureOpenDays
	if horizon <= 0 {
		horizon = 60
	}
	st, err := m.statusFromDB(db, horizon)
	if err != nil {
		return err
	}
	if !st.NeedsAutoUpdate {
		return nil
	}

	// Optional manual priority source.
	if strings.TrimSpace(cfg.SourceCSVPath) != "" {
		if err := m.ImportCSVFile(cfg.SourceCSVPath); err != nil {
			_ = setMeta(db, "last_error", err.Error())
			return err
		}
		return nil
	}
	if strings.TrimSpace(cfg.SourceURL) != "" && strings.HasSuffix(strings.ToLower(strings.TrimSpace(cfg.SourceURL)), ".csv") {
		if err := m.ImportFromURL(cfg.SourceURL); err != nil {
			_ = setMeta(db, "last_error", err.Error())
			return err
		}
		return nil
	}

	source := m.source
	if source == nil {
		source = NewSHFESource("")
	}
	// Keep injected sources (tests/mocks) untouched.
	if _, ok := source.(*shfeSource); ok {
		source = NewSHFESourceWithOptions(cfg.SourceURL, SourceOptions{
			EnableBrowserFallback: cfg.BrowserFallback,
			BrowserPath:           cfg.BrowserPath,
			BrowserHeadless:       cfg.BrowserHeadless,
		})
	}
	ann, err := source.FetchLatest(context.Background())
	if err != nil {
		_ = setMeta(db, "last_error", err.Error())
		logger.Error("fetch shfe announcement failed", "error", err)
		return err
	}
	if err := m.applyAnnouncement(db, ann); err != nil {
		_ = setMeta(db, "last_error", err.Error())
		return err
	}
	_ = setMeta(db, "last_success_at", time.Now().Format(time.RFC3339))
	_ = setMeta(db, "last_source", "shfe")
	_ = setMeta(db, "last_announcement_year", strconv.Itoa(ann.Year))
	_ = setMeta(db, "last_announcement_url", ann.Meta.URL)
	_ = setMeta(db, "last_error", "")
	return nil
}

func (m *Manager) ImportCSVFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty csv path")
	}
	abs := path
	if !filepath.IsAbs(abs) {
		abs, _ = filepath.Abs(abs)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read csv file failed: %w", err)
	}
	return m.ImportCSVBytes(data, "file:"+abs)
}

func (m *Manager) ImportFromURL(url string) error {
	url = strings.TrimSpace(url)
	if url == "" {
		return fmt.Errorf("empty source url")
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("http get failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http status not ok: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body failed: %w", err)
	}
	return m.ImportCSVBytes(body, "url:"+url)
}

func (m *Manager) ImportCSVBytes(data []byte, source string) error {
	db, err := dbx.Open(m.dbPath)
	if err != nil {
		return fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	if err := ensureSchema(db); err != nil {
		return err
	}

	rows, err := parseCSVRows(data)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`
INSERT INTO trading_calendar(trade_date,is_open,updated_at)
VALUES(?,?,?)
ON DUPLICATE KEY UPDATE
  is_open=VALUES(is_open),
  updated_at=VALUES(updated_at)`)
	if err != nil {
		return fmt.Errorf("prepare upsert failed: %w", err)
	}
	defer stmt.Close()

	now := time.Now().Format(time.RFC3339)
	for _, row := range rows {
		if _, err := stmt.Exec(row.TradeDate, row.IsOpen, now); err != nil {
			return fmt.Errorf("upsert trading_calendar failed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}

	_ = setMeta(db, "last_success_at", time.Now().Format(time.RFC3339))
	_ = setMeta(db, "last_source", source)
	_ = setMeta(db, "last_error", "")
	logger.Info("calendar import finished", "rows", len(rows), "source", source)
	return nil
}

func (m *Manager) ImportTDXDailyBytes(data []byte, source string) (TDXDailyImportResult, error) {
	db, err := dbx.Open(m.dbPath)
	if err != nil {
		return TDXDailyImportResult{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	if err := ensureSchema(db); err != nil {
		return TDXDailyImportResult{}, err
	}

	dates, err := parseTDXDailyDates(data)
	if err != nil {
		return TDXDailyImportResult{}, err
	}
	sort.Strings(dates)
	minDate := dates[0]
	maxDate := dates[len(dates)-1]
	logger.Info("calendar tdx daily parsed",
		"first_trading_day", minDate,
		"last_trading_day", maxDate,
		"total_trading_days", len(dates),
		"target_table", "trading_calendar",
		"source", source,
	)

	tx, err := db.Begin()
	if err != nil {
		return TDXDailyImportResult{}, fmt.Errorf("begin tx failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	delRes, err := tx.Exec(`DELETE FROM trading_calendar WHERE trade_date >= ? AND trade_date <= ?`, minDate, maxDate)
	if err != nil {
		logger.Error("calendar tdx daily write failed", "target_table", "trading_calendar", "write_success", false, "error", err)
		return TDXDailyImportResult{}, fmt.Errorf("delete trading_calendar range failed: %w", err)
	}
	deletedRows := int64(0)
	if delRes != nil {
		if affected, e := delRes.RowsAffected(); e == nil {
			deletedRows = affected
		}
	}

	stmt, err := tx.Prepare(`
INSERT INTO trading_calendar(trade_date,is_open,updated_at)
VALUES(?,?,?)
ON DUPLICATE KEY UPDATE
  is_open=VALUES(is_open),
  updated_at=VALUES(updated_at)`)
	if err != nil {
		logger.Error("calendar tdx daily write failed", "target_table", "trading_calendar", "write_success", false, "error", err)
		return TDXDailyImportResult{}, fmt.Errorf("prepare upsert failed: %w", err)
	}
	defer stmt.Close()

	now := time.Now().Format(time.RFC3339)
	for _, d := range dates {
		if _, err := stmt.Exec(d, 1, now); err != nil {
			logger.Error("calendar tdx daily write failed", "target_table", "trading_calendar", "write_success", false, "error", err)
			return TDXDailyImportResult{}, fmt.Errorf("upsert trading_calendar failed: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		logger.Error("calendar tdx daily write failed", "target_table", "trading_calendar", "write_success", false, "error", err)
		return TDXDailyImportResult{}, fmt.Errorf("commit tx failed: %w", err)
	}

	_ = setMeta(db, "last_success_at", time.Now().Format(time.RFC3339))
	_ = setMeta(db, "last_source", source)
	_ = setMeta(db, "last_error", "")

	result := TDXDailyImportResult{
		ImportedDays:     len(dates),
		DeletedRangeRows: int(deletedRows),
		MinDate:          minDate,
		MaxDate:          maxDate,
		LastSource:       source,
		TableName:        "trading_calendar",
		WriteSuccess:     true,
	}
	logger.Info("calendar tdx daily import finished",
		"imported_days", result.ImportedDays,
		"deleted_range_rows", result.DeletedRangeRows,
		"min_date", result.MinDate,
		"max_date", result.MaxDate,
		"target_table", result.TableName,
		"write_success", result.WriteSuccess,
		"source", source,
	)
	return result, nil
}

func (m *Manager) Status(minFutureOpenDays int) (Status, error) {
	db, err := dbx.Open(m.dbPath)
	if err != nil {
		return Status{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	if err := ensureSchema(db); err != nil {
		return Status{}, err
	}
	st, err := m.statusFromDB(db, minFutureOpenDays)
	if err != nil {
		return Status{}, err
	}
	return *st, nil
}

func (m *Manager) applyAnnouncement(db *sql.DB, ann Announcement) error {
	if ann.Year < 2000 || ann.Year > 2100 {
		return fmt.Errorf("invalid announcement year: %d", ann.Year)
	}
	closed := make(map[string]bool, len(ann.ClosedDays))
	for _, d := range ann.ClosedDays {
		closed[d.Format(dateLayout)] = true
	}
	start := time.Date(ann.Year, 1, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(ann.Year, 12, 31, 0, 0, 0, 0, time.Local)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`
INSERT INTO trading_calendar(trade_date,is_open,updated_at)
VALUES(?,?,?)
ON DUPLICATE KEY UPDATE
  is_open=VALUES(is_open),
  updated_at=VALUES(updated_at)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	ts := time.Now().Format(time.RFC3339)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		isOpen := 1
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			isOpen = 0
		}
		if closed[d.Format(dateLayout)] {
			isOpen = 0
		}
		if _, err := stmt.Exec(d.Format(dateLayout), isOpen, ts); err != nil {
			return fmt.Errorf("upsert trading day failed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if ann.Meta.URL != "" {
		_ = setMeta(db, "shfe.announcement_url."+strconv.Itoa(ann.Year), ann.Meta.URL)
	}
	if ann.Meta.Title != "" {
		_ = setMeta(db, "shfe.announcement_title."+strconv.Itoa(ann.Year), ann.Meta.Title)
	}
	if !ann.Meta.PublishedAt.IsZero() {
		_ = setMeta(db, "shfe.announcement_published_at."+strconv.Itoa(ann.Year), ann.Meta.PublishedAt.Format(time.RFC3339))
	}
	logger.Info("calendar announcement applied", "year", ann.Year, "closed_days", len(ann.ClosedDays), "url", ann.Meta.URL)
	return nil
}

type csvRow struct {
	TradeDate string
	IsOpen    int
}

func parseCSVRows(data []byte) ([]csvRow, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	all, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv failed: %w", err)
	}
	rows := make([]csvRow, 0, len(all))
	for i, rec := range all {
		if len(rec) == 0 {
			continue
		}
		first := strings.TrimSpace(rec[0])
		if first == "" {
			continue
		}
		lower := strings.ToLower(first)
		if i == 0 && (strings.Contains(lower, "date") || strings.Contains(lower, "trade_date")) {
			continue
		}
		dateText := normalizeDate(first)
		if dateText == "" {
			continue
		}
		isOpen := 1
		if len(rec) >= 2 {
			v := strings.TrimSpace(rec[1])
			if v != "" {
				n, convErr := strconv.Atoi(v)
				if convErr != nil {
					return nil, fmt.Errorf("invalid is_open value at row %d", i+1)
				}
				if n == 0 {
					isOpen = 0
				}
			}
		}
		rows = append(rows, csvRow{TradeDate: dateText, IsOpen: isOpen})
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no valid calendar rows")
	}
	return rows, nil
}

func normalizeDate(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	v = strings.ReplaceAll(v, "/", "-")
	layouts := []string{"2006-01-02", "20060102"}
	for _, layout := range layouts {
		if ts, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return ts.Format(dateLayout)
		}
	}
	return ""
}

func parseTDXDailyDates(data []byte) ([]string, error) {
	text := decodeTextPreferGBK(data)
	lines := splitLines(text)
	if len(lines) == 0 {
		return nil, fmt.Errorf("未解析到有效交易日")
	}
	header := strings.TrimSpace(strings.TrimPrefix(lines[0], "\ufeff"))
	if !strings.Contains(header, "日线") {
		return nil, fmt.Errorf("上传文件不是日线数据（需包含“日线”标识）")
	}

	dates := make(map[string]struct{})
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || i <= 1 {
			continue
		}
		if strings.HasPrefix(line, "#数据来源") {
			break
		}
		fields := strings.Split(line, ",")
		if len(fields) == 0 {
			continue
		}
		dateText := strings.TrimSpace(fields[0])
		if dateText == "" {
			continue
		}
		normalized := normalizeDate(dateText)
		if normalized == "" {
			if looksLikeDateToken(dateText) {
				return nil, fmt.Errorf("日线数据行格式不合法: 行号 %d", i+1)
			}
			continue
		}
		if len(fields) < 2 {
			return nil, fmt.Errorf("日线数据行格式不合法: 行号 %d", i+1)
		}
		dates[normalized] = struct{}{}
	}
	if len(dates) == 0 {
		return nil, fmt.Errorf("未解析到有效交易日")
	}
	out := make([]string, 0, len(dates))
	for d := range dates {
		out = append(out, d)
	}
	return out, nil
}

func decodeTextPreferGBK(data []byte) string {
	reader := transform.NewReader(bytes.NewReader(data), simplifiedchinese.GBK.NewDecoder())
	if out, err := io.ReadAll(reader); err == nil {
		text := string(out)
		if strings.Contains(text, "日线") || strings.Contains(text, "#数据来源") {
			return text
		}
	}
	return string(data)
}

func splitLines(text string) []string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return strings.Split(text, "\n")
}

func looksLikeDateToken(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	for _, r := range token {
		if (r >= '0' && r <= '9') || r == '/' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func ensureSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS trading_calendar(
  trade_date DATE PRIMARY KEY,
  is_open TINYINT NOT NULL,
  updated_at DATETIME NOT NULL
)`,
		`CREATE INDEX idx_trading_calendar_open_date ON trading_calendar(is_open, trade_date)`,
		`CREATE TABLE IF NOT EXISTS trading_calendar_meta(
  k VARCHAR(191) PRIMARY KEY,
  v TEXT NOT NULL
)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			if strings.Contains(strings.ToLower(stmt), "create index") && isDuplicateIndexErr(err) {
				continue
			}
			return fmt.Errorf("ensure trading calendar schema failed: %w", err)
		}
	}
	return nil
}

func isDuplicateIndexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}

func setMeta(db *sql.DB, k string, v string) error {
	_, err := db.Exec(`
INSERT INTO trading_calendar_meta(k,v) VALUES(?,?)
ON DUPLICATE KEY UPDATE v=VALUES(v)`, k, v)
	return err
}

func getMeta(db *sql.DB, k string) string {
	var v string
	if err := db.QueryRow(`SELECT v FROM trading_calendar_meta WHERE k=?`, k).Scan(&v); err != nil {
		return ""
	}
	return v
}

func getMetaTime(db *sql.DB, k string) time.Time {
	v := getMeta(db, k)
	if v == "" {
		return time.Time{}
	}
	out, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}
	}
	return out
}

func (m *Manager) statusFromDB(db *sql.DB, minFutureOpenDays int) (*Status, error) {
	if minFutureOpenDays <= 0 {
		minFutureOpenDays = 60
	}
	st := &Status{NeedFutureDays: minFutureOpenDays, DaysToHorizon: -1}
	if err := db.QueryRow(`SELECT COUNT(1) FROM trading_calendar`).Scan(&st.TotalRows); err != nil {
		return nil, err
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM trading_calendar WHERE is_open=1`).Scan(&st.OpenRows); err != nil {
		return nil, err
	}
	_ = db.QueryRow(`SELECT IFNULL(DATE_FORMAT(MIN(trade_date), '%Y-%m-%d'), '') FROM trading_calendar`).Scan(&st.MinDate)
	_ = db.QueryRow(`SELECT IFNULL(DATE_FORMAT(MAX(trade_date), '%Y-%m-%d'), '') FROM trading_calendar`).Scan(&st.MaxDate)
	_ = db.QueryRow(`SELECT IFNULL(DATE_FORMAT(MAX(trade_date), '%Y-%m-%d'), '') FROM trading_calendar WHERE is_open=1`).Scan(&st.MaxOpenTradeDate)
	st.LastCheckAt = getMeta(db, "last_check_at")
	st.LastSuccessAt = getMeta(db, "last_success_at")
	st.LastSource = getMeta(db, "last_source")
	st.LastError = getMeta(db, "last_error")
	if y, err := strconv.Atoi(getMeta(db, "last_announcement_year")); err == nil {
		st.LastAnnouncementYear = y
	}

	today := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), 0, 0, 0, 0, time.Local)
	if st.MaxOpenTradeDate == "" {
		st.NeedsAutoUpdate = true
		return st, nil
	}
	maxDay, err := time.ParseInLocation(dateLayout, st.MaxOpenTradeDate, time.Local)
	if err != nil {
		st.NeedsAutoUpdate = true
		return st, nil
	}
	st.DaysToHorizon = int(maxDay.Sub(today).Hours() / 24)
	st.NeedsAutoUpdate = st.DaysToHorizon < minFutureOpenDays
	return st, nil
}
