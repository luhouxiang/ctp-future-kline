package trader

import (
	dbx "ctp-go-demo/internal/db"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/logger"
)

const (
	colInstrumentID = "InstrumentID"
	colExchange     = "Exchange"
	colTime         = "DataTime"
	colLegacyTime   = "Time"
	colAdjustedTime = "AdjustedTime"
	colPeriod       = "Period"
	colOpen         = "Open"
	colHigh         = "High"
	colLow          = "Low"
	colClose        = "Close"
	colVolume       = "Volume"
	colOpenInterest = "OpenInterest"
	colSettlement   = "SettlementPrice"

	instrumentTablePrefix    = "future_kline_instrument_1m_"
	l9TablePrefix            = "future_kline_l9_1m_"
	weightedIndexTablePrefix = "future_kline_weighted_index_" // legacy
	legacyTablePrefix        = "future_kline_"
	legacyL9TablePrefix      = "future_kline_l9_" // legacy
)

type minuteBar struct {
	Variety         string
	InstrumentID    string
	Exchange        string
	MinuteTime      time.Time
	AdjustedTime    time.Time
	Period          string
	Open            float64
	High            float64
	Low             float64
	Close           float64
	Volume          int64
	OpenInterest    float64
	SettlementPrice float64
}

type klineStore struct {
	db     *sql.DB
	mu     sync.Mutex
	tables map[string]struct{}
}

func newKlineStore(path string) (*klineStore, error) {
	logger.Info("kline store open begin", "db_path", path)
	start := time.Now()
	db, err := dbx.Open(path)
	if err != nil {
		logger.Error("kline store open failed", "db_path", path, "error", err)
		return nil, fmt.Errorf("open mysql failed: %w", err)
	}
	if err := withSQLiteBusyRetry(func() error { return migrateExistingKlineTables(db) }); err != nil {
		_ = db.Close()
		logger.Error("kline store migrate failed", "db_path", path, "error", err)
		return nil, err
	}
	logger.Info("kline store open success", "db_path", path, "elapsed_ms", time.Since(start).Milliseconds())
	return &klineStore{db: db, tables: make(map[string]struct{})}, nil
}

func (s *klineStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *klineStore) UpsertMinuteBar(bar minuteBar) error {
	tableName, err := tableNameForVariety(bar.Variety)
	if err != nil {
		return err
	}
	return s.upsertMinuteBarToTable(tableName, bar)
}

func (s *klineStore) UpsertL9MinuteBar(bar minuteBar) error {
	tableName, err := tableNameForL9Variety(bar.Variety)
	if err != nil {
		return err
	}
	return s.upsertMinuteBarToTable(tableName, bar)
}

func (s *klineStore) QueryMinuteBarsByVariety(variety string, minuteTime time.Time) ([]minuteBar, error) {
	tableName, err := tableNameForVariety(variety)
	if err != nil {
		return nil, err
	}
	if err := s.ensureTable(tableName); err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf(`
SELECT "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"
FROM "%s"
WHERE "%s" = ? AND "%s" = ?;`,
		colInstrumentID, colExchange, colTime, colAdjustedTime, colPeriod, colOpen, colHigh, colLow, colClose, colVolume, colOpenInterest, colSettlement,
		tableName,
		colTime, colPeriod,
	)

	rows, err := s.db.Query(stmt, minuteTime.Format("2006-01-02 15:04:00"), "1m")
	if err != nil {
		return nil, fmt.Errorf("query minute bars failed: %w", err)
	}
	defer rows.Close()

	var out []minuteBar
	for rows.Next() {
		var bar minuteBar
		var ts, adjusted time.Time
		if err := rows.Scan(
			&bar.InstrumentID,
			&bar.Exchange,
			&ts,
			&adjusted,
			&bar.Period,
			&bar.Open,
			&bar.High,
			&bar.Low,
			&bar.Close,
			&bar.Volume,
			&bar.OpenInterest,
			&bar.SettlementPrice,
		); err != nil {
			return nil, fmt.Errorf("scan minute bars failed: %w", err)
		}
		bar.Variety = normalizeVariety(variety)
		bar.MinuteTime = ts
		bar.AdjustedTime = bar.MinuteTime
		if !adjusted.IsZero() {
			bar.AdjustedTime = adjusted
		}
		out = append(out, bar)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate minute bars failed: %w", err)
	}
	return out, nil
}

func (s *klineStore) upsertMinuteBarToTable(tableName string, bar minuteBar) error {
	if err := s.ensureTable(tableName); err != nil {
		return err
	}
	storedInstrumentID := normalizeInstrumentIDForTable(bar.InstrumentID, tableName)
	if storedInstrumentID == "" {
		return fmt.Errorf("invalid instrument id %q for table %q", bar.InstrumentID, tableName)
	}

	stmt := fmt.Sprintf(`
INSERT INTO "%s"
("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s");`,
		tableName,
		colInstrumentID, colExchange, colTime, colAdjustedTime, colPeriod, colOpen, colHigh, colLow, colClose, colVolume, colOpenInterest, colSettlement,
		colAdjustedTime, colAdjustedTime,
		colOpen, colOpen,
		colHigh, colHigh,
		colLow, colLow,
		colClose, colClose,
		colVolume, colVolume,
		colOpenInterest, colOpenInterest,
		colSettlement, colSettlement,
	)

	_, err := s.db.Exec(
		stmt,
		storedInstrumentID,
		bar.Exchange,
		bar.MinuteTime.Format("2006-01-02 15:04:00"),
		chooseAdjustedTime(bar).Format("2006-01-02 15:04:00"),
		bar.Period,
		bar.Open,
		bar.High,
		bar.Low,
		bar.Close,
		bar.Volume,
		bar.OpenInterest,
		bar.SettlementPrice,
	)
	if err != nil {
		return fmt.Errorf("upsert kline row failed: %w", err)
	}
	return nil
}

func (s *klineStore) ensureTable(tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tables[tableName]; ok {
		return nil
	}

	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS "%s" (
  "%s" VARCHAR(32) NOT NULL,
  "%s" VARCHAR(16) NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" VARCHAR(8) NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" BIGINT NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  PRIMARY KEY ("%s", "%s", "%s", "%s")
);`,
		tableName,
		colInstrumentID,
		colExchange,
		colTime,
		colAdjustedTime,
		colPeriod,
		colOpen,
		colHigh,
		colLow,
		colClose,
		colVolume,
		colOpenInterest,
		colSettlement,
		colTime, colInstrumentID, colExchange, colPeriod,
	)
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("create kline table failed: %w", err)
	}
	if err := ensureDataTimeColumn(s.db, tableName); err != nil {
		return err
	}
	if err := ensureAdjustedTimeColumn(s.db, tableName); err != nil {
		return err
	}
	if err := ensureAdjustedTimeIndex(s.db, tableName); err != nil {
		return err
	}

	s.tables[tableName] = struct{}{}
	return nil
}

func ensureAdjustedTimeColumn(db *sql.DB, tableName string) error {
	hasCol, err := hasTableColumn(db, tableName, colAdjustedTime)
	if err != nil {
		return err
	}
	if !hasCol {
		alter := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" DATETIME`, tableName, colAdjustedTime)
		if _, err := db.Exec(alter); err != nil {
			return fmt.Errorf("add AdjustedTime column failed: %w", err)
		}
	}
	backfill := fmt.Sprintf(`UPDATE "%s" SET "%s" = "%s" WHERE "%s" IS NULL`, tableName, colAdjustedTime, colTime, colAdjustedTime)
	if _, err := db.Exec(backfill); err != nil {
		return fmt.Errorf("backfill AdjustedTime failed: %w", err)
	}
	return nil
}

func ensureDataTimeColumn(db *sql.DB, tableName string) error {
	hasDataTime, err := hasTableColumn(db, tableName, colTime)
	if err != nil {
		return err
	}
	if hasDataTime {
		return nil
	}
	hasLegacyTime, err := hasTableColumn(db, tableName, colLegacyTime)
	if err != nil {
		return err
	}
	if !hasLegacyTime {
		return fmt.Errorf("table %s missing both %s and %s columns", tableName, colTime, colLegacyTime)
	}
	rename := fmt.Sprintf(`ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"`, tableName, colLegacyTime, colTime)
	if _, err := db.Exec(rename); err != nil {
		return fmt.Errorf("rename %s to %s failed: %w", colLegacyTime, colTime, err)
	}
	return nil
}

func ensureAdjustedTimeIndex(db *sql.DB, tableName string) error {
	byInstrument := fmt.Sprintf(`CREATE INDEX "idx_%s_inst_period_adj" ON "%s"("%s","%s","%s" DESC)`,
		tableName, tableName, colInstrumentID, colPeriod, colAdjustedTime)
	if _, err := db.Exec(byInstrument); err != nil && !isDuplicateIndexErr(err) {
		return fmt.Errorf("create instrument adjusted index failed: %w", err)
	}
	byAdjusted := fmt.Sprintf(`CREATE INDEX "idx_%s_adj" ON "%s"("%s" DESC)`,
		tableName, tableName, colAdjustedTime)
	if _, err := db.Exec(byAdjusted); err != nil && !isDuplicateIndexErr(err) {
		return fmt.Errorf("create adjusted index failed: %w", err)
	}
	return nil
}

func migrateExistingKlineTables(db *sql.DB) error {
	logger.Info("kline migrate begin")
	if err := migrateLegacyTableNames(db); err != nil {
		return err
	}

	rows, err := db.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'future_kline_%'`)
	if err != nil {
		return fmt.Errorf("query existing kline tables failed: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scan existing kline table name failed: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate existing kline tables failed: %w", err)
	}

	for _, tableName := range tableNames {
		logger.Info("kline migrate table begin", "table", tableName)
		if err := ensureDataTimeColumn(db, tableName); err != nil {
			return fmt.Errorf("migrate table %q data time failed: %w", tableName, err)
		}
		if err := ensureAdjustedTimeColumn(db, tableName); err != nil {
			return fmt.Errorf("migrate table %q failed: %w", tableName, err)
		}
		if err := ensureAdjustedTimeIndex(db, tableName); err != nil {
			return fmt.Errorf("migrate adjusted index for table %q failed: %w", tableName, err)
		}
		if err := normalizeInstrumentIDColumn(db, tableName); err != nil {
			return fmt.Errorf("normalize instrument id for table %q failed: %w", tableName, err)
		}
		logger.Info("kline migrate table done", "table", tableName)
	}
	logger.Info("kline migrate done", "table_count", len(tableNames))
	return nil
}

func migrateLegacyTableNames(db *sql.DB) error {
	rows, err := db.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'future_kline_%'
ORDER BY table_name`)
	if err != nil {
		return fmt.Errorf("query kline tables for rename failed: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scan table name for rename failed: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table names for rename failed: %w", err)
	}

	for _, oldName := range tableNames {
		newName, ok := legacyToNewTableName(oldName)
		if !ok || newName == oldName {
			continue
		}
		if tableExists(db, newName) {
			continue
		}
		stmt := fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, oldName, newName)
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("rename table %q to %q failed: %w", oldName, newName, err)
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName string) bool {
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name=?`, tableName).Scan(&cnt); err != nil {
		return false
	}
	return cnt > 0
}

func legacyToNewTableName(tableName string) (string, bool) {
	if strings.HasPrefix(tableName, l9TablePrefix) {
		return "", false
	}
	if strings.HasPrefix(tableName, legacyL9TablePrefix) {
		variety := sanitizeSQLIdent(strings.TrimPrefix(tableName, legacyL9TablePrefix))
		if variety == "" {
			return "", false
		}
		return l9TablePrefix + variety, true
	}
	if strings.HasPrefix(tableName, weightedIndexTablePrefix) {
		variety := sanitizeSQLIdent(strings.TrimPrefix(tableName, weightedIndexTablePrefix))
		if variety == "" {
			return "", false
		}
		return l9TablePrefix + variety, true
	}
	if strings.HasPrefix(tableName, legacyTablePrefix) &&
		!strings.HasPrefix(tableName, instrumentTablePrefix) &&
		!strings.HasPrefix(tableName, weightedIndexTablePrefix) &&
		!strings.HasPrefix(tableName, l9TablePrefix) {
		variety := sanitizeSQLIdent(strings.TrimPrefix(tableName, legacyTablePrefix))
		if variety == "" {
			return "", false
		}
		return instrumentTablePrefix + variety, true
	}
	return "", false
}

func normalizeInstrumentIDColumn(db *sql.DB, tableName string) error {
	// Keep full instrument id text and normalize only case/whitespace.
	stmt := fmt.Sprintf(`UPDATE "%s" SET "%s"=lower(trim("%s"))`,
		tableName, colInstrumentID, colInstrumentID)
	if _, err := db.Exec(stmt); err != nil {
		return err
	}
	// Backfill legacy l9 rows that used plain "l9" into "<variety>l9".
	if strings.HasPrefix(tableName, l9TablePrefix) || strings.HasPrefix(tableName, weightedIndexTablePrefix) || strings.HasPrefix(tableName, legacyL9TablePrefix) {
		variety := extractVarietyFromTableName(tableName)
		if variety != "" {
			expected := variety + "l9"
			fixStmt := fmt.Sprintf(`UPDATE "%s" SET "%s"=? WHERE "%s"='l9' OR "%s"=''`,
				tableName, colInstrumentID, colInstrumentID, colInstrumentID)
			if _, err := db.Exec(fixStmt, expected); err != nil {
				return err
			}
		}
	}
	return nil
}

func hasTableColumn(db *sql.DB, tableName string, column string) (bool, error) {
	rows, err := db.Query(`
SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?`, tableName)
	if err != nil {
		return false, fmt.Errorf("query table info failed: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, fmt.Errorf("scan table info failed: %w", err)
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table info failed: %w", err)
	}
	return false, nil
}

func chooseAdjustedTime(bar minuteBar) time.Time {
	if !bar.AdjustedTime.IsZero() {
		return bar.AdjustedTime
	}
	return bar.MinuteTime
}

func withSQLiteBusyRetry(fn func() error) error {
	const maxAttempts = 8
	baseDelay := 250 * time.Millisecond
	maxDelay := 3 * time.Second
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) || attempt == maxAttempts-1 {
			return err
		}
		delay := baseDelay * time.Duration(1<<attempt)
		if delay > maxDelay {
			delay = maxDelay
		}
		jitter := time.Duration(rand.Int63n(int64(delay / 5)))
		time.Sleep(delay + jitter)
	}
	return nil
}

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "lock wait timeout") ||
		strings.Contains(msg, "deadlock found") ||
		strings.Contains(msg, "error 1205") ||
		strings.Contains(msg, "error 1213")
}

func isDuplicateIndexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}

func tableNameForVariety(variety string) (string, error) {
	name := normalizeVariety(variety)
	if name == "" {
		return "", fmt.Errorf("invalid variety for table name: %q", variety)
	}
	return instrumentTablePrefix + sanitizeSQLIdent(name), nil
}

func tableNameForL9Variety(variety string) (string, error) {
	name := normalizeVariety(variety)
	if name == "" {
		return "", fmt.Errorf("invalid variety for l9 table name: %q", variety)
	}
	return l9TablePrefix + sanitizeSQLIdent(name), nil
}

func sanitizeSQLIdent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func extractVarietyFromTableName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, instrumentTablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, instrumentTablePrefix))
	case strings.HasPrefix(tableName, l9TablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, l9TablePrefix))
	case strings.HasPrefix(tableName, weightedIndexTablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, weightedIndexTablePrefix))
	case strings.HasPrefix(tableName, legacyL9TablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, legacyL9TablePrefix))
	case strings.HasPrefix(tableName, legacyTablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, legacyTablePrefix))
	default:
		return ""
	}
}

func normalizeInstrumentIDForTable(instrumentID string, tableName string) string {
	_ = tableName
	return strings.ToLower(strings.TrimSpace(instrumentID))
}

func normalizeContractID(instrumentID string, variety string) string {
	s := strings.ToLower(strings.TrimSpace(instrumentID))
	if s == "" {
		return ""
	}
	variety = strings.ToLower(strings.TrimSpace(variety))
	if variety != "" && strings.HasPrefix(s, variety) {
		s = strings.TrimPrefix(s, variety)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	leading := 0
	for _, ch := range s {
		if ch >= 'a' && ch <= 'z' {
			leading++
			continue
		}
		break
	}
	if leading > 0 {
		s = s[leading:]
	}
	return s
}
