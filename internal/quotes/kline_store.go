// kline_store.go 封装 K 线存储层。
// 它负责管理 1m、L9、mm 等表结构，并提供 minute bar 的 upsert/query 能力，
// 是行情聚合结果落库的统一入口。
package quotes

import (
	dbx "ctp-future-kline/internal/db"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/logger"
)

const (
	colInstrumentID = "InstrumentID"
	colExchange     = "Exchange"
	colTime         = "DataTime"
	colAdjustedTime = "AdjustedTime"
	colPeriod       = "Period"
	colOpen         = "Open"
	colHigh         = "High"
	colLow          = "Low"
	colClose        = "Close"
	colVolume       = "Volume"
	colOpenInterest = "OpenInterest"
	colSettlement   = "SettlementPrice"

	instrumentTablePrefix   = "future_kline_instrument_1m_"
	l9TablePrefix           = "future_kline_l9_1m_"
	instrumentMMTablePrefix = "future_kline_instrument_mm_"
	l9MMTablePrefix         = "future_kline_l9_mm_"
)

// minuteBar 是系统内部统一使用的分钟线结构。
//
// 其中：
// 1. MinuteTime 表示标签分钟时间键，对应落库 DataTime
// 2. AdjustedTime 表示实际交易日期下的同标签分钟时间键
// 3. Period 对于实时聚合固定为 "1m"，在写入 mm 表时由聚合器另行生成
type minuteBar struct {
	Variety              string
	InstrumentID         string
	Exchange             string
	Replay               bool
	MinuteTime           time.Time
	AdjustedTime         time.Time
	SourceReceivedAt     time.Time
	FlushStartedAt       time.Time
	SideEffectEnqueuedAt time.Time
	Period               string
	Open                 float64
	High                 float64
	Low                  float64
	Close                float64
	Volume               int64
	OpenInterest         float64
	SettlementPrice      float64
}

// klineStore 封装分钟线、L9 分钟线和相关表结构的写入逻辑。
type klineStore struct {
	db     *sql.DB
	mu     sync.Mutex
	tables map[string]struct{}
}

// newKlineStore 打开数据库并确保分钟线相关表结构已经准备好。
func newKlineStore(path string) (*klineStore, error) {
	logger.Info("kline store open begin", "db_path", path)
	start := time.Now()
	db, err := dbx.Open(path)
	if err != nil {
		logger.Error("kline store open failed", "db_path", path, "error", err)
		return nil, fmt.Errorf("open mysql failed: %w", err)
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

// UpsertMinuteBar 把普通合约的 1m bar 写入对应品种表。
func (s *klineStore) UpsertMinuteBar(bar minuteBar) error {
	tableName, err := tableNameForVariety(bar.Variety)
	if err != nil {
		return err
	}
	return s.upsertMinuteBarToTable(tableName, bar)
}

// UpsertL9MinuteBar 把 L9 主连 1m bar 写入对应品种的 L9 表。
func (s *klineStore) UpsertL9MinuteBar(bar minuteBar) error {
	tableName, err := tableNameForL9Variety(bar.Variety)
	if err != nil {
		return err
	}
	return s.upsertMinuteBarToTable(tableName, bar)
}

// QueryMinuteBarsByVariety 查询某个品种在指定分钟的全部合约 bar，供 L9 聚合使用。
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

// upsertMinuteBarToTable 是统一的分钟线写表实现。
// 普通合约 1m 表和 L9 1m 表都会走这里，只是目标表名不同。
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
	if err := ensureAdjustedTimeIndex(s.db, tableName); err != nil {
		return err
	}

	s.tables[tableName] = struct{}{}
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

// chooseAdjustedTime 选择写入数据库时使用的 AdjustedTime。
// 如果调用方未提供 AdjustedTime，则退回到 MinuteTime，保证时间键始终存在。
func chooseAdjustedTime(bar minuteBar) time.Time {
	if !bar.AdjustedTime.IsZero() {
		return bar.AdjustedTime
	}
	return bar.MinuteTime
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

// tableNameForL9Variety 生成某个品种的 L9 1m 表名。
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
	case strings.HasPrefix(tableName, instrumentMMTablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, instrumentMMTablePrefix))
	case strings.HasPrefix(tableName, l9MMTablePrefix):
		return sanitizeSQLIdent(strings.TrimPrefix(tableName, l9MMTablePrefix))
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
