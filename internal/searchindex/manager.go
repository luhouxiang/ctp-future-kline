package searchindex

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/logger"
)

const timeLayout = "2006-01-02 15:04:05"

type Item struct {
	TableName  string    `json:"table_name"`
	Symbol     string    `json:"symbol"`
	SymbolNorm string    `json:"symbol_norm"`
	Variety    string    `json:"variety"`
	Exchange   string    `json:"exchange"`
	Kind       string    `json:"kind"`
	MinTime    time.Time `json:"min_time"`
	MaxTime    time.Time `json:"max_time"`
	BarCount   int64     `json:"bar_count"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Manager struct {
	dbPath          string
	refreshInterval time.Duration

	mu          sync.Mutex
	cond        *sync.Cond
	refreshing  bool
	lastRefresh time.Time
}

const (
	instrumentTablePrefix    = "future_kline_instrument_1m_"
	l9TablePrefix            = "future_kline_l9_1m_"
	weightedIndexTablePrefix = "future_kline_weighted_index_" // legacy
	legacyTablePrefix        = "future_kline_"
	legacyL9TablePrefix      = "future_kline_l9_" // legacy
)

func NewManager(dbPath string, refreshInterval time.Duration) *Manager {
	if refreshInterval <= 0 {
		refreshInterval = 30 * time.Second
	}
	m := &Manager{
		dbPath:          dbPath,
		refreshInterval: refreshInterval,
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *Manager) Invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastRefresh = time.Time{}
}

func (m *Manager) EnsureFresh() error {
	m.mu.Lock()
	for {
		needRefresh := time.Since(m.lastRefresh) >= m.refreshInterval || m.lastRefresh.IsZero()
		if !needRefresh {
			m.mu.Unlock()
			return nil
		}
		if !m.refreshing {
			m.refreshing = true
			break
		}
		m.cond.Wait()
	}

	m.mu.Unlock()
	if err := m.rebuild(); err != nil {
		m.mu.Lock()
		m.refreshing = false
		m.cond.Broadcast()
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.lastRefresh = time.Now()
	m.refreshing = false
	m.cond.Broadcast()
	m.mu.Unlock()
	return nil
}

func (m *Manager) Search(keyword string, start time.Time, end time.Time, page int, pageSize int) ([]Item, int, error) {
	if err := m.EnsureFresh(); err != nil {
		return nil, 0, err
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}

	db, err := m.openDB()
	if err != nil {
		return nil, 0, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	keyword = strings.ToLower(strings.TrimSpace(keyword))
	whereClauses := []string{
		"min_time <= ?",
		"max_time >= ?",
	}
	args := []any{
		end.Format(timeLayout),
		start.Format(timeLayout),
	}
	if keyword != "" {
		kw := "%" + keyword + "%"
		whereClauses = append(whereClauses, "(symbol_norm LIKE ? OR variety LIKE ? OR lower(exchange) LIKE ? OR lower(table_name) LIKE ?)")
		args = append(args, kw, kw, kw, kw)
	}
	whereSQL := strings.Join(whereClauses, "\n  AND ")

	countSQL := `
SELECT COUNT(1)
FROM kline_search_index
WHERE ` + whereSQL

	var total int
	logger.Info("search index count query",
		"sql", renderExecutableSQL(countSQL, args...),
	)
	if err := db.QueryRow(countSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count search index failed: %w", err)
	}
	logger.Info("search index count result", "total", total)

	offset := (page - 1) * pageSize
	querySQL := `
SELECT table_name, symbol, symbol_norm, variety, exchange, kind, min_time, max_time, bar_count, updated_at
FROM kline_search_index
WHERE ` + whereSQL + `
ORDER BY max_time DESC, symbol ASC
LIMIT ? OFFSET ?`
	queryArgs := append(append([]any{}, args...), pageSize, offset)

	rows, err := db.Query(querySQL, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query search index failed: %w", err)
	}
	defer rows.Close()

	out := make([]Item, 0, pageSize)
	logger.Info("search index query",
		"sql", renderExecutableSQL(querySQL, queryArgs...),
	)
	for rows.Next() {
		var it Item
		var minS, maxS, updatedS time.Time
		if err := rows.Scan(
			&it.TableName,
			&it.Symbol,
			&it.SymbolNorm,
			&it.Variety,
			&it.Exchange,
			&it.Kind,
			&minS,
			&maxS,
			&it.BarCount,
			&updatedS,
		); err != nil {
			return nil, 0, fmt.Errorf("scan search row failed: %w", err)
		}
		it.MinTime = minS
		it.MaxTime = maxS
		it.UpdatedAt = updatedS
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate search rows failed: %w", err)
	}
	logger.Info("search index query result", "rows", len(out), "total", total)
	return out, total, nil
}

func (m *Manager) LookupBySymbol(symbol string, kind string, variety string) (*Item, error) {
	if err := m.EnsureFresh(); err != nil {
		return nil, err
	}
	db, err := m.openDB()
	if err != nil {
		return nil, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	kind = strings.ToLower(strings.TrimSpace(kind))
	symbolNorm, parsedVariety := normalizeLookupSymbol(symbol, kind)
	if symbolNorm == "" {
		return nil, nil
	}
	if strings.TrimSpace(variety) == "" {
		variety = parsedVariety
	}
	variety = normalizeVariety(variety)

	query := `
SELECT table_name, symbol, symbol_norm, variety, exchange, kind, min_time, max_time, bar_count, updated_at
FROM kline_search_index
WHERE symbol_norm = ? AND kind = ?
  AND (? = '' OR variety = ?)
ORDER BY max_time DESC
LIMIT 1`
	logger.Info("search index lookup query",
		"sql", renderExecutableSQL(query, symbolNorm, kind, variety, variety),
	)
	var it Item
	var minS, maxS, updatedS time.Time
	err = db.QueryRow(query, symbolNorm, kind, variety, variety).Scan(
		&it.TableName,
		&it.Symbol,
		&it.SymbolNorm,
		&it.Variety,
		&it.Exchange,
		&it.Kind,
		&minS,
		&maxS,
		&it.BarCount,
		&updatedS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Info("search index lookup result", "found", false, "symbol_norm", symbolNorm, "kind", kind, "variety", variety)
			return nil, nil
		}
		return nil, fmt.Errorf("lookup symbol failed: %w", err)
	}
	it.MinTime = minS
	it.MaxTime = maxS
	it.UpdatedAt = updatedS
	logger.Info("search index lookup result",
		"found", true,
		"table", it.TableName,
		"symbol", it.Symbol,
		"kind", it.Kind,
		"bar_count", it.BarCount,
	)
	return &it, nil
}

func renderExecutableSQL(query string, args ...any) string {
	out := strings.TrimSpace(query)
	for _, arg := range args {
		out = strings.Replace(out, "?", mysqlLiteral(arg), 1)
	}
	return navicatSQL(out)
}

func navicatSQL(query string) string {
	out := strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
	// Use MySQL identifier quotes so SQL can be pasted directly into Navicat.
	return strings.ReplaceAll(out, `"`, "`")
}

func mysqlLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case time.Time:
		return "'" + x.Format(timeLayout) + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
	}
}

func (m *Manager) rebuild() error {
	db, err := m.openDB()
	if err != nil {
		return fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = ensureSchema(tx); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM kline_search_index`); err != nil {
		return fmt.Errorf("clear search index failed: %w", err)
	}

	tables, err := listKlineTables(tx)
	if err != nil {
		return err
	}
	logger.Info("search index rebuild tables", "count", len(tables))
	now := time.Now().Format(timeLayout)
	totalInserted := 0
	for _, tableName := range tables {
		kind := tableKindFromName(tableName)
		variety := extractVarietyFromTableName(tableName)
		if kind == "" || variety == "" {
			continue
		}

		timeExpr, err := resolveQueryTimeExpr(tx, tableName)
		if err != nil {
			return err
		}
		groupSQL := fmt.Sprintf(`
SELECT "InstrumentID","Exchange",MIN(%s),MAX(%s),COUNT(1)
FROM "%s"
GROUP BY "InstrumentID","Exchange"`, timeExpr, timeExpr, tableName)
		logger.Info("search index rebuild table query", "table", tableName, "sql", navicatSQL(groupSQL))
		rows, qErr := tx.Query(groupSQL)
		if qErr != nil {
			return fmt.Errorf("scan table %s failed: %w", tableName, qErr)
		}
		type groupedRow struct {
			symbol   string
			exchange string
			minS     time.Time
			maxS     time.Time
			cnt      int64
		}
		grouped := make([]groupedRow, 0, 128)
		for rows.Next() {
			var r groupedRow
			if scanErr := rows.Scan(&r.symbol, &r.exchange, &r.minS, &r.maxS, &r.cnt); scanErr != nil {
				_ = rows.Close()
				return fmt.Errorf("scan grouped row from %s failed: %w", tableName, scanErr)
			}
			grouped = append(grouped, r)
		}
		if iterErr := rows.Err(); iterErr != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate grouped rows failed: %w", iterErr)
		}
		_ = rows.Close()
		logger.Info("search index rebuild table grouped result", "table", tableName, "group_count", len(grouped))
		inserted := 0
		for _, r := range grouped {
			normSymbol := normalizeStoredSymbol(r.symbol, kind, variety)
			if normSymbol == "" {
				continue
			}
			if _, execErr := tx.Exec(`
INSERT INTO kline_search_index
(table_name, symbol, symbol_norm, variety, exchange, kind, min_time, max_time, bar_count, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				tableName,
				normSymbol,
				normSymbol,
				variety,
				strings.ToUpper(strings.TrimSpace(r.exchange)),
				kind,
				r.minS.Format(timeLayout),
				r.maxS.Format(timeLayout),
				r.cnt,
				now,
			); execErr != nil {
				return fmt.Errorf("insert search index row failed: %w", execErr)
			}
			inserted++
		}
		totalInserted += inserted
		logger.Info("search index rebuild table insert result", "table", tableName, "inserted_rows", inserted)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit rebuild failed: %w", err)
	}
	logger.Info("search index rebuild done", "inserted_rows", totalInserted)
	return nil
}

func (m *Manager) openDB() (*sql.DB, error) {
	return dbx.Open(m.dbPath)
}

func ensureSchema(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS kline_search_index (
  table_name VARCHAR(191) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  symbol_norm VARCHAR(64) NOT NULL,
  variety VARCHAR(32) NOT NULL,
  exchange VARCHAR(16) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  min_time DATETIME NOT NULL,
  max_time DATETIME NOT NULL,
  bar_count BIGINT NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (table_name, symbol, kind)
)`,
		`CREATE INDEX idx_kline_search_symbol_norm ON kline_search_index(symbol_norm)`,
		`CREATE INDEX idx_kline_search_variety ON kline_search_index(variety)`,
		`CREATE INDEX idx_kline_search_time_range ON kline_search_index(min_time, max_time)`,
		`CREATE INDEX idx_kline_search_kind ON kline_search_index(kind)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			if strings.Contains(strings.ToLower(stmt), "create index") && isDuplicateIndexErr(err) {
				continue
			}
			return fmt.Errorf("ensure search index schema failed: %w", err)
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

func listKlineTables(tx *sql.Tx) ([]string, error) {
	rows, err := tx.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'future_kline_%'
  AND table_name <> 'kline_search_index'
ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("list kline tables failed: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name failed: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table names failed: %w", err)
	}
	return out, nil
}

func normalizeVariety(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	end := 0
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return value[:end]
}

func tableKindFromName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, l9TablePrefix), strings.HasPrefix(tableName, weightedIndexTablePrefix), strings.HasPrefix(tableName, legacyL9TablePrefix):
		return "l9"
	case strings.HasPrefix(tableName, instrumentTablePrefix):
		return "contract"
	case strings.HasPrefix(tableName, legacyTablePrefix):
		return "contract"
	default:
		return ""
	}
}

func extractVarietyFromTableName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, instrumentTablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, instrumentTablePrefix))
	case strings.HasPrefix(tableName, l9TablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, l9TablePrefix))
	case strings.HasPrefix(tableName, weightedIndexTablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, weightedIndexTablePrefix))
	case strings.HasPrefix(tableName, legacyL9TablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, legacyL9TablePrefix))
	case strings.HasPrefix(tableName, legacyTablePrefix):
		base := strings.TrimPrefix(tableName, legacyTablePrefix)
		if strings.HasPrefix(base, "instrument_1m_") || strings.HasPrefix(base, "weighted_index_") || strings.HasPrefix(base, "l9_1m_") {
			return ""
		}
		return normalizeVariety(base)
	default:
		return ""
	}
}

func normalizeStoredSymbol(symbol string, kind string, variety string) string {
	_ = kind
	_ = variety
	return strings.ToLower(strings.TrimSpace(symbol))
}

func normalizeLookupSymbol(symbol string, kind string) (string, string) {
	s := strings.ToLower(strings.TrimSpace(symbol))
	if s == "" {
		return "", ""
	}
	if kind == "l9" {
		if strings.HasSuffix(s, "l9") {
			v := normalizeVariety(strings.TrimSuffix(s, "l9"))
			return s, v
		}
		if s == "l9" {
			return "l9", ""
		}
		v := normalizeVariety(s)
		if v == "" {
			return s, ""
		}
		return v + "l9", v
	}
	return s, normalizeVariety(s)
}

func tableHasColumn(tx *sql.Tx, tableName string, column string) (bool, error) {
	rows, err := tx.Query(`
SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func resolveQueryTimeExpr(tx *sql.Tx, tableName string) (string, error) {
	dataTimeCol := ""
	hasDataTime, err := tableHasColumn(tx, tableName, "DataTime")
	if err != nil {
		return "", fmt.Errorf("inspect table %s DataTime failed: %w", tableName, err)
	}
	if hasDataTime {
		dataTimeCol = `"DataTime"`
	} else {
		hasLegacyTime, err := tableHasColumn(tx, tableName, "Time")
		if err != nil {
			return "", fmt.Errorf("inspect table %s Time failed: %w", tableName, err)
		}
		if !hasLegacyTime {
			return "", fmt.Errorf("table %s missing both DataTime and Time columns", tableName)
		}
		dataTimeCol = `"Time"`
	}
	return dataTimeCol, nil
}
