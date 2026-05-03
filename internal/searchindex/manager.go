package searchindex

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/logger"
)

const timeLayout = "2006-01-02 15:04:05"

var ErrBusy = errors.New("search index rebuild already running")

type Item struct {
	// TableName 是索引命中的底层 K 线表名。
	TableName string `json:"table_name"`
	// Symbol 是展示给前端的合约或品种标识。
	Symbol string `json:"symbol"`
	// SymbolNorm 是标准化后的 symbol，用于匹配和去重。
	SymbolNorm string `json:"symbol_norm"`
	// Variety 是品种代码。
	Variety string `json:"variety"`
	// Kind 表示 contract 或 l9。
	Kind string `json:"kind"`
	// BarCount 是该记录对应的 K 线数量。
	BarCount int64 `json:"bar_count"`
	// UpdatedAt 是该条索引最近一次成功更新的时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type Target struct {
	// TableName 是要重建的底层表名。
	TableName string `json:"table_name"`
	// Symbol 是要重建的 symbol。
	Symbol string `json:"symbol"`
	// Variety 是 symbol 所属品种。
	Variety string `json:"variety"`
	// Kind 表示 contract 或 l9。
	Kind string `json:"kind"`
}

type Manager struct {
	// dbPath 是业务数据库连接串。
	dbPath string

	// mu 保护重建互斥状态。
	mu sync.Mutex
	// rebuilding 标识当前是否有索引重建任务在执行。
	rebuilding bool
}

const (
	instrumentTablePrefix   = "future_kline_instrument_1m_"
	l9TablePrefix           = "future_kline_l9_1m_"
	instrumentMMTablePrefix = "future_kline_instrument_mm_"
	l9MMTablePrefix         = "future_kline_l9_mm_"
)

func NewManager(dbPath string, _ time.Duration) *Manager {
	return &Manager{dbPath: dbPath}
}

func (m *Manager) Invalidate() {}

func (m *Manager) EnsureFresh() error { return nil }

func (m *Manager) LookupBySymbol(symbol string, kind string, variety string) (*Item, error) {
	db, err := m.openDB()
	if err != nil {
		return nil, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	return lookupBySymbolDB(db, symbol, kind, variety)
}

func (m *Manager) LookupItems(targets []Target) (map[string]Item, error) {
	db, err := m.openDB()
	if err != nil {
		return nil, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	return lookupItemsDB(db, targets)
}

func (m *Manager) RebuildAll() error {
	return m.runExclusive(func() error {
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
		now := time.Now()
		totalInserted := 0
		for _, tableName := range tables {
			kind := tableKindFromName(tableName)
			variety := extractVarietyFromTableName(tableName)
			if kind == "" || variety == "" {
				continue
			}
			rows, scanErr := collectGroupedRows(tx, tableName, nil)
			if scanErr != nil {
				return scanErr
			}
			inserted, upsertErr := upsertRows(tx, tableName, kind, variety, rows, now)
			if upsertErr != nil {
				return upsertErr
			}
			totalInserted += inserted
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit rebuild failed: %w", err)
		}
		logger.Info("search index rebuild all done", "inserted_rows", totalInserted)
		return nil
	})
}

func (m *Manager) RebuildItems(targets []Target) error {
	targets = normalizeTargets(targets)
	if len(targets) == 0 {
		return nil
	}
	return m.runExclusive(func() error {
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

		groupedTargets := make(map[string][]Target)
		for _, target := range targets {
			groupedTargets[target.TableName] = append(groupedTargets[target.TableName], target)
		}
		now := time.Now()
		for tableName, tableTargets := range groupedTargets {
			kind := tableKindFromName(tableName)
			variety := extractVarietyFromTableName(tableName)
			if kind == "" || variety == "" {
				continue
			}
			if err := deleteTargets(tx, tableTargets); err != nil {
				return err
			}

			symbols := make([]string, 0, len(tableTargets))
			for _, target := range tableTargets {
				symbols = append(symbols, normalizeStoredSymbol(target.Symbol, target.Kind, target.Variety))
			}
			rows, scanErr := collectGroupedRows(tx, tableName, dedupeStrings(symbols))
			if scanErr != nil {
				return scanErr
			}
			if _, upsertErr := upsertRows(tx, tableName, kind, variety, rows, now); upsertErr != nil {
				return upsertErr
			}
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit rebuild items failed: %w", err)
		}
		logger.Info("search index rebuild items done", "item_count", len(targets))
		return nil
	})
}

func (m *Manager) RefreshBySymbol(symbol string, kind string, variety string) (*Item, error) {
	target := Target{
		Symbol:  symbol,
		Kind:    kind,
		Variety: variety,
	}
	target = normalizeTarget(target)
	if target.Symbol == "" || target.Kind == "" || target.Variety == "" {
		return nil, nil
	}
	target.TableName = tableNameForTarget(target)
	if target.TableName == "" {
		return nil, nil
	}
	if err := m.RebuildItems([]Target{target}); err != nil {
		return nil, err
	}
	return m.LookupBySymbol(target.Symbol, target.Kind, target.Variety)
}

func (m *Manager) runExclusive(fn func() error) error {
	m.mu.Lock()
	if m.rebuilding {
		m.mu.Unlock()
		return ErrBusy
	}
	m.rebuilding = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.rebuilding = false
		m.mu.Unlock()
	}()
	return fn()
}

func (m *Manager) openDB() (*sql.DB, error) {
	return dbx.Open(m.dbPath)
}

func lookupBySymbolDB(db *sql.DB, symbol string, kind string, variety string) (*Item, error) {
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
SELECT table_name, symbol, symbol_norm, variety, kind, bar_count, updated_at
FROM kline_search_index
WHERE symbol_norm = ? AND kind = ?
  AND (? = '' OR variety = ?)
ORDER BY updated_at DESC
LIMIT 1`
	var it Item
	var updatedS time.Time
	err := db.QueryRow(query, symbolNorm, kind, variety, variety).Scan(
		&it.TableName,
		&it.Symbol,
		&it.SymbolNorm,
		&it.Variety,
		&it.Kind,
		&it.BarCount,
		&updatedS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("lookup symbol failed: %w", err)
	}
	it.UpdatedAt = updatedS
	return &it, nil
}

func lookupItemsDB(db *sql.DB, targets []Target) (map[string]Item, error) {
	targets = normalizeTargets(targets)
	out := make(map[string]Item, len(targets))
	if len(targets) == 0 {
		return out, nil
	}
	type argRow struct {
		symbol string
		kind   string
	}
	argsRows := make([]argRow, 0, len(targets))
	varieties := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		argsRows = append(argsRows, argRow{
			symbol: normalizeStoredSymbol(target.Symbol, target.Kind, target.Variety),
			kind:   target.Kind,
		})
		if target.Variety != "" {
			varieties[target.Variety] = struct{}{}
		}
	}

	whereParts := make([]string, 0, len(argsRows))
	args := make([]any, 0, len(argsRows)*2+len(varieties))
	for _, row := range argsRows {
		whereParts = append(whereParts, "(symbol_norm = ? AND kind = ?)")
		args = append(args, row.symbol, row.kind)
	}
	query := `
SELECT table_name, symbol, symbol_norm, variety, kind, bar_count, updated_at
FROM kline_search_index
WHERE ` + strings.Join(whereParts, " OR ")
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch lookup search index failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item Item
		if err := rows.Scan(
			&item.TableName,
			&item.Symbol,
			&item.SymbolNorm,
			&item.Variety,
			&item.Kind,
			&item.BarCount,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan search index batch row failed: %w", err)
		}
		out[targetKey(Target{Symbol: item.SymbolNorm, Kind: item.Kind, Variety: item.Variety})] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search index batch rows failed: %w", err)
	}
	return out, nil
}

func ensureSchema(tx *sql.Tx) error {
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS kline_search_index (
  table_name VARCHAR(191) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  symbol_norm VARCHAR(64) NOT NULL,
  variety VARCHAR(32) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  bar_count BIGINT NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (table_name, symbol, kind)
)`); err != nil {
		return fmt.Errorf("ensure search index schema failed: %w", err)
	}
	stmts := []string{
		`CREATE INDEX idx_kline_search_symbol_norm ON kline_search_index(symbol_norm)`,
		`CREATE INDEX idx_kline_search_variety ON kline_search_index(variety)`,
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

type groupedRow struct {
	symbol string
	cnt    int64
}

func collectGroupedRows(tx *sql.Tx, tableName string, symbols []string) ([]groupedRow, error) {
	var (
		query string
		args  []any
	)
	if len(symbols) == 0 {
		query = fmt.Sprintf(`
SELECT lower("InstrumentID"), COUNT(1)
FROM "%s"
GROUP BY lower("InstrumentID")`, tableName)
	} else {
		placeholders := make([]string, 0, len(symbols))
		args = make([]any, 0, len(symbols))
		for _, symbol := range symbols {
			placeholders = append(placeholders, "?")
			args = append(args, strings.ToLower(strings.TrimSpace(symbol)))
		}
		query = fmt.Sprintf(`
SELECT lower("InstrumentID"), COUNT(1)
FROM "%s"
WHERE lower("InstrumentID") IN (%s)
GROUP BY lower("InstrumentID")`, tableName, strings.Join(placeholders, ","))
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("scan table %s failed: %w", tableName, err)
	}
	defer rows.Close()

	out := make([]groupedRow, 0, 64)
	for rows.Next() {
		var row groupedRow
		if err := rows.Scan(&row.symbol, &row.cnt); err != nil {
			return nil, fmt.Errorf("scan grouped row from %s failed: %w", tableName, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate grouped rows from %s failed: %w", tableName, err)
	}
	return out, nil
}

func upsertRows(tx *sql.Tx, tableName string, kind string, variety string, rows []groupedRow, now time.Time) (int, error) {
	inserted := 0
	for _, row := range rows {
		normSymbol := normalizeStoredSymbol(row.symbol, kind, variety)
		if normSymbol == "" {
			continue
		}
		if _, err := tx.Exec(`
INSERT INTO kline_search_index
(table_name, symbol, symbol_norm, variety, kind, bar_count, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  bar_count=VALUES(bar_count),
  updated_at=VALUES(updated_at)`,
			tableName,
			normSymbol,
			normSymbol,
			variety,
			kind,
			row.cnt,
			now.Format(timeLayout),
		); err != nil {
			return inserted, fmt.Errorf("insert search index row failed: %w", err)
		}
		inserted++
	}
	return inserted, nil
}

func deleteTargets(tx *sql.Tx, targets []Target) error {
	for _, target := range targets {
		if _, err := tx.Exec(`
DELETE FROM kline_search_index
WHERE table_name = ? AND symbol_norm = ? AND kind = ?`,
			target.TableName,
			normalizeStoredSymbol(target.Symbol, target.Kind, target.Variety),
			target.Kind,
		); err != nil {
			return fmt.Errorf("delete target search index row failed: %w", err)
		}
	}
	return nil
}

func normalizeTargets(targets []Target) []Target {
	out := make([]Target, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target = normalizeTarget(target)
		if target.Symbol == "" || target.Kind == "" || target.Variety == "" {
			continue
		}
		if target.TableName == "" {
			target.TableName = tableNameForTarget(target)
		}
		if target.TableName == "" {
			continue
		}
		key := targetKey(target)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, target)
	}
	return out
}

func normalizeTarget(target Target) Target {
	target.Kind = strings.ToLower(strings.TrimSpace(target.Kind))
	target.Variety = normalizeVariety(target.Variety)
	target.Symbol = normalizeStoredSymbol(target.Symbol, target.Kind, target.Variety)
	target.TableName = strings.TrimSpace(target.TableName)
	if target.Variety == "" {
		_, parsedVariety := normalizeLookupSymbol(target.Symbol, target.Kind)
		target.Variety = parsedVariety
	}
	return target
}

func targetKey(target Target) string {
	return normalizeStoredSymbol(target.Symbol, target.Kind, target.Variety) + "|" + strings.ToLower(strings.TrimSpace(target.Kind)) + "|" + normalizeVariety(target.Variety)
}

func tableNameForTarget(target Target) string {
	switch target.Kind {
	case "contract":
		return instrumentTablePrefix + target.Variety
	case "l9":
		return l9TablePrefix + target.Variety
	default:
		return ""
	}
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
	case strings.HasPrefix(tableName, instrumentMMTablePrefix), strings.HasPrefix(tableName, l9MMTablePrefix):
		return ""
	case strings.HasPrefix(tableName, l9TablePrefix):
		return "l9"
	case strings.HasPrefix(tableName, instrumentTablePrefix):
		return "contract"
	default:
		return ""
	}
}

func extractVarietyFromTableName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, instrumentMMTablePrefix), strings.HasPrefix(tableName, l9MMTablePrefix):
		return ""
	case strings.HasPrefix(tableName, instrumentTablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, instrumentTablePrefix))
	case strings.HasPrefix(tableName, l9TablePrefix):
		return normalizeVariety(strings.TrimPrefix(tableName, l9TablePrefix))
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

func renderExecutableSQL(query string, args ...any) string {
	out := strings.TrimSpace(query)
	for _, arg := range args {
		out = strings.Replace(out, "?", mysqlLiteral(arg), 1)
	}
	return navicatSQL(out)
}

func navicatSQL(query string) string {
	out := strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
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

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
