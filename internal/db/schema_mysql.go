package db

import (
	"database/sql"
	"fmt"
	"strings"

	"ctp-go-demo/internal/config"
)

func EnsureDatabaseAndSchema(cfg config.DBConfig, db *sql.DB) error {
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
		`CREATE TABLE IF NOT EXISTS bus_consume_dedup (
  consumer_id VARCHAR(128) NOT NULL,
  event_id VARCHAR(128) NOT NULL,
  processed_at DATETIME NOT NULL,
  PRIMARY KEY (consumer_id, event_id)
)`,
		`CREATE INDEX idx_bus_consume_dedup_processed_at ON bus_consume_dedup(processed_at DESC)`,
		`CREATE TABLE IF NOT EXISTS chart_layouts (
  owner VARCHAR(64) NOT NULL DEFAULT 'admin',
  symbol VARCHAR(64) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  variety VARCHAR(32) NOT NULL,
  timeframe VARCHAR(16) NOT NULL,
  theme VARCHAR(16) NOT NULL,
  layout_json JSON NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (owner, symbol, kind, variety, timeframe)
)`,
		`CREATE TABLE IF NOT EXISTS chart_drawings (
  id VARCHAR(64) NOT NULL,
  owner VARCHAR(64) NOT NULL DEFAULT 'admin',
  symbol VARCHAR(64) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  variety VARCHAR(32) NOT NULL,
  timeframe VARCHAR(16) NOT NULL,
  type VARCHAR(16) NOT NULL,
  points_json JSON NOT NULL,
  text_value TEXT NOT NULL,
  style_json JSON NOT NULL,
  object_class VARCHAR(32) NOT NULL DEFAULT 'general',
  start_time BIGINT NULL,
  end_time BIGINT NULL,
  start_price DOUBLE NULL,
  end_price DOUBLE NULL,
  line_color VARCHAR(32) NULL,
  line_width DOUBLE NULL,
  line_style VARCHAR(16) NULL,
  left_cap VARCHAR(16) NULL,
  right_cap VARCHAR(16) NULL,
  label_text TEXT NULL,
  label_pos VARCHAR(16) NULL,
  label_align VARCHAR(16) NULL,
  visible_range VARCHAR(16) NOT NULL DEFAULT 'all',
  locked TINYINT NOT NULL,
  visible TINYINT NOT NULL,
  z_index BIGINT NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_chart_drawings_scope ON chart_drawings(symbol, kind, variety, timeframe, updated_at)`,
		`CREATE INDEX idx_chart_drawings_owner_scope ON chart_drawings(owner, symbol, kind, variety, timeframe, updated_at)`,
		`CREATE INDEX idx_chart_drawings_object_class ON chart_drawings(object_class)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			if strings.Contains(strings.ToLower(stmt), "create index") && isDuplicateObjectError(err) {
				continue
			}
			return fmt.Errorf("ensure mysql schema failed: %w", err)
		}
	}
	if err := ensureChartTablesEvolution(db); err != nil {
		return err
	}
	return nil
}

func ensureChartTablesEvolution(db *sql.DB) error {
	if err := ensureColumn(db, "chart_layouts", "owner", "ALTER TABLE chart_layouts ADD COLUMN owner VARCHAR(64) NOT NULL DEFAULT 'admin' FIRST"); err != nil {
		return err
	}
	if err := ensurePrimaryKey(db, "chart_layouts", []string{"owner", "symbol", "kind", "variety", "timeframe"}); err != nil {
		return err
	}

	changes := []struct {
		column string
		ddl    string
	}{
		{"owner", "ALTER TABLE chart_drawings ADD COLUMN owner VARCHAR(64) NOT NULL DEFAULT 'admin' AFTER id"},
		{"object_class", "ALTER TABLE chart_drawings ADD COLUMN object_class VARCHAR(32) NOT NULL DEFAULT 'general' AFTER style_json"},
		{"start_time", "ALTER TABLE chart_drawings ADD COLUMN start_time BIGINT NULL AFTER object_class"},
		{"end_time", "ALTER TABLE chart_drawings ADD COLUMN end_time BIGINT NULL AFTER start_time"},
		{"start_price", "ALTER TABLE chart_drawings ADD COLUMN start_price DOUBLE NULL AFTER end_time"},
		{"end_price", "ALTER TABLE chart_drawings ADD COLUMN end_price DOUBLE NULL AFTER start_price"},
		{"line_color", "ALTER TABLE chart_drawings ADD COLUMN line_color VARCHAR(32) NULL AFTER end_price"},
		{"line_width", "ALTER TABLE chart_drawings ADD COLUMN line_width DOUBLE NULL AFTER line_color"},
		{"line_style", "ALTER TABLE chart_drawings ADD COLUMN line_style VARCHAR(16) NULL AFTER line_width"},
		{"left_cap", "ALTER TABLE chart_drawings ADD COLUMN left_cap VARCHAR(16) NULL AFTER line_style"},
		{"right_cap", "ALTER TABLE chart_drawings ADD COLUMN right_cap VARCHAR(16) NULL AFTER left_cap"},
		{"label_text", "ALTER TABLE chart_drawings ADD COLUMN label_text TEXT NULL AFTER right_cap"},
		{"label_pos", "ALTER TABLE chart_drawings ADD COLUMN label_pos VARCHAR(16) NULL AFTER label_text"},
		{"label_align", "ALTER TABLE chart_drawings ADD COLUMN label_align VARCHAR(16) NULL AFTER label_pos"},
		{"visible_range", "ALTER TABLE chart_drawings ADD COLUMN visible_range VARCHAR(16) NOT NULL DEFAULT 'all' AFTER label_align"},
	}
	for _, ch := range changes {
		if err := ensureColumn(db, "chart_drawings", ch.column, ch.ddl); err != nil {
			return err
		}
	}

	if _, err := db.Exec(`CREATE INDEX idx_chart_drawings_owner_scope ON chart_drawings(owner, symbol, kind, variety, timeframe, updated_at)`); err != nil && !isDuplicateObjectError(err) {
		return fmt.Errorf("ensure chart_drawings owner scope index failed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX idx_chart_drawings_object_class ON chart_drawings(object_class)`); err != nil && !isDuplicateObjectError(err) {
		return fmt.Errorf("ensure chart_drawings object_class index failed: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE chart_drawings MODIFY COLUMN z_index BIGINT NOT NULL`); err != nil {
		return fmt.Errorf("ensure chart_drawings z_index bigint failed: %w", err)
	}

	if _, err := db.Exec(`UPDATE chart_drawings SET owner='admin' WHERE owner IS NULL OR owner=''`); err != nil {
		return fmt.Errorf("backfill drawing owner failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_layouts SET owner='admin' WHERE owner IS NULL OR owner=''`); err != nil {
		return fmt.Errorf("backfill layout owner failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET object_class=CASE WHEN type='trendline' THEN 'trendline' ELSE 'general' END WHERE object_class IS NULL OR object_class=''`); err != nil {
		return fmt.Errorf("backfill drawing object_class failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET label_text=text_value WHERE (label_text IS NULL OR label_text='') AND text_value<>''`); err != nil {
		return fmt.Errorf("backfill drawing label_text failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET line_color=JSON_UNQUOTE(JSON_EXTRACT(style_json,'$.color')) WHERE (line_color IS NULL OR line_color='') AND JSON_EXTRACT(style_json,'$.color') IS NOT NULL`); err != nil {
		return fmt.Errorf("backfill drawing line_color failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET line_width=CAST(JSON_UNQUOTE(JSON_EXTRACT(style_json,'$.width')) AS DOUBLE) WHERE line_width IS NULL AND JSON_EXTRACT(style_json,'$.width') IS NOT NULL`); err != nil {
		return fmt.Errorf("backfill drawing line_width failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET start_time=CAST(JSON_UNQUOTE(JSON_EXTRACT(points_json,'$[0].time')) AS SIGNED), end_time=CAST(JSON_UNQUOTE(JSON_EXTRACT(points_json,'$[1].time')) AS SIGNED) WHERE type='trendline' AND (start_time IS NULL OR end_time IS NULL)`); err != nil {
		return fmt.Errorf("backfill drawing time range failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE chart_drawings SET start_price=CAST(JSON_UNQUOTE(JSON_EXTRACT(points_json,'$[0].price')) AS DOUBLE), end_price=CAST(JSON_UNQUOTE(JSON_EXTRACT(points_json,'$[1].price')) AS DOUBLE) WHERE type='trendline' AND (start_price IS NULL OR end_price IS NULL)`); err != nil {
		return fmt.Errorf("backfill drawing price range failed: %w", err)
	}
	return nil
}

func ensureColumn(db *sql.DB, tableName, columnName, ddl string) error {
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(1) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?`, tableName, columnName).Scan(&cnt); err != nil {
		return fmt.Errorf("check column %s.%s failed: %w", tableName, columnName, err)
	}
	if cnt > 0 {
		return nil
	}
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("add column %s.%s failed: %w", tableName, columnName, err)
	}
	return nil
}

func ensurePrimaryKey(db *sql.DB, tableName string, want []string) error {
	rows, err := db.Query(`SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=DATABASE() AND table_name=? AND constraint_name='PRIMARY' ORDER BY ordinal_position`, tableName)
	if err != nil {
		return fmt.Errorf("query primary key for %s failed: %w", tableName, err)
	}
	defer rows.Close()
	current := make([]string, 0, 8)
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return fmt.Errorf("scan primary key for %s failed: %w", tableName, err)
		}
		current = append(current, strings.ToLower(strings.TrimSpace(c)))
	}
	if len(current) == len(want) {
		same := true
		for i := range want {
			if current[i] != strings.ToLower(want[i]) {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY, ADD PRIMARY KEY (%s)", tableName, strings.Join(want, ","))); err != nil {
		return fmt.Errorf("alter primary key for %s failed: %w", tableName, err)
	}
	return nil
}

func isDuplicateObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") ||
		strings.Contains(msg, "already exists")
}
