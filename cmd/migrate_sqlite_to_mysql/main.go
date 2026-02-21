package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"ctp-go-demo/internal/config"
	dbx "ctp-go-demo/internal/db"

	_ "modernc.org/sqlite"
)

func main() {
	sqlitePath := flag.String("sqlite-path", "flow/future_kline.db", "source sqlite file path")
	mysqlHost := flag.String("mysql-host", "localhost", "mysql host")
	mysqlPort := flag.Int("mysql-port", 3306, "mysql port")
	mysqlUser := flag.String("mysql-user", "root", "mysql user")
	mysqlPassword := flag.String("mysql-password", "", "mysql password")
	mysqlDB := flag.String("mysql-db", "future_kline", "mysql database")
	truncate := flag.Bool("truncate", false, "truncate target tables before import")
	batchSize := flag.Int("batch-size", 1000, "batch size")
	flag.Parse()

	if *batchSize <= 0 {
		*batchSize = 1000
	}

	src, err := sql.Open("sqlite", *sqlitePath)
	if err != nil {
		fatalf("open sqlite failed: %v", err)
	}
	defer src.Close()

	cfg := config.DBConfig{
		Driver:   "mysql",
		Host:     *mysqlHost,
		Port:     *mysqlPort,
		User:     *mysqlUser,
		Password: *mysqlPassword,
		Database: *mysqlDB,
		Params:   "parseTime=true&loc=Local&multiStatements=false",
	}
	if err := dbx.EnsureDatabase(cfg); err != nil {
		fatalf("ensure mysql database failed: %v", err)
	}
	dst, err := dbx.Open(dbx.BuildDSN(cfg))
	if err != nil {
		fatalf("open mysql failed: %v", err)
	}
	defer dst.Close()
	if err := dbx.EnsureDatabaseAndSchema(cfg, dst); err != nil {
		fatalf("ensure mysql schema failed: %v", err)
	}

	tables, err := listSourceTables(src)
	if err != nil {
		fatalf("list source tables failed: %v", err)
	}
	fmt.Printf("source tables matched: %d\n", len(tables))

	for _, table := range tables {
		start := time.Now()
		readCount, writeCount, err := migrateTable(src, dst, table, *truncate, *batchSize)
		if err != nil {
			fatalf("migrate table %s failed: %v", table, err)
		}
		fmt.Printf("table=%s read=%d write=%d elapsed_ms=%d\n",
			table, readCount, writeCount, time.Since(start).Milliseconds())
	}
	fmt.Println("migration done")
}

func listSourceTables(src *sql.DB) ([]string, error) {
	rows, err := src.Query(`
SELECT name
FROM sqlite_master
WHERE type='table'
  AND (
    name LIKE 'future_kline_%'
    OR name IN ('trading_calendar','trading_calendar_meta','kline_search_index','bus_consume_dedup')
  )
ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func migrateTable(src *sql.DB, dst *sql.DB, table string, truncate bool, batchSize int) (int64, int64, error) {
	if strings.HasPrefix(table, "future_kline_") {
		if err := ensureKlineTable(dst, table); err != nil {
			return 0, 0, err
		}
		return migrateKlineLike(src, dst, table, truncate, batchSize)
	}
	switch table {
	case "trading_calendar":
		return migrateTradingCalendar(src, dst, truncate)
	case "trading_calendar_meta":
		return migrateTradingCalendarMeta(src, dst, truncate)
	case "kline_search_index":
		return migrateKlineSearchIndex(src, dst, truncate)
	case "bus_consume_dedup":
		return migrateBusConsumeDedup(src, dst, truncate)
	default:
		return 0, 0, fmt.Errorf("unsupported table: %s", table)
	}
}

func ensureKlineTable(dst *sql.DB, table string) error {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS "%s" (
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
  "SettlementPrice" DOUBLE NOT NULL,
  PRIMARY KEY ("DataTime", "InstrumentID", "Exchange", "Period")
)`, table)
	if _, err := dst.Exec(ddl); err != nil {
		return err
	}
	if _, err := dst.Exec(fmt.Sprintf(`CREATE INDEX "idx_%s_inst_period_adj" ON "%s"("InstrumentID","Period","AdjustedTime" DESC)`, table, table)); err != nil && !isDuplicateIndexErr(err) {
		return err
	}
	if _, err := dst.Exec(fmt.Sprintf(`CREATE INDEX "idx_%s_adj" ON "%s"("AdjustedTime" DESC)`, table, table)); err != nil && !isDuplicateIndexErr(err) {
		return err
	}
	return nil
}

func migrateKlineLike(src *sql.DB, dst *sql.DB, table string, truncate bool, batchSize int) (int64, int64, error) {
	if truncate {
		if _, err := dst.Exec(fmt.Sprintf(`TRUNCATE TABLE "%s"`, table)); err != nil {
			return 0, 0, err
		}
	}
	rows, err := src.Query(fmt.Sprintf(`
SELECT "InstrumentID","Exchange","DataTime","AdjustedTime","Period","Open","High","Low","Close","Volume","OpenInterest","SettlementPrice"
FROM "%s"`, table))
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	stmt := fmt.Sprintf(`
INSERT INTO "%s"
("InstrumentID","Exchange","DataTime","AdjustedTime","Period","Open","High","Low","Close","Volume","OpenInterest","SettlementPrice")
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  "AdjustedTime"=VALUES("AdjustedTime"),
  "Open"=VALUES("Open"),
  "High"=VALUES("High"),
  "Low"=VALUES("Low"),
  "Close"=VALUES("Close"),
  "Volume"=VALUES("Volume"),
  "OpenInterest"=VALUES("OpenInterest"),
  "SettlementPrice"=VALUES("SettlementPrice")`, table)
	prep, err := dst.Prepare(stmt)
	if err != nil {
		return 0, 0, err
	}
	defer prep.Close()
	var readCount int64
	var writeCount int64
	for rows.Next() {
		var instrument, exchange, dt, adt, period string
		var open, high, low, closeP float64
		var volume int64
		var oi, settle float64
		if err := rows.Scan(&instrument, &exchange, &dt, &adt, &period, &open, &high, &low, &closeP, &volume, &oi, &settle); err != nil {
			return readCount, writeCount, err
		}
		readCount++
		if _, err := prep.Exec(instrument, exchange, dt, adt, period, open, high, low, closeP, volume, oi, settle); err != nil {
			return readCount, writeCount, err
		}
		writeCount++
	}
	return readCount, writeCount, rows.Err()
}

func migrateTradingCalendar(src *sql.DB, dst *sql.DB, truncate bool) (int64, int64, error) {
	if truncate {
		if _, err := dst.Exec(`TRUNCATE TABLE trading_calendar`); err != nil {
			return 0, 0, err
		}
	}
	rows, err := src.Query(`SELECT trade_date,is_open,updated_at FROM trading_calendar`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	prep, err := dst.Prepare(`
INSERT INTO trading_calendar(trade_date,is_open,updated_at)
VALUES(?,?,?)
ON DUPLICATE KEY UPDATE
  is_open=VALUES(is_open),
  updated_at=VALUES(updated_at)`)
	if err != nil {
		return 0, 0, err
	}
	defer prep.Close()
	var r, w int64
	for rows.Next() {
		var d string
		var open int
		var updated string
		if err := rows.Scan(&d, &open, &updated); err != nil {
			return r, w, err
		}
		r++
		if _, err := prep.Exec(d, open, updated); err != nil {
			return r, w, err
		}
		w++
	}
	return r, w, rows.Err()
}

func migrateTradingCalendarMeta(src *sql.DB, dst *sql.DB, truncate bool) (int64, int64, error) {
	if truncate {
		if _, err := dst.Exec(`TRUNCATE TABLE trading_calendar_meta`); err != nil {
			return 0, 0, err
		}
	}
	rows, err := src.Query(`SELECT k,v FROM trading_calendar_meta`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	prep, err := dst.Prepare(`INSERT INTO trading_calendar_meta(k,v) VALUES(?,?) ON DUPLICATE KEY UPDATE v=VALUES(v)`)
	if err != nil {
		return 0, 0, err
	}
	defer prep.Close()
	var r, w int64
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return r, w, err
		}
		r++
		if _, err := prep.Exec(k, v); err != nil {
			return r, w, err
		}
		w++
	}
	return r, w, rows.Err()
}

func migrateKlineSearchIndex(src *sql.DB, dst *sql.DB, truncate bool) (int64, int64, error) {
	if truncate {
		if _, err := dst.Exec(`TRUNCATE TABLE kline_search_index`); err != nil {
			return 0, 0, err
		}
	}
	rows, err := src.Query(`SELECT table_name,symbol,symbol_norm,variety,exchange,kind,min_time,max_time,bar_count,updated_at FROM kline_search_index`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	prep, err := dst.Prepare(`
INSERT INTO kline_search_index(table_name,symbol,symbol_norm,variety,exchange,kind,min_time,max_time,bar_count,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  symbol_norm=VALUES(symbol_norm),
  variety=VALUES(variety),
  exchange=VALUES(exchange),
  min_time=VALUES(min_time),
  max_time=VALUES(max_time),
  bar_count=VALUES(bar_count),
  updated_at=VALUES(updated_at)`)
	if err != nil {
		return 0, 0, err
	}
	defer prep.Close()
	var r, w int64
	for rows.Next() {
		var tableName, symbol, symbolNorm, variety, exchange, kind, minTime, maxTime, updatedAt string
		var barCount int64
		if err := rows.Scan(&tableName, &symbol, &symbolNorm, &variety, &exchange, &kind, &minTime, &maxTime, &barCount, &updatedAt); err != nil {
			return r, w, err
		}
		r++
		if _, err := prep.Exec(tableName, symbol, symbolNorm, variety, exchange, kind, minTime, maxTime, barCount, updatedAt); err != nil {
			return r, w, err
		}
		w++
	}
	return r, w, rows.Err()
}

func migrateBusConsumeDedup(src *sql.DB, dst *sql.DB, truncate bool) (int64, int64, error) {
	if truncate {
		if _, err := dst.Exec(`TRUNCATE TABLE bus_consume_dedup`); err != nil {
			return 0, 0, err
		}
	}
	rows, err := src.Query(`SELECT consumer_id,event_id,processed_at FROM bus_consume_dedup`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	prep, err := dst.Prepare(`INSERT IGNORE INTO bus_consume_dedup(consumer_id,event_id,processed_at) VALUES(?,?,?)`)
	if err != nil {
		return 0, 0, err
	}
	defer prep.Close()
	var r, w int64
	for rows.Next() {
		var consumerID, eventID, processedAt string
		if err := rows.Scan(&consumerID, &eventID, &processedAt); err != nil {
			return r, w, err
		}
		r++
		if _, err := prep.Exec(consumerID, eventID, processedAt); err != nil {
			return r, w, err
		}
		w++
	}
	return r, w, rows.Err()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func isDuplicateIndexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}
