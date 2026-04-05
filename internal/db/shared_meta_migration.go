package db

import (
	"database/sql"
	"fmt"
	"strings"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
)

var sharedMetaTableNames = []string{
	"trading_calendar",
	"trading_calendar_meta",
	"trading_sessions",
}

func MigrateSharedMetaTables(cfg config.DBConfig) error {
	sharedName := DatabaseForRole(cfg, RoleSharedMeta)
	sharedCfg := ConfigForRole(cfg, RoleSharedMeta)
	sharedDB, err := Open(BuildDSN(sharedCfg))
	if err != nil {
		return fmt.Errorf("open shared meta db failed: %w", err)
	}
	defer sharedDB.Close()
	if err := EnsureDatabaseAndSchemaForRole(sharedCfg, RoleSharedMeta, sharedDB); err != nil {
		return err
	}

	for _, role := range []string{
		RoleMarketRealtime,
		RoleMarketReplay,
		RoleTradeLive,
		RoleTradePaperLive,
		RoleTradePaperReplay,
		RoleChartUserRealtime,
		RoleChartUserReplay,
	} {
		sourceName := DatabaseForRole(cfg, role)
		if sourceName == "" || strings.EqualFold(sourceName, sharedName) {
			continue
		}
		sourceDB, err := Open(DSNForRole(cfg, role))
		if err != nil {
			return fmt.Errorf("open source db for shared meta migration failed: role=%s: %w", role, err)
		}
		if err := migrateSharedTablesFromSource(sourceDB, sourceName, sharedName); err != nil {
			_ = sourceDB.Close()
			return err
		}
		if err := sourceDB.Close(); err != nil {
			return err
		}
	}
	return nil
}

func migrateSharedTablesFromSource(db *sql.DB, sourceDBName string, targetDBName string) error {
	for _, tableName := range sharedMetaTableNames {
		exists, err := tableExists(db, sourceDBName, tableName)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		logger.Info("shared meta migration found legacy table", "source_db", sourceDBName, "target_db", targetDBName, "table", tableName)
		if err := migrateSharedTable(db, sourceDBName, targetDBName, tableName); err != nil {
			return err
		}
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", sourceDBName, tableName)); err != nil {
			return fmt.Errorf("drop migrated legacy table failed: source_db=%s table=%s: %w", sourceDBName, tableName, err)
		}
		logger.Info("shared meta migration dropped legacy table", "source_db", sourceDBName, "table", tableName)
	}
	return nil
}

func migrateSharedTable(db *sql.DB, sourceDBName string, targetDBName string, tableName string) error {
	var stmt string
	switch tableName {
	case "trading_calendar":
		stmt = fmt.Sprintf(`INSERT INTO %s (trade_date,is_open,updated_at)
SELECT trade_date,is_open,updated_at FROM %s
ON DUPLICATE KEY UPDATE
  is_open=VALUES(is_open),
  updated_at=VALUES(updated_at)`,
			qualifiedTable(targetDBName, tableName),
			qualifiedTable(sourceDBName, tableName),
		)
	case "trading_calendar_meta":
		stmt = fmt.Sprintf(`INSERT INTO %s (k,v)
SELECT k,v FROM %s
ON DUPLICATE KEY UPDATE
  v=VALUES(v)`,
			qualifiedTable(targetDBName, tableName),
			qualifiedTable(sourceDBName, tableName),
		)
	case "trading_sessions":
		stmt = fmt.Sprintf(`INSERT INTO %s (variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at)
SELECT variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at FROM %s
ON DUPLICATE KEY UPDATE
  session_text=VALUES(session_text),
  session_json=VALUES(session_json),
  is_completed=VALUES(is_completed),
  sample_trade_date=VALUES(sample_trade_date),
  validated_trade_date=VALUES(validated_trade_date),
  match_ratio=VALUES(match_ratio),
  updated_at=VALUES(updated_at)`,
			qualifiedTable(targetDBName, tableName),
			qualifiedTable(sourceDBName, tableName),
		)
	default:
		return fmt.Errorf("unsupported shared meta table: %s", tableName)
	}
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("migrate legacy shared meta table failed: source_db=%s target_db=%s table=%s: %w", sourceDBName, targetDBName, tableName, err)
	}
	logger.Info("shared meta migration copied table", "source_db", sourceDBName, "target_db", targetDBName, "table", tableName)
	return nil
}

func tableExists(db *sql.DB, schemaName string, tableName string) (bool, error) {
	var exists int
	err := db.QueryRow(`
SELECT COUNT(1)
FROM information_schema.tables
WHERE table_schema = ?
  AND table_name = ?`, schemaName, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check table exists failed: schema=%s table=%s: %w", schemaName, tableName, err)
	}
	return exists > 0, nil
}

func qualifiedTable(schemaName string, tableName string) string {
	return fmt.Sprintf("`%s`.`%s`", schemaName, tableName)
}
