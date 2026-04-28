package testmysql

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
)

const dbNamePrefix = "codex_test_"

var dbSeq atomic.Uint64
var cleanupOnce sync.Once
var processLockDB *sql.DB

func NewDatabase(t *testing.T) string {
	t.Helper()
	return newDatabaseTB(t)
}

func NewBenchmarkDatabase(b *testing.B) string {
	b.Helper()
	return newDatabaseTB(b)
}

func newDatabaseTB(tb testing.TB) string {
	tb.Helper()

	cfg := baseConfig()
	cleanupOnce.Do(func() {
		lockDB, err := acquireProcessLock(cfg)
		if err != nil {
			tb.Fatalf("acquire mysql test lock failed: %v", err)
		}
		processLockDB = lockDB
		if err := purgeStaleDatabases(cfg); err != nil {
			tb.Fatalf("purge stale test databases failed: %v", err)
		}
	})
	cfg.Database = dbName(tb.Name())

	if err := dbx.EnsureDatabase(cfg); err != nil {
		tb.Fatalf("ensure test database failed: %v", err)
	}

	db, err := dbx.Open(dbx.BuildDSN(cfg))
	if err != nil {
		tb.Fatalf("open test database failed: %v", err)
	}
	if err := dbx.EnsureDatabaseAndSchema(cfg, db); err != nil {
		_ = db.Close()
		tb.Fatalf("ensure test schema failed: %v", err)
	}
	if err := db.Close(); err != nil {
		tb.Fatalf("close test database failed: %v", err)
	}

	tb.Cleanup(func() {
		dropDatabaseTB(tb, cfg)
	})
	return dbx.BuildDSN(cfg)
}

func Open(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := dbx.Open(dsn)
	if err != nil {
		t.Fatalf("open mysql failed: %v", err)
	}
	return db
}

func baseConfig() config.DBConfig {
	fileCfg := loadConfigFromWorkspace()
	port := 3306
	if raw := strings.TrimSpace(os.Getenv("MYSQL_TEST_PORT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			port = parsed
		}
	} else if fileCfg.Port > 0 {
		port = fileCfg.Port
	}
	return config.DBConfig{
		Driver:   "mysql",
		Host:     envOr("MYSQL_TEST_HOST", fallbackString(fileCfg.Host, "localhost")),
		Port:     port,
		User:     envOr("MYSQL_TEST_USER", fallbackString(fileCfg.User, "root")),
		Password: envOr("MYSQL_TEST_PASSWORD", fileCfg.Password),
		Params:   envOr("MYSQL_TEST_PARAMS", fallbackString(fileCfg.Params, "parseTime=true&loc=Local&multiStatements=false")),
	}
}

func envOr(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func fallbackString(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func dbName(testName string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_", ".", "_", "-", "_")
	suffix := replacer.Replace(strings.ToLower(strings.TrimSpace(testName)))
	if suffix == "" {
		suffix = "unnamed"
	}
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return fmt.Sprintf("%s%s_%08x_%04x", dbNamePrefix, suffix, uint32(time.Now().UnixNano()), uint16(dbSeq.Add(1)))
}

func dropDatabase(t *testing.T, cfg config.DBConfig) {
	t.Helper()
	dropDatabaseTB(t, cfg)
}

func dropDatabaseTB(tb testing.TB, cfg config.DBConfig) {
	tb.Helper()
	if !strings.HasPrefix(cfg.Database, dbNamePrefix) {
		tb.Fatalf("refusing to drop non-test database %q", cfg.Database)
	}

	admin, err := openAdminDB(cfg)
	if err != nil {
		tb.Fatalf("open mysql admin connection failed during cleanup: %v", err)
	}
	defer admin.Close()

	if err := dropDatabaseByName(admin, cfg.Database); err != nil {
		tb.Fatalf("drop test database failed: %v", err)
	}
}

func purgeStaleDatabases(cfg config.DBConfig) error {
	admin, err := openAdminDB(cfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	rows, err := admin.Query(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'codex_test\_%'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, name := range names {
		if err := dropDatabaseByName(admin, name); err != nil {
			return err
		}
	}
	return nil
}

func acquireProcessLock(cfg config.DBConfig) (*sql.DB, error) {
	admin, err := openAdminDB(cfg)
	if err != nil {
		return nil, err
	}
	var locked int
	if err := admin.QueryRow(`SELECT GET_LOCK('ctp_future_kline_testmysql', 120)`).Scan(&locked); err != nil {
		_ = admin.Close()
		return nil, err
	}
	if locked != 1 {
		_ = admin.Close()
		return nil, fmt.Errorf("timeout waiting for mysql test lock")
	}
	return admin, nil
}

func openAdminDB(cfg config.DBConfig) (*sql.DB, error) {
	return dbx.Open(dbx.BuildAdminDSN(cfg))
}

func dropDatabaseByName(admin *sql.DB, name string) error {
	if !strings.HasPrefix(name, dbNamePrefix) {
		return fmt.Errorf("refusing to drop non-test database %q", name)
	}
	quoted := strings.ReplaceAll(name, "`", "``")
	_, err := admin.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", quoted))
	return err
}

func loadConfigFromWorkspace() config.DBConfig {
	path := findWorkspaceConfig()
	if path == "" {
		return config.DBConfig{}
	}
	cfg, err := config.Load(path)
	if err != nil {
		return config.DBConfig{}
	}
	return cfg.DB
}

func findWorkspaceConfig() string {
	start, err := os.Getwd()
	if err != nil {
		return ""
	}
	dir := start
	for {
		candidates := []string{
			filepath.Join(dir, "config", "config.json"),
			filepath.Join(dir, "config.json"),
			filepath.Join(dir, "..", "ctp-future-resources", "config", "config.json"),
		}
		for _, candidate := range candidates {
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				return candidate
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
