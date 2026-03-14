package testmysql

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ctp-go-demo/internal/config"
	dbx "ctp-go-demo/internal/db"
)

const dbNamePrefix = "codex_test_"

var dbSeq atomic.Uint64

func NewDatabase(t *testing.T) string {
	t.Helper()

	cfg := baseConfig()
	cfg.Database = dbName(t.Name())

	if err := dbx.EnsureDatabase(cfg); err != nil {
		t.Fatalf("ensure test database failed: %v", err)
	}

	db, err := dbx.Open(dbx.BuildDSN(cfg))
	if err != nil {
		t.Fatalf("open test database failed: %v", err)
	}
	if err := dbx.EnsureDatabaseAndSchema(cfg, db); err != nil {
		_ = db.Close()
		t.Fatalf("ensure test schema failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close test database failed: %v", err)
	}

	t.Cleanup(func() {
		dropDatabase(t, cfg)
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
	if !strings.HasPrefix(cfg.Database, dbNamePrefix) {
		t.Fatalf("refusing to drop non-test database %q", cfg.Database)
	}

	admin, err := dbx.Open(dbx.BuildAdminDSN(cfg))
	if err != nil {
		t.Fatalf("open mysql admin connection failed during cleanup: %v", err)
	}
	defer admin.Close()

	if _, err := admin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, cfg.Database)); err != nil {
		t.Fatalf("drop test database failed: %v", err)
	}
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
		candidate := filepath.Join(dir, "..", "ctp-future-resources", "config", "config.json")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
