package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"ctp-go-demo/internal/config"
	_ "github.com/go-sql-driver/mysql"
)

func BuildDSN(cfg config.DBConfig) string {
	params := normalizeParams(cfg.Params)
	escapedUser := url.QueryEscape(cfg.User)
	escapedPass := url.QueryEscape(cfg.Password)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		escapedUser, escapedPass, cfg.Host, cfg.Port, cfg.Database, params)
}

func BuildAdminDSN(cfg config.DBConfig) string {
	params := normalizeParams(cfg.Params)
	escapedUser := url.QueryEscape(cfg.User)
	escapedPass := url.QueryEscape(cfg.Password)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",
		escapedUser, escapedPass, cfg.Host, cfg.Port, params)
}

func normalizeParams(raw string) string {
	params := strings.TrimSpace(raw)
	if params == "" {
		params = "parseTime=true&loc=Local"
	}
	values, err := url.ParseQuery(params)
	if err != nil {
		values = url.Values{}
	}
	if values.Get("parseTime") == "" {
		values.Set("parseTime", "true")
	}
	if values.Get("loc") == "" {
		values.Set("loc", "Local")
	}
	// Avoid protocol state issues from unconsumed multi result sets.
	values.Set("multiStatements", "false")
	return values.Encode()
}

func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Keep compatibility with existing SQL that uses "quoted identifiers".
	if _, err := db.Exec(`SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set ansi_quotes failed: %w", err)
	}
	return db, nil
}

func EnsureDatabase(cfg config.DBConfig) error {
	admin, err := Open(BuildAdminDSN(cfg))
	if err != nil {
		return err
	}
	defer admin.Close()
	ddl := fmt.Sprintf(
		`CREATE DATABASE IF NOT EXISTS "%s" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci`,
		cfg.Database,
	)
	_, err = admin.Exec(ddl)
	return err
}
