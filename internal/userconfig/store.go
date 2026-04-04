package userconfig

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
)

const (
	DefaultOwner = "admin"

	scopeTrade            = "trade"
	scopeAppMode          = "app_mode"
	keyEnabled            = "enabled"
	keyCurrentMode        = "current_mode"
	keyReplayResumeCursor = "replay_resume_cursor"
)

// Store 管理用户级配置覆盖项的持久化读写。
type Store struct {
	db *sql.DB
}

// TradeOverrides 描述当前支持的实盘交易覆盖项。
type TradeOverrides struct {
	Enabled *bool
}

// AppOverrides 表示一组可作用到基础 config.json 之上的用户覆盖项。
type AppOverrides struct {
	Trade TradeOverrides
}

func NewStore(dsn string) (*Store, error) {
	db, err := dbx.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) LoadAppOverrides(owner string) (AppOverrides, error) {
	if owner == "" {
		owner = DefaultOwner
	}
	rows, err := s.db.Query(`
SELECT scope_name,item_key,value_json
FROM user_config
WHERE owner=?
`, owner)
	if err != nil {
		return AppOverrides{}, err
	}
	defer rows.Close()

	var out AppOverrides
	for rows.Next() {
		var scopeName string
		var itemKey string
		var raw string
		if err := rows.Scan(&scopeName, &itemKey, &raw); err != nil {
			return AppOverrides{}, err
		}
		switch {
		case scopeName == scopeTrade && itemKey == keyEnabled:
			var enabled bool
			if err := json.Unmarshal([]byte(raw), &enabled); err != nil {
				return AppOverrides{}, fmt.Errorf("decode user_config %s.%s failed: %w", scopeName, itemKey, err)
			}
			out.Trade.Enabled = &enabled
		}
	}
	if err := rows.Err(); err != nil {
		return AppOverrides{}, err
	}
	return out, nil
}

func (s *Store) SaveTradeEnabled(owner string, enabled bool) error {
	return s.saveValue(owner, scopeTrade, keyEnabled, enabled)
}

func (s *Store) LoadRawValue(owner, scopeName, itemKey string) ([]byte, bool, error) {
	if owner == "" {
		owner = DefaultOwner
	}
	var raw string
	err := s.db.QueryRow(`
SELECT value_json
FROM user_config
WHERE owner=? AND scope_name=? AND item_key=?
`, owner, scopeName, itemKey).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return []byte(raw), true, nil
}

func (s *Store) SaveAppMode(owner string, mode string) error {
	return s.saveValue(owner, scopeAppMode, keyCurrentMode, mode)
}

func (s *Store) LoadAppMode(owner string) (string, bool, error) {
	raw, ok, err := s.LoadRawValue(owner, scopeAppMode, keyCurrentMode)
	if !ok || err != nil {
		return "", ok, err
	}
	var mode string
	if err := json.Unmarshal(raw, &mode); err != nil {
		return "", false, err
	}
	return mode, true, nil
}

func (s *Store) SaveReplayResumeCursor(owner string, cursor *bus.FileCursor) error {
	return s.saveValue(owner, scopeAppMode, keyReplayResumeCursor, cursor)
}

func (s *Store) LoadReplayResumeCursor(owner string) (*bus.FileCursor, bool, error) {
	raw, ok, err := s.LoadRawValue(owner, scopeAppMode, keyReplayResumeCursor)
	if !ok || err != nil {
		return nil, ok, err
	}
	if string(raw) == "null" {
		return nil, true, nil
	}
	var cursor bus.FileCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return nil, false, err
	}
	return &cursor, true, nil
}

func (s *Store) saveValue(owner, scopeName, itemKey string, value any) error {
	if owner == "" {
		owner = DefaultOwner
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO user_config(owner,scope_name,item_key,value_json,updated_at)
VALUES(?,?,?,?,?)
ON DUPLICATE KEY UPDATE
value_json=VALUES(value_json),
updated_at=VALUES(updated_at)
`, owner, scopeName, itemKey, string(raw), time.Now())
	return err
}

func ApplyAppOverrides(cfg config.AppConfig, overrides AppOverrides) config.AppConfig {
	if overrides.Trade.Enabled != nil {
		v := *overrides.Trade.Enabled
		cfg.Trade.Enabled = &v
	}
	return cfg
}
