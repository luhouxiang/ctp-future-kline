package userconfig

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/klinesettings"
)

const (
	DefaultOwner = "admin"

	scopeTrade            = "trade"
	scopeAppMode          = "app_mode"
	scopeKlineGeneration  = "kline_generation"
	scopeStrategy         = "strategy"
	keyEnabled            = "enabled"
	keyCurrentMode        = "current_mode"
	keyReplayResumeCursor = "replay_resume_cursor"
	keyKlineSettings      = "settings"
	keyCompositions       = "compositions"
)

// Store 管理用户级配置覆盖项的持久化读写。
type Store struct {
	db *sql.DB
}

// StrategyComposition 描述前端编排的一组策略实例模板。
//
// 运行时仍落到普通 strategy_instances：helper 策略先启动并产生只读 features，
// primary 策略最后启动并根据 features 裁决最终开平仓信号。
type StrategyComposition struct {
	CompositionID          string                    `json:"composition_id"`
	DisplayName            string                    `json:"display_name"`
	PrimaryStrategyID      string                    `json:"primary_strategy_id"`
	HelperStrategyIDs      []string                  `json:"helper_strategy_ids"`
	PrimaryParams          map[string]any            `json:"primary_params"`
	HelperParamsByStrategy map[string]map[string]any `json:"helper_params_by_strategy"`
	UpdatedAt              time.Time                 `json:"updated_at"`
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

func (s *Store) SaveKlineGenerationSettings(owner string, settings klinesettings.Settings) error {
	return s.saveValue(owner, scopeKlineGeneration, keyKlineSettings, klinesettings.Normalize(settings))
}

func (s *Store) LoadKlineGenerationSettings(owner string) (klinesettings.Settings, bool, error) {
	raw, ok, err := s.LoadRawValue(owner, scopeKlineGeneration, keyKlineSettings)
	if !ok || err != nil {
		return klinesettings.Default(), ok, err
	}
	var settings klinesettings.Settings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return klinesettings.Default(), false, err
	}
	return klinesettings.Normalize(settings), true, nil
}

func (s *Store) SaveStrategyCompositions(owner string, items []StrategyComposition) error {
	return s.saveValue(owner, scopeStrategy, keyCompositions, normalizeStrategyCompositions(items))
}

func (s *Store) LoadStrategyCompositions(owner string) ([]StrategyComposition, bool, error) {
	raw, ok, err := s.LoadRawValue(owner, scopeStrategy, keyCompositions)
	if !ok || err != nil {
		return nil, ok, err
	}
	var items []StrategyComposition
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, false, err
	}
	return normalizeStrategyCompositions(items), true, nil
}

func normalizeStrategyCompositions(items []StrategyComposition) []StrategyComposition {
	out := make([]StrategyComposition, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.CompositionID == "" || item.PrimaryStrategyID == "" {
			continue
		}
		if _, exists := seen[item.CompositionID]; exists {
			continue
		}
		seen[item.CompositionID] = struct{}{}
		if item.DisplayName == "" {
			item.DisplayName = item.CompositionID
		}
		if item.PrimaryParams == nil {
			item.PrimaryParams = map[string]any{}
		}
		if item.HelperParamsByStrategy == nil {
			item.HelperParamsByStrategy = map[string]map[string]any{}
		}
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now()
		}
		out = append(out, item)
	}
	return out
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
