package db

import (
	"database/sql"
	"fmt"
	"strings"

	"ctp-future-kline/internal/config"
)

func EnsureDatabaseAndSchema(cfg config.DBConfig, db *sql.DB) error {
	return ensureSchemaStatements(db, fullSchemaStatements())
}

func EnsureDatabaseAndSchemaForRole(cfg config.DBConfig, role string, db *sql.DB) error {
	return ensureSchemaStatements(db, schemaStatementsForRole(role))
}

func ensureSchemaStatements(db *sql.DB, stmts []string) error {
	if len(stmts) == 0 {
		return nil
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			stmtText := strings.ToLower(strings.TrimSpace(stmt))
			if (strings.Contains(stmtText, "create index") || strings.Contains(stmtText, "create unique index")) && isDuplicateObjectError(err) {
				continue
			}
			return fmt.Errorf("ensure mysql schema failed: %w", err)
		}
	}
	return nil
}

func schemaStatementsForRole(role string) []string {
	switch role {
	case RoleSharedMeta:
		return append([]string{}, sharedMetaSchemaStatements()...)
	case RoleMarketRealtime:
		return append([]string{}, marketSchemaStatements(false)...)
	case RoleMarketReplay:
		return append([]string{}, marketSchemaStatements(true)...)
	case RoleChartUserRealtime, RoleChartUserReplay:
		return append([]string{}, chartSchemaStatements()...)
	case RoleTradeLive, RoleTradePaperLive, RoleTradePaperReplay:
		return append([]string{}, tradeSchemaStatements()...)
	default:
		return fullSchemaStatements()
	}
}

func fullSchemaStatements() []string {
	stmts := []string{}
	stmts = append(stmts, sharedMetaSchemaStatements()...)
	stmts = append(stmts, marketSchemaStatements(true)...)
	stmts = append(stmts, chartSchemaStatements()...)
	stmts = append(stmts, tradeSchemaStatements()...)
	return stmts
}

func sharedMetaSchemaStatements() []string {
	return []string{
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
		`CREATE TABLE IF NOT EXISTS trading_sessions (
  variety VARCHAR(32) NOT NULL,
  session_text VARCHAR(255) NOT NULL,
  session_json JSON NOT NULL,
  is_completed TINYINT NOT NULL DEFAULT 0,
  sample_trade_date DATE NULL,
  validated_trade_date DATE NULL,
  match_ratio DOUBLE NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (variety)
)`,
		`CREATE INDEX idx_trading_sessions_completed ON trading_sessions(is_completed)`,
		`CREATE INDEX idx_trading_sessions_updated_at ON trading_sessions(updated_at)`,
		`CREATE TABLE IF NOT EXISTS ctp_instruments (
  instrument_id VARCHAR(64) NOT NULL,
  exchange_id VARCHAR(16) NOT NULL,
  exchange_inst_id VARCHAR(64) NOT NULL,
  instrument_name VARCHAR(128) NOT NULL,
  product_id VARCHAR(32) NOT NULL,
  product_class VARCHAR(8) NOT NULL,
  delivery_year INT NOT NULL DEFAULT 0,
  delivery_month INT NOT NULL DEFAULT 0,
  max_market_order_volume INT NOT NULL DEFAULT 0,
  min_market_order_volume INT NOT NULL DEFAULT 0,
  max_limit_order_volume INT NOT NULL DEFAULT 0,
  min_limit_order_volume INT NOT NULL DEFAULT 0,
  volume_multiple INT NOT NULL DEFAULT 0,
  price_tick DOUBLE NOT NULL DEFAULT 0,
  create_date VARCHAR(16) NOT NULL,
  open_date VARCHAR(16) NOT NULL,
  expire_date VARCHAR(16) NOT NULL,
  start_deliv_date VARCHAR(16) NOT NULL,
  end_deliv_date VARCHAR(16) NOT NULL,
  inst_life_phase VARCHAR(8) NOT NULL,
  is_trading TINYINT NOT NULL DEFAULT 0,
  position_type VARCHAR(8) NOT NULL,
  position_date_type VARCHAR(8) NOT NULL,
  long_margin_ratio DOUBLE NOT NULL DEFAULT 0,
  short_margin_ratio DOUBLE NOT NULL DEFAULT 0,
  max_margin_side_algorithm VARCHAR(8) NOT NULL,
  underlying_instr_id VARCHAR(64) NOT NULL,
  strike_price DOUBLE NOT NULL DEFAULT 0,
  options_type VARCHAR(8) NOT NULL,
  underlying_multiple DOUBLE NOT NULL DEFAULT 0,
  combination_type VARCHAR(8) NOT NULL,
  trading_day VARCHAR(16) NOT NULL DEFAULT '',
  sync_trading_day VARCHAR(16) NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (sync_trading_day, instrument_id, exchange_id)
)`,
		`CREATE INDEX idx_ctp_instruments_sync_day ON ctp_instruments(sync_trading_day, updated_at)`,
		`CREATE INDEX idx_ctp_instruments_trading_day ON ctp_instruments(trading_day, updated_at)`,
		`CREATE INDEX idx_ctp_instruments_product ON ctp_instruments(product_id)`,
		`CREATE TABLE IF NOT EXISTS ctp_instrument_sync_log (
  trading_day VARCHAR(16) PRIMARY KEY,
  instrument_count INT NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS ctp_commission_rates (
  instrument_id VARCHAR(64) NOT NULL,
  exchange_id VARCHAR(16) NOT NULL,
  open_ratio_by_money DOUBLE NOT NULL DEFAULT 0,
  open_ratio_by_volume DOUBLE NOT NULL DEFAULT 0,
  close_ratio_by_money DOUBLE NOT NULL DEFAULT 0,
  close_ratio_by_volume DOUBLE NOT NULL DEFAULT 0,
  close_today_ratio_by_money DOUBLE NOT NULL DEFAULT 0,
  close_today_ratio_by_volume DOUBLE NOT NULL DEFAULT 0,
  sync_trading_day VARCHAR(16) NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (sync_trading_day, instrument_id, exchange_id)
)`,
		`CREATE INDEX idx_ctp_commission_rates_sync_day ON ctp_commission_rates(sync_trading_day, updated_at)`,
		`CREATE TABLE IF NOT EXISTS ctp_margin_rates (
  instrument_id VARCHAR(64) NOT NULL,
  exchange_id VARCHAR(16) NOT NULL,
  hedge_flag VARCHAR(8) NOT NULL DEFAULT '',
  long_margin_ratio_by_money DOUBLE NOT NULL DEFAULT 0,
  long_margin_ratio_by_volume DOUBLE NOT NULL DEFAULT 0,
  short_margin_ratio_by_money DOUBLE NOT NULL DEFAULT 0,
  short_margin_ratio_by_volume DOUBLE NOT NULL DEFAULT 0,
  is_relative TINYINT NOT NULL DEFAULT 0,
  sync_trading_day VARCHAR(16) NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (instrument_id, exchange_id, hedge_flag)
)`,
		`CREATE INDEX idx_ctp_margin_rates_sync_day ON ctp_margin_rates(sync_trading_day, updated_at)`,
		`CREATE TABLE IF NOT EXISTS ctp_product_exchange (
  product_id VARCHAR(32) NOT NULL,
  product_id_norm VARCHAR(32) NOT NULL,
  exchange_id VARCHAR(16) NOT NULL,
  product_class VARCHAR(8) NOT NULL,
  volume_multiple INT NOT NULL DEFAULT 0,
  price_tick DOUBLE NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (product_id, exchange_id)
)`,
		`CREATE UNIQUE INDEX idx_ctp_product_exchange_norm_exchange ON ctp_product_exchange(product_id_norm, exchange_id)`,
		`CREATE INDEX idx_ctp_product_exchange_exchange ON ctp_product_exchange(exchange_id, updated_at)`,
		`CREATE TABLE IF NOT EXISTS user_config (
  owner VARCHAR(64) NOT NULL DEFAULT 'admin',
  scope_name VARCHAR(64) NOT NULL,
  item_key VARCHAR(128) NOT NULL,
  value_json JSON NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (owner, scope_name, item_key)
)`,
		`CREATE INDEX idx_user_config_updated_at ON user_config(updated_at DESC)`,
	}
}

func marketSchemaStatements(includeReplayDedup bool) []string {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS kline_search_index (
  table_name VARCHAR(191) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  symbol_norm VARCHAR(64) NOT NULL,
  variety VARCHAR(32) NOT NULL,
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
	if includeReplayDedup {
		stmts = append(stmts,
			`CREATE TABLE IF NOT EXISTS bus_consume_dedup (
  consumer_id VARCHAR(128) NOT NULL,
  event_id VARCHAR(128) NOT NULL,
  processed_at DATETIME NOT NULL,
  PRIMARY KEY (consumer_id, event_id)
)`,
			`CREATE INDEX idx_bus_consume_dedup_processed_at ON bus_consume_dedup(processed_at DESC)`,
		)
	}
	return stmts
}

func chartSchemaStatements() []string {
	return []string{
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
}

func tradeSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS strategy_definitions (
  strategy_id VARCHAR(128) NOT NULL,
  display_name VARCHAR(191) NOT NULL,
  entry_script VARCHAR(255) NOT NULL,
  version VARCHAR(64) NOT NULL,
  default_params_json JSON NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (strategy_id)
)`,
		`CREATE TABLE IF NOT EXISTS strategy_instances (
  instance_id VARCHAR(128) NOT NULL,
  strategy_id VARCHAR(128) NOT NULL,
  display_name VARCHAR(191) NOT NULL,
  mode VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  symbols_json JSON NOT NULL,
  timeframe VARCHAR(32) NOT NULL,
  params_json JSON NOT NULL,
  last_signal_at DATETIME NULL,
  last_target_position DOUBLE NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  updated_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (instance_id)
)`,
		`CREATE INDEX idx_strategy_instances_strategy_id ON strategy_instances(strategy_id)`,
		`CREATE INDEX idx_strategy_instances_mode_status ON strategy_instances(mode, status)`,
		`CREATE TABLE IF NOT EXISTS strategy_signals (
  id BIGINT NOT NULL AUTO_INCREMENT,
  instance_id VARCHAR(128) NOT NULL,
  strategy_id VARCHAR(128) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(32) NOT NULL,
  mode VARCHAR(32) NOT NULL,
  event_time DATETIME NOT NULL,
  target_position DOUBLE NOT NULL,
  confidence DOUBLE NOT NULL,
  reason TEXT NOT NULL,
  metrics_json JSON NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_strategy_signals_instance_time ON strategy_signals(instance_id, event_time DESC)`,
		`CREATE INDEX idx_strategy_signals_symbol_time ON strategy_signals(symbol, event_time DESC)`,
		`CREATE TABLE IF NOT EXISTS strategy_traces (
  trace_id BIGINT NOT NULL AUTO_INCREMENT,
  instance_id VARCHAR(128) NOT NULL,
  strategy_id VARCHAR(128) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(32) NOT NULL,
  mode VARCHAR(32) NOT NULL,
  event_type VARCHAR(32) NOT NULL,
  event_time DATETIME NOT NULL,
  step_key VARCHAR(128) NOT NULL,
  step_label VARCHAR(191) NOT NULL,
  step_index INT NOT NULL,
  step_total INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  reason TEXT NOT NULL,
  checks_json JSON NOT NULL,
  metrics_json JSON NOT NULL,
  signal_preview_json JSON NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (trace_id)
)`,
		`CREATE INDEX idx_strategy_traces_instance_time ON strategy_traces(instance_id, event_time DESC)`,
		`CREATE INDEX idx_strategy_traces_symbol_time ON strategy_traces(symbol, event_time DESC)`,
		`CREATE TABLE IF NOT EXISTS strategy_runs (
  run_id VARCHAR(128) NOT NULL,
  instance_id VARCHAR(128) NOT NULL,
  strategy_id VARCHAR(128) NOT NULL,
  run_type VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(32) NOT NULL,
  output_path VARCHAR(512) NOT NULL,
  summary_json JSON NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME NULL,
  last_error TEXT NULL,
  PRIMARY KEY (run_id)
)`,
		`CREATE INDEX idx_strategy_runs_instance_started ON strategy_runs(instance_id, started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS order_audit_logs (
  id BIGINT NOT NULL AUTO_INCREMENT,
  instance_id VARCHAR(128) NOT NULL,
  strategy_id VARCHAR(128) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  mode VARCHAR(32) NOT NULL,
  event_time DATETIME NOT NULL,
  target_position DOUBLE NOT NULL,
  current_position DOUBLE NOT NULL,
  planned_delta DOUBLE NOT NULL,
  risk_status VARCHAR(32) NOT NULL,
  risk_reason TEXT NOT NULL,
  order_status VARCHAR(32) NOT NULL,
  audit_json JSON NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_order_audit_instance_time ON order_audit_logs(instance_id, event_time DESC)`,
		`CREATE INDEX idx_order_audit_symbol_time ON order_audit_logs(symbol, event_time DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_accounts (
  account_id VARCHAR(128) NOT NULL,
  broker_id VARCHAR(64) NOT NULL,
  investor_id VARCHAR(64) NOT NULL,
  display_name VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (account_id)
)`,
		`CREATE TABLE IF NOT EXISTS trade_account_snapshots (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id VARCHAR(128) NOT NULL,
  static_balance DOUBLE NOT NULL DEFAULT 0,
  balance DOUBLE NOT NULL,
  available DOUBLE NOT NULL,
  margin_value DOUBLE NOT NULL,
  frozen_margin DOUBLE NOT NULL DEFAULT 0,
  frozen_commission DOUBLE NOT NULL DEFAULT 0,
  frozen_premium DOUBLE NOT NULL DEFAULT 0,
  frozen_cash DOUBLE NOT NULL,
  deposit DOUBLE NOT NULL DEFAULT 0,
  withdraw DOUBLE NOT NULL DEFAULT 0,
  premium DOUBLE NOT NULL DEFAULT 0,
  other_fee DOUBLE NOT NULL DEFAULT 0,
  commission DOUBLE NOT NULL,
  close_profit DOUBLE NOT NULL,
  position_profit DOUBLE NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_trade_account_snapshots_account_time ON trade_account_snapshots(account_id, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_positions (
  account_id VARCHAR(128) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  exchange_id VARCHAR(32) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  hedge_flag VARCHAR(16) NOT NULL,
  yd_position INT NOT NULL,
  today_position INT NOT NULL,
  position INT NOT NULL,
  open_cost DOUBLE NOT NULL,
  position_cost DOUBLE NOT NULL,
  use_margin DOUBLE NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (account_id, symbol, direction, hedge_flag)
)`,
		`CREATE INDEX idx_trade_positions_symbol ON trade_positions(symbol, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_orders (
  command_id VARCHAR(128) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  order_ref VARCHAR(64) NOT NULL,
  front_id INT NOT NULL,
  session_id INT NOT NULL,
  exchange_id VARCHAR(32) NOT NULL,
  order_sys_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  offset_flag VARCHAR(32) NOT NULL,
  limit_price DOUBLE NOT NULL,
  volume_total_original INT NOT NULL,
  volume_traded INT NOT NULL,
  volume_canceled INT NOT NULL,
  order_status VARCHAR(32) NOT NULL,
  submit_status VARCHAR(32) NOT NULL,
  status_msg TEXT NOT NULL,
  inserted_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (command_id)
)`,
		`CREATE UNIQUE INDEX idx_trade_orders_account_orderref ON trade_orders(account_id, order_ref)`,
		`CREATE INDEX idx_trade_orders_symbol_time ON trade_orders(symbol, updated_at DESC)`,
		`CREATE INDEX idx_trade_orders_ordersysid ON trade_orders(account_id, exchange_id, order_sys_id)`,
		`CREATE TABLE IF NOT EXISTS trade_trades (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id VARCHAR(128) NOT NULL,
  trade_id VARCHAR(64) NOT NULL,
  order_ref VARCHAR(64) NOT NULL,
  order_sys_id VARCHAR(64) NOT NULL,
  exchange_id VARCHAR(32) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  offset_flag VARCHAR(32) NOT NULL,
  price DOUBLE NOT NULL,
  volume INT NOT NULL,
  trade_time DATETIME NOT NULL,
  trading_day VARCHAR(16) NOT NULL,
  received_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE UNIQUE INDEX idx_trade_trades_unique ON trade_trades(account_id, exchange_id, trade_id)`,
		`CREATE INDEX idx_trade_trades_symbol_time ON trade_trades(symbol, trade_time DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_command_audits (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id VARCHAR(128) NOT NULL,
  command_id VARCHAR(128) NOT NULL,
  command_type VARCHAR(32) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  risk_status VARCHAR(32) NOT NULL,
  risk_reason TEXT NOT NULL,
  request_json JSON NOT NULL,
  response_json JSON NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_trade_command_audits_command ON trade_command_audits(command_id)`,
		`CREATE INDEX idx_trade_command_audits_symbol_time ON trade_command_audits(symbol, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_query_audits (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id VARCHAR(128) NOT NULL,
  query_type VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  detail TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (id)
)`,
		`CREATE INDEX idx_trade_query_audits_account_time ON trade_query_audits(account_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS trade_session_state (
  account_id VARCHAR(128) NOT NULL,
  front_id INT NOT NULL,
  session_id INT NOT NULL,
  next_order_ref BIGINT NOT NULL,
  connected TINYINT NOT NULL,
  authenticated TINYINT NOT NULL,
  logged_in TINYINT NOT NULL,
  settlement_confirmed TINYINT NOT NULL,
  trading_day VARCHAR(16) NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (account_id)
)`,
	}
}

func isDuplicateObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") ||
		strings.Contains(msg, "already exists")
}
