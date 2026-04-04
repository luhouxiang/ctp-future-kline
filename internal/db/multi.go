package db

import "ctp-future-kline/internal/config"

const (
	RoleSharedMeta        = "shared_meta"
	RoleMarketRealtime    = "market_realtime"
	RoleMarketReplay      = "market_replay"
	RoleTradeLive         = "trade_realtime_live"
	RoleTradePaperLive    = "trade_sim_realtime"
	RoleTradePaperReplay  = "trade_sim_replay"
	RoleChartUserRealtime = "chart_user_realtime"
	RoleChartUserReplay   = "chart_user_replay"
)

func ConfigForRole(cfg config.DBConfig, role string) config.DBConfig {
	out := cfg
	out.Database = DatabaseForRole(cfg, role)
	return out
}

func DatabaseForRole(cfg config.DBConfig, role string) string {
	base := baseDatabaseName(cfg.Database)
	switch role {
	case RoleSharedMeta:
		return firstNonEmpty(cfg.SharedMetaDatabase, base+"_shared_meta", cfg.Database)
	case RoleMarketRealtime:
		return firstNonEmpty(cfg.MarketRealtimeDatabase, base+"_market_realtime", cfg.Database)
	case RoleMarketReplay:
		return firstNonEmpty(cfg.MarketReplayDatabase, base+"_market_replay", cfg.Database)
	case RoleTradeLive:
		return firstNonEmpty(cfg.TradeLiveDatabase, base+"_trade_live_realtime", cfg.Database)
	case RoleTradePaperLive:
		return firstNonEmpty(cfg.TradePaperLiveDatabase, base+"_trade_sim_realtime", cfg.Database)
	case RoleTradePaperReplay:
		return firstNonEmpty(cfg.TradePaperReplayDatabase, base+"_trade_sim_replay", cfg.Database)
	case RoleChartUserRealtime:
		return firstNonEmpty(cfg.ChartUserRealtimeDatabase, base+"_chart_user_realtime", cfg.Database)
	case RoleChartUserReplay:
		return firstNonEmpty(cfg.ChartUserReplayDatabase, base+"_chart_user_replay", cfg.Database)
	default:
		return cfg.Database
	}
}

func DSNForRole(cfg config.DBConfig, role string) string {
	return BuildDSN(ConfigForRole(cfg, role))
}

func EnsureAllLogicalDatabases(cfg config.DBConfig) error {
	roles := []string{
		RoleSharedMeta,
		RoleMarketRealtime,
		RoleMarketReplay,
		RoleTradeLive,
		RoleTradePaperLive,
		RoleTradePaperReplay,
		RoleChartUserRealtime,
		RoleChartUserReplay,
	}
	seen := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		dbName := DatabaseForRole(cfg, role)
		if _, ok := seen[dbName]; ok {
			continue
		}
		seen[dbName] = struct{}{}
		roleCfg := ConfigForRole(cfg, role)
		if err := EnsureDatabase(roleCfg); err != nil {
			return err
		}
		db, err := Open(BuildDSN(roleCfg))
		if err != nil {
			return err
		}
		if err := EnsureDatabaseAndSchema(roleCfg, db); err != nil {
			_ = db.Close()
			return err
		}
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func baseDatabaseName(name string) string {
	if len(name) > len("_kline") && name[len(name)-len("_kline"):] == "_kline" {
		return name[:len(name)-len("_kline")]
	}
	return name
}
