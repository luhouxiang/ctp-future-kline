package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ctp-future-kline/internal/config"
)

func TestLoadSuccessAndDefaults(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_product_info": "",
    "user_id": "888888",
    "password": "simnowpassword",
    "subscribe_instruments": ["rb2405"],
    "connect_wait_seconds": 0,
    "authenticate_wait_seconds": -1,
    "login_wait_seconds": 0,
    "md_connect_wait_seconds": 0,
    "md_login_wait_seconds": -1,
    "md_receive_seconds": 0
  }
}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.CTP.ConnectWaitSeconds != 5 {
		t.Fatalf("ConnectWaitSeconds = %d, want 5", cfg.CTP.ConnectWaitSeconds)
	}
	if cfg.CTP.AuthenticateWaitSeconds != 5 {
		t.Fatalf("AuthenticateWaitSeconds = %d, want 5", cfg.CTP.AuthenticateWaitSeconds)
	}
	if cfg.CTP.LoginWaitSeconds != 5 {
		t.Fatalf("LoginWaitSeconds = %d, want 5", cfg.CTP.LoginWaitSeconds)
	}
	if cfg.CTP.MdConnectWaitSeconds != 5 {
		t.Fatalf("MdConnectWaitSeconds = %d, want 5", cfg.CTP.MdConnectWaitSeconds)
	}
	if cfg.CTP.MdLoginWaitSeconds != 5 {
		t.Fatalf("MdLoginWaitSeconds = %d, want 5", cfg.CTP.MdLoginWaitSeconds)
	}
	if cfg.CTP.MdReceiveSeconds != 30 {
		t.Fatalf("MdReceiveSeconds = %d, want 30", cfg.CTP.MdReceiveSeconds)
	}
	if !cfg.CTP.IsL9AsyncEnabled() {
		t.Fatal("IsL9AsyncEnabled() = false, want true when config missing")
	}
	if cfg.Web.ListenAddr != "127.0.0.1:8080" {
		t.Fatalf("Web.ListenAddr = %q, want 127.0.0.1:8080", cfg.Web.ListenAddr)
	}
	if cfg.Web.MarketOpenStaleSeconds != 60 {
		t.Fatalf("Web.MarketOpenStaleSeconds = %d, want 60", cfg.Web.MarketOpenStaleSeconds)
	}
	if !cfg.Web.IsAutoOpenBrowser() {
		t.Fatal("Web.IsAutoOpenBrowser() = false, want true by default")
	}
	if cfg.Web.DrawDebugDefault != 0 {
		t.Fatalf("Web.DrawDebugDefault = %d, want 0", cfg.Web.DrawDebugDefault)
	}
	if cfg.Web.BrowserLogDefault != 0 {
		t.Fatalf("Web.BrowserLogDefault = %d, want 0", cfg.Web.BrowserLogDefault)
	}
	if cfg.Web.IsDrawDebugDefaultEnabled() {
		t.Fatal("Web.IsDrawDebugDefaultEnabled() = true, want false by default")
	}
	if cfg.Web.IsBrowserLogDefaultEnabled() {
		t.Fatal("Web.IsBrowserLogDefaultEnabled() = true, want false by default")
	}
	if !cfg.CTP.IsMdReconnectEnabled() {
		t.Fatal("IsMdReconnectEnabled() = false, want true by default")
	}
	if cfg.CTP.MdReconnectInitialMS != 1000 {
		t.Fatalf("MdReconnectInitialMS = %d, want 1000", cfg.CTP.MdReconnectInitialMS)
	}
	if cfg.CTP.MdReconnectMaxMS != 30000 {
		t.Fatalf("MdReconnectMaxMS = %d, want 30000", cfg.CTP.MdReconnectMaxMS)
	}
	if cfg.CTP.MdReconnectJitterRatio != 0.2 {
		t.Fatalf("MdReconnectJitterRatio = %v, want 0.2", cfg.CTP.MdReconnectJitterRatio)
	}
	if cfg.CTP.MdReloginWaitSeconds != 3 {
		t.Fatalf("MdReloginWaitSeconds = %d, want 3", cfg.CTP.MdReloginWaitSeconds)
	}
	if cfg.CTP.TickDedupWindowSeconds != 2 {
		t.Fatalf("TickDedupWindowSeconds = %d, want 2", cfg.CTP.TickDedupWindowSeconds)
	}
	if cfg.CTP.DriftThresholdSeconds != 5 {
		t.Fatalf("DriftThresholdSeconds = %d, want 5", cfg.CTP.DriftThresholdSeconds)
	}
	if cfg.CTP.DriftResumeTicks != 3 {
		t.Fatalf("DriftResumeTicks = %d, want 3", cfg.CTP.DriftResumeTicks)
	}
	if cfg.CTP.NoTickWarnSeconds != 60 {
		t.Fatalf("NoTickWarnSeconds = %d, want 60", cfg.CTP.NoTickWarnSeconds)
	}
	if !cfg.CTP.IsBusEnabled() {
		t.Fatal("IsBusEnabled() = false, want true by default")
	}
	if cfg.CTP.BusFlushMS != 200 {
		t.Fatalf("BusFlushMS = %d, want 200", cfg.CTP.BusFlushMS)
	}
	if cfg.CTP.ReplayDefaultMode != "realtime" {
		t.Fatalf("ReplayDefaultMode = %q, want realtime", cfg.CTP.ReplayDefaultMode)
	}
	if cfg.CTP.ReplayDefaultSpeed != 1.0 {
		t.Fatalf("ReplayDefaultSpeed = %v, want 1.0", cfg.CTP.ReplayDefaultSpeed)
	}
	if !cfg.CTP.IsReplayAllowOrderCommandDispatch() {
		t.Fatal("IsReplayAllowOrderCommandDispatch() = false, want true")
	}
	if cfg.CTP.QueueSpoolDir != filepath.Join(".", "flow", "queue_spool") {
		t.Fatalf("QueueSpoolDir = %q, want %q", cfg.CTP.QueueSpoolDir, filepath.Join(".", "flow", "queue_spool"))
	}
	if cfg.CTP.QueueAlertWarnPercent != 60 || cfg.CTP.QueueAlertCriticalPercent != 80 || cfg.CTP.QueueAlertEmergencyPercent != 95 || cfg.CTP.QueueAlertRecoverPercent != 50 {
		t.Fatalf("unexpected queue alert defaults: warn=%d critical=%d emergency=%d recover=%d", cfg.CTP.QueueAlertWarnPercent, cfg.CTP.QueueAlertCriticalPercent, cfg.CTP.QueueAlertEmergencyPercent, cfg.CTP.QueueAlertRecoverPercent)
	}
	if cfg.CTP.ShardCapacity != 8192 || cfg.CTP.PersistCapacity != 16384 || cfg.CTP.MMDeferredCapacity != 16384 || cfg.CTP.L9TaskCapacity != 4096 {
		t.Fatalf("unexpected primary queue defaults: shard=%d persist=%d mm=%d l9=%d", cfg.CTP.ShardCapacity, cfg.CTP.PersistCapacity, cfg.CTP.MMDeferredCapacity, cfg.CTP.L9TaskCapacity)
	}
	if cfg.CTP.DBWriterCount != 4 || cfg.CTP.DBFlushBatch != 512 || cfg.CTP.DBFlushIntervalMS != 30 || cfg.CTP.MMDeferredIntervalMS != 100 || cfg.CTP.MMDeferredBatch != 512 {
		t.Fatalf("unexpected db writer defaults: workers=%d flush_batch=%d flush_interval=%d mm_interval=%d mm_batch=%d",
			cfg.CTP.DBWriterCount, cfg.CTP.DBFlushBatch, cfg.CTP.DBFlushIntervalMS, cfg.CTP.MMDeferredIntervalMS, cfg.CTP.MMDeferredBatch)
	}
	if cfg.CTP.FilePerShardCapacity != 1025 || cfg.CTP.SideEffectTickCapacity != 16384 || cfg.CTP.SideEffectBarCapacity != 4096 {
		t.Fatalf("unexpected side queue defaults: file=%d tick=%d bar=%d", cfg.CTP.FilePerShardCapacity, cfg.CTP.SideEffectTickCapacity, cfg.CTP.SideEffectBarCapacity)
	}
	if cfg.CTP.ChartSubscriberCapacity != 4096 || cfg.CTP.StatusSubscriberCapacity != 16 {
		t.Fatalf("unexpected subscriber defaults: chart=%d status=%d", cfg.CTP.ChartSubscriberCapacity, cfg.CTP.StatusSubscriberCapacity)
	}
	if cfg.CTP.StrategyEventCapacity != 32 || cfg.CTP.TradeEventCapacity != 64 || cfg.CTP.TradeGatewayEventCapacity != 32 || cfg.CTP.MDDisconnectCapacity != 16 {
		t.Fatalf("unexpected event queue defaults: strategy=%d trade=%d gateway=%d md_disconnect=%d", cfg.CTP.StrategyEventCapacity, cfg.CTP.TradeEventCapacity, cfg.CTP.TradeGatewayEventCapacity, cfg.CTP.MDDisconnectCapacity)
	}
	if cfg.DB.Driver != "mysql" {
		t.Fatalf("DB.Driver = %q, want mysql", cfg.DB.Driver)
	}
	if cfg.DB.Host != "localhost" {
		t.Fatalf("DB.Host = %q, want localhost", cfg.DB.Host)
	}
	if cfg.DB.Port != 3306 {
		t.Fatalf("DB.Port = %d, want 3306", cfg.DB.Port)
	}
	if cfg.DB.User != "root" {
		t.Fatalf("DB.User = %q, want root", cfg.DB.User)
	}
	if cfg.DB.Database != "future_kline" {
		t.Fatalf("DB.Database = %q, want future_kline", cfg.DB.Database)
	}
	if cfg.DB.SharedMetaDatabase != "future_shared_meta" {
		t.Fatalf("DB.SharedMetaDatabase = %q, want future_shared_meta", cfg.DB.SharedMetaDatabase)
	}
	if cfg.DB.MarketRealtimeDatabase != "future_market_realtime" {
		t.Fatalf("DB.MarketRealtimeDatabase = %q, want future_market_realtime", cfg.DB.MarketRealtimeDatabase)
	}
	if cfg.DB.MarketReplayDatabase != "future_market_replay" {
		t.Fatalf("DB.MarketReplayDatabase = %q, want future_market_replay", cfg.DB.MarketReplayDatabase)
	}
	if cfg.DB.TradeLiveDatabase != "future_trade_live_realtime" {
		t.Fatalf("DB.TradeLiveDatabase = %q, want future_trade_live_realtime", cfg.DB.TradeLiveDatabase)
	}
	if cfg.DB.TradePaperLiveDatabase != "future_trade_sim_realtime" {
		t.Fatalf("DB.TradePaperLiveDatabase = %q, want future_trade_sim_realtime", cfg.DB.TradePaperLiveDatabase)
	}
	if cfg.DB.TradePaperReplayDatabase != "future_trade_sim_replay" {
		t.Fatalf("DB.TradePaperReplayDatabase = %q, want future_trade_sim_replay", cfg.DB.TradePaperReplayDatabase)
	}
	if cfg.DB.ChartUserRealtimeDatabase != "future_chart_user_realtime" {
		t.Fatalf("DB.ChartUserRealtimeDatabase = %q, want future_chart_user_realtime", cfg.DB.ChartUserRealtimeDatabase)
	}
	if cfg.DB.ChartUserReplayDatabase != "future_chart_user_replay" {
		t.Fatalf("DB.ChartUserReplayDatabase = %q, want future_chart_user_replay", cfg.DB.ChartUserReplayDatabase)
	}
	if cfg.Trade.IsEnabled() {
		t.Fatal("Trade.IsEnabled() = true, want false by default")
	}
	if cfg.Trade.AccountID != "default" {
		t.Fatalf("Trade.AccountID = %q, want default", cfg.Trade.AccountID)
	}
	if !cfg.Trade.IsAutoConfirmSettlement() {
		t.Fatal("Trade.IsAutoConfirmSettlement() = false, want true")
	}
	if !cfg.Trade.IsBlockStrategyLiveOrder() {
		t.Fatal("Trade.IsBlockStrategyLiveOrder() = false, want true")
	}
	if cfg.Trade.MaxOrderVolume != 10 {
		t.Fatalf("Trade.MaxOrderVolume = %d, want 10", cfg.Trade.MaxOrderVolume)
	}
	if cfg.Trade.QueryPollIntervalMS != 5000 {
		t.Fatalf("Trade.QueryPollIntervalMS = %d, want 5000", cfg.Trade.QueryPollIntervalMS)
	}
	if cfg.Trade.PositionSyncIntervalMS != 3000 {
		t.Fatalf("Trade.PositionSyncIntervalMS = %d, want 3000", cfg.Trade.PositionSyncIntervalMS)
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{ invalid json }`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want parse error")
	}
	if !strings.Contains(err.Error(), "parse config file failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()

	_, err := config.Load(filepath.Join(t.TempDir(), "not-exist.json"))
	if err == nil {
		t.Fatal("Load() error = nil, want read error")
	}
	if !strings.Contains(err.Error(), "read config file failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name: "missing flow_path",
			content: `{
  "ctp": {
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "subscribe_instruments": ["rb2405"]
  }
}`,
			wantErr: "ctp.flow_path is required",
		},
		{
			name: "missing md_front_addr",
			content: `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "subscribe_instruments": ["rb2405"]
  }
}`,
			wantErr: "ctp.md_front_addr is required",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeTempConfig(t, tc.content)

			_, err := config.Load(path)
			if err == nil {
				t.Fatalf("Load() error = nil, want %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load() error = %v, want contains %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadSubscribeInstrumentsOptional(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword"
  }
}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.CTP.SubscribeInstruments) != 0 {
		t.Fatalf("SubscribeInstruments len = %d, want 0", len(cfg.CTP.SubscribeInstruments))
	}
}

func TestLoadEnableL9AsyncConfig(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "enable_l9_async": false
  }
}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CTP.IsL9AsyncEnabled() {
		t.Fatal("IsL9AsyncEnabled() = true, want false")
	}
}

func TestLoadCTPAccountConfigByUserID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{
  "ctp": {
    "flow_path": "./flow",
    "user_id": "888888"
  }
}`), 0o644); err != nil {
		t.Fatalf("write test config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "888888.json"), []byte(`{
  "trader_front_addr": "tcp://180.168.146.187:10201",
  "md_front_addr": "tcp://180.168.146.187:10211",
  "broker_id": "9999",
  "app_id": "simnow_client_test",
  "auth_code": "0000000000000000",
  "user_product_info": "",
  "password": "simnowpassword"
}`), 0o644); err != nil {
		t.Fatalf("write account config failed: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CTP.TraderFrontAddr != "tcp://180.168.146.187:10201" {
		t.Fatalf("TraderFrontAddr = %q, want account config value", cfg.CTP.TraderFrontAddr)
	}
	if cfg.CTP.MdFrontAddr != "tcp://180.168.146.187:10211" {
		t.Fatalf("MdFrontAddr = %q, want account config value", cfg.CTP.MdFrontAddr)
	}
	if cfg.CTP.BrokerID != "9999" || cfg.CTP.AppID != "simnow_client_test" || cfg.CTP.AuthCode != "0000000000000000" {
		t.Fatalf("unexpected merged account config: %+v", cfg.CTP)
	}
	if cfg.CTP.Password != "simnowpassword" {
		t.Fatalf("Password = %q, want account config value", cfg.CTP.Password)
	}
}

func TestLoadCTPAccountConfigFromUserIDPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configDir := filepath.Join(root, "config")
	privateDir := filepath.Join(root, "private")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir failed: %v", err)
	}
	if err := os.MkdirAll(privateDir, 0o755); err != nil {
		t.Fatalf("mkdir private dir failed: %v", err)
	}
	path := filepath.Join(configDir, "config.json")
	if err := os.WriteFile(path, []byte(`{
  "ctp": {
    "flow_path": "./flow",
    "user_id": "888888",
    "user_id_path": "../private"
  }
}`), 0o644); err != nil {
		t.Fatalf("write test config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(privateDir, "888888.json"), []byte(`{
  "trader_front_addr": "tcp://180.168.146.187:10201",
  "md_front_addr": "tcp://180.168.146.187:10211",
  "broker_id": "9999",
  "app_id": "simnow_client_test",
  "auth_code": "0000000000000000",
  "password": "simnowpassword"
}`), 0o644); err != nil {
		t.Fatalf("write private account config failed: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CTP.TraderFrontAddr != "tcp://180.168.146.187:10201" {
		t.Fatalf("TraderFrontAddr = %q, want private account config value", cfg.CTP.TraderFrontAddr)
	}
	if cfg.CTP.Password != "simnowpassword" {
		t.Fatalf("Password = %q, want private account config value", cfg.CTP.Password)
	}
}

func TestLoadCTPAccountConfigUserIDMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{
  "ctp": {
    "flow_path": "./flow",
    "user_id": "888888"
  }
}`), 0o644); err != nil {
		t.Fatalf("write test config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "888888.json"), []byte(`{
  "user_id": "999999",
  "trader_front_addr": "tcp://180.168.146.187:10201",
  "md_front_addr": "tcp://180.168.146.187:10211",
  "broker_id": "9999",
  "app_id": "simnow_client_test",
  "auth_code": "0000000000000000",
  "password": "simnowpassword"
}`), 0o644); err != nil {
		t.Fatalf("write account config failed: %v", err)
	}

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "ctp account config user_id mismatch") {
		t.Fatalf("Load() error = %v, want user_id mismatch", err)
	}
}

func TestLoadInvalidReconnectRange(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "md_reconnect_initial_backoff_ms": 2000,
    "md_reconnect_max_backoff_ms": 1000
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "md_reconnect_max_backoff_ms must be >=") {
		t.Fatalf("Load() error = %v, want reconnect range validation error", err)
	}
}

func TestLoadInvalidReconnectJitter(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "md_reconnect_jitter_ratio": 1.5
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "md_reconnect_jitter_ratio") {
		t.Fatalf("Load() error = %v, want reconnect jitter validation error", err)
	}
}

func TestLoadInvalidDriftResumeTicks(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "drift_resume_consecutive_ticks": -1
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "drift_resume_consecutive_ticks") {
		t.Fatalf("Load() error = %v, want drift resume validation error", err)
	}
}

func TestLoadInvalidReplayMode(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "replay_default_mode": "invalid"
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "replay_default_mode") {
		t.Fatalf("Load() error = %v, want replay mode validation error", err)
	}
}

func TestLoadInvalidReplaySpeed(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "replay_default_speed": -1
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "replay_default_speed") {
		t.Fatalf("Load() error = %v, want replay speed validation error", err)
	}
}

func TestLoadInvalidQueueAlertThresholds(t *testing.T) {
	t.Parallel()

	path := writeTempConfig(t, `{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.168.146.187:10201",
    "md_front_addr": "tcp://180.168.146.187:10211",
    "broker_id": "9999",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "user_id": "888888",
    "password": "simnowpassword",
    "queue_alert_warn_percent": 90,
    "queue_alert_critical_percent": 80
  }
}`)

	_, err := config.Load(path)
	if err == nil || !strings.Contains(err.Error(), "queue_alert_critical_percent") {
		t.Fatalf("Load() error = %v, want queue alert threshold validation error", err)
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write test config failed: %v", err)
	}
	return path
}
