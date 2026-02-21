package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ctp-go-demo/internal/config"
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
	if cfg.CTP.ReplayDefaultMode != "fast" {
		t.Fatalf("ReplayDefaultMode = %q, want fast", cfg.CTP.ReplayDefaultMode)
	}
	if cfg.CTP.ReplayDefaultSpeed != 1.0 {
		t.Fatalf("ReplayDefaultSpeed = %v, want 1.0", cfg.CTP.ReplayDefaultSpeed)
	}
	if !cfg.CTP.IsReplayAllowOrderCommandDispatch() {
		t.Fatal("IsReplayAllowOrderCommandDispatch() = false, want true")
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

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write test config failed: %v", err)
	}
	return path
}
