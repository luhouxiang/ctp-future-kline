package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type AppConfig struct {
	CTP      CTPConfig      `json:"ctp"`
	DB       DBConfig       `json:"db"`
	Web      WebConfig      `json:"web"`
	Calendar CalendarConfig `json:"calendar"`
}

type DBConfig struct {
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	Params   string `json:"params"`
}

type CTPConfig struct {
	FlowPath                string   `json:"flow_path"`
	TraderFrontAddr         string   `json:"trader_front_addr"`
	MdFrontAddr             string   `json:"md_front_addr"`
	BrokerID                string   `json:"broker_id"`
	AppID                   string   `json:"app_id"`
	AuthCode                string   `json:"auth_code"`
	UserProductInfo         string   `json:"user_product_info"`
	UserID                  string   `json:"user_id"`
	Password                string   `json:"password"`
	SubscribeInstruments    []string `json:"subscribe_instruments"`
	EnableL9Async           *bool    `json:"enable_l9_async"`
	ConnectWaitSeconds      int      `json:"connect_wait_seconds"`
	AuthenticateWaitSeconds int      `json:"authenticate_wait_seconds"`
	LoginWaitSeconds        int      `json:"login_wait_seconds"`
	MdConnectWaitSeconds    int      `json:"md_connect_wait_seconds"`
	MdLoginWaitSeconds      int      `json:"md_login_wait_seconds"`
	MdReceiveSeconds        int      `json:"md_receive_seconds"`
	MdReconnectEnabled      *bool    `json:"md_reconnect_enabled"`
	MdReconnectInitialMS    int      `json:"md_reconnect_initial_backoff_ms"`
	MdReconnectMaxMS        int      `json:"md_reconnect_max_backoff_ms"`
	MdReconnectJitterRatio  float64  `json:"md_reconnect_jitter_ratio"`
	MdReloginWaitSeconds    int      `json:"md_relogin_wait_seconds"`
	TickDedupWindowSeconds  int      `json:"tick_dedup_window_seconds"`
	DriftThresholdSeconds   int      `json:"drift_threshold_seconds"`
	DriftResumeTicks        int      `json:"drift_resume_consecutive_ticks"`
	NoTickWarnSeconds       int      `json:"no_tick_warn_seconds"`
	BusEnabled              *bool    `json:"bus_enabled"`
	BusLogPath              string   `json:"bus_log_path"`
	BusFlushMS              int      `json:"bus_flush_ms"`
	ReplayDefaultMode       string   `json:"replay_default_mode"`
	ReplayDefaultSpeed      float64  `json:"replay_default_speed"`
	ReplayAllowOrderCommand *bool    `json:"replay_allow_order_command_dispatch"`
	DBDSN                   string   `json:"-"`
}

type WebConfig struct {
	ListenAddr             string `json:"listen_addr"`
	AutoOpenBrowser        *bool  `json:"auto_open_browser"`
	MarketOpenStaleSeconds int    `json:"market_open_stale_seconds"`
	DrawDebugDefault       int    `json:"draw_debug_default"`
	BrowserLogDefault      int    `json:"browser_log_default"`
}

type CalendarConfig struct {
	AutoUpdateOnStart  *bool  `json:"auto_update_on_start"`
	MinFutureOpenDays  int    `json:"min_future_open_days"`
	SourceURL          string `json:"source_url"`
	SourceCSVPath      string `json:"source_csv_path"`
	CheckIntervalHours int    `json:"check_interval_hours"`
	BrowserFallback    *bool  `json:"browser_fallback"`
	BrowserPath        string `json:"browser_path"`
	BrowserHeadless    *bool  `json:"browser_headless"`
}

func Load(path string) (AppConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return AppConfig{}, fmt.Errorf("read config file failed: %w", err)
	}
	content = bytes.TrimPrefix(content, []byte{0xEF, 0xBB, 0xBF})

	var cfg AppConfig
	if err := json.Unmarshal(content, &cfg); err != nil {
		return AppConfig{}, fmt.Errorf("parse config file failed: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return AppConfig{}, err
	}
	return cfg, nil
}

func (c *AppConfig) Validate() error {
	if c.CTP.FlowPath == "" {
		return errors.New("ctp.flow_path is required")
	}
	if c.CTP.TraderFrontAddr == "" {
		return errors.New("ctp.trader_front_addr is required")
	}
	if c.CTP.MdFrontAddr == "" {
		return errors.New("ctp.md_front_addr is required")
	}
	if c.CTP.BrokerID == "" {
		return errors.New("ctp.broker_id is required")
	}
	if c.CTP.AppID == "" {
		return errors.New("ctp.app_id is required")
	}
	if c.CTP.AuthCode == "" {
		return errors.New("ctp.auth_code is required")
	}
	if c.CTP.UserID == "" {
		return errors.New("ctp.user_id is required")
	}
	if c.CTP.Password == "" {
		return errors.New("ctp.password is required")
	}

	if c.CTP.ConnectWaitSeconds <= 0 {
		c.CTP.ConnectWaitSeconds = 5
	}
	if c.CTP.AuthenticateWaitSeconds <= 0 {
		c.CTP.AuthenticateWaitSeconds = 5
	}
	if c.CTP.LoginWaitSeconds <= 0 {
		c.CTP.LoginWaitSeconds = 5
	}
	if c.CTP.MdConnectWaitSeconds <= 0 {
		c.CTP.MdConnectWaitSeconds = 5
	}
	if c.CTP.MdLoginWaitSeconds <= 0 {
		c.CTP.MdLoginWaitSeconds = 5
	}
	if c.CTP.MdReceiveSeconds <= 0 {
		c.CTP.MdReceiveSeconds = 30
	}

	if c.Web.ListenAddr == "" {
		c.Web.ListenAddr = "127.0.0.1:8080"
	}
	if c.Web.MarketOpenStaleSeconds <= 0 {
		c.Web.MarketOpenStaleSeconds = 60
	}
	if c.Web.AutoOpenBrowser == nil {
		defaultAutoOpen := true
		c.Web.AutoOpenBrowser = &defaultAutoOpen
	}
	if c.Web.DrawDebugDefault != 1 {
		c.Web.DrawDebugDefault = 0
	}
	if c.Web.BrowserLogDefault != 1 {
		c.Web.BrowserLogDefault = 0
	}
	if c.CTP.MdReconnectEnabled == nil {
		v := true
		c.CTP.MdReconnectEnabled = &v
	}
	if c.CTP.MdReconnectInitialMS == 0 {
		c.CTP.MdReconnectInitialMS = 1000
	}
	if c.CTP.MdReconnectMaxMS == 0 {
		c.CTP.MdReconnectMaxMS = 30000
	}
	if c.CTP.MdReconnectJitterRatio == 0 {
		c.CTP.MdReconnectJitterRatio = 0.2
	}
	if c.CTP.MdReloginWaitSeconds == 0 {
		c.CTP.MdReloginWaitSeconds = 3
	}
	if c.CTP.TickDedupWindowSeconds == 0 {
		c.CTP.TickDedupWindowSeconds = 2
	}
	if c.CTP.DriftThresholdSeconds == 0 {
		c.CTP.DriftThresholdSeconds = 5
	}
	if c.CTP.DriftResumeTicks == 0 {
		c.CTP.DriftResumeTicks = 3
	}
	if c.CTP.NoTickWarnSeconds == 0 {
		c.CTP.NoTickWarnSeconds = c.Web.MarketOpenStaleSeconds
		if c.CTP.NoTickWarnSeconds < 30 {
			c.CTP.NoTickWarnSeconds = 30
		}
	}
	if c.CTP.MdReconnectInitialMS <= 0 {
		return errors.New("ctp.md_reconnect_initial_backoff_ms must be > 0")
	}
	if c.CTP.MdReconnectMaxMS <= 0 {
		return errors.New("ctp.md_reconnect_max_backoff_ms must be > 0")
	}
	if c.CTP.MdReconnectMaxMS < c.CTP.MdReconnectInitialMS {
		return errors.New("ctp.md_reconnect_max_backoff_ms must be >= ctp.md_reconnect_initial_backoff_ms")
	}
	if c.CTP.MdReconnectJitterRatio < 0 || c.CTP.MdReconnectJitterRatio > 1 {
		return errors.New("ctp.md_reconnect_jitter_ratio must be in [0,1]")
	}
	if c.CTP.MdReloginWaitSeconds <= 0 {
		return errors.New("ctp.md_relogin_wait_seconds must be > 0")
	}
	if c.CTP.TickDedupWindowSeconds <= 0 {
		return errors.New("ctp.tick_dedup_window_seconds must be > 0")
	}
	if c.CTP.DriftThresholdSeconds <= 0 {
		return errors.New("ctp.drift_threshold_seconds must be > 0")
	}
	if c.CTP.DriftResumeTicks < 1 {
		return errors.New("ctp.drift_resume_consecutive_ticks must be >= 1")
	}
	if c.CTP.NoTickWarnSeconds < 30 {
		return errors.New("ctp.no_tick_warn_seconds must be >= 30")
	}
	if c.CTP.BusEnabled == nil {
		v := true
		c.CTP.BusEnabled = &v
	}
	if c.CTP.BusFlushMS == 0 {
		c.CTP.BusFlushMS = 200
	}
	if c.CTP.BusFlushMS < 0 {
		return errors.New("ctp.bus_flush_ms must be >= 0")
	}
	if c.CTP.ReplayDefaultMode == "" {
		c.CTP.ReplayDefaultMode = "fast"
	}
	switch c.CTP.ReplayDefaultMode {
	case "fast", "realtime":
	default:
		return errors.New("ctp.replay_default_mode must be one of: fast,realtime")
	}
	if c.CTP.ReplayDefaultSpeed == 0 {
		c.CTP.ReplayDefaultSpeed = 1.0
	}
	if c.CTP.ReplayDefaultSpeed <= 0 {
		return errors.New("ctp.replay_default_speed must be > 0")
	}
	if c.CTP.ReplayAllowOrderCommand == nil {
		v := true
		c.CTP.ReplayAllowOrderCommand = &v
	}
	if c.Calendar.AutoUpdateOnStart == nil {
		v := true
		c.Calendar.AutoUpdateOnStart = &v
	}
	if c.Calendar.MinFutureOpenDays <= 0 {
		c.Calendar.MinFutureOpenDays = 60
	}
	if c.Calendar.CheckIntervalHours <= 0 {
		c.Calendar.CheckIntervalHours = 24
	}
	if c.Calendar.BrowserFallback == nil {
		v := true
		c.Calendar.BrowserFallback = &v
	}
	if c.Calendar.BrowserHeadless == nil {
		v := true
		c.Calendar.BrowserHeadless = &v
	}
	if c.DB.Driver == "" {
		c.DB.Driver = "mysql"
	}
	if c.DB.Driver != "mysql" {
		return errors.New("db.driver must be mysql")
	}
	if c.DB.Host == "" {
		c.DB.Host = "localhost"
	}
	if c.DB.Port <= 0 {
		c.DB.Port = 3306
	}
	if c.DB.User == "" {
		c.DB.User = "root"
	}
	if c.DB.Database == "" {
		c.DB.Database = "future_kline"
	}
	if c.DB.Params == "" {
		c.DB.Params = "parseTime=true&loc=Local&multiStatements=false"
	}

	return nil
}

func (c CTPConfig) IsL9AsyncEnabled() bool {
	if c.EnableL9Async == nil {
		return true
	}
	return *c.EnableL9Async
}

func (c CTPConfig) IsMdReconnectEnabled() bool {
	if c.MdReconnectEnabled == nil {
		return true
	}
	return *c.MdReconnectEnabled
}

func (c CTPConfig) IsBusEnabled() bool {
	if c.BusEnabled == nil {
		return true
	}
	return *c.BusEnabled
}

func (c CTPConfig) IsReplayAllowOrderCommandDispatch() bool {
	if c.ReplayAllowOrderCommand == nil {
		return true
	}
	return *c.ReplayAllowOrderCommand
}

func (w WebConfig) IsAutoOpenBrowser() bool {
	if w.AutoOpenBrowser == nil {
		return true
	}
	return *w.AutoOpenBrowser
}

func (w WebConfig) IsDrawDebugDefaultEnabled() bool {
	return w.DrawDebugDefault == 1
}

func (w WebConfig) IsBrowserLogDefaultEnabled() bool {
	return w.BrowserLogDefault == 1
}

func (c CalendarConfig) IsAutoUpdateOnStart() bool {
	if c.AutoUpdateOnStart == nil {
		return true
	}
	return *c.AutoUpdateOnStart
}

func (c CalendarConfig) IsBrowserFallbackEnabled() bool {
	if c.BrowserFallback == nil {
		return true
	}
	return *c.BrowserFallback
}

func (c CalendarConfig) IsBrowserHeadless() bool {
	if c.BrowserHeadless == nil {
		return true
	}
	return *c.BrowserHeadless
}
