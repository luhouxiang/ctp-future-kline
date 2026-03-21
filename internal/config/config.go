// config.go 定义应用总配置结构，并负责把 config.json 解析成可运行的 AppConfig。
// 这里同时承担默认值补齐和基础校验，是所有运行模块共享的配置入口。
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type AppConfig struct {
	// CTP 保存行情和交易两条 CTP 链路共享的接入参数。
	CTP CTPConfig `json:"ctp"`
	// DB 描述底层 MySQL 连接参数，供行情、回放、策略、交易等模块共用。
	DB DBConfig `json:"db"`
	// Web 控制 HTTP 服务监听地址和前端默认行为。
	Web WebConfig `json:"web"`
	// Calendar 控制交易日历自动更新和数据源抓取策略。
	Calendar CalendarConfig `json:"calendar"`
	// Log 控制全局日志级别。
	Log LogConfig `json:"log"`
	// Strategy 控制 Python 策略子系统是否启用及其连接参数。
	Strategy StrategyConfig `json:"strategy"`
	// Trade 控制实盘交易子系统是否启用及其风控/轮询参数。
	Trade TradeConfig `json:"trade"`
}

type DBConfig struct {
	// Driver 指定数据库驱动类型，当前主要用于 MySQL。
	Driver string `json:"driver"`
	// Host 是数据库主机地址。
	Host string `json:"host"`
	// Port 是数据库端口。
	Port int `json:"port"`
	// User 是数据库用户名。
	User string `json:"user"`
	// Password 是数据库密码。
	Password string `json:"password"`
	// Database 是业务库名称。
	Database string `json:"database"`
	// Params 保存附加 DSN 参数，例如字符集、时区等。
	Params string `json:"params"`
}

type CTPConfig struct {
	// FlowPath 是 CTP 流文件、tick CSV、bus 日志等本地落盘目录的根路径。
	FlowPath string `json:"flow_path"`
	// TraderFrontAddr 是 Trader API 前置地址，用于认证、查合约及交易侧操作。
	TraderFrontAddr string `json:"trader_front_addr"`
	// MdFrontAddr 是 Market Data API 前置地址，用于订阅和接收实时行情。
	MdFrontAddr string `json:"md_front_addr"`
	// BrokerID 是 CTP 券商编号。
	BrokerID string `json:"broker_id"`
	// AppID 是穿透认证使用的应用标识。
	AppID string `json:"app_id"`
	// AuthCode 是穿透认证码。
	AuthCode string `json:"auth_code"`
	// UserProductInfo 是客户端产品信息，会带到部分登录或认证请求中。
	UserProductInfo string `json:"user_product_info"`
	// UserID 是 CTP 账号。
	UserID string `json:"user_id"`
	// Password 是 CTP 密码。
	Password string `json:"password"`
	// SubscribeInstruments 指定需要订阅的合约或品种前缀过滤条件。
	SubscribeInstruments []string `json:"subscribe_instruments"`
	// EnableL9Async 控制是否异步计算主连/L9 分钟线。
	EnableL9Async *bool `json:"enable_l9_async"`
	// EnableMultiMinute 控制是否继续聚合 mm 周期分钟线。
	EnableMultiMinute *bool `json:"enable_multi_minute"`
	// ConnectWaitSeconds 是 Trader 前置连接后的等待时长。
	ConnectWaitSeconds int `json:"connect_wait_seconds"`
	// AuthenticateWaitSeconds 是发送认证请求后的等待时长。
	AuthenticateWaitSeconds int `json:"authenticate_wait_seconds"`
	// LoginWaitSeconds 是 Trader 登录后的等待时长。
	LoginWaitSeconds int `json:"login_wait_seconds"`
	// MdConnectWaitSeconds 是 MD 前置连接后的等待时长。
	MdConnectWaitSeconds int `json:"md_connect_wait_seconds"`
	// MdLoginWaitSeconds 是 MD 登录后的等待时长。
	MdLoginWaitSeconds int `json:"md_login_wait_seconds"`
	// MdReceiveSeconds 用于一次性运行模式下持续接收行情的时长。
	MdReceiveSeconds int `json:"md_receive_seconds"`
	// MdReconnectEnabled 控制行情断线后是否自动重连。
	MdReconnectEnabled *bool `json:"md_reconnect_enabled"`
	// MdReconnectInitialMS 是重连退避的初始等待时间。
	MdReconnectInitialMS int `json:"md_reconnect_initial_backoff_ms"`
	// MdReconnectMaxMS 是重连退避的最大等待时间。
	MdReconnectMaxMS int `json:"md_reconnect_max_backoff_ms"`
	// MdReconnectJitterRatio 为重连等待加入抖动比例，避免固定节奏重试。
	MdReconnectJitterRatio float64 `json:"md_reconnect_jitter_ratio"`
	// MdReloginWaitSeconds 是重连登录成功后再次订阅前的等待时间。
	MdReloginWaitSeconds int `json:"md_relogin_wait_seconds"`
	// TickDedupWindowSeconds 是重复 tick 判定窗口。
	TickDedupWindowSeconds int `json:"tick_dedup_window_seconds"`
	// DriftThresholdSeconds 是允许的 tick 时间漂移阈值。
	DriftThresholdSeconds int `json:"drift_threshold_seconds"`
	// DriftResumeTicks 是漂移恢复前要求连续正常 tick 的数量。
	DriftResumeTicks int `json:"drift_resume_consecutive_ticks"`
	// NoTickWarnSeconds 是前置已连通但长时间无 tick 时的告警阈值。
	NoTickWarnSeconds int `json:"no_tick_warn_seconds"`
	// BusEnabled 控制是否启用 bus 文件总线旁路。
	BusEnabled *bool `json:"bus_enabled"`
	// BusLogPath 是 bus 文件总线的落盘目录。
	BusLogPath string `json:"bus_log_path"`
	// BusFlushMS 是 bus 文件总线刷盘间隔。
	BusFlushMS int `json:"bus_flush_ms"`
	// ReplayDefaultMode 是前端回放默认模式。
	ReplayDefaultMode string `json:"replay_default_mode"`
	// ReplayDefaultSpeed 是前端回放默认速度倍率。
	ReplayDefaultSpeed float64 `json:"replay_default_speed"`
	// ReplayAllowOrderCommand 控制回放期间是否允许派发订单指令。
	ReplayAllowOrderCommand *bool `json:"replay_allow_order_command_dispatch"`
	// DBDSN 是运行时补入的数据库连接串，不从 JSON 直接读取。
	DBDSN string `json:"-"`
}

type WebConfig struct {
	// ListenAddr 是 HTTP 和 WebSocket 服务监听地址。
	ListenAddr string `json:"listen_addr"`
	// AutoOpenBrowser 控制启动后是否自动打开浏览器。
	AutoOpenBrowser *bool `json:"auto_open_browser"`
	// MarketOpenStaleSeconds 用于根据最近 tick 时间推断市场是否仍然活跃。
	MarketOpenStaleSeconds int `json:"market_open_stale_seconds"`
	// DrawDebugDefault 控制图表绘制调试开关的默认值。
	DrawDebugDefault int `json:"draw_debug_default"`
	// BrowserLogDefault 控制前端浏览器日志采集开关的默认值。
	BrowserLogDefault int `json:"browser_log_default"`
}

type CalendarConfig struct {
	// AutoUpdateOnStart 控制服务启动时是否先检查并补齐交易日历。
	AutoUpdateOnStart *bool `json:"auto_update_on_start"`
	// MinFutureOpenDays 是未来至少应保证存在的交易日数量。
	MinFutureOpenDays int `json:"min_future_open_days"`
	// SourceURL 是交易日历在线抓取的数据源地址。
	SourceURL string `json:"source_url"`
	// SourceCSVPath 是本地 CSV 导入时使用的日历文件路径。
	SourceCSVPath string `json:"source_csv_path"`
	// CheckIntervalHours 是后台定期检查日历更新的间隔。
	CheckIntervalHours int `json:"check_interval_hours"`
	// BrowserFallback 控制在线抓取失败时是否启用浏览器兜底抓取。
	BrowserFallback *bool `json:"browser_fallback"`
	// BrowserPath 指定浏览器兜底抓取时使用的浏览器可执行文件。
	BrowserPath string `json:"browser_path"`
	// BrowserHeadless 控制浏览器兜底抓取时是否启用无头模式。
	BrowserHeadless *bool `json:"browser_headless"`
}

type LogConfig struct {
	// Level 是全局日志等级，例如 debug、info、warn、error。
	Level string `json:"level"`
}

type StrategyConfig struct {
	// Enabled 控制是否启用策略子系统。
	Enabled *bool `json:"enabled"`
	// GRPCAddr 是 Go 侧连接 Python 策略服务的地址。
	GRPCAddr string `json:"grpc_addr"`
	// AutoStart 控制启动时是否自动拉起 Python 策略进程。
	AutoStart *bool `json:"auto_start"`
	// PythonEntry 是 Python 策略服务入口脚本。
	PythonEntry string `json:"python_entry"`
	// PythonWorkdir 是启动 Python 策略服务时的工作目录。
	PythonWorkdir string `json:"python_workdir"`
	// HealthcheckIntervalMS 是策略服务健康检查周期。
	HealthcheckIntervalMS int `json:"healthcheck_interval_ms"`
	// RequestTimeoutMS 是 gRPC 请求超时时间。
	RequestTimeoutMS int `json:"request_timeout_ms"`
	// BacktestOutputDir 是策略回测结果输出目录。
	BacktestOutputDir string `json:"backtest_output_dir"`
}

type TradeConfig struct {
	// Enabled 控制是否启用实盘交易子系统。
	Enabled *bool `json:"enabled"`
	// AccountID 是系统内部使用的交易账户标识。
	AccountID string `json:"account_id"`
	// AutoConfirmSettlement 控制登录后是否自动做结算确认。
	AutoConfirmSettlement *bool `json:"auto_confirm_settlement"`
	// MaxOrderVolume 是单笔手工或策略下单的最大手数限制。
	MaxOrderVolume int `json:"max_order_volume"`
	// AllowedSymbols 是允许交易的品种白名单。
	AllowedSymbols []string `json:"allowed_symbols"`
	// BlockStrategyLiveOrder 控制是否阻止策略直接发实盘单。
	BlockStrategyLiveOrder *bool `json:"block_strategy_live_order"`
	// QueryPollIntervalMS 是账户、委托、成交轮询间隔。
	QueryPollIntervalMS int `json:"query_poll_interval_ms"`
	// PositionSyncIntervalMS 是持仓单独同步的轮询间隔。
	PositionSyncIntervalMS int `json:"position_sync_interval_ms"`
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
		c.CTP.ReplayDefaultMode = "realtime"
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
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	switch c.Log.Level {
	case "debug", "DEBUG", "info", "INFO", "warn", "WARN", "error", "ERROR":
	default:
		return errors.New("log.level must be one of: debug,info,warn,error")
	}
	if c.Strategy.Enabled == nil {
		v := false
		c.Strategy.Enabled = &v
	}
	if c.Strategy.AutoStart == nil {
		v := true
		c.Strategy.AutoStart = &v
	}
	if c.Strategy.GRPCAddr == "" {
		c.Strategy.GRPCAddr = "127.0.0.1:50051"
	}
	if c.Strategy.HealthcheckIntervalMS <= 0 {
		c.Strategy.HealthcheckIntervalMS = 2000
	}
	if c.Strategy.RequestTimeoutMS <= 0 {
		c.Strategy.RequestTimeoutMS = 3000
	}
	if c.Strategy.BacktestOutputDir == "" {
		c.Strategy.BacktestOutputDir = "flow/strategy_backtests"
	}
	if c.Strategy.HealthcheckIntervalMS <= 0 {
		return errors.New("strategy.healthcheck_interval_ms must be > 0")
	}
	if c.Strategy.RequestTimeoutMS <= 0 {
		return errors.New("strategy.request_timeout_ms must be > 0")
	}
	if c.Trade.Enabled == nil {
		v := false
		c.Trade.Enabled = &v
	}
	if stringsTrim(c.Trade.AccountID) == "" {
		c.Trade.AccountID = "default"
	}
	if c.Trade.AutoConfirmSettlement == nil {
		v := true
		c.Trade.AutoConfirmSettlement = &v
	}
	if c.Trade.MaxOrderVolume <= 0 {
		c.Trade.MaxOrderVolume = 10
	}
	if c.Trade.BlockStrategyLiveOrder == nil {
		v := true
		c.Trade.BlockStrategyLiveOrder = &v
	}
	if c.Trade.QueryPollIntervalMS <= 0 {
		c.Trade.QueryPollIntervalMS = 5000
	}
	if c.Trade.PositionSyncIntervalMS <= 0 {
		c.Trade.PositionSyncIntervalMS = 3000
	}
	if c.Trade.MaxOrderVolume <= 0 {
		return errors.New("trade.max_order_volume must be > 0")
	}
	if c.Trade.QueryPollIntervalMS <= 0 {
		return errors.New("trade.query_poll_interval_ms must be > 0")
	}
	if c.Trade.PositionSyncIntervalMS <= 0 {
		return errors.New("trade.position_sync_interval_ms must be > 0")
	}

	return nil
}

func (c CTPConfig) IsL9AsyncEnabled() bool {
	if c.EnableL9Async == nil {
		return false
	}
	return *c.EnableL9Async
}

func (c CTPConfig) IsMultiMinuteEnabled() bool {
	if c.EnableMultiMinute == nil {
		return false
	}
	return *c.EnableMultiMinute
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

func (c StrategyConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return false
	}
	return *c.Enabled
}

func (c StrategyConfig) IsAutoStart() bool {
	if c.AutoStart == nil {
		return true
	}
	return *c.AutoStart
}

func (c TradeConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return false
	}
	return *c.Enabled
}

func (c TradeConfig) IsAutoConfirmSettlement() bool {
	if c.AutoConfirmSettlement == nil {
		return true
	}
	return *c.AutoConfirmSettlement
}

func (c TradeConfig) IsBlockStrategyLiveOrder() bool {
	if c.BlockStrategyLiveOrder == nil {
		return true
	}
	return *c.BlockStrategyLiveOrder
}

func stringsTrim(v string) string {
	return strings.TrimSpace(v)
}
