package trade

import "time"

const (
	CommandTypeSubmit = "submit_order"
	CommandTypeCancel = "cancel_order"

	RiskStatusAllowed = "allowed"
	RiskStatusBlocked = "blocked"

	QueryStatusOK    = "ok"
	QueryStatusError = "error"
)

type TradeStatus struct {
	// Enabled 表示实盘交易子系统是否启用。
	Enabled bool `json:"enabled"`
	// AccountID 是系统内部使用的交易账户标识。
	AccountID string `json:"account_id"`
	// TraderFront 表示交易前置是否已连通。
	TraderFront bool `json:"trader_front"`
	// TraderLogin 表示交易会话是否已登录。
	TraderLogin bool `json:"trader_login"`
	// SettlementConfirmed 表示结算确认是否已完成。
	SettlementConfirmed bool `json:"settlement_confirmed"`
	// TradingDay 是当前交易日。
	TradingDay string `json:"trading_day"`
	// FrontID 是登录会话返回的 FrontID。
	FrontID int `json:"front_id"`
	// SessionID 是登录会话返回的 SessionID。
	SessionID int `json:"session_id"`
	// LastError 是最近一次交易侧错误。
	LastError string `json:"last_error"`
	// LastQueryAt 是最近一次查询账户、持仓、委托或成交成功时间。
	LastQueryAt time.Time `json:"last_query_at"`
	// MetaSyncTradingDay 是实时模拟元数据同步使用的当前交易日。
	MetaSyncTradingDay string `json:"meta_sync_trading_day,omitempty"`
	// FeeGap 是当前交易日手续费缺口数量。
	FeeGap int `json:"fee_gap,omitempty"`
	// MarginGap 是当前交易日保证金缺口数量。
	MarginGap int `json:"margin_gap,omitempty"`
	// FeeLaneRunning 表示手续费补齐 lane 是否在运行。
	FeeLaneRunning bool `json:"fee_lane_running,omitempty"`
	// MarginLaneRunning 表示保证金补齐 lane 是否在运行。
	MarginLaneRunning bool `json:"margin_lane_running,omitempty"`
	// FeeThrottleMS 是手续费补齐 lane 当前节流毫秒。
	FeeThrottleMS int64 `json:"fee_throttle_ms,omitempty"`
	// MarginThrottleMS 是保证金补齐 lane 当前节流毫秒。
	MarginThrottleMS int64 `json:"margin_throttle_ms,omitempty"`
	// UpdatedAt 是状态对象最后更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type TradingAccountSnapshot struct {
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// StaticBalance 是静态权益（上日权益口径）。
	StaticBalance float64 `json:"static_balance"`
	// Balance 是动态权益。
	Balance float64 `json:"balance"`
	// Available 是可用资金。
	Available float64 `json:"available"`
	// Margin 是占用保证金。
	Margin float64 `json:"margin"`
	// FrozenMargin 是冻结保证金。
	FrozenMargin float64 `json:"frozen_margin"`
	// FrozenCommission 是冻结手续费。
	FrozenCommission float64 `json:"frozen_commission"`
	// FrozenPremium 是冻结权利金。
	FrozenPremium float64 `json:"frozen_premium"`
	// FrozenCash 是冻结资金。
	FrozenCash float64 `json:"frozen_cash"`
	// Deposit 是入金。
	Deposit float64 `json:"deposit"`
	// Withdraw 是出金。
	Withdraw float64 `json:"withdraw"`
	// Premium 是权利金。
	Premium float64 `json:"premium"`
	// OtherFee 是其他费用。
	OtherFee float64 `json:"other_fee"`
	// Commission 是累计手续费。
	Commission float64 `json:"commission"`
	// CloseProfit 是平仓盈亏。
	CloseProfit float64 `json:"close_profit"`
	// PositionProfit 是持仓盈亏。
	PositionProfit float64 `json:"position_profit"`
	// UpdatedAt 是快照更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type PositionSnapshot struct {
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// Symbol 是合约代码。
	Symbol string `json:"symbol"`
	// Exchange 是交易所代码。
	Exchange string `json:"exchange"`
	// Direction 是持仓方向，如 long 或 short。
	Direction string `json:"direction"`
	// HedgeFlag 是投保标志。
	HedgeFlag string `json:"hedge_flag"`
	// YdPosition 是昨仓数量。
	YdPosition int `json:"yd_position"`
	// TodayPosition 是今仓数量。
	TodayPosition int `json:"today_position"`
	// Position 是总持仓。
	Position int `json:"position"`
	// OpenCost 是开仓成本。
	OpenCost float64 `json:"open_cost"`
	// PositionCost 是持仓成本。
	PositionCost float64 `json:"position_cost"`
	// UseMargin 是该持仓占用保证金。
	UseMargin float64 `json:"use_margin"`
	// UpdatedAt 是快照更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type OrderRecord struct {
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// CommandID 是系统内部订单指令 ID。
	CommandID string `json:"command_id"`
	// OrderRef 是 CTP 本地报单引用。
	OrderRef string `json:"order_ref"`
	// FrontID 是报单所属前置编号。
	FrontID int `json:"front_id"`
	// SessionID 是报单所属会话编号。
	SessionID int `json:"session_id"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// OrderSysID 是交易所返回的系统单号。
	OrderSysID string `json:"order_sys_id"`
	// Symbol 是合约代码。
	Symbol string `json:"symbol"`
	// Direction 是买卖方向。
	Direction string `json:"direction"`
	// OffsetFlag 是开平标志。
	OffsetFlag string `json:"offset_flag"`
	// LimitPrice 是限价价格。
	LimitPrice float64 `json:"limit_price"`
	// VolumeTotalOriginal 是原始委托手数。
	VolumeTotalOriginal int `json:"volume_total_original"`
	// VolumeTraded 是已成交手数。
	VolumeTraded int `json:"volume_traded"`
	// VolumeCanceled 是已撤手数。
	VolumeCanceled int `json:"volume_canceled"`
	// OrderStatus 是委托状态的业务映射值。
	OrderStatus string `json:"order_status"`
	// SubmitStatus 是报单提交状态。
	SubmitStatus string `json:"submit_status"`
	// StatusMsg 是交易所或柜台返回的状态描述。
	StatusMsg string `json:"status_msg"`
	// InsertedAt 是系统首次记录该委托的时间。
	InsertedAt time.Time `json:"inserted_at"`
	// UpdatedAt 是最近一次状态更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type TradeRecord struct {
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// TradeID 是成交编号。
	TradeID string `json:"trade_id"`
	// OrderRef 是关联的本地报单引用。
	OrderRef string `json:"order_ref"`
	// OrderSysID 是关联的交易所系统单号。
	OrderSysID string `json:"order_sys_id"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// Symbol 是合约代码。
	Symbol string `json:"symbol"`
	// Direction 是买卖方向。
	Direction string `json:"direction"`
	// OffsetFlag 是开平标志。
	OffsetFlag string `json:"offset_flag"`
	// Price 是成交价。
	Price float64 `json:"price"`
	// Volume 是成交手数。
	Volume int `json:"volume"`
	// TradeTime 是成交发生时间。
	TradeTime time.Time `json:"trade_time"`
	// TradingDay 是成交所属交易日。
	TradingDay string `json:"trading_day"`
	// ReceivedAt 是系统接收到该成交回报的时间。
	ReceivedAt time.Time `json:"received_at"`
}

type SubmitOrderRequest struct {
	// AccountID 是目标账户标识。
	AccountID string `json:"account_id"`
	// Symbol 是下单合约。
	Symbol string `json:"symbol"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// Direction 是买卖方向。
	Direction string `json:"direction"`
	// OffsetFlag 是开平标志。
	OffsetFlag string `json:"offset_flag"`
	// LimitPrice 是限价价格。
	LimitPrice float64 `json:"limit_price"`
	// Volume 是下单手数。
	Volume int `json:"volume"`
	// ClientTag 是前端或调用方附带的标识。
	ClientTag string `json:"client_tag"`
	// Reason 记录这次下单的来源或原因。
	Reason string `json:"reason"`
}

type PaperMarketTick struct {
	// Symbol 是合约代码。
	Symbol string `json:"symbol"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// TradingDay 是交易日（YYYYMMDD）。
	TradingDay string `json:"trading_day"`
	// ActionDay 是自然日（YYYYMMDD）。
	ActionDay string `json:"action_day"`
	// UpdateTime 是行情时间（HH:MM:SS）。
	UpdateTime string `json:"update_time"`
	// UpdateMillisec 是行情毫秒部分。
	UpdateMillisec int `json:"update_millisec"`
	// LastPrice 是最新价。
	LastPrice float64 `json:"last_price"`
	// BidPrice1 是买一价。
	BidPrice1 float64 `json:"bid_price1"`
	// AskPrice1 是卖一价。
	AskPrice1 float64 `json:"ask_price1"`
}

type CancelOrderRequest struct {
	// AccountID 是目标账户标识。
	AccountID string `json:"account_id"`
	// CommandID 是系统内部指令 ID。
	CommandID string `json:"command_id"`
	// OrderRef 是本地报单引用。
	OrderRef string `json:"order_ref"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// OrderSysID 是交易所系统单号。
	OrderSysID string `json:"order_sys_id"`
	// FrontID 是下单会话 FrontID。
	FrontID int `json:"front_id"`
	// SessionID 是下单会话 SessionID。
	SessionID int `json:"session_id"`
	// Reason 记录撤单来源或原因。
	Reason string `json:"reason"`
}

type AccountAdjustRequest struct {
	AccountID             string  `json:"account_id"`
	DepositDelta          float64 `json:"deposit_delta"`
	WithdrawDelta         float64 `json:"withdraw_delta"`
	PremiumDelta          float64 `json:"premium_delta"`
	OtherFeeDelta         float64 `json:"other_fee_delta"`
	FrozenCommissionDelta float64 `json:"frozen_commission_delta"`
	FrozenPremiumDelta    float64 `json:"frozen_premium_delta"`
	FrozenMarginDelta     float64 `json:"frozen_margin_delta"`
}

type OrderCommandAudit struct {
	// ID 是数据库自增主键。
	ID int64 `json:"id"`
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// CommandID 是系统内部订单命令 ID。
	CommandID string `json:"command_id"`
	// CommandType 表示 submit_order 或 cancel_order。
	CommandType string `json:"command_type"`
	// Symbol 是相关合约代码。
	Symbol string `json:"symbol"`
	// RiskStatus 表示风控是否放行。
	RiskStatus string `json:"risk_status"`
	// RiskReason 是风控拒绝或提示原因。
	RiskReason string `json:"risk_reason"`
	// Request 保存原始请求载荷。
	Request map[string]any `json:"request"`
	// Response 保存执行结果或回执快照。
	Response map[string]any `json:"response"`
	// CreatedAt 是审计记录创建时间。
	CreatedAt time.Time `json:"created_at"`
}

type QueryAudit struct {
	// ID 是数据库自增主键。
	ID int64 `json:"id"`
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// QueryType 表示 account、positions、orders、trades 等查询种类。
	QueryType string `json:"query_type"`
	// Status 表示本次查询成功还是失败。
	Status string `json:"status"`
	// Detail 保存错误信息或 OK 说明。
	Detail string `json:"detail"`
	// CreatedAt 是查询审计时间。
	CreatedAt time.Time `json:"created_at"`
}

type SessionState struct {
	// AccountID 是账户标识。
	AccountID string `json:"account_id"`
	// FrontID 是最近登录会话的 FrontID。
	FrontID int `json:"front_id"`
	// SessionID 是最近登录会话的 SessionID。
	SessionID int `json:"session_id"`
	// NextOrderRef 是下次分配给报单的本地引用序号。
	NextOrderRef int64 `json:"next_order_ref"`
	// Connected 表示前置是否已连接。
	Connected bool `json:"connected"`
	// Authenticated 表示认证是否成功。
	Authenticated bool `json:"authenticated"`
	// LoggedIn 表示登录是否成功。
	LoggedIn bool `json:"logged_in"`
	// SettlementConfirmed 表示结算确认是否已完成。
	SettlementConfirmed bool `json:"settlement_confirmed"`
	// TradingDay 是该会话对应交易日。
	TradingDay string `json:"trading_day"`
	// UpdatedAt 是会话状态更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type EventEnvelope struct {
	// Type 是广播事件类型，例如 trade_account_update。
	Type string `json:"type"`
	// Data 是事件负载，具体结构随 Type 变化。
	Data any `json:"data"`
}

type TerminalSummary struct {
	AccountID        string    `json:"account_id"`
	Mode             string    `json:"mode"`
	TradingDay       string    `json:"trading_day"`
	ReplayTime       string    `json:"replay_time,omitempty"`
	Symbol           string    `json:"symbol,omitempty"`
	Balance          float64   `json:"balance"`
	Available        float64   `json:"available"`
	Margin           float64   `json:"margin"`
	FrozenMargin     float64   `json:"frozen_margin"`
	FrozenCommission float64   `json:"frozen_commission"`
	FrozenPremium    float64   `json:"frozen_premium"`
	FrozenCash       float64   `json:"frozen_cash"`
	StaticBalance    float64   `json:"static_balance"`
	Deposit          float64   `json:"deposit"`
	Withdraw         float64   `json:"withdraw"`
	Premium          float64   `json:"premium"`
	OtherFee         float64   `json:"other_fee"`
	PositionProfit   float64   `json:"position_profit"`
	CloseProfit      float64   `json:"close_profit"`
	RiskRatio        float64   `json:"risk_ratio"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type TerminalOrderEntryDefaults struct {
	AccountID  string  `json:"account_id"`
	Symbol     string  `json:"symbol,omitempty"`
	ExchangeID string  `json:"exchange_id,omitempty"`
	Volume     int     `json:"volume"`
	LimitPrice float64 `json:"limit_price,omitempty"`
}

type TerminalWorkingOrder struct {
	OrderRecord
	RemainingVolume int `json:"remaining_volume"`
}

type TerminalPosition struct {
	PositionSnapshot
	Closable     int     `json:"closable"`
	FrozenVolume int     `json:"frozen_volume"`
	AvgPrice     float64 `json:"avg_price"`
	MarketPrice  float64 `json:"market_price"`
	FloatPnL     float64 `json:"float_pnl"`
	MarketValue  float64 `json:"market_value"`
}

type TerminalFunds struct {
	AccountID        string    `json:"account_id"`
	StaticBalance    float64   `json:"static_balance"`
	DynamicBalance   float64   `json:"dynamic_balance"`
	Available        float64   `json:"available"`
	FrozenMargin     float64   `json:"frozen_margin"`
	FrozenCommission float64   `json:"frozen_commission"`
	FrozenPremium    float64   `json:"frozen_premium"`
	FrozenCash       float64   `json:"frozen_cash"`
	Deposit          float64   `json:"deposit"`
	Withdraw         float64   `json:"withdraw"`
	Premium          float64   `json:"premium"`
	OtherFee         float64   `json:"other_fee"`
	Margin           float64   `json:"margin"`
	Commission       float64   `json:"commission"`
	CloseProfit      float64   `json:"close_profit"`
	PositionProfit   float64   `json:"position_profit"`
	RiskRatio        float64   `json:"risk_ratio"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type TerminalSnapshot struct {
	Summary            TerminalSummary            `json:"summary"`
	OrderEntryDefaults TerminalOrderEntryDefaults `json:"order_entry_defaults"`
	WorkingOrders      []TerminalWorkingOrder     `json:"working_orders"`
	Positions          []TerminalPosition         `json:"positions"`
	Orders             []OrderRecord              `json:"orders"`
	Trades             []TradeRecord              `json:"trades"`
	Funds              TerminalFunds              `json:"funds"`
}

type CommissionRateSnapshot struct {
	InstrumentID            string    `json:"instrument_id"`
	ExchangeID              string    `json:"exchange_id"`
	OpenRatioByMoney        float64   `json:"open_ratio_by_money"`
	OpenRatioByVolume       float64   `json:"open_ratio_by_volume"`
	CloseRatioByMoney       float64   `json:"close_ratio_by_money"`
	CloseRatioByVolume      float64   `json:"close_ratio_by_volume"`
	CloseTodayRatioByMoney  float64   `json:"close_today_ratio_by_money"`
	CloseTodayRatioByVolume float64   `json:"close_today_ratio_by_volume"`
	UpdatedAt               time.Time `json:"updated_at"`
}

type MarginRateSnapshot struct {
	InstrumentID             string    `json:"instrument_id"`
	ExchangeID               string    `json:"exchange_id"`
	HedgeFlag                string    `json:"hedge_flag"`
	LongMarginRatioByMoney   float64   `json:"long_margin_ratio_by_money"`
	LongMarginRatioByVolume  float64   `json:"long_margin_ratio_by_volume"`
	ShortMarginRatioByMoney  float64   `json:"short_margin_ratio_by_money"`
	ShortMarginRatioByVolume float64   `json:"short_margin_ratio_by_volume"`
	IsRelative               bool      `json:"is_relative"`
	UpdatedAt                time.Time `json:"updated_at"`
}

type GatewayEvent struct {
	// Type 是底层网关事件类型。
	Type string
	// Order 用于携带委托状态更新事件。
	Order *OrderRecord
	// Trade 用于携带成交事件。
	Trade *TradeRecord
	// Err 用于携带底层网关错误。
	Err error
}

type Gateway interface {
	Start() error
	Close() error
	Status() TradeStatus
	RefreshAccount() (TradingAccountSnapshot, error)
	RefreshPositions() ([]PositionSnapshot, error)
	RefreshOrders() ([]OrderRecord, error)
	RefreshTrades() ([]TradeRecord, error)
	SubmitOrder(commandID string, req SubmitOrderRequest) (OrderRecord, error)
	CancelOrder(req CancelOrderRequest) (OrderRecord, error)
	Subscribe() (<-chan GatewayEvent, func())
}
