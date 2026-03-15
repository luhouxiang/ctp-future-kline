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
	Enabled             bool      `json:"enabled"`
	AccountID           string    `json:"account_id"`
	TraderFront         bool      `json:"trader_front"`
	TraderLogin         bool      `json:"trader_login"`
	SettlementConfirmed bool      `json:"settlement_confirmed"`
	TradingDay          string    `json:"trading_day"`
	FrontID             int       `json:"front_id"`
	SessionID           int       `json:"session_id"`
	LastError           string    `json:"last_error"`
	LastQueryAt         time.Time `json:"last_query_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type TradingAccountSnapshot struct {
	AccountID      string    `json:"account_id"`
	Balance        float64   `json:"balance"`
	Available      float64   `json:"available"`
	Margin         float64   `json:"margin"`
	FrozenCash     float64   `json:"frozen_cash"`
	Commission     float64   `json:"commission"`
	CloseProfit    float64   `json:"close_profit"`
	PositionProfit float64   `json:"position_profit"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type PositionSnapshot struct {
	AccountID     string    `json:"account_id"`
	Symbol        string    `json:"symbol"`
	Exchange      string    `json:"exchange"`
	Direction     string    `json:"direction"`
	HedgeFlag     string    `json:"hedge_flag"`
	YdPosition    int       `json:"yd_position"`
	TodayPosition int       `json:"today_position"`
	Position      int       `json:"position"`
	OpenCost      float64   `json:"open_cost"`
	PositionCost  float64   `json:"position_cost"`
	UseMargin     float64   `json:"use_margin"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type OrderRecord struct {
	AccountID           string    `json:"account_id"`
	CommandID           string    `json:"command_id"`
	OrderRef            string    `json:"order_ref"`
	FrontID             int       `json:"front_id"`
	SessionID           int       `json:"session_id"`
	ExchangeID          string    `json:"exchange_id"`
	OrderSysID          string    `json:"order_sys_id"`
	Symbol              string    `json:"symbol"`
	Direction           string    `json:"direction"`
	OffsetFlag          string    `json:"offset_flag"`
	LimitPrice          float64   `json:"limit_price"`
	VolumeTotalOriginal int       `json:"volume_total_original"`
	VolumeTraded        int       `json:"volume_traded"`
	VolumeCanceled      int       `json:"volume_canceled"`
	OrderStatus         string    `json:"order_status"`
	SubmitStatus        string    `json:"submit_status"`
	StatusMsg           string    `json:"status_msg"`
	InsertedAt          time.Time `json:"inserted_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type TradeRecord struct {
	AccountID  string    `json:"account_id"`
	TradeID    string    `json:"trade_id"`
	OrderRef   string    `json:"order_ref"`
	OrderSysID string    `json:"order_sys_id"`
	ExchangeID string    `json:"exchange_id"`
	Symbol     string    `json:"symbol"`
	Direction  string    `json:"direction"`
	OffsetFlag string    `json:"offset_flag"`
	Price      float64   `json:"price"`
	Volume     int       `json:"volume"`
	TradeTime  time.Time `json:"trade_time"`
	TradingDay string    `json:"trading_day"`
	ReceivedAt time.Time `json:"received_at"`
}

type SubmitOrderRequest struct {
	AccountID  string  `json:"account_id"`
	Symbol     string  `json:"symbol"`
	ExchangeID string  `json:"exchange_id"`
	Direction  string  `json:"direction"`
	OffsetFlag string  `json:"offset_flag"`
	LimitPrice float64 `json:"limit_price"`
	Volume     int     `json:"volume"`
	ClientTag  string  `json:"client_tag"`
	Reason     string  `json:"reason"`
}

type CancelOrderRequest struct {
	AccountID  string `json:"account_id"`
	CommandID  string `json:"command_id"`
	OrderRef   string `json:"order_ref"`
	ExchangeID string `json:"exchange_id"`
	OrderSysID string `json:"order_sys_id"`
	FrontID    int    `json:"front_id"`
	SessionID  int    `json:"session_id"`
	Reason     string `json:"reason"`
}

type OrderCommandAudit struct {
	ID          int64          `json:"id"`
	AccountID   string         `json:"account_id"`
	CommandID   string         `json:"command_id"`
	CommandType string         `json:"command_type"`
	Symbol      string         `json:"symbol"`
	RiskStatus  string         `json:"risk_status"`
	RiskReason  string         `json:"risk_reason"`
	Request     map[string]any `json:"request"`
	Response    map[string]any `json:"response"`
	CreatedAt   time.Time      `json:"created_at"`
}

type QueryAudit struct {
	ID        int64     `json:"id"`
	AccountID string    `json:"account_id"`
	QueryType string    `json:"query_type"`
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

type SessionState struct {
	AccountID           string    `json:"account_id"`
	FrontID             int       `json:"front_id"`
	SessionID           int       `json:"session_id"`
	NextOrderRef        int64     `json:"next_order_ref"`
	Connected           bool      `json:"connected"`
	Authenticated       bool      `json:"authenticated"`
	LoggedIn            bool      `json:"logged_in"`
	SettlementConfirmed bool      `json:"settlement_confirmed"`
	TradingDay          string    `json:"trading_day"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type EventEnvelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type GatewayEvent struct {
	Type  string
	Order *OrderRecord
	Trade *TradeRecord
	Err   error
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
