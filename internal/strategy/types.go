package strategy

import "time"

const (
	InstanceStatusStopped = "stopped"
	InstanceStatusRunning = "running"
	InstanceStatusError   = "error"

	RunTypeRealtime = "realtime"
	RunTypeReplay   = "replay"
	RunTypeBacktest = "backtest"

	OrderStatusSimulated = "simulated_submitted"
	OrderStatusBlocked   = "blocked"
	OrderStatusNoop      = "noop"
	RiskStatusAllowed    = "allowed"
	RiskStatusBlocked    = "blocked"
)

type ManagerStatus struct {
	Enabled          bool      `json:"enabled"`
	ProcessRunning   bool      `json:"process_running"`
	Connected        bool      `json:"connected"`
	GRPCAddr         string    `json:"grpc_addr"`
	PythonEntry      string    `json:"python_entry"`
	LastError        string    `json:"last_error"`
	LastHealthAt     time.Time `json:"last_health_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	Definitions      int       `json:"definitions"`
	Instances        int       `json:"instances"`
	RunningCount     int       `json:"running_count"`
	SignalCount      int64     `json:"signal_count"`
	AuditCount       int64     `json:"audit_count"`
	BacktestRunCount int64     `json:"backtest_run_count"`
}

type StrategyDefinition struct {
	StrategyID    string         `json:"strategy_id"`
	DisplayName   string         `json:"display_name"`
	EntryScript   string         `json:"entry_script"`
	Version       string         `json:"version"`
	DefaultParams map[string]any `json:"default_params"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type StrategyInstance struct {
	InstanceID         string         `json:"instance_id"`
	StrategyID         string         `json:"strategy_id"`
	DisplayName        string         `json:"display_name"`
	Mode               string         `json:"mode"`
	Status             string         `json:"status"`
	AccountID          string         `json:"account_id"`
	Symbols            []string       `json:"symbols"`
	Timeframe          string         `json:"timeframe"`
	Params             map[string]any `json:"params"`
	LastSignalAt       *time.Time     `json:"last_signal_at,omitempty"`
	LastTargetPosition float64        `json:"last_target_position"`
	LastError          string         `json:"last_error,omitempty"`
	UpdatedAt          time.Time      `json:"updated_at"`
	CreatedAt          time.Time      `json:"created_at"`
}

type SignalRecord struct {
	ID             int64          `json:"id"`
	InstanceID     string         `json:"instance_id"`
	StrategyID     string         `json:"strategy_id"`
	Symbol         string         `json:"symbol"`
	Timeframe      string         `json:"timeframe"`
	Mode           string         `json:"mode"`
	EventTime      time.Time      `json:"event_time"`
	TargetPosition float64        `json:"target_position"`
	Confidence     float64        `json:"confidence"`
	Reason         string         `json:"reason"`
	Metrics        map[string]any `json:"metrics"`
	CreatedAt      time.Time      `json:"created_at"`
}

type StrategyRun struct {
	RunID      string         `json:"run_id"`
	InstanceID string         `json:"instance_id"`
	StrategyID string         `json:"strategy_id"`
	RunType    string         `json:"run_type"`
	Status     string         `json:"status"`
	Symbol     string         `json:"symbol"`
	Timeframe  string         `json:"timeframe"`
	OutputPath string         `json:"output_path"`
	Summary    map[string]any `json:"summary"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
	LastError  string         `json:"last_error,omitempty"`
}

type OrderAuditRecord struct {
	ID              int64          `json:"id"`
	InstanceID      string         `json:"instance_id"`
	StrategyID      string         `json:"strategy_id"`
	Symbol          string         `json:"symbol"`
	Mode            string         `json:"mode"`
	EventTime       time.Time      `json:"event_time"`
	TargetPosition  float64        `json:"target_position"`
	CurrentPosition float64        `json:"current_position"`
	PlannedDelta    float64        `json:"planned_delta"`
	RiskStatus      string         `json:"risk_status"`
	RiskReason      string         `json:"risk_reason"`
	OrderStatus     string         `json:"order_status"`
	Audit           map[string]any `json:"audit"`
	CreatedAt       time.Time      `json:"created_at"`
}

type ExecutionPlan struct {
	CurrentPosition float64 `json:"current_position"`
	TargetPosition  float64 `json:"target_position"`
	PlannedDelta    float64 `json:"planned_delta"`
	RiskStatus      string  `json:"risk_status"`
	RiskReason      string  `json:"risk_reason"`
	OrderStatus     string  `json:"order_status"`
}

type OrdersStatus struct {
	Mode        string             `json:"mode"`
	Positions   map[string]float64 `json:"positions"`
	LastAuditAt *time.Time         `json:"last_audit_at,omitempty"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

type TickEvent struct {
	InstrumentID    string    `json:"instrument_id"`
	ExchangeID      string    `json:"exchange_id"`
	ActionDay       string    `json:"action_day"`
	TradingDay      string    `json:"trading_day"`
	UpdateTime      string    `json:"update_time"`
	UpdateMillisec  int       `json:"update_millisec"`
	ReceivedAt      time.Time `json:"received_at"`
	LastPrice       float64   `json:"last_price"`
	Volume          int       `json:"volume"`
	OpenInterest    float64   `json:"open_interest"`
	SettlementPrice float64   `json:"settlement_price"`
	BidPrice1       float64   `json:"bid_price1"`
	AskPrice1       float64   `json:"ask_price1"`
}

type BarEvent struct {
	Variety         string    `json:"variety"`
	InstrumentID    string    `json:"instrument_id"`
	Exchange        string    `json:"exchange"`
	DataTime        time.Time `json:"data_time"`
	AdjustedTime    time.Time `json:"adjusted_time"`
	Period          string    `json:"period"`
	Open            float64   `json:"open"`
	High            float64   `json:"high"`
	Low             float64   `json:"low"`
	Close           float64   `json:"close"`
	Volume          int64     `json:"volume"`
	OpenInterest    float64   `json:"open_interest"`
	SettlementPrice float64   `json:"settlement_price"`
}

type EventEnvelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}
