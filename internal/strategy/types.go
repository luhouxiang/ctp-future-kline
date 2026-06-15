package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	InstanceStatusStopped = "stopped"
	InstanceStatusRunning = "running"
	InstanceStatusError   = "error"

	RunTypeRealtime     = "realtime"
	RunTypeReplay       = "replay"
	RunTypeBacktest     = "backtest"
	RunTypeReplayReport = "replay_report"

	OrderStatusSimulated = "simulated_submitted"
	OrderStatusBlocked   = "blocked"
	OrderStatusPaused    = "paused"
	OrderStatusNoop      = "noop"
	RiskStatusAllowed    = "allowed"
	RiskStatusBlocked    = "blocked"
)

type ManagerStatus struct {
	// Enabled 表示策略子系统是否启用。
	Enabled bool `json:"enabled"`
	// ProcessRunning 表示 Python 策略进程是否存活。
	ProcessRunning bool `json:"process_running"`
	// Connected 表示 Go 侧是否已连上策略 HTTP 服务。
	Connected bool `json:"connected"`
	// HTTPAddr 是当前 HTTP 服务地址。
	HTTPAddr string `json:"http_addr"`
	// PythonExecutable 是当前使用的 Python 解释器路径。
	PythonExecutable string `json:"python_executable"`
	// PythonCondaEnvPath 是当前注入的 conda 环境根目录。
	PythonCondaEnvPath string `json:"python_conda_env_path"`
	// PythonEntry 是策略进程入口脚本。
	PythonEntry string `json:"python_entry"`
	// LastError 是最近一次策略侧错误。
	LastError string `json:"last_error"`
	// LastHealthAt 是最近一次健康检查成功时间。
	LastHealthAt time.Time `json:"last_health_at"`
	// LastRestartAt 是最近一次启动或重启 Python 策略服务时间。
	LastRestartAt time.Time `json:"last_restart_at"`
	// UpdatedAt 是状态更新时间。
	UpdatedAt time.Time `json:"updated_at"`
	// Definitions 是已同步到本地的策略定义数量。
	Definitions int `json:"definitions"`
	// Instances 是本地保存的策略实例数量。
	Instances int `json:"instances"`
	// RunningCount 是当前运行中的策略实例数量。
	RunningCount int `json:"running_count"`
	// SignalCount 是累计策略信号数。
	SignalCount int64 `json:"signal_count"`
	// AuditCount 是累计订单审计记录数。
	AuditCount int64 `json:"audit_count"`
	// BacktestRunCount 是累计回测或优化运行记录数。
	BacktestRunCount int64 `json:"backtest_run_count"`
	// AutoExecutionPaused 表示策略信号到订单执行这一步是否被人工暂停。
	AutoExecutionPaused bool `json:"auto_execution_paused"`
}

type StrategyDefinition struct {
	StrategyID    string         `json:"strategy_id"`
	DisplayName   string         `json:"display_name"`
	EntryScript   string         `json:"entry_script"`
	Version       string         `json:"version"`
	DefaultParams map[string]any `json:"default_params"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

func (d *StrategyDefinition) UnmarshalJSON(data []byte) error {
	type rawDefinition struct {
		StrategyID    string          `json:"strategy_id"`
		DisplayName   string          `json:"display_name"`
		EntryScript   string          `json:"entry_script"`
		Version       string          `json:"version"`
		DefaultParams map[string]any  `json:"default_params"`
		UpdatedAt     json.RawMessage `json:"updated_at"`
	}

	var raw rawDefinition
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	d.StrategyID = raw.StrategyID
	d.DisplayName = raw.DisplayName
	d.EntryScript = raw.EntryScript
	d.Version = raw.Version
	d.DefaultParams = raw.DefaultParams

	if len(raw.UpdatedAt) == 0 || string(raw.UpdatedAt) == "null" {
		d.UpdatedAt = time.Time{}
		return nil
	}
	updatedAt, err := parseStrategyDefinitionUpdatedAt(raw.UpdatedAt)
	if err != nil {
		return err
	}
	d.UpdatedAt = updatedAt
	return nil
}

func parseStrategyDefinitionUpdatedAt(raw json.RawMessage) (time.Time, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return time.Time{}, err
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, nil
	}

	// Python 旧调试进程曾返回没有时区的时间字符串；Go 的 time.Time 默认只接受 RFC3339。
	// 这里兼容旧格式，是为了避免一个展示字段解析失败导致整批 Python ListStrategies 被丢弃。
	if ts, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return ts, nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if ts, err := time.ParseInLocation(layout, text, time.Local); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("parse strategy definition updated_at %q failed", text)
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
	LastStartedAt      *time.Time     `json:"last_started_at,omitempty"`
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

type TraceCheck struct {
	Name        string `json:"name"`
	Passed      bool   `json:"passed"`
	Current     any    `json:"current,omitempty"`
	Target      any    `json:"target,omitempty"`
	Delta       any    `json:"delta,omitempty"`
	Description string `json:"description,omitempty"`
}

type StrategyTraceRecord struct {
	TraceID       int64          `json:"trace_id"`
	InstanceID    string         `json:"instance_id"`
	StrategyID    string         `json:"strategy_id"`
	Symbol        string         `json:"symbol"`
	Timeframe     string         `json:"timeframe"`
	Mode          string         `json:"mode"`
	EventType     string         `json:"event_type"`
	EventTime     time.Time      `json:"event_time"`
	StepKey       string         `json:"step_key"`
	StepLabel     string         `json:"step_label"`
	StepIndex     int            `json:"step_index"`
	StepTotal     int            `json:"step_total"`
	Status        string         `json:"status"`
	Reason        string         `json:"reason"`
	Checks        []TraceCheck   `json:"checks"`
	Metrics       map[string]any `json:"metrics"`
	SignalPreview map[string]any `json:"signal_preview"`
	CreatedAt     time.Time      `json:"created_at"`
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

type StrategyOrderRequest struct {
	Instance        StrategyInstance `json:"instance"`
	Symbol          string           `json:"symbol"`
	Mode            string           `json:"mode"`
	EventTime       time.Time        `json:"event_time"`
	CurrentPosition float64          `json:"current_position"`
	TargetPosition  float64          `json:"target_position"`
	PlannedDelta    float64          `json:"planned_delta"`
	Reason          string           `json:"reason"`
	Confidence      float64          `json:"confidence"`
	Metrics         map[string]any   `json:"metrics"`
}

type StrategyOrderResult struct {
	OrderID string         `json:"order_id,omitempty"`
	Status  string         `json:"status"`
	Reason  string         `json:"reason,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

type StrategyOrderExecutor interface {
	CurrentPosition(symbol string) (float64, error)
	SubmitStrategyOrder(context.Context, StrategyOrderRequest) (StrategyOrderResult, error)
}

type OrdersStatus struct {
	// Mode 表示当前订单执行模式，例如 simulated。
	Mode string `json:"mode"`
	// AutoExecutionPaused 表示策略信号到订单执行这一步是否被人工暂停。
	AutoExecutionPaused bool `json:"auto_execution_paused"`
	// Positions 是按 symbol 汇总的目标或模拟持仓。
	Positions map[string]float64 `json:"positions"`
	// LastAuditAt 是最近一次订单审计时间。
	LastAuditAt *time.Time `json:"last_audit_at,omitempty"`
	// UpdatedAt 是状态更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type TickEvent struct {
	// ReplayTaskID 是复盘训练/回放任务 ID，仅 replay 模式下有值。
	ReplayTaskID string `json:"replay_task_id,omitempty"`
	// InstrumentID 是合约代码。
	InstrumentID string `json:"instrument_id"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"exchange_id"`
	// ActionDay 是自然日。
	ActionDay string `json:"action_day"`
	// TradingDay 是业务交易日。
	TradingDay string `json:"trading_day"`
	// UpdateTime 是 HH:MM:SS 时间部分。
	UpdateTime string `json:"update_time"`
	// UpdateMillisec 是毫秒部分。
	UpdateMillisec int `json:"update_millisec"`
	// ReceivedAt 是 Go 侧接收时间。
	ReceivedAt time.Time `json:"received_at"`
	// LastPrice 是最新价。
	LastPrice float64 `json:"last_price"`
	// Volume 是累计成交量。
	Volume int `json:"volume"`
	// OpenInterest 是持仓量。
	OpenInterest float64 `json:"open_interest"`
	// SettlementPrice 是结算价。
	SettlementPrice float64 `json:"settlement_price"`
	// BidPrice1 是买一价。
	BidPrice1 float64 `json:"bid_price1"`
	// AskPrice1 是卖一价。
	AskPrice1 float64 `json:"ask_price1"`
}

type BarEvent struct {
	// ReplayTaskID 是复盘训练/回放任务 ID，仅 replay 模式下有值。
	ReplayTaskID string `json:"replay_task_id,omitempty"`
	// Variety 是品种代码。
	Variety string `json:"variety"`
	// InstrumentID 是合约代码。
	InstrumentID string `json:"instrument_id"`
	// Exchange 是交易所代码。
	Exchange string `json:"exchange"`
	// DataTime 保留原始业务/交易日上下文，不用于策略排序或定位。
	DataTime time.Time `json:"data_time"`
	// AdjustedTime 是跨夜修正后的实际时间轴。
	AdjustedTime time.Time `json:"adjusted_time"`
	// Period 是 bar 周期，例如 1m、5m。
	Period string `json:"period"`
	// Open 是开盘价。
	Open float64 `json:"open"`
	// High 是最高价。
	High float64 `json:"high"`
	// Low 是最低价。
	Low float64 `json:"low"`
	// Close 是收盘价。
	Close float64 `json:"close"`
	// Volume 是本 bar 成交量增量。
	Volume int64 `json:"volume"`
	// OpenInterest 是 bar 结束时持仓量。
	OpenInterest float64 `json:"open_interest"`
	// SettlementPrice 是 bar 对应的结算价字段。
	SettlementPrice float64 `json:"settlement_price"`
}

type EventEnvelope struct {
	// Type 是策略模块广播事件类型。
	Type string `json:"type"`
	// Data 是事件携带的数据体。
	Data any `json:"data"`
}
