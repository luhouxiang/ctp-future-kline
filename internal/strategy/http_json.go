package strategy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ctp-future-kline/internal/logger"
)

type HealthRequest struct{}

type HealthResponse struct {
	// OK 表示健康检查是否通过。
	OK bool `json:"ok"`
	// Version 是策略服务版本。
	Version string `json:"version"`
	// ServerTime 是策略服务当前时间。
	ServerTime string `json:"server_time"`
}

type ListStrategiesRequest struct{}

type ListStrategiesResponse struct {
	// Strategies 是当前服务暴露的策略定义列表。
	Strategies []StrategyDefinition `json:"strategies"`
}

type LoadStrategyRequest struct {
	// StrategyID 是待加载策略定义 ID。
	StrategyID string `json:"strategy_id"`
}

type StartInstanceRequest struct {
	// Instance 是待启动的策略实例配置。
	Instance StrategyInstance `json:"instance"`
}

type StartRequirementsRequest struct {
	// Instance 是待查询启动要求的策略实例配置。
	Instance StrategyInstance `json:"instance"`
}

type StartRequirementsResponse struct {
	// WarmupTarget 是启动前至少需要预热的 K 线数量。
	WarmupTarget int `json:"warmup_target"`
	// RequiresAnchorTime 表示 warmup_target > 0 时是否必须提供起始锚点时间。
	RequiresAnchorTime bool `json:"requires_anchor_time"`
}

type StopInstanceRequest struct {
	// InstanceID 是待停止的策略实例 ID。
	InstanceID string `json:"instance_id"`
}

type DecisionRequest struct {
	// Instance 是当前触发决策的策略实例。
	Instance StrategyInstance `json:"instance"`
	// Symbol 是当前事件对应合约。
	Symbol string `json:"symbol"`
	// EventTime 是事件时间。
	EventTime string `json:"event_time"`
	// Mode 是运行模式，如 realtime 或 replay。
	Mode string `json:"mode"`
	// CurrentPosition 是当前持仓。
	CurrentPosition float64 `json:"current_position"`
	// Account 是账户快照扩展信息。
	Account map[string]any `json:"account"`
	// Tick 是 tick 驱动决策时附带的 tick 数据。
	Tick *TickEvent `json:"tick,omitempty"`
	// Bar 是 bar 驱动决策时附带的 bar 数据。
	Bar *BarEvent `json:"bar,omitempty"`
}

type SignalDecision struct {
	// NoSignal 表示本次事件没有产生交易意图，Go 侧不持久化 signal，也不执行模拟仓位变更。
	NoSignal bool `json:"no_signal,omitempty"`
	// InstanceID 是发出信号的实例 ID。
	InstanceID string `json:"instance_id"`
	// Symbol 是目标合约。
	Symbol string `json:"symbol"`
	// EventTime 是信号事件时间。
	EventTime string `json:"event_time"`
	// TargetPosition 是目标仓位。
	TargetPosition float64 `json:"target_position"`
	// Confidence 是置信度。
	Confidence float64 `json:"confidence"`
	// Reason 是信号原因说明。
	Reason string `json:"reason"`
	// Metrics 是附带的策略指标。
	Metrics map[string]any `json:"metrics"`
	// Trace 是本次策略判断过程快照。无信号时也可返回，用于策略执行可视化。
	Trace *StrategyTraceRecord `json:"trace,omitempty"`
}

type BacktestRequest struct {
	// RunID 是回测任务 ID。
	RunID string `json:"run_id"`
	// Instance 是回测使用的策略实例配置。
	Instance StrategyInstance `json:"instance"`
	// Symbol 是回测标的。
	Symbol string `json:"symbol"`
	// Timeframe 是回测周期。
	Timeframe string `json:"timeframe"`
	// StartTime 是回测起始时间。
	StartTime string `json:"start_time"`
	// EndTime 是回测结束时间。
	EndTime string `json:"end_time"`
	// Parameters 是回测参数集。
	Parameters map[string]any `json:"parameters"`
}

type BacktestResponse struct {
	// RunID 是回测任务 ID。
	RunID string `json:"run_id"`
	// Status 是回测状态。
	Status string `json:"status"`
	// Summary 是摘要结果。
	Summary map[string]any `json:"summary"`
	// Result 是详细结果载荷。
	Result map[string]any `json:"result"`
}

type BacktestResultRequest struct {
	// RunID 是要查询结果的回测任务 ID。
	RunID string `json:"run_id"`
}

type ParameterSweepRequest struct {
	// StrategyID 是待优化策略 ID。
	StrategyID string `json:"strategy_id"`
	// Symbol 是优化标的。
	Symbol string `json:"symbol"`
	// Timeframe 是优化周期。
	Timeframe string `json:"timeframe"`
	// Grid 是参数搜索网格。
	Grid map[string][]any `json:"grid"`
	// StartTime 是优化起始时间。
	StartTime string `json:"start_time"`
	// EndTime 是优化结束时间。
	EndTime string `json:"end_time"`
}

type ParameterSweepResponse struct {
	// RunID 是参数扫描任务 ID。
	RunID string `json:"run_id"`
	// Status 是扫描任务状态。
	Status string `json:"status"`
	// Summary 是优化摘要结果。
	Summary map[string]any `json:"summary"`
}

type StrategyServiceClient struct {
	baseURL    string
	httpClient *http.Client
}

const strategyHTTPIdleConnTimeout = time.Second

type asyncPushResponse struct {
	OK     bool   `json:"ok"`
	PushID string `json:"push_id"`
	Status string `json:"status"`
	Error  string `json:"error"`
}

type asyncResultRequest struct {
	PushID string `json:"push_id"`
	WaitMS int    `json:"wait_ms"`
}

type asyncResultResponse struct {
	PushID string          `json:"push_id"`
	Status string          `json:"status"`
	Error  string          `json:"error"`
	Result json.RawMessage `json:"result"`
}

type strategyHTTPError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *strategyHTTPError) Error() string {
	return fmt.Sprintf("strategy http post %s failed: status=%d body=%s", e.Path, e.StatusCode, strings.TrimSpace(e.Body))
}

func NewStrategyServiceClient(addr string, httpClient *http.Client) *StrategyServiceClient {
	if httpClient == nil {
		httpClient = newStrategyHTTPClient()
	}
	return &StrategyServiceClient{baseURL: normalizeStrategyHTTPBaseURL(addr), httpClient: httpClient}
}

func newStrategyHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Uvicorn closes idle HTTP/1.1 connections after 5s by default. Replay bars
	// often arrive at that cadence, so Go must retire idle sockets first or a
	// POST can race the server-side close and fail with EOF.
	transport.IdleConnTimeout = strategyHTTPIdleConnTimeout
	return &http.Client{Transport: transport}
}

func normalizeStrategyHTTPBaseURL(addr string) string {
	base := strings.TrimSpace(addr)
	if base == "" {
		base = "127.0.0.1:50051"
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return strings.TrimRight(base, "/")
}

func (c *StrategyServiceClient) Ping(ctx context.Context) (HealthResponse, error) {
	var out HealthResponse
	err := c.postJSON(ctx, "/health/ping", HealthRequest{}, &out)
	return out, err
}

func (c *StrategyServiceClient) ListStrategies(ctx context.Context) (ListStrategiesResponse, error) {
	var out ListStrategiesResponse
	err := c.postJSON(ctx, "/registry/list", ListStrategiesRequest{}, &out)
	logger.Info("ListStrategies call", "out", out, "err", err)
	return out, err
}

func (c *StrategyServiceClient) LoadStrategy(ctx context.Context, req LoadStrategyRequest) error {
	var out HealthResponse
	err := c.postJSON(ctx, "/runtime/load", req, &out)
	logger.Info("LoadStrategy call", "req", req, "out", out, "err", err)
	return err
}

func (c *StrategyServiceClient) GetStartRequirements(ctx context.Context, req StartRequirementsRequest) (StartRequirementsResponse, error) {
	var out StartRequirementsResponse
	err := c.postJSON(ctx, "/runtime/start-requirements", req, &out)
	logger.Info("GetStartRequirements call", "req", req, "out", out, "err", err)
	return out, err
}

func (c *StrategyServiceClient) StartInstance(ctx context.Context, req StartInstanceRequest) error {
	var out HealthResponse
	err := c.postJSON(ctx, "/runtime/start", req, &out)
	logger.Info("StartInstance call", "req", req, "out", out, "err", err)
	return err
}

func (c *StrategyServiceClient) StopInstance(ctx context.Context, req StopInstanceRequest) error {
	var out HealthResponse
	err := c.postJSON(ctx, "/runtime/stop", req, &out)
	logger.Info("StopInstance call", "req", req, "out", out, "err", err)
	return err
}

func (c *StrategyServiceClient) OnTick(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	return c.pushAndWait(ctx, "/runtime/on_tick", req)
}

func (c *StrategyServiceClient) OnBar(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	decision, err := c.pushAndWait(ctx, "/runtime/on_bar", req)
	logger.Info("OnBar call", "req", req, "out", decision, "err", err)
	return decision, err
}

func (c *StrategyServiceClient) OnReplayBar(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	decision, err := c.pushAndWait(ctx, "/runtime/on_replay_bar", req)
	logger.Info("OnReplayBar call", "req", req, "out", decision, "err", err)
	return decision, err
}

func (c *StrategyServiceClient) RunBacktest(ctx context.Context, req BacktestRequest) (BacktestResponse, error) {
	var out BacktestResponse
	err := c.postJSON(ctx, "/backtest/run", req, &out)
	logger.Info("RunBacktest call", "req", req, "out", out, "err", err)
	return out, err
}

func (c *StrategyServiceClient) GetBacktestResult(ctx context.Context, req BacktestResultRequest) (BacktestResponse, error) {
	var out BacktestResponse
	err := c.postJSON(ctx, "/backtest/result", req, &out)
	logger.Info("GetBacktestResult call", "req", req, "out", out, "err", err)
	return out, err
}

func (c *StrategyServiceClient) RunParameterSweep(ctx context.Context, req ParameterSweepRequest) (ParameterSweepResponse, error) {
	var out ParameterSweepResponse
	err := c.postJSON(ctx, "/optimizer/run-sweep", req, &out)
	logger.Info("RunParameterSweep call", "req", req, "out", out, "err", err)
	return out, err
}

func (c *StrategyServiceClient) pushAndWait(ctx context.Context, path string, req DecisionRequest) (SignalDecision, error) {
	var push asyncPushResponse
	if err := c.postJSON(ctx, path, req, &push); err != nil {
		return SignalDecision{}, err
	}
	if !push.OK || strings.TrimSpace(push.PushID) == "" {
		if strings.TrimSpace(push.Error) != "" {
			return SignalDecision{}, fmt.Errorf("strategy push failed: %s", push.Error)
		}
		return SignalDecision{}, fmt.Errorf("strategy push failed: missing push_id")
	}
	for {
		if err := ctx.Err(); err != nil {
			return SignalDecision{}, err
		}
		var result asyncResultResponse
		err := c.postJSON(ctx, "/runtime/result", asyncResultRequest{PushID: push.PushID, WaitMS: 200}, &result)
		if err != nil {
			var httpErr *strategyHTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
				var expired asyncResultResponse
				_ = json.Unmarshal([]byte(httpErr.Body), &expired)
				if strings.EqualFold(strings.TrimSpace(expired.Status), "expired") {
					return SignalDecision{}, fmt.Errorf("strategy async task expired: push_id=%s", push.PushID)
				}
			}
			return SignalDecision{}, err
		}
		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "done":
			var decision SignalDecision
			if len(result.Result) > 0 && string(result.Result) != "null" {
				if err := json.Unmarshal(result.Result, &decision); err != nil {
					return SignalDecision{}, fmt.Errorf("decode strategy async result failed: %w", err)
				}
			}
			return decision, nil
		case "error":
			return SignalDecision{}, fmt.Errorf("strategy async task failed: %s", strings.TrimSpace(result.Error))
		case "expired":
			return SignalDecision{}, fmt.Errorf("strategy async task expired: push_id=%s", push.PushID)
		case "queued", "running", "":
			continue
		default:
			return SignalDecision{}, fmt.Errorf("strategy async task returned unknown status %q", result.Status)
		}
	}
}

func (c *StrategyServiceClient) postJSON(ctx context.Context, path string, in any, out any) error {
	if c == nil {
		return fmt.Errorf("strategy http client is nil")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &strategyHTTPError{Path: path, StatusCode: resp.StatusCode, Body: string(raw)}
	}
	if out == nil {
		return nil
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(out); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return nil
}
