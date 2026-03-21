package strategy

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const jsonCodecName = "json"

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return jsonCodecName }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

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
	// cc 是底层 gRPC 连接。
	cc *grpc.ClientConn
}

func DialStrategyService(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(jsonCodecName)),
	)
}

func NewStrategyServiceClient(cc *grpc.ClientConn) *StrategyServiceClient {
	return &StrategyServiceClient{cc: cc}
}

func (c *StrategyServiceClient) Ping(ctx context.Context) (HealthResponse, error) {
	var out HealthResponse
	err := c.cc.Invoke(ctx, "/strategy.Health/Ping", &HealthRequest{}, &out)
	return out, err
}

func (c *StrategyServiceClient) ListStrategies(ctx context.Context) (ListStrategiesResponse, error) {
	var out ListStrategiesResponse
	err := c.cc.Invoke(ctx, "/strategy.Registry/ListStrategies", &ListStrategiesRequest{}, &out)
	return out, err
}

func (c *StrategyServiceClient) LoadStrategy(ctx context.Context, req LoadStrategyRequest) error {
	var out HealthResponse
	return c.cc.Invoke(ctx, "/strategy.Runtime/LoadStrategy", &req, &out)
}

func (c *StrategyServiceClient) StartInstance(ctx context.Context, req StartInstanceRequest) error {
	var out HealthResponse
	return c.cc.Invoke(ctx, "/strategy.Runtime/StartInstance", &req, &out)
}

func (c *StrategyServiceClient) StopInstance(ctx context.Context, req StopInstanceRequest) error {
	var out HealthResponse
	return c.cc.Invoke(ctx, "/strategy.Runtime/StopInstance", &req, &out)
}

func (c *StrategyServiceClient) OnTick(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	var out SignalDecision
	err := c.cc.Invoke(ctx, "/strategy.Runtime/OnTick", &req, &out)
	return out, err
}

func (c *StrategyServiceClient) OnBar(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	var out SignalDecision
	err := c.cc.Invoke(ctx, "/strategy.Runtime/OnBar", &req, &out)
	return out, err
}

func (c *StrategyServiceClient) OnReplayBar(ctx context.Context, req DecisionRequest) (SignalDecision, error) {
	var out SignalDecision
	err := c.cc.Invoke(ctx, "/strategy.Runtime/OnReplayBar", &req, &out)
	return out, err
}

func (c *StrategyServiceClient) RunBacktest(ctx context.Context, req BacktestRequest) (BacktestResponse, error) {
	var out BacktestResponse
	err := c.cc.Invoke(ctx, "/strategy.Backtest/RunBacktest", &req, &out)
	return out, err
}

func (c *StrategyServiceClient) GetBacktestResult(ctx context.Context, req BacktestResultRequest) (BacktestResponse, error) {
	var out BacktestResponse
	err := c.cc.Invoke(ctx, "/strategy.Backtest/GetBacktestResult", &req, &out)
	return out, err
}

func (c *StrategyServiceClient) RunParameterSweep(ctx context.Context, req ParameterSweepRequest) (ParameterSweepResponse, error) {
	var out ParameterSweepResponse
	err := c.cc.Invoke(ctx, "/strategy.Optimizer/RunParameterSweep", &req, &out)
	return out, err
}

type ServiceHandlers interface {
	Ping(context.Context, HealthRequest) (HealthResponse, error)
	ListStrategies(context.Context, ListStrategiesRequest) (ListStrategiesResponse, error)
	LoadStrategy(context.Context, LoadStrategyRequest) (HealthResponse, error)
	StartInstance(context.Context, StartInstanceRequest) (HealthResponse, error)
	StopInstance(context.Context, StopInstanceRequest) (HealthResponse, error)
	OnTick(context.Context, DecisionRequest) (SignalDecision, error)
	OnBar(context.Context, DecisionRequest) (SignalDecision, error)
	OnReplayBar(context.Context, DecisionRequest) (SignalDecision, error)
	RunBacktest(context.Context, BacktestRequest) (BacktestResponse, error)
	GetBacktestResult(context.Context, BacktestResultRequest) (BacktestResponse, error)
	RunParameterSweep(context.Context, ParameterSweepRequest) (ParameterSweepResponse, error)
}

func RegisterStrategyServiceServer(s grpc.ServiceRegistrar, h ServiceHandlers) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "strategy.Health",
		HandlerType: (*ServiceHandlers)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Ping",
			Handler: unaryHandler(func(ctx context.Context, req any) (any, error) {
				return h.Ping(ctx, req.(HealthRequest))
			}, func() any { return &HealthRequest{} }),
		}},
	}, h)
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "strategy.Registry",
		HandlerType: (*ServiceHandlers)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "ListStrategies",
			Handler: unaryHandler(func(ctx context.Context, req any) (any, error) {
				return h.ListStrategies(ctx, req.(ListStrategiesRequest))
			}, func() any { return &ListStrategiesRequest{} }),
		}},
	}, h)
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "strategy.Runtime",
		HandlerType: (*ServiceHandlers)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "LoadStrategy", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.LoadStrategy(ctx, req.(LoadStrategyRequest)) }, func() any { return &LoadStrategyRequest{} })},
			{MethodName: "StartInstance", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) {
				return h.StartInstance(ctx, req.(StartInstanceRequest))
			}, func() any { return &StartInstanceRequest{} })},
			{MethodName: "StopInstance", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.StopInstance(ctx, req.(StopInstanceRequest)) }, func() any { return &StopInstanceRequest{} })},
			{MethodName: "OnTick", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.OnTick(ctx, req.(DecisionRequest)) }, func() any { return &DecisionRequest{} })},
			{MethodName: "OnBar", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.OnBar(ctx, req.(DecisionRequest)) }, func() any { return &DecisionRequest{} })},
			{MethodName: "OnReplayBar", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.OnReplayBar(ctx, req.(DecisionRequest)) }, func() any { return &DecisionRequest{} })},
		},
	}, h)
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "strategy.Backtest",
		HandlerType: (*ServiceHandlers)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "RunBacktest", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) { return h.RunBacktest(ctx, req.(BacktestRequest)) }, func() any { return &BacktestRequest{} })},
			{MethodName: "GetBacktestResult", Handler: unaryHandler(func(ctx context.Context, req any) (any, error) {
				return h.GetBacktestResult(ctx, req.(BacktestResultRequest))
			}, func() any { return &BacktestResultRequest{} })},
		},
	}, h)
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "strategy.Optimizer",
		HandlerType: (*ServiceHandlers)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "RunParameterSweep",
			Handler: unaryHandler(func(ctx context.Context, req any) (any, error) {
				return h.RunParameterSweep(ctx, req.(ParameterSweepRequest))
			}, func() any { return &ParameterSweepRequest{} }),
		}},
	}, h)
}

func unaryHandler(call func(context.Context, any) (any, error), newReq func() any) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := newReq()
		if err := dec(req); err != nil {
			return nil, err
		}
		value := req
		switch v := req.(type) {
		case *HealthRequest:
			value = *v
		case *ListStrategiesRequest:
			value = *v
		case *LoadStrategyRequest:
			value = *v
		case *StartInstanceRequest:
			value = *v
		case *StopInstanceRequest:
			value = *v
		case *DecisionRequest:
			value = *v
		case *BacktestRequest:
			value = *v
		case *BacktestResultRequest:
			value = *v
		case *ParameterSweepRequest:
			value = *v
		}
		if interceptor == nil {
			return call(ctx, value)
		}
		info := &grpc.UnaryServerInfo{
			Server:     srv,
			FullMethod: "",
		}
		return interceptor(ctx, value, info, func(ctx context.Context, req any) (any, error) {
			return call(ctx, req)
		})
	}
}
