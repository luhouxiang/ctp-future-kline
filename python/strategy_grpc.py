# -*- coding: utf-8 -*-
"""gRPC 服务门面与命令行启动入口。

本模块只处理协议层问题：
- 把普通 Python 方法包装成 gRPC unary handler；
- 在 handler 边界做 JSON bytes <-> dict 的转换；
- 把 ValueError 映射为 INVALID_ARGUMENT，把未知异常映射为 INTERNAL；
- 构建 generic gRPC server，不依赖 protobuf 生成代码。

为什么不用 protobuf 生成类：
当前 Go/Python 策略协议已经约定使用 JSON bytes，策略请求字段也会随算法快速迭代。
generic handler 可以让 Python 策略先快速演进，等协议稳定后再考虑强 protobuf schema。
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections.abc import Callable
from concurrent import futures
from typing import Any

from strategy_common import (
    DEFAULT_STRATEGY_LOG_FILE,
    _configure_logging,
    _dumps,
    _ensure_logging_configured,
    _loads,
    _log_strategy_phase,
)
from strategy_registry import StrategyFactory, StrategyRegistryEntry, StrategyRuntimeInstance
from strategy_types import JSONObject, RequestDict, ResponseDict

try:
    import grpc
except ModuleNotFoundError:
    # 允许单测和静态导入在未安装 grpcio 的环境里运行；
    # 只有真正 build_server/add_unary 时才要求 grpc 存在。
    grpc = None

logger = logging.getLogger(__name__)


class StrategyService:
    """策略服务对象。

    该类不直接依赖 grpc，而是提供普通 Python 方法；
    add_unary 会把这些方法包装成 gRPC unary-unary handler。
    """

    def __init__(self) -> None:
        """初始化策略工厂并注册内置策略。

        每个 StrategyService 实例持有一个 StrategyFactory。
        这样单测可以创建隔离服务对象，生产进程则通常只创建一个服务对象挂到 gRPC server。
        """
        _ensure_logging_configured()
        self.factory: StrategyFactory = StrategyFactory.builtin()
        self.strategies: dict[str, StrategyRegistryEntry] = self.factory.strategies
        logger.info("strategy registry initialized2: %s", ",".join(sorted(self.strategies)))

    def Ping(self, request: RequestDict, context: Any) -> ResponseDict:
        """健康检查接口。"""
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def ListStrategies(self, request: RequestDict, context: Any) -> ResponseDict:
        """返回当前服务支持的策略元数据列表。"""
        return {
            "strategies": self.factory.list_definitions()
        }

    def LoadStrategy(self, request: RequestDict, context: Any) -> ResponseDict:
        """加载/校验指定策略。

        当前实现中的策略均已在 __init__ 注册，因此这里只校验 strategy_id 是否存在。
        """
        strategy_id = request.get("strategy_id", "")
        self.factory.load_strategy(strategy_id)
        logger.info("strategy loaded: %s", strategy_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def GetStartRequirements(self, request: RequestDict, context: Any) -> ResponseDict:
        """返回启动该实例前需要满足的 warmup 条件。

        这个接口存在的原因：Go 侧负责取历史 K 线，但最少需要多少根只有策略自己知道。
        让 Python 先返回 warmup_target，可以避免 Go 侧为每个策略写一套硬编码规则。
        """
        instance = request.get("instance") or {}
        out = self.factory.start_requirements(instance)
        logger.info(
            "strategy start requirements: instance_id=%s，strategy_id=%s，mode=%s，timeframe=%s，warmup_target=%s，requires_anchor_time=%s",
            instance.get("instance_id", ""),
            instance.get("strategy_id", ""),
            instance.get("mode", ""),
            instance.get("timeframe", ""),
            out.get("warmup_target"),
            out.get("requires_anchor_time"),
        )
        return out

    def StartInstance(self, request: RequestDict, context: Any) -> ResponseDict:
        """启动策略实例，并把 instance 交给对应策略初始化。

        instance.params 里可能已经包含 Go 侧按 warmup_target 精确取回的 warmup_bars。
        这里不再二次取数，只把完整 instance 交给 StrategyFactory，由具体策略决定如何 warmup。
        """
        instance = request.get("instance") or {}
        logger.info("strategy StartInstance begin...")
        runtime = self.factory.start_instance(instance)
        logger.info(
            "strategy StartInstance end.  runtime_count=%s", len(self.factory.instances),
        )
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request: RequestDict, context: Any) -> ResponseDict:
        """停止策略实例。

        工厂会清理该 instance_id 在所有 mode 下的运行对象。
        """
        instance_id = request.get("instance_id", "")
        removed = self.factory.stop_instance(instance_id)
        logger.info("strategy instance stopped: instance_id=%s runtime_removed=%s", instance_id, removed)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request: RequestDict, context: Any) -> ResponseDict:
        """接收 tick 事件并分发给请求中指定的策略。

        是否使用 tick 由策略自己决定；例如当前 MA20 baseline 明确忽略 tick，sample 策略则会处理 tick。
        """
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_tick", req))

    def OnBar(self, request: RequestDict, context: Any) -> ResponseDict:
        """接收实时 K 线事件并分发给请求中指定的策略。"""
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_bar", req))

    def OnReplayBar(self, request: RequestDict, context: Any) -> ResponseDict:
        """接收回放 K 线事件并分发给请求中指定的策略。

        单独保留 replay 方法，是为了让策略按 mode 隔离状态；同一个 instance_id 可以同时有 live/replay 状态。
        """
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_replay_bar", req))

    def RunBacktest(self, request: RequestDict, context: Any) -> ResponseDict:
        """运行回测的占位接口。

        当前只返回固定 mock 结果，表示接口链路可用；
        若要接入真实回测，需要在这里调用回测引擎并返回 equity_curve/trades 等结果。
        """
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"trades": 0, "pnl": 0, "note": "mock backtest stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def GetBacktestResult(self, request: RequestDict, context: Any) -> ResponseDict:
        """查询回测结果的占位接口。"""
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"note": "mock backtest result stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def RunParameterSweep(self, request: RequestDict, context: Any) -> ResponseDict:
        """参数扫描/优化的占位接口。

        request.grid 预期是一个参数名到候选值列表的 dict。
        当前 summary.candidates 简单统计所有列表长度之和，并非笛卡尔积组合数量。
        """
        return {
            "run_id": f"opt-{int(time.time() * 1000)}",
            "status": "done",
            "summary": {"candidates": sum(len(v) for v in request.get("grid", {}).values()), "note": "mock optimizer stub"},
        }

    def _strategy_for_instance(self, instance: JSONObject) -> StrategyRegistryEntry:
        """根据 instance.strategy_id 获取策略注册项。"""
        strategy_id = instance.get("strategy_id", "")
        return self.factory.load_strategy(strategy_id)

    def _strategy_for_request(self, request: RequestDict) -> StrategyRuntimeInstance:
        """从请求定位已启动运行实例。"""
        return self.factory.runtime_for_request(request)

    def _run_and_log_phase(self, request: RequestDict, fn: Callable[[RequestDict], ResponseDict]) -> ResponseDict:
        """统一执行策略调用并记录 trace 阶段日志。"""
        out = fn(request)
        _log_strategy_phase(request, out)
        return out


# -----------------------------------------------------------------------------
# gRPC handler 构建
# -----------------------------------------------------------------------------
def add_unary(
    handler: StrategyService,
    service_name: str,
    method_name: str,
    fn: Callable[[RequestDict, Any], ResponseDict],
) -> Any:
    """把普通 Python 方法包装成 gRPC unary-unary handler。

    参数：
        handler: 保留参数，当前实现未使用。调用处传入 service。
        service_name: gRPC 服务名，例如 "strategy.Runtime"。
        method_name: gRPC 方法名，例如 "OnBar"。
        fn: 真正处理请求的 Python 函数。

    工作流程：
    1. gRPC 收到 bytes 请求；
    2. invoke 使用 _loads 转成 dict；
    3. 调用 fn(dict, context)；
    4. 使用 _dumps 将返回 dict 转成 JSON bytes。

    异常处理：
    - ValueError：视为请求参数错误，返回 INVALID_ARGUMENT；
    - 其他异常：视为服务内部错误，返回 INTERNAL。
    """
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
    full_name = f"{service_name}/{method_name}"

    def invoke(request: bytes, context: Any) -> ResponseDict:
        """gRPC 实际调用的内部函数。"""
        try:
            return fn(_loads(request), context)
        except ValueError as exc:
            logger.warning("strategy request rejected: %s: %s", full_name, exc)
            if context is not None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            raise
        except Exception as exc:
            logger.exception("strategy request failed: %s", full_name)
            if context is not None:
                context.abort(grpc.StatusCode.INTERNAL, str(exc))
            raise

    return grpc.unary_unary_rpc_method_handler(
        invoke,
        # 请求反序列化器不做 protobuf 解析，直接把 bytes 交给 invoke。
        request_deserializer=lambda b: b,
        # 响应序列化器将 dict 转为 JSON bytes。
        response_serializer=_dumps,
    )


def build_server() -> Any:
    """创建并配置 gRPC server。

    这里使用 generic handler 手动注册服务名和方法名，是为了和 Go 侧自定义 JSON gRPC 客户端保持一致。
    只要方法名稳定，Python 侧内部拆分文件不会影响 Go 侧调用路径。
    """
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
    _ensure_logging_configured()
    service = StrategyService()
    # 使用最多 8 个工作线程并发处理 unary 请求。
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    # 使用 generic handler 手动注册服务与方法，无需 protobuf 生成代码。
    server.add_generic_rpc_handlers(
        (
            # 健康检查服务。
            grpc.method_handlers_generic_handler(
                "strategy.Health",
                {"Ping": add_unary(service, "strategy.Health", "Ping", service.Ping)},
            ),
            # 策略注册表服务。
            grpc.method_handlers_generic_handler(
                "strategy.Registry",
                {"ListStrategies": add_unary(service, "strategy.Registry", "ListStrategies", service.ListStrategies)},
            ),
            # 策略运行时服务：加载、启动、停止、接收行情事件。
            grpc.method_handlers_generic_handler(
                "strategy.Runtime",
                {
                    "LoadStrategy": add_unary(service, "strategy.Runtime", "LoadStrategy", service.LoadStrategy),
                    "GetStartRequirements": add_unary(service, "strategy.Runtime", "GetStartRequirements", service.GetStartRequirements),
                    "StartInstance": add_unary(service, "strategy.Runtime", "StartInstance", service.StartInstance),
                    "StopInstance": add_unary(service, "strategy.Runtime", "StopInstance", service.StopInstance),
                    "OnTick": add_unary(service, "strategy.Runtime", "OnTick", service.OnTick),
                    "OnBar": add_unary(service, "strategy.Runtime", "OnBar", service.OnBar),
                    "OnReplayBar": add_unary(service, "strategy.Runtime", "OnReplayBar", service.OnReplayBar),
                },
            ),
            # 回测服务，目前为 mock/stub。
            grpc.method_handlers_generic_handler(
                "strategy.Backtest",
                {
                    "RunBacktest": add_unary(service, "strategy.Backtest", "RunBacktest", service.RunBacktest),
                    "GetBacktestResult": add_unary(service, "strategy.Backtest", "GetBacktestResult", service.GetBacktestResult),
                },
            ),
            # 参数优化服务，目前为 mock/stub。
            grpc.method_handlers_generic_handler(
                "strategy.Optimizer",
                {"RunParameterSweep": add_unary(service, "strategy.Optimizer", "RunParameterSweep", service.RunParameterSweep)},
            ),
        )
    )
    return server


# -----------------------------------------------------------------------------
# 命令行入口
# -----------------------------------------------------------------------------
def main() -> None:
    """命令行启动入口。

    示例：
        python strategy_service.py --addr 127.0.0.1:50051
    """
    # 默认只监听本机：策略服务由本机 Go 进程管理，不需要暴露到外部网络。
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    parser.add_argument("--log-file", default=DEFAULT_STRATEGY_LOG_FILE)
    parser.add_argument("--log-level", default=os.environ.get("STRATEGY_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    _configure_logging(args.log_file, args.log_level)
    # 创建 gRPC server，绑定端口并启动。
    server = build_server()
    bound_port = server.add_insecure_port(args.addr)
    if bound_port == 0:
        logger.error("strategy service bind failed: addr=%s log_file=%s", args.addr, args.log_file)
        raise SystemExit(f"strategy service bind failed: {args.addr}")
    server.start()
    logger.info("strategy service listening on %s log_file=%s", args.addr, args.log_file)
    print(f"strategy service listening on {args.addr}", flush=True)
    # 阻塞主线程，直到进程被终止。
    server.wait_for_termination()


# 本模块不直接写 if __name__ == "__main__"；兼容入口 strategy_service.py 会调用 main()。
# 这样 Go 侧仍可启动旧入口，同时本文件也能被单测安全导入。
