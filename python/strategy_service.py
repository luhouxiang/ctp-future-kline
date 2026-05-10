# -*- coding: utf-8 -*-
"""策略服务兼容门面与命令行入口。

这个文件现在故意保持很薄，只做两件事：
- re-export 旧代码里从 `strategy_service` 导入的类、常量和辅助函数；
- 保留 `python strategy_service.py --addr ...` 这一启动方式，避免 Go 配置和文档立即跟着改。

拆分后的实际实现按职责分布：
- strategy_types.py：共享 JSON 类型、状态常量、策略 ID；
- strategy_common.py：日志、JSON 编解码、响应/trace 工具、Strategy 基类；
- strategy_sample.py：最小示例策略；
- ma20_pullback_short.py：MA20 baseline/pullback-short 状态机；
- ma20_weak_pullback.py：弱反弹状态机以及 hard/score 过滤变体；
- strategy_registry.py：策略注册表、运行实例工厂、StartInstance 生命周期；
- strategy_grpc.py：gRPC 服务门面、handler 构造、CLI main()。

为什么保留这个门面：
Go 侧配置里的 `python_entry` 仍指向 `python/strategy_service.py`，单测和变体入口模块也历史性地从
`strategy_service` 取公共符号。直接改路径会扩大改动面，所以这里作为稳定兼容层存在。
"""

from __future__ import annotations

from ma20_pullback_short import MA20PullbackShortStrategy, MA20WeakBaselineStrategy, PullbackBarContext, PullbackShortState
from ma20_weak_pullback import MA20WeakPullbackShortStrategy, MA20WeakPullbackVariantStrategy, WeakPullbackShortState
from strategy_common import (
    DEFAULT_STRATEGY_LOG_FILE,
    Strategy,
    _bar_event_time,
    _check,
    _configure_logging,
    _dumps,
    _ensure_logging_configured,
    _event_ts,
    _float,
    _instance_id,
    _instance_mode,
    _instance_start_log_payload,
    _int,
    _loads,
    _log_strategy_phase,
    _mode_key,
    _mode_from_value,
    _no_signal,
    _normalize_symbols,
    _params,
    _require_fields,
    _strategy_params_for_instance,
    _trace,
)
from strategy_grpc import StrategyService, add_unary, build_server, main
from strategy_registry import StrategyFactory, StrategyRegistryEntry, StrategyRuntimeInstance
from strategy_sample import SampleMomentumStrategy
from strategy_types import *

__all__ = [name for name in globals() if not name.startswith("__")]


if __name__ == "__main__":
    main()
