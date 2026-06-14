# -*- coding: utf-8 -*-
"""策略注册表与运行实例生命周期管理。

本模块是“策略定义”和“正在运行的策略实例”之间的隔离层：
- 策略定义 template 只用于注册和展示，不直接承接行情；
- StartInstance 时会 clone 一个 runtime strategy，避免不同实例共享状态；
- runtime key 使用 (mode, instance_id)，保证同一个实例 ID 在 live/replay 下互不影响；
- symbol 级别状态由具体策略维护，注册表只校验请求的 symbol 是否属于启动时声明的订阅列表。

为什么不让传输层直接持有策略对象：
HTTP/CLI 等入口都只应该负责协议和错误边界，策略实例的复制、warmup、stop 清理属于运行时生命周期。
把生命周期集中到这里，可以避免后续新增入口时重复实现 StartInstance 语义。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from threading import RLock
from typing import Any

from detect_box_indicator import DetectBoxIndicatorStrategy
from ma20_pullback_short import MA20PullbackShortStrategy, MA20WeakPullbackBaselineStrategy
from ma20_state_diagram_short import MA20StateDiagramShortStrategy
from ma20_weak_pullback import (
    MA20WeakPullbackHardFilterStrategy,
    MA20WeakPullbackScoreFilterStrategy,
    MA20WeakPullbackShortStrategy,
)
from strategy_common import Strategy, _instance_id, _instance_mode, _int, _mode_key, _normalize_symbols
from strategy_sample import SampleMomentumStrategy
from strategy_types import (
    JSONObject,
    RequestDict,
    ResponseDict,
    RuntimeKey,
    StrategyDefinition,
)
from zigzag_atr_indicator import ATRZigZagIndicatorStrategy


def _builtin_strategies() -> tuple[Strategy, ...]:
    """构造内置策略模板列表。

    baseline/hard-filter/score-filter 是三个“外部可选择策略”，但它们共享底层类。
    这里统一修正 entry_script，是为了让 ListStrategies 返回用户实际选择的入口文件，
    同时保持算法实现只有一份，减少变体之间行为漂移。
    """
    return (
        # SampleMomentumStrategy(),
        # MA20PullbackShortStrategy(),
        # MA20WeakPullbackShortStrategy(),
        MA20WeakPullbackBaselineStrategy(),
        MA20StateDiagramShortStrategy(),
        ATRZigZagIndicatorStrategy(),
        DetectBoxIndicatorStrategy(),
        # MA20WeakPullbackHardFilterStrategy(),
        # MA20WeakPullbackScoreFilterStrategy(),
    )


def _clone_definition(definition: StrategyDefinition | None) -> StrategyDefinition:
    """复制策略定义，避免运行实例修改 default_params 时污染注册表。

    definition 内部包含 default_params 这种嵌套 dict，浅拷贝不够。
    如果不 deep copy，一个实例覆盖参数后可能影响后续实例或 ListStrategies 输出。
    """
    return copy.deepcopy(definition or {})


def _clone_strategy_template(strategy: Strategy) -> Strategy:
    """基于注册模板创建一个新的运行策略对象。

    每个 StartInstance 必须得到独立策略对象，因为策略对象内部保存 states 和锁。
    这里按具体策略类重新构造，是为了把“模板元数据”和“运行状态”彻底分开。
    """
    definition = _clone_definition(getattr(strategy, "definition", {}))
    if isinstance(strategy, MA20PullbackShortStrategy):
        return strategy.__class__(definition=definition)
    if isinstance(strategy, MA20WeakPullbackShortStrategy):
        return strategy.__class__(definition=definition)
    if isinstance(strategy, MA20StateDiagramShortStrategy):
        return strategy.__class__(definition=definition)
    if isinstance(strategy, ATRZigZagIndicatorStrategy):
        return strategy.__class__(definition=definition)
    if isinstance(strategy, DetectBoxIndicatorStrategy):
        return strategy.__class__(definition=definition)
    if isinstance(strategy, SampleMomentumStrategy):
        return SampleMomentumStrategy()
    return strategy.__class__()


@dataclass
class StrategyRegistryEntry:
    """工厂注册表中的策略定义项。

    strategy_id 是外部选择键；definition 是可展示元数据；template 是创建运行实例的原型。
    三者放在一个 dataclass 中，避免工厂里维护多张平行字典导致错配。
    """

    strategy_id: str
    definition: StrategyDefinition
    template: Strategy

    def create_runtime(self) -> Strategy:
        """创建独立运行策略对象。

        不直接返回 template，是因为 template 在注册表中长期存在，不能持有某个实例的行情状态。
        """
        return _clone_strategy_template(self.template)

    def required_warmup_bars(self, instance: JSONObject) -> int:
        """读取策略启动前需要的最少 K 线数量，并兜底为非负数。"""
        return max(0, _int(self.template.required_warmup_bars(instance), 0))


class StrategyRuntimeInstance:
    """一次 StartInstance 对应的运行实例。"""

    def __init__(self, entry: StrategyRegistryEntry, instance: JSONObject) -> None:
        self.entry: StrategyRegistryEntry = entry
        self.instance: JSONObject = copy.deepcopy(instance or {})
        self.instance_id: str = str(self.instance.get("instance_id") or "")
        self.mode: str = _instance_mode(self.instance)
        self.symbols: list[str] = _normalize_symbols(self.instance.get("symbols") or [])
        self.strategy: Strategy = entry.create_runtime()
        # start_instance 放在构造末尾：只有 entry/instance/mode/symbols 都准备好后才允许策略初始化状态。
        self.strategy.start_instance(self.instance)

    def _assert_symbol_allowed(self, request: RequestDict) -> None:
        """防止未订阅品种误打到某个已启动实例。

        Go 侧按实例分发行情，但 Python 侧仍做一次保护；这样配置错 symbol 时会明确报错，
        不会悄悄为陌生 symbol 创建状态或返回无意义信号。
        """
        symbol = str(request.get("symbol") or "")
        if self.symbols and symbol and symbol not in self.symbols:
            raise ValueError(
                "strategy runtime symbol not started: "
                f"instance_id={self.instance_id} mode={self.mode} symbol={symbol} symbols={','.join(self.symbols)}"
            )

    def on_tick(self, request: RequestDict) -> ResponseDict:
        self._assert_symbol_allowed(request)
        return self.strategy.on_tick(request)

    def on_bar(self, request: RequestDict) -> ResponseDict:
        self._assert_symbol_allowed(request)
        return self.strategy.on_bar(request)

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        self._assert_symbol_allowed(request)
        return self.strategy.on_replay_bar(request)


class StrategyFactory:
    """单例策略工厂：注册策略定义，并维护已启动的运行实例。"""

    def __init__(self) -> None:
        self._strategies: dict[str, StrategyRegistryEntry] = {}
        self._instances: dict[RuntimeKey, StrategyRuntimeInstance] = {}
        self._lock: RLock = RLock()

    @property
    def strategies(self) -> dict[str, StrategyRegistryEntry]:
        return self._strategies

    @property
    def instances(self) -> dict[RuntimeKey, StrategyRuntimeInstance]:
        return self._instances

    def register(self, strategy: Strategy) -> None:
        """注册策略模板。

        register 只接受带 strategy_id 的模板对象；运行实例由 start_instance 再创建。
        这个边界能防止“注册对象”和“运行对象”混用。
        """
        strategy_id = str(strategy.definition.get("strategy_id") or "")
        if not strategy_id:
            raise ValueError("strategy definition missing strategy_id")
        self._strategies[strategy_id] = StrategyRegistryEntry(
            strategy_id=strategy_id,
            definition=_clone_definition(strategy.definition),
            template=strategy,
        )

    @classmethod
    def builtin(cls) -> "StrategyFactory":
        """创建包含全部内置策略的工厂。"""
        factory = cls()
        for strategy in _builtin_strategies():
            factory.register(strategy)
        return factory

    def list_definitions(self) -> list[StrategyDefinition]:
        """返回策略定义副本，避免调用方修改注册表内部 definition。"""
        return [_clone_definition(entry.definition) for entry in self._strategies.values()]

    def load_strategy(self, strategy_id: Any) -> StrategyRegistryEntry:
        """按 strategy_id 获取注册项；未知策略直接抛 ValueError 交给上层入口映射成请求错误。"""
        strategy_id = str(strategy_id or "")
        if strategy_id not in self._strategies:
            raise ValueError(f"unknown strategy_id: {strategy_id}")
        return self._strategies[strategy_id]

    def start_requirements(self, instance: JSONObject) -> dict[str, Any]:
        """返回启动前 warmup 需求。

        Go 侧会先调用这个接口决定要从锚点向前取多少根 K 线。
        Python 返回的是策略真正需要的数量，避免 Go 侧硬编码某个算法的 warmup 规则。
        """
        entry = self.load_strategy((instance or {}).get("strategy_id", ""))
        warmup_target = entry.required_warmup_bars(instance or {})
        return {
            "warmup_target": warmup_target,
            "requires_anchor_time": warmup_target > 0,
        }

    def _key_for_instance(self, instance: JSONObject | None) -> RuntimeKey:
        """从 StartInstance 的 instance 生成 runtime key。"""
        instance_id = str((instance or {}).get("instance_id") or "")
        if not instance_id:
            raise ValueError("missing required field(s): instance.instance_id")
        return (_instance_mode(instance), instance_id)

    def _key_for_request(self, request: RequestDict) -> RuntimeKey:
        """从行情请求生成 runtime key。"""
        instance_id = _instance_id(request)
        if not instance_id:
            raise ValueError("missing required field(s): instance.instance_id")
        return (_mode_key(request), instance_id)

    def start_instance(self, instance: JSONObject) -> StrategyRuntimeInstance:
        """启动一个运行实例，并替换同 key 下的旧实例。

        替换语义是有意的：用户修改参数重新启动同一个 instance_id 时，新实例应立即接管后续行情。
        """
        entry = self.load_strategy((instance or {}).get("strategy_id", ""))
        key = self._key_for_instance(instance)
        runtime = StrategyRuntimeInstance(entry, instance)
        with self._lock:
            self._instances[key] = runtime
        return runtime

    def stop_instance(self, instance_id: Any) -> int:
        """停止所有 mode 下同 instance_id 的运行实例。"""
        instance_id = str(instance_id or "")
        removed = 0
        with self._lock:
            for key, runtime in list(self._instances.items()):
                if key[1] != instance_id:
                    continue
                runtime.strategy.stop_instance(instance_id)
                del self._instances[key]
                removed += 1
        return removed

    def runtime_for_request(self, request: RequestDict) -> StrategyRuntimeInstance:
        """根据行情请求找到已启动 runtime；找不到则说明调用顺序错误。"""
        key = self._key_for_request(request)
        with self._lock:
            runtime = self._instances.get(key)
        if runtime is None:
            instance = request.get("instance") or {}
            raise ValueError(
                "strategy runtime instance not started: "
                f"instance_id={key[1]} strategy_id={instance.get('strategy_id', '')} mode={key[0]}"
            )
        return runtime

    def dispatch(self, method_name: str, request: RequestDict) -> ResponseDict:
        """把 OnTick/OnBar/OnReplayBar 分发到运行策略对象。"""
        runtime = self.runtime_for_request(request)
        return getattr(runtime, method_name)(request)


# -----------------------------------------------------------------------------
# 策略服务门面：把入口方法映射到具体策略
# -----------------------------------------------------------------------------
