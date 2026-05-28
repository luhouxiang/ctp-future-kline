# -*- coding: utf-8 -*-
"""策略服务共享类型与状态常量。

这个文件只放“跨模块共享、但没有运行逻辑”的定义：
- JSON 边界类型：Go 侧通过 HTTP 传入的是 JSON 请求体，Python 内部统一用 dict/list 表达；
- 状态常量：策略状态要同时出现在算法、metrics、trace、单测和前端展示中，集中定义可以避免拼写分叉；
- 策略 ID：注册表、入口文件和测试都引用这些 ID，集中定义后变体新增/改名更容易审计。

为什么不把这些定义留在各个策略文件：
拆分 `strategy_service.py` 后，策略、注册表、运行时门面都需要同一批类型和常量。
如果每个文件各自定义字符串，最容易出现“代码能跑但 trace.step_key 和测试/前端不一致”的隐性错误。
"""

from __future__ import annotations

from typing import Any, Literal, TypeAlias

# JSONPrimitive/JSONValue 用来描述 JSON 边界的真实形状。
# 这里不强行建模所有字段，是因为 Go 侧和回测/前端会带不同字段进来；
# 策略函数通过 _float/_int/_require_fields 在使用点做收窄，比伪造一个不完整 TypedDict 更可靠。
JSONPrimitive: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONPrimitive | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, Any]
RequestDict: TypeAlias = dict[str, Any]
ResponseDict: TypeAlias = dict[str, Any]
MetricsDict: TypeAlias = dict[str, Any]
TraceDict: TypeAlias = dict[str, Any]
CheckDict: TypeAlias = dict[str, Any]
BarDict: TypeAlias = dict[str, Any]
TickDict: TypeAlias = dict[str, Any]
StrategyDefinition: TypeAlias = dict[str, Any]
StrategySettings: TypeAlias = dict[str, Any]

# ModeKey 只允许 live/replay 两类运行时状态。这样同一个 instance_id 在实盘和回放中不会互相污染。
ModeKey: TypeAlias = Literal["live", "replay"]
TraceStatus: TypeAlias = Literal["waiting", "passed", "failed", "done"]

# StateKey 定位单个策略状态：同一策略实例可以同时订阅多个 symbol，也可以分别跑 live/replay。
StateKey: TypeAlias = tuple[ModeKey, str, str]

# RuntimeKey 定位一次 StartInstance 创建的运行实例；symbol 级状态由具体策略自己维护。
RuntimeKey: TypeAlias = tuple[ModeKey, str]

# MA20 baseline / pullback-short state constants.
# 这 5 个状态是 baseline 的公开状态机枚举；metrics.state 和 trace.step_key 都应使用这些值。
WAIT_BREAK_BELOW_MA20: str = "WAIT_BREAK_BELOW_MA20"    # 等待价格跌破 MA20 形成突破信号
BROKEN_BELOW_MA20: str = "BROKEN_BELOW_MA20"            # 已经确认价格跌破 MA20，等待回抽确认
WAIT_BREAK_TOUCH_OPEN: str = "WAIT_BREAK_TOUCH_OPEN"    # 等待价格跌破触碰 K 开盘价
SIGNAL_ACTIVE: str = "SIGNAL_ACTIVE"                    # 信号已触发，等待确认入场
DONE: str = "DONE"                                      # 交易已完成

# MA20 weak-pullback state and variant constants.
# 弱反弹策略比 baseline 多了趋势/结构过滤和 reaction_low 确认，因此单独保留这些步骤名。
TREND_STRUCTURE_FILTER: str = "TREND_STRUCTURE_FILTER"
WAIT_PULLBACK_TOUCH_MA20: str = "WAIT_PULLBACK_TOUCH_MA20"
WAIT_BREAK_REACTION_LOW: str = "WAIT_BREAK_REACTION_LOW"
SHORT_SIGNAL: str = "SHORT_SIGNAL"

# 策略 ID 是外部配置、Go 注册、前端选择和 Python 注册表之间的契约，不应在各文件中手写多份。
MA20_WEAK_STRATEGY_ID: str = "ma20.weak_pullback_short"
MA20_WEAK_BASELINE_STRATEGY_ID: str = "ma20.weak_pullback_short.baseline"
MA20_WEAK_HARD_FILTER_STRATEGY_ID: str = "ma20.weak_pullback_short.hard_filter"
MA20_WEAK_SCORE_FILTER_STRATEGY_ID: str = "ma20.weak_pullback_short.score_filter"
MA20_STATE_DIAGRAM_STRATEGY_ID: str = "ma20.state_diagram_short"

# MA20 state-diagram short strategy constants.
ABOVE_MA20: str = "ABOVE_MA20"
BELOW_MA20: str = "BELOW_MA20"
REBOUND_TO_HIGH: str = "REBOUND_TO_HIGH"
SECOND_BREAK_SIGNAL: str = "SECOND_BREAK_SIGNAL"
PROFIT_HOLDING: str = "PROFIT_HOLDING"
LOSS_HOLDING: str = "LOSS_HOLDING"
TAKE_PROFIT: str = "TAKE_PROFIT"
STOP_LOSS: str = "STOP_LOSS"

# Literal 类型让状态字段的可选值在注解里可见；运行时仍是普通字符串，便于 JSON 序列化。
PullbackStateName: TypeAlias = Literal[
    "WAIT_BREAK_BELOW_MA20",
    "BROKEN_BELOW_MA20",
    "WAIT_BREAK_TOUCH_OPEN",
    "SIGNAL_ACTIVE",
    "DONE",
]
WeakPullbackStateName: TypeAlias = Literal[
    "WAIT_BREAK_BELOW_MA20",
    "TREND_STRUCTURE_FILTER",
    "WAIT_PULLBACK_TOUCH_MA20",
    "WAIT_BREAK_REACTION_LOW",
    "SIGNAL_ACTIVE",
    "DONE",
]
WeakRegime: TypeAlias = Literal[
    "",
    "BEARISH_REVERSAL_CANDIDATE",
    "WEAK_BEARISH_CANDIDATE",
    "UNCLEAR",
]
StateDiagramShortStateName: TypeAlias = Literal[
    "ABOVE_MA20",
    "BELOW_MA20",
    "REBOUND_TO_HIGH",
    "SECOND_BREAK_SIGNAL",
    "PROFIT_HOLDING",
    "LOSS_HOLDING",
]
