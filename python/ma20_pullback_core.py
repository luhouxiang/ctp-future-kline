# -*- coding: utf-8 -*-
"""MA 反抽做空策略的纯 K 线算法内核。

目的：
- 让开仓/平仓判断只依赖 K 线、均线值和算法状态，不依赖 HTTP 请求、前端 trace 或中文展示文案；
- 把可复用的交易规则从 `ma20_pullback_short.py` 适配层中拆出来，使算法更通用、更容易回测和单测；
- 保持算法结果稳定：只输出机器可读的 `PullbackDecision`，由外层决定如何展示给用户。

功能：
- 维护 `PullbackShortState`：记录形态识别、入场、持仓跟踪和平仓所需的最小状态；
- `evaluate_setup`：根据新 K 线判断是否完成做空开仓信号；
- `evaluate_exit`：开仓后根据新 K 线判断是否止损、结构止盈、继续持有或观察结束；
- reset/clear 函数：统一清理状态，避免不同入口各自重置造成状态不一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from strategy_indicators import bar_range
from strategy_types import (
    BROKEN_BELOW_MA20,
    DONE,
    SIGNAL_ACTIVE,
    WAIT_BREAK_BELOW_MA20,
    WAIT_BREAK_TOUCH_OPEN,
    PullbackStateName,
)

# 算法动作枚举：core 只返回这些机器可读动作，展示层再翻译成中文 trace/响应。
CoreAction = Literal[
    "wait_ma_ready",
    "wait",
    "setup_broken",
    "touch_found",
    "enter_short",
    "entry_confirmed",
    "exit_failure",
    "exit_success",
    "exit_unresolved",
    "reset",
]


@dataclass
class PullbackShortState:
    """单个运行维度下的算法状态。

    运行维度通常是 mode + instance_id + symbol。这里仅保存算法必须的数据：
    K 线历史、形态锚点、入场价、止损价、最低点、MA 触碰状态等。
    展示状态、中文提示和前端进度不应放在这里。
    """

    # 当前算法状态：等待跌破、已跌破等待反抽、等待触碰 K 开盘价被跌破、已触发信号、已持仓等。
    state: PullbackStateName = WAIT_BREAK_BELOW_MA20
    # 最近收盘价序列：用于外层或算法计算移动平均线时保留必要历史。
    closes: list[float] = field(default_factory=list)
    # 最近真实波幅 TR 序列：持仓止损使用动态 TR 阈值，因此在非持仓阶段也持续维护。
    trs: list[float] = field(default_factory=list)
    # 已处理的最后一根 K 线时间戳：用于过滤重复或乱序 K 线。
    last_bar_ts: float | None = None
    # 上一根 K 线收盘价：用于判断是否从均线上方向下跌破。
    prev_close: float | None = None
    # 上一根 K 线对应均线值：和 prev_close 配合判断跌破动作。
    prev_ma: float | None = None
    # 反抽触碰均线那根 K 线的开盘价：后续跌破该价才触发做空信号。
    touch_open: float | None = None
    # 反抽触碰均线那根 K 线的最高价：记录反抽力度，供展示和后续扩展过滤使用。
    touch_high: float | None = None
    # 反抽触碰发生时的均线值：记录当时压力位位置。
    touch_ma20: float | None = None
    # 反抽触碰发生的 K 线时间：用于日志、回放定位和前端展示。
    touch_time: str = ""
    # 触碰均线后已经等待的 K 线根数：用于判断是否等待超时。
    wait_bars: int = 0
    # 形态失败后是否要求下一次必须整根 K 线完整跌破：防止旧形态残留影响下一轮。
    reset_requires_full_break: bool = False
    # 是否已经出现过整根 K 线站上均线：只有 armed 后才允许识别从上向下跌破。
    break_below_armed: bool = False
    # 实际做空入场价：信号触发后的下一根 K 线开盘确认。
    signal_entry: float | None = None
    # 做空触发价：通常是反抽触碰 K 的开盘价，用于记录信号来源。
    signal_trigger_price: float | None = None
    # 信号触发 K 线的波动尺度：用于计算止损和结构止盈阈值。
    signal_atr: float | None = None
    # 兼容字段：旧固定 ATR 止盈目标价，目前只作为诊断展示，不作为核心止盈条件。
    signal_profit_target: float | None = None
    # 固定止损目标价：做空后收盘价达到该价则止损。
    signal_adverse_target: float | None = None
    # 持仓后已经观察的 K 线数量：用于观察窗口控制。
    signal_bars: int = 0
    # 做空信号触发时间：用于日志、回放定位和前端展示。
    signal_time: str = ""
    # 做空入场以来的最低价：用于盈利后计算从最低点反弹的幅度。
    signal_lowest_low: float | None = None
    # 上一根观察 K 线的最低价：用于判断低点是否连续上移。
    signal_prev_low: float | None = None
    # 入场后连续未创新低的 K 线数量：亏损状态下用于识别盘面转强。
    signal_no_new_low_bars: int = 0
    # 连续低点上移的 K 线数量：触碰均线后用于确认结构止盈。
    signal_rising_low_bars: int = 0
    # 盈利持仓后是否已经向上触碰均线：未触碰前不考虑结构止盈。
    signal_ma20_touched: bool = False
    # 首次触碰均线时对应的入场后最低价：保留触碰上下文，便于诊断和扩展。
    signal_ma20_touch_low: float | None = None
    # 下单确认时的 MA20/MA60 斜率，用于亏损进入止损区域后判断空头趋势是否更陡。
    signal_entry_ma20_slope: float | None = None
    signal_entry_ma60_slope: float | None = None
    # 最近一次动态 TR 止损阈值，便于 metrics/trace 展示。
    signal_stop_tr_avg: float | None = None


@dataclass(frozen=True)
class PullbackKLine:
    """算法内核使用的标准 K 线输入。

    外部请求里的字段可能很多，进入 core 前应先转换成这个最小结构。
    volume 表示成交量，open_interest 表示持仓量；当前开平仓规则暂未使用，
    但作为标准 K 线字段保留，便于后续增加量仓过滤条件。
    """
    open: float # K 线开盘价。
    # K 线最高价。
    high: float
    # K 线最低价。
    low: float
    # K 线收盘价，当前止损和止盈确认都以收盘价为准。
    close: float
    # K 线成交量，当前规则暂未使用，预留给量能过滤。
    volume: float = 0.0
    # K 线持仓量，当前规则暂未使用，预留给仓量变化过滤。
    open_interest: float = 0.0
    # K 线时间，主要用于状态记录、日志和回放定位。
    event_time: str = ""


@dataclass(frozen=True)
class PullbackEntrySettings:
    """开仓阶段参数。

    当前只控制反抽触碰 MA 后最多等待多少根 K 线跌破触碰 K 开盘价。
    """

    # 触碰均线后最多等待多少根 K 线跌破触碰 K 开盘价；超时则形态失败。
    max_wait_bars: int


@dataclass(frozen=True)
class PullbackExitSettings:
    """平仓阶段参数。

    这些参数描述止损、盘面转强退出、结构止盈所需的阈值，全部由外层策略配置传入。
    """

    # 平仓观察窗口；亏损且超过窗口仍未触发条件时结束本轮观察。
    observation_bars: int
    # 兼容旧参数：固定 ATR 止盈倍数，目前仅用于计算诊断字段 profit_target。
    profit_atr_multiple: float
    # 固定止损 ATR 倍数；做空后收盘价达到 entry + adverse_atr_multiple * ATR 时止损。
    adverse_atr_multiple: float
    # 亏损状态下连续未创新低多少根，判定盘面转强并止损。
    strength_exit_bars: int
    # 盈利后触碰均线，从最低点反弹至少多少 ATR 才允许止盈。
    profit_rebound_atr_multiple: float
    # 触碰均线后连续低点上移多少根，确认结构止盈。
    profit_rising_low_bars: int
    # 完全站上均线的大阳线实体至少多少 ATR，触发强阳止盈。
    strong_bull_atr_multiple: float


@dataclass(frozen=True)
class PullbackDecision:
    """算法内核的统一决策结果。

    action 是机器可读动作；reason/checks/values 是给适配层构造 trace、日志和测试断言用的结构化信息。
    """

    # 决策动作：例如 enter_short、exit_success、exit_failure、wait。
    action: CoreAction
    # 机器可读原因：适配层可据此映射成中文展示文案。
    reason: str
    # 条件检查结果：记录每个核心条件是否通过。
    checks: dict[str, bool] = field(default_factory=dict)
    # 决策过程中的关键数值：如最低价、反弹幅度、阈值等。
    values: dict[str, float | int | bool | None] = field(default_factory=dict)
    # 目标仓位：开仓信号时为 -1，其它场景通常为 None。
    target_position: int | None = None
    # 信号置信度：开仓信号展示和执行层可使用。
    confidence: float = 0.0


def clear_signal(state: PullbackShortState) -> None:
    """清空持仓/信号跟踪字段，保留 K 线历史和上一根 MA 上下文。"""
    state.signal_entry = None
    state.signal_trigger_price = None
    state.signal_atr = None
    state.signal_profit_target = None
    state.signal_adverse_target = None
    state.signal_bars = 0
    state.signal_time = ""
    state.signal_lowest_low = None
    state.signal_prev_low = None
    state.signal_no_new_low_bars = 0
    state.signal_rising_low_bars = 0
    state.signal_ma20_touched = False
    state.signal_ma20_touch_low = None
    state.signal_entry_ma20_slope = None
    state.signal_entry_ma60_slope = None
    state.signal_stop_tr_avg = None


def reset_after_failure(state: PullbackShortState) -> None:
    """形态失败后的重置。

    失败后要求下一轮必须重新确认完整跌破，防止刚失败的反抽形态立刻复用旧上下文。
    """
    state.state = WAIT_BREAK_BELOW_MA20
    state.touch_open = None
    state.touch_high = None
    state.touch_ma20 = None
    state.touch_time = ""
    state.wait_bars = 0
    state.reset_requires_full_break = True
    state.break_below_armed = False
    clear_signal(state)


def reset_after_signal_result(state: PullbackShortState) -> None:
    """开仓信号完成一次结果判定后的重置。

    结果可以是止损、止盈或观察结束。此时可以进入下一轮标准形态识别。
    """
    state.state = WAIT_BREAK_BELOW_MA20
    state.touch_open = None
    state.touch_high = None
    state.touch_ma20 = None
    state.touch_time = ""
    state.wait_bars = 0
    state.reset_requires_full_break = False
    state.break_below_armed = False
    clear_signal(state)


def start_short_signal(
    state: PullbackShortState,
    bar: PullbackKLine,
    trigger_price: float,
    settings: PullbackExitSettings,
) -> float:
    """记录做空信号触发。

    本函数只表示“开仓信号出现”，真实持仓价由下一根 K 线开盘通过 `confirm_short_entry` 确认。
    """
    atr = bar_range(bar.high, bar.low)
    state.state = SIGNAL_ACTIVE
    state.signal_entry = None
    state.signal_trigger_price = trigger_price
    state.signal_atr = atr
    state.signal_profit_target = trigger_price - settings.profit_atr_multiple * atr
    state.signal_adverse_target = trigger_price + settings.adverse_atr_multiple * atr
    state.signal_bars = 0
    state.signal_time = bar.event_time
    return atr


def confirm_short_entry(
    state: PullbackShortState,
    bar: PullbackKLine,
    settings: PullbackExitSettings,
    current_ma20_slope: float | None = None,
    current_ma60_slope: float | None = None,
) -> PullbackDecision:
    """用下一根 K 线开盘价确认做空持仓，并初始化平仓跟踪状态。"""
    state.state = DONE
    state.signal_entry = bar.open
    atr = max(0.0001, state.signal_atr or 0.0)
    state.signal_profit_target = bar.open - settings.profit_atr_multiple * atr
    state.signal_adverse_target = bar.open + settings.adverse_atr_multiple * atr
    state.signal_lowest_low = bar.open
    state.signal_prev_low = None
    state.signal_no_new_low_bars = 0
    state.signal_rising_low_bars = 0
    state.signal_ma20_touched = False
    state.signal_ma20_touch_low = None
    state.signal_entry_ma20_slope = current_ma20_slope
    state.signal_entry_ma60_slope = current_ma60_slope
    state.signal_stop_tr_avg = None
    return PullbackDecision("entry_confirmed", "short entry confirmed", {"entry_confirmed": True}, {"entry_price": bar.open})


def evaluate_setup(
    state: PullbackShortState,
    bar: PullbackKLine,
    current_ma: float,
    previous_close: float | None,
    previous_ma: float | None,
    entry_settings: PullbackEntrySettings,
    exit_settings: PullbackExitSettings,
    current_ma20_slope: float | None = None,
    current_ma60_slope: float | None = None,
) -> PullbackDecision:
    """根据当前 K 线推进开仓形态。

    可能结果包括继续等待、跌破 MA、触碰 MA、触发做空信号、下一根开盘确认入场。
    """
    if state.state == WAIT_BREAK_BELOW_MA20:
        return _evaluate_wait_break(state, bar, current_ma, previous_close, previous_ma)
    if state.state == BROKEN_BELOW_MA20:
        return _evaluate_wait_touch(state, bar, current_ma)
    if state.state == WAIT_BREAK_TOUCH_OPEN:
        return _evaluate_wait_touch_open_break(state, bar, current_ma, entry_settings, exit_settings)
    if state.state == SIGNAL_ACTIVE:
        return confirm_short_entry(state, bar, exit_settings, current_ma20_slope, current_ma60_slope)
    return PullbackDecision("wait", "state is not an entry setup state")


def evaluate_exit(
    state: PullbackShortState,
    bar: PullbackKLine,
    current_ma: float,
    settings: PullbackExitSettings,
    current_ma20_slope: float | None = None,
    current_ma60_slope: float | None = None,
    stop_tr_avg: float | None = None,
) -> PullbackDecision:
    """根据当前 K 线判断已开空仓位是否需要平仓。

    亏损时优先判断固定止损和盘面转强；盈利时只有触碰 MA 后才考虑结构止盈。
    """
    state.signal_bars += 1
    atr = max(0.0001, state.signal_atr or 0.0)
    entry = state.signal_entry
    if state.signal_lowest_low is None or bar.low < state.signal_lowest_low:
        state.signal_lowest_low = bar.low
        state.signal_no_new_low_bars = 0
    else:
        state.signal_no_new_low_bars += 1
    if state.signal_prev_low is not None and bar.low > state.signal_prev_low:
        state.signal_rising_low_bars += 1
    else:
        state.signal_rising_low_bars = 0
    state.signal_prev_low = bar.low
    if bar.high >= current_ma:
        state.signal_ma20_touched = True
        if state.signal_ma20_touch_low is None:
            state.signal_ma20_touch_low = state.signal_lowest_low

    state.signal_stop_tr_avg = stop_tr_avg
    profitable = entry is not None and bar.close < entry
    losing = entry is not None and bar.close >= entry
    adverse_distance = (bar.close - entry) if entry is not None else None
    in_dynamic_stop_zone = (
        losing
        and stop_tr_avg is not None
        and adverse_distance is not None
        and adverse_distance >= stop_tr_avg
    )
    ma20_more_bearish = (
        current_ma20_slope is not None
        and state.signal_entry_ma20_slope is not None
        and current_ma20_slope < state.signal_entry_ma20_slope
    )
    ma60_more_bearish = (
        current_ma60_slope is not None
        and state.signal_entry_ma60_slope is not None
        and current_ma60_slope < state.signal_entry_ma60_slope
    )
    dynamic_stop_deferred = bool(in_dynamic_stop_zone and ma20_more_bearish and ma60_more_bearish)
    hit_adverse = bool(in_dynamic_stop_zone and not dynamic_stop_deferred)
    legacy_no_new_low_stop = entry is not None and bar.close >= entry and state.signal_no_new_low_bars >= settings.strength_exit_bars
    legacy_stood_above_stop = entry is not None and not profitable and bar.open > current_ma and bar.close > current_ma
    rebound = bar.close - (state.signal_lowest_low if state.signal_lowest_low is not None else bar.close)
    rebound_ok = state.signal_ma20_touched and rebound >= settings.profit_rebound_atr_multiple * atr
    rising_low_take = profitable and rebound_ok and state.signal_rising_low_bars >= settings.profit_rising_low_bars
    strong_bull_take = (
        profitable
        and bar.low > current_ma
        and bar.close > bar.open
        and (bar.close - bar.open) >= settings.strong_bull_atr_multiple * atr
    )
    checks = {
        "hit_adverse": hit_adverse,
        "losing": losing,
        "in_dynamic_stop_zone": bool(in_dynamic_stop_zone),
        "ma20_more_bearish_than_entry": ma20_more_bearish,
        "ma60_more_bearish_than_entry": ma60_more_bearish,
        "dynamic_stop_deferred": dynamic_stop_deferred,
        "no_new_low_stop": False,
        "stood_above_stop": False,
        "legacy_no_new_low_stop": legacy_no_new_low_stop,
        "legacy_stood_above_stop": legacy_stood_above_stop,
        "ma_touched": state.signal_ma20_touched,
        "rebound_ok": rebound_ok,
        "rising_low_take": rising_low_take,
        "strong_bull_take": strong_bull_take,
        "within_observation": state.signal_bars < settings.observation_bars,
    }
    values = {
        "lowest_low": state.signal_lowest_low,
        "no_new_low_bars": state.signal_no_new_low_bars,
        "rising_low_bars": state.signal_rising_low_bars,
        "rebound": rebound,
        "rebound_threshold": settings.profit_rebound_atr_multiple * atr,
        "adverse_distance": adverse_distance,
        "dynamic_stop_threshold": stop_tr_avg,
        "entry_ma20_slope": state.signal_entry_ma20_slope,
        "entry_ma60_slope": state.signal_entry_ma60_slope,
        "current_ma20_slope": current_ma20_slope,
        "current_ma60_slope": current_ma60_slope,
    }
    if hit_adverse:
        return PullbackDecision("exit_failure", "dynamic stop zone hit", checks, values)
    if rising_low_take or strong_bull_take:
        return PullbackDecision("exit_success", "trend-following profit exit", checks, values)
    if state.signal_bars >= settings.observation_bars and not profitable:
        return PullbackDecision("exit_unresolved", "observation ended without structural exit", checks, values)
    return PullbackDecision("wait", "holding short until structural exit", checks, values)


def _evaluate_wait_break(
    state: PullbackShortState,
    bar: PullbackKLine,
    current_ma: float,
    previous_close: float | None,
    previous_ma: float | None,
) -> PullbackDecision:
    """阶段一：等待价格从上向下跌破均线。"""
    full_above = bar.open > current_ma and bar.high > current_ma and bar.low > current_ma and bar.close > current_ma
    if not state.break_below_armed:
        state.break_below_armed = full_above
        return PullbackDecision("wait", "waiting for first full bar above MA", {"full_above": full_above})
    full_break = bar.open < current_ma and bar.close < current_ma
    cross_break = previous_close is not None and previous_ma is not None and previous_close >= previous_ma and bar.close < current_ma
    if (state.reset_requires_full_break and full_break) or (not state.reset_requires_full_break and cross_break):
        state.state = BROKEN_BELOW_MA20
        state.reset_requires_full_break = False
        return PullbackDecision(
            "setup_broken",
            "break below MA confirmed",
            {"open_below_ma": bar.open < current_ma, "close_below_ma": bar.close < current_ma, "cross_break": cross_break},
        )
    return PullbackDecision(
        "wait",
        "waiting for MA break",
        {"open_below_ma": bar.open < current_ma, "close_below_ma": bar.close < current_ma, "cross_break": cross_break},
    )


def _evaluate_wait_touch(state: PullbackShortState, bar: PullbackKLine, current_ma: float) -> PullbackDecision:
    """阶段二：跌破后等待反抽触碰均线。"""
    touched = bar.high >= current_ma
    if touched:
        state.state = WAIT_BREAK_TOUCH_OPEN
        state.touch_open = bar.open
        state.touch_high = bar.high
        state.touch_ma20 = current_ma
        state.touch_time = bar.event_time
        state.wait_bars = 1
        return PullbackDecision("touch_found", "MA touch bar found", {"touch_ma": True})
    return PullbackDecision("wait", "waiting for MA touch", {"touch_ma": False})


def _evaluate_wait_touch_open_break(
    state: PullbackShortState,
    bar: PullbackKLine,
    current_ma: float,
    entry_settings: PullbackEntrySettings,
    exit_settings: PullbackExitSettings,
) -> PullbackDecision:
    """阶段三：触碰均线后等待跌破触碰 K 的开盘价，从而触发做空信号。"""
    stood_above = bar.open > current_ma and bar.close > current_ma
    broke_touch_open = state.touch_open is not None and bar.low <= state.touch_open
    checks = {
        "not_stood_above": not stood_above,
        "within_wait": state.wait_bars < entry_settings.max_wait_bars,
        "has_touch_open": state.touch_open is not None,
        "broke_touch_open": broke_touch_open,
    }
    if broke_touch_open:
        trigger_price = state.touch_open if state.touch_open is not None else bar.low
        atr = start_short_signal(state, bar, trigger_price, exit_settings)
        return PullbackDecision(
            "enter_short",
            "bar broke below MA touch bar open",
            checks,
            {"trigger_price": trigger_price, "entry_price": trigger_price, "atr": atr},
            target_position=-1,
            confidence=0.8,
        )
    if stood_above:
        reset_after_failure(state)
        state.break_below_armed = True
        return PullbackDecision("reset", "stood above MA", checks)
    state.wait_bars += 1
    if state.wait_bars >= entry_settings.max_wait_bars:
        reset_after_failure(state)
        return PullbackDecision("reset", "wait bars exceeded", checks)
    return PullbackDecision("wait", "waiting for touch open break", checks)
