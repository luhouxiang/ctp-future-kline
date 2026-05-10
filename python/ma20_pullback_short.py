# -*- coding: utf-8 -*-
"""MA20 baseline / pullback-short 状态机。

本模块实现两类策略：
- `MA20PullbackShortStrategy`：原始 MA20 跌破-反抽-再跌破做空算法；
- `MA20WeakBaselineStrategy`：弱反弹策略族里的 baseline 变体，复用同一套父类状态机。

核心设计原因：
- 只使用 bar 驱动：回放和实盘能走同一条逻辑，不再依赖 tick 采样时机；
- warmup 只计算 MA 输入：历史 K 线是为了让第一根正式行情能计算 MA，不应该提前触发交易状态；
- 状态拆成 handler：每个状态只处理自己的转换条件，避免一个大函数里混合“等待跌破/等待触碰/等待入场/观察止盈止损”；
- `DONE` 表示“已持仓观察中”：止盈、止损、未决是 signal_result，不再增加额外状态枚举。
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

from strategy_common import (
    Strategy,
    _bar_event_time,
    _check,
    _event_ts,
    _float,
    _instance_id,
    _instance_mode,
    _instance_start_log_payload,
    _int,
    _mode_key,
    _no_signal,
    _normalize_symbols,
    _params,
    _require_fields,
    _strategy_params_for_instance,
    _trace,
)
from strategy_types import (
    BROKEN_BELOW_MA20,
    DONE,
    MA20_WEAK_BASELINE_STRATEGY_ID,
    SIGNAL_ACTIVE,
    WAIT_BREAK_BELOW_MA20,
    WAIT_BREAK_TOUCH_OPEN,
    BarDict,
    CheckDict,
    JSONObject,
    MetricsDict,
    ModeKey,
    PullbackStateName,
    RequestDict,
    ResponseDict,
    StateKey,
    StrategyDefinition,
    TraceDict,
)

logger = logging.getLogger(__name__)

# MA20 反抽做空策略状态对象
# -----------------------------------------------------------------------------
@dataclass
class PullbackShortState:
    """保存 MA20 反抽做空策略在单个实例+品种+模式下的状态。

    状态维度由 (mode, instance_id, symbol) 唯一定位。
    这样可以让同一策略同时服务多个策略实例、多个交易品种、live/replay 两种模式。
    """

    # 当前状态机状态，默认等待第一次跌破 MA。
    state: PullbackStateName = WAIT_BREAK_BELOW_MA20
    # 最近收盘价序列，用来计算移动平均线。会被裁剪，避免无限增长。
    closes: list[float] = field(default_factory=list)
    # 已处理的最后一根 K 线时间戳，用于过滤重复或乱序 K 线。
    last_bar_ts: float | None = None
    # 上一根 K 线的收盘价，用于判断“从上向下跌破”。
    prev_close: float | None = None
    # 上一根 K 线对应的均线值，用于判断“从上向下跌破”。
    prev_ma: float | None = None
    # 反抽触碰 MA 的那根 K 线的开盘价；后续 bar 跌破该价时触发做空。
    touch_open: float | None = None
    # 反抽触碰 MA 的那根 K 线的最高价，用于记录形态信息。
    touch_high: float | None = None
    # 触碰发生时的 MA 值，用于后续 metrics/trace 展示。
    touch_ma20: float | None = None
    # 触碰 K 的时间，便于前端或日志定位触发形态。
    touch_time: str = ""
    # 从找到触碰 K 之后已经等待了多少根 bar，用于超时重置。
    wait_bars: int = 0
    # 形态失败后是否要求下一次必须“整根 K 线都在 MA 下方”才重新确认跌破。
    reset_requires_full_break: bool = False
    # 是否已经看到至少一根 OHLC 全部在 MA 上方的 K 线；只有 armed 后才允许等待“跌破 MA”。
    break_below_armed: bool = False
    # 信号入场价；进入 SIGNAL_ACTIVE 后才有值。
    signal_entry: float | None = None
    # 触发做空的价格，先记录触碰K开盘价；下一根bar开盘后再确认 signal_entry。
    signal_trigger_price: float | None = None
    # 触发K线的波动尺度，下一根bar确认入场后用于重新计算止盈/止损。
    signal_atr: float | None = None
    # 信号观察窗口内的目标止盈价，用来判断这个形态是否先兑现。
    signal_profit_target: float | None = None
    # 信号观察窗口内的反向失败价，用来标记信号是否先被破坏。
    signal_adverse_target: float | None = None
    # 信号触发后已经观察了多少根 bar。
    signal_bars: int = 0
    # 信号触发时间，用于日志、trace 和回测展示。
    signal_time: str = ""


@dataclass
class PullbackBarContext:
    """MA20 baseline 单根 bar 的解析结果。

    为什么要引入 context 对象：
    `_on_bar_locked` 负责解析和计算 MA，状态 handler 只关心已经标准化的字段。
    这样后续新增检查项时，不需要给每个 handler 传一长串位置参数，也能让变量类型一眼可见。
    """

    # 原始请求，用于构造响应、trace 和读取 instance 参数。
    request: RequestDict
    # 当前 symbol 的可变状态；handler 修改它来推进状态机。
    state: PullbackShortState
    # 原始 bar dict 保留给后续扩展，当前主要使用解析后的 OHLC 字段。
    bar: BarDict
    # 均线周期，例如默认 20，也允许实例参数覆盖为 MA55 等。
    ma_period: int
    # 触碰 MA 后最多等待多少根 bar 跌破触碰 K 开盘价。
    max_wait_bars: int
    # 展示标签，例如 MA20/MA55，用于 trace 文案。
    ma_label: str
    # 当前 bar 的 OHLC，已经通过 _float 归一化。
    open_price: float
    high: float
    low: float
    close: float
    # 事件时间字符串和可比较时间戳；时间戳可能解析失败，所以允许 None。
    event_time: str
    event_ts: float | None
    # 追加当前 close 之前计算出的前一根 MA/close，用来判断“从上向下跌破”。
    previous_ma: float | None
    previous_close: float | None
    # 追加当前 close 后得到的当前 MA。
    ma20: float

# MA20 反抽做空策略
# -----------------------------------------------------------------------------
class MA20PullbackShortStrategy(Strategy):
    """MA20 反抽做空策略。

    该策略使用状态机识别一个偏空形态：

    第 1 阶段：等待 MA 数据足够。
    第 2 阶段：等待价格从上向下跌破 MA。
    第 3 阶段：跌破后，等待价格反抽，K 线最高价触碰/超过 MA。
    第 4 阶段：找到触碰 K 后，等待后续 bar 跌破该 K 线开盘价。
    第 5 阶段：触发 SHORT 后先进入 SIGNAL_ACTIVE，下一根 bar 开盘确认入场后进入 DONE。
    DONE 表示已持仓观察中，止盈/止损/未决通过 signal_result 输出，不再作为独立状态。

    失败重置条件：
    - 在等待 bar 跌破触碰 K 开盘价期间，如果某根 K 线开盘和收盘都重新站上 MA，形态失败；
    - 如果等待 bar 数超过 max_wait_bars，也视为形态失败；
    - 失败后 reset_requires_full_break=True，要求下一轮用 full_break 重新确认下破。

    参数：
    - ma_period：均线周期，默认 20，最小 2；
    - max_wait_bars：触碰 K 出现后最多等待多少根 bar，默认 6，最小 1。
    """

    definition: StrategyDefinition = {
        "strategy_id": "ma20.pullback_short",
        "display_name": "MA20 Pullback Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {
            "ma_period": 20,
            "max_wait_bars": 6,
            "observation_bars": 24,
            "profit_atr_multiple": 1.0,
            "adverse_atr_multiple": 0.8,
        },
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        """初始化单个运行实例的状态容器与锁。"""
        if definition is not None:
            self.definition = definition
        # states 保存当前运行实例内每个 (mode, instance_id, symbol) 对应的 PullbackShortState。
        self.states: dict[StateKey, PullbackShortState] = {}
        # gRPC server 使用线程池处理请求，因此运行状态读写需要加锁。
        self._lock: RLock = RLock()

    def required_warmup_bars(self, instance: JSONObject) -> int:
        """返回启动所需的最少 warmup K 线数量。

        显式 warmup_target 优先，是为了让 Go 侧/前端可以强制指定某次运行的取数数量。
        否则只返回 ma_period：baseline 只需要 MA 样本，不再额外加 20 根让历史形态推进状态机。
        """
        params = _strategy_params_for_instance(self.definition, instance)
        if _int(params.get("warmup_target"), 0) > 0:
            return _int(params.get("warmup_target"), 0)
        return max(2, _int(params.get("ma_period"), 20))

    def start_instance(self, instance: JSONObject) -> None:
        """启动一个策略实例，并为其订阅品种初始化状态。

        instance 预期包含：
        - instance_id: 策略实例 ID；
        - symbols: 交易品种列表；
        - params: 策略参数，可包含 warmup_bars；
        - mode: live 或 replay。
        """
        logger.info("start_instance args=%s", json.dumps(_instance_start_log_payload(instance), ensure_ascii=False, sort_keys=True))
        instance_id = instance.get("instance_id", "")
        symbols = instance.get("symbols") or []
        params = instance.get("params") or {}
        mode = _instance_mode(instance)
        # 若 symbols 为空，使用空字符串作为兜底 symbol key，保证状态仍可创建。
        if not symbols:
            symbols = [""]
        with self._lock:
            for symbol in symbols:
                self.states[(mode, instance_id, str(symbol))] = PullbackShortState()
        # warmup 只补齐 MA 计算所需 closes，不推进交易状态。
        applied_counts = self._apply_warmup_bars(instance, params.get("warmup_bars") or [], mode)
        self.validate_warmup(instance, applied_counts)

    def stop_instance(self, instance_id: str) -> None:
        """停止策略实例，并清理该实例下所有 symbol/mode 的状态。"""
        with self._lock:
            # 复制 keys 后再删除，避免遍历字典时修改字典。
            for key in list(self.states):
                if len(key) >= 2 and key[1] == instance_id:
                    del self.states[key]

    def _state_for(self, request: RequestDict) -> PullbackShortState:
        """根据请求定位策略状态；不存在说明实例未显式启动或 symbol 不匹配。"""
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            raise ValueError(
                "strategy runtime instance not started: "
                f"instance_id={key[1]} mode={key[0]} symbol={key[2]} strategy_id={self.definition.get('strategy_id', '')}"
            )
        return self.states[key]

    def _apply_warmup_bars(self, instance: JSONObject, bars: Any, mode: str) -> dict[str, int]:
        """用 warmup_bars 仅预热 MA 输入，不执行交易状态转移。

        这里故意不调用 `_on_bar_locked`：
        `_on_bar_locked` 会推进 WAIT_BREAK_BELOW_MA20 -> BROKEN_BELOW_MA20 等交易状态，
        而 warmup 的目标只是让正式第一根 bar 有足够 MA 上下文。
        如果用历史 K 线提前跑完整状态机，锚点前的形态会污染锚点后的交易判断。
        """
        counts: dict[str, int] = {}
        if not isinstance(bars, list) or not bars:
            return counts
        symbol = ""
        symbols = instance.get("symbols") or []
        if symbols:
            symbol = str(symbols[0])
        params = _strategy_params_for_instance(self.definition, instance)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        target = self.required_warmup_bars(instance)
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            item_symbol = str(bar.get("symbol") or symbol)
            if target > 0 and counts.get(item_symbol, 0) >= target:
                continue
            state = self.states.get((mode, instance.get("instance_id", ""), item_symbol))
            if state is None:
                continue
            close = _float(bar.get("close"), None)
            if close is None:
                continue
            event_ts = _event_ts(_bar_event_time(bar))
            if event_ts is not None and state.last_bar_ts is not None and event_ts <= state.last_bar_ts:
                continue
            previous_ma = self._ma(state.closes, ma_period)
            state.closes.append(close)
            self._trim_closes(state, ma_period)
            # prev_close/prev_ma 只作为正式第一根 bar 的“上一根上下文”，不代表 warmup 已进入交易状态。
            state.prev_close = close
            state.prev_ma = previous_ma
            if event_ts is not None:
                state.last_bar_ts = event_ts
            counts[item_symbol] = counts.get(item_symbol, 0) + 1
            # 保留精确请求的最少K线，不为了后续状态判断额外喂历史形态。
            state.state = WAIT_BREAK_BELOW_MA20
            state.break_below_armed = False
            state.reset_requires_full_break = False
        return counts

    def validate_warmup(self, instance: JSONObject, applied_counts: dict[str, int] | None = None) -> None:
        target = self.required_warmup_bars(instance)
        if target <= 0:
            return
        instance_id = instance.get("instance_id", "")
        mode = _instance_mode(instance)
        symbols = instance.get("symbols") or [""]
        applied_counts = applied_counts or {}
        params = _strategy_params_for_instance(self.definition, instance)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        missing: list[str] = []
        with self._lock:
            for symbol in symbols:
                key = (mode, instance_id, str(symbol))
                state = self.states.get(key)
                ma_value = self._ma((state.closes if state else []), ma_period)
                applied = _int(applied_counts.get(str(symbol)), 0)
                if state is None or ma_value is None or applied < target:
                    missing.append(f"{symbol}:{applied}/{target}")
        if missing:
            raise ValueError(
                "strategy warmup is insufficient for MA calculation: "
                f"instance_id={instance_id} strategy_id={self.definition.get('strategy_id', '')} "
                f"mode={mode} ma_period={ma_period} warmup_target={target} symbols={','.join(missing)}"
            )

    def _settings(self, request: RequestDict) -> tuple[int, int]:
        """读取并规范化策略参数。"""
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        # 均线周期至少为 2，避免 period=0/1 等无意义或危险配置。
        ma_period = max(2, _int(params.get("ma_period"), 20))
        # 触碰后等待 bar 数至少为 1。
        max_wait_bars = max(1, _int(params.get("max_wait_bars"), 6))
        return ma_period, max_wait_bars

    def _signal_settings(self, request: RequestDict) -> StrategySettings:
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "observation_bars": max(1, _int(params.get("observation_bars"), 24)),
            "profit_atr_multiple": max(0.0001, _float(params.get("profit_atr_multiple"), 1.0)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
        }

    def _ma(self, closes: list[float], period: int) -> float | None:
        """计算最近 period 个收盘价的简单移动平均线 SMA。"""
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def _reset_after_failure(self, state: PullbackShortState) -> None:
        """形态失败后的状态重置。

        重置内容：
        - 回到等待跌破 MA 状态；
        - 清除触碰 K 相关记录；
        - wait_bars 清零；
        - 要求下一次必须 full_break 才能重新确认跌破。
        """
        state.state = WAIT_BREAK_BELOW_MA20
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.wait_bars = 0
        state.reset_requires_full_break = True
        state.break_below_armed = False
        self._clear_signal(state)

    def _reset_after_signal_result(self, state: PullbackShortState) -> None:
        """信号成功/失败/超时结束后，回到等待下一次 MA 跌破。"""
        state.state = WAIT_BREAK_BELOW_MA20
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.wait_bars = 0
        state.reset_requires_full_break = False
        state.break_below_armed = False
        self._clear_signal(state)

    def _clear_signal(self, state: PullbackShortState) -> None:
        state.signal_entry = None
        state.signal_trigger_price = None
        state.signal_atr = None
        state.signal_profit_target = None
        state.signal_adverse_target = None
        state.signal_bars = 0
        state.signal_time = ""

    def _start_short_signal(
        self,
        state: PullbackShortState,
        trigger_price: float,
        high: float,
        low: float,
        event_time: str,
        request: RequestDict,
    ) -> float:
        """记录做空信号触发上下文。

        baseline 在触碰 K 开盘价被跌破时先触发目标仓位，但真实持仓入场价要等下一根 bar 开盘确认。
        因此这里记录 signal_trigger_price 和信号 bar 的 ATR，`_handle_signal_active` 再用下一根开盘价重算目标。
        """
        settings = self._signal_settings(request)
        atr = max(0.0001, high - low)
        state.state = SIGNAL_ACTIVE
        state.signal_entry = None
        state.signal_trigger_price = trigger_price
        state.signal_atr = atr
        state.signal_profit_target = trigger_price - settings["profit_atr_multiple"] * atr
        state.signal_adverse_target = trigger_price + settings["adverse_atr_multiple"] * atr
        state.signal_bars = 0
        state.signal_time = event_time
        return atr

    def _signal_metrics(self, state: PullbackShortState, ma20: float | None, ma_period: int) -> MetricsDict:
        metrics = self._base_metrics(state, ma20, ma_period)
        metrics.update({
            "signal_entry": state.signal_entry,
            "signal_trigger_price": state.signal_trigger_price,
            "signal_atr": state.signal_atr,
            "profit_target": state.signal_profit_target,
            "adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_time": state.signal_time,
        })
        return metrics

    def _evaluate_done_signal(
        self,
        request: RequestDict,
        state: PullbackShortState,
        high: float,
        low: float,
        ma20: float,
        ma_period: int,
    ) -> ResponseDict:
        """在 DONE 状态下观察止盈/止损/未决结果。

        DONE 不是“算法停止”，而是“已持仓并等待结果”。
        做空信号先触及 adverse_target 记为 failure，先触及 profit_target 记为 success；
        若 observation_bars 内都没触及，输出 unresolved 并重置到下一轮等待。
        """
        settings = self._signal_settings(request)
        state.signal_bars += 1
        hit_adverse = state.signal_adverse_target is not None and high >= state.signal_adverse_target
        hit_profit = state.signal_profit_target is not None and low <= state.signal_profit_target
        checks = [
            _check("触及止损价", hit_adverse, high, state.signal_adverse_target),
            _check("触及止盈价", hit_profit, low, state.signal_profit_target),
            _check("观察窗口未结束", state.signal_bars < settings["observation_bars"], state.signal_bars, settings["observation_bars"]),
        ]
        if hit_adverse:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "failure"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号失败", 5, "failed", "adverse target hit before profit target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "adverse target hit before profit target", metrics, trace)
        if hit_profit:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "success"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号成功", 5, "passed", "profit target hit before adverse target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "profit target hit before adverse target", metrics, trace)
        if state.signal_bars >= settings["observation_bars"]:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "unresolved"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号观察结束", 5, "done", "observation window ended without profit/adverse target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "observation window ended without profit/adverse target", metrics, trace)
        metrics = self._signal_metrics(state, ma20, ma_period)
        trace = _trace(request, "bar", DONE, "持仓观察中", 5, "waiting", "waiting for signal result", checks, metrics)
        return _no_signal(request, "waiting for signal result", metrics, trace)

    def _ma_label(self, period: int) -> str:
        """根据均线周期生成展示标签，例如 period=20 时为 MA20。"""
        return f"MA{period}"

    def _trim_closes(self, state: PullbackShortState, ma_period: int) -> None:
        """裁剪 closes 序列，避免历史收盘价无限增长。

        需要保留 ma_period + 1 个值，是因为：
        - ma_period 个值用于计算当前 MA；
        - 额外 1 个值有助于比较前一根 bar 的 close/MA 状态。
        """
        keep = max(ma_period + 1, 2)
        if len(state.closes) > keep:
            state.closes = state.closes[-keep:]

    def _base_metrics(
        self,
        state: PullbackShortState,
        ma20: float | None = None,
        ma_period: int = 20,
    ) -> MetricsDict:
        """生成 MA20 策略通用诊断指标。"""
        return {
            "signal": "",
            "state": state.state,
            "ma_period": ma_period,
            "ma": ma20,
            # 保留 ma20 字段名兼容前端；即使 ma_period 不是 20，这里仍沿用原字段。
            "ma20": ma20,
            "touch_open": state.touch_open,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "touch_time": state.touch_time,
            "trigger_price": None,
            "signal_trigger_price": state.signal_trigger_price,
            "signal_entry": state.signal_entry,
            "signal_atr": state.signal_atr,
            "profit_target": state.signal_profit_target,
            "adverse_target": state.signal_adverse_target,
            "wait_bars": state.wait_bars,
            "step_total": 5,
        }

    def _bar_trace(
        self,
        request: RequestDict,
        state: PullbackShortState,
        step_key: str,
        step_label: str,
        step_index: int,
        status: str,
        reason: str,
        checks: list[CheckDict],
        ma20: float | None = None,
        ma_period: int = 20,
    ) -> TraceDict:
        """构造 bar 事件的 trace。"""
        return _trace(
            request,
            "bar",
            step_key,
            step_label,
            step_index,
            status,
            reason,
            checks,
            self._base_metrics(state, ma20, ma_period),
        )

    def on_bar(self, request: RequestDict) -> ResponseDict:
        """线程安全地处理 K 线事件。"""
        with self._lock:
            return self._on_bar_locked(request)

    def _on_bar_locked(self, request: RequestDict) -> ResponseDict:
        """解析单根 K 线并按当前状态分派到对应处理函数。

        这个函数只做四件事：
        1. 校验并解析 bar；
        2. 维护 close 序列和 MA；
        3. 构造 PullbackBarContext；
        4. 根据 state.state 分派到具体 handler。

        交易判断本身不放在这里，是为了让每个状态转换都能独立阅读和测试。
        """
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        _require_fields(bar, ("open", "high", "low", "close"))

        state = self._state_for(request)
        ma_period, max_wait_bars = self._settings(request)
        ma_label = self._ma_label(ma_period)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        close = _float(bar.get("close"))
        event_time = _bar_event_time(bar, request.get("event_time", ""))
        event_ts = _event_ts(event_time)
        logger.info("pullback_on_bar: symbol=%s event_time=%s state=%s close=%.4f",
                    request.get("symbol", ""), event_time, state.state, close)

        if event_ts is not None and state.last_bar_ts is not None and event_ts <= state.last_bar_ts:
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state, state.prev_ma, ma_period))
        if event_ts is not None:
            state.last_bar_ts = event_ts

        previous_ma = self._ma(state.closes, ma_period)
        previous_close = state.prev_close
        state.closes.append(close)
        self._trim_closes(state, ma_period)
        ma20 = self._ma(state.closes, ma_period)

        if ma20 is None:
            state.prev_close = close
            state.prev_ma = ma20
            checks = [
                _check("MA样本数量", len(state.closes) >= ma_period, len(state.closes), ma_period, len(state.closes) - ma_period, "等待收集足够K线计算均线")
            ]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", f"等待 {ma_label} 数据足够", 1, "waiting", "waiting for enough bars", checks, ma20, ma_period)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ma20, ma_period), trace)

        ctx = PullbackBarContext(
            request=request,
            state=state,
            bar=bar,
            ma_period=ma_period,
            max_wait_bars=max_wait_bars,
            ma_label=ma_label,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            event_time=event_time,
            event_ts=event_ts,
            previous_ma=previous_ma,
            previous_close=previous_close,
            ma20=ma20,
        )
        handlers = {
            WAIT_BREAK_BELOW_MA20: self._handle_wait_break_below_ma20,
            BROKEN_BELOW_MA20: self._handle_broken_below_ma20,
            WAIT_BREAK_TOUCH_OPEN: self._handle_wait_break_touch_open,
            SIGNAL_ACTIVE: self._handle_signal_active,
            DONE: self._handle_done,
        }
        # 状态分派表是显式列出的 5 状态机。未知状态走重置分支，避免异常状态长期卡住实例。
        handler = handlers.get(state.state)
        if handler is None:
            self._reset_after_signal_result(state)
            out = _no_signal(request, "unknown state reset", self._base_metrics(state, ma20, ma_period))
        else:
            out = handler(ctx)
        state.prev_close = close
        state.prev_ma = ma20
        return out

    def _handle_wait_break_below_ma20(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 1：等待有效跌破 MA。

        先要求出现一根 OHLC 全部在 MA 上方的 bar，是为了避免策略启动时价格已经在 MA 下方，
        warmup 后第一根正式 bar 直接被误认为“从上向下跌破”。只有先确认市场曾完整站上 MA，
        后续 cross_break 才有“跌破”语义。
        """
        state = ctx.state
        full_above = ctx.open_price > ctx.ma20 and ctx.high > ctx.ma20 and ctx.low > ctx.ma20 and ctx.close > ctx.ma20
        if not state.break_below_armed:
            state.break_below_armed = full_above
            checks = [_check(f"整根K线站上{ctx.ma_label}", full_above, ctx.close, ctx.ma20, ctx.close - ctx.ma20, f"等待出现 OHLC 全部在 {ctx.ma_label} 上方的K线")]
            trace = self._bar_trace(ctx.request, state, WAIT_BREAK_BELOW_MA20, f"等待先完整站上 {ctx.ma_label}", 1, "waiting", "waiting for first full bar above MA", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, "waiting for first full bar above MA", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)

        full_break = ctx.open_price < ctx.ma20 and ctx.close < ctx.ma20
        cross_break = ctx.previous_close is not None and ctx.previous_ma is not None and ctx.previous_close >= ctx.previous_ma and ctx.close < ctx.ma20
        checks = [
            _check(f"开盘低于{ctx.ma_label}", ctx.open_price < ctx.ma20, ctx.open_price, ctx.ma20, ctx.open_price - ctx.ma20),
            _check(f"收盘低于{ctx.ma_label}", ctx.close < ctx.ma20, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
            _check("从上向下跌破", cross_break, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
        ]
        if (state.reset_requires_full_break and full_break) or (not state.reset_requires_full_break and cross_break):
            state.state = BROKEN_BELOW_MA20
            state.reset_requires_full_break = False
            trace = self._bar_trace(ctx.request, state, BROKEN_BELOW_MA20, f"已跌破，等待反抽触碰 {ctx.ma_label}", 2, "passed", f"break below {ctx.ma_label} confirmed", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"break below {ctx.ma_label} confirmed", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx.request, state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "waiting", "no trade signal", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "no trade signal", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)

    def _handle_broken_below_ma20(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 2：跌破后等待反抽触碰 MA。

        触碰条件使用 high >= MA，而不是 close >= MA：
        反抽做空关注的是价格曾经回抽到均线压力附近，哪怕收盘又回落，也说明压力位被测试过。
        """
        state = ctx.state
        checks = [_check(f"最高价触碰{ctx.ma_label}", ctx.high >= ctx.ma20, ctx.high, ctx.ma20, ctx.high - ctx.ma20, f"等待反抽到{ctx.ma_label}附近")]
        if ctx.high >= ctx.ma20:
            state.state = WAIT_BREAK_TOUCH_OPEN
            state.touch_open = ctx.open_price
            state.touch_high = ctx.high
            state.touch_ma20 = ctx.ma20
            state.touch_time = ctx.event_time
            state.wait_bars = 1
            trace = self._bar_trace(ctx.request, state, WAIT_BREAK_TOUCH_OPEN, "等待跌破触碰K开盘价", 3, "passed", f"{ctx.ma_label} touch bar found", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"{ctx.ma_label} touch bar found", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx.request, state, BROKEN_BELOW_MA20, f"已跌破，等待反抽触碰 {ctx.ma_label}", 2, "waiting", "waiting for MA touch", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "waiting for MA touch", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)

    def _handle_wait_break_touch_open(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 3：触碰 MA 后等待跌破触碰 K 开盘价。

        触碰 K 的开盘价是这套 baseline 提示词中的“回抽失败确认线”。
        后续 bar 的 low 跌破该价时，说明反抽后买盘没有延续，才输出 SHORT。
        """
        state = ctx.state
        stood_above = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        wait_remaining = ctx.max_wait_bars - state.wait_bars
        broke_touch_open = state.touch_open is not None and ctx.low <= state.touch_open
        checks = [
            _check(f"未重新站上{ctx.ma_label}", not stood_above, ctx.close, ctx.ma20, ctx.close - ctx.ma20, "重新站上则形态失败"),
            _check("等待未超时", state.wait_bars < ctx.max_wait_bars, state.wait_bars, ctx.max_wait_bars, wait_remaining),
            _check("触碰K开盘价有效", state.touch_open is not None, state.touch_open, "not null"),
            _check("K线跌破触碰K开盘价", broke_touch_open, ctx.low, state.touch_open),
        ]
        if broke_touch_open:
            atr = self._start_short_signal(state, state.touch_open, ctx.high, ctx.low, ctx.event_time, ctx.request)
            metrics = self._signal_metrics(state, ctx.ma20, ctx.ma_period)
            metrics.update({
                "signal": "SHORT",
                "touch_open": state.touch_open,
                "touch_high": state.touch_high,
                "touch_ma20": state.touch_ma20,
                "touch_time": state.touch_time,
                "trigger_price": state.touch_open,
                "entry_price": state.touch_open,
                "atr": atr,
            })
            trace = _trace(ctx.request, "bar", SIGNAL_ACTIVE, "做空信号已触发", 4, "passed", "SHORT: bar broke below MA touch bar open", checks, metrics, {"target_position": -1, "confidence": 0.8, "signal": "SHORT"})
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": -1,
                "confidence": 0.8,
                "reason": "SHORT: bar broke below MA touch bar open",
                "metrics": metrics,
                "trace": trace,
            }
        if stood_above:
            # 重新站上 MA 表示回抽可能转强，此时形态失效；下一轮仍允许从 armed 状态继续等待跌破。
            self._reset_after_failure(state)
            state.break_below_armed = True
            trace = self._bar_trace(ctx.request, state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "failed", f"reset: stood above {ctx.ma_label}", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"reset: stood above {ctx.ma_label}", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)
        state.wait_bars += 1
        if state.wait_bars >= ctx.max_wait_bars:
            self._reset_after_failure(state)
            trace = self._bar_trace(ctx.request, state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "failed", "reset: wait bars exceeded", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, "reset: wait bars exceeded", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx.request, state, WAIT_BREAK_TOUCH_OPEN, "等待跌破触碰K开盘价", 3, "waiting", "waiting for touch open break", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "waiting for touch open break", self._base_metrics(state, ctx.ma20, ctx.ma_period), trace)

    def _handle_signal_active(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 4：信号触发后的下一根 bar 开盘确认入场。

        这样做是为了避免在同一根触发 bar 内同时完成“触发”和“观察结果”。
        回测中用下一根开盘价作为持仓上下文，能更接近实际下单延迟，也让止盈/止损观察从持仓后开始。
        """
        state = ctx.state
        state.state = DONE
        state.signal_entry = ctx.open_price
        settings = self._signal_settings(ctx.request)
        atr = max(0.0001, state.signal_atr or 0.0)
        state.signal_profit_target = ctx.open_price - settings["profit_atr_multiple"] * atr
        state.signal_adverse_target = ctx.open_price + settings["adverse_atr_multiple"] * atr
        metrics = self._signal_metrics(state, ctx.ma20, ctx.ma_period)
        metrics["signal"] = "SHORT"
        checks = [_check("下一根K线开盘确认持仓", True, ctx.open_price, "entry open")]
        trace = self._bar_trace(ctx.request, state, DONE, "已持仓，等待止盈/止损", 5, "passed", "short position confirmed at next bar open", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "short position confirmed at next bar open", metrics, trace)

    def _handle_done(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 5：持仓观察，继续判断止盈/止损/未决。"""
        return self._evaluate_done_signal(ctx.request, ctx.state, ctx.high, ctx.low, ctx.ma20, ctx.ma_period)

    def on_tick(self, request: RequestDict) -> ResponseDict:
        """baseline 策略只由 bar 驱动，tick 不参与入场。"""
        with self._lock:
            state = self._state_for(request)
            ma_period, _ = self._settings(request)
            return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state, None, ma_period))

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        """回放 K 线事件复用实时 K 线逻辑。"""
        return self.on_bar(request)


class MA20WeakBaselineStrategy(MA20PullbackShortStrategy):
    """The baseline MA20 break-touch-break strategy as a selectable variant."""

    def __init__(self) -> None:
        definition = dict(MA20PullbackShortStrategy.definition)
        definition.update({
            "strategy_id": MA20_WEAK_BASELINE_STRATEGY_ID,
            "display_name": "MA20 Weak Pullback Baseline",
            "default_params": {
                "ma_period": 20,
                "max_wait_bars": 6,
                "observation_bars": 24,
                "profit_atr_multiple": 1.0,
                "adverse_atr_multiple": 0.8,
                "algorithm": "baseline",
            },
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
        super().__init__(definition=definition)
