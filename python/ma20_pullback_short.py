# -*- coding: utf-8 -*-
"""MA20 baseline / pullback-short 策略适配层。

目的：
- 保留对策略运行框架的兼容接口：start/stop/on_bar/on_tick/on_replay_bar；
- 负责请求解析、warmup、实例状态字典、响应组装、metrics 和 trace 展示；
- 将真正的开仓/平仓算法委托给 `ma20_pullback_core.py`，避免算法逻辑与 UI 展示逻辑混在一起。

功能：
- 实现两类策略：
- `MA20PullbackShortStrategy`：原始 MA20 跌破-反抽-再跌破做空算法；
- `MA20WeakPullbackBaselineStrategy`：弱反弹策略族里的 baseline 变体，复用同一套父类状态机。
- 将外部 bar 请求转换为 core 使用的 `PullbackKLine`；
- 将 core 返回的 `PullbackDecision` 转换成前端需要的 no_signal/signal 响应和 trace；
- 管理 warmup K 线，只预热指标输入，不让历史 K 线提前推进交易状态。

职责边界：
- 本文件可以定义展示字段、trace 步骤、中文提示和运行框架适配；
- 本文件不应新增交易规则。新增开仓/平仓规则应优先放入 `ma20_pullback_core.py`；
- 标准指标计算应放入 `strategy_indicators.py`，本文件只调用。

核心设计原则：
- 只使用 bar 驱动：回放和实盘能走同一条逻辑，不再依赖 tick 采样时机；
- warmup 只计算 MA 输入：历史 K 线是为了让第一根正式行情能计算 MA，不应该提前触发交易状态；
- 状态拆成 handler：每个状态只处理自己的转换条件，避免一个大函数里混合“等待跌破/等待触碰/等待入场/观察止盈止损”；
- `DONE` 表示“已持仓观察中”：止盈、止损、未决是 signal_result，不再增加额外状态枚举。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from threading import RLock
from typing import Any

from ma20_pullback_core import (
    PullbackDecision,
    PullbackEntrySettings,
    PullbackExitSettings,
    PullbackKLine,
    PullbackShortState,
    clear_signal,
    evaluate_exit,
    evaluate_setup,
    reset_after_failure,
    reset_after_signal_result,
)
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
    _rfc3339_utc_now,
    _strategy_params_for_instance,
    _trace,
)
from strategy_indicators import moving_average_slope, simple_moving_average, trim_tail, trimmed_top_average, true_range
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
    RequestDict,
    ResponseDict,
    StateKey,
    StrategyDefinition,
    TraceDict,
)

logger = logging.getLogger(__name__)

@dataclass
class PullbackBarContext:
    """MA20 baseline 单根 bar 的解析结果。

    功能：
    - 保存外层已经完成解析的请求、OHLC、MA、上一根 MA/close 等上下文；
    - 避免 handler 直接读取原始 dict，降低字段缺失和类型转换散落各处的风险。

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
    # 持仓动态止损使用的 MA60、MA20/MA60 斜率和 TR 阈值。
    ma60: float | None = None
    ma20_slope: float | None = None
    ma60_slope: float | None = None
    stop_tr_avg: float | None = None

# MA20 反抽做空策略
# -----------------------------------------------------------------------------
class MA20PullbackShortStrategy(Strategy):
    """MA20 反抽做空策略。

    目的：
    - 作为策略框架可调用的运行时类；
    - 维护每个实例/品种/mode 的 `PullbackShortState`；
    - 通过 core 算法判断是否开仓、是否平仓，再把结果包装成现有前端和 Go 侧可识别的响应。

    算法形态：

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

    # 策略定义：提供给 Go 侧和前端注册表读取的策略元数据。
    definition: StrategyDefinition = {
        # 策略唯一 ID：启动、回放、信号记录都通过它定位策略。
        "strategy_id": "ma20.pullback_short",
        # 前端展示名称：策略列表中显示给用户看的名称。
        "display_name": "MA20 Pullback Short",
        # Python 运行入口：Go 侧拉起策略服务时使用。
        "entry_script": "python/strategy_service.py",
        # 策略版本号：用于识别策略定义变更。
        "version": "1.0.0",
        # 默认参数：实例未显式覆盖时使用。
        "default_params": {
            # 均线周期：默认使用 MA20，也可配置成其它周期。
            "ma_period": 20,
            # 动态止损使用的长均线和斜率回看。
            "ma60_period": 60,
            "slope_lookback_bars": 5,
            # 动态止损 TR 口径：最近60个TR中取最大10个，去掉最大2个后8个求平均。
            "stop_tr_lookback": 60,
            "stop_tr_top_n": 10,
            "stop_tr_drop_n": 2,
            # 反抽触碰均线后最多等待多少根 K 线跌破触碰 K 开盘价。
            "max_wait_bars": 6,
            # 平仓观察窗口：亏损且超过窗口仍未触发条件时结束观察。
            "observation_bars": 240,
            # 兼容旧参数：固定 ATR 止盈倍数，目前只用于诊断字段。
            "profit_atr_multiple": 1.0,
            # 固定止损 ATR 倍数：做空后收盘达到止损价则退出。
            "adverse_atr_multiple": 0.8,
            # 亏损后连续未创新低的根数阈值，用于盘面转强止损。
            "strength_exit_bars": 3,
            # 盈利后触碰均线，从最低点反弹多少 ATR 才允许止盈。
            "profit_rebound_atr_multiple": 1.0,
            # 触碰均线后连续低点上移多少根，确认结构止盈。
            "profit_rising_low_bars": 2,
            # 强阳完全站稳均线的实体 ATR 阈值，用于强阳止盈。
            "strong_bull_atr_multiple": 1.5,
        },
        # Go 侧 StrategyDefinition.updated_at 是 time.Time；带 Z 的 RFC3339 能避免 JSON 解码失败。
        "updated_at": _rfc3339_utc_now(),
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        """初始化单个运行实例的状态容器与锁。"""
        if definition is not None:
            self.definition = definition
        # states 保存当前运行实例内每个 (mode, instance_id, symbol) 对应的 PullbackShortState。
        # 运行状态表：key=(mode, instance_id, symbol)，value=该维度下的算法状态。
        self.states: dict[StateKey, PullbackShortState] = {}
        # 状态锁：HTTP 服务会并发处理请求，因此运行状态读写需要加锁。
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
            high = _float(bar.get("high", close), close)
            low = _float(bar.get("low", close), close)
            previous_close = state.prev_close
            state.closes.append(close)
            state.trs.append(true_range(high, low, previous_close))
            self._trim_closes(state, ma_period, params)
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
            "observation_bars": max(1, _int(params.get("observation_bars"), 240)),
            "profit_atr_multiple": max(0.0001, _float(params.get("profit_atr_multiple"), 1.0)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
            "strength_exit_bars": max(1, _int(params.get("strength_exit_bars"), 3)),
            "profit_rebound_atr_multiple": max(0.0001, _float(params.get("profit_rebound_atr_multiple"), _float(params.get("profit_atr_multiple"), 1.0))),
            "profit_rising_low_bars": max(1, _int(params.get("profit_rising_low_bars"), 2)),
            "strong_bull_atr_multiple": max(0.0001, _float(params.get("strong_bull_atr_multiple"), 1.5)),
            "ma60_period": max(3, _int(params.get("ma60_period"), 60)),
            "slope_lookback_bars": max(1, _int(params.get("slope_lookback_bars"), 5)),
            "stop_tr_lookback": max(1, _int(params.get("stop_tr_lookback"), 60)),
            "stop_tr_top_n": max(1, _int(params.get("stop_tr_top_n"), 10)),
            "stop_tr_drop_n": max(0, _int(params.get("stop_tr_drop_n"), 2)),
        }

    def _entry_settings(self, request: RequestDict) -> PullbackEntrySettings:
        """读取开仓阶段参数，并转换成 core 使用的配置对象。"""
        _, max_wait_bars = self._settings(request)
        return PullbackEntrySettings(max_wait_bars=max_wait_bars)

    def _exit_settings(self, request: RequestDict) -> PullbackExitSettings:
        """读取平仓阶段参数，并转换成 core 使用的配置对象。"""
        settings = self._signal_settings(request)
        return PullbackExitSettings(
            observation_bars=settings["observation_bars"],
            profit_atr_multiple=settings["profit_atr_multiple"],
            adverse_atr_multiple=settings["adverse_atr_multiple"],
            strength_exit_bars=settings["strength_exit_bars"],
            profit_rebound_atr_multiple=settings["profit_rebound_atr_multiple"],
            profit_rising_low_bars=settings["profit_rising_low_bars"],
            strong_bull_atr_multiple=settings["strong_bull_atr_multiple"],
        )

    def _ma(self, closes: list[float], period: int) -> float | None:
        """计算最近 period 个收盘价的简单移动平均线 SMA。"""
        return simple_moving_average(closes, period)

    def _reset_after_failure(self, state: PullbackShortState) -> None:
        """形态失败后的状态重置。

        重置内容：
        - 回到等待跌破 MA 状态；
        - 清除触碰 K 相关记录；
        - wait_bars 清零；
        - 要求下一次必须 full_break 才能重新确认跌破。
        """
        reset_after_failure(state)

    def _reset_after_signal_result(self, state: PullbackShortState) -> None:
        """信号成功/失败/超时结束后，回到等待下一次 MA 跌破。"""
        reset_after_signal_result(state)

    def _clear_signal(self, state: PullbackShortState) -> None:
        clear_signal(state)

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
            "signal_lowest_low": state.signal_lowest_low,
            "signal_no_new_low_bars": state.signal_no_new_low_bars,
            "signal_rising_low_bars": state.signal_rising_low_bars,
            "signal_ma20_touched": state.signal_ma20_touched,
            "signal_ma20_touch_low": state.signal_ma20_touch_low,
            "signal_entry_ma20_slope": state.signal_entry_ma20_slope,
            "signal_entry_ma60_slope": state.signal_entry_ma60_slope,
            "dynamic_stop_tr_avg": state.signal_stop_tr_avg,
        })
        return metrics

    def _evaluate_done_signal(
        self,
        request: RequestDict,
        state: PullbackShortState,
        open_price: float,
        high: float,
        low: float,
        close: float,
        ma20: float,
        ma_period: int,
        ma20_slope: float | None = None,
        ma60_slope: float | None = None,
        stop_tr_avg: float | None = None,
    ) -> ResponseDict:
        """在 DONE 状态下观察结构性止盈、止损/未决结果。"""
        bar = request.get("bar") or {}
        decision = evaluate_exit(
            state,
            PullbackKLine(
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=self._bar_volume(bar),
                open_interest=self._bar_open_interest(bar),
                event_time=_bar_event_time(bar, request.get("event_time", "")),
            ),
            ma20,
            self._exit_settings(request),
            current_ma20_slope=ma20_slope,
            current_ma60_slope=ma60_slope,
            stop_tr_avg=stop_tr_avg,
        )
        checks = self._exit_checks(request, state, decision, open_price, high, close, ma20)
        if decision.action == "exit_failure" and decision.reason == "dynamic stop zone hit":
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "failure"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "动态TR止损", 5, "failed", "dynamic stop zone hit", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "dynamic stop zone hit", metrics, trace)
        if decision.action == "exit_failure":
            reason = "market strengthened before short worked"
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "failure"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "盘面转强止损", 5, "failed", reason, checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, reason, metrics, trace)
        if decision.action == "exit_success":
            reason = "trend-following profit exit after MA reclaim"
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "success"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "结构止盈", 5, "passed", reason, checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, reason, metrics, trace)
        if decision.action == "exit_unresolved":
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "unresolved"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号观察结束", 5, "done", "observation window ended without profit/adverse target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "observation window ended without profit/adverse target", metrics, trace)
        metrics = self._signal_metrics(state, ma20, ma_period)
        trace = _trace(request, "bar", DONE, "持仓跟踪中", 5, "waiting", "holding short until structural exit", checks, metrics)
        return _no_signal(request, "holding short until structural exit", metrics, trace)

    def _ma_label(self, period: int) -> str:
        """根据均线周期生成展示标签，例如 period=20 时为 MA20。"""
        return f"MA{period}"

    def _trim_closes(self, state: PullbackShortState, ma_period: int, params: StrategySettings | None = None) -> None:
        """裁剪 closes 序列，避免历史收盘价无限增长。

        需要保留 ma_period + 1 个值，是因为：
        - ma_period 个值用于计算当前 MA；
        - 额外 1 个值有助于比较前一根 bar 的 close/MA 状态。
        动态止损还需要 MA60 斜率和最近 60 个 TR，因此保留更长的公共历史。
        """
        params = params or {}
        ma60_period = max(3, _int(params.get("ma60_period"), 60))
        slope_lookback = max(1, _int(params.get("slope_lookback_bars"), 5))
        stop_tr_lookback = max(1, _int(params.get("stop_tr_lookback"), 60))
        keep = max(ma_period + 1, ma60_period + slope_lookback + 1, stop_tr_lookback + 1, 2)
        state.closes = trim_tail(state.closes, keep)
        state.trs = trim_tail(state.trs, keep)

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
            "dynamic_stop_tr_avg": state.signal_stop_tr_avg,
            "signal_entry_ma20_slope": state.signal_entry_ma20_slope,
            "signal_entry_ma60_slope": state.signal_entry_ma60_slope,
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

    def _core_bar(self, ctx: PullbackBarContext) -> PullbackKLine:
        """将适配层上下文转换为 core 的最小 K 线结构。"""
        return PullbackKLine(
            open=ctx.open_price,
            high=ctx.high,
            low=ctx.low,
            close=ctx.close,
            volume=self._bar_volume(ctx.bar),
            open_interest=self._bar_open_interest(ctx.bar),
            event_time=ctx.event_time,
        )

    def _bar_volume(self, bar: BarDict) -> float:
        """读取 K 线成交量。

        当前行情字段使用 volume；若后续区分累计成交量/本根成交量，可在这里统一兼容。
        """
        return _float(bar.get("volume"), 0.0)

    def _bar_open_interest(self, bar: BarDict) -> float:
        """读取 K 线持仓量。

        标准字段为 open_interest，同时兼容前端或外部数据可能传入的 openInterest/position。
        """
        return _float(bar.get("open_interest", bar.get("openInterest", bar.get("position", 0.0))), 0.0)

    def _entry_decision(self, ctx: PullbackBarContext) -> PullbackDecision:
        """调用 core 开仓算法，返回机器可读的开仓阶段决策。"""
        return evaluate_setup(
            ctx.state,
            self._core_bar(ctx),
            ctx.ma20,
            ctx.previous_close,
            ctx.previous_ma,
            self._entry_settings(ctx.request),
            self._exit_settings(ctx.request),
            current_ma20_slope=ctx.ma20_slope,
            current_ma60_slope=ctx.ma60_slope,
        )

    def _setup_checks(self, ctx: PullbackBarContext, decision: PullbackDecision) -> list[CheckDict]:
        """把 core 开仓检查项转换成前端 trace 使用的检查列表。"""
        c = decision.checks
        if "full_above" in c:
            return [_check(f"整根K线站上{ctx.ma_label}", c["full_above"], ctx.close, ctx.ma20, ctx.close - ctx.ma20, f"等待出现 OHLC 全部在 {ctx.ma_label} 上方的K线")]
        if "touch_ma" in c:
            return [_check(f"最高价触碰{ctx.ma_label}", c["touch_ma"], ctx.high, ctx.ma20, ctx.high - ctx.ma20, f"等待反抽到{ctx.ma_label}附近")]
        if "broke_touch_open" in c:
            wait_remaining = ctx.max_wait_bars - ctx.state.wait_bars
            return [
                _check(f"未重新站上{ctx.ma_label}", c["not_stood_above"], ctx.close, ctx.ma20, ctx.close - ctx.ma20, "重新站上则形态失败"),
                _check("等待未超时", c["within_wait"], ctx.state.wait_bars, ctx.max_wait_bars, wait_remaining),
                _check("触碰K开盘价有效", c["has_touch_open"], ctx.state.touch_open, "not null"),
                _check("K线跌破触碰K开盘价", c["broke_touch_open"], ctx.low, ctx.state.touch_open),
            ]
        return [
            _check(f"开盘低于{ctx.ma_label}", c.get("open_below_ma", False), ctx.open_price, ctx.ma20, ctx.open_price - ctx.ma20),
            _check(f"收盘低于{ctx.ma_label}", c.get("close_below_ma", False), ctx.close, ctx.ma20, ctx.close - ctx.ma20),
            _check("从上向下跌破", c.get("cross_break", False), ctx.close, ctx.ma20, ctx.close - ctx.ma20),
        ]

    def _exit_checks(self, request: RequestDict, state: PullbackShortState, decision: PullbackDecision, open_price: float, high: float, close: float, ma20: float) -> list[CheckDict]:
        """把 core 平仓检查项转换成前端 trace 使用的检查列表。"""
        c = decision.checks
        values = decision.values
        settings = self._signal_settings(request)
        rebound = values.get("rebound")
        rebound_threshold = values.get("rebound_threshold")
        return [
            _check("亏损进入动态TR止损区", c.get("in_dynamic_stop_zone", False), values.get("adverse_distance"), values.get("dynamic_stop_threshold")),
            _check("MA20斜率比下单点更向下", c.get("ma20_more_bearish_than_entry", False), values.get("current_ma20_slope"), values.get("entry_ma20_slope")),
            _check("MA60斜率比下单点更向下", c.get("ma60_more_bearish_than_entry", False), values.get("current_ma60_slope"), values.get("entry_ma60_slope")),
            _check("动态止损触发", c.get("hit_adverse", False), close, state.signal_entry),
            _check("亏损后连续未创新低", c.get("no_new_low_stop", False), state.signal_no_new_low_bars, settings["strength_exit_bars"]),
            _check("亏损后重新站上MA", c.get("stood_above_stop", False), close, ma20),
            _check("盈利后触碰MA", c.get("ma_touched", False), high, ma20),
            _check("触碰MA后反弹幅度达标", c.get("rebound_ok", False), rebound, rebound_threshold),
            _check("触碰MA后低点上移", c.get("rising_low_take", False), state.signal_rising_low_bars, settings["profit_rising_low_bars"]),
            _check("强阳站稳MA", c.get("strong_bull_take", False), close - open_price, settings["strong_bull_atr_multiple"]),
            _check("观察窗口未结束", c.get("within_observation", False), state.signal_bars, settings["observation_bars"]),
        ]

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
            return _no_signal(request, "no bar", None, None)
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
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state, state.prev_ma, ma_period), None, False)
        if event_ts is not None:
            state.last_bar_ts = event_ts

        previous_ma = self._ma(state.closes, ma_period)
        previous_close = state.prev_close
        state.closes.append(close)
        signal_settings = self._signal_settings(request)
        state.trs.append(true_range(high, low, previous_close))
        self._trim_closes(state, ma_period, signal_settings)
        ma20 = self._ma(state.closes, ma_period)

        if ma20 is None:
            state.prev_close = close
            state.prev_ma = ma20
            checks = [
                _check("MA样本数量", len(state.closes) >= ma_period, len(state.closes), ma_period, len(state.closes) - ma_period, "等待收集足够K线计算均线")
            ]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", f"等待 {ma_label} 数据足够", 1, "waiting", "waiting for enough bars", checks, ma20, ma_period)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ma20, ma_period), trace, False)

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
            ma60=self._ma(state.closes, signal_settings["ma60_period"]),
            ma20_slope=moving_average_slope(state.closes, 20, signal_settings["slope_lookback_bars"]),
            ma60_slope=moving_average_slope(state.closes, signal_settings["ma60_period"], signal_settings["slope_lookback_bars"]),
            stop_tr_avg=trimmed_top_average(
                state.trs,
                signal_settings["stop_tr_lookback"],
                signal_settings["stop_tr_top_n"],
                signal_settings["stop_tr_drop_n"],
            ),
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
            out = _no_signal(request, "unknown state reset", self._base_metrics(state, ma20, ma_period), None, False)
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
        decision = self._entry_decision(ctx)
        checks = self._setup_checks(ctx, decision)
        if "full_above" in decision.checks:
            trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_BELOW_MA20, f"等待先完整站上 {ctx.ma_label}", 1, "waiting", "waiting for first full bar above MA", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, "waiting for first full bar above MA", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)
        if decision.action == "setup_broken":
            trace = self._bar_trace(ctx.request, ctx.state, BROKEN_BELOW_MA20, f"已跌破，等待反抽触碰 {ctx.ma_label}", 2, "passed", f"break below {ctx.ma_label} confirmed", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"break below {ctx.ma_label} confirmed", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)
        trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "waiting", "no trade signal", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "no trade signal", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)

    def _handle_broken_below_ma20(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 2：跌破后等待反抽触碰 MA。

        触碰条件使用 high >= MA，而不是 close >= MA：
        反抽做空关注的是价格曾经回抽到均线压力附近，哪怕收盘又回落，也说明压力位被测试过。
        """
        decision = self._entry_decision(ctx)
        checks = self._setup_checks(ctx, decision)
        if decision.action == "touch_found":
            trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_TOUCH_OPEN, "等待跌破触碰K开盘价", 3, "passed", f"{ctx.ma_label} touch bar found", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"{ctx.ma_label} touch bar found", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx.request, ctx.state, BROKEN_BELOW_MA20, f"已跌破，等待反抽触碰 {ctx.ma_label}", 2, "waiting", "waiting for MA touch", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "waiting for MA touch", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)

    def _handle_wait_break_touch_open(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 3：触碰 MA 后等待跌破触碰 K 开盘价。

        触碰 K 的开盘价是这套 baseline 提示词中的“回抽失败确认线”。
        后续 bar 的 low 跌破该价时，说明反抽后买盘没有延续，才输出 SHORT。
        """
        touch_open = ctx.state.touch_open
        touch_high = ctx.state.touch_high
        touch_ma20 = ctx.state.touch_ma20
        touch_time = ctx.state.touch_time
        decision = self._entry_decision(ctx)
        checks = self._setup_checks(ctx, decision)
        if decision.action == "enter_short":
            metrics = self._signal_metrics(ctx.state, ctx.ma20, ctx.ma_period)
            metrics.update({
                "signal": "SHORT",
                "touch_open": touch_open,
                "touch_high": touch_high,
                "touch_ma20": touch_ma20,
                "touch_time": touch_time,
                "trigger_price": decision.values.get("trigger_price"),
                "entry_price": decision.values.get("entry_price"),
                "atr": decision.values.get("atr"),
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
        if decision.action == "reset" and decision.reason == "stood above MA":
            trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "failed", f"reset: stood above {ctx.ma_label}", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, f"reset: stood above {ctx.ma_label}", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        if decision.action == "reset":
            trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ctx.ma_label}", 1, "failed", "reset: wait bars exceeded", checks, ctx.ma20, ctx.ma_period)
            return _no_signal(ctx.request, "reset: wait bars exceeded", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx.request, ctx.state, WAIT_BREAK_TOUCH_OPEN, "等待跌破触碰K开盘价", 3, "waiting", "waiting for touch open break", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "waiting for touch open break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)

    def _handle_signal_active(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 4：信号触发后的下一根 bar 开盘确认入场。

        这样做是为了避免在同一根触发 bar 内同时完成“触发”和“观察结果”。
        回测中用下一根开盘价作为持仓上下文，能更接近实际下单延迟，也让止盈/止损观察从持仓后开始。
        """
        self._entry_decision(ctx)
        metrics = self._signal_metrics(ctx.state, ctx.ma20, ctx.ma_period)
        metrics["signal"] = "SHORT"
        checks = [_check("下一根K线开盘确认持仓", True, ctx.open_price, "entry open")]
        trace = self._bar_trace(ctx.request, ctx.state, DONE, "已持仓，等待止盈/止损", 5, "passed", "short position confirmed at next bar open", checks, ctx.ma20, ctx.ma_period)
        return _no_signal(ctx.request, "short position confirmed at next bar open", metrics, trace)

    def _handle_done(self, ctx: PullbackBarContext) -> ResponseDict:
        """状态 5：持仓观察，继续判断止盈/止损/未决。"""
        return self._evaluate_done_signal(
            ctx.request,
            ctx.state,
            ctx.open_price,
            ctx.high,
            ctx.low,
            ctx.close,
            ctx.ma20,
            ctx.ma_period,
            ma20_slope=ctx.ma20_slope,
            ma60_slope=ctx.ma60_slope,
            stop_tr_avg=ctx.stop_tr_avg,
        )

    def on_tick(self, request: RequestDict) -> ResponseDict:
        """baseline 策略只由 bar 驱动，tick 不参与入场。"""
        with self._lock:
            state = self._state_for(request)
            ma_period, _ = self._settings(request)
            return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state, None, ma_period))

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        """回放 K 线事件复用实时 K 线逻辑。"""
        return self.on_bar(request)


class MA20WeakPullbackBaselineStrategy(MA20PullbackShortStrategy):
    """页面中的 `MA20 Weak Pullback Baseline` 可选择策略类。

    这个类显式存在，是为了让页面策略项和代码类一一对应：
    用户看到 baseline，就能在代码里找到 baseline class。

    它继承 `MA20PullbackShortStrategy`，而不是弱反弹 hard/score 的结构过滤状态机，
    是因为 baseline 的定义就是“跌破 MA -> 反抽触碰 MA -> 跌破触碰 K 开盘价”，
    不包含 MA60/MA120 结构过滤、reaction_low 或评分闸门。
    """

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        if definition is None:
            definition = dict(MA20PullbackShortStrategy.definition)
            definition.update({
                # baseline 策略唯一 ID：弱反弹策略族中用于选择 baseline 算法。
                "strategy_id": MA20_WEAK_BASELINE_STRATEGY_ID,
                # 前端展示名称：用于策略列表和回放结果展示。
                "display_name": "MA20 Weak Pullback Baseline",
                # baseline 入口脚本：保持页面策略项和代码入口对应。
                "entry_script": "python/ma20_weak_pullback_baseline.py",
                # baseline 默认参数：继承 MA20 反抽做空逻辑，并额外标识 algorithm。
                "default_params": {
                    # 均线周期：baseline 默认观察 MA20。
                    "ma_period": 20,
                    # 动态止损使用的长均线和斜率回看。
                    "ma60_period": 60,
                    "slope_lookback_bars": 5,
                    # 动态止损 TR 口径：最近60个TR中取最大10个，去掉最大2个后8个求平均。
                    "stop_tr_lookback": 60,
                    "stop_tr_top_n": 10,
                    "stop_tr_drop_n": 2,
                    # 反抽触碰均线后最多等待多少根 K 线跌破触碰 K 开盘价。
                    "max_wait_bars": 6,
                    # 平仓观察窗口。
                    "observation_bars": 240,
                    # 兼容旧参数：固定 ATR 止盈倍数，目前只用于诊断字段。
                    "profit_atr_multiple": 1.0,
                    # 固定止损 ATR 倍数。
                    "adverse_atr_multiple": 0.8,
                    # 亏损后连续未创新低的根数阈值。
                    "strength_exit_bars": 3,
                    # 盈利后触碰均线，从最低点反弹多少 ATR 才允许止盈。
                    "profit_rebound_atr_multiple": 1.0,
                    # 触碰均线后连续低点上移多少根，确认结构止盈。
                    "profit_rising_low_bars": 2,
                    # 强阳完全站稳均线的实体 ATR 阈值。
                    "strong_bull_atr_multiple": 1.5,
                    # 算法标识：回测或前端区分 baseline/hard_filter/score_filter。
                    "algorithm": "baseline",
                },
                # 策略定义更新时间：Go 侧按 RFC3339 解析。
                "updated_at": _rfc3339_utc_now(),
            })
        super().__init__(definition=definition)


# 兼容旧入口文件和旧导入名。新代码应使用 MA20WeakPullbackBaselineStrategy，
# 这样类名和页面上的策略名称保持一致。
MA20WeakBaselineStrategy = MA20WeakPullbackBaselineStrategy
