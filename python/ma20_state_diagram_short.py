# -*- coding: utf-8 -*-
"""MA20 状态图做空策略。

该策略按用户给出的状态图显式推进状态：
INIT -> ABOVE_MA20 -> BELOW_MA20 -> REBOUND_TO_HIGH -> SECOND_BREAK_SIGNAL
-> PROFIT_HOLDING / LOSS_HOLDING，最终以 TAKE_PROFIT 或 STOP_LOSS trace
结束一轮并回到 INIT。
"""

from __future__ import annotations

import json
import logging
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
    _params,
    _require_fields,
    _rfc3339_utc_now,
    _strategy_params_for_instance,
    _trace,
)
from strategy_indicators import moving_average_slope, simple_moving_average, trim_tail, trimmed_top_average, true_range
from strategy_types import (
    ABOVE_MA20,
    BELOW_MA20,
    INIT,
    LOSS_HOLDING,
    MA20_STATE_DIAGRAM_STRATEGY_ID,
    PROFIT_HOLDING,
    REBOUND_TO_HIGH,
    SECOND_BREAK_SIGNAL,
    STOP_LOSS,
    TAKE_PROFIT,
    BarDict,
    CheckDict,
    JSONObject,
    MetricsDict,
    RequestDict,
    ResponseDict,
    StateDiagramShortStateName,
    StateKey,
    StrategyDefinition,
)

logger = logging.getLogger(__name__)


@dataclass
class StateDiagramShortState:  # 单个策略实例和合约的状态机运行状态
    state: StateDiagramShortStateName = INIT  # 当前状态机节点
    closes: list[float] = field(default_factory=list)  # 收盘价缓存
    trs: list[float] = field(default_factory=list)  # 真实波幅缓存
    bar_index: int = 0  # 下一根可接受K线的0基序号，用于和独立指标结构点对齐
    current_bar_index: int | None = None  # 最近一根已处理K线的0基序号
    last_bar_ts: float | None = None  # 最近已处理K线时间戳
    prev_close: float | None = None  # 上一根K线收盘价
    prev_ma: float | None = None  # 上一根K线对应MA值
    above_ready: bool = False  # 兼容旧metrics：是否已从INIT进入MA20上方状态
    reclaim_above_bars: int = 0  # 连续站稳MA20的K线数
    touch_open: float | None = None  # 触碰MA20那根K线的开盘价
    touch_close: float | None = None  # 触碰MA20那根K线的收盘价
    touch_high: float | None = None  # 触碰MA20那根K线的最高价
    touch_ma20: float | None = None  # 触碰时的MA20值
    touch_time: str = ""  # 触碰MA20的K线时间
    trigger_line: float | None = None  # 二次跌破触发线
    wait_bars: int = 0  # 等待二次跌破的K线数
    signal_entry: float | None = None  # 空头持仓确认入场价
    signal_trigger_price: float | None = None  # 空头信号触发价
    signal_atr: float | None = None  # 信号K线波幅
    signal_adverse_target: float | None = None  # 逆向止损参考价
    signal_time: str = ""  # 空头信号触发时间
    signal_bar_index: int | None = None  # 空头信号触发K线序号，用于判断后续ZigZag波谷
    signal_bars: int = 0  # 信号后观察K线数
    signal_lowest_low: float | None = None  # 信号后最低价
    signal_prev_low: float | None = None  # 上一根持仓K线低点
    signal_no_new_low_bars: int = 0  # 连续未创新低K线数
    signal_rising_low_bars: int = 0  # 连续低点上移K线数
    signal_ma20_touched: bool = False  # 持仓后是否触碰MA20
    signal_entry_ma20_slope: float | None = None  # 入场时MA20斜率
    signal_entry_ma60_slope: float | None = None  # 入场时MA60斜率
    signal_stop_tr_avg: float | None = None  # 当前动态止损波幅均值
    zigzag_recent_peaks: list[dict[str, Any]] = field(default_factory=list)  # 最近ZigZag波峰快照
    zigzag_peak_distances: list[int] = field(default_factory=list)  # 最近波峰到当前K线的距离
    zigzag_recent_troughs: list[dict[str, Any]] = field(default_factory=list)  # 最近ZigZag波谷快照
    zigzag_trough_distances: list[int] = field(default_factory=list)  # 最近波谷到当前K线的距离


@dataclass
class StateDiagramBarContext:  # 单根K线处理时的只读上下文
    request: RequestDict  # 原始策略请求
    state: StateDiagramShortState  # 当前合约状态对象
    bar: BarDict  # 当前K线数据
    ma_period: int  # MA20计算周期
    max_wait_bars: int  # 二次跌破最大等待K线数
    reclaim_confirm_bars: int  # 连续站稳MA20确认K线数
    open_price: float  # 当前K线开盘价
    high: float  # 当前K线最高价
    low: float  # 当前K线最低价
    close: float  # 当前K线收盘价
    event_time: str  # 当前K线事件时间
    previous_ma: float | None  # 处理当前K线前的MA值
    previous_close: float | None  # 处理当前K线前的收盘价
    ma20: float  # 当前MA20值
    ma60: float | None  # 当前MA60值
    ma20_slope: float | None  # 当前MA20斜率
    ma60_slope: float | None  # 当前MA60斜率
    atr26: float | None  # 当前ATR(26)
    stop_tr_avg: float | None  # 动态止损使用的波幅均值


class MA20StateDiagramShortStrategy(Strategy):  # MA20状态图做空策略实现
    definition: StrategyDefinition = {  # 策略元数据和默认参数
        "strategy_id": MA20_STATE_DIAGRAM_STRATEGY_ID,
        "display_name": "MA20 State Diagram Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {
            "ma_period": 20,
            "ma60_period": 60,
            "slope_lookback_bars": 5,
            "stop_tr_lookback": 60,
            "stop_tr_top_n": 10,
            "stop_tr_drop_n": 2,
            "take_profit_atr_period": 26,
            "max_wait_bars": 6,
            "reclaim_confirm_bars": 2,
            "observation_bars": 240,
            "adverse_atr_multiple": 0.8,
            "strength_exit_bars": 3,
            "profit_rebound_atr_multiple": 1.0,
            "profit_rising_low_bars": 2,
            "strong_bull_atr_multiple": 1.5,
            "use_zigzag_trough_take_profit": True,
            "prefer_zigzag_trough_take_profit": True,
            "entry_ma20_slope_max": 1000000000.0,
            "entry_ma60_slope_max": 1000000000.0,
            "entry_zigzag_peak_min_bars": 0,
            "entry_zigzag_peak_max_bars": 1000000000,
        },
        "updated_at": _rfc3339_utc_now(),
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        # 初始化策略定义、状态表和并发锁。
        if definition is not None:
            self.definition = definition
        self.states: dict[StateKey, StateDiagramShortState] = {}  # 按模式、实例、合约索引的状态表
        self._lock: RLock = RLock()  # 保护状态表和状态推进的可重入锁

    def required_warmup_bars(self, instance: JSONObject) -> int:
        # 返回实例启动前至少需要预热的K线数量，保证MA计算有足够样本。
        params = _strategy_params_for_instance(self.definition, instance)
        required = max(
            2,
            _int(params.get("ma_period"), 20),
            _int(params.get("ma60_period"), 60),
            _int(params.get("take_profit_atr_period"), 26),
        )
        requested = _int(params.get("warmup_target"), 0)
        return max(required, requested)

    def start_instance(self, instance: JSONObject) -> None:
        # 启动策略实例，为每个合约创建独立状态，并应用预热K线。
        logger.info("start_state_diagram_instance args=%s", json.dumps(_instance_start_log_payload(instance), ensure_ascii=False, sort_keys=True))
        instance_id = str(instance.get("instance_id") or "")
        symbols = instance.get("symbols") or [""]
        mode = _instance_mode(instance)
        with self._lock:
            for symbol in symbols:
                self.states[(mode, instance_id, str(symbol))] = StateDiagramShortState()
        counts = self._apply_warmup_bars(instance, (instance.get("params") or {}).get("warmup_bars") or [], mode)
        self.validate_warmup(instance, counts)

    def stop_instance(self, instance_id: str) -> None:
        # 停止策略实例时清理该实例的所有合约状态。
        with self._lock:
            for key in list(self.states):
                if key[1] == instance_id:
                    del self.states[key]

    def validate_warmup(self, instance: JSONObject, applied_counts: dict[str, int] | None = None) -> None:
        # 校验预热数据是否足以计算MA，避免运行期第一批K线因样本不足误判。
        target = self.required_warmup_bars(instance)
        if target <= 0:
            return
        params = _strategy_params_for_instance(self.definition, instance)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        ma60_period = max(3, _int(params.get("ma60_period"), 60))
        take_profit_atr_period = max(1, _int(params.get("take_profit_atr_period"), 26))
        mode = _instance_mode(instance)
        instance_id = str(instance.get("instance_id") or "")
        symbols = instance.get("symbols") or [""]
        applied_counts = applied_counts or {}
        missing: list[str] = []
        with self._lock:
            for symbol in symbols:
                state = self.states.get((mode, instance_id, str(symbol)))
                ma_value = simple_moving_average(state.closes if state else [], ma_period)
                ma60_value = simple_moving_average(state.closes if state else [], ma60_period)
                atr_value = simple_moving_average(state.trs if state else [], take_profit_atr_period)
                applied = _int(applied_counts.get(str(symbol)), 0)
                if state is None or ma_value is None or ma60_value is None or atr_value is None or applied < target:
                    missing.append(f"{symbol}:{applied}/{target}")
        if missing:
            raise ValueError(
                "strategy warmup is insufficient for MA/ATR calculation: "
                f"instance_id={instance_id} strategy_id={self.definition.get('strategy_id', '')} "
                f"mode={mode} ma_period={ma_period} ma60_period={ma60_period} "
                f"take_profit_atr_period={take_profit_atr_period} warmup_target={target} symbols={','.join(missing)}"
            )

    def _state_for(self, request: RequestDict) -> StateDiagramShortState:
        # 根据请求中的模式、实例和合约定位运行状态。
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            raise ValueError(
                "strategy runtime instance not started: "
                f"instance_id={key[1]} mode={key[0]} symbol={key[2]} strategy_id={self.definition.get('strategy_id', '')}"
            )
        return self.states[key]

    def _apply_warmup_bars(self, instance: JSONObject, bars: Any, mode: str) -> dict[str, int]:
        # 将历史K线灌入状态缓存，只更新指标样本，不触发交易信号。
        counts: dict[str, int] = {}
        if not isinstance(bars, list) or not bars:
            return counts
        params = _strategy_params_for_instance(self.definition, instance)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        signal_settings = self._signal_settings_from_params(params)
        target = self.required_warmup_bars(instance)
        default_symbol = str((instance.get("symbols") or [""])[0])
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            symbol = str(bar.get("symbol") or default_symbol)
            if target > 0 and counts.get(symbol, 0) >= target:
                continue
            state = self.states.get((mode, str(instance.get("instance_id") or ""), symbol))
            if state is None:
                continue
            close = _float(bar.get("close"), None)
            if close is None:
                continue
            event_ts = _event_ts(_bar_event_time(bar))
            if event_ts is not None and state.last_bar_ts is not None and event_ts <= state.last_bar_ts:
                continue
            previous_ma = simple_moving_average(state.closes, ma_period)
            high = _float(bar.get("high", close), close)
            low = _float(bar.get("low", close), close)
            previous_close = state.prev_close
            state.closes.append(close)
            state.trs.append(true_range(high, low, previous_close))
            self._trim(state, ma_period, signal_settings)
            state.prev_close = close
            state.prev_ma = previous_ma
            if event_ts is not None:
                state.last_bar_ts = event_ts
            state.current_bar_index = state.bar_index
            state.bar_index += 1
            # 预热结束后从初始观察状态开始，等待完整站上MA20。
            state.state = INIT
            state.above_ready = False
            state.reclaim_above_bars = 0
            counts[symbol] = counts.get(symbol, 0) + 1
        return counts

    def _settings(self, request: RequestDict) -> tuple[int, int, int]:
        # 合并默认参数和请求参数，提取状态图主流程使用的基础参数。
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return (
            max(2, _int(params.get("ma_period"), 20)),
            max(1, _int(params.get("max_wait_bars"), 6)),
            max(1, _int(params.get("reclaim_confirm_bars"), 2)),
        )

    def _signal_settings(self, request: RequestDict) -> dict[str, Any]:
        # 合并参数后生成信号确认、止盈止损和观察窗口使用的参数。
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return self._signal_settings_from_params(params)

    def _signal_settings_from_params(self, params: dict[str, Any]) -> dict[str, Any]:
        # 规范化信号参数下限，避免0或负数导致指标、阈值计算异常。
        return {
            "ma60_period": max(3, _int(params.get("ma60_period"), 60)),
            "slope_lookback_bars": max(1, _int(params.get("slope_lookback_bars"), 5)),
            "stop_tr_lookback": max(1, _int(params.get("stop_tr_lookback"), 60)),
            "stop_tr_top_n": max(1, _int(params.get("stop_tr_top_n"), 10)),
            "stop_tr_drop_n": max(0, _int(params.get("stop_tr_drop_n"), 2)),
            "take_profit_atr_period": max(1, _int(params.get("take_profit_atr_period"), 26)),
            "observation_bars": max(1, _int(params.get("observation_bars"), 240)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
            "strength_exit_bars": max(1, _int(params.get("strength_exit_bars"), 3)),
            "profit_rebound_atr_multiple": max(0.0001, _float(params.get("profit_rebound_atr_multiple"), 1.0)),
            "profit_rising_low_bars": max(1, _int(params.get("profit_rising_low_bars"), 2)),
            "strong_bull_atr_multiple": max(0.0001, _float(params.get("strong_bull_atr_multiple"), 1.5)),
            "use_zigzag_trough_take_profit": bool(params.get("use_zigzag_trough_take_profit", True)),
            "prefer_zigzag_trough_take_profit": bool(params.get("prefer_zigzag_trough_take_profit", True)),
            "entry_ma20_slope_max": _float(params.get("entry_ma20_slope_max"), 1000000000.0),
            "entry_ma60_slope_max": _float(params.get("entry_ma60_slope_max"), 1000000000.0),
            "entry_zigzag_peak_min_bars": max(0, _int(params.get("entry_zigzag_peak_min_bars"), 0)),
            "entry_zigzag_peak_max_bars": max(0, _int(params.get("entry_zigzag_peak_max_bars"), 1000000000)),
        }

    def _trim(self, state: StateDiagramShortState, ma_period: int, settings: dict[str, Any]) -> None:
        # 裁剪指标缓存，只保留后续MA、斜率和波幅计算需要的最近样本。
        keep = max(ma_period, settings["ma60_period"] + settings["slope_lookback_bars"], settings["stop_tr_lookback"], settings["take_profit_atr_period"]) + 5
        state.closes = trim_tail(state.closes, keep)
        state.trs = trim_tail(state.trs, keep)

    def _base_metrics(
        self,
        state: StateDiagramShortState,
        ma20: float | None = None,
        ma_period: int = 20,
        ma60: float | None = None,
    ) -> MetricsDict:
        # 构造统一返回的指标快照，供无信号、trace和信号响应复用。
        return {
            "signal": "",
            "state": state.state,
            "step_total": 8,
            "ma": ma20,
            "ma20": ma20,
            "ma60": ma60,
            "ma_period": ma_period,
            "above_ready": state.above_ready,
            "reclaim_above_bars": state.reclaim_above_bars,
            "touch_open": state.touch_open,
            "touch_close": state.touch_close,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "trigger_line": state.trigger_line,
            "wait_bars": state.wait_bars,
            "signal_entry": state.signal_entry,
            "signal_bar_index": state.signal_bar_index,
            "signal_trigger_price": state.signal_trigger_price,
            "signal_atr": state.signal_atr,
            "signal_adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_lowest_low": state.signal_lowest_low,
            "signal_no_new_low_bars": state.signal_no_new_low_bars,
            "signal_rising_low_bars": state.signal_rising_low_bars,
            "signal_ma20_touched": state.signal_ma20_touched,
            "signal_stop_tr_avg": state.signal_stop_tr_avg,
            "atr26": simple_moving_average(state.trs, 26),
            "bar_index": state.current_bar_index,
            "zigzag_recent_peaks": state.zigzag_recent_peaks,
            "zigzag_peak_distances": state.zigzag_peak_distances,
            "zigzag_recent_troughs": state.zigzag_recent_troughs,
            "zigzag_trough_distances": state.zigzag_trough_distances,
        }

    def _update_zigzag_features(self, state: StateDiagramShortState, request: RequestDict) -> None:
        # ZigZag 是独立指标；这里只消费 Go 注入的只读快照，不读取其它策略内存状态。
        features = request.get("features") or {}
        if not isinstance(features, dict):
            state.zigzag_recent_peaks = []
            state.zigzag_peak_distances = []
            state.zigzag_recent_troughs = []
            state.zigzag_trough_distances = []
            return
        zigzag = features.get("zigzag_atr26") or {}
        peaks = zigzag.get("peaks") if isinstance(zigzag, dict) else []
        troughs = zigzag.get("troughs") if isinstance(zigzag, dict) else []
        if state.current_bar_index is None:
            state.zigzag_recent_peaks = []
            state.zigzag_peak_distances = []
            state.zigzag_recent_troughs = []
            state.zigzag_trough_distances = []
            return
        if not isinstance(peaks, list):
            peaks = []
        if not isinstance(troughs, list):
            troughs = []
        state.zigzag_recent_peaks, state.zigzag_peak_distances = self._normalize_zigzag_points(peaks, state.current_bar_index)
        state.zigzag_recent_troughs, state.zigzag_trough_distances = self._normalize_zigzag_points(troughs, state.current_bar_index)

    def _normalize_zigzag_points(self, items: list[Any], current_bar_index: int) -> tuple[list[dict[str, Any]], list[int]]:
        recent: list[dict[str, Any]] = []
        distances: list[int] = []
        for item in items[:3]:
            if not isinstance(item, dict):
                continue
            try:
                pivot_index = int(item.get("pivot_index"))
            except (TypeError, ValueError):
                continue
            distance = int(current_bar_index) - pivot_index
            peak = {
                "pivot_index": pivot_index,
                "pivot_time": item.get("pivot_time"),
                "pivot_price": item.get("pivot_price"),
                "confirmed_time": item.get("confirmed_time"),
                "confirmed_index": item.get("confirmed_index"),
                "bars_from_peak": distance,
            }
            recent.append(peak)
            distances.append(distance)
        return recent, distances

    def _bar_trace(self, ctx: StateDiagramBarContext, step_key: str, label: str, index: int, status: str, reason: str, checks: list[CheckDict]) -> dict[str, Any]:
        # 生成单根K线在状态图中的执行轨迹。
        return _trace(ctx.request, "bar", step_key, label, index, status, reason, checks, self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60))

    def _bar_check(self, ctx: StateDiagramBarContext, name: str, passed: Any, current: Any = None, target: Any = None, delta: Any = None) -> CheckDict:
        # 只在明确当前K线所处状态后创建检查项；_check 会立即写日志。
        return _check(name, passed, current, target, delta, "dt:{}".format(ctx.event_time[:-6]))

    def _transition_check(self, ctx: StateDiagramBarContext, from_state: str, to_state: str, reason: str) -> CheckDict:
        # 状态改变时额外记录一条切换信息，避免普通等待K线刷出所有候选条件。
        return _check("状态切换", True, from_state, to_state, None, "{}; dt:{}".format(reason, ctx.event_time))

    def _reset_setup(self, state: StateDiagramShortState, keep_above_ready: bool = False) -> None:
        # 重置建仓前状态，清空触碰和信号信息，准备下一轮状态图识别。
        state.state = INIT
        state.above_ready = keep_above_ready
        state.reclaim_above_bars = 0
        state.touch_open = None
        state.touch_close = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.trigger_line = None
        state.wait_bars = 0
        self._clear_signal(state)

    def _clear_signal(self, state: StateDiagramShortState) -> None:
        # 清空信号后持仓观察相关字段。
        state.signal_entry = None
        state.signal_bar_index = None
        state.signal_trigger_price = None
        state.signal_atr = None
        state.signal_adverse_target = None
        state.signal_time = ""
        state.signal_bars = 0
        state.signal_lowest_low = None
        state.signal_prev_low = None
        state.signal_no_new_low_bars = 0
        state.signal_rising_low_bars = 0
        state.signal_ma20_touched = False
        state.signal_entry_ma20_slope = None
        state.signal_entry_ma60_slope = None
        state.signal_stop_tr_avg = None

    def on_bar(self, request: RequestDict) -> ResponseDict:
        # K线入口，加锁后推进状态机。
        with self._lock:
            return self._on_bar_locked(request)

    def _on_bar_locked(self, request: RequestDict) -> ResponseDict:
        # 处理单根K线：校验、更新指标、构建上下文，并按当前状态分派处理函数。
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        _require_fields(bar, ("open", "high", "low", "close"))
        state = self._state_for(request)
        ma_period, max_wait_bars, reclaim_confirm_bars = self._settings(request)
        settings = self._signal_settings(request)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        close = _float(bar.get("close"))
        event_time = _bar_event_time(bar, request.get("event_time", ""))
        event_ts = _event_ts(event_time)
        if event_ts is not None and state.last_bar_ts is not None and event_ts <= state.last_bar_ts:
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state, state.prev_ma, ma_period), None, False)
        if event_ts is not None:
            state.last_bar_ts = event_ts
        state.current_bar_index = state.bar_index
        state.bar_index += 1
        self._update_zigzag_features(state, request)

        # 先用旧缓存计算上一根MA，再追加当前K线，便于判断是否从上向下跌破MA20。
        previous_ma = simple_moving_average(state.closes, ma_period)
        previous_close = state.prev_close
        state.closes.append(close)
        state.trs.append(true_range(high, low, previous_close))
        self._trim(state, ma_period, settings)
        ma20 = simple_moving_average(state.closes, ma_period)
        if ma20 is None:
            state.prev_close = close
            state.prev_ma = ma20
            checks = [_check("MA样本数量", len(state.closes) >= ma_period, len(state.closes), ma_period)]
            trace = _trace(request, "bar", "WAIT_MA_READY", "等待 MA20 数据足够", 1, "waiting", "waiting for enough bars", checks, self._base_metrics(state, ma20, ma_period))
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ma20, ma_period), trace, False)

        # 汇总当前K线、均线、斜率和动态止损波幅，后续状态处理只读这个上下文。
        ctx = StateDiagramBarContext(
            request=request,
            state=state,
            bar=bar,
            ma_period=ma_period,
            max_wait_bars=max_wait_bars,
            reclaim_confirm_bars=reclaim_confirm_bars,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            event_time=event_time,
            previous_ma=previous_ma,
            previous_close=previous_close,
            ma20=ma20,
            ma60=simple_moving_average(state.closes, settings["ma60_period"]),
            ma20_slope=moving_average_slope(state.closes, ma_period, settings["slope_lookback_bars"]),
            ma60_slope=moving_average_slope(state.closes, settings["ma60_period"], settings["slope_lookback_bars"]),
            atr26=simple_moving_average(state.trs, settings["take_profit_atr_period"]),
            stop_tr_avg=trimmed_top_average(state.trs, settings["stop_tr_lookback"], settings["stop_tr_top_n"], settings["stop_tr_drop_n"]),
        )
        # 当前状态决定使用哪段状态图逻辑；持仓盈利和亏损共用持仓处理函数。
        handlers = {
            INIT: self._handle_init,
            ABOVE_MA20: self._handle_above_ma20,
            BELOW_MA20: self._handle_below_ma20,
            REBOUND_TO_HIGH: self._handle_rebound_to_high,
            SECOND_BREAK_SIGNAL: self._handle_second_break_signal,
            PROFIT_HOLDING: self._handle_holding,
            LOSS_HOLDING: self._handle_holding,
            TAKE_PROFIT: self._handle_terminal_reset,
            STOP_LOSS: self._handle_terminal_reset,
        }
        handler = handlers.get(state.state)
        if handler is None:
            self._reset_setup(state, keep_above_ready=False)
            out = _no_signal(request, "unknown state reset", self._base_metrics(state, ma20, ma_period), None, False)
        else:
            out = handler(ctx)
        state.prev_close = close
        state.prev_ma = ma20
        return out

    def _handle_init(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 状态1：初始待机，只有收盘价站上MA20才进入基准状态。
        above = ctx.close > ctx.ma20
        if above:
            checks = [
                self._bar_check(ctx, "收盘价站上MA20", True, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
                self._transition_check(ctx, INIT, ABOVE_MA20, "close above MA20 baseline ready"),
            ]
            ctx.state.state = ABOVE_MA20
            ctx.state.above_ready = True
            ctx.state.reclaim_above_bars = 0
            trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 2, "passed", "close above MA20 baseline ready", checks)
            return _no_signal(ctx.request, "close above MA20 baseline ready", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)
        checks = [self._bar_check(ctx, "收盘价仍在MA20下方", True, ctx.close, ctx.ma20, ctx.close - ctx.ma20)]
        trace = self._bar_trace(ctx, INIT, "初始待机", 1, "waiting", "waiting for close above MA20", checks)
        return _no_signal(ctx.request, "waiting for close above MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)

    def _handle_terminal_reset(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 防御性处理：终态不应跨K线持久存在；若遇到则直接回到INIT。
        previous = ctx.state.state
        self._reset_setup(ctx.state)
        checks = [self._transition_check(ctx, previous, INIT, "terminal state reset")]
        trace = self._bar_trace(ctx, INIT, "初始待机", 1, "done", "terminal state reset", checks)
        return _no_signal(ctx.request, "terminal state reset", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)

    def _handle_above_ma20(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 状态2：已经具备MA20上方基准，等待收盘从上向下跌破MA20。
        cross_break = ctx.previous_close is not None and ctx.previous_ma is not None and ctx.previous_close >= ctx.previous_ma and ctx.close < ctx.ma20
        if cross_break:
            # 第一次跌破成立，进入MA20下方等待反弹触碰阶段。
            checks = [
                self._bar_check(ctx, "收盘从上向下跌破MA20", cross_break, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
                self._transition_check(ctx, ABOVE_MA20, BELOW_MA20, "break below MA20 confirmed"),
            ]
            ctx.state.state = BELOW_MA20
            ctx.state.reclaim_above_bars = 0
            trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 3, "passed", "break below MA20 confirmed", checks)
            return _no_signal(ctx.request, "break below MA20 confirmed", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)
        checks = [self._bar_check(ctx, "等待收盘跌破MA20", ctx.close >= ctx.ma20, ctx.close, ctx.ma20, ctx.close - ctx.ma20)]
        trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 2, "waiting", "waiting above MA20 break", checks)
        return _no_signal(ctx.request, "waiting above MA20 break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)

    def _handle_below_ma20(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 状态3：跌破后等待反弹触碰MA20；若连续站稳MA20则重置到基准状态。
        stood_above = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        touched = ctx.high >= ctx.ma20
        if stood_above:
            ctx.state.reclaim_above_bars += 1
            if ctx.state.reclaim_above_bars >= ctx.reclaim_confirm_bars:
                # 连续站稳MA20说明跌破结构失效，回到基准状态。
                checks = [
                    self._bar_check(ctx, "连续站稳MA20", True, ctx.state.reclaim_above_bars, ctx.reclaim_confirm_bars),
                    self._transition_check(ctx, BELOW_MA20, ABOVE_MA20, "reclaimed MA20"),
                ]
                self._reset_setup(ctx.state, keep_above_ready=True)
                ctx.state.state = ABOVE_MA20
                trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 2, "passed", "reclaimed MA20", checks)
                return _no_signal(ctx.request, "reclaimed MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
            checks = [self._bar_check(ctx, "等待连续站稳MA20确认", True, ctx.state.reclaim_above_bars, ctx.reclaim_confirm_bars)]
            trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 3, "waiting", "waiting for reclaim confirmation", checks)
            return _no_signal(ctx.request, "waiting for reclaim confirmation", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)
        ctx.state.reclaim_above_bars = 0
        if touched:
            # 记录触碰K线的开收低位作为后续二次跌破触发线。
            checks = [
                self._bar_check(ctx, "最高价反弹触碰MA20", touched, ctx.high, ctx.ma20, ctx.high - ctx.ma20),
                self._transition_check(ctx, BELOW_MA20, REBOUND_TO_HIGH, "rebound touched MA20"),
            ]
            ctx.state.state = REBOUND_TO_HIGH
            ctx.state.touch_open = ctx.open_price
            ctx.state.touch_close = ctx.close
            ctx.state.touch_high = ctx.high
            ctx.state.touch_ma20 = ctx.ma20
            ctx.state.touch_time = ctx.event_time
            ctx.state.trigger_line = min(ctx.open_price, ctx.close)
            ctx.state.wait_bars = 0
            trace = self._bar_trace(ctx, REBOUND_TO_HIGH, "反弹至高点", 4, "passed", "rebound touched MA20", checks)
            return _no_signal(ctx.request, "rebound touched MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
        checks = [self._bar_check(ctx, "未重新站上MA20，等待反弹触碰", True, ctx.high, ctx.ma20, ctx.high - ctx.ma20)]
        trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 3, "waiting", "waiting for rebound to MA20", checks)
        return _no_signal(ctx.request, "waiting for rebound to MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace, False)

    def _handle_rebound_to_high(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 状态4：反弹触碰MA20后，在限定K线内等待再次收盘跌破MA20。
        ctx.state.wait_bars += 1
        stood_above = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        broke_trigger = ctx.close < ctx.ma20
        if stood_above:
            ctx.state.reclaim_above_bars += 1
            if ctx.state.reclaim_above_bars >= ctx.reclaim_confirm_bars:
                # 等待二次跌破前连续站稳MA20，当前形态作废。
                checks = [
                    self._bar_check(ctx, "连续站稳MA20", True, ctx.state.reclaim_above_bars, ctx.reclaim_confirm_bars),
                    self._transition_check(ctx, REBOUND_TO_HIGH, ABOVE_MA20, "reset: stood above MA20 before second break"),
                ]
                self._reset_setup(ctx.state, keep_above_ready=True)
                ctx.state.state = ABOVE_MA20
                trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 2, "failed", "reset: stood above MA20 before second break", checks)
                return _no_signal(ctx.request, "reset: stood above MA20 before second break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
            checks = [self._bar_check(ctx, "等待连续站稳MA20确认", True, ctx.state.reclaim_above_bars, ctx.reclaim_confirm_bars)]
            trace = self._bar_trace(ctx, REBOUND_TO_HIGH, "反弹至高点", 4, "waiting", "waiting for second break or reclaim confirmation", checks)
            return _no_signal(ctx.request, "waiting for second break or reclaim confirmation", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
        ctx.state.reclaim_above_bars = 0
        if broke_trigger:
            settings = self._signal_settings(ctx.request)
            ma20_slope_blocked = ctx.ma20_slope is not None and ctx.ma20_slope > settings["entry_ma20_slope_max"]
            ma60_slope_blocked = ctx.ma60_slope is not None and ctx.ma60_slope > settings["entry_ma60_slope_max"]
            peak_blocked = not self._has_entry_peak_in_range(ctx.state, settings)
            if ma20_slope_blocked or ma60_slope_blocked or peak_blocked:
                checks = [
                    self._bar_check(ctx, "入场MA20斜率过滤", not ma20_slope_blocked, ctx.ma20_slope, settings["entry_ma20_slope_max"]),
                    self._bar_check(ctx, "入场MA60斜率过滤", not ma60_slope_blocked, ctx.ma60_slope, settings["entry_ma60_slope_max"]),
                    self._bar_check(ctx, "入场ZigZag波峰过滤", not peak_blocked, ctx.state.zigzag_peak_distances, f"{settings['entry_zigzag_peak_min_bars']}-{settings['entry_zigzag_peak_max_bars']}"),
                    self._transition_check(ctx, REBOUND_TO_HIGH, INIT, "entry filter blocked short"),
                ]
                self._reset_setup(ctx.state)
                trace = self._bar_trace(ctx, INIT, "初始待机", 1, "failed", "entry filter blocked short", checks)
                return _no_signal(ctx.request, "entry filter blocked short", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
            # 二次跌破触发空头信号，并按当前K线收盘价确认空头持仓。
            checks = [
                self._bar_check(ctx, "收盘再次跌破MA20", broke_trigger, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
                self._transition_check(ctx, REBOUND_TO_HIGH, SECOND_BREAK_SIGNAL, "SHORT: second close below MA20"),
            ]
            trigger = ctx.close
            atr = max(0.0001, ctx.high - ctx.low)
            ctx.state.state = SECOND_BREAK_SIGNAL
            ctx.state.signal_trigger_price = trigger
            ctx.state.signal_entry = trigger
            ctx.state.signal_bar_index = ctx.state.current_bar_index
            ctx.state.signal_atr = atr
            ctx.state.signal_adverse_target = ctx.close + settings["adverse_atr_multiple"] * atr
            ctx.state.signal_time = ctx.event_time
            ctx.state.signal_lowest_low = ctx.low
            ctx.state.signal_prev_low = ctx.low
            ctx.state.signal_no_new_low_bars = 0
            ctx.state.signal_rising_low_bars = 0
            ctx.state.signal_ma20_touched = ctx.high >= ctx.ma20
            ctx.state.signal_entry_ma20_slope = ctx.ma20_slope
            ctx.state.signal_entry_ma60_slope = ctx.ma60_slope
            metrics = self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60)
            metrics.update({"signal": "SHORT", "trigger_price": trigger, "entry_price": trigger, "atr": atr})
            trace = _trace(ctx.request, "bar", SECOND_BREAK_SIGNAL, "再次跌出出信号", 5, "passed", "SHORT: second close below MA20", checks, metrics, {"target_position": -1, "confidence": 0.8, "signal": "SHORT"})
            ctx.state.state = LOSS_HOLDING
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": -1,
                "confidence": 0.8,
                "reason": "SHORT: second close below MA20",
                "metrics": metrics,
                "trace": trace,
            }
        if ctx.state.wait_bars > ctx.max_wait_bars:
            # 超过等待窗口还没有二次跌破，回到MA20下方等待新的反弹触碰。
            checks = [
                self._bar_check(ctx, "二次跌破等待超时", True, ctx.state.wait_bars, ctx.max_wait_bars),
                self._transition_check(ctx, REBOUND_TO_HIGH, BELOW_MA20, "second break timeout"),
            ]
            ctx.state.state = BELOW_MA20
            ctx.state.wait_bars = 0
            ctx.state.touch_open = None
            ctx.state.touch_close = None
            ctx.state.touch_high = None
            ctx.state.touch_ma20 = None
            ctx.state.trigger_line = None
            trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 3, "failed", "second break timeout", checks)
            return _no_signal(ctx.request, "second break timeout", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)
        checks = [self._bar_check(ctx, "等待收盘再次跌破MA20", True, ctx.close, ctx.ma20, ctx.close - ctx.ma20)]
        trace = self._bar_trace(ctx, REBOUND_TO_HIGH, "反弹至高点", 4, "waiting", "waiting for second break", checks)
        return _no_signal(ctx.request, "waiting for second break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60), trace)

    def _handle_second_break_signal(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 兼容防御：SECOND_BREAK_SIGNAL 是瞬态；若跨K线残留，则按已有入场价分流到持仓态。
        entry = ctx.state.signal_entry if ctx.state.signal_entry is not None else ctx.close
        ctx.state.signal_entry = entry
        next_state = PROFIT_HOLDING if ctx.close < entry else LOSS_HOLDING
        checks = [
            self._bar_check(ctx, "瞬态信号转入持仓", True, ctx.close, entry, ctx.close - entry),
            self._transition_check(ctx, SECOND_BREAK_SIGNAL, next_state, "short position already confirmed"),
        ]
        ctx.state.state = next_state
        step_key = ctx.state.state
        label = "赚钱中" if step_key == PROFIT_HOLDING else "亏钱中"
        trace = self._bar_trace(ctx, step_key, label, 6 if step_key == PROFIT_HOLDING else 7, "passed", "short position already confirmed", checks)
        metrics = self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period, ctx.ma60)
        metrics["signal"] = "SHORT"
        return _no_signal(ctx.request, "short position already confirmed", metrics, trace)

    def _handle_holding(self, ctx: StateDiagramBarContext) -> ResponseDict:
        # 状态5/6：持仓观察，按盈利回撤、亏损止损和观察窗口决定退出。
        state = ctx.state
        settings = self._signal_settings(ctx.request)
        state.signal_bars += 1
        entry = state.signal_entry
        atr = max(0.0001, state.signal_atr or (ctx.high - ctx.low))
        # 更新持仓以来的最低价、未创新低计数和低点上移计数。
        if state.signal_lowest_low is None or ctx.low < state.signal_lowest_low:
            state.signal_lowest_low = ctx.low
            state.signal_no_new_low_bars = 0
        else:
            state.signal_no_new_low_bars += 1
        if state.signal_prev_low is not None and ctx.low > state.signal_prev_low:
            state.signal_rising_low_bars += 1
        else:
            state.signal_rising_low_bars = 0
        state.signal_prev_low = ctx.low
        if ctx.high >= ctx.ma20:
            state.signal_ma20_touched = True
        state.signal_stop_tr_avg = ctx.stop_tr_avg

        # 盈亏状态由当前收盘价相对入场价决定，并同步到状态图节点。
        previous_holding_state = state.state
        profitable = entry is not None and ctx.close < entry
        state.state = PROFIT_HOLDING if profitable else LOSS_HOLDING
        # 亏损止损区使用动态波幅阈值；若均线斜率继续转弱，可延后止损。
        adverse_distance = (ctx.close - entry) if entry is not None else None
        dynamic_threshold = ctx.stop_tr_avg or ((state.signal_adverse_target - entry) if entry is not None and state.signal_adverse_target is not None else None)
        in_stop_zone = not profitable and dynamic_threshold is not None and adverse_distance is not None and adverse_distance >= dynamic_threshold
        ma20_more_bearish = ctx.ma20_slope is not None and state.signal_entry_ma20_slope is not None and ctx.ma20_slope < state.signal_entry_ma20_slope
        ma60_more_bearish = ctx.ma60_slope is not None and state.signal_entry_ma60_slope is not None and ctx.ma60_slope < state.signal_entry_ma60_slope
        stop_deferred = bool(in_stop_zone and ma20_more_bearish and ma60_more_bearish)
        hit_stop = bool(in_stop_zone and not stop_deferred)
        no_new_low_stop = not profitable and state.signal_no_new_low_bars >= settings["strength_exit_bars"]
        stood_above_ma20 = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        if not profitable and stood_above_ma20:
            state.reclaim_above_bars += 1
        elif not profitable:
            state.reclaim_above_bars = 0
        break_rebound_high_stop = not profitable and state.touch_high is not None and ctx.close > state.touch_high
        stood_above_stop = not profitable and state.reclaim_above_bars >= ctx.reclaim_confirm_bars
        bearish_ma60_stop_guard = (
            ctx.ma60 is not None
            and ctx.ma20 < ctx.ma60
            and ctx.high < ctx.ma60
        )
        # 盈利退出先看反弹是否触碰MA20，再用MA60、ATR(26)和均线斜率判断是否应该让利润继续奔跑。
        ma20_reclaim_distance = ctx.close - ctx.ma20
        ma20_slope_down = ctx.ma20_slope is not None and ctx.ma20_slope < 0
        ma60_slope_down = ctx.ma60_slope is not None and ctx.ma60_slope < 0
        close_above_ma60_take = profitable and state.signal_ma20_touched and ctx.ma60 is not None and ctx.close > ctx.ma60
        zigzag_trough_take = profitable and settings.get("use_zigzag_trough_take_profit") and self._has_confirmed_trough_after_signal(state)
        prefer_zigzag_take = bool(settings.get("use_zigzag_trough_take_profit") and settings.get("prefer_zigzag_trough_take_profit"))
        below_ma60_after_ma20_reclaim = profitable and state.signal_ma20_touched and ctx.ma60 is not None and ctx.close <= ctx.ma60 and ctx.close > ctx.ma20
        ma20_reclaim_too_small_take_block = below_ma60_after_ma20_reclaim and ctx.atr26 is not None and ma20_reclaim_distance < ctx.atr26
        ma_slope_down_take_block = below_ma60_after_ma20_reclaim and ctx.atr26 is not None and ma20_reclaim_distance >= ctx.atr26 and ma20_slope_down and ma60_slope_down
        below_ma60_take = below_ma60_after_ma20_reclaim and ctx.atr26 is not None and ma20_reclaim_distance >= ctx.atr26 and not (ma20_slope_down and ma60_slope_down)
        ma_reclaim_take = close_above_ma60_take or below_ma60_take
        take_profit = zigzag_trough_take or (ma_reclaim_take and not prefer_zigzag_take)
        holding_state_changed = previous_holding_state != state.state
        if take_profit:
            # 盈利结构退出，标记本轮信号成功后重置状态图。
            if zigzag_trough_take:
                trough = state.zigzag_recent_troughs[0] if state.zigzag_recent_troughs else {}
                exit_check = self._bar_check(ctx, "ZigZag确认盈利波谷", True, trough.get("pivot_index"), state.signal_bar_index)
                reason = "ATR ZigZag trough confirmed after short profit"
            elif close_above_ma60_take:
                exit_check = self._bar_check(ctx, "盈利反弹收盘超过MA60", True, ctx.close, ctx.ma60, ctx.close - ctx.ma60)
                reason = "profit rebound closed above MA60"
            else:
                exit_check = self._bar_check(ctx, "MA60下反弹强度达到ATR且斜率未双双向下", True, ma20_reclaim_distance, ctx.atr26)
                reason = "profit rebound above MA20 reached ATR26 without dual bearish slopes"
            checks = [exit_check]
            if holding_state_changed:
                checks.append(self._transition_check(ctx, previous_holding_state, state.state, "holding profit/loss state changed"))
            checks.append(self._transition_check(ctx, state.state, TAKE_PROFIT, "trend-following profit exit after MA20 reclaim"))
            state.state = TAKE_PROFIT
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period, ctx.ma60)
            metrics["signal_result"] = "success"
            trace = _trace(ctx.request, "bar", TAKE_PROFIT, "止盈", 8, "passed", reason, checks, metrics, {"target_position": 0, "confidence": 0.8, "signal": "CLOSE_SHORT"})
            self._reset_setup(state)
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": 0,
                "confidence": 0.8,
                "reason": reason,
                "metrics": metrics,
                "trace": trace,
            }
        if not bearish_ma60_stop_guard and (hit_stop or no_new_low_stop or break_rebound_high_stop or stood_above_stop):
            # 亏损或走势转强触发退出，标记本轮信号失败后重置。
            if hit_stop:
                exit_check = self._bar_check(ctx, "亏损进入止损区", True, adverse_distance, dynamic_threshold)
                reason = "dynamic stop zone hit"
            elif no_new_low_stop:
                exit_check = self._bar_check(ctx, "亏损后连续未创新低", True, state.signal_no_new_low_bars, settings["strength_exit_bars"])
                reason = "loss holding failed to make new lows"
            elif break_rebound_high_stop:
                exit_check = self._bar_check(ctx, "亏损后收盘突破反弹高点", True, ctx.close, state.touch_high, ctx.close - state.touch_high)
                reason = "loss holding closed above rebound high"
            else:
                exit_check = self._bar_check(ctx, "亏损后连续站稳MA20", True, state.reclaim_above_bars, ctx.reclaim_confirm_bars)
                reason = "loss holding confirmed MA20 reclaim"
            checks = [exit_check]
            if holding_state_changed:
                checks.append(self._transition_check(ctx, previous_holding_state, state.state, "holding profit/loss state changed"))
            checks.append(self._transition_check(ctx, state.state, STOP_LOSS, "stop loss exit"))
            state.state = STOP_LOSS
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period, ctx.ma60)
            metrics["signal_result"] = "failure"
            trace = _trace(ctx.request, "bar", STOP_LOSS, "止损", 8, "failed", reason, checks, metrics, {"target_position": 0, "confidence": 0.8, "signal": "CLOSE_SHORT"})
            self._reset_setup(state)
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": 0,
                "confidence": 0.8,
                "reason": reason,
                "metrics": metrics,
                "trace": trace,
            }
        if not bearish_ma60_stop_guard and state.signal_bars >= settings["observation_bars"] and not profitable:
            # 观察窗口结束仍未形成有效盈利结构，按未解决的止损路径结束。
            checks = [self._bar_check(ctx, "观察窗口结束仍未盈利", True, state.signal_bars, settings["observation_bars"])]
            if holding_state_changed:
                checks.append(self._transition_check(ctx, previous_holding_state, state.state, "holding profit/loss state changed"))
            checks.append(self._transition_check(ctx, state.state, STOP_LOSS, "observation window ended without structural exit"))
            state.state = STOP_LOSS
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period, ctx.ma60)
            metrics["signal_result"] = "unresolved"
            trace = _trace(ctx.request, "bar", STOP_LOSS, "止损", 8, "done", "observation window ended without structural exit", checks, metrics, {"target_position": 0, "confidence": 0.8, "signal": "CLOSE_SHORT"})
            self._reset_setup(state)
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": 0,
                "confidence": 0.8,
                "reason": "observation window ended without structural exit",
                "metrics": metrics,
                "trace": trace,
            }
        if profitable:
            if not state.signal_ma20_touched:
                current_check = self._bar_check(ctx, "当前持仓盈利，等待触碰MA20", True, ctx.close, entry, ctx.close - entry)
            elif ctx.ma60 is not None and ctx.close > ctx.ma60:
                current_check = self._bar_check(ctx, "盈利反弹收盘超过MA60", True, ctx.close, ctx.ma60, ctx.close - ctx.ma60)
            elif ctx.close <= ctx.ma20:
                current_check = self._bar_check(ctx, "盈利后触碰MA20但未站上", True, ctx.close, ctx.ma20, ctx.close - ctx.ma20)
            elif ma20_reclaim_too_small_take_block:
                current_check = self._bar_check(ctx, "MA60下站上MA20但距离小于ATR26，不止盈", True, ma20_reclaim_distance, ctx.atr26)
            elif ma_slope_down_take_block:
                current_check = self._bar_check(ctx, "MA60下反弹达ATR但MA20/MA60斜率均向下，不止盈", True, ctx.ma20_slope, ctx.ma60_slope)
            else:
                current_check = self._bar_check(ctx, "盈利后等待止盈条件", True, ma20_reclaim_distance, ctx.atr26)
        elif stop_deferred:
            current_check = self._bar_check(ctx, "止损区被空头斜率延后", True, ctx.ma20_slope, state.signal_entry_ma20_slope)
        elif bearish_ma60_stop_guard:
            current_check = self._bar_check(ctx, "空头排列且K线低于MA60，忽略止损", True, ctx.high, ctx.ma60, ctx.high - ctx.ma60)
        elif not profitable and stood_above_ma20:
            current_check = self._bar_check(ctx, "亏损后站上MA20，等待确认", True, state.reclaim_above_bars, ctx.reclaim_confirm_bars)
        else:
            current_check = self._bar_check(ctx, "当前持仓亏损，等待退出条件", True, adverse_distance, dynamic_threshold)
        checks = [current_check]
        if holding_state_changed:
            checks.append(self._transition_check(ctx, previous_holding_state, state.state, "holding profit/loss state changed"))
        label = "赚钱中" if state.state == PROFIT_HOLDING else "亏钱中"
        trace = self._bar_trace(ctx, state.state, label, 6 if state.state == PROFIT_HOLDING else 7, "waiting", "holding short until exit state", checks)
        metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period, ctx.ma60)
        metrics["signal"] = "SHORT"
        return _no_signal(ctx.request, "holding short until exit state", metrics, trace)

    def _has_confirmed_trough_after_signal(self, state: StateDiagramShortState) -> bool:
        if state.signal_bar_index is None:
            return False
        for trough in state.zigzag_recent_troughs:
            try:
                pivot_index = int(trough.get("pivot_index"))
                confirmed_index = int(trough.get("confirmed_index"))
            except (TypeError, ValueError):
                continue
            if pivot_index >= state.signal_bar_index and confirmed_index >= pivot_index:
                return True
        return False

    def _has_entry_peak_in_range(self, state: StateDiagramShortState, settings: dict[str, Any]) -> bool:
        min_bars = settings["entry_zigzag_peak_min_bars"]
        max_bars = settings["entry_zigzag_peak_max_bars"]
        if min_bars <= 0 and max_bars >= 1000000000:
            return True
        for distance in state.zigzag_peak_distances:
            if min_bars <= distance <= max_bars:
                return True
        return False

    def on_tick(self, request: RequestDict) -> ResponseDict:
        # Tick入口：该策略只按K线推进，tick事件直接返回无信号。
        with self._lock:
            state = self._state_for(request)
            ma_period, _, _ = self._settings(request)
            return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state, None, ma_period))

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        # 回放K线与实时K线共用同一套状态推进逻辑。
        return self.on_bar(request)
