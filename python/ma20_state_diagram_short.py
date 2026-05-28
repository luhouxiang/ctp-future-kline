# -*- coding: utf-8 -*-
"""MA20 状态图做空策略。

该策略按用户给出的状态图显式推进 6 个中间状态：
ABOVE_MA20 -> BELOW_MA20 -> REBOUND_TO_HIGH -> SECOND_BREAK_SIGNAL
-> PROFIT_HOLDING / LOSS_HOLDING，最终以 TAKE_PROFIT 或 STOP_LOSS trace
结束一轮并回到 ABOVE_MA20。
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
class StateDiagramShortState:
    state: StateDiagramShortStateName = ABOVE_MA20
    closes: list[float] = field(default_factory=list)
    trs: list[float] = field(default_factory=list)
    last_bar_ts: float | None = None
    prev_close: float | None = None
    prev_ma: float | None = None
    above_ready: bool = False
    touch_open: float | None = None
    touch_close: float | None = None
    touch_high: float | None = None
    touch_ma20: float | None = None
    touch_time: str = ""
    trigger_line: float | None = None
    wait_bars: int = 0
    signal_entry: float | None = None
    signal_trigger_price: float | None = None
    signal_atr: float | None = None
    signal_adverse_target: float | None = None
    signal_time: str = ""
    signal_bars: int = 0
    signal_lowest_low: float | None = None
    signal_prev_low: float | None = None
    signal_no_new_low_bars: int = 0
    signal_rising_low_bars: int = 0
    signal_ma20_touched: bool = False
    signal_entry_ma20_slope: float | None = None
    signal_entry_ma60_slope: float | None = None
    signal_stop_tr_avg: float | None = None


@dataclass
class StateDiagramBarContext:
    request: RequestDict
    state: StateDiagramShortState
    bar: BarDict
    ma_period: int
    max_wait_bars: int
    open_price: float
    high: float
    low: float
    close: float
    event_time: str
    previous_ma: float | None
    previous_close: float | None
    ma20: float
    ma60: float | None
    ma20_slope: float | None
    ma60_slope: float | None
    stop_tr_avg: float | None


class MA20StateDiagramShortStrategy(Strategy):
    definition: StrategyDefinition = {
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
            "max_wait_bars": 6,
            "observation_bars": 240,
            "adverse_atr_multiple": 0.8,
            "strength_exit_bars": 3,
            "profit_rebound_atr_multiple": 1.0,
            "profit_rising_low_bars": 2,
            "strong_bull_atr_multiple": 1.5,
        },
        "updated_at": _rfc3339_utc_now(),
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        if definition is not None:
            self.definition = definition
        self.states: dict[StateKey, StateDiagramShortState] = {}
        self._lock: RLock = RLock()

    def required_warmup_bars(self, instance: JSONObject) -> int:
        params = _strategy_params_for_instance(self.definition, instance)
        if _int(params.get("warmup_target"), 0) > 0:
            return _int(params.get("warmup_target"), 0)
        return max(2, _int(params.get("ma_period"), 20))

    def start_instance(self, instance: JSONObject) -> None:
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
        with self._lock:
            for key in list(self.states):
                if key[1] == instance_id:
                    del self.states[key]

    def validate_warmup(self, instance: JSONObject, applied_counts: dict[str, int] | None = None) -> None:
        target = self.required_warmup_bars(instance)
        if target <= 0:
            return
        params = _strategy_params_for_instance(self.definition, instance)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        mode = _instance_mode(instance)
        instance_id = str(instance.get("instance_id") or "")
        symbols = instance.get("symbols") or [""]
        applied_counts = applied_counts or {}
        missing: list[str] = []
        with self._lock:
            for symbol in symbols:
                state = self.states.get((mode, instance_id, str(symbol)))
                ma_value = simple_moving_average(state.closes if state else [], ma_period)
                applied = _int(applied_counts.get(str(symbol)), 0)
                if state is None or ma_value is None or applied < target:
                    missing.append(f"{symbol}:{applied}/{target}")
        if missing:
            raise ValueError(
                "strategy warmup is insufficient for MA calculation: "
                f"instance_id={instance_id} strategy_id={self.definition.get('strategy_id', '')} "
                f"mode={mode} ma_period={ma_period} warmup_target={target} symbols={','.join(missing)}"
            )

    def _state_for(self, request: RequestDict) -> StateDiagramShortState:
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            raise ValueError(
                "strategy runtime instance not started: "
                f"instance_id={key[1]} mode={key[0]} symbol={key[2]} strategy_id={self.definition.get('strategy_id', '')}"
            )
        return self.states[key]

    def _apply_warmup_bars(self, instance: JSONObject, bars: Any, mode: str) -> dict[str, int]:
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
            state.state = ABOVE_MA20
            state.above_ready = False
            counts[symbol] = counts.get(symbol, 0) + 1
        return counts

    def _settings(self, request: RequestDict) -> tuple[int, int]:
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return max(2, _int(params.get("ma_period"), 20)), max(1, _int(params.get("max_wait_bars"), 6))

    def _signal_settings(self, request: RequestDict) -> dict[str, Any]:
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return self._signal_settings_from_params(params)

    def _signal_settings_from_params(self, params: dict[str, Any]) -> dict[str, Any]:
        return {
            "ma60_period": max(3, _int(params.get("ma60_period"), 60)),
            "slope_lookback_bars": max(1, _int(params.get("slope_lookback_bars"), 5)),
            "stop_tr_lookback": max(1, _int(params.get("stop_tr_lookback"), 60)),
            "stop_tr_top_n": max(1, _int(params.get("stop_tr_top_n"), 10)),
            "stop_tr_drop_n": max(0, _int(params.get("stop_tr_drop_n"), 2)),
            "observation_bars": max(1, _int(params.get("observation_bars"), 240)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
            "strength_exit_bars": max(1, _int(params.get("strength_exit_bars"), 3)),
            "profit_rebound_atr_multiple": max(0.0001, _float(params.get("profit_rebound_atr_multiple"), 1.0)),
            "profit_rising_low_bars": max(1, _int(params.get("profit_rising_low_bars"), 2)),
            "strong_bull_atr_multiple": max(0.0001, _float(params.get("strong_bull_atr_multiple"), 1.5)),
        }

    def _trim(self, state: StateDiagramShortState, ma_period: int, settings: dict[str, Any]) -> None:
        keep = max(ma_period, settings["ma60_period"] + settings["slope_lookback_bars"], settings["stop_tr_lookback"]) + 5
        state.closes = trim_tail(state.closes, keep)
        state.trs = trim_tail(state.trs, keep)

    def _base_metrics(self, state: StateDiagramShortState, ma20: float | None = None, ma_period: int = 20) -> MetricsDict:
        return {
            "signal": "",
            "state": state.state,
            "step_total": 6,
            "ma": ma20,
            "ma20": ma20,
            "ma_period": ma_period,
            "above_ready": state.above_ready,
            "touch_open": state.touch_open,
            "touch_close": state.touch_close,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "trigger_line": state.trigger_line,
            "wait_bars": state.wait_bars,
            "signal_entry": state.signal_entry,
            "signal_trigger_price": state.signal_trigger_price,
            "signal_atr": state.signal_atr,
            "signal_adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_lowest_low": state.signal_lowest_low,
            "signal_no_new_low_bars": state.signal_no_new_low_bars,
            "signal_rising_low_bars": state.signal_rising_low_bars,
            "signal_ma20_touched": state.signal_ma20_touched,
            "signal_stop_tr_avg": state.signal_stop_tr_avg,
        }

    def _bar_trace(self, ctx: StateDiagramBarContext, step_key: str, label: str, index: int, status: str, reason: str, checks: list[CheckDict]) -> dict[str, Any]:
        return _trace(ctx.request, "bar", step_key, label, index, status, reason, checks, self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period))

    def _reset_setup(self, state: StateDiagramShortState, keep_above_ready: bool = True) -> None:
        state.state = ABOVE_MA20
        state.above_ready = keep_above_ready
        state.touch_open = None
        state.touch_close = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.trigger_line = None
        state.wait_bars = 0
        self._clear_signal(state)

    def _clear_signal(self, state: StateDiagramShortState) -> None:
        state.signal_entry = None
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
        with self._lock:
            return self._on_bar_locked(request)

    def _on_bar_locked(self, request: RequestDict) -> ResponseDict:
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        _require_fields(bar, ("open", "high", "low", "close"))
        state = self._state_for(request)
        ma_period, max_wait_bars = self._settings(request)
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

        ctx = StateDiagramBarContext(
            request=request,
            state=state,
            bar=bar,
            ma_period=ma_period,
            max_wait_bars=max_wait_bars,
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
            stop_tr_avg=trimmed_top_average(state.trs, settings["stop_tr_lookback"], settings["stop_tr_top_n"], settings["stop_tr_drop_n"]),
        )
        handlers = {
            ABOVE_MA20: self._handle_above_ma20,
            BELOW_MA20: self._handle_below_ma20,
            REBOUND_TO_HIGH: self._handle_rebound_to_high,
            SECOND_BREAK_SIGNAL: self._handle_second_break_signal,
            PROFIT_HOLDING: self._handle_holding,
            LOSS_HOLDING: self._handle_holding,
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

    def _handle_above_ma20(self, ctx: StateDiagramBarContext) -> ResponseDict:
        full_above = ctx.open_price > ctx.ma20 and ctx.high > ctx.ma20 and ctx.low > ctx.ma20 and ctx.close > ctx.ma20
        cross_break = ctx.previous_close is not None and ctx.previous_ma is not None and ctx.previous_close >= ctx.previous_ma and ctx.close < ctx.ma20
        checks = [
            _check("整根K线在MA20之上", full_above, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
            _check("收盘从上向下跌破MA20", cross_break, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
        ]
        if not ctx.state.above_ready:
            ctx.state.above_ready = full_above
            trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 1, "waiting", "waiting for first full bar above MA20", checks)
            return _no_signal(ctx.request, "waiting for first full bar above MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)
        if cross_break:
            ctx.state.state = BELOW_MA20
            trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 2, "passed", "break below MA20 confirmed", checks)
            return _no_signal(ctx.request, "break below MA20 confirmed", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)
        trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 1, "waiting", "waiting above MA20 break", checks)
        return _no_signal(ctx.request, "waiting above MA20 break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)

    def _handle_below_ma20(self, ctx: StateDiagramBarContext) -> ResponseDict:
        stood_above = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        touched = ctx.high >= ctx.ma20
        checks = [
            _check("未重新站上MA20", not stood_above, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
            _check("最高价反弹触碰MA20", touched, ctx.high, ctx.ma20, ctx.high - ctx.ma20),
        ]
        if stood_above:
            self._reset_setup(ctx.state, keep_above_ready=True)
            trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 1, "passed", "reclaimed MA20", checks)
            return _no_signal(ctx.request, "reclaimed MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        if touched:
            ctx.state.state = REBOUND_TO_HIGH
            ctx.state.touch_open = ctx.open_price
            ctx.state.touch_close = ctx.close
            ctx.state.touch_high = ctx.high
            ctx.state.touch_ma20 = ctx.ma20
            ctx.state.touch_time = ctx.event_time
            ctx.state.trigger_line = min(ctx.open_price, ctx.close)
            ctx.state.wait_bars = 0
            trace = self._bar_trace(ctx, REBOUND_TO_HIGH, "反弹至高点", 3, "passed", "rebound touched MA20", checks)
            return _no_signal(ctx.request, "rebound touched MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 2, "waiting", "waiting for rebound to MA20", checks)
        return _no_signal(ctx.request, "waiting for rebound to MA20", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace, False)

    def _handle_rebound_to_high(self, ctx: StateDiagramBarContext) -> ResponseDict:
        ctx.state.wait_bars += 1
        stood_above = ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        broke_trigger = ctx.state.trigger_line is not None and ctx.low <= ctx.state.trigger_line
        checks = [
            _check("未重新站上MA20", not stood_above, ctx.close, ctx.ma20, ctx.close - ctx.ma20),
            _check("触发等待未超时", ctx.state.wait_bars <= ctx.max_wait_bars, ctx.state.wait_bars, ctx.max_wait_bars),
            _check("跌破触碰K开收低价", broke_trigger, ctx.low, ctx.state.trigger_line),
        ]
        if stood_above:
            self._reset_setup(ctx.state, keep_above_ready=True)
            trace = self._bar_trace(ctx, ABOVE_MA20, "MA20之上", 1, "failed", "reset: stood above MA20 before second break", checks)
            return _no_signal(ctx.request, "reset: stood above MA20 before second break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        if broke_trigger:
            trigger = ctx.state.trigger_line if ctx.state.trigger_line is not None else ctx.low
            atr = max(0.0001, ctx.high - ctx.low)
            settings = self._signal_settings(ctx.request)
            ctx.state.state = SECOND_BREAK_SIGNAL
            ctx.state.signal_trigger_price = trigger
            ctx.state.signal_atr = atr
            ctx.state.signal_adverse_target = trigger + settings["adverse_atr_multiple"] * atr
            ctx.state.signal_time = ctx.event_time
            metrics = self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period)
            metrics.update({"signal": "SHORT", "trigger_price": trigger, "entry_price": trigger, "atr": atr})
            trace = _trace(ctx.request, "bar", SECOND_BREAK_SIGNAL, "再次跌出出信号", 4, "passed", "SHORT: broke touch bar open/close low", checks, metrics, {"target_position": -1, "confidence": 0.8, "signal": "SHORT"})
            return {
                "no_signal": False,
                "instance_id": _instance_id(ctx.request),
                "symbol": ctx.request.get("symbol", ""),
                "event_time": ctx.request.get("event_time", ""),
                "target_position": -1,
                "confidence": 0.8,
                "reason": "SHORT: broke touch bar open/close low",
                "metrics": metrics,
                "trace": trace,
            }
        if ctx.state.wait_bars > ctx.max_wait_bars:
            ctx.state.state = BELOW_MA20
            ctx.state.wait_bars = 0
            ctx.state.touch_open = None
            ctx.state.touch_close = None
            ctx.state.touch_high = None
            ctx.state.touch_ma20 = None
            ctx.state.trigger_line = None
            trace = self._bar_trace(ctx, BELOW_MA20, "跌破MA20之下", 2, "failed", "second break timeout", checks)
            return _no_signal(ctx.request, "second break timeout", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)
        trace = self._bar_trace(ctx, REBOUND_TO_HIGH, "反弹至高点", 3, "waiting", "waiting for second break", checks)
        return _no_signal(ctx.request, "waiting for second break", self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period), trace)

    def _handle_second_break_signal(self, ctx: StateDiagramBarContext) -> ResponseDict:
        settings = self._signal_settings(ctx.request)
        ctx.state.signal_entry = ctx.open_price
        atr = max(0.0001, ctx.state.signal_atr or (ctx.high - ctx.low))
        ctx.state.signal_atr = atr
        ctx.state.signal_adverse_target = ctx.open_price + settings["adverse_atr_multiple"] * atr
        ctx.state.signal_lowest_low = ctx.open_price
        ctx.state.signal_prev_low = None
        ctx.state.signal_no_new_low_bars = 0
        ctx.state.signal_rising_low_bars = 0
        ctx.state.signal_ma20_touched = False
        ctx.state.signal_entry_ma20_slope = ctx.ma20_slope
        ctx.state.signal_entry_ma60_slope = ctx.ma60_slope
        ctx.state.state = PROFIT_HOLDING if ctx.close < ctx.open_price else LOSS_HOLDING
        checks = [_check("下一根K线开盘确认持仓", True, ctx.open_price, "entry open")]
        step_key = ctx.state.state
        label = "赚钱中" if step_key == PROFIT_HOLDING else "亏钱中"
        trace = self._bar_trace(ctx, step_key, label, 5 if step_key == PROFIT_HOLDING else 6, "passed", "short position confirmed at next bar open", checks)
        metrics = self._base_metrics(ctx.state, ctx.ma20, ctx.ma_period)
        metrics["signal"] = "SHORT"
        return _no_signal(ctx.request, "short position confirmed at next bar open", metrics, trace)

    def _handle_holding(self, ctx: StateDiagramBarContext) -> ResponseDict:
        state = ctx.state
        settings = self._signal_settings(ctx.request)
        state.signal_bars += 1
        entry = state.signal_entry
        atr = max(0.0001, state.signal_atr or (ctx.high - ctx.low))
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

        profitable = entry is not None and ctx.close < entry
        state.state = PROFIT_HOLDING if profitable else LOSS_HOLDING
        adverse_distance = (ctx.close - entry) if entry is not None else None
        dynamic_threshold = ctx.stop_tr_avg or ((state.signal_adverse_target - entry) if entry is not None and state.signal_adverse_target is not None else None)
        in_stop_zone = not profitable and dynamic_threshold is not None and adverse_distance is not None and adverse_distance >= dynamic_threshold
        ma20_more_bearish = ctx.ma20_slope is not None and state.signal_entry_ma20_slope is not None and ctx.ma20_slope < state.signal_entry_ma20_slope
        ma60_more_bearish = ctx.ma60_slope is not None and state.signal_entry_ma60_slope is not None and ctx.ma60_slope < state.signal_entry_ma60_slope
        stop_deferred = bool(in_stop_zone and ma20_more_bearish and ma60_more_bearish)
        hit_stop = bool(in_stop_zone and not stop_deferred)
        no_new_low_stop = not profitable and state.signal_no_new_low_bars >= settings["strength_exit_bars"]
        stood_above_stop = not profitable and ctx.open_price > ctx.ma20 and ctx.close > ctx.ma20
        rebound = ctx.close - (state.signal_lowest_low if state.signal_lowest_low is not None else ctx.close)
        rebound_ok = state.signal_ma20_touched and rebound >= settings["profit_rebound_atr_multiple"] * atr
        rising_low_take = profitable and rebound_ok and state.signal_rising_low_bars >= settings["profit_rising_low_bars"]
        strong_bull_take = profitable and ctx.low > ctx.ma20 and ctx.close > ctx.open_price and (ctx.close - ctx.open_price) >= settings["strong_bull_atr_multiple"] * atr
        checks = [
            _check("当前持仓盈利", profitable, ctx.close, entry),
            _check("亏损进入止损区", in_stop_zone, adverse_distance, dynamic_threshold),
            _check("止损未被空头斜率延后", not stop_deferred, ctx.ma20_slope, state.signal_entry_ma20_slope),
            _check("亏损后连续未创新低", no_new_low_stop, state.signal_no_new_low_bars, settings["strength_exit_bars"]),
            _check("亏损后重新站上MA20", stood_above_stop, ctx.close, ctx.ma20),
            _check("盈利后触碰MA20", state.signal_ma20_touched, ctx.high, ctx.ma20),
            _check("触碰MA20后反弹达标", rebound_ok, rebound, settings["profit_rebound_atr_multiple"] * atr),
            _check("触碰MA20后低点上移", rising_low_take, state.signal_rising_low_bars, settings["profit_rising_low_bars"]),
            _check("强阳站稳MA20", strong_bull_take, ctx.close - ctx.open_price, settings["strong_bull_atr_multiple"] * atr),
        ]
        if rising_low_take or strong_bull_take:
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period)
            metrics["signal_result"] = "success"
            trace = _trace(ctx.request, "bar", TAKE_PROFIT, "止盈", 6, "passed", "trend-following profit exit after MA20 reclaim", checks, metrics)
            self._reset_setup(state, keep_above_ready=True)
            return _no_signal(ctx.request, "trend-following profit exit after MA20 reclaim", metrics, trace)
        if hit_stop or no_new_low_stop or stood_above_stop:
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period)
            metrics["signal_result"] = "failure"
            reason = "dynamic stop zone hit" if hit_stop else "market strengthened before short worked"
            trace = _trace(ctx.request, "bar", STOP_LOSS, "止损", 6, "failed", reason, checks, metrics)
            self._reset_setup(state, keep_above_ready=True)
            return _no_signal(ctx.request, reason, metrics, trace)
        if state.signal_bars >= settings["observation_bars"] and not profitable:
            metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period)
            metrics["signal_result"] = "unresolved"
            trace = _trace(ctx.request, "bar", STOP_LOSS, "止损", 6, "done", "observation window ended without structural exit", checks, metrics)
            self._reset_setup(state, keep_above_ready=True)
            return _no_signal(ctx.request, "observation window ended without structural exit", metrics, trace)
        label = "赚钱中" if state.state == PROFIT_HOLDING else "亏钱中"
        trace = self._bar_trace(ctx, state.state, label, 5 if state.state == PROFIT_HOLDING else 6, "waiting", "holding short until exit state", checks)
        metrics = self._base_metrics(state, ctx.ma20, ctx.ma_period)
        metrics["signal"] = "SHORT"
        return _no_signal(ctx.request, "holding short until exit state", metrics, trace)

    def on_tick(self, request: RequestDict) -> ResponseDict:
        with self._lock:
            state = self._state_for(request)
            ma_period, _ = self._settings(request)
            return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state, None, ma_period))

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        return self.on_bar(request)
