# -*- coding: utf-8 -*-
"""ATR ZigZag peak/trough indicator emitted through strategy traces."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Any

from strategy_common import (
    Strategy,
    _bar_event_time,
    _event_ts,
    _float,
    _instance_id,
    _mode_key,
    _no_signal,
    _rfc3339_utc_now,
    _strategy_params_for_instance,
    _trace,
)
from strategy_types import JSONObject, RequestDict, ResponseDict, StateKey, StrategyDefinition, ZIGZAG_ATR26_INDICATOR_ID


@dataclass
class ZigZagState:
    mode: str
    instance_id: str
    symbol: str
    stage: str = "INIT"
    bars: list[dict[str, Any]] = field(default_factory=list)
    trs: list[float] = field(default_factory=list)
    init_cache: list[dict[str, Any]] = field(default_factory=list)
    last_fixed: dict[str, Any] | None = None
    curr_provisional: dict[str, Any] | None = None
    next_provisional: dict[str, Any] | None = None
    last_event_ts: float | None = None
    next_index: int = 0


class ATRZigZagIndicatorStrategy(Strategy):
    definition: StrategyDefinition = {
        "strategy_id": ZIGZAG_ATR26_INDICATOR_ID,
        "display_name": "ATR26 ZigZag 波峰波谷",
        "entry_script": "zigzag_atr_indicator.py",
        "version": "1.0.0",
        "updated_at": _rfc3339_utc_now(),
        "default_params": {
            "atr_period": 26,
            "atr_multiple": 2.0,
            "min_bars": 5,
            "warmup_target": 26,
        },
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        if definition is not None:
            self.definition = definition
        self._states: dict[StateKey, ZigZagState] = {}
        self._lock = RLock()

    def required_warmup_bars(self, instance: JSONObject) -> int:
        params = _strategy_params_for_instance(self.definition, instance)
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        return max(atr_period, int(_float(params.get("warmup_target"), atr_period)))

    def start_instance(self, instance: JSONObject) -> None:
        params = _strategy_params_for_instance(self.definition, instance)
        warmup_bars = params.get("warmup_bars")
        symbols = instance.get("symbols") if isinstance(instance.get("symbols"), list) else []
        with self._lock:
            for symbol in symbols:
                state = self._state_for(instance, str(symbol))
                if isinstance(warmup_bars, list):
                    for bar in warmup_bars:
                        if isinstance(bar, dict):
                            self._process_bar(state, bar, params)

    def stop_instance(self, instance_id: str) -> None:
        with self._lock:
            for key in list(self._states.keys()):
                if key[1] == instance_id:
                    del self._states[key]

    def on_bar(self, request: RequestDict) -> ResponseDict:
        bar = request.get("bar") or {}
        if not isinstance(bar, dict):
            return _no_signal(request, "no bar")
        params = _strategy_params_for_instance(self.definition, request.get("instance") or {})
        with self._lock:
            state = self._state_for(request.get("instance") or {}, str(request.get("symbol") or ""))
            event_ts = _event_ts(_bar_event_time(bar, request.get("event_time")))
            if state.last_event_ts is not None and event_ts is not None and event_ts <= state.last_event_ts:
                return _no_signal(request, "duplicate or out-of-order zigzag bar", self._metrics(state, params), None, False)
            if event_ts is not None:
                state.last_event_ts = event_ts
            event = self._process_bar(state, bar, params)
            metrics = self._metrics(state, params)
            if event is None:
                return _no_signal(request, "waiting for ATR ZigZag reversal", metrics, None, False)
            metrics.update(event)
            step_key = "ZIGZAG_PEAK" if event["zigzag_type"] == "PEAK" else "ZIGZAG_TROUGH"
            step_label = "确认波峰" if event["zigzag_type"] == "PEAK" else "确认波谷"
            trace = _trace(
                request,
                "bar",
                step_key,
                step_label,
                1,
                "passed",
                f"{event['zigzag_type']} confirmed by ATR reversal",
                [],
                metrics,
                {"indicator": "zigzag_atr26", "zigzag_type": event["zigzag_type"]},
            )
            return _no_signal(request, f"{event['zigzag_type']} confirmed", metrics, trace, False)

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        return self.on_bar(request)

    def _state_for(self, instance: JSONObject, symbol: str) -> ZigZagState:
        key: StateKey = (_mode_key({"mode": instance.get("mode")}), str(instance.get("instance_id") or ""), symbol)
        state = self._states.get(key)
        if state is None:
            state = ZigZagState(mode=key[0], instance_id=key[1], symbol=key[2])
            self._states[key] = state
        return state

    def _process_bar(self, state: ZigZagState, bar: dict[str, Any], params: dict[str, Any]) -> dict[str, Any] | None:
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        min_bars = max(1, int(_float(params.get("min_bars"), _float(params.get("min_pivot_bars"), 5))))
        normalized = self._normalize_bar(bar, state.next_index)
        state.next_index += 1
        tr = self._true_range(state.bars[-1] if state.bars else None, normalized)
        state.bars.append(normalized)
        state.trs.append(tr)
        keep = max(atr_period + min_bars * 3 + 8, 128)
        if len(state.bars) > keep:
            drop = len(state.bars) - keep
            state.bars = state.bars[drop:]
            state.trs = state.trs[drop:]
            if state.stage == "INIT" and len(state.init_cache) > keep:
                state.init_cache = state.init_cache[-keep:]
        if state.stage == "INIT":
            state.init_cache.append(normalized)
        atr = self._atr(state.trs, atr_period)
        if atr is None or atr <= 0:
            return None
        reversal_value = atr * max(0.0001, _float(params.get("atr_multiple"), 2.0))

        if state.stage == "INIT":
            self._initialize(state, reversal_value)
            return None

        if not state.last_fixed or not state.curr_provisional:
            return None

        high = normalized["high"]
        low = normalized["low"]
        if state.last_fixed["type"] == "TROUGH":
            if state.next_provisional is None:
                if high > state.curr_provisional["kline"]["high"]:
                    state.curr_provisional = self._point("PEAK", normalized)
                elif state.curr_provisional["kline"]["high"] - low >= reversal_value:
                    state.next_provisional = self._point("TROUGH", normalized)
            else:
                if high > state.curr_provisional["kline"]["high"]:
                    state.curr_provisional = self._point("PEAK", normalized)
                    state.next_provisional = None
                else:
                    if low < state.next_provisional["kline"]["low"]:
                        state.next_provisional = self._point("TROUGH", normalized)
                    if high - state.next_provisional["kline"]["low"] >= reversal_value:
                        if not self._lock_spacing_ok(state, min_bars):
                            return None
                        locked = state.curr_provisional
                        previous = state.last_fixed
                        state.last_fixed = state.curr_provisional
                        state.curr_provisional = state.next_provisional
                        state.next_provisional = self._point("PEAK", normalized)
                        return self._event("PEAK", locked, previous, normalized, reversal_value)

        elif state.last_fixed["type"] == "PEAK":
            if state.next_provisional is None:
                if low < state.curr_provisional["kline"]["low"]:
                    state.curr_provisional = self._point("TROUGH", normalized)
                elif high - state.curr_provisional["kline"]["low"] >= reversal_value:
                    state.next_provisional = self._point("PEAK", normalized)
            else:
                if low < state.curr_provisional["kline"]["low"]:
                    state.curr_provisional = self._point("TROUGH", normalized)
                    state.next_provisional = None
                else:
                    if high > state.next_provisional["kline"]["high"]:
                        state.next_provisional = self._point("PEAK", normalized)
                    if state.next_provisional["kline"]["high"] - low >= reversal_value:
                        if not self._lock_spacing_ok(state, min_bars):
                            return None
                        locked = state.curr_provisional
                        previous = state.last_fixed
                        state.last_fixed = state.curr_provisional
                        state.curr_provisional = state.next_provisional
                        state.next_provisional = self._point("TROUGH", normalized)
                        return self._event("TROUGH", locked, previous, normalized, reversal_value)
        return None

    def _initialize(self, state: ZigZagState, reversal_value: float) -> None:
        highs = [item["high"] for item in state.init_cache]
        lows = [item["low"] for item in state.init_cache]
        max_h = max(highs)
        min_l = min(lows)
        if max_h - min_l < reversal_value:
            return
        idx_max = highs.index(max_h)
        idx_min = lows.index(min_l)
        if idx_min < idx_max:
            state.last_fixed = self._point("TROUGH", state.init_cache[idx_min])
            state.curr_provisional = self._point("PEAK", state.init_cache[idx_max])
        else:
            state.last_fixed = self._point("PEAK", state.init_cache[idx_max])
            state.curr_provisional = self._point("TROUGH", state.init_cache[idx_min])
        state.stage = "RUNNING"
        state.init_cache.clear()

    def _point(self, typ: str, bar: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": typ,
            "kline": bar,
            "index": int(bar["index"]),
        }

    def _lock_spacing_ok(self, state: ZigZagState, min_bars: int) -> bool:
        if not state.last_fixed or not state.curr_provisional or not state.next_provisional:
            return False
        cond1 = int(state.curr_provisional["index"]) - int(state.last_fixed["index"]) >= min_bars
        cond2 = int(state.next_provisional["index"]) - int(state.curr_provisional["index"]) >= min_bars
        return cond1 and cond2

    def _normalize_bar(self, bar: dict[str, Any], index: int) -> dict[str, Any]:
        ts = _bar_event_time(bar)
        return {
            "index": index,
            "adjusted_time": ts,
            "open": _float(bar.get("open")),
            "high": _float(bar.get("high")),
            "low": _float(bar.get("low")),
            "close": _float(bar.get("close")),
        }

    def _true_range(self, prev: dict[str, Any] | None, bar: dict[str, Any]) -> float:
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        tr = max(0.0, high - low)
        if prev is None:
            return tr
        prev_close = _float(prev.get("close"))
        return max(tr, abs(high - prev_close), abs(low - prev_close))

    def _atr(self, trs: list[float], period: int) -> float | None:
        if len(trs) < period:
            return None
        recent = trs[-period:]
        return sum(recent) / period

    def _event(
        self,
        typ: str,
        locked: dict[str, Any],
        previous: dict[str, Any],
        confirmed: dict[str, Any],
        atr: float,
    ) -> dict[str, Any]:
        pivot = locked["kline"]
        previous_index = int(previous["index"])
        price = pivot["high"] if typ == "PEAK" else pivot["low"]
        bars_since_previous = int(locked["index"]) - previous_index
        return {
            "indicator": "zigzag_atr26",
            "signal": "ZIGZAG",
            "zigzag_type": typ,
            "pivot_index": locked["index"],
            "pivot_time": pivot["adjusted_time"],
            "pivot_price": price,
            "pivot_high": pivot["high"],
            "pivot_low": pivot["low"],
            "previous_pivot_index": previous_index,
            "pivot_bars_since_previous": bars_since_previous,
            "confirmed_time": confirmed["adjusted_time"],
            "confirmed_index": confirmed["index"],
            "confirmed_high": confirmed["high"],
            "confirmed_low": confirmed["low"],
            "reversal_value": atr,
        }

    def _metrics(self, state: ZigZagState, params: dict[str, Any]) -> dict[str, Any]:
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        atr = self._atr(state.trs, atr_period)
        atr_multiple = max(0.0001, _float(params.get("atr_multiple"), 2.0))
        min_bars = max(1, int(_float(params.get("min_bars"), _float(params.get("min_pivot_bars"), 5))))
        return {
            "indicator": "zigzag_atr26",
            "state": state.stage,
            "stage": state.stage,
            "step_total": 1,
            "atr_period": atr_period,
            "atr_multiple": atr_multiple,
            "min_bars": min_bars,
            "atr": atr,
            "reversal_threshold": None if atr is None else atr * atr_multiple,
            "bars": len(state.bars),
            "next_index": state.next_index,
            "last_fixed_type": (state.last_fixed or {}).get("type"),
            "last_fixed_index": (state.last_fixed or {}).get("index"),
            "curr_provisional_type": (state.curr_provisional or {}).get("type"),
            "curr_provisional_index": (state.curr_provisional or {}).get("index"),
            "next_provisional_type": (state.next_provisional or {}).get("type"),
            "next_provisional_index": (state.next_provisional or {}).get("index"),
        }
