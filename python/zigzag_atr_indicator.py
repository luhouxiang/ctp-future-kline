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
    phase: str = "INIT"
    bars: list[dict[str, Any]] = field(default_factory=list)
    trs: list[float] = field(default_factory=list)
    extreme_bar: dict[str, Any] | None = None
    last_event_ts: float | None = None


class ATRZigZagIndicatorStrategy(Strategy):
    definition: StrategyDefinition = {
        "strategy_id": ZIGZAG_ATR26_INDICATOR_ID,
        "display_name": "ATR26 ZigZag 波峰波谷",
        "entry_script": "zigzag_atr_indicator.py",
        "version": "1.0.0",
        "updated_at": _rfc3339_utc_now(),
        "default_params": {
            "atr_period": 26,
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
        normalized = self._normalize_bar(bar)
        tr = self._true_range(state.bars[-1] if state.bars else None, normalized)
        state.bars.append(normalized)
        state.trs.append(tr)
        keep = max(atr_period + 4, 128)
        if len(state.bars) > keep:
            drop = len(state.bars) - keep
            state.bars = state.bars[drop:]
            state.trs = state.trs[drop:]
        atr = self._atr(state.trs, atr_period)
        if atr is None or atr <= 0:
            return None
        if state.phase == "INIT" or state.extreme_bar is None:
            state.extreme_bar = normalized
            state.phase = "FIND_PEAK"
            return None
        if state.phase == "FIND_PEAK":
            if normalized["high"] > state.extreme_bar["high"]:
                state.extreme_bar = normalized
            if normalized["low"] <= state.extreme_bar["high"] - atr:
                peak = state.extreme_bar
                state.phase = "FIND_TROUGH"
                state.extreme_bar = normalized
                return self._event("PEAK", peak, normalized, atr)
        elif state.phase == "FIND_TROUGH":
            if normalized["low"] < state.extreme_bar["low"]:
                state.extreme_bar = normalized
            if normalized["high"] >= state.extreme_bar["low"] + atr:
                trough = state.extreme_bar
                state.phase = "FIND_PEAK"
                state.extreme_bar = normalized
                return self._event("TROUGH", trough, normalized, atr)
        return None

    def _normalize_bar(self, bar: dict[str, Any]) -> dict[str, Any]:
        ts = _bar_event_time(bar)
        return {
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

    def _event(self, typ: str, pivot: dict[str, Any], confirmed: dict[str, Any], atr: float) -> dict[str, Any]:
        price = pivot["high"] if typ == "PEAK" else pivot["low"]
        return {
            "indicator": "zigzag_atr26",
            "signal": "ZIGZAG",
            "zigzag_type": typ,
            "pivot_time": pivot["adjusted_time"],
            "pivot_price": price,
            "pivot_high": pivot["high"],
            "pivot_low": pivot["low"],
            "confirmed_time": confirmed["adjusted_time"],
            "confirmed_high": confirmed["high"],
            "confirmed_low": confirmed["low"],
            "reversal_value": atr,
        }

    def _metrics(self, state: ZigZagState, params: dict[str, Any]) -> dict[str, Any]:
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        atr = self._atr(state.trs, atr_period)
        return {
            "indicator": "zigzag_atr26",
            "state": state.phase,
            "step_total": 1,
            "atr_period": atr_period,
            "atr": atr,
            "bars": len(state.bars),
            "extreme_time": (state.extreme_bar or {}).get("adjusted_time"),
            "extreme_high": (state.extreme_bar or {}).get("high"),
            "extreme_low": (state.extreme_bar or {}).get("low"),
        }
