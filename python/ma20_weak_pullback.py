# -*- coding: utf-8 -*-
"""MA20 弱反弹做空策略与过滤变体。

弱反弹策略相比 baseline 多做两层判断：
1. 先确认上涨结构被破坏，避免把强上涨中的普通回撤当成做空机会；
2. 在反抽触碰 MA20 后，要求再次跌破 reaction_low，确认反抽失败后才做空。

本模块还提供 hard-filter 和 score-filter 两个变体：
- hard-filter：只使用结构过滤，不额外比较多空评分；
- score-filter：要求空头失败分不低于多头停顿分，更保守地过滤强趋势停顿。

为什么这套策略仍使用 bar 驱动：
回放/回测通常只有 K 线，没有逐 tick 时序。把入场条件限定为 bar 内可复现的 high/low/open/close，
可以保证同一份历史数据在实盘回放和测试中得到一致结果。
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
    _normalize_symbols,
    _params,
    _require_fields,
    _rfc3339_utc_now,
    _strategy_params_for_instance,
    _trace,
)
from strategy_types import (
    DONE,
    MA20_WEAK_HARD_FILTER_STRATEGY_ID,
    MA20_WEAK_SCORE_FILTER_STRATEGY_ID,
    MA20_WEAK_STRATEGY_ID,
    SHORT_SIGNAL,
    SIGNAL_ACTIVE,
    TREND_STRUCTURE_FILTER,
    WAIT_BREAK_BELOW_MA20,
    WAIT_BREAK_REACTION_LOW,
    WAIT_PULLBACK_TOUCH_MA20,
    BarDict,
    CheckDict,
    JSONObject,
    MetricsDict,
    StateKey,
    StrategyDefinition,
    StrategySettings,
    TraceDict,
    RequestDict,
    ResponseDict,
    WeakPullbackStateName,
    WeakRegime,
)

logger = logging.getLogger(__name__)


@dataclass
class WeakPullbackShortState:
    """MA20 弱反弹做空策略在单个实例+品种+模式下的状态。

    这套状态来自前面的策略提示词：先排除“强上涨里的正常回撤”，再寻找结构破坏后的弱反弹。
    因此它比基线策略多保存 MA60/MA120、结构低点、强弱评分和 reaction_low 等上下文。
    """

    # 当前状态机状态；取值在 WAIT_BREAK_BELOW_MA20/TREND_STRUCTURE_FILTER/
    # WAIT_PULLBACK_TOUCH_MA20/WAIT_BREAK_REACTION_LOW/SIGNAL_ACTIVE/DONE 中切换。
    state: WeakPullbackStateName = WAIT_BREAK_BELOW_MA20
    # OHLC 序列用于计算 MA20/MA60/MA120、ATR 和结构低点；会被 _trim 控制长度。
    opens: list[float] = field(default_factory=list)
    highs: list[float] = field(default_factory=list)
    lows: list[float] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    # 最近处理过的 bar 时间戳，用于过滤重复或乱序事件。
    last_bar_ts: float | None = None
    # 上一根 bar 的 close 和 MA20，用来判断“从上向下跌破 MA20”。
    prev_close: float | None = None
    prev_ma20: float | None = None
    # 第一次跌破 MA20 的时间和样本索引；bars_since_break 统计结构确认等待时间。
    break_time: str = ""
    break_index: int = -1
    bars_since_break: int = 0
    # reaction_low 是跌破后的反抽低点；弱反弹策略要求后续再次跌破它才入场。
    reaction_low: float | None = None
    # 触碰 MA20 的反抽 K 线信息，用于 trace 解释“弱反弹”发生在哪里。
    touch_open: float | None = None
    touch_high: float | None = None
    touch_ma20: float | None = None
    touch_time: str = ""
    # 触碰 MA20 后等待跌破 reaction_low 的 bar 数。
    trigger_wait_bars: int = 0
    # regime 是结构过滤后的市场分类，当前可能是 BEARISH_REVERSAL_CANDIDATE、
    # WEAK_BEARISH_CANDIDATE 或 UNCLEAR。
    regime: WeakRegime = ""
    # 两个评分不是为了预测涨跌，而是把提示词里的“强上涨停顿”和“弱势失败”拆成可审计指标。
    bullish_pause_score: float = 0.0
    bearish_failure_score: float = 0.0
    # 信号触发后用于观察胜负的入场价、止盈价、失败价和观察计数。
    signal_entry: float | None = None
    signal_profit_target: float | None = None
    signal_adverse_target: float | None = None
    signal_bars: int = 0
    signal_time: str = ""
    signal_lowest_low: float | None = None
    signal_prev_low: float | None = None
    signal_no_new_low_bars: int = 0
    signal_rising_low_bars: int = 0
    signal_ma20_touched: bool = False
    signal_ma20_touch_low: float | None = None


# -----------------------------------------------------------------------------

class MA20WeakPullbackShortStrategy(Strategy):
    """MA20 弱反弹做空策略。

    这版策略对应“弱反弹做空”的提示词目标：不是看到价格碰 MA20 就空，而是先确认上涨结构被破坏，
    再把反抽 MA20 当成弱反弹，最后用跌破 reaction_low 作为入场确认。

    为什么只用 bar 驱动：
    - 历史回放和回测只有 bar 时也能完全复现入场，不依赖实时 tick 采样；
    - 反抽做空的核心判断是结构、均线和反应低点，bar 级别更稳定；
    - tick 在这里容易把 MA20 附近的噪声放大成不可复现的入场。
    """

    definition: StrategyDefinition = {
        "strategy_id": MA20_WEAK_STRATEGY_ID,
        "display_name": "MA20 Weak Pullback Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {
            "ma20_period": 20,
            "ma60_period": 60,
            "ma120_period": 120,
            "atr_period": 14,
            "observation_bars": 240,
            "profit_atr_multiple": 1.0,
            "adverse_atr_multiple": 1.2,
            "strength_exit_bars": 3,
            "profit_rebound_atr_multiple": 1.0,
            "profit_rising_low_bars": 2,
            "strong_bull_atr_multiple": 1.5,
            "exit_ma20_distance_atr_multiple": 0.8,
            "swing_lookback_bars": 20,
            "slope_lookback_bars": 5,
            "structure_wait_bars": 3,
            "touch_wait_bars": 12,
            "trigger_wait_bars": 6,
            "use_score_filter": True,
        },
        # Go 侧 StrategyDefinition.updated_at 是 time.Time；带 Z 的 RFC3339 能避免 JSON 解码失败。
        "updated_at": _rfc3339_utc_now(),
    }

    def __init__(self, definition: StrategyDefinition | None = None, use_score_filter: bool | None = None) -> None:
        """初始化弱反弹策略模板或运行实例。

        definition 允许注册表用拷贝后的元数据创建运行实例；
        use_score_filter 允许变体入口只修改评分过滤开关，而不复制整套状态机。
        """
        if definition is not None:
            self.definition = definition
        if use_score_filter is not None:
            default_params = dict(self.definition.get("default_params") or {})
            default_params["use_score_filter"] = bool(use_score_filter)
            self.definition = {**self.definition, "default_params": default_params}
        self.states: dict[StateKey, WeakPullbackShortState] = {}
        self._lock: RLock = RLock()

    def required_warmup_bars(self, instance: JSONObject) -> int:
        """返回弱反弹策略需要的 warmup K 线数量。

        弱反弹策略需要 MA120、斜率、ATR 和结构低点，因此默认比 baseline 更长。
        这里保留 max(140, ma120 + slope + 20)，是为了让 MA120 和 slope 都有稳定上下文，
        额外 20 根用于结构低点/反抽窗口，而不是为了提前触发交易信号。
        """
        params = _strategy_params_for_instance(self.definition, instance)
        if _int(params.get("warmup_target"), 0) > 0:
            return _int(params.get("warmup_target"), 0)
        ma120 = max(120, _int(params.get("ma120_period"), 120))
        slope = max(5, _int(params.get("slope_lookback_bars"), 5))
        return max(140, ma120 + slope + 20)

    def start_instance(self, instance: JSONObject) -> None:
        """启动策略实例并喂入 warmup。

        与 baseline 不同，弱反弹策略的 warmup 当前仍走完整 `_on_bar_locked`，
        因为它需要在锚点前建立结构/均线/ATR 的综合上下文。
        如果以后要求弱反弹也不推进交易状态，应按 baseline 的 warmup 方式单独重构。
        """
        logger.info("ma20 weak pullback start_instance args=%s", json.dumps(_instance_start_log_payload(instance), ensure_ascii=False, sort_keys=True))
        instance_id = instance.get("instance_id", "")
        symbols = instance.get("symbols") or [""]
        mode = _instance_mode(instance)
        with self._lock:
            for symbol in symbols:
                self.states[(mode, instance_id, str(symbol))] = WeakPullbackShortState()
        applied_counts = self._apply_warmup_bars(instance, (instance.get("params") or {}).get("warmup_bars") or [], mode)
        self.validate_warmup(instance, applied_counts)

    def stop_instance(self, instance_id: str) -> None:
        """清理同 instance_id 下所有 mode/symbol 的状态。"""
        with self._lock:
            for key in list(self.states):
                if len(key) >= 2 and key[1] == instance_id:
                    del self.states[key]

    def _state_for(self, request: RequestDict) -> WeakPullbackShortState:
        """根据 mode + instance_id + symbol 定位状态。"""
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            raise ValueError(
                "strategy runtime instance not started: "
                f"instance_id={key[1]} mode={key[0]} symbol={key[2]} strategy_id={self.definition.get('strategy_id', '')}"
            )
        return self.states[key]

    def _settings(self, request: RequestDict) -> StrategySettings:
        """读取弱反弹策略参数并做下限保护。

        参数可能来自 Go 前端配置、实例启动参数或默认定义。这里统一转成 int/bool，
        是为了让后续状态机只面对已规范化的值，避免每个判断分支都重复处理空值和字符串。
        """
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "ma20": max(2, _int(params.get("ma20_period"), _int(params.get("ma_period"), 20))),
            "ma60": max(3, _int(params.get("ma60_period"), 60)),
            "ma120": max(4, _int(params.get("ma120_period"), 120)),
            "atr": max(2, _int(params.get("atr_period"), 14)),
            "swing": max(2, _int(params.get("swing_lookback_bars"), 20)),
            "slope": max(1, _int(params.get("slope_lookback_bars"), 5)),
            "structure_wait": max(1, _int(params.get("structure_wait_bars"), 3)),
            "touch_wait": max(1, _int(params.get("touch_wait_bars"), 12)),
            "trigger_wait": max(1, _int(params.get("trigger_wait_bars"), 6)),
            "use_score_filter": bool(params.get("use_score_filter", True)),
        }

    def _apply_warmup_bars(self, instance: JSONObject, bars: Any, mode: str) -> dict[str, int]:
        """将启动前历史 K 线喂给弱反弹策略。

        这里捕获单根 warmup 异常后继续处理后续 K 线，是为了容忍历史数据里偶发坏行。
        最终是否足够由 validate_warmup 统一判定，避免因为一根无效 bar 直接中断整个实例启动。
        """
        instance_id = instance.get("instance_id", "")
        params = instance.get("params") or {}
        counts: dict[str, int] = {}
        for idx, bar in enumerate(bars):
            try:
                symbol = str(bar.get("symbol") or (instance.get("symbols") or [""])[0])
                req = {
                    "instance": {
                        "instance_id": instance_id,
                        "strategy_id": self.definition["strategy_id"],
                        "timeframe": instance.get("timeframe", ""),
                        "params": params,
                    },
                    "symbol": symbol,
                    "mode": mode,
                    "event_time": _bar_event_time(bar, f"warmup-{idx}"),
                    "bar": bar,
                }
                self._on_bar_locked(req)
                counts[symbol] = counts.get(symbol, 0) + 1
            except Exception as exc:
                logger.warning("weak pullback warmup bar ignored: %s", exc)
        return counts

    def validate_warmup(self, instance: JSONObject, applied_counts: dict[str, int] | None = None) -> None:
        """校验 warmup 是否足够计算弱反弹所需指标。

        只检查 MA20/MA120 与实际喂入数量，是因为这两项是后续结构过滤的最低门槛。
        ATR 和 swing low 可能在边界样本下短暂为 None，状态机已有等待分支处理。
        """
        target = self.required_warmup_bars(instance)
        if target <= 0:
            return
        instance_id = instance.get("instance_id", "")
        mode = _instance_mode(instance)
        symbols = instance.get("symbols") or [""]
        applied_counts = applied_counts or {}
        params = _strategy_params_for_instance(self.definition, instance)
        settings = {
            "ma20": max(2, _int(params.get("ma20_period"), _int(params.get("ma_period"), 20))),
            "ma60": max(3, _int(params.get("ma60_period"), 60)),
            "ma120": max(4, _int(params.get("ma120_period"), 120)),
            "atr": max(2, _int(params.get("atr_period"), 14)),
            "swing": max(2, _int(params.get("swing_lookback_bars"), 20)),
            "slope": max(1, _int(params.get("slope_lookback_bars"), 5)),
        }
        missing: list[str] = []
        with self._lock:
            for symbol in symbols:
                key = (mode, instance_id, str(symbol))
                state = self.states.get(key)
                ind = self._indicators(state, settings) if state is not None else {}
                applied = _int(applied_counts.get(str(symbol)), 0)
                enough = state is not None and ind.get("ma20") is not None and ind.get("ma120") is not None and applied >= target
                if not enough:
                    missing.append(f"{symbol}:{applied}/{target}")
        if missing:
            raise ValueError(
                "strategy warmup is insufficient for weak pullback indicators: "
                f"instance_id={instance_id} strategy_id={self.definition.get('strategy_id', '')} "
                f"mode={mode} warmup_target={target} symbols={','.join(missing)}"
            )

    def _sma(self, values: list[float], period: int) -> float | None:
        """简单移动平均线；样本不足时返回 None，让调用方明确进入等待状态。"""
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    def _slope(self, values: list[float], period: int, lookback: int) -> float:
        """计算均线斜率的近似值，用来区分“强上涨回撤”和“转弱候选”。

        这里不做复杂线性回归，是因为提示词要求的是可解释的结构过滤：
        当前 MA 与 lookback 前 MA 的差值已经足够说明均线方向。
        """
        if len(values) < period + lookback:
            return 0.0
        now = sum(values[-period:]) / period
        prev = sum(values[-period - lookback:-lookback]) / period
        return (now - prev) / lookback

    def _atr(self, state: WeakPullbackShortState, period: int) -> float | None:
        """计算平均真实波幅 ATR，作为信号观察期止盈/失败阈值的尺度。"""
        if len(state.closes) < period + 1:
            return None
        trs: list[float] = []
        start = len(state.closes) - period
        for idx in range(start, len(state.closes)):
            prev_close = state.closes[idx - 1] if idx > 0 else state.closes[idx]
            trs.append(max(
                state.highs[idx] - state.lows[idx],
                abs(state.highs[idx] - prev_close),
                abs(state.lows[idx] - prev_close),
            ))
        return sum(trs) / len(trs)

    def _last_swing_low(self, state: WeakPullbackShortState, lookback: int) -> float | None:
        """取当前 bar 之前的近期低点，作为“上涨结构是否被破坏”的参照。"""
        if len(state.lows) < 2:
            return None
        prior = state.lows[:-1]
        return min(prior[-lookback:]) if prior else None

    def _indicators(self, state: WeakPullbackShortState, settings: StrategySettings) -> MetricsDict:
        """集中计算指标，避免状态机各分支重复读取不同版本的 MA/ATR。"""
        ma20 = self._sma(state.closes, settings["ma20"])
        ma60 = self._sma(state.closes, settings["ma60"])
        ma120 = self._sma(state.closes, settings["ma120"])
        return {
            "ma20": ma20,
            "ma60": ma60,
            "ma120": ma120,
            "ma20_slope": self._slope(state.closes, settings["ma20"], settings["slope"]),
            "ma60_slope": self._slope(state.closes, settings["ma60"], settings["slope"]),
            "ma120_slope": self._slope(state.closes, settings["ma120"], settings["slope"]),
            "atr14": self._atr(state, settings["atr"]),
            "last_swing_low": self._last_swing_low(state, settings["swing"]),
        }

    def _trim(self, state: WeakPullbackShortState, settings: StrategySettings) -> None:
        """裁剪 OHLC 序列，保留足够指标窗口但避免服务长期运行时内存无限增长。"""
        keep = max(settings["ma120"] + settings["slope"] + 2, settings["swing"] + 2, settings["atr"] + 2)
        for name in ("opens", "highs", "lows", "closes"):
            values = getattr(state, name)
            if len(values) > keep:
                setattr(state, name, values[-keep:])

    def _base_metrics(self, state: WeakPullbackShortState, ind: MetricsDict | None = None) -> MetricsDict:
        """生成弱反弹策略统一诊断指标。

        metrics 同时服务日志、前端和单测，所以这里宁可字段多一些，也不要让调用方再去读内部状态对象。
        """
        ind = ind or {}
        return {
            "signal": "",
            "state": state.state,
            "regime": state.regime,
            "ma20": ind.get("ma20"),
            "ma60": ind.get("ma60"),
            "ma120": ind.get("ma120"),
            "ma20_slope": ind.get("ma20_slope"),
            "ma60_slope": ind.get("ma60_slope"),
            "ma120_slope": ind.get("ma120_slope"),
            "atr14": ind.get("atr14"),
            "last_swing_low": ind.get("last_swing_low"),
            "reaction_low": state.reaction_low,
            "touch_open": state.touch_open,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "touch_time": state.touch_time,
            "bullish_pause_score": state.bullish_pause_score,
            "bearish_failure_score": state.bearish_failure_score,
            "bars_since_break": state.bars_since_break,
            "trigger_wait_bars": state.trigger_wait_bars,
            "signal_entry": state.signal_entry,
            "profit_target": state.signal_profit_target,
            "adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_time": state.signal_time,
            "signal_lowest_low": state.signal_lowest_low,
            "signal_no_new_low_bars": state.signal_no_new_low_bars,
            "signal_rising_low_bars": state.signal_rising_low_bars,
            "signal_ma20_touched": state.signal_ma20_touched,
            "signal_ma20_touch_low": state.signal_ma20_touch_low,
            "step_total": 6,
        }

    def _bar_trace(
        self,
        request: RequestDict,
        state: WeakPullbackShortState,
        key: str,
        label: str,
        index: int,
        status: str,
        reason: str,
        checks: list[CheckDict],
        ind: MetricsDict | None = None,
        preview: dict[str, Any] | None = None,
    ) -> TraceDict:
        """构造弱反弹策略的 bar trace。"""
        return _trace(request, "bar", key, label, index, status, reason, checks, self._base_metrics(state, ind), preview)

    def _reset(self, state: WeakPullbackShortState) -> None:
        """重置形态状态，但保留 OHLC 历史样本。

        这样做是为了让下一轮形态马上能继续使用已经计算出的 MA/ATR/结构低点；
        如果把样本也清空，策略会在每次失败后重新等待很长的 warmup。
        """
        state.state = WAIT_BREAK_BELOW_MA20
        state.break_time = ""
        state.break_index = -1
        state.bars_since_break = 0
        state.reaction_low = None
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.trigger_wait_bars = 0
        state.regime = ""
        state.bullish_pause_score = 0.0
        state.bearish_failure_score = 0.0
        self._clear_signal(state)

    def _clear_signal(self, state: WeakPullbackShortState) -> None:
        """清理信号观察字段，保留 OHLC/指标样本。"""
        state.signal_entry = None
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

    def _signal_settings(self, request: RequestDict) -> StrategySettings:
        """读取信号结果观察参数。

        这些参数只影响入场后的 success/failure/unresolved 判定，不改变前面的形态识别。
        """
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "observation_bars": max(1, _int(params.get("observation_bars"), 240)),
            "profit_atr_multiple": max(0.0001, _float(params.get("profit_atr_multiple"), 1.0)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 1.2)),
            "strength_exit_bars": max(1, _int(params.get("strength_exit_bars"), 3)),
            "profit_rebound_atr_multiple": max(0.0001, _float(params.get("profit_rebound_atr_multiple"), _float(params.get("profit_atr_multiple"), 1.0))),
            "profit_rising_low_bars": max(1, _int(params.get("profit_rising_low_bars"), 2)),
            "strong_bull_atr_multiple": max(0.0001, _float(params.get("strong_bull_atr_multiple"), 1.5)),
            "exit_ma20_distance_atr_multiple": max(0.0001, _float(params.get("exit_ma20_distance_atr_multiple"), 0.8)),
        }

    def _short_trend_intact(self, close: float, ind: MetricsDict, settings: StrategySettings) -> bool:
        """空头均线结构未被破坏时继续持有，避免 MA20 附近的小反弹过早平仓。"""
        ma20, ma60 = ind.get("ma20"), ind.get("ma60")
        atr = max(0.0001, _float(ind.get("atr14"), 0.0))
        return bool(
            ma20 is not None
            and ma60 is not None
            and ma20 <= ma60
            and close <= ma60
            and close <= ma20 + settings["exit_ma20_distance_atr_multiple"] * atr
            and ind.get("ma20_slope", 0) <= 0
        )

    def _start_short_signal(
        self,
        state: WeakPullbackShortState,
        entry_price: float,
        atr: float | None,
        event_time: str,
        request: RequestDict,
    ) -> float:
        """进入 SIGNAL_ACTIVE，并设置观察期止盈/止损目标。

        entry_price 使用 reaction_low，是因为弱反弹策略的确认条件就是再次跌破该反抽低点。
        ATR 提供波动尺度，避免固定点数在不同品种或不同时段下失真。
        """
        settings = self._signal_settings(request)
        atr = max(0.0001, atr or 0.0)
        state.state = SIGNAL_ACTIVE
        state.signal_entry = entry_price
        state.signal_profit_target = entry_price - settings["profit_atr_multiple"] * atr
        state.signal_adverse_target = entry_price + settings["adverse_atr_multiple"] * atr
        state.signal_bars = 0
        state.signal_time = event_time
        state.signal_lowest_low = entry_price
        state.signal_prev_low = None
        state.signal_no_new_low_bars = 0
        state.signal_rising_low_bars = 0
        state.signal_ma20_touched = False
        state.signal_ma20_touch_low = None
        return atr

    def _evaluate_active_signal(
        self,
        request: RequestDict,
        state: WeakPullbackShortState,
        open_price: float,
        high: float,
        low: float,
        close: float,
        ind: MetricsDict,
    ) -> ResponseDict:
        """SIGNAL_ACTIVE 内观察结构性止盈、止损或窗口结束。

        做空盈利后不再用固定 ATR 止盈。只要价格没有反弹触碰 MA20，就继续持有；
        触碰 MA20 后再用反弹幅度、低点上移或强阳站稳 MA20 确认止盈。
        """
        settings = self._signal_settings(request)
        state.signal_bars += 1
        atr = max(0.0001, _float(ind.get("atr14"), 0.0))
        ma20 = ind.get("ma20")
        entry = state.signal_entry
        if state.signal_lowest_low is None or low < state.signal_lowest_low:
            state.signal_lowest_low = low
            state.signal_no_new_low_bars = 0
        else:
            state.signal_no_new_low_bars += 1
        if state.signal_prev_low is not None and low > state.signal_prev_low:
            state.signal_rising_low_bars += 1
        else:
            state.signal_rising_low_bars = 0
        state.signal_prev_low = low
        if ma20 is not None and high >= ma20:
            state.signal_ma20_touched = True
            if state.signal_ma20_touch_low is None:
                state.signal_ma20_touch_low = state.signal_lowest_low

        profitable = entry is not None and close < entry
        short_trend_intact = self._short_trend_intact(close, ind, settings)
        trend_invalidated = not short_trend_intact
        hit_adverse = state.signal_adverse_target is not None and close >= state.signal_adverse_target and trend_invalidated
        no_new_low_stop = (
            entry is not None
            and close >= entry
            and trend_invalidated
            and state.signal_no_new_low_bars >= settings["strength_exit_bars"]
        )
        stood_above_ma20_stop = (
            entry is not None
            and not profitable
            and trend_invalidated
            and ma20 is not None
            and open_price > ma20
            and close > ma20
        )
        rebound = close - (state.signal_lowest_low if state.signal_lowest_low is not None else close)
        rebound_ok = state.signal_ma20_touched and rebound >= settings["profit_rebound_atr_multiple"] * atr
        rising_low_take = profitable and trend_invalidated and rebound_ok and state.signal_rising_low_bars >= settings["profit_rising_low_bars"]
        strong_bull_take = (
            profitable
            and trend_invalidated
            and ma20 is not None
            and low > ma20
            and close > open_price
            and (close - open_price) >= settings["strong_bull_atr_multiple"] * atr
        )
        checks = [
            _check("收盘触及止损价", hit_adverse, close, state.signal_adverse_target),
            _check("亏损后连续未创新低", no_new_low_stop, state.signal_no_new_low_bars, settings["strength_exit_bars"]),
            _check("亏损后重新站上MA20", stood_above_ma20_stop, close, ma20),
            _check("空头均线趋势未失效", short_trend_intact, close, ind.get("ma60")),
            _check("未明显远离MA20", ma20 is not None and close <= ma20 + settings["exit_ma20_distance_atr_multiple"] * atr, close, ma20),
            _check("盈利后触碰MA20", state.signal_ma20_touched, high, ma20),
            _check("触碰MA20后反弹幅度达标", rebound_ok, rebound, settings["profit_rebound_atr_multiple"] * atr),
            _check("触碰MA20后低点上移", rising_low_take, state.signal_rising_low_bars, settings["profit_rising_low_bars"]),
            _check("强阳站稳MA20", strong_bull_take, close - open_price, settings["strong_bull_atr_multiple"] * atr),
            _check("观察窗口未结束", state.signal_bars < settings["observation_bars"], state.signal_bars, settings["observation_bars"]),
        ]
        if hit_adverse:
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "failure"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "信号失败", 6, "failed", "adverse target hit before profit target", checks, ind)
            self._reset(state)
            return _no_signal(request, "adverse target hit before profit target", metrics, trace)
        if no_new_low_stop or stood_above_ma20_stop:
            reason = "market strengthened before short worked"
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "failure"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "盘面转强止损", 6, "failed", reason, checks, ind)
            self._reset(state)
            return _no_signal(request, reason, metrics, trace)
        if rising_low_take or strong_bull_take:
            reason = "trend-following profit exit after MA20 reclaim"
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "success"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "结构止盈", 6, "passed", reason, checks, ind)
            self._reset(state)
            return _no_signal(request, reason, metrics, trace)
        if state.signal_bars >= settings["observation_bars"] and not profitable:
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "unresolved"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "信号观察结束", 6, "done", "observation window ended without profit/adverse target", checks, ind)
            self._reset(state)
            return _no_signal(request, "observation window ended without profit/adverse target", metrics, trace)
        trace = self._bar_trace(request, state, SIGNAL_ACTIVE, "持仓跟踪中", 6, "waiting", "holding short until structural exit", checks, ind)
        return _no_signal(request, "holding short until structural exit", self._base_metrics(state, ind), trace)

    def _strong_uptrend(self, close: float, ind: MetricsDict) -> bool:
        """判断是否仍是强上涨回撤。

        提示词里要求避免把强趋势中的正常回踩误判成做空机会，所以这里要求
        MA20 > MA60 > MA120、长中期均线仍向上、并且收盘仍在 MA60 上方。
        """
        ma20, ma60, ma120 = ind.get("ma20"), ind.get("ma60"), ind.get("ma120")
        return (
            ma20 is not None and ma60 is not None and ma120 is not None
            and ma20 > ma60 > ma120
            and ind.get("ma60_slope", 0) > 0
            and ind.get("ma120_slope", 0) > 0
            and close > ma60
        )

    def _structure_broken(self, close: float, ind: MetricsDict) -> bool:
        """用收盘跌破近期 swing low 表示上涨结构被破坏。"""
        last_swing_low = ind.get("last_swing_low")
        return last_swing_low is not None and close < last_swing_low

    def _classify_regime(self, close: float, ind: MetricsDict) -> WeakRegime:
        """把结构破坏后的市场状态压缩为少数可审计标签。"""
        if self._structure_broken(close, ind) and ind.get("ma20_slope", 0) < 0:
            return "BEARISH_REVERSAL_CANDIDATE"
        ma60 = ind.get("ma60")
        if ma60 is not None and close < ma60 and ind.get("ma20_slope", 0) < 0 and ind.get("ma60_slope", 0) <= 0:
            return "WEAK_BEARISH_CANDIDATE"
        return "UNCLEAR"

    def _scores(
        self,
        state: WeakPullbackShortState,
        open_price: float,
        low: float,
        close: float,
        ind: MetricsDict,
    ) -> tuple[float, float]:
        """计算“多头停顿分”和“空头失败分”。

        这不是机器学习评分，而是把提示词里的硬条件拆成可解释的加分项：
        strong uptrend、快速收回 MA20、结构未破坏会增加多头停顿分；
        结构破坏、均线转弱、跌破 reaction_low 会增加空头失败分。
        score_filter 变体要求 bear >= bull，hard_filter 变体则跳过这道评分闸门。
        """
        bull = 0.0
        bear = 0.0
        if self._strong_uptrend(close, ind):
            bull += 2
        ma20, ma60, ma120 = ind.get("ma20"), ind.get("ma60"), ind.get("ma120")
        if ma20 is not None and ma60 is not None and ma120 is not None and ma20 > ma60 > ma120:
            bull += 1
        structure = self._structure_broken(close, ind)
        if structure:
            bear += 2
        else:
            bull += 2
        atr = ind.get("atr14") or 0
        if ma20 is not None and atr > 0:
            break_strength = ma20 - close
            if break_strength < 0.5 * atr:
                bull += 1
            else:
                bear += 1
        if state.bars_since_break <= 3 and ma20 is not None and close > ma20:
            bull += 1
        if ind.get("ma20_slope", 0) < 0:
            bear += 1
        if ind.get("ma60_slope", 0) <= 0:
            bear += 1
        if ma20 is not None and close < ma20:
            bear += 1
        if state.reaction_low is not None and low <= state.reaction_low:
            bear += 2
        if close > open_price:
            bull += 1
        elif close < open_price:
            bear += 1
        state.bullish_pause_score = bull
        state.bearish_failure_score = bear
        return bull, bear

    def _handle_structure_filter(
        self,
        request: RequestDict,
        state: WeakPullbackShortState,
        open_price: float,
        low: float,
        close: float,
        ind: MetricsDict,
        settings: StrategySettings,
    ) -> ResponseDict:
        """执行趋势/结构过滤。

        这个阶段是弱反弹策略和基线策略最大的区别：基线只看跌破-反抽-再跌破，
        弱反弹策略先确认“上涨结构真的坏了”，否则 MA20 附近的反抽大概率只是强趋势中的停顿。
        """
        strong_up = self._strong_uptrend(close, ind)
        structure = self._structure_broken(close, ind)
        bull, bear = self._scores(state, open_price, low, close, ind)
        checks = [
            _check("不是强上涨回撤", not (strong_up and not structure), close, ind.get("ma60")),
            _check("跌破最近结构低点", structure, close, ind.get("last_swing_low")),
            _check("结构等待未超时", state.bars_since_break <= settings["structure_wait"], state.bars_since_break, settings["structure_wait"]),
        ]
        if strong_up and not structure:
            reason = "strong uptrend pullback, ignore MA20 break"
            self._reset(state)
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤", 2, "failed", reason, checks, ind)
            return _no_signal(request, reason, self._base_metrics(state, ind), trace)
        if not structure:
            if close > ind.get("ma20"):
                reason = "BULLISH_PAUSE: fast reclaim above MA20"
                self._reset(state)
                trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "快速收回 MA20", 2, "failed", reason, checks, ind)
                return _no_signal(request, reason, self._base_metrics(state, ind), trace)
            if state.bars_since_break >= settings["structure_wait"]:
                reason = "structure break not confirmed"
                self._reset(state)
                trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "结构破坏未确认", 2, "failed", reason, checks, ind)
                return _no_signal(request, reason, self._base_metrics(state, ind), trace)
            state.state = TREND_STRUCTURE_FILTER
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "等待破坏上涨结构", 2, "waiting", "waiting for swing low break", checks, ind)
            return _no_signal(request, "waiting for swing low break", self._base_metrics(state, ind), trace)
        regime = self._classify_regime(close, ind)
        state.regime = regime
        checks.append(_check("属于弱势或转空候选", regime in ("BEARISH_REVERSAL_CANDIDATE", "WEAK_BEARISH_CANDIDATE"), regime, "bearish candidate"))
        if regime not in ("BEARISH_REVERSAL_CANDIDATE", "WEAK_BEARISH_CANDIDATE"):
            reason = "not bearish regime"
            self._reset(state)
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤", 2, "failed", reason, checks, ind)
            return _no_signal(request, reason, self._base_metrics(state, ind), trace)
        state.state = WAIT_PULLBACK_TOUCH_MA20
        trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤通过", 2, "passed", regime, checks, ind)
        return _no_signal(request, regime, self._base_metrics(state, ind), trace)

    def on_bar(self, request: RequestDict) -> ResponseDict:
        """线程安全地处理实时 K 线事件。"""
        with self._lock:
            return self._on_bar_locked(request)

    def _on_bar_locked(self, request: RequestDict) -> ResponseDict:
        """处理单根 K 线并推进弱反弹状态机。

        当前弱反弹状态机仍集中在这个函数里，是为了保持原有行为不被拆分重构影响。
        关键状态依次为：
        WAIT_BREAK_BELOW_MA20 -> TREND_STRUCTURE_FILTER -> WAIT_PULLBACK_TOUCH_MA20
        -> WAIT_BREAK_REACTION_LOW -> SIGNAL_ACTIVE。
        """
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        _require_fields(bar, ("open", "high", "low", "close"))
        state = self._state_for(request)
        settings = self._settings(request)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        close = _float(bar.get("close"))
        event_time = _bar_event_time(bar, request.get("event_time", ""))
        logger.info(
            "WeakStrategy._on_bar_locked: instance_id=%s,symbol=%s,event_time=%s,close=%.4f",
            _instance_id(request),
            request.get("symbol", ""),
            event_time,
            close,
        )
        event_ts = _event_ts(event_time)
        if event_ts is not None and state.last_bar_ts is not None and event_ts <= state.last_bar_ts:
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state))
        if event_ts is not None:
            state.last_bar_ts = event_ts

        previous_ma20 = self._sma(state.closes, settings["ma20"])
        previous_close = state.prev_close
        state.opens.append(open_price)
        state.highs.append(high)
        state.lows.append(low)
        state.closes.append(close)
        self._trim(state, settings)
        ind = self._indicators(state, settings)
        ma20 = ind.get("ma20")
        if ind.get("ma120") is None or ma20 is None:
            state.prev_close = close
            state.prev_ma20 = ma20
            checks = [_check("MA120样本数量", len(state.closes) >= settings["ma120"], len(state.closes), settings["ma120"])]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", "等待 MA20/MA60/MA120 数据足够", 1, "waiting", "waiting for enough bars", checks, ind)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ind), trace)

        def finish(out: ResponseDict) -> ResponseDict:
            state.prev_close = close
            state.prev_ma20 = ma20
            return out

        if state.state == SIGNAL_ACTIVE:
            return finish(self._evaluate_active_signal(request, state, open_price, high, low, close, ind))

        if state.state == DONE:
            self._reset(state)
            trace = self._bar_trace(request, state, WAIT_BREAK_BELOW_MA20, "等待跌破 MA20", 1, "waiting", "ready for next attempt", [], ind)
            return finish(_no_signal(request, "ready for next attempt", self._base_metrics(state, ind), trace))

        if state.state == WAIT_BREAK_BELOW_MA20:
            # 首次跌破必须是“上一根在 MA20 上方、当前收盘跌到 MA20 下方”。
            # 这样可以过滤掉策略启动时已经在 MA20 下方的震荡样本。
            cross_break = previous_close is not None and previous_ma20 is not None and previous_close >= previous_ma20 and close < ma20
            checks = [
                _check("从上向下跌破MA20", cross_break, close, ma20, close - ma20),
                _check("上一根收盘在MA20上方", previous_close is not None and previous_ma20 is not None and previous_close >= previous_ma20, previous_close, previous_ma20),
            ]
            if not cross_break:
                trace = self._bar_trace(request, state, WAIT_BREAK_BELOW_MA20, "等待跌破 MA20", 1, "waiting", "waiting for MA20 cross break", checks, ind)
                return finish(_no_signal(request, "waiting for MA20 cross break", self._base_metrics(state, ind), trace))
            state.break_time = event_time
            state.break_index = len(state.closes) - 1
            state.bars_since_break = 1
            state.reaction_low = low
            out = self._handle_structure_filter(request, state, open_price, low, close, ind, settings)
            return finish(out)

        if state.state == TREND_STRUCTURE_FILTER:
            # 跌破 MA20 后继续等待结构确认；reaction_low 持续取更低值，作为后续反抽失败的确认线。
            state.bars_since_break += 1
            state.reaction_low = low if state.reaction_low is None else min(state.reaction_low, low)
            out = self._handle_structure_filter(request, state, open_price, low, close, ind, settings)
            return finish(out)

        if state.state == WAIT_PULLBACK_TOUCH_MA20:
            # 只有在结构破坏之后，反抽触碰 MA20 才被解释为“弱反弹”；
            # 如果快速重新站上 MA20，则更像强势修复，直接重置。
            state.bars_since_break += 1
            state.reaction_low = low if state.reaction_low is None else min(state.reaction_low, low)
            stood_above = open_price > ma20 and close > ma20
            wait_used = max(0, state.bars_since_break - settings["structure_wait"])
            checks = [
                _check("未重新站上MA20", not stood_above, close, ma20, close - ma20),
                _check("反抽等待未超时", wait_used <= settings["touch_wait"], wait_used, settings["touch_wait"]),
                _check("最高价触碰MA20", high >= ma20, high, ma20, high - ma20),
            ]
            if stood_above:
                reason = "reset: stood above MA20 before weak pullback"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if wait_used > settings["touch_wait"]:
                reason = "weak pullback touch timeout"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if high >= ma20:
                state.state = WAIT_BREAK_REACTION_LOW
                state.touch_open = open_price
                state.touch_high = high
                state.touch_ma20 = ma20
                state.touch_time = event_time
                state.trigger_wait_bars = 0
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "弱反触碰 MA20", 3, "passed", "weak pullback touched MA20", checks, ind)
                return finish(_no_signal(request, "weak pullback touched MA20", self._base_metrics(state, ind), trace))
            trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "waiting", "waiting for weak pullback touch", checks, ind)
            return finish(_no_signal(request, "waiting for weak pullback touch", self._base_metrics(state, ind), trace))

        if state.state == WAIT_BREAK_REACTION_LOW:
            # 最终触发不使用触碰 K 开盘价，而使用 reaction_low：
            # 弱反弹策略关心的是“反抽后的低点再次被打穿”，这比 baseline 更强调结构失败。
            state.trigger_wait_bars += 1
            stood_above = open_price > ma20 and close > ma20
            bull, bear = self._scores(state, open_price, low, close, ind)
            checks = [
                _check("未重新站上MA20", not stood_above, close, ma20, close - ma20),
                _check("触发等待未超时", state.trigger_wait_bars <= settings["trigger_wait"], state.trigger_wait_bars, settings["trigger_wait"]),
                _check("跌破反抽低点", state.reaction_low is not None and low <= state.reaction_low, low, state.reaction_low),
                _check("空头评分不低于多头停顿", (not settings["use_score_filter"]) or bear >= bull, bear, bull),
            ]
            if stood_above:
                reason = "reset: stood above MA20 before reaction low break"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if state.trigger_wait_bars > settings["trigger_wait"]:
                reason = "reaction low break timeout"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if state.reaction_low is not None and low <= state.reaction_low:
                if settings["use_score_filter"] and bull > bear:
                    reason = "bullish pause score exceeds bearish failure score"
                    self._reset(state)
                    trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "评分过滤", 4, "failed", reason, checks, ind)
                    return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
                atr = self._start_short_signal(state, state.reaction_low, ind.get("atr14") or (high - low), event_time, request)
                metrics = self._base_metrics(state, ind)
                metrics.update({
                    "signal": "SHORT",
                    "entry_price": state.reaction_low,
                    "trigger_price": state.reaction_low,
                    "reaction_low": state.reaction_low,
                    "atr14": atr,
                })
                trace = _trace(
                    request,
                    "bar",
                    SHORT_SIGNAL,
                    "做空信号",
                    5,
                    "passed",
                    "SHORT: broke weak pullback reaction low",
                    checks,
                    metrics,
                    {"target_position": -1, "confidence": 0.82, "signal": "SHORT"},
                )
                return finish({
                    "no_signal": False,
                    "instance_id": _instance_id(request),
                    "symbol": request.get("symbol", ""),
                    "event_time": request.get("event_time", ""),
                    "target_position": -1,
                    "confidence": 0.82,
                    "reason": "SHORT: broke weak pullback reaction low",
                    "metrics": metrics,
                    "trace": trace,
                })
            trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "waiting", "waiting for reaction low break", checks, ind)
            return finish(_no_signal(request, "waiting for reaction low break", self._base_metrics(state, ind), trace))

        return finish(_no_signal(request, "unknown state", self._base_metrics(state, ind)))

    def on_tick(self, request: RequestDict) -> ResponseDict:
        state = self._state_for(request)
        return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state))

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        return self.on_bar(request)


class MA20WeakPullbackVariantStrategy(MA20WeakPullbackShortStrategy):
    """弱反弹过滤变体基类。

    这个类只改 strategy_id/display_name/algorithm/use_score_filter，不改状态机。
    具体可选择策略由下面的 HardFilter/ScoreFilter 两个显式类承载。
    保留本类是为了避免入口文件或旧代码还在通过参数构造变体时失效。
    """

    def __init__(
        self,
        strategy_id: str | None = None,
        display_name: str = "",
        algorithm: str = "",
        use_score_filter: bool | None = None,
        definition: StrategyDefinition | None = None,
    ) -> None:
        if definition is None:
            definition = dict(MA20WeakPullbackShortStrategy.definition)
            default_params = dict(definition.get("default_params") or {})
            default_params.update({
                "algorithm": algorithm,
                "use_score_filter": bool(use_score_filter),
            })
            definition.update({
                "strategy_id": strategy_id or "",
                "display_name": display_name,
                "default_params": default_params,
                "updated_at": _rfc3339_utc_now(),
            })
        if use_score_filter is None:
            default_params = definition.get("default_params") or {}
            use_score_filter = bool(default_params.get("use_score_filter", True))
        super().__init__(definition=definition, use_score_filter=use_score_filter)


class MA20WeakPullbackHardFilterStrategy(MA20WeakPullbackShortStrategy):
    """页面中的 `MA20 Weak Pullback Hard Filter` 可选择策略类。

    这个类显式存在，是为了让页面上的 hard-filter 策略项能直接对应到代码。
    它继承 `MA20WeakPullbackShortStrategy`，复用弱反弹的结构过滤状态机；
    与 score-filter 的差异只在默认关闭 `use_score_filter`。
    """

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        if definition is None:
            definition = self._definition(
                MA20_WEAK_HARD_FILTER_STRATEGY_ID,
                "MA20 Weak Pullback Hard Filter",
                "hard_filter",
                False,
                "python/ma20_weak_pullback_hard_filter.py",
            )
        super().__init__(definition=definition, use_score_filter=False)

    @staticmethod
    def _definition(
        strategy_id: str,
        display_name: str,
        algorithm: str,
        use_score_filter: bool,
        entry_script: str,
    ) -> StrategyDefinition:
        definition = dict(MA20WeakPullbackShortStrategy.definition)
        default_params = dict(definition.get("default_params") or {})
        default_params.update({
            "algorithm": algorithm,
            "use_score_filter": bool(use_score_filter),
        })
        definition.update({
            "strategy_id": strategy_id,
            "display_name": display_name,
            "entry_script": entry_script,
            "default_params": default_params,
            "updated_at": _rfc3339_utc_now(),
        })
        return definition


class MA20WeakPullbackScoreFilterStrategy(MA20WeakPullbackShortStrategy):
    """页面中的 `MA20 Weak Pullback Score Filter` 可选择策略类。

    它和 hard-filter 继承同一个弱反弹基类，状态机完全一致；
    唯一区别是默认打开 score filter，要求空头失败分不低于多头停顿分后才允许做空。
    """

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        if definition is None:
            definition = MA20WeakPullbackHardFilterStrategy._definition(
                MA20_WEAK_SCORE_FILTER_STRATEGY_ID,
                "MA20 Weak Pullback Score Filter",
                "score_filter",
                True,
                "python/ma20_weak_pullback_score_filter.py",
            )
        super().__init__(definition=definition, use_score_filter=True)
