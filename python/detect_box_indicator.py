# -*- coding: utf-8 -*-
"""DetectBox 箱体整理指标策略。

算法目标：
- 作为只读指标策略运行，不产生开仓/平仓交易信号；
- 在一段 K 线中寻找“价格进入低波动压缩区间后，在水平支撑/压力之间反复震荡”的箱体整理；
- 识别成功后通过 strategy trace 输出 `BOX_DETECTED`，前端根据 trace.metrics 里的箱体上下沿和起止时间画矩形标记；
- 同时输出 `feature_key="detect_box"` 和 `feature_payload`，供组合策略里的主策略读取。

核心识别流程：
1. 保存最近 `lookback` 根 K 线，并同步维护每根 K 线的 True Range；
2. 在多个候选长度里优先寻找 K 线数量最多的横向区间；
3. 对候选区间尝试裁掉开头少量不属于箱体的 K 线，避免早期噪音撑大箱体；
4. 用收盘价线性回归作为中轴，对 high/low 残差做分位数裁剪，去除少数影线毛刺；
5. 用优化后的上下沿确认箱体高度、边界触碰、短实体和横向运动；
6. 校验收盘价不能有明显趋势斜率、首尾漂移、单边阶梯或 V/倒 V 反转；
7. 校验近期 ATR 相对基准 ATR 已经明显压缩，或绝对 TR 已经足够低；
8. 局部波峰/波谷只做辅助诊断；没有标准波峰但边界触碰充足时也允许识别；
9. 通过所有条件后，输出一个箱体事件。

重要约束：
- 这个策略只确认“形态存在”，不会直接下单；
- `last_box` 用于避免同一个箱体在每根新 K 线重复刷 trace；
- 默认参数偏保守，强调 ATR 压缩后的整理，不追求把所有横盘都标出来。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median
from threading import RLock
from typing import Any

from strategy_common import (
    Strategy,
    _bar_event_time,
    _event_ts,
    _float,
    _mode_key,
    _no_signal,
    _rfc3339_utc_now,
    _strategy_params_for_instance,
    _trace,
)
from strategy_types import DETECT_BOX_INDICATOR_ID, JSONObject, RequestDict, ResponseDict, StateKey, StrategyDefinition


@dataclass
class DetectBoxState:
    """单个运行实例、单个 symbol 的 DetectBox 运行状态。"""

    # mode 区分 live/replay，避免实盘和回放共用状态。
    mode: str
    # instance_id 对应前端启动的一次策略实例。
    instance_id: str
    # symbol 是当前状态所属合约。
    symbol: str
    # bars 保存规范化后的 K 线，字段统一为 index/adjusted_time/open/high/low/close。
    bars: list[dict[str, Any]] = field(default_factory=list)
    # trs 保存每根 K 线的 True Range，用于 ATR 和压缩率计算。
    trs: list[float] = field(default_factory=list)
    # last_event_ts 防止乱序或重复 bar 破坏滑动窗口。
    last_event_ts: float | None = None
    # next_index 是策略内部连续编号，不依赖外部 K 线数组下标。
    next_index: int = 0
    # last_box 记录最近一次已输出箱体的起止和上下沿，用于识别“同一箱体延续”。
    last_box: dict[str, Any] | None = None
    # last_box_key 保留最近 trace 的简短标识，便于日志/调试观察。
    last_box_key: str = ""


class DetectBoxIndicatorStrategy(Strategy):
    """箱体整理指标策略。

    运行方式与 ATR26 ZigZag 指标一致：继承 Strategy、响应 on_bar/on_replay_bar、
    返回 no_signal=True，并在识别到形态时附带 trace。
    """

    definition: StrategyDefinition = {
        "strategy_id": DETECT_BOX_INDICATOR_ID,
        "display_name": "DetectBox 箱体整理",
        "entry_script": "detect_box_indicator.py",
        "version": "1.0.9",
        "updated_at": _rfc3339_utc_now(),
        "default_params": {
            # lookback：每次识别使用最近多少根 K 线作为候选箱体区域。
            "lookback": 80,
            # min_box_bars：允许识别的最短横盘箱体长度。
            # 夜盘末尾到早盘开盘可能只有 10 多根有效 K 线，也应识别窄箱体。
            "min_box_bars": 10,
            # max_window_size_checks：每次从最长候选开始最多检查多少个窗口长度，避免为短框过度扫描。
            "max_window_size_checks": 24,
            # max_box_end_lag_bars：允许箱体结束在最新 K 线前几根。
            # 用于“最新 K 线已经破位，但要把破位前箱体画出来”的场景。
            "max_box_end_lag_bars": 6,
            # pivot_window：局部极值窗口；当前高点高于前后 N 根才算波峰，低点同理。
            "pivot_window": 3,
            # atr_period：基准 ATR 周期，用于衡量箱体高度和压缩程度。
            "atr_period": 26,
            # error_pct：波峰/波谷相对箱体上下沿允许的价格百分比误差。
            "error_pct": 0.01,
            # boundary_atr_factor：边界误差的 ATR 兜底，避免低价或高波动品种误差过窄。
            "boundary_atr_factor": 0.45,
            # min_pivots：局部波峰/波谷的参考下限；0 表示不作为硬门槛。
            "min_pivots": 0,
            # min_inside_ratio：候选区间内收盘价落在箱体上下沿附近的最低比例。
            "min_inside_ratio": 0.92,
            # min_alternations：波峰/波谷交替次数参考下限；只有极值数量足够时才启用。
            "min_alternations": 2,
            # min_height_atr/max_height_atr：箱体高度按 ATR 计的有效范围。
            "min_height_atr": 0.15,
            "max_height_atr": 3.0,
            # tight_box_height_pct/max_tight_box_height_atr：
            # 当箱体价格百分比已经非常窄时，允许 ATR 倍数略高，避免低 ATR 环境漏掉横盘。
            "tight_box_height_pct": 0.004,
            "max_tight_box_height_atr": 4.0,
            # compression_window：近期 ATR 计算窗口，用来判断波动是否已经压缩。
            "compression_window": 12,
            # max_compression_ratio：近期 ATR / 基准 ATR 的上限；越小越要求压缩明显。
            "max_compression_ratio": 0.65,
            # max_box_height_pct：箱体高度相对支撑位的上限，避免大区间被标为箱体。
            "max_box_height_pct": 0.018,
            # max_close_slope_pct：回归斜率投影到整个箱体后的最大价格漂移比例。
            "max_close_slope_pct": 0.004,
            # max_close_drift_pct：箱体首尾收盘价最大漂移比例。
            "max_close_drift_pct": 0.006,
            # min_small_body_ratio：候选窗口内至少多少比例的 K 线实体必须较短。
            "min_small_body_ratio": 0.68,
            # max_body_height_ratio：单根实体超过箱体高度多少比例时，不再视为短实体。
            "max_body_height_ratio": 0.35,
            # max_body_atr_ratio：单根实体超过 ATR 多少比例时，不再视为短实体。
            "max_body_atr_ratio": 0.55,
            # min_horizontal_step_ratio：至少多少比例的相邻收盘位移必须较小，防止阶梯上涨/下跌被误判。
            "min_horizontal_step_ratio": 0.55,
            # max_step_height_ratio：相邻收盘位移超过箱体高度多少比例时，视为非横向移动。
            "max_step_height_ratio": 0.35,
            # max_step_atr_ratio：相邻收盘位移超过 ATR 多少比例时，视为非横向移动。
            "max_step_atr_ratio": 0.55,
            # max_directional_move_ratio：净位移 / 总位移的上限，用于过滤一路上涨/一路下跌。
            "max_directional_move_ratio": 0.30,
            # max_half_slope_pct：前半段或后半段自身斜率上限，用于过滤 V 字、倒 V 和横盘后趋势段。
            "max_half_slope_pct": 0.002,
            # max_opposite_half_slope_sum_pct：前后半段反向斜率合计上限，用于过滤 V 字/倒 V。
            "max_opposite_half_slope_sum_pct": 0.003,
            # max_segment_mean_drift_ratio：三段平均收盘价最大漂移 / 箱体高度，过滤横盘后整体上移/下移。
            "max_segment_mean_drift_ratio": 0.50,
            # max_close_range_height_ratio：收盘价振幅 / 箱体高度，过滤收盘价从箱体一端扫到另一端的趋势段。
            "max_close_range_height_ratio": 1.05,
            # max_leading_trim_bars/max_leading_trim_ratio：
            # 允许从候选箱体开头裁掉少量后来证明不属于箱体的 K 线。
            "max_leading_trim_bars": 10,
            "max_leading_trim_ratio": 0.25,
            # regression_upper_quantile/regression_lower_quantile：
            # 基于回归中轴的 high/low 残差分位数，用于剔除少数上下影线毛刺并收窄箱体。
            "regression_upper_quantile": 0.82,
            "regression_lower_quantile": 0.18,
            # max_spike_ratio：允许被视为毛刺、落在优化箱体外的 high/low 比例。
            "max_spike_ratio": 0.22,
            # spike_tolerance_atr_ratio：优化箱体外超过多少 ATR 才计为真正毛刺。
            "spike_tolerance_atr_ratio": 0.08,
            # merge_height_growth_ratio：重叠箱体替换时，长箱体高度最多允许比旧箱体增加多少。
            "merge_height_growth_ratio": 0.25,
            # long_box_*：长时间横向压缩优先合并，允许比短窗口更高的内部漂移和毛刺比例。
            "long_box_min_bars": 18,
            "long_box_min_small_body_ratio": 0.50,
            "long_box_min_horizontal_step_ratio": 0.50,
            "long_box_max_half_slope_pct": 0.004,
            "long_box_max_segment_mean_drift_ratio": 1.50,
            "long_box_max_close_range_height_ratio": 2.20,
            "long_box_max_spike_ratio": 0.40,
            # max_bar_gap_seconds：候选箱体内部相邻 K 线允许的最大时间间隔，避免跨午休/夜盘间隔拼箱体。
            "max_bar_gap_seconds": 10800,
            # max_avg_tr_pct：当基准 ATR 已经被压低时，用绝对平均 TR 兜底判断低波动。
            "max_avg_tr_pct": 0.004,
            # min_boundary_touches：上下边界至少各有多少次触碰，替代“必须有标准波峰/波谷”的硬要求。
            "min_boundary_touches": 2,
            # boundary_touch_ratio：距离箱体边界多少比例以内算触碰。
            "boundary_touch_ratio": 0.3,
            # warmup_target：启动策略时至少预热的历史 K 线数量。
            "warmup_target": 80,
        },
    }

    def __init__(self, definition: StrategyDefinition | None = None) -> None:
        """初始化策略模板或运行实例。

        definition 由注册表 clone 后传入，保证每个运行实例有独立参数副本。
        """
        if definition is not None:
            self.definition = definition
        # _states 按 (mode, instance_id, symbol) 分隔状态。
        self._states: dict[StateKey, DetectBoxState] = {}
        # _lock 保护策略状态，避免 HTTP runtime 并发调用时出现竞态。
        self._lock = RLock()

    def required_warmup_bars(self, instance: JSONObject) -> int:
        """返回启动前所需的预热 K 线数量。

        箱体识别至少需要覆盖 lookback，否则刚启动时无法构造完整候选区间。
        """
        params = _strategy_params_for_instance(self.definition, instance)
        lookback = max(20, int(_float(params.get("lookback"), 80)))
        return max(lookback, int(_float(params.get("warmup_target"), lookback)))

    def start_instance(self, instance: JSONObject) -> None:
        """启动实例并消费 warmup_bars。

        Go 侧启动策略时会把锚点前的历史 K 线放入 params.warmup_bars。
        这里先把历史 K 线喂入状态，后续实时/回放 bar 到来时即可立刻识别。
        """
        params = _strategy_params_for_instance(self.definition, instance)
        warmup_bars = params.get("warmup_bars")
        symbols = instance.get("symbols") if isinstance(instance.get("symbols"), list) else []
        with self._lock:
            for symbol in symbols:
                state = self._state_for(instance, str(symbol))
                if isinstance(warmup_bars, list):
                    for bar in warmup_bars:
                        if isinstance(bar, dict):
                            self._append_bar(state, bar, params)

    def stop_instance(self, instance_id: str) -> None:
        """停止实例时清理该 instance_id 下所有 symbol/mode 状态。"""
        with self._lock:
            for key in list(self._states.keys()):
                if key[1] == instance_id:
                    del self._states[key]

    def on_bar(self, request: RequestDict) -> ResponseDict:
        """处理一根 K 线并在识别成功时输出 BOX_DETECTED trace。

        返回值始终是 no_signal=True，因为 DetectBox 是指标策略；
        真正的交易决策应由组合策略中的主策略读取 features 后完成。
        """
        bar = request.get("bar") or {}
        if not isinstance(bar, dict):
            return _no_signal(request, "no bar")
        params = _strategy_params_for_instance(self.definition, request.get("instance") or {})
        with self._lock:
            state = self._state_for(request.get("instance") or {}, str(request.get("symbol") or ""))
            event_ts = _event_ts(_bar_event_time(bar, request.get("event_time")))
            if state.last_event_ts is not None and event_ts is not None and event_ts <= state.last_event_ts:
                return _no_signal(request, "duplicate or out-of-order detect-box bar", self._metrics(state, params), None, False)
            if event_ts is not None:
                state.last_event_ts = event_ts
            self._append_bar(state, bar, params)
            box = self._detect_box(state, params)
            if box is None:
                atr_period = max(1, int(_float(params.get("atr_period"), 26)))
                atr = self._atr(state.trs, atr_period)
                if atr is not None and atr > 0:
                    box = self._extend_last_box(state, params, atr)
            metrics = self._metrics(state, params)
            if box is None:
                return _no_signal(request, "waiting for box consolidation", metrics, None, False)

            # 同一段箱体会随着滚动窗口持续被识别；这里只在新箱体出现时输出 trace。
            if self._same_box_as_last(state, box):
                return _no_signal(request, "box consolidation unchanged", metrics, None, False)
            key = f"{box['box_start_index']}-{box['box_support']:.8f}-{box['box_resistance']:.8f}"
            state.last_box_key = key
            state.last_box = {
                "start_index": box["box_start_index"],
                "end_index": box["box_end_index"],
                "support": box["box_support"],
                "resistance": box["box_resistance"],
                "bar_count": box.get("box_optimized_bar_count"),
                "height": box["box_resistance"] - box["box_support"],
                "base_height": box.get("box_base_height", box["box_resistance"] - box["box_support"]),
                "leading_trim_bars": box.get("box_leading_trim_bars"),
            }
            metrics.update(box)
            # trace 是前端画箱体和组合策略 feature 注入的正式输出协议。
            trace = _trace(
                request,
                "bar",
                "BOX_DETECTED",
                "识别箱体整理",
                1,
                "passed",
                "box consolidation detected",
                [],
                metrics,
                {"indicator": "detect_box", "box": True},
            )
            return _no_signal(request, "box consolidation detected", metrics, trace, False)

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        """回放 K 线使用与实时 K 线完全相同的识别逻辑。"""
        return self.on_bar(request)

    def _state_for(self, instance: JSONObject, symbol: str) -> DetectBoxState:
        """按 mode + instance_id + symbol 获取或创建状态。"""
        key: StateKey = (_mode_key({"mode": instance.get("mode")}), str(instance.get("instance_id") or ""), symbol)
        state = self._states.get(key)
        if state is None:
            state = DetectBoxState(mode=key[0], instance_id=key[1], symbol=key[2])
            self._states[key] = state
        return state

    def _append_bar(self, state: DetectBoxState, bar: dict[str, Any], params: dict[str, Any]) -> None:
        """把外部 bar 规范化后追加到滑动窗口，并同步计算 True Range。"""
        normalized = self._normalize_bar(bar, state.next_index)
        state.next_index += 1
        tr = self._true_range(state.bars[-1] if state.bars else None, normalized)
        state.bars.append(normalized)
        state.trs.append(tr)
        # keep 比 lookback 稍大，给 pivot_window 和 ATR 压缩计算留出缓冲。
        keep = max(128, int(_float(params.get("lookback"), 80)) + int(_float(params.get("pivot_window"), 5)) * 4 + 16)
        if len(state.bars) > keep:
            drop = len(state.bars) - keep
            state.bars = state.bars[drop:]
            state.trs = state.trs[drop:]

    def _detect_box(self, state: DetectBoxState, params: dict[str, Any]) -> dict[str, Any] | None:
        """执行箱体识别，成功时返回 trace metrics，失败时返回 None。

        这里的过滤顺序是从低成本到高约束：
        数据量/ATR -> 多长度候选扫描 -> 箱体高度 -> 横向程度 -> ATR/绝对波动压缩 -> 边界误差 -> 箱内比例。
        任一条件失败都直接返回 None。
        """
        lookback = max(20, int(_float(params.get("lookback"), 80)))
        min_box_bars = max(8, int(_float(params.get("min_box_bars"), 16)))
        pivot_window = max(1, int(_float(params.get("pivot_window"), 5)))
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        atr = self._atr(state.trs, atr_period)
        if atr is None or atr <= 0:
            return None
        max_window = min(lookback, len(state.bars))
        if max_window < max(min_box_bars, pivot_window * 2 + 5):
            return None
        min_window = max(min_box_bars, pivot_window * 2 + 5)

        # 从长到短扫描候选区间。长箱体优先，短箱体也不会因为固定 lookback 太长而漏掉。
        # end_lag=1 能在最新 K 线破位时，仍然输出“上一根 K 线前已经形成的箱体”。
        max_end_lag = max(0, int(_float(params.get("max_box_end_lag_bars"), 1)))
        for end_lag in range(0, max_end_lag + 1):
            available = len(state.bars) - end_lag
            if available < min_window:
                continue
            available_window = min(lookback, available)
            min_scanned_window = max(min_window, available_window - max(1, int(_float(params.get("max_window_size_checks"), 24))) + 1)
            for window_size in range(available_window, min_scanned_window - 1, -1):
                candidate = state.bars[available - window_size : available]
                out = self._detect_box_candidate(state, candidate, params, atr, pivot_window, atr_period)
                if out is not None:
                    if self._overlaps_last_box(state, out, params):
                        continue
                    return out
        return None

    def _detect_box_candidate(
        self,
        state: DetectBoxState,
        bars: list[dict[str, Any]],
        params: dict[str, Any],
        atr: float,
        pivot_window: int,
        atr_period: int,
    ) -> dict[str, Any] | None:
        """检测一个具体长度的候选窗口。

        同一个窗口内会尝试裁掉开头少量 K 线，并对每个裁剪后的窗口做回归去毛刺。
        最终优先选择 K 线数量最多、其次箱体高度更窄的结果。
        """
        min_box_bars = max(8, int(_float(params.get("min_box_bars"), 10)))
        max_trim = min(
            max(0, int(_float(params.get("max_leading_trim_bars"), 10))),
            max(0, int(len(bars) * _float(params.get("max_leading_trim_ratio"), 0.25))),
            max(0, len(bars) - min_box_bars),
        )
        best: dict[str, Any] | None = None
        for trim in range(0, max_trim + 1):
            candidate = bars[trim:]
            out = self._detect_box_candidate_core(state, candidate, params, atr, pivot_window, atr_period, trim)
            if out is not None and (best is None or self._box_score(out) > self._box_score(best)):
                best = out
        return best

    def _detect_box_candidate_core(
        self,
        state: DetectBoxState,
        bars: list[dict[str, Any]],
        params: dict[str, Any],
        atr: float,
        pivot_window: int,
        atr_period: int,
        leading_trim_bars: int,
    ) -> dict[str, Any] | None:
        """检测已经裁剪过开头的候选窗口。"""
        min_pivots = max(0, int(_float(params.get("min_pivots"), 0)))
        if self._max_bar_gap_seconds(bars) > _float(params.get("max_bar_gap_seconds"), 1800):
            return None

        # 1. 居中窗口找局部波峰/波谷。它们用于诊断和辅助确认，不再是窄横盘的硬门槛。
        peaks: list[dict[str, Any]] = []
        troughs: list[dict[str, Any]] = []
        for i in range(pivot_window, len(bars) - pivot_window):
            center = bars[i]
            window = bars[i - pivot_window : i + pivot_window + 1]
            high = center["high"]
            low = center["low"]
            if high == max(item["high"] for item in window):
                peaks.append({"type": "PEAK", "bar": center, "price": high})
            if low == min(item["low"] for item in window):
                troughs.append({"type": "TROUGH", "bar": center, "price": low})

        # 2. 用回归中轴 + 残差分位数优化箱体上下沿。
        #    raw_high/raw_low 只用于诊断；真正画框的 support/resistance 会剔除少量影线毛刺。
        raw_resistance = max(item["high"] for item in bars)
        raw_support = min(item["low"] for item in bars)
        optimized_bounds = self._optimized_box_bounds(bars, params)
        if optimized_bounds is None:
            return None
        resistance = optimized_bounds["resistance"]
        support = optimized_bounds["support"]
        peak_mid = float(median([item["price"] for item in peaks])) if peaks else resistance
        trough_mid = float(median([item["price"] for item in troughs])) if troughs else support
        if resistance <= support:
            return None

        # 3. 箱体高度必须处于合理范围：
        #    - 太小可能只是噪音；
        #    - 太大说明并非压缩整理；
        #    - 高度百分比用于过滤价格绝对区间过宽的候选。
        height = resistance - support
        height_pct = height / support if support else 0
        height_atr = height / atr
        max_height_atr = _float(params.get("max_height_atr"), 3.0)
        if height_pct <= _float(params.get("tight_box_height_pct"), 0.004):
            max_height_atr = max(max_height_atr, _float(params.get("max_tight_box_height_atr"), 4.0))
        if height_atr < _float(params.get("min_height_atr"), 0.8) or height_atr > max_height_atr:
            return None
        if height_pct > _float(params.get("max_box_height_pct"), 0.018):
            return None
        spike_tolerance = max(0.0, atr * _float(params.get("spike_tolerance_atr_ratio"), 0.08))
        is_long_box = len(bars) >= int(_float(params.get("long_box_min_bars"), 18))
        upper_spikes = sum(1 for item in bars if item["high"] > resistance + spike_tolerance)
        lower_spikes = sum(1 for item in bars if item["low"] < support - spike_tolerance)
        max_spike_ratio = _float(params.get("max_spike_ratio"), 0.22)
        if is_long_box:
            max_spike_ratio = max(max_spike_ratio, _float(params.get("long_box_max_spike_ratio"), 0.40))
        max_spikes = max(0, int(len(bars) * max_spike_ratio))
        if upper_spikes > max_spikes or lower_spikes > max_spikes:
            return None

        # 4. 横向运动过滤。
        #    目标形态是 K 线横向移动，不是缓慢单边下跌或上涨形成的窄通道。
        slope_pct = self._close_slope_pct(bars)
        if abs(slope_pct) > _float(params.get("max_close_slope_pct"), 0.004):
            return None
        drift_pct = self._close_drift_pct(bars)
        if drift_pct > _float(params.get("max_close_drift_pct"), 0.006):
            return None
        body_quality = self._body_quality(bars, height, atr, params)
        min_small_body_ratio = _float(params.get("min_small_body_ratio"), 0.68)
        if is_long_box:
            min_small_body_ratio = min(min_small_body_ratio, _float(params.get("long_box_min_small_body_ratio"), 0.50))
        if body_quality["small_body_ratio"] < min_small_body_ratio:
            return None
        motion_quality = self._horizontal_motion_quality(bars, height, atr, params)
        min_horizontal_step_ratio = _float(params.get("min_horizontal_step_ratio"), 0.55)
        if is_long_box:
            min_horizontal_step_ratio = min(min_horizontal_step_ratio, _float(params.get("long_box_min_horizontal_step_ratio"), 0.50))
        if motion_quality["horizontal_step_ratio"] < min_horizontal_step_ratio:
            return None
        if motion_quality["directional_move_ratio"] > _float(params.get("max_directional_move_ratio"), 0.30):
            return None
        half_slope_limit = _float(params.get("max_half_slope_pct"), 0.002)
        if is_long_box:
            half_slope_limit = max(half_slope_limit, _float(params.get("long_box_max_half_slope_pct"), 0.004))
        if max(abs(motion_quality["first_half_slope_pct"]), abs(motion_quality["second_half_slope_pct"])) > half_slope_limit:
            return None
        opposite_half_slope_sum = motion_quality["opposite_half_slope_sum_pct"]
        if opposite_half_slope_sum > _float(params.get("max_opposite_half_slope_sum_pct"), 0.003):
            return None
        max_segment_mean_drift_ratio = _float(params.get("max_segment_mean_drift_ratio"), 0.50)
        if is_long_box:
            max_segment_mean_drift_ratio = max(max_segment_mean_drift_ratio, _float(params.get("long_box_max_segment_mean_drift_ratio"), 1.50))
        if motion_quality["segment_mean_drift_ratio"] > max_segment_mean_drift_ratio:
            return None
        max_close_range_height_ratio = _float(params.get("max_close_range_height_ratio"), 1.05)
        if is_long_box:
            max_close_range_height_ratio = max(max_close_range_height_ratio, _float(params.get("long_box_max_close_range_height_ratio"), 2.20))
        if motion_quality["close_range_height_ratio"] > max_close_range_height_ratio:
            return None

        # 5. ATR 压缩过滤。
        #    recent_atr 表示最近几根 K 线的平均真实波幅；
        #    baseline_atr 表示较长周期的基准波幅；
        #    recent_tr_pct 是绝对低波动兜底，处理“基准 ATR 已经被长箱体压低”的情况。
        compression_window = max(3, int(_float(params.get("compression_window"), 12)))
        if len(state.trs) < max(atr_period, compression_window * 2):
            return None
        recent_atr = sum(state.trs[-compression_window:]) / compression_window
        baseline_atr = self._atr(state.trs, atr_period)
        if baseline_atr is None or baseline_atr <= 0:
            return None
        compression_ratio = recent_atr / baseline_atr
        recent_tr_pct = recent_atr / max(1e-9, (support + resistance) * 0.5)
        compression_ok = compression_ratio <= _float(params.get("max_compression_ratio"), 0.65)
        absolute_low_vol_ok = recent_tr_pct <= _float(params.get("max_avg_tr_pct"), 0.004)
        if not compression_ok and not absolute_low_vol_ok:
            return None

        # 6. 边界误差过滤。
        #    误差上限取“价格百分比”和“ATR 倍数”中的较大值，兼顾不同价位和波动环境。
        error_pct = max(0.0, _float(params.get("error_pct"), 0.015))
        boundary_atr_factor = max(0.0, _float(params.get("boundary_atr_factor"), 0.6))
        peak_limit = max(abs(peak_mid) * error_pct, atr * boundary_atr_factor)
        trough_limit = max(abs(trough_mid) * error_pct, atr * boundary_atr_factor)
        if peaks and max(abs(item["price"] - peak_mid) for item in peaks) > peak_limit:
            return None
        if troughs and max(abs(item["price"] - trough_mid) for item in troughs) > trough_limit:
            return None

        # 7. 边界触碰过滤。目标图形是价格横向贴着上下沿小幅移动，
        #    因此即使没有标准波峰/波谷，也应通过高低价触碰次数确认箱体有效。
        touch_tolerance = max(height * _float(params.get("boundary_touch_ratio"), 0.3), atr * 0.15)
        upper_touches = sum(1 for item in bars if item["high"] >= resistance - touch_tolerance)
        lower_touches = sum(1 for item in bars if item["low"] <= support + touch_tolerance)
        min_touches = max(1, int(_float(params.get("min_boundary_touches"), 2)))
        if upper_touches < min_touches or lower_touches < min_touches:
            return None
        if len(peaks) < min_pivots or len(troughs) < min_pivots:
            return None

        # 8. 箱内比例过滤。允许少量影线或短暂噪音，但大部分收盘价必须在箱体范围内。
        inside_count = sum(1 for item in bars if support - trough_limit <= item["close"] <= resistance + peak_limit)
        inside_ratio = inside_count / len(bars)
        if inside_ratio < _float(params.get("min_inside_ratio"), 0.92):
            return None

        # 9. 高低点交替过滤。只有极值数量足够时启用，避免漏掉“贴边横走但无标准波峰”的箱体。
        pivots = sorted([*peaks, *troughs], key=lambda item: int(item["bar"]["index"]))
        alternations = 0
        previous = ""
        for item in pivots:
            typ = item["type"]
            if previous and typ != previous:
                alternations += 1
            previous = typ
        min_alternations = int(_float(params.get("min_alternations"), 2))
        if len(peaks) >= min_pivots and len(troughs) >= min_pivots and min_pivots > 0 and alternations < min_alternations:
            return None

        # 10. 记录当前最后一根 K 线是否已经收盘突破箱体。
        #    突破状态目前只作为 metrics 输出，不直接生成交易信号。
        start = bars[0]
        end = bars[-1]
        breakout = "none"
        if end["close"] > resistance + peak_limit:
            breakout = "up"
        elif end["close"] < support - trough_limit:
            breakout = "down"
        payload = {
            "support": support,
            "resistance": resistance,
            "raw_support": raw_support,
            "raw_resistance": raw_resistance,
            "base_height": height,
            "leading_trim_bars": leading_trim_bars,
            "optimized_bar_count": len(bars),
            "upper_spikes": upper_spikes,
            "lower_spikes": lower_spikes,
            "regression_mid": optimized_bounds["regression_mid"],
            "regression_slope": optimized_bounds["regression_slope"],
            "start_index": start["index"],
            "end_index": end["index"],
            "start_time": start["adjusted_time"],
            "end_time": end["adjusted_time"],
            "height_pct": height_pct,
            "height_atr": height_atr,
            "recent_atr": recent_atr,
            "baseline_atr": baseline_atr,
            "compression_ratio": compression_ratio,
            "recent_tr_pct": recent_tr_pct,
            "close_slope_pct": slope_pct,
            "close_drift_pct": drift_pct,
            "small_body_ratio": body_quality["small_body_ratio"],
            "max_body_height_ratio": body_quality["max_body_height_ratio"],
            "avg_body_height_ratio": body_quality["avg_body_height_ratio"],
            "horizontal_step_ratio": motion_quality["horizontal_step_ratio"],
            "directional_move_ratio": motion_quality["directional_move_ratio"],
            "first_half_slope_pct": motion_quality["first_half_slope_pct"],
            "second_half_slope_pct": motion_quality["second_half_slope_pct"],
            "opposite_half_slope_sum_pct": opposite_half_slope_sum,
            "segment_mean_drift_ratio": motion_quality["segment_mean_drift_ratio"],
            "close_range_height_ratio": motion_quality["close_range_height_ratio"],
            "inside_ratio": inside_ratio,
            "peak_count": len(peaks),
            "trough_count": len(troughs),
            "alternations": alternations,
            "upper_touches": upper_touches,
            "lower_touches": lower_touches,
            "breakout": breakout,
        }
        # 返回字段分两层：
        # - box_* 是前端和 trace 列表直接展示/绘图用；
        # - feature_key/feature_payload 是组合策略通用 feature 注入协议。
        return {
            "indicator": "detect_box",
            "signal": "BOX",
            "box_support": support,
            "box_resistance": resistance,
            "box_raw_support": payload["raw_support"],
            "box_raw_resistance": payload["raw_resistance"],
            "box_base_height": payload["base_height"],
            "box_leading_trim_bars": payload["leading_trim_bars"],
            "box_optimized_bar_count": payload["optimized_bar_count"],
            "box_upper_spikes": payload["upper_spikes"],
            "box_lower_spikes": payload["lower_spikes"],
            "box_regression_mid": payload["regression_mid"],
            "box_regression_slope": payload["regression_slope"],
            "box_start_index": payload["start_index"],
            "box_end_index": payload["end_index"],
            "box_start_time": payload["start_time"],
            "box_end_time": payload["end_time"],
            "box_height_pct": payload["height_pct"],
            "box_height_atr": payload["height_atr"],
            "box_recent_atr": payload["recent_atr"],
            "box_baseline_atr": payload["baseline_atr"],
            "box_compression_ratio": payload["compression_ratio"],
            "box_recent_tr_pct": payload["recent_tr_pct"],
            "box_close_slope_pct": payload["close_slope_pct"],
            "box_close_drift_pct": payload["close_drift_pct"],
            "box_small_body_ratio": payload["small_body_ratio"],
            "box_max_body_height_ratio": payload["max_body_height_ratio"],
            "box_avg_body_height_ratio": payload["avg_body_height_ratio"],
            "box_horizontal_step_ratio": payload["horizontal_step_ratio"],
            "box_directional_move_ratio": payload["directional_move_ratio"],
            "box_first_half_slope_pct": payload["first_half_slope_pct"],
            "box_second_half_slope_pct": payload["second_half_slope_pct"],
            "box_opposite_half_slope_sum_pct": payload["opposite_half_slope_sum_pct"],
            "box_segment_mean_drift_ratio": payload["segment_mean_drift_ratio"],
            "box_close_range_height_ratio": payload["close_range_height_ratio"],
            "box_inside_ratio": payload["inside_ratio"],
            "box_peak_count": payload["peak_count"],
            "box_trough_count": payload["trough_count"],
            "box_alternations": payload["alternations"],
            "box_upper_touches": payload["upper_touches"],
            "box_lower_touches": payload["lower_touches"],
            "box_breakout": payload["breakout"],
            "feature_key": "detect_box",
            "feature_payload": payload,
        }

    def _box_score(self, box: dict[str, Any]) -> tuple[int, float, int]:
        """给候选箱体打分。

        箱体价值首先来自横向压缩持续的 K 线数量，其次才是高度更窄；
        如果数量和高度接近，少裁剪开头 K 线的候选优先。
        """
        bar_count = int(_float(box.get("box_optimized_bar_count"), 0))
        height = max(0.0, _float(box.get("box_resistance"), 0) - _float(box.get("box_support"), 0))
        trim = int(_float(box.get("box_leading_trim_bars"), 0))
        return (bar_count, -height, -trim)

    def _extend_last_box(self, state: DetectBoxState, params: dict[str, Any], atr: float) -> dict[str, Any] | None:
        """按“高度小幅增加则尽量合并”的规则向右扩展上一个箱体。

        普通检测会重新审视整段走势，容易因为分段均值漂移而把长箱体截断；
        扩展模式以上一个已成立箱体为基准，只要求新增 K 线仍是短实体横向 K 线，
        并且优化后箱体高度不超过旧箱体高度的 `1 + merge_height_growth_ratio`。
        """
        last = state.last_box
        if not isinstance(last, dict):
            return None
        start_index = int(_float(last.get("start_index"), -1))
        end_index = int(_float(last.get("end_index"), -1))
        last_height = max(0.0, _float(last.get("base_height"), _float(last.get("height"), 0)))
        last_count = int(_float(last.get("bar_count"), 0))
        if start_index < 0 or end_index < 0 or last_height <= 0:
            return None
        start_pos = next((i for i, item in enumerate(state.bars) if int(item.get("index", -1)) == start_index), -1)
        if start_pos < 0:
            return None
        growth_ratio = _float(params.get("merge_height_growth_ratio"), 0.25)
        max_height = last_height * (1 + growth_ratio)
        max_end_lag = max(0, int(_float(params.get("max_box_end_lag_bars"), 6)))
        best: dict[str, Any] | None = None
        for end_lag in range(0, max_end_lag + 1):
            end_pos = len(state.bars) - end_lag
            if end_pos <= start_pos:
                continue
            bars = state.bars[start_pos:end_pos]
            if len(bars) <= last_count or int(bars[-1].get("index", -1)) <= end_index:
                continue
            bounds = self._optimized_box_bounds(bars, params)
            if bounds is None:
                continue
            support = bounds["support"]
            resistance = bounds["resistance"]
            height = resistance - support
            if height <= 0 or height > max_height:
                continue
            new_bars = [item for item in bars if int(item.get("index", -1)) > end_index]
            if not new_bars:
                continue
            if self._max_bar_gap_seconds(new_bars) > _float(params.get("max_bar_gap_seconds"), 10800):
                continue
            body_quality = self._body_quality(new_bars, height, atr, params)
            min_body = min(_float(params.get("min_small_body_ratio"), 0.68), _float(params.get("long_box_min_small_body_ratio"), 0.50))
            if body_quality["small_body_ratio"] < min_body:
                continue
            spike_tolerance = max(0.0, atr * _float(params.get("spike_tolerance_atr_ratio"), 0.08))
            upper_spikes = sum(1 for item in bars if item["high"] > resistance + spike_tolerance)
            lower_spikes = sum(1 for item in bars if item["low"] < support - spike_tolerance)
            max_spike_ratio = max(_float(params.get("max_spike_ratio"), 0.22), _float(params.get("long_box_max_spike_ratio"), 0.40))
            max_spikes = max(0, int(len(bars) * max_spike_ratio))
            if upper_spikes > max_spikes or lower_spikes > max_spikes:
                continue
            new_inside_ratio = sum(1 for item in new_bars if support <= item["close"] <= resistance) / len(new_bars)
            if new_inside_ratio < _float(params.get("min_inside_ratio"), 0.92):
                continue
            motion = self._horizontal_motion_quality(bars, height, atr, params)
            if motion["directional_move_ratio"] > _float(params.get("max_directional_move_ratio"), 0.30):
                continue
            raw_resistance = max(item["high"] for item in bars)
            raw_support = min(item["low"] for item in bars)
            touch_tolerance = max(height * _float(params.get("boundary_touch_ratio"), 0.3), atr * 0.15)
            upper_touches = sum(1 for item in bars if item["high"] >= resistance - touch_tolerance)
            lower_touches = sum(1 for item in bars if item["low"] <= support + touch_tolerance)
            compression_window = max(3, int(_float(params.get("compression_window"), 12)))
            recent_atr = sum(state.trs[-compression_window:]) / compression_window if len(state.trs) >= compression_window else atr
            compression_ratio = recent_atr / atr if atr > 0 else None
            recent_tr_pct = recent_atr / max(1e-9, (support + resistance) * 0.5)
            all_body_quality = self._body_quality(bars, height, atr, params)
            start = bars[0]
            end = bars[-1]
            payload = {
                "indicator": "detect_box",
                "signal": "BOX",
                "box_support": support,
                "box_resistance": resistance,
                "box_raw_support": raw_support,
                "box_raw_resistance": raw_resistance,
                "box_base_height": last_height,
                "box_leading_trim_bars": int(_float(last.get("leading_trim_bars"), 0)),
                "box_optimized_bar_count": len(bars),
                "box_upper_spikes": upper_spikes,
                "box_lower_spikes": lower_spikes,
                "box_regression_mid": bounds["regression_mid"],
                "box_regression_slope": bounds["regression_slope"],
                "box_start_index": start["index"],
                "box_end_index": end["index"],
                "box_start_time": start["adjusted_time"],
                "box_end_time": end["adjusted_time"],
                "box_height_pct": height / support if support else 0,
                "box_height_atr": height / atr,
                "box_recent_atr": recent_atr,
                "box_baseline_atr": atr,
                "box_compression_ratio": compression_ratio,
                "box_recent_tr_pct": recent_tr_pct,
                "box_close_slope_pct": self._close_slope_pct(bars),
                "box_close_drift_pct": self._close_drift_pct(bars),
                "box_small_body_ratio": all_body_quality["small_body_ratio"],
                "box_max_body_height_ratio": all_body_quality["max_body_height_ratio"],
                "box_avg_body_height_ratio": all_body_quality["avg_body_height_ratio"],
                "box_horizontal_step_ratio": motion["horizontal_step_ratio"],
                "box_directional_move_ratio": motion["directional_move_ratio"],
                "box_first_half_slope_pct": motion["first_half_slope_pct"],
                "box_second_half_slope_pct": motion["second_half_slope_pct"],
                "box_opposite_half_slope_sum_pct": motion["opposite_half_slope_sum_pct"],
                "box_segment_mean_drift_ratio": motion["segment_mean_drift_ratio"],
                "box_close_range_height_ratio": motion["close_range_height_ratio"],
                "box_inside_ratio": sum(1 for item in bars if support <= item["close"] <= resistance) / len(bars),
                "box_peak_count": 0,
                "box_trough_count": 0,
                "box_alternations": 0,
                "box_upper_touches": upper_touches,
                "box_lower_touches": lower_touches,
                "box_breakout": "none",
                "feature_key": "detect_box",
            }
            feature_payload = {
                "support": support,
                "resistance": resistance,
                "raw_support": raw_support,
                "raw_resistance": raw_resistance,
                "start_index": start["index"],
                "end_index": end["index"],
                "start_time": start["adjusted_time"],
                "end_time": end["adjusted_time"],
                "height_pct": payload["box_height_pct"],
                "height_atr": payload["box_height_atr"],
                "optimized_bar_count": len(bars),
                "upper_spikes": upper_spikes,
                "lower_spikes": lower_spikes,
            }
            payload["feature_payload"] = feature_payload
            if best is None or self._box_score(payload) > self._box_score(best):
                best = payload
        return best

    def _optimized_box_bounds(self, bars: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, float] | None:
        """通过回归中轴和残差分位数得到去毛刺后的箱体上下沿。

        做法：
        1. 对收盘价做线性回归，得到这段横向运动的中轴；
        2. 计算每根 high/low 相对回归中轴的残差；
        3. 取 high 残差的较高分位数和 low 残差的较低分位数作为有效上下沿；
        4. 少数超出上下沿的影线被视为毛刺，不再撑大矩形高度。
        """
        if len(bars) < 2:
            return None
        closes = [_float(item.get("close"), 0) for item in bars]
        slope, intercept = self._linear_regression(closes)
        predictions = [intercept + slope * i for i in range(len(bars))]
        mid = sum(predictions) / len(predictions)
        high_residuals = [_float(item.get("high"), 0) - predictions[i] for i, item in enumerate(bars)]
        low_residuals = [_float(item.get("low"), 0) - predictions[i] for i, item in enumerate(bars)]
        upper_q = min(1.0, max(0.5, _float(params.get("regression_upper_quantile"), 0.82)))
        lower_q = max(0.0, min(0.5, _float(params.get("regression_lower_quantile"), 0.18)))
        resistance = mid + self._percentile(high_residuals, upper_q)
        support = mid + self._percentile(low_residuals, lower_q)
        if resistance <= support:
            return None
        return {
            "support": support,
            "resistance": resistance,
            "regression_mid": mid,
            "regression_slope": slope,
        }

    def _linear_regression(self, values: list[float]) -> tuple[float, float]:
        """对 y=values 做一元线性回归，返回 slope/intercept。"""
        n = len(values)
        if n < 2:
            return 0.0, values[0] if values else 0.0
        xs = list(range(n))
        x_mean = sum(xs) / n
        y_mean = sum(values) / n
        denom = sum((x - x_mean) ** 2 for x in xs)
        if denom <= 0:
            return 0.0, y_mean
        slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values)) / denom
        intercept = y_mean - slope * x_mean
        return slope, intercept

    def _percentile(self, values: list[float], q: float) -> float:
        """返回 values 的线性插值分位数。"""
        if not values:
            return 0.0
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        pos = (len(ordered) - 1) * min(1.0, max(0.0, q))
        lower = int(pos)
        upper = min(len(ordered) - 1, lower + 1)
        weight = pos - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    def _same_box_as_last(self, state: DetectBoxState, box: dict[str, Any]) -> bool:
        """判断当前候选箱体是否只是上一个箱体的滚动延续。

        前端现在会画出所有 BOX_DETECTED trace，因此去重应在指标源头完成。
        这里把“时间区间有重叠，并且支撑/压力相差不大”的候选视为同一箱体，
        避免 lookback 窗口每前进一步就输出一个几乎相同的矩形。
        """
        last = state.last_box
        if not isinstance(last, dict):
            return False
        if self._can_replace_last_box(last, box):
            return False
        current_start = int(_float(box.get("box_start_index"), -1))
        last_end = int(_float(last.get("end_index"), -1))
        if current_start > last_end:
            return False
        last_support = _float(last.get("support"), 0)
        last_resistance = _float(last.get("resistance"), 0)
        support = _float(box.get("box_support"), 0)
        resistance = _float(box.get("box_resistance"), 0)
        last_height = max(0.0, last_resistance - last_support)
        current_height = max(0.0, resistance - support)
        tolerance = max(last_height, current_height) * 0.35
        if tolerance <= 0:
            return False
        return abs(support - last_support) <= tolerance and abs(resistance - last_resistance) <= tolerance

    def _overlaps_last_box(self, state: DetectBoxState, box: dict[str, Any], params: dict[str, Any]) -> bool:
        """判断候选箱体是否与上一个已输出箱体有时间重叠。

        DetectBox 会从长窗口扫到短窗口。若一个候选和上一个箱体重叠，
        即使上下沿略有变化，也通常只是同一区域的不同切片，不应重复输出。
        """
        last = state.last_box
        if not isinstance(last, dict):
            return False
        if self._can_replace_last_box(last, box, params):
            return False
        current_start = int(_float(box.get("box_start_index"), -1))
        last_end = int(_float(last.get("end_index"), -1))
        return current_start <= last_end

    def _can_replace_last_box(self, last: dict[str, Any], box: dict[str, Any], params: dict[str, Any] | None = None) -> bool:
        """判断重叠新箱体能否替换旧箱体。

        新箱体 K 线数量更多时优先，但高度不能无节制放大；
        默认只允许比旧箱体最多增高 25%，符合“能合并尽合并，但高度小幅增大”的规则。
        """
        current_count = int(_float(box.get("box_optimized_bar_count"), 0))
        last_count = int(_float(last.get("bar_count"), 0))
        if current_count <= last_count:
            return self._box_score(box) > self._last_box_score(last)
        current_height = max(0.0, _float(box.get("box_resistance"), 0) - _float(box.get("box_support"), 0))
        last_height = max(0.0, _float(last.get("base_height"), _float(last.get("height"), 0)))
        if last_height <= 0:
            return True
        growth_ratio = _float((params or {}).get("merge_height_growth_ratio"), 0.25)
        return current_height <= last_height * (1 + growth_ratio)

    def _last_box_score(self, last: dict[str, Any]) -> tuple[int, float, int]:
        """把 last_box 状态转换为与 _box_score 可比较的分数。"""
        bar_count = int(_float(last.get("bar_count"), 0))
        height = max(0.0, _float(last.get("height"), 0))
        trim = int(_float(last.get("leading_trim_bars"), 0))
        return (bar_count, -height, -trim)

    def _close_slope_pct(self, bars: list[dict[str, Any]]) -> float:
        """计算收盘价回归斜率投影到整个窗口后的价格比例。

        返回值接近 0 表示横向；正值表示向上漂移，负值表示向下漂移。
        """
        n = len(bars)
        if n < 2:
            return 0.0
        xs = list(range(n))
        closes = [_float(item.get("close"), 0) for item in bars]
        x_mean = sum(xs) / n
        y_mean = sum(closes) / n
        denom = sum((x - x_mean) ** 2 for x in xs)
        if denom <= 0 or y_mean == 0:
            return 0.0
        slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, closes)) / denom
        return (slope * (n - 1)) / y_mean

    def _close_drift_pct(self, bars: list[dict[str, Any]]) -> float:
        """计算窗口首尾收盘价漂移比例。"""
        if len(bars) < 2:
            return 0.0
        first = _float(bars[0].get("close"), 0)
        last = _float(bars[-1].get("close"), 0)
        base = max(1e-9, abs((first + last) * 0.5))
        return abs(last - first) / base

    def _body_quality(self, bars: list[dict[str, Any]], height: float, atr: float, params: dict[str, Any]) -> dict[str, float]:
        """评估候选窗口的 K 线实体是否足够短。

        箱体整理的目标是小实体横向停留；如果大实体数量较多，即使高低点落在一个矩形里，
        也更像趋势过程中的一段下跌/上涨或 V 字反弹，不应产出箱体。
        """
        if not bars or height <= 0 or atr <= 0:
            return {"small_body_ratio": 0.0, "max_body_height_ratio": 0.0, "avg_body_height_ratio": 0.0}
        max_body_height_ratio = max(0.0, _float(params.get("max_body_height_ratio"), 0.35))
        max_body_atr_ratio = max(0.0, _float(params.get("max_body_atr_ratio"), 0.55))
        bodies = [abs(_float(item.get("close"), 0) - _float(item.get("open"), 0)) for item in bars]
        small_count = sum(1 for body in bodies if body <= height * max_body_height_ratio and body <= atr * max_body_atr_ratio)
        return {
            "small_body_ratio": small_count / len(bars),
            "max_body_height_ratio": max(bodies) / height,
            "avg_body_height_ratio": (sum(bodies) / len(bodies)) / height,
        }

    def _horizontal_motion_quality(self, bars: list[dict[str, Any]], height: float, atr: float, params: dict[str, Any]) -> dict[str, float]:
        """评估收盘价路径是否以横向运动为主。

        这里同时过滤三类误报：
        - 净位移占总位移过高：一路上涨或一路下跌；
        - 小位移比例过低：K 线在箱体内阶梯式推进；
        - 前后半段斜率过大：V 字、倒 V，或“前半横盘后半趋势”的组合。
        """
        if len(bars) < 2 or height <= 0 or atr <= 0:
            return {
                "horizontal_step_ratio": 0.0,
                "directional_move_ratio": 0.0,
                "first_half_slope_pct": 0.0,
                "second_half_slope_pct": 0.0,
                "opposite_half_slope_sum_pct": 0.0,
                "segment_mean_drift_ratio": 0.0,
                "close_range_height_ratio": 0.0,
            }
        closes = [_float(item.get("close"), 0) for item in bars]
        diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        step_limit = min(height * _float(params.get("max_step_height_ratio"), 0.35), atr * _float(params.get("max_step_atr_ratio"), 0.55))
        horizontal_count = sum(1 for diff in diffs if abs(diff) <= step_limit)
        total_move = sum(abs(diff) for diff in diffs)
        directional_move_ratio = abs(closes[-1] - closes[0]) / total_move if total_move > 0 else 0.0
        mid = max(2, len(bars) // 2)
        first_half = bars[: mid + 1]
        second_half = bars[mid:]
        first_half_slope = self._close_slope_pct(first_half)
        second_half_slope = self._close_slope_pct(second_half)
        opposite_half_slope_sum = abs(first_half_slope) + abs(second_half_slope) if first_half_slope * second_half_slope < 0 else 0.0
        segment_mean_drift_ratio = self._segment_mean_drift_ratio(closes, height)
        close_range_height_ratio = (max(closes) - min(closes)) / height if height > 0 else 0.0
        return {
            "horizontal_step_ratio": horizontal_count / len(diffs),
            "directional_move_ratio": directional_move_ratio,
            "first_half_slope_pct": first_half_slope,
            "second_half_slope_pct": second_half_slope,
            "opposite_half_slope_sum_pct": opposite_half_slope_sum,
            "segment_mean_drift_ratio": segment_mean_drift_ratio,
            "close_range_height_ratio": close_range_height_ratio,
        }

    def _segment_mean_drift_ratio(self, closes: list[float], height: float) -> float:
        """计算三段平均收盘价的最大漂移占箱体高度比例。

        只看整体斜率可能漏掉“前几根横盘、后几根整体上移/下移”的路径；
        三段均值能直接衡量箱体内部是否在发生价格中枢迁移。
        """
        if len(closes) < 6 or height <= 0:
            return 0.0
        third = max(1, len(closes) // 3)
        segments = [closes[:third], closes[third : third * 2], closes[third * 2 :]]
        means = [sum(segment) / len(segment) for segment in segments if segment]
        if len(means) < 2:
            return 0.0
        return (max(means) - min(means)) / height

    def _max_bar_gap_seconds(self, bars: list[dict[str, Any]]) -> float:
        """返回候选窗口内相邻 K 线最大的时间间隔秒数。

        箱体必须发生在连续交易片段内；如果跨午休、收盘到夜盘开盘等长间隔，
        价格路径并不是连续横向运动，不能把两段走势合并为一个矩形。
        """
        timestamps = [_event_ts(item.get("adjusted_time")) for item in bars]
        gaps = [
            float(timestamps[i] - timestamps[i - 1])
            for i in range(1, len(timestamps))
            if timestamps[i] is not None and timestamps[i - 1] is not None
        ]
        return max(gaps) if gaps else 0.0

    def _normalize_bar(self, bar: dict[str, Any], index: int) -> dict[str, Any]:
        """把不同入口传入的 bar 统一成算法内部字段。"""
        return {
            "index": index,
            "adjusted_time": _bar_event_time(bar),
            "open": _float(bar.get("open")),
            "high": _float(bar.get("high")),
            "low": _float(bar.get("low")),
            "close": _float(bar.get("close")),
        }

    def _true_range(self, prev: dict[str, Any] | None, bar: dict[str, Any]) -> float:
        """计算 True Range。

        True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))。
        第一根没有 prev_close 时退化为 high-low。
        """
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        tr = max(0.0, high - low)
        if prev is None:
            return tr
        prev_close = _float(prev.get("close"))
        return max(tr, abs(high - prev_close), abs(low - prev_close))

    def _atr(self, trs: list[float], period: int) -> float | None:
        """计算简单 ATR。

        当前实现使用最近 period 个 True Range 的算术平均，便于指标稳定且可解释。
        """
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period

    def _metrics(self, state: DetectBoxState, params: dict[str, Any]) -> dict[str, Any]:
        """构造每次无信号/有 trace 时都可返回的状态快照。"""
        atr_period = max(1, int(_float(params.get("atr_period"), 26)))
        return {
            "indicator": "detect_box",
            "state": "RUNNING",
            "stage": "RUNNING",
            "step_total": 1,
            "lookback": max(20, int(_float(params.get("lookback"), 80))),
            "min_box_bars": max(8, int(_float(params.get("min_box_bars"), 10))),
            "max_box_end_lag_bars": max(0, int(_float(params.get("max_box_end_lag_bars"), 1))),
            "pivot_window": max(1, int(_float(params.get("pivot_window"), 5))),
            "atr_period": atr_period,
            "atr": self._atr(state.trs, atr_period),
            "compression_window": max(3, int(_float(params.get("compression_window"), 12))),
            "max_compression_ratio": _float(params.get("max_compression_ratio"), 0.65),
            "tight_box_height_pct": _float(params.get("tight_box_height_pct"), 0.004),
            "max_tight_box_height_atr": _float(params.get("max_tight_box_height_atr"), 4.0),
            "min_small_body_ratio": _float(params.get("min_small_body_ratio"), 0.68),
            "min_horizontal_step_ratio": _float(params.get("min_horizontal_step_ratio"), 0.55),
            "max_directional_move_ratio": _float(params.get("max_directional_move_ratio"), 0.30),
            "max_half_slope_pct": _float(params.get("max_half_slope_pct"), 0.002),
            "max_opposite_half_slope_sum_pct": _float(params.get("max_opposite_half_slope_sum_pct"), 0.003),
            "max_segment_mean_drift_ratio": _float(params.get("max_segment_mean_drift_ratio"), 0.50),
            "max_close_range_height_ratio": _float(params.get("max_close_range_height_ratio"), 1.05),
            "max_bar_gap_seconds": _float(params.get("max_bar_gap_seconds"), 1800),
            "min_boundary_touches": max(1, int(_float(params.get("min_boundary_touches"), 2))),
            "bars": len(state.bars),
            "next_index": state.next_index,
        }
