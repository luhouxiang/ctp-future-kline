# -*- coding: utf-8 -*-
"""示例动量策略。

这个策略不是生产算法，它的价值在于保持一条最小可用的策略链路：
- Go 侧可以用它验证 Python 服务启动、HTTP 调用、JSON 编解码和响应结构；
- 前端可以用它验证 `target_position/confidence/reason/metrics` 的展示；
- 新策略接入时可以对照它理解 Strategy 基类需要覆盖哪些方法。

为什么单独拆出来：
真正的 MA20 策略代码已经比较大，如果示例策略继续混在里面，会干扰阅读交易状态机。
拆成独立文件后，策略注册表仍可统一注册它，但维护 MA20 算法时不需要扫过样例逻辑。
"""

from __future__ import annotations

from strategy_common import Strategy, _float, _instance_id, _rfc3339_utc_now
from strategy_types import MetricsDict, RequestDict, ResponseDict, StrategyDefinition

# 示例动量策略
# -----------------------------------------------------------------------------
class SampleMomentumStrategy(Strategy):
    """一个极简示例策略，用于演示策略接口如何返回信号。

    K 线事件：
        - close > open：目标仓位 1；
        - close < open：目标仓位 -1；
        - close == open：目标仓位 0。

    tick 事件：
        - last_price >= ask：目标仓位 1；
        - last_price <= bid：目标仓位 -1；
        - 否则目标仓位 0。

    注意：
        该策略不是真实交易策略，只是接口样例。
    """

    # definition 会被 ListStrategies 返回给外部系统。
    definition: StrategyDefinition = {
        "strategy_id": "sample.momentum",
        "display_name": "Sample Momentum",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {"threshold": 0.2},
        # updated_at 会被 Go 解到 time.Time；必须带 Z/时区，否则 ListStrategies 会失败并导致页面没有策略列表。
        "updated_at": _rfc3339_utc_now(),
    }

    def _decision(self, request: RequestDict) -> ResponseDict:
        """统一处理 bar/tick 两类事件并生成示例目标仓位。

        这个函数故意不保存任何状态：
        样例策略要证明“无状态策略也能接入运行时”，所以只根据当前事件返回方向。
        真实策略如果需要 warmup、状态机或多 symbol 状态，应参考 MA20 策略而不是这里。
        """
        bar = request.get("bar") or {}
        tick = request.get("tick") or {}
        metrics: MetricsDict = {}
        # 默认保持当前仓位；只有 bar/tick 判断出方向时才覆盖。
        target = request.get("current_position", 0)
        if bar:
            # K 线逻辑：用收盘价减开盘价作为方向判断。
            close = _float(bar.get("close"))
            open_price = _float(bar.get("open"), close)
            delta = close - open_price
            metrics = {"bar_delta": delta}
            target = 1 if delta > 0 else -1 if delta < 0 else 0
        elif tick:
            # tick 逻辑：根据最新价相对一档买卖价的位置生成方向。
            last_price = _float(tick.get("last_price"))
            bid = _float(tick.get("bid_price1"), last_price)
            ask = _float(tick.get("ask_price1"), last_price)
            metrics = {"spread": ask - bid}
            target = 1 if last_price >= ask else -1 if last_price <= bid else 0
        return {
            "no_signal": False,
            "instance_id": _instance_id(request),
            "symbol": request.get("symbol", ""),
            "event_time": request.get("event_time", ""),
            "target_position": target,
            "confidence": 0.5,
            "reason": "sample strategy decision",
            "metrics": metrics,
        }

    def on_tick(self, request: RequestDict) -> ResponseDict:
        """tick 事件复用 _decision。"""
        return self._decision(request)

    def on_bar(self, request: RequestDict) -> ResponseDict:
        """bar 事件复用 _decision。"""
        return self._decision(request)


# -----------------------------------------------------------------------------
