# -*- coding: utf-8 -*-
"""MA20 弱反弹 hard-filter 变体入口。

这个文件只负责把 hard-filter 作为独立策略暴露给 Go/前端。
算法仍复用 `MA20WeakPullbackHardFilterStrategy`，这样页面策略项和代码类一一对应；
状态机仍来自共同的弱反弹基类，不复制三套算法。
"""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.hard_filter"
DISPLAY_NAME: str = "MA20 Weak Pullback Hard Filter"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_hard_filter.py"
ALGORITHM: str = "hard_filter"


def build_strategy(runtime: Any) -> Any:
    """从宿主 runtime 模块构造 hard-filter 变体。

    hard-filter 的含义：保留趋势/结构过滤，但不要求 bearish_failure_score >= bullish_pause_score。
    这样做的缘由是策略调参时需要区分“结构过滤本身是否有效”和“额外评分过滤是否过度保守”。
    """
    strategy = runtime.MA20WeakPullbackHardFilterStrategy()
    # entry_script 必须指向本文件，否则前端看到的会是共享实现文件，无法判断用户实际选择了哪个变体。
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
