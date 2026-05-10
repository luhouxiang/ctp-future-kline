# -*- coding: utf-8 -*-
"""MA20 弱反弹 score-filter 变体入口。

这个入口把“带评分闸门的弱反弹做空”暴露成独立策略。
它不复制算法代码，只通过 `use_score_filter=True` 打开评分条件，避免多个入口文件各自维护状态机细节。
"""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.score_filter"
DISPLAY_NAME: str = "MA20 Weak Pullback Score Filter"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_score_filter.py"
ALGORITHM: str = "score_filter"


def build_strategy(runtime: Any) -> Any:
    """从宿主 runtime 模块构造 score-filter 变体。

    score-filter 保留同一套弱反弹状态机，但做空前要求 bearish_failure_score 至少不低于
    bullish_pause_score。这个条件来自提示词中的核心约束：不要把强上涨中的普通停顿误判为空头机会。
    """
    strategy = runtime.MA20WeakPullbackVariantStrategy(STRATEGY_ID, DISPLAY_NAME, ALGORITHM, True)
    # entry_script 保持为变体入口文件，外部定义和日志才能反查到用户配置的具体策略。
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
