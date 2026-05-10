# -*- coding: utf-8 -*-
"""MA20 弱反弹 baseline 变体入口。

这个文件不是算法实现文件，而是“可选择策略”的入口描述：
- Go/前端可以把它当成独立策略文件展示；
- 真正算法复用 `MA20WeakBaselineStrategy`，避免 baseline 逻辑复制一份后和主实现漂移；
- entry_script 固定成当前文件，方便日志和 ListStrategies 指回用户选择的策略文件。
"""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.baseline"
DISPLAY_NAME: str = "MA20 Weak Pullback Baseline"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_baseline.py"


def build_strategy(runtime: Any) -> Any:
    """从宿主 runtime 模块构造 baseline 策略。

    runtime 是已导入的 `strategy_service` 兼容门面。
    为什么不直接 import 具体实现类：入口文件可能由 Go 侧按脚本路径动态加载，
    复用 runtime 可以避免 Python 解释器里出现两套同名模块和两份状态常量。
    """
    strategy = runtime.MA20WeakBaselineStrategy()
    # 变体类负责算法默认参数；入口文件只改元数据，让用户和日志看到“这个可选文件”产生了策略。
    strategy.definition = {
        **strategy.definition,
        "strategy_id": STRATEGY_ID,
        "display_name": DISPLAY_NAME,
        "entry_script": ENTRY_SCRIPT,
    }
    return strategy
