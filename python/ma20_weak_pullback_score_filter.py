# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Score Filter strategy entry module."""

STRATEGY_ID = "ma20.weak_pullback_short.score_filter"
DISPLAY_NAME = "MA20 Weak Pullback Score Filter"
ENTRY_SCRIPT = "python/ma20_weak_pullback_score_filter.py"
ALGORITHM = "score_filter"


def build_strategy(runtime):
    strategy = runtime.MA20WeakPullbackVariantStrategy(STRATEGY_ID, DISPLAY_NAME, ALGORITHM, True)
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
