# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Hard Filter strategy entry module."""

STRATEGY_ID = "ma20.weak_pullback_short.hard_filter"
DISPLAY_NAME = "MA20 Weak Pullback Hard Filter"
ENTRY_SCRIPT = "python/ma20_weak_pullback_hard_filter.py"
ALGORITHM = "hard_filter"


def build_strategy(runtime):
    strategy = runtime.MA20WeakPullbackVariantStrategy(STRATEGY_ID, DISPLAY_NAME, ALGORITHM, False)
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
