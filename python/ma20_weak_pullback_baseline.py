# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Baseline strategy entry module."""

STRATEGY_ID = "ma20.weak_pullback_short.baseline"
DISPLAY_NAME = "MA20 Weak Pullback Baseline"
ENTRY_SCRIPT = "python/ma20_weak_pullback_baseline.py"


def build_strategy(runtime):
    strategy = runtime.MA20WeakBaselineStrategy()
    strategy.definition = {
        **strategy.definition,
        "strategy_id": STRATEGY_ID,
        "display_name": DISPLAY_NAME,
        "entry_script": ENTRY_SCRIPT,
    }
    return strategy
