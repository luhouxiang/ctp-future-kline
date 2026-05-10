# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Hard Filter strategy entry module."""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.hard_filter"
DISPLAY_NAME: str = "MA20 Weak Pullback Hard Filter"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_hard_filter.py"
ALGORITHM: str = "hard_filter"


def build_strategy(runtime: Any) -> Any:
    """Build the hard-filter variant from the host runtime module.

    Hard-filter means the strategy keeps the trend/structure gates but disables
    the extra bull-vs-bear score comparison. This exists because earlier tuning
    asked for a stricter structural version and a more conservative score-filter
    version to be compared independently.
    """
    strategy = runtime.MA20WeakPullbackVariantStrategy(STRATEGY_ID, DISPLAY_NAME, ALGORITHM, False)
    # Keep entry_script variant-specific so ListStrategies and front-end status
    # show the selected strategy file instead of the shared implementation file.
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
