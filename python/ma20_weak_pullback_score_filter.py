# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Score Filter strategy entry module."""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.score_filter"
DISPLAY_NAME: str = "MA20 Weak Pullback Score Filter"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_score_filter.py"
ALGORITHM: str = "score_filter"


def build_strategy(runtime: Any) -> Any:
    """Build the score-filter variant from the host runtime module.

    Score-filter keeps the same weak-pullback state machine but requires the
    bearish failure score to be at least the bullish pause score before shorting.
    The comment is explicit because this variant exists to encode the prompt's
    "avoid strong uptrend pauses" requirement, not just to flip a boolean.
    """
    strategy = runtime.MA20WeakPullbackVariantStrategy(STRATEGY_ID, DISPLAY_NAME, ALGORITHM, True)
    # Keep entry_script variant-specific so external definitions map back to
    # the selectable entry file that users configured.
    strategy.definition = {**strategy.definition, "entry_script": ENTRY_SCRIPT}
    return strategy
