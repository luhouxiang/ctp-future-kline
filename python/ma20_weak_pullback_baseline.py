# -*- coding: utf-8 -*-
"""MA20 Weak Pullback Baseline strategy entry module."""

from __future__ import annotations

from typing import Any


STRATEGY_ID: str = "ma20.weak_pullback_short.baseline"
DISPLAY_NAME: str = "MA20 Weak Pullback Baseline"
ENTRY_SCRIPT: str = "python/ma20_weak_pullback_baseline.py"


def build_strategy(runtime: Any) -> Any:
    """Build the baseline strategy variant from the host runtime module.

    runtime is the already imported strategy_service module. Keeping this entry
    file thin lets Go and the UI expose the variant as a separate strategy while
    sharing the tested implementation class in strategy_service.py.
    """
    strategy = runtime.MA20WeakBaselineStrategy()
    # The variant class owns the algorithm defaults; the entry module only fixes
    # metadata that tells users and logs which selectable file produced it.
    strategy.definition = {
        **strategy.definition,
        "strategy_id": STRATEGY_ID,
        "display_name": DISPLAY_NAME,
        "entry_script": ENTRY_SCRIPT,
    }
    return strategy
