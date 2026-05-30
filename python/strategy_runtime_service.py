# -*- coding: utf-8 -*-
"""Strategy runtime service methods shared by the HTTP transport."""

from __future__ import annotations

import logging
import time
from typing import Any

from strategy_common import (
    _ensure_logging_configured,
    _log_strategy_phase,
)
from strategy_registry import StrategyFactory, StrategyRegistryEntry, StrategyRuntimeInstance
from strategy_types import JSONObject, RequestDict, ResponseDict

logger = logging.getLogger(__name__)


class StrategyService:
    """Transport-agnostic strategy service object."""

    def __init__(self) -> None:
        _ensure_logging_configured()
        self.factory: StrategyFactory = StrategyFactory.builtin()
        self.strategies: dict[str, StrategyRegistryEntry] = self.factory.strategies
        logger.info("strategy registry initialized: %s", ",".join(sorted(self.strategies)))

    def Ping(self, request: RequestDict, context: Any) -> ResponseDict:
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def ListStrategies(self, request: RequestDict, context: Any) -> ResponseDict:
        return {"strategies": self.factory.list_definitions()}

    def LoadStrategy(self, request: RequestDict, context: Any) -> ResponseDict:
        strategy_id = request.get("strategy_id", "")
        self.factory.load_strategy(strategy_id)
        logger.info("strategy loaded: %s", strategy_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def GetStartRequirements(self, request: RequestDict, context: Any) -> ResponseDict:
        instance = request.get("instance") or {}
        out = self.factory.start_requirements(instance)
        logger.info(
            "strategy start requirements: instance_id=%s strategy_id=%s mode=%s timeframe=%s warmup_target=%s requires_anchor_time=%s",
            instance.get("instance_id", ""),
            instance.get("strategy_id", ""),
            instance.get("mode", ""),
            instance.get("timeframe", ""),
            out.get("warmup_target"),
            out.get("requires_anchor_time"),
        )
        return out

    def StartInstance(self, request: RequestDict, context: Any) -> ResponseDict:
        instance = request.get("instance") or {}
        instance_id = instance.get("instance_id", "")
        logger.info("strategy StartInstance begin..., instance_id=%s strategy_id=%s mode=%s timeframe=%s", 
                    instance_id, instance.get("strategy_id", ""), instance.get("mode", ""), instance.get("timeframe", ""))
        self.factory.start_instance(instance)
        logger.info("strategy StartInstance end runtime_count=%s", len(self.factory.instances))
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request: RequestDict, context: Any) -> ResponseDict:
        instance_id = request.get("instance_id", "")
        removed = self.factory.stop_instance(instance_id)
        logger.info("strategy instance stopped: instance_id=%s runtime_removed=%s", instance_id, removed)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request: RequestDict, context: Any) -> ResponseDict:
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_tick", req))

    def OnBar(self, request: RequestDict, context: Any) -> ResponseDict:
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_bar", req))

    def OnReplayBar(self, request: RequestDict, context: Any) -> ResponseDict:
        return self._run_and_log_phase(request, lambda req: self.factory.dispatch("on_replay_bar", req))

    def RunBacktest(self, request: RequestDict, context: Any) -> ResponseDict:
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"trades": 0, "pnl": 0, "note": "mock backtest stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def GetBacktestResult(self, request: RequestDict, context: Any) -> ResponseDict:
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"note": "mock backtest result stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def RunParameterSweep(self, request: RequestDict, context: Any) -> ResponseDict:
        return {
            "run_id": f"opt-{int(time.time() * 1000)}",
            "status": "done",
            "summary": {"candidates": sum(len(v) for v in request.get("grid", {}).values()), "note": "mock optimizer stub"},
        }

    def _strategy_for_instance(self, instance: JSONObject) -> StrategyRegistryEntry:
        strategy_id = instance.get("strategy_id", "")
        return self.factory.load_strategy(strategy_id)

    def _strategy_for_request(self, request: RequestDict) -> StrategyRuntimeInstance:
        return self.factory.runtime_for_request(request)

    def _run_and_log_phase(self, request: RequestDict, fn: Any) -> ResponseDict:
        out = fn(request)
        _log_strategy_phase(request, out)
        return out
