import argparse
import json
import time
from concurrent import futures

import grpc


def _loads(data):
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _dumps(value):
    return json.dumps(value).encode("utf-8")


class StrategyService:
    def Ping(self, request, context):
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def ListStrategies(self, request, context):
        return {
            "strategies": [
                {
                    "strategy_id": "sample.momentum",
                    "display_name": "Sample Momentum",
                    "entry_script": "python/strategy_service.py",
                    "version": "1.0.0",
                    "default_params": {"threshold": 0.2},
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                }
            ]
        }

    def LoadStrategy(self, request, context):
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StartInstance(self, request, context):
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request, context):
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request, context):
        return self._decision(request)

    def OnBar(self, request, context):
        return self._decision(request)

    def OnReplayBar(self, request, context):
        return self._decision(request)

    def RunBacktest(self, request, context):
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"trades": 0, "pnl": 0, "note": "sample backtest"},
            "result": {"equity_curve": [], "trades": []},
        }

    def GetBacktestResult(self, request, context):
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"note": "sample result"},
            "result": {"equity_curve": [], "trades": []},
        }

    def RunParameterSweep(self, request, context):
        return {
            "run_id": f"opt-{int(time.time() * 1000)}",
            "status": "done",
            "summary": {"candidates": sum(len(v) for v in request.get("grid", {}).values())},
        }

    def _decision(self, request):
        bar = request.get("bar") or {}
        tick = request.get("tick") or {}
        metrics = {}
        target = request.get("current_position", 0)
        if bar:
            close = float(bar.get("close", 0))
            open_price = float(bar.get("open", close))
            delta = close - open_price
            metrics = {"bar_delta": delta}
            target = 1 if delta > 0 else -1 if delta < 0 else 0
        elif tick:
            last_price = float(tick.get("last_price", 0))
            bid = float(tick.get("bid_price1", last_price))
            ask = float(tick.get("ask_price1", last_price))
            metrics = {"spread": ask - bid}
            target = 1 if last_price >= ask else -1 if last_price <= bid else 0
        return {
            "instance_id": request.get("instance", {}).get("instance_id", ""),
            "symbol": request.get("symbol", ""),
            "event_time": request.get("event_time", ""),
            "target_position": target,
            "confidence": 0.5,
            "reason": "sample strategy decision",
            "metrics": metrics,
        }


def add_unary(handler, service_name, method_name, fn):
    return grpc.unary_unary_rpc_method_handler(
        lambda request, context: fn(_loads(request), context),
        request_deserializer=lambda b: b,
        response_serializer=_dumps,
    )


def build_server():
    service = StrategyService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "strategy.Health",
                {"Ping": add_unary(service, "strategy.Health", "Ping", service.Ping)},
            ),
            grpc.method_handlers_generic_handler(
                "strategy.Registry",
                {"ListStrategies": add_unary(service, "strategy.Registry", "ListStrategies", service.ListStrategies)},
            ),
            grpc.method_handlers_generic_handler(
                "strategy.Runtime",
                {
                    "LoadStrategy": add_unary(service, "strategy.Runtime", "LoadStrategy", service.LoadStrategy),
                    "StartInstance": add_unary(service, "strategy.Runtime", "StartInstance", service.StartInstance),
                    "StopInstance": add_unary(service, "strategy.Runtime", "StopInstance", service.StopInstance),
                    "OnTick": add_unary(service, "strategy.Runtime", "OnTick", service.OnTick),
                    "OnBar": add_unary(service, "strategy.Runtime", "OnBar", service.OnBar),
                    "OnReplayBar": add_unary(service, "strategy.Runtime", "OnReplayBar", service.OnReplayBar),
                },
            ),
            grpc.method_handlers_generic_handler(
                "strategy.Backtest",
                {
                    "RunBacktest": add_unary(service, "strategy.Backtest", "RunBacktest", service.RunBacktest),
                    "GetBacktestResult": add_unary(service, "strategy.Backtest", "GetBacktestResult", service.GetBacktestResult),
                },
            ),
            grpc.method_handlers_generic_handler(
                "strategy.Optimizer",
                {"RunParameterSweep": add_unary(service, "strategy.Optimizer", "RunParameterSweep", service.RunParameterSweep)},
            ),
        )
    )
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    args = parser.parse_args()
    server = build_server()
    server.add_insecure_port(args.addr)
    server.start()
    print(f"strategy service listening on {args.addr}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()

