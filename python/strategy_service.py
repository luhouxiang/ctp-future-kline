import argparse
import json
import time
from concurrent import futures
from dataclasses import dataclass, field

try:
    import grpc
except ModuleNotFoundError:
    grpc = None


def _loads(data):
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _dumps(value):
    return json.dumps(value).encode("utf-8")


WAIT_BREAK_BELOW_MA20 = "WAIT_BREAK_BELOW_MA20"
BROKEN_BELOW_MA20 = "BROKEN_BELOW_MA20"
WAIT_BREAK_TOUCH_OPEN = "WAIT_BREAK_TOUCH_OPEN"
DONE = "DONE"


def _float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _instance_id(request):
    return (request.get("instance") or {}).get("instance_id", "")


def _params(request):
    return (request.get("instance") or {}).get("params") or {}


def _no_signal(request, reason, metrics=None, trace=None):
    out = {
        "no_signal": True,
        "instance_id": _instance_id(request),
        "symbol": request.get("symbol", ""),
        "event_time": request.get("event_time", ""),
        "target_position": request.get("current_position", 0),
        "confidence": 0,
        "reason": reason,
        "metrics": metrics or {},
    }
    if trace is not None:
        out["trace"] = trace
    return out


def _trace(request, event_type, step_key, step_label, step_index, status, reason, checks=None, metrics=None, signal_preview=None):
    instance = request.get("instance") or {}
    return {
        "instance_id": _instance_id(request),
        "strategy_id": instance.get("strategy_id", ""),
        "symbol": request.get("symbol", ""),
        "timeframe": instance.get("timeframe", ""),
        "mode": request.get("mode", ""),
        "event_type": event_type,
        "event_time": request.get("event_time", ""),
        "step_key": step_key,
        "step_label": step_label,
        "step_index": step_index,
        "step_total": 5,
        "status": status,
        "reason": reason,
        "checks": checks or [],
        "metrics": metrics or {},
        "signal_preview": signal_preview or {},
    }


def _check(name, passed, current=None, target=None, delta=None, description=""):
    return {
        "name": name,
        "passed": bool(passed),
        "current": current,
        "target": target,
        "delta": delta,
        "description": description,
    }


class Strategy:
    definition = {}

    def start_instance(self, instance):
        return None

    def stop_instance(self, instance_id):
        return None

    def on_tick(self, request):
        return _no_signal(request, "tick ignored")

    def on_bar(self, request):
        return _no_signal(request, "bar ignored")

    def on_replay_bar(self, request):
        return self.on_bar(request)


class SampleMomentumStrategy(Strategy):
    definition = {
        "strategy_id": "sample.momentum",
        "display_name": "Sample Momentum",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {"threshold": 0.2},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def _decision(self, request):
        bar = request.get("bar") or {}
        tick = request.get("tick") or {}
        metrics = {}
        target = request.get("current_position", 0)
        if bar:
            close = _float(bar.get("close"))
            open_price = _float(bar.get("open"), close)
            delta = close - open_price
            metrics = {"bar_delta": delta}
            target = 1 if delta > 0 else -1 if delta < 0 else 0
        elif tick:
            last_price = _float(tick.get("last_price"))
            bid = _float(tick.get("bid_price1"), last_price)
            ask = _float(tick.get("ask_price1"), last_price)
            metrics = {"spread": ask - bid}
            target = 1 if last_price >= ask else -1 if last_price <= bid else 0
        return {
            "instance_id": _instance_id(request),
            "symbol": request.get("symbol", ""),
            "event_time": request.get("event_time", ""),
            "target_position": target,
            "confidence": 0.5,
            "reason": "sample strategy decision",
            "metrics": metrics,
        }

    def on_tick(self, request):
        return self._decision(request)

    def on_bar(self, request):
        return self._decision(request)


@dataclass
class PullbackShortState:
    state: str = WAIT_BREAK_BELOW_MA20
    closes: list[float] = field(default_factory=list)
    prev_close: float | None = None
    prev_ma: float | None = None
    touch_open: float | None = None
    touch_high: float | None = None
    touch_ma20: float | None = None
    touch_time: str = ""
    wait_bars: int = 0
    reset_requires_full_break: bool = False


class MA20PullbackShortStrategy(Strategy):
    definition = {
        "strategy_id": "ma20.pullback_short",
        "display_name": "MA20 Pullback Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {"ma_period": 20, "max_wait_bars": 6},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def __init__(self):
        self.states = {}

    def start_instance(self, instance):
        instance_id = instance.get("instance_id", "")
        symbols = instance.get("symbols") or []
        if not symbols:
            self.states[(instance_id, "")] = PullbackShortState()
            return
        for symbol in symbols:
            self.states[(instance_id, str(symbol))] = PullbackShortState()

    def stop_instance(self, instance_id):
        for key in list(self.states):
            if key[0] == instance_id:
                del self.states[key]

    def _state_for(self, request):
        key = (_instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            self.states[key] = PullbackShortState()
        return self.states[key]

    def _settings(self, request):
        params = _params(request)
        ma_period = max(2, _int(params.get("ma_period"), 20))
        max_wait_bars = max(1, _int(params.get("max_wait_bars"), 6))
        return ma_period, max_wait_bars

    def _ma(self, closes, period):
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def _reset_after_failure(self, state):
        state.state = WAIT_BREAK_BELOW_MA20
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.wait_bars = 0
        state.reset_requires_full_break = True

    def _base_metrics(self, state, ma20=None):
        return {
            "signal": "",
            "state": state.state,
            "ma20": ma20,
            "touch_open": state.touch_open,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "touch_time": state.touch_time,
            "trigger_price": None,
            "wait_bars": state.wait_bars,
        }

    def _bar_trace(self, request, state, step_key, step_label, step_index, status, reason, checks, ma20=None):
        return _trace(
            request,
            "bar",
            step_key,
            step_label,
            step_index,
            status,
            reason,
            checks,
            self._base_metrics(state, ma20),
        )

    def _tick_trace(self, request, state, status, reason, checks, last_price):
        metrics = self._base_metrics(state, state.touch_ma20)
        metrics["trigger_price"] = last_price
        return _trace(
            request,
            "key_tick",
            WAIT_BREAK_TOUCH_OPEN,
            "等待跌破触碰K开盘价",
            4,
            status,
            reason,
            checks,
            metrics,
        )

    def on_bar(self, request):
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")

        state = self._state_for(request)
        ma_period, max_wait_bars = self._settings(request)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        close = _float(bar.get("close"))
        data_time = bar.get("data_time") or request.get("event_time", "")

        previous_ma = self._ma(state.closes, ma_period)
        previous_close = state.prev_close
        state.closes.append(close)
        ma20 = self._ma(state.closes, ma_period)
        if ma20 is None:
            state.prev_close = close
            state.prev_ma = ma20
            checks = [
                _check("MA样本数量", len(state.closes) >= ma_period, len(state.closes), ma_period, len(state.closes) - ma_period, "等待收集足够K线计算均线")
            ]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", "等待 MA20 数据足够", 1, "waiting", "waiting for enough bars", checks, ma20)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ma20), trace)

        if state.state == DONE:
            state.prev_close = close
            state.prev_ma = ma20
            trace = self._bar_trace(request, state, DONE, "已触发/已完成", 5, "done", "strategy already done", [], ma20)
            return _no_signal(request, "strategy already done", self._base_metrics(state, ma20), trace)

        step_key = state.state
        step_label = "等待跌破 MA20"
        step_index = 2
        status = "waiting"
        reason = "no trade signal"
        checks = []

        if state.state == WAIT_BREAK_BELOW_MA20:
            full_break = open_price < ma20 and close < ma20
            cross_break = previous_close is not None and previous_ma is not None and previous_close >= previous_ma and close < ma20
            checks = [
                _check("开盘低于MA20", open_price < ma20, open_price, ma20, open_price - ma20),
                _check("收盘低于MA20", close < ma20, close, ma20, close - ma20),
                _check("从上向下跌破", cross_break, close, ma20, close - ma20),
            ]
            if (state.reset_requires_full_break and full_break) or (not state.reset_requires_full_break and cross_break):
                state.state = BROKEN_BELOW_MA20
                state.reset_requires_full_break = False
                step_key = BROKEN_BELOW_MA20
                step_label = "已跌破，等待反抽触碰 MA20"
                step_index = 3
                status = "passed"
                reason = "break below MA20 confirmed"
            else:
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = "等待跌破 MA20"
                step_index = 2

        elif state.state == BROKEN_BELOW_MA20:
            checks = [
                _check("最高价触碰MA20", high >= ma20, high, ma20, high - ma20, "等待反抽到MA20附近")
            ]
            if high >= ma20:
                state.state = WAIT_BREAK_TOUCH_OPEN
                state.touch_open = open_price
                state.touch_high = high
                state.touch_ma20 = ma20
                state.touch_time = data_time
                state.wait_bars = 1
                step_key = WAIT_BREAK_TOUCH_OPEN
                step_label = "等待跌破触碰K开盘价"
                step_index = 4
                status = "passed"
                reason = "MA20 touch bar found"
            else:
                step_key = BROKEN_BELOW_MA20
                step_label = "已跌破，等待反抽触碰 MA20"
                step_index = 3

        elif state.state == WAIT_BREAK_TOUCH_OPEN:
            stood_above = open_price > ma20 and close > ma20
            wait_remaining = max_wait_bars - state.wait_bars
            checks = [
                _check("未重新站上MA20", not stood_above, close, ma20, close - ma20, "重新站上则形态失败"),
                _check("等待未超时", state.wait_bars < max_wait_bars, state.wait_bars, max_wait_bars, wait_remaining),
                _check("触碰K开盘价有效", state.touch_open is not None, state.touch_open, "not null"),
            ]
            if stood_above:
                self._reset_after_failure(state)
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = "等待跌破 MA20"
                step_index = 2
                status = "failed"
                reason = "reset: stood above MA20"
            else:
                state.wait_bars += 1
                if state.wait_bars >= max_wait_bars:
                    self._reset_after_failure(state)
                    step_key = WAIT_BREAK_BELOW_MA20
                    step_label = "等待跌破 MA20"
                    step_index = 2
                    status = "failed"
                    reason = "reset: wait bars exceeded"
                else:
                    step_key = WAIT_BREAK_TOUCH_OPEN
                    step_label = "等待跌破触碰K开盘价"
                    step_index = 4

        state.prev_close = close
        state.prev_ma = ma20
        trace = self._bar_trace(request, state, step_key, step_label, step_index, status, reason, checks, ma20)
        return _no_signal(request, reason, self._base_metrics(state, ma20), trace)

    def on_tick(self, request):
        tick = request.get("tick") or {}
        state = self._state_for(request)
        if state.state != WAIT_BREAK_TOUCH_OPEN or state.touch_open is None:
            return _no_signal(request, "waiting for setup", self._base_metrics(state))

        last_price = _float(tick.get("last_price"))
        checks = [
            _check("最新价跌破触碰K开盘价", last_price < state.touch_open, last_price, state.touch_open, last_price - state.touch_open)
        ]
        if last_price >= state.touch_open:
            trace = self._tick_trace(request, state, "waiting", "touch open not broken", checks, last_price)
            return _no_signal(request, "touch open not broken", self._base_metrics(state), trace)

        state.state = DONE
        metrics = self._base_metrics(state, state.touch_ma20)
        metrics.update(
            {
                "signal": "SHORT",
                "touch_open": state.touch_open,
                "touch_high": state.touch_high,
                "touch_ma20": state.touch_ma20,
                "touch_time": state.touch_time,
                "trigger_price": last_price,
            }
        )
        trace = _trace(
            request,
            "key_tick",
            DONE,
            "已触发/已完成",
            5,
            "passed",
            "SHORT: tick broke below MA20 touch bar open",
            checks,
            metrics,
            {"target_position": -1, "confidence": 0.8, "signal": "SHORT"},
        )
        return {
            "instance_id": _instance_id(request),
            "symbol": request.get("symbol", ""),
            "event_time": request.get("event_time", ""),
            "target_position": -1,
            "confidence": 0.8,
            "reason": "SHORT: tick broke below MA20 touch bar open",
            "metrics": metrics,
            "trace": trace,
        }

    def on_replay_bar(self, request):
        return self.on_bar(request)


class StrategyService:
    def __init__(self):
        self.strategies = {
            "sample.momentum": SampleMomentumStrategy(),
            "ma20.pullback_short": MA20PullbackShortStrategy(),
        }

    def Ping(self, request, context):
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def ListStrategies(self, request, context):
        return {
            "strategies": [strategy.definition for strategy in self.strategies.values()]
        }

    def LoadStrategy(self, request, context):
        strategy_id = request.get("strategy_id", "")
        if strategy_id not in self.strategies:
            if grpc is not None and context is not None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"unknown strategy_id: {strategy_id}")
            raise ValueError(f"unknown strategy_id: {strategy_id}")
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StartInstance(self, request, context):
        instance = request.get("instance") or {}
        strategy = self._strategy_for_instance(instance)
        strategy.start_instance(instance)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request, context):
        instance_id = request.get("instance_id", "")
        for strategy in self.strategies.values():
            strategy.stop_instance(instance_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request, context):
        return self._strategy_for_request(request).on_tick(request)

    def OnBar(self, request, context):
        return self._strategy_for_request(request).on_bar(request)

    def OnReplayBar(self, request, context):
        return self._strategy_for_request(request).on_replay_bar(request)

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

    def _strategy_for_instance(self, instance):
        strategy_id = instance.get("strategy_id", "")
        strategy = self.strategies.get(strategy_id)
        if strategy is None:
            raise ValueError(f"unknown strategy_id: {strategy_id}")
        return strategy

    def _strategy_for_request(self, request):
        return self._strategy_for_instance(request.get("instance") or {})


def add_unary(handler, service_name, method_name, fn):
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
    return grpc.unary_unary_rpc_method_handler(
        lambda request, context: fn(_loads(request), context),
        request_deserializer=lambda b: b,
        response_serializer=_dumps,
    )


def build_server():
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
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
