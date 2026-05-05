import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from strategy_service import (  # noqa: E402
    BROKEN_BELOW_MA20,
    MA20_WEAK_BASELINE_STRATEGY_ID,
    MA20_WEAK_HARD_FILTER_STRATEGY_ID,
    MA20_WEAK_SCORE_FILTER_STRATEGY_ID,
    MA20_WEAK_STRATEGY_ID,
    SHORT_SIGNAL,
    SIGNAL_ACTIVE,
    StrategyService,
    WAIT_BREAK_BELOW_MA20,
    WAIT_BREAK_TOUCH_OPEN,
    WAIT_BREAK_REACTION_LOW,
    WAIT_PULLBACK_TOUCH_MA20,
    TREND_STRUCTURE_FILTER,
    MA20PullbackShortStrategy,
    MA20WeakPullbackShortStrategy,
)


def bar_req(instance_id="inst-1", symbol="rb2601", open_=100, high=100, low=100, close=100, idx=1, params=None):
    return {
        "instance": {
            "instance_id": instance_id,
            "strategy_id": "ma20.pullback_short",
            "params": params or {"ma_period": 20, "max_wait_bars": 6},
        },
        "symbol": symbol,
        "event_time": f"2026-01-01T09:{idx:02d}:00+08:00",
        "current_position": 0,
        "bar": {
            "data_time": f"2026-01-01T09:{idx:02d}:00+08:00",
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        },
    }


def tick_req(instance_id="inst-1", symbol="rb2601", last_price=100, idx=1):
    return {
        "instance": {
            "instance_id": instance_id,
            "strategy_id": "ma20.pullback_short",
            "params": {"ma_period": 20, "max_wait_bars": 6},
        },
        "symbol": symbol,
        "event_time": f"2026-01-01T10:{idx:02d}:00+08:00",
        "current_position": 0,
        "tick": {"last_price": last_price},
    }


def weak_bar_req(instance_id="weak-1", symbol="yl9", open_=100, high=100.5, low=99.5, close=100, idx=1, params=None):
    hour = 9 + idx // 60
    minute = idx % 60
    ts = f"2026-01-01T{hour:02d}:{minute:02d}:00+08:00"
    return {
        "instance": {
            "instance_id": instance_id,
            "strategy_id": MA20_WEAK_STRATEGY_ID,
            "params": params or {
                "ma20_period": 20,
                "ma60_period": 60,
                "ma120_period": 120,
                "structure_wait_bars": 3,
                "touch_wait_bars": 12,
                "trigger_wait_bars": 6,
                "use_score_filter": True,
            },
        },
        "symbol": symbol,
        "event_time": ts,
        "current_position": 0,
        "bar": {
            "data_time": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        },
    }


class MA20PullbackShortStrategyTest(unittest.TestCase):
    def setUp(self):
        self.strategy = MA20PullbackShortStrategy()

    def warmup(self, count=20, close=100):
        for i in range(1, count + 1):
            value = close + 1 if i == count and count >= 20 else close
            self.strategy.on_bar(bar_req(open_=value, high=value, low=value, close=value, idx=i))

    def state(self):
        return self.strategy.states[("live", "inst-1", "rb2601")]

    def test_not_enough_bars_keeps_waiting(self):
        self.warmup(18)

        out = self.strategy.on_bar(bar_req(close=100, idx=20))

        self.assertTrue(out["no_signal"])
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)
        self.assertEqual(out["trace"]["step_key"], "WAIT_MA_READY")
        self.assertEqual(out["trace"]["step_index"], 1)
        self.assertEqual(out["trace"]["event_type"], "bar")

    def test_break_below_ma20_then_touch_records_touch_bar(self):
        self.warmup()

        break_out = self.strategy.on_bar(bar_req(open_=100, high=100, low=99, close=99, idx=21))
        self.assertEqual(self.state().state, BROKEN_BELOW_MA20)
        self.assertEqual(break_out["trace"]["step_key"], BROKEN_BELOW_MA20)

        touch_out = self.strategy.on_bar(bar_req(open_=99.5, high=100.2, low=99.4, close=99.6, idx=22))
        state = self.state()

        self.assertEqual(state.state, WAIT_BREAK_TOUCH_OPEN)
        self.assertEqual(touch_out["trace"]["step_key"], WAIT_BREAK_TOUCH_OPEN)
        self.assertTrue(any(check["name"] == "最高价触碰MA20" and check["passed"] for check in touch_out["trace"]["checks"]))
        self.assertEqual(state.touch_open, 99.5)
        self.assertEqual(state.touch_high, 100.2)
        self.assertEqual(state.wait_bars, 1)
        self.assertTrue(state.touch_time)

    def test_tick_breaks_touch_open_once(self):
        self.warmup()
        self.strategy.on_bar(bar_req(open_=100, high=100, low=99, close=99, idx=21))
        self.strategy.on_bar(bar_req(open_=99.5, high=100.2, low=99.4, close=99.6, idx=22))

        out = self.strategy.on_tick(tick_req(last_price=99.49))
        again = self.strategy.on_tick(tick_req(last_price=99.0, idx=2))

        self.assertFalse(out.get("no_signal", False))
        self.assertEqual(out["target_position"], -1)
        self.assertEqual(out["trace"]["event_type"], "key_tick")
        self.assertEqual(out["trace"]["step_key"], SHORT_SIGNAL)
        self.assertEqual(out["trace"]["signal_preview"]["signal"], "SHORT")
        self.assertEqual(out["metrics"]["signal"], "SHORT")
        self.assertEqual(out["metrics"]["touch_open"], 99.5)
        self.assertEqual(out["metrics"]["touch_high"], 100.2)
        self.assertEqual(out["metrics"]["trigger_price"], 99.49)
        self.assertEqual(self.state().state, SIGNAL_ACTIVE)
        self.assertTrue(again["no_signal"])

    def test_stood_above_ma20_resets_and_requires_full_break(self):
        self.warmup()
        self.strategy.on_bar(bar_req(open_=100, high=100, low=99, close=99, idx=21))
        self.strategy.on_bar(bar_req(open_=99.5, high=100.2, low=99.4, close=99.6, idx=22))

        self.strategy.on_bar(bar_req(open_=101, high=101.2, low=100.9, close=101, idx=23))
        state = self.state()
        self.assertEqual(state.state, WAIT_BREAK_BELOW_MA20)
        self.assertTrue(state.reset_requires_full_break)

        self.strategy.on_bar(bar_req(open_=99, high=99.1, low=98, close=98.5, idx=24))
        self.assertEqual(self.state().state, BROKEN_BELOW_MA20)
        self.assertFalse(self.state().reset_requires_full_break)

    def test_touch_wait_timeout_resets_after_sixth_bar(self):
        self.warmup()
        self.strategy.on_bar(bar_req(open_=100, high=100, low=99, close=99, idx=21))
        self.strategy.on_bar(bar_req(open_=99.5, high=100.2, low=99.4, close=99.6, idx=22))

        for idx in range(23, 28):
            self.strategy.on_bar(bar_req(open_=99.6, high=99.7, low=99.55, close=99.6, idx=idx))

        state = self.state()
        self.assertEqual(state.state, WAIT_BREAK_BELOW_MA20)
        self.assertTrue(state.reset_requires_full_break)
        self.assertIsNone(state.touch_open)

    def test_replay_state_does_not_pollute_live_state(self):
        self.warmup()
        req = bar_req(open_=100, high=100, low=99, close=99, idx=21)
        req["mode"] = "replay"

        self.strategy.on_replay_bar(req)

        self.assertEqual(self.strategy.states[("replay", "inst-1", "rb2601")].state, WAIT_BREAK_BELOW_MA20)
        self.assertEqual(self.strategy.states[("live", "inst-1", "rb2601")].state, WAIT_BREAK_BELOW_MA20)

    def test_start_instance_applies_warmup_bars(self):
        warmup = [
            {
                "symbol": "rb2601",
                "data_time": f"2026-01-01T09:{i:02d}:00+08:00",
                "open": 101 if i == 20 else 100,
                "high": 101 if i == 20 else 100,
                "low": 101 if i == 20 else 100,
                "close": 101 if i == 20 else 100,
            }
            for i in range(1, 21)
        ]
        self.strategy.start_instance({
            "instance_id": "replay-1",
            "strategy_id": "ma20.pullback_short",
            "mode": "replay",
            "symbols": ["rb2601"],
            "params": {"ma_period": 20, "max_wait_bars": 6, "warmup_bars": warmup},
        })

        state = self.strategy.states[("replay", "replay-1", "rb2601")]
        self.assertEqual(len(state.closes), 20)
        req = bar_req(instance_id="replay-1", open_=100, high=100, low=99, close=99, idx=21)
        req["mode"] = "replay"
        out = self.strategy.on_replay_bar(req)
        self.assertEqual(out["trace"]["step_key"], BROKEN_BELOW_MA20)

    def test_starting_below_ma20_requires_full_bar_above_before_break_wait(self):
        warmup = [
            {
                "symbol": "rb2601",
                "data_time": f"2026-01-01T09:{i:02d}:00+08:00",
                "open": 100,
                "high": 100,
                "low": 100,
                "close": 100,
            }
            for i in range(1, 21)
        ]
        self.strategy.start_instance({
            "instance_id": "replay-below",
            "strategy_id": "ma20.pullback_short",
            "mode": "replay",
            "symbols": ["rb2601"],
            "params": {"ma_period": 20, "max_wait_bars": 6, "warmup_bars": warmup},
        })

        req1 = bar_req(instance_id="replay-below", open_=99.8, high=100.0, low=99.0, close=99.2, idx=21)
        req1["mode"] = "replay"
        out1 = self.strategy.on_replay_bar(req1)
        self.assertEqual(out1["trace"]["step_key"], WAIT_BREAK_BELOW_MA20)
        self.assertIn("full bar above", out1["reason"])

        req2 = bar_req(instance_id="replay-below", open_=101.2, high=101.4, low=101.1, close=101.3, idx=22)
        req2["mode"] = "replay"
        out2 = self.strategy.on_replay_bar(req2)
        self.assertEqual(out2["trace"]["step_key"], WAIT_BREAK_BELOW_MA20)
        self.assertTrue(self.strategy.states[("replay", "replay-below", "rb2601")].break_below_armed)

        req3 = bar_req(instance_id="replay-below", open_=100.8, high=100.9, low=99.0, close=99.2, idx=23)
        req3["mode"] = "replay"
        out3 = self.strategy.on_replay_bar(req3)
        self.assertEqual(out3["trace"]["step_key"], BROKEN_BELOW_MA20)


class MA20WeakPullbackShortStrategyTest(unittest.TestCase):
    def setUp(self):
        self.strategy = MA20WeakPullbackShortStrategy()

    def state(self):
        return self.strategy.states[("live", "weak-1", "yl9")]

    def warmup_flat(self, count=120):
        for i in range(1, count + 1):
            self.strategy.on_bar(weak_bar_req(idx=i, open_=100, high=100.5, low=99.5, close=100))

    def warmup_uptrend(self, count=130):
        close = 100.0
        for i in range(1, count + 1):
            close = 100 + i * 0.2
            self.strategy.on_bar(weak_bar_req(idx=i, open_=close, high=close + 0.4, low=close - 0.4, close=close))
        return close

    def test_strong_uptrend_without_structure_break_is_filtered(self):
        last_close = self.warmup_uptrend()

        out = self.strategy.on_bar(weak_bar_req(idx=131, open_=last_close - 3, high=last_close - 2.8, low=last_close - 3.6, close=last_close - 3.2))

        self.assertTrue(out["no_signal"])
        self.assertEqual(out["trace"]["step_key"], TREND_STRUCTURE_FILTER)
        self.assertEqual(out["trace"]["status"], "failed")
        self.assertIn("strong uptrend", out["reason"])
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)

    def test_fast_reclaim_is_bullish_pause(self):
        self.warmup_flat()
        self.strategy.on_bar(weak_bar_req(idx=121, open_=100, high=100.1, low=99.4, close=99.7))

        out = self.strategy.on_bar(weak_bar_req(idx=122, open_=99.8, high=100.4, low=99.7, close=100.2))

        self.assertTrue(out["no_signal"])
        self.assertEqual(out["trace"]["step_key"], TREND_STRUCTURE_FILTER)
        self.assertIn("BULLISH_PAUSE", out["reason"])
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)

    def test_weak_pullback_short_signal_is_bar_driven(self):
        self.warmup_flat()
        break_out = self.strategy.on_bar(weak_bar_req(idx=121, open_=100, high=100.2, low=98.7, close=98.9))
        self.assertEqual(break_out["trace"]["step_key"], TREND_STRUCTURE_FILTER)
        self.assertEqual(self.state().state, WAIT_PULLBACK_TOUCH_MA20)

        touch_out = self.strategy.on_bar(weak_bar_req(idx=122, open_=99.2, high=100.1, low=98.8, close=99.0))
        self.assertTrue(touch_out["no_signal"])
        self.assertEqual(self.state().state, WAIT_BREAK_REACTION_LOW)

        signal = self.strategy.on_bar(weak_bar_req(idx=123, open_=99.0, high=99.2, low=98.7, close=98.9))

        self.assertFalse(signal.get("no_signal", False))
        self.assertEqual(signal["target_position"], -1)
        self.assertEqual(signal["trace"]["step_key"], "SHORT_SIGNAL")
        self.assertEqual(signal["metrics"]["signal"], "SHORT")
        self.assertEqual(self.state().state, SIGNAL_ACTIVE)

        result = self.strategy.on_bar(weak_bar_req(idx=124, open_=98.8, high=99.0, low=97.0, close=98.0))
        self.assertTrue(result["no_signal"])
        self.assertEqual(result["trace"]["step_key"], "SIGNAL_RESULT")
        self.assertEqual(result["trace"]["status"], "passed")
        self.assertEqual(result["metrics"]["signal_result"], "success")
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)

        next_wait = self.strategy.on_bar(weak_bar_req(idx=125, open_=98.0, high=98.2, low=97.5, close=97.8))
        self.assertTrue(next_wait["no_signal"])
        self.assertEqual(next_wait["trace"]["step_key"], WAIT_BREAK_BELOW_MA20)
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)

    def test_touch_bar_does_not_trigger_on_same_bar(self):
        self.warmup_flat()
        self.strategy.on_bar(weak_bar_req(idx=121, open_=100, high=100.2, low=98.7, close=98.9))

        out = self.strategy.on_bar(weak_bar_req(idx=122, open_=99.2, high=100.1, low=98.0, close=99.0))

        self.assertTrue(out["no_signal"])
        self.assertEqual(out["trace"]["step_key"], "WAIT_PULLBACK_TOUCH_MA20")
        self.assertEqual(self.state().state, WAIT_BREAK_REACTION_LOW)

    def test_replay_state_does_not_pollute_live_state(self):
        self.warmup_flat()
        req = weak_bar_req(idx=121, open_=100, high=100.2, low=98.7, close=98.9)
        req["mode"] = "replay"

        self.strategy.on_replay_bar(req)

        self.assertEqual(self.strategy.states[("replay", "weak-1", "yl9")].state, WAIT_BREAK_BELOW_MA20)
        self.assertEqual(self.strategy.states[("live", "weak-1", "yl9")].state, WAIT_BREAK_BELOW_MA20)


class StrategyServiceRegistryTest(unittest.TestCase):
    def test_weak_pullback_variants_expose_separate_entry_scripts(self):
        service = StrategyService()

        definitions = {
            item["strategy_id"]: item.get("entry_script")
            for item in service.ListStrategies({}, None)["strategies"]
        }

        self.assertEqual(definitions[MA20_WEAK_BASELINE_STRATEGY_ID], "python/ma20_weak_pullback_baseline.py")
        self.assertEqual(definitions[MA20_WEAK_HARD_FILTER_STRATEGY_ID], "python/ma20_weak_pullback_hard_filter.py")
        self.assertEqual(definitions[MA20_WEAK_SCORE_FILTER_STRATEGY_ID], "python/ma20_weak_pullback_score_filter.py")


if __name__ == "__main__":
    unittest.main()
