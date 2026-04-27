import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from strategy_service import (  # noqa: E402
    BROKEN_BELOW_MA20,
    DONE,
    WAIT_BREAK_BELOW_MA20,
    WAIT_BREAK_TOUCH_OPEN,
    MA20PullbackShortStrategy,
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


class MA20PullbackShortStrategyTest(unittest.TestCase):
    def setUp(self):
        self.strategy = MA20PullbackShortStrategy()

    def warmup(self, count=20, close=100):
        for i in range(1, count + 1):
            self.strategy.on_bar(bar_req(close=close, idx=i))

    def state(self):
        return self.strategy.states[("inst-1", "rb2601")]

    def test_not_enough_bars_keeps_waiting(self):
        self.warmup(19)

        out = self.strategy.on_bar(bar_req(close=100, idx=20))

        self.assertTrue(out["no_signal"])
        self.assertEqual(self.state().state, WAIT_BREAK_BELOW_MA20)

    def test_break_below_ma20_then_touch_records_touch_bar(self):
        self.warmup()

        self.strategy.on_bar(bar_req(open_=100, high=100, low=99, close=99, idx=21))
        self.assertEqual(self.state().state, BROKEN_BELOW_MA20)

        self.strategy.on_bar(bar_req(open_=99.5, high=100.2, low=99.4, close=99.6, idx=22))
        state = self.state()

        self.assertEqual(state.state, WAIT_BREAK_TOUCH_OPEN)
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
        self.assertEqual(out["metrics"]["signal"], "SHORT")
        self.assertEqual(out["metrics"]["touch_open"], 99.5)
        self.assertEqual(out["metrics"]["touch_high"], 100.2)
        self.assertEqual(out["metrics"]["trigger_price"], 99.49)
        self.assertEqual(self.state().state, DONE)
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
            self.strategy.on_bar(bar_req(open_=99.4, high=99.7, low=99.2, close=99.4, idx=idx))

        state = self.state()
        self.assertEqual(state.state, WAIT_BREAK_BELOW_MA20)
        self.assertTrue(state.reset_requires_full_break)
        self.assertIsNone(state.touch_open)


if __name__ == "__main__":
    unittest.main()
