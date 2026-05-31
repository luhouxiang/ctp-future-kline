import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from zigzag_atr_indicator import ATRZigZagIndicatorStrategy  # noqa: E402


def req(idx, open_=100, high=101, low=100, close=100.5):
    ts = f"2026-01-01T09:{idx:02d}:00+08:00"
    return {
        "instance": {
            "instance_id": "zigzag-1",
            "strategy_id": "indicator.zigzag_atr26",
            "mode": "replay",
            "timeframe": "5m",
            "params": {"atr_period": 3, "warmup_target": 3},
        },
        "symbol": "ao2609",
        "event_time": ts,
        "mode": "replay",
        "current_position": 0,
        "bar": {
            "adjusted_time": ts,
            "data_time": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        },
    }


class ATRZigZagIndicatorStrategyTest(unittest.TestCase):
    def test_emits_peak_and_trough_traces(self):
        strategy = ATRZigZagIndicatorStrategy()
        cases = [
            req(0, high=101, low=100, close=100.5),
            req(1, high=102, low=101, close=101.5),
            req(2, high=103, low=102, close=102.5),
            req(3, open_=103, high=110, low=109, close=109.5),
            req(4, open_=109, high=109, low=106, close=106.5),
            req(5, open_=106, high=101, low=100, close=100.5),
            req(6, open_=101, high=104, low=100.5, close=103.5),
            req(7, open_=104, high=106, low=103, close=105),
        ]
        outs = [strategy.on_replay_bar(item) for item in cases]

        peak = outs[5]
        self.assertTrue(peak["no_signal"])
        self.assertEqual(peak["trace"]["step_key"], "ZIGZAG_PEAK")
        self.assertEqual(peak["trace"]["metrics"]["zigzag_type"], "PEAK")
        self.assertEqual(peak["trace"]["metrics"]["pivot_price"], 110)
        self.assertEqual(peak["trace"]["metrics"]["confirmed_time"], cases[5]["bar"]["adjusted_time"])

        trough = outs[7]
        self.assertTrue(trough["no_signal"])
        self.assertEqual(trough["trace"]["step_key"], "ZIGZAG_TROUGH")
        self.assertEqual(trough["trace"]["metrics"]["zigzag_type"], "TROUGH")
        self.assertEqual(trough["trace"]["metrics"]["pivot_price"], 100)
        self.assertEqual(trough["trace"]["metrics"]["confirmed_time"], cases[7]["bar"]["adjusted_time"])


if __name__ == "__main__":
    unittest.main()
