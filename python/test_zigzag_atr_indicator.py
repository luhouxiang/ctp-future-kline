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
            "params": {"atr_period": 26, "atr_multiple": 2.0, "min_bars": 5, "warmup_target": 26},
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
    def test_default_params_use_atr26_double_reversal_and_five_bar_spacing(self):
        defaults = ATRZigZagIndicatorStrategy.definition["default_params"]

        self.assertEqual(defaults["atr_period"], 26)
        self.assertEqual(defaults["atr_multiple"], 2.0)
        self.assertEqual(defaults["min_bars"], 5)

    def test_locks_peak_and_trough_after_atr_reversal_and_min_bars(self):
        strategy = ATRZigZagIndicatorStrategy()
        cases = []
        for idx in range(26):
            close = 100 + idx * 0.2
            cases.append(req(idx, open_=close - 0.05, high=close + 0.2, low=close - 0.2, close=close))
        cases.extend(
            [
                req(26, open_=105, high=108, low=104.8, close=107.5),
                req(27, open_=107.5, high=112, low=107.2, close=111.5),
                req(28, open_=111.5, high=115, low=111, close=114.5),
                req(29, open_=114.5, high=114.8, low=108, close=109),
                req(30, open_=109, high=109.2, low=104, close=105),
                req(31, open_=105, high=105.2, low=100, close=101),
                req(32, open_=101, high=104, low=99, close=103.5),
                req(33, open_=103.5, high=108, low=98, close=107.5),
                req(34, open_=107.5, high=110, low=107, close=109.5),
                req(35, open_=109.5, high=112, low=109, close=111.5),
                req(36, open_=111.5, high=112.5, low=108, close=109),
                req(37, open_=109, high=109.5, low=104, close=105),
                req(38, open_=105, high=114, low=104, close=113),
            ]
        )

        outs = [strategy.on_replay_bar(item) for item in cases]

        peak = outs[33]
        self.assertTrue(peak["no_signal"])
        self.assertEqual(peak["trace"]["step_key"], "ZIGZAG_PEAK")
        peak_metrics = peak["trace"]["metrics"]
        self.assertEqual(peak_metrics["zigzag_type"], "PEAK")
        self.assertEqual(peak_metrics["pivot_index"], 28)
        self.assertEqual(peak_metrics["pivot_price"], 115)
        self.assertEqual(peak_metrics["confirmed_time"], cases[33]["bar"]["adjusted_time"])
        self.assertEqual(peak_metrics["confirmed_index"], 33)
        self.assertGreaterEqual(peak_metrics["pivot_bars_since_previous"], 5)
        self.assertAlmostEqual(peak_metrics["reversal_value"], peak_metrics["atr"] * 2.0)
        self.assertEqual(peak_metrics["feature_key"], "zigzag_peak_recent_short")
        self.assertEqual(peak_metrics["feature_schema"], "feature_score.v1")
        self.assertEqual(peak_metrics["feature_payload"]["score"], 10.0)
        self.assertEqual(peak_metrics["feature_payload"]["raw"]["pivot_index"], 28)
        self.assertEqual(peak_metrics["feature_payload"]["raw"]["zigzag_type"], "PEAK")

        trough = outs[38]
        self.assertTrue(trough["no_signal"])
        self.assertEqual(trough["trace"]["step_key"], "ZIGZAG_TROUGH")
        trough_metrics = trough["trace"]["metrics"]
        self.assertEqual(trough_metrics["zigzag_type"], "TROUGH")
        self.assertEqual(trough_metrics["pivot_index"], 33)
        self.assertEqual(trough_metrics["pivot_price"], 98)
        self.assertEqual(trough_metrics["confirmed_time"], cases[38]["bar"]["adjusted_time"])
        self.assertGreaterEqual(trough_metrics["pivot_bars_since_previous"], 5)
        self.assertAlmostEqual(trough_metrics["reversal_value"], trough_metrics["atr"] * 2.0)
        self.assertEqual(trough_metrics["feature_key"], "zigzag_trough_profit_short")
        self.assertEqual(trough_metrics["feature_schema"], "feature_score.v1")
        self.assertEqual(trough_metrics["feature_payload"]["score"], 10.0)


if __name__ == "__main__":
    unittest.main()
