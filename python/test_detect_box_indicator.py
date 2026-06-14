import math
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from detect_box_indicator import DetectBoxIndicatorStrategy  # noqa: E402
from strategy_registry import StrategyFactory  # noqa: E402
from strategy_types import DETECT_BOX_INDICATOR_ID  # noqa: E402


def req(idx, open_=100, high=101, low=99, close=100):
    ts = f"2026-01-01T09:{idx:02d}:00+08:00"
    return {
        "instance": {
            "instance_id": "box-1",
            "strategy_id": DETECT_BOX_INDICATOR_ID,
            "mode": "replay",
            "timeframe": "5m",
            "params": {
                "lookback": 32,
                "min_box_bars": 10,
                "max_window_size_checks": 24,
                "max_box_end_lag_bars": 6,
                "pivot_window": 3,
                "atr_period": 50,
                "error_pct": 0.01,
                "boundary_atr_factor": 0.45,
                "min_pivots": 0,
                "min_inside_ratio": 0.92,
                "min_alternations": 2,
                "min_height_atr": 0.15,
                "max_height_atr": 3,
                "compression_window": 8,
                "max_compression_ratio": 0.65,
                "max_box_height_pct": 0.018,
                "tight_box_height_pct": 0.004,
                "max_tight_box_height_atr": 4.0,
                "max_close_slope_pct": 0.004,
                "max_close_drift_pct": 0.006,
                "min_small_body_ratio": 0.68,
                "max_body_height_ratio": 0.35,
                "max_body_atr_ratio": 0.55,
                "min_horizontal_step_ratio": 0.55,
                "max_step_height_ratio": 0.35,
                "max_step_atr_ratio": 0.55,
                "max_directional_move_ratio": 0.30,
                "max_half_slope_pct": 0.002,
                "max_opposite_half_slope_sum_pct": 0.003,
                "max_segment_mean_drift_ratio": 0.50,
                "max_close_range_height_ratio": 1.05,
                "max_leading_trim_bars": 10,
                "max_leading_trim_ratio": 0.25,
                "regression_upper_quantile": 0.82,
                "regression_lower_quantile": 0.18,
                "max_spike_ratio": 0.22,
                "spike_tolerance_atr_ratio": 0.08,
                "merge_height_growth_ratio": 0.25,
                "long_box_min_bars": 18,
                "long_box_min_small_body_ratio": 0.50,
                "long_box_min_horizontal_step_ratio": 0.50,
                "long_box_max_half_slope_pct": 0.004,
                "long_box_max_segment_mean_drift_ratio": 1.50,
                "long_box_max_close_range_height_ratio": 2.20,
                "long_box_max_spike_ratio": 0.40,
                "max_bar_gap_seconds": 10800,
                "max_avg_tr_pct": 0.004,
                "min_boundary_touches": 2,
                "boundary_touch_ratio": 0.3,
                "warmup_target": 50,
            },
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


class DetectBoxIndicatorStrategyTest(unittest.TestCase):
    def test_registered_as_builtin_strategy(self):
        definitions = {item["strategy_id"]: item for item in StrategyFactory.builtin().list_definitions()}

        self.assertIn(DETECT_BOX_INDICATOR_ID, definitions)
        self.assertEqual(definitions[DETECT_BOX_INDICATOR_ID]["display_name"], "DetectBox 箱体整理")

    def test_default_params_are_strict_enough_for_compressed_box(self):
        defaults = DetectBoxIndicatorStrategy.definition["default_params"]

        self.assertEqual(defaults["min_box_bars"], 10)
        self.assertEqual(defaults["max_box_end_lag_bars"], 6)
        self.assertEqual(defaults["min_pivots"], 0)
        self.assertEqual(defaults["min_alternations"], 2)
        self.assertGreaterEqual(defaults["min_inside_ratio"], 0.92)
        self.assertLessEqual(defaults["max_compression_ratio"], 0.65)
        self.assertLessEqual(defaults["max_height_atr"], 3)
        self.assertEqual(defaults["max_tight_box_height_atr"], 4.0)
        self.assertLessEqual(defaults["max_close_slope_pct"], 0.004)
        self.assertGreaterEqual(defaults["min_small_body_ratio"], 0.68)
        self.assertGreaterEqual(defaults["min_horizontal_step_ratio"], 0.55)
        self.assertLessEqual(defaults["max_directional_move_ratio"], 0.30)
        self.assertLessEqual(defaults["max_half_slope_pct"], 0.002)
        self.assertLessEqual(defaults["max_opposite_half_slope_sum_pct"], 0.003)
        self.assertLessEqual(defaults["max_segment_mean_drift_ratio"], 0.50)
        self.assertLessEqual(defaults["max_close_range_height_ratio"], 1.05)
        self.assertEqual(defaults["max_leading_trim_bars"], 10)
        self.assertGreaterEqual(defaults["regression_upper_quantile"], 0.8)
        self.assertLessEqual(defaults["regression_lower_quantile"], 0.2)
        self.assertLessEqual(defaults["max_spike_ratio"], 0.22)
        self.assertEqual(defaults["merge_height_growth_ratio"], 0.25)
        self.assertEqual(defaults["long_box_min_bars"], 18)
        self.assertLessEqual(defaults["long_box_min_small_body_ratio"], 0.50)
        self.assertGreaterEqual(defaults["long_box_max_close_range_height_ratio"], 2.20)
        self.assertLessEqual(defaults["max_bar_gap_seconds"], 10800)

    def test_detects_box_and_emits_trace_feature_payload(self):
        strategy = DetectBoxIndicatorStrategy()
        outs = []
        for idx in range(90):
            if idx < 30:
                base = 100 + math.sin(idx * math.pi / 4) * 6
                outs.append(strategy.on_replay_bar(req(idx, open_=base, high=base + 1.4, low=base - 1.4, close=base)))
            else:
                base = 100 + math.sin((idx - 30) * math.pi / 4) * 0.20
                phase = (idx - 30) % 6
                high = 100.6 if phase in (0, 1) else base + 0.05
                low = 99.4 if phase in (3, 4) else base - 0.05
                outs.append(strategy.on_replay_bar(req(idx, open_=base, high=high, low=low, close=base)))

        traces = [item["trace"] for item in outs if item.get("trace")]
        self.assertTrue(traces)
        trace = max(traces, key=lambda item: item["metrics"].get("box_optimized_bar_count", 0))
        self.assertEqual(trace["step_key"], "BOX_DETECTED")
        metrics = trace["metrics"]
        self.assertEqual(metrics["indicator"], "detect_box")
        self.assertEqual(metrics["feature_key"], "detect_box")
        self.assertEqual(metrics["feature_payload"]["support"], metrics["box_support"])
        self.assertEqual(metrics["feature_payload"]["resistance"], metrics["box_resistance"])
        self.assertGreater(metrics["box_resistance"], metrics["box_support"])
        self.assertGreaterEqual(metrics["box_upper_touches"], 2)
        self.assertGreaterEqual(metrics["box_lower_touches"], 2)
        self.assertGreaterEqual(metrics["box_inside_ratio"], 0.92)
        self.assertIsNotNone(metrics["box_compression_ratio"])
        self.assertLessEqual(abs(metrics["box_close_slope_pct"]), 0.004)
        self.assertGreaterEqual(metrics["box_small_body_ratio"], 0.68)
        self.assertGreaterEqual(metrics["box_horizontal_step_ratio"], 0.55)
        self.assertLessEqual(metrics["box_close_range_height_ratio"], 1.05)

    def test_does_not_detect_narrow_trending_channel_as_box(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(90):
            if idx < 30:
                base = 100 + math.sin(idx * math.pi / 4) * 6
                out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.4, low=base - 1.4, close=base))
            else:
                base = 100 - (idx - 30) * 0.08 + math.sin((idx - 30) * math.pi / 4) * 0.25
                out = strategy.on_replay_bar(req(idx, open_=base, high=base + 0.05, low=base - 0.05, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertFalse(traces)

    def test_rejects_one_way_drop_with_last_bar_rebound(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(50):
            base = 120 + math.sin(idx * math.pi / 4) * 5
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.2, low=base - 1.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        closes = [110.0, 109.3, 108.4, 107.5, 106.6, 105.8, 105.0, 104.1, 103.4, 102.8, 102.2, 103.2]
        for offset, close in enumerate(closes, start=1):
            idx = 50 + offset
            open_ = closes[offset - 2] if offset > 1 else close + 0.4
            high = max(open_, close) + 0.35
            low = min(open_, close) - 0.35
            out = strategy.on_replay_bar(req(idx, open_=open_, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertFalse(traces)

    def test_rejects_v_shape_path_as_box(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(50):
            base = 100 + math.sin(idx * math.pi / 4) * 5
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.2, low=base - 1.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        closes = [100.0, 99.2, 98.4, 97.7, 97.1, 96.8, 97.2, 97.9, 98.7, 99.5, 100.1, 100.5]
        for offset, close in enumerate(closes, start=1):
            idx = 50 + offset
            open_ = closes[offset - 2] if offset > 1 else close
            high = max(open_, close) + 0.25
            low = min(open_, close) - 0.25
            out = strategy.on_replay_bar(req(idx, open_=open_, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertFalse(traces)

    def test_rejects_flat_start_followed_by_uptrend(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(50):
            base = 100 + math.sin(idx * math.pi / 4) * 5
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.2, low=base - 1.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        closes = [100.0, 100.1, 99.9, 100.0, 100.2, 100.8, 101.5, 102.2, 102.9, 103.6, 104.1, 104.6]
        for offset, close in enumerate(closes, start=1):
            idx = 50 + offset
            open_ = closes[offset - 2] if offset > 1 else close
            high = max(open_, close) + 0.2
            low = min(open_, close) - 0.2
            out = strategy.on_replay_bar(req(idx, open_=open_, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertFalse(traces)

    def test_detects_short_box_before_breakout_bar(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        idx = 0
        for idx in range(50):
            base = 2760 + math.sin(idx * math.pi / 4) * 8
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 2.2, low=base - 2.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        box_bars = [
            (2765.0, 2767.0, 2764.4),
            (2764.6, 2765.2, 2763.8),
            (2764.2, 2764.8, 2762.0),
            (2765.2, 2767.0, 2764.5),
            (2764.8, 2765.4, 2763.7),
            (2764.0, 2764.7, 2762.0),
            (2765.5, 2767.0, 2764.9),
            (2764.5, 2765.0, 2763.6),
            (2764.0, 2764.7, 2762.0),
            (2765.0, 2767.0, 2764.3),
            (2764.6, 2765.1, 2763.5),
            (2764.0, 2764.6, 2762.0),
        ]
        for offset, (close, high, low) in enumerate(box_bars, start=1):
            idx = 50 + offset
            out = strategy.on_replay_bar(req(idx, open_=close, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        breakout_idx = 50 + len(box_bars) + 1
        out = strategy.on_replay_bar(req(breakout_idx, open_=2763.0, high=2763.5, low=2755.0, close=2757.0))
        if out.get("trace"):
            traces.append(out["trace"])

        self.assertTrue(traces)
        metrics = max((item["metrics"] for item in traces), key=lambda item: item.get("box_optimized_bar_count", 0))
        self.assertLess(metrics["box_end_index"], breakout_idx)
        self.assertGreaterEqual(metrics["box_lower_touches"], 2)
        self.assertGreaterEqual(metrics["box_upper_touches"], 2)
        self.assertLessEqual(metrics["box_height_pct"], 0.018)

    def test_regression_trims_wick_spikes_and_keeps_long_box(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(50):
            base = 100 + math.sin(idx * math.pi / 4) * 5
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.2, low=base - 1.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        for offset in range(24):
            idx = 50 + offset
            close = 100 + math.sin(offset * math.pi / 3) * 0.12
            high = close + 0.18
            low = close - 0.18
            if offset in (2, 11):
                high += 1.0
            if offset in (8, 14, 15):
                low -= 1.2
            out = strategy.on_replay_bar(req(idx, open_=close, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertTrue(traces)
        metrics = max((item["metrics"] for item in traces), key=lambda item: item.get("box_optimized_bar_count", 0))
        self.assertGreaterEqual(metrics["box_optimized_bar_count"], 20)
        self.assertGreater(metrics["box_raw_resistance"] - metrics["box_raw_support"], metrics["box_resistance"] - metrics["box_support"])
        self.assertGreaterEqual(metrics["box_upper_spikes"] + metrics["box_lower_spikes"], 1)

    def test_trims_leading_bars_to_preserve_best_box(self):
        strategy = DetectBoxIndicatorStrategy()
        traces = []
        for idx in range(50):
            base = 100 + math.sin(idx * math.pi / 4) * 5
            out = strategy.on_replay_bar(req(idx, open_=base, high=base + 1.2, low=base - 1.2, close=base))
            if out.get("trace"):
                traces.append(out["trace"])

        leading = [(101.2, 101.6, 100.8), (100.9, 101.2, 100.5), (100.6, 101.0, 100.3)]
        for offset, (close, high, low) in enumerate(leading):
            idx = 50 + offset
            out = strategy.on_replay_bar(req(idx, open_=close, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        for offset in range(18):
            idx = 53 + offset
            close = 100 + math.sin(offset * math.pi / 3) * 0.10
            high = close + 0.15
            low = close - 0.15
            out = strategy.on_replay_bar(req(idx, open_=close, high=high, low=low, close=close))
            if out.get("trace"):
                traces.append(out["trace"])

        self.assertTrue(traces)
        metrics = max((item["metrics"] for item in traces), key=lambda item: item.get("box_optimized_bar_count", 0))
        self.assertGreaterEqual(metrics["box_start_index"], 53)
        self.assertGreaterEqual(metrics["box_optimized_bar_count"], 16)


if __name__ == "__main__":
    unittest.main()
