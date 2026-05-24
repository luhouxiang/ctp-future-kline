import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from ma20_pullback_core import (  # noqa: E402
    PullbackExitSettings,
    PullbackKLine,
    PullbackShortState,
    confirm_short_entry,
    evaluate_exit,
)


class MA20PullbackCoreDynamicStopTest(unittest.TestCase):
    def settings(self):
        return PullbackExitSettings(
            observation_bars=240,
            profit_atr_multiple=1.0,
            adverse_atr_multiple=0.8,
            strength_exit_bars=99,
            profit_rebound_atr_multiple=1.0,
            profit_rising_low_bars=2,
            strong_bull_atr_multiple=1.5,
        )

    def entered_state(self):
        settings = self.settings()
        state = PullbackShortState()
        state.signal_atr = 1.0
        confirm_short_entry(
            state,
            PullbackKLine(open=100, high=100, low=99, close=99),
            settings,
            current_ma20_slope=-0.10,
            current_ma60_slope=-0.05,
        )
        return state

    def test_dynamic_stop_defers_when_ma20_and_ma60_are_more_bearish(self):
        out = evaluate_exit(
            self.entered_state(),
            PullbackKLine(open=100.5, high=101.5, low=100.4, close=101.2),
            current_ma=100,
            settings=self.settings(),
            current_ma20_slope=-0.20,
            current_ma60_slope=-0.08,
            stop_tr_avg=1.0,
        )

        self.assertEqual(out.action, "wait")
        self.assertTrue(out.checks["in_dynamic_stop_zone"])
        self.assertTrue(out.checks["dynamic_stop_deferred"])

    def test_dynamic_stop_exits_when_slopes_are_not_both_more_bearish(self):
        out = evaluate_exit(
            self.entered_state(),
            PullbackKLine(open=100.5, high=101.5, low=100.4, close=101.2),
            current_ma=100,
            settings=self.settings(),
            current_ma20_slope=-0.05,
            current_ma60_slope=-0.08,
            stop_tr_avg=1.0,
        )

        self.assertEqual(out.action, "exit_failure")
        self.assertEqual(out.reason, "dynamic stop zone hit")
        self.assertTrue(out.checks["in_dynamic_stop_zone"])
        self.assertFalse(out.checks["dynamic_stop_deferred"])


if __name__ == "__main__":
    unittest.main()
