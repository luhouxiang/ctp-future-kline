# ao2609 MA20 + ZigZag 5m Strategy

## New Runnable Algorithm

Composition ID: `ma20_state_zigzag_trend_friend_ao2609_5m`

Display name: `MA20转势持盈短线@ao2609-5m`

This is a separate runnable composition. It keeps the original `MA20状态图 + ZigZag指标` template available, and adds a faster ZigZag confirmation profile tuned for roughly one open/close trading signal per day on `ao2609` 5m.

## Recommended live-paper configuration

Primary strategy: `ma20.trend_friend_short`

Implementation: `python/ma20_trend_friend_short.py`. This strategy has its own strategy ID, class, state and source file; later changes do not affect `ma20.state_diagram_short`.

Primary params:

```json
{
  "feature_dependencies": ["trend_friend_zigzag_atr26"],
  "observation_bars": 360,
  "strength_exit_bars": 6,
  "use_zigzag_trough_take_profit": true,
  "prefer_zigzag_trough_take_profit": true,
  "entry_ma20_slope_max": 0.0,
  "entry_ma60_slope_max": 0.5,
  "entry_zigzag_peak_min_bars": 0,
  "entry_zigzag_peak_max_bars": 80,
  "profit_rebound_atr_multiple": 99.0,
  "profit_rising_low_bars": 99,
  "strong_bull_atr_multiple": 99.0
}
```

Helper strategy: `indicator.trend_friend_zigzag_atr26`

Helper implementation: `python/trend_friend_zigzag_atr_indicator.py`. It is independent from `indicator.zigzag_atr26`.

Helper params:

```json
{
  "atr_multiple": 3.0,
  "min_bars": 8
}
```

The primary strategy uses ZigZag trough take-profit first. MA20/MA60 profit exits are disabled while `prefer_zigzag_trough_take_profit` is true, so profitable shorts are held until a confirmed ATR ZigZag trough or a stop condition. The MA rebound take-profit thresholds are intentionally set very high in this template so the live behavior stays focused on confirmed trend reversal exits.

Go candidate-search exit mode: `zigzag_trough`

Backtest-only params:

```json
{
  "exit_ma20_distance_atr": 1.2,
  "zigzag_atr_period": 26,
  "zigzag_atr_multiple": 3.0,
  "zigzag_min_bars": 8
}
```

## ao2609 5m validation

Data table: `future_kline_instrument_mm_ao`

Instrument: `ao2609`

Period: `5m`

Go candidate-search outputs:

- `flow/strategy_backtests/ao2609_ma20_zigzag_trough_final.json`
- `flow/strategy_backtests/ao2609_ma20_zigzag_trough_final_2026.json`
- `flow/strategy_backtests/ao2609_ma20_zigzag_trough_final_recent.json`

Primary strategy replay summary:

| Window | Entries | Net points | Avg points | Profit factor | Max DD | Max single loss | Trade signals/day |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full sample | 98 | 308.00 | 3.14 | 1.51 | 153.00 | -28.00 | 0.94 |
| From 2026-01-01 | 65 | 270.00 | 4.15 | 1.69 | 81.00 | -28.00 | 1.02 |
| From 2026-04-01 | 32 | 138.00 | 4.31 | 1.85 | 81.00 | -25.00 | 1.05 |

Big-drop capture by `realized_profit_points / entry_to_lowest_low_points` for trades whose favorable excursion reached at least 20 points:

- Full sample: `0.623`
- From 2026-01-01: `0.585`
- From 2026-04-01: `0.589`

## Live-paper operating gates

- Start with one contract only.
- Use `live_paper` first; keep real strategy orders blocked until paper behavior is verified.
- Pause auto execution after 5 consecutive losses or if account balance drops below static balance.
- Resume only after the next setup completes or after manual review of the latest MA20/ZigZag trace.
