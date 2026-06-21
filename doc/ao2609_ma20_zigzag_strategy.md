# ao2609 MA20 + ZigZag 5m Strategy

## Recommended live-paper configuration

Primary strategy: `ma20.state_diagram_short`

Primary params:

```json
{
  "feature_dependencies": ["zigzag_atr26"],
  "observation_bars": 360,
  "strength_exit_bars": 6,
  "use_zigzag_trough_take_profit": true,
  "prefer_zigzag_trough_take_profit": true,
  "entry_ma20_slope_max": 0.0,
  "entry_ma60_slope_max": 0.5,
  "entry_zigzag_peak_min_bars": 0,
  "entry_zigzag_peak_max_bars": 80,
  "profit_rebound_atr_multiple": 2.5,
  "profit_rising_low_bars": 3,
  "strong_bull_atr_multiple": 2.5
}
```

Helper strategy: `indicator.zigzag_atr26`

Helper params:

```json
{
  "atr_multiple": 3.5,
  "min_bars": 10
}
```

The primary strategy uses ZigZag trough take-profit first. MA20/MA60 profit exits are disabled while `prefer_zigzag_trough_take_profit` is true, so profitable shorts are held until a confirmed ATR ZigZag trough or a stop condition.

Go candidate-search exit mode: `zigzag_trough`

Backtest-only params:

```json
{
  "exit_ma20_distance_atr": 1.2,
  "zigzag_atr_period": 26,
  "zigzag_atr_multiple": 3.5,
  "zigzag_min_bars": 10
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

| Window | Signals | Net points | Avg points | Profit factor | Max DD | Max loss seq | Net after 3 pts/order |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full sample | 69 | 352.00 | 5.10 | 1.8167 | 144.00 | 13 | 145.00 |
| From 2026-01-01 | 39 | 303.00 | 7.77 | 2.3004 | 81.00 | 8 | 186.00 |
| From 2026-04-01 | 14 | 220.00 | 15.71 | 5.7826 | 22.00 | 3 | 178.00 |

Big-drop capture by `realized_profit_points / entry_to_lowest_low_points` for trades whose favorable excursion reached at least 20 points:

- Full sample: `0.5848`
- From 2026-01-01: `0.5448`
- From 2026-04-01: `0.6591`

## Live-paper operating gates

- Start with one contract only.
- Use `live_paper` first; keep real strategy orders blocked until paper behavior is verified.
- Pause auto execution after 5 consecutive losses or if account balance drops below static balance.
- Resume only after the next setup completes or after manual review of the latest MA20/ZigZag trace.
