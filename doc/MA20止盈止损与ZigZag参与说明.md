# MA20 止盈止损与 ZigZag 参与说明

本文按当前源码整理 MA20 做空策略的止盈止损逻辑，以及 ATR ZigZag 指标目前怎样进入 MA20 策略链路。

## 结论

当前实现里，ZigZag 并没有直接参与 MA20 的止损条件判断。它作为独立指标先运行，Go 侧缓存最近确认的 ZigZag 波峰，并把这些波峰作为只读 `features.zigzag_atr26.peaks` 注入给 MA20 状态图策略。

MA20 状态图策略只把这些 ZigZag 波峰转成诊断指标：

- `zigzag_recent_peaks`：最近最多 3 个 ZigZag 波峰快照。
- `zigzag_peak_distances`：这些波峰距离当前 K 线的根数。

止损、止盈是否触发，仍然只由 MA20/MA60、TR/ATR、入场价、反弹高点、连续未创新低、连续站稳 MA20 等条件决定。测试用例 `test_zigzag_features_are_optional_metrics_enrichment` 也说明 ZigZag 是可选指标增强；缺失 ZigZag 时策略流程保持不变。

## ZigZag 数据链路

### 1. 独立指标策略计算 ZigZag

指标策略为 `indicator.zigzag_atr26`，实现文件是 `python/zigzag_atr_indicator.py`。

默认参数：

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `atr_period` | 26 | ATR 计算周期 |
| `atr_multiple` | 2.0 | 反转确认阈值，等于 `ATR * atr_multiple` |
| `min_bars` | 5 | 相邻结构点最小间隔 |
| `warmup_target` | 26 | 启动预热 K 线数量 |

指标确认波峰或波谷后输出 trace：

- `ZIGZAG_PEAK`：确认波峰。
- `ZIGZAG_TROUGH`：确认波谷。

### 2. Go 侧只缓存波峰

`internal/strategy/manager.go` 中的 `updateFeatureCacheFromTrace` 只缓存 `zigzag_type == "PEAK"` 的结构点。缓存维度是：

- `mode`
- `symbol`
- `timeframe`
- `indicator`

每个维度最多保留 64 个波峰。向交易策略发请求时，`featuresFor` 取最近最多 3 个波峰，注入请求：

```json
{
  "features": {
    "zigzag_atr26": {
      "peaks": [
        {
          "pivot_index": 18,
          "pivot_time": "...",
          "pivot_price": 102.5,
          "confirmed_time": "...",
          "confirmed_index": 20
        }
      ]
    }
  }
}
```

### 3. MA20 状态图策略消费快照

`python/ma20_state_diagram_short.py` 的 `_update_zigzag_features` 读取 `features.zigzag_atr26.peaks`，只做两件事：

1. 复制最近最多 3 个波峰字段到 `state.zigzag_recent_peaks`。
2. 用 `当前 bar_index - pivot_index` 计算 `bars_from_peak`，同步到 `zigzag_peak_distances`。

这些值进入 trace metrics，方便前端展示和复盘解释，但当前没有进入止损表达式。

## MA20 状态图做空流程

策略 ID：`ma20.state_diagram_short`。

整体状态：

```text
INIT
  -> ABOVE_MA20
  -> BELOW_MA20
  -> REBOUND_TO_HIGH
  -> SECOND_BREAK_SIGNAL
  -> PROFIT_HOLDING / LOSS_HOLDING
  -> TAKE_PROFIT / STOP_LOSS
  -> INIT
```

开仓前流程：

1. `INIT`：等待收盘价站上 MA20。
2. `ABOVE_MA20`：等待收盘从 MA20 上方向下跌破 MA20。
3. `BELOW_MA20`：跌破后等待最高价反弹触碰 MA20。
4. `REBOUND_TO_HIGH`：触碰 MA20 后，在 `max_wait_bars` 内等待再次收盘跌破 MA20。
5. 二次跌破成立后发出 `SHORT`，入场价按当前收盘价记录，进入持仓观察。

触碰 MA20 那根 K 线会记录：

- `touch_open`
- `touch_close`
- `touch_high`
- `touch_ma20`
- `trigger_line = min(touch_open, touch_close)`

其中当前状态图版实际二次开仓条件是“收盘再次跌破 MA20”，不是跌破 `trigger_line`。

## 持仓阶段公共维护指标

每根持仓 K 线都会先更新这些状态：

- `signal_bars`：持仓观察 K 线数。
- `signal_lowest_low`：入场以来最低价。
- `signal_no_new_low_bars`：连续未创新低根数。
- `signal_rising_low_bars`：连续低点上移根数。
- `signal_ma20_touched`：持仓后最高价是否触碰过 MA20。
- `signal_stop_tr_avg`：动态止损使用的 TR 阈值。

盈亏状态按收盘价相对入场价判断：

- 做空后 `close < entry`：`PROFIT_HOLDING`。
- 做空后 `close >= entry`：`LOSS_HOLDING`。

## 止损算法

止损只在亏损状态下触发，即 `close >= entry`。

### 1. 动态 TR 止损

动态阈值优先使用 `stop_tr_avg`：

```text
adverse_distance = close - entry
dynamic_threshold = stop_tr_avg
in_stop_zone = close >= entry && adverse_distance >= dynamic_threshold
```

`stop_tr_avg` 的口径来自参数：

- 最近 `stop_tr_lookback = 60` 个 TR。
- 取最大的 `stop_tr_top_n = 10` 个。
- 去掉最大的 `stop_tr_drop_n = 2` 个。
- 剩余 8 个求均值。

如果 `stop_tr_avg` 为空，则退回使用信号 K 线计算出的固定逆向目标：

```text
signal_adverse_target = entry + adverse_atr_multiple * signal_atr
dynamic_threshold = signal_adverse_target - entry
```

默认 `adverse_atr_multiple = 0.8`。

### 2. 空头斜率延后止损

进入止损区后，不一定马上止损。若 MA20 和 MA60 当前斜率都比入场时更偏空，则延后止损：

```text
ma20_more_bearish = current_ma20_slope < entry_ma20_slope
ma60_more_bearish = current_ma60_slope < entry_ma60_slope
stop_deferred = in_stop_zone && ma20_more_bearish && ma60_more_bearish
hit_stop = in_stop_zone && !stop_deferred
```

也就是说，价格已经不利，但均线趋势加速向下时，策略允许空单继续持有。

### 3. 连续未创新低止损

亏损后，如果连续 `strength_exit_bars` 根 K 线没有创出入场以来新低，认为空头没有继续推进：

```text
no_new_low_stop = close >= entry && signal_no_new_low_bars >= strength_exit_bars
```

默认 `strength_exit_bars = 3`。

### 4. 突破反弹高点止损

如果当前收盘价突破最初反弹触碰 MA20 那根 K 线的最高价，认为反弹高点被收复：

```text
break_rebound_high_stop = close >= entry && touch_high != nil && close > touch_high
```

### 5. 连续站稳 MA20 止损

亏损状态下，如果 K 线开盘和收盘都在 MA20 上方，会累计 `reclaim_above_bars`。达到确认根数后止损：

```text
stood_above_ma20 = open > ma20 && close > ma20
stood_above_stop = reclaim_above_bars >= reclaim_confirm_bars
```

默认 `reclaim_confirm_bars = 2`。

### 6. MA60 空头保护

上述止损条件会被一个保护条件拦截：

```text
bearish_ma60_stop_guard =
    ma60 != nil
    && ma20 < ma60
    && high < ma60
```

当 MA20 位于 MA60 下方，且当前 K 线最高价也没碰到 MA60，策略认为空头结构还在，暂时忽略止损条件。

### 7. 观察窗口结束

如果没有触发止盈，且观察窗口结束时仍未盈利，则以 `STOP_LOSS` 路径结束，结果标记为 `unresolved`：

```text
signal_bars >= observation_bars && close >= entry
```

默认 `observation_bars = 240`。

## 止盈算法

止盈只在盈利状态下触发，即 `close < entry`。核心思想是：先等价格从低位反弹触碰 MA20，再判断反弹是否足够强，或者是否已经收复 MA60。

### 前置条件：盈利后触碰 MA20

持仓后只要某根 K 线最高价触碰 MA20：

```text
high >= ma20
```

就设置：

```text
signal_ma20_touched = true
```

没有触碰 MA20 前，即使已有浮盈，也继续持有，不止盈。

### 1. 收盘站上 MA60 止盈

如果盈利状态下已经触碰过 MA20，且收盘价进一步站上 MA60，则止盈：

```text
close_above_ma60_take =
    close < entry
    && signal_ma20_touched
    && ma60 != nil
    && close > ma60
```

这是最直接的利润保护：空单盈利后，反弹已经强到收复 MA60，退出。

### 2. MA60 下方的反弹强度止盈

如果价格还没站上 MA60，但已经站上 MA20，则看站上 MA20 的距离是否达到 ATR26：

```text
below_ma60_after_ma20_reclaim =
    close < entry
    && signal_ma20_touched
    && ma60 != nil
    && close <= ma60
    && close > ma20

ma20_reclaim_distance = close - ma20
```

当 `ma20_reclaim_distance < ATR26` 时，不止盈，认为反弹不足。

当 `ma20_reclaim_distance >= ATR26` 时，再看 MA20 和 MA60 斜率：

- 如果 MA20/MA60 斜率双双向下：不止盈，让利润继续奔跑。
- 如果不是双双向下：止盈。

表达式：

```text
below_ma60_take =
    below_ma60_after_ma20_reclaim
    && atr26 != nil
    && ma20_reclaim_distance >= atr26
    && !(ma20_slope < 0 && ma60_slope < 0)
```

## 与旧 `ma20.pullback_short` 的区别

旧策略 `ma20.pullback_short` 的平仓核心在 `python/ma20_pullback_core.py`：

- 开仓是“跌破 MA -> 反抽触碰 MA -> 跌破触碰 K 开盘价 -> 下一根开盘确认入场”。
- 止损主要是动态 TR 止损；进入止损区后，如果 MA20/MA60 斜率更偏空，会延后止损。
- 止盈是盈利后触碰 MA，再满足“反弹达到阈值且低点连续上移”或“强阳完全站上 MA”。

状态图策略 `ma20.state_diagram_short` 则改成了更显式的 8 状态流程，并把止损细化为：

- 动态 TR 止损。
- 连续未创新低止损。
- 收盘突破反弹高点止损。
- 连续站稳 MA20 止损。
- MA60 空头保护。
- 观察窗口结束。

## 当前 ZigZag 与止损的关系

当前关系可以概括为：

```text
ZigZag 指标 -> Go 缓存最近波峰 -> 注入 MA20 features -> MA20 trace metrics 展示
```

不是：

```text
ZigZag 波峰 -> 直接触发或拦截 MA20 止损
```

因此，若前端看到 MA20 trace 里有 `zigzag_recent_peaks` 或 `zigzag_peak_distances`，它表示“当前 K 线附近存在哪些已确认波峰”，而不是“这些波峰已经改变了止损规则”。

## 如果后续要让 ZigZag 真正参与止损

可以基于当前已注入的 `zigzag_recent_peaks` 加规则，但需要新增代码和测试。较自然的方向是：

1. 把最近 ZigZag 波峰作为结构止损位：亏损后 `close > recent_peak.pivot_price` 时止损。
2. 把 `bars_from_peak` 作为过滤：只使用距离当前较近的波峰，避免很久以前的结构点影响当前交易。
3. 把 `touch_high` 和最近 ZigZag 波峰合成止损线：例如 `max(touch_high, recent_peak_price)`，突破后认为反弹结构被收复。
4. 在 MA60 空头保护下允许 ZigZag 强结构突破优先止损，避免单纯均线保护过度延迟退出。

这些属于待实现规则，不是当前代码行为。
