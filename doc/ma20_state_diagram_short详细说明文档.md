# ma20_state_diagram_short 详细说明文档

本文说明策略 `ma20.state_diagram_short` 的当前源码行为，重点覆盖三件事：

- 如何开仓：从 MA20 上方基准、跌破、反抽触碰，到二次跌破确认做空。
- 如何止损：亏损状态下的动态 TR 止损、结构止损、MA20/MA60 保护。
- 如何止盈：盈利状态下的 ZigZag 波谷优先止盈，以及 MA20/MA60 反弹止盈。

对应实现文件：

- Python 策略：`python/ma20_state_diagram_short.py`
- 共享状态常量：`python/strategy_types.py`
- 通用指标：`python/strategy_indicators.py`
- ZigZag feature 注入：`internal/strategy/manager.go`

## 1. 策略定位

`ma20.state_diagram_short` 是一个按完整 K 线推进的 MA20 状态图做空策略。

它不处理 tick 入场，也不在 tick 上推进状态。`on_tick` 只返回 `bar driven strategy ignores ticks`。实盘和回放都通过 `on_bar` / `on_replay_bar` 使用同一套状态机。

策略输出两类交易信号：

| 信号 | target_position | 含义 |
| --- | ---: | --- |
| `SHORT` | `-1` | 二次跌破确认后开空 |
| `CLOSE_SHORT` | `0` | 止盈、止损或观察窗口结束后平空 |

状态图共有 8 个展示步骤：

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

前端展示标签对应为：

| 状态 | 展示含义 |
| --- | --- |
| `INIT` | 初始待机 |
| `ABOVE_MA20` | MA20 之上 |
| `BELOW_MA20` | 跌破 MA20 之下 |
| `REBOUND_TO_HIGH` | 反弹至高点 |
| `SECOND_BREAK_SIGNAL` | 再次跌出出信号 |
| `PROFIT_HOLDING` | 赚钱中 |
| `LOSS_HOLDING` | 亏钱中 |
| `TAKE_PROFIT` | 止盈 |
| `STOP_LOSS` | 止损 |

## 2. 指标和数据口径

### 2.1 K 线去重

每根 K 线必须带有 `open/high/low/close`。如果当前 K 线时间戳小于或等于上一根已处理 K 线时间戳，策略直接忽略，避免重复或乱序 K 线污染状态。

### 2.2 MA 和斜率

默认参数：

| 参数 | 默认值 | 用途 |
| --- | ---: | --- |
| `ma_period` | 20 | 主均线，文档中称 MA20 |
| `ma60_period` | 60 | 趋势过滤和止盈止损保护 |
| `slope_lookback_bars` | 5 | MA20/MA60 斜率计算回看根数 |

均线是简单移动平均：

```text
MA(period) = 最近 period 根 close 的算术平均值
```

斜率也是简单差值平均：

```text
MA_slope = (当前 MA - lookback 根之前的 MA) / lookback
```

开仓时会记录入场时的 `ma20_slope`、`ma60_slope`，后续用于判断止损是否可以延后。

### 2.3 TR / ATR

单根真实波幅 TR：

```text
TR = max(
  high - low,
  abs(high - previous_close),
  abs(low - previous_close),
  0.0001
)
```

止盈中使用的 `atr26` 本质是最近 `take_profit_atr_period` 根 TR 的简单平均，默认周期是 26。

动态止损使用 `stop_tr_avg`，默认口径：

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `stop_tr_lookback` | 60 | 最近 60 根 TR |
| `stop_tr_top_n` | 10 | 取其中最大的 10 根 |
| `stop_tr_drop_n` | 2 | 去掉最大的 2 根 |

也就是：

```text
stop_tr_avg = avg(最近60根TR中最大的10根，去掉最大的2根后剩余的8根)
```

如果样本不足无法计算 `stop_tr_avg`，止损会退回使用入场 K 线波幅：

```text
signal_atr = max(high - low, 0.0001)
signal_adverse_target = entry + adverse_atr_multiple * signal_atr
dynamic_threshold = signal_adverse_target - entry
```

默认 `adverse_atr_multiple = 0.8`。

### 2.4 预热要求

启动实例时至少需要满足 MA20、MA60、ATR 周期中最大的样本数。默认最大值是 MA60，所以默认至少预热 60 根 K 线。若传入 `warmup_target`，取更大的那个。

## 3. 关键参数

源码默认参数如下：

| 参数 | 默认值 | 当前用途 |
| --- | ---: | --- |
| `ma_period` | 20 | MA20 主状态机 |
| `ma60_period` | 60 | MA60 保护和反弹止盈 |
| `slope_lookback_bars` | 5 | MA 斜率 |
| `stop_tr_lookback` | 60 | 动态止损 TR 回看 |
| `stop_tr_top_n` | 10 | 动态止损取最大 TR 个数 |
| `stop_tr_drop_n` | 2 | 动态止损剔除极端 TR 个数 |
| `take_profit_atr_period` | 26 | 反弹止盈 ATR 周期 |
| `max_wait_bars` | 6 | 触碰 MA20 后等待二次跌破的窗口 |
| `reclaim_confirm_bars` | 2 | 连续站稳 MA20 的确认根数 |
| `observation_bars` | 240 | 持仓最大观察窗口 |
| `adverse_atr_multiple` | 0.8 | 入场波幅兜底止损倍数 |
| `strength_exit_bars` | 3 | 亏损后连续未创新低止损根数 |
| `use_zigzag_trough_take_profit` | `true` | 是否允许 ZigZag 波谷止盈 |
| `prefer_zigzag_trough_take_profit` | `true` | 是否优先只用 ZigZag 止盈 |
| `entry_ma20_slope_max` | `1000000000.0` | 入场 MA20 斜率上限 |
| `entry_ma60_slope_max` | `1000000000.0` | 入场 MA60 斜率上限 |
| `entry_zigzag_peak_min_bars` | 0 | 入场要求的 ZigZag 波峰最小距离 |
| `entry_zigzag_peak_max_bars` | `1000000000` | 入场要求的 ZigZag 波峰最大距离 |

默认组合 `ma20_state_zigzag` 会覆盖部分参数：

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
  "entry_zigzag_peak_max_bars": 80
}
```

注意：`profit_rebound_atr_multiple`、`profit_rising_low_bars`、`strong_bull_atr_multiple` 仍存在于默认参数中，但当前 Python 状态图实现没有把它们纳入止盈止损表达式。当前 MA 反弹止盈使用的是 `close - ma20 >= atr26`，没有乘 `profit_rebound_atr_multiple`。

## 4. 开仓算法

开仓只做空，不做多。完整流程是：

```text
先站上 MA20
  -> 从上向下跌破 MA20
  -> 反弹触碰 MA20
  -> 再次收盘跌破 MA20
  -> 通过入场过滤
  -> 按当前收盘价开空
```

### 4.1 INIT：等待站上 MA20

`INIT` 是初始状态。只有当前 K 线收盘价严格大于 MA20，才认为有了“从 MA20 上方开始观察”的基准：

```text
above = close > ma20
```

成立后：

- 状态切换到 `ABOVE_MA20`。
- `above_ready = true`。
- 继续等待后续从上向下跌破。

如果 `close <= ma20`，继续停留在 `INIT`。

### 4.2 ABOVE_MA20：等待从上向下跌破 MA20

跌破必须满足上一根和当前根共同确认：

```text
cross_break =
  previous_close != nil
  && previous_ma != nil
  && previous_close >= previous_ma
  && close < ma20
```

这里的 `previous_ma` 是处理当前 K 线前，用旧 close 缓存计算出来的 MA；`ma20` 是追加当前 close 后的当前 MA。

成立后：

- 状态切换到 `BELOW_MA20`。
- 不开仓，只认为“第一次跌破成立”。

如果不成立，继续在 `ABOVE_MA20` 等待。

### 4.3 BELOW_MA20：等待反弹触碰 MA20

第一次跌破后，策略等待价格从下方向上反弹，最高价触碰 MA20：

```text
touched = high >= ma20
```

触碰成立后，记录触碰 K 线的信息：

| 字段 | 含义 |
| --- | --- |
| `touch_open` | 触碰 K 线开盘价 |
| `touch_close` | 触碰 K 线收盘价 |
| `touch_high` | 触碰 K 线最高价，后续可作为结构止损参考 |
| `touch_ma20` | 触碰时的 MA20 |
| `touch_time` | 触碰 K 线时间 |
| `trigger_line` | `min(touch_open, touch_close)` |

随后状态切换到 `REBOUND_TO_HIGH`。

当前源码中 `trigger_line` 只记录和展示，不作为实际二次开仓触发线。实际二次开仓条件是“收盘再次跌破 MA20”。

如果在等待触碰期间出现连续站稳 MA20，跌破结构作废：

```text
stood_above = open > ma20 && close > ma20
```

当 `stood_above` 连续达到 `reclaim_confirm_bars` 根，状态回到 `ABOVE_MA20`，重新等待下一次跌破。

默认 `reclaim_confirm_bars = 2`。

### 4.4 REBOUND_TO_HIGH：等待二次收盘跌破 MA20

触碰 MA20 后，策略开始统计等待根数：

```text
wait_bars += 1
```

二次跌破条件：

```text
broke_trigger = close < ma20
```

成立后进入入场过滤。注意它不是：

```text
close < trigger_line
```

也不是：

```text
low < trigger_line
```

也就是说，只要触碰后某根完整 K 线收盘再次低于 MA20，就具备做空候选资格。

如果等待过程中连续站稳 MA20：

```text
open > ma20 && close > ma20
```

连续达到 `reclaim_confirm_bars` 根，则形态作废，重置后回到 `ABOVE_MA20`。

如果等待超过 `max_wait_bars` 仍未二次跌破，则清空本次触碰信息，回到 `BELOW_MA20`，继续等待下一次反弹触碰。

### 4.5 入场过滤

二次跌破成立后，不一定直接开空。还要通过三类过滤。

#### 4.5.1 MA20 斜率过滤

```text
ma20_slope_blocked =
  ma20_slope != nil
  && ma20_slope > entry_ma20_slope_max
```

如果当前 MA20 斜率大于上限，则禁止开空。

默认 `entry_ma20_slope_max` 非常大，等于不限制。默认组合把它设为 `0.0`，表示 MA20 斜率必须小于等于 0 才允许开空。

如果样本不足导致 `ma20_slope == nil`，不会因为斜率过滤阻塞。

#### 4.5.2 MA60 斜率过滤

```text
ma60_slope_blocked =
  ma60_slope != nil
  && ma60_slope > entry_ma60_slope_max
```

默认不限制。默认组合设为 `0.5`，表示 MA60 斜率不能明显向上。

#### 4.5.3 ZigZag 波峰距离过滤

Go 侧会把最近 ZigZag 波峰注入：

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

Python 策略会把每个波峰转换为距离：

```text
bars_from_peak = current_bar_index - pivot_index
```

入场过滤要求至少存在一个波峰距离落在区间内：

```text
entry_zigzag_peak_min_bars <= bars_from_peak <= entry_zigzag_peak_max_bars
```

源码默认区间是 `0` 到 `1000000000`，等于不限制。默认组合设为 `0` 到 `80`，表示二次跌破时最近 80 根内必须有一个已确认 ZigZag 波峰，否则不做空。

如果启用了有限区间但没有 ZigZag feature，过滤会失败。

### 4.6 开仓信号

三类过滤都通过后，策略按当前 K 线收盘价开空：

```text
entry = close
trigger_price = close
target_position = -1
signal = SHORT
confidence = 0.8
```

同时记录：

| 字段 | 含义 |
| --- | --- |
| `signal_bar_index` | 开仓 K 线序号，用于判断后续 ZigZag 波谷是否发生在入场之后 |
| `signal_atr` | 开仓 K 线高低点范围，最小 0.0001 |
| `signal_adverse_target` | 兜底逆向止损参考价 |
| `signal_lowest_low` | 持仓以来最低价，初始为开仓 K 线 low |
| `signal_prev_low` | 上一根持仓 K 线 low，初始为开仓 K 线 low |
| `signal_ma20_touched` | 开仓 K 线 high 是否已经触碰 MA20 |
| `signal_entry_ma20_slope` | 开仓时 MA20 斜率 |
| `signal_entry_ma60_slope` | 开仓时 MA60 斜率 |

返回开仓信号时，trace 的步骤是 `SECOND_BREAK_SIGNAL`；内部状态随后进入 `LOSS_HOLDING`，等待下一根 K 线按盈亏重新分流到 `PROFIT_HOLDING` 或 `LOSS_HOLDING`。

## 5. 持仓阶段公共维护

持仓后每根 K 线都会先更新这些字段：

```text
signal_bars += 1
```

最低价推进：

```text
if low < signal_lowest_low:
  signal_lowest_low = low
  signal_no_new_low_bars = 0
else:
  signal_no_new_low_bars += 1
```

低点上移统计：

```text
if low > signal_prev_low:
  signal_rising_low_bars += 1
else:
  signal_rising_low_bars = 0
signal_prev_low = low
```

MA20 触碰标记：

```text
if high >= ma20:
  signal_ma20_touched = true
```

盈亏状态按当前收盘价相对入场价判断：

```text
profitable = close < entry

if profitable:
  state = PROFIT_HOLDING
else:
  state = LOSS_HOLDING
```

因为这是空单，`close < entry` 才是盈利。

## 6. 止损算法

止损只在亏损状态下触发：

```text
not profitable
```

也就是：

```text
close >= entry
```

如果当前处于盈利状态，下面这些止损条件都不会触发。

### 6.1 动态 TR 止损

亏损距离：

```text
adverse_distance = close - entry
```

动态阈值优先使用 `stop_tr_avg`：

```text
dynamic_threshold = stop_tr_avg
```

如果 `stop_tr_avg` 不可用，则使用入场时记录的兜底阈值：

```text
dynamic_threshold = signal_adverse_target - entry
```

进入止损区：

```text
in_stop_zone =
  not profitable
  && dynamic_threshold != nil
  && adverse_distance != nil
  && adverse_distance >= dynamic_threshold
```

如果没有斜率延后保护，则触发动态止损。

### 6.2 空头斜率延后止损

动态止损区不是必然立刻平仓。如果当前 MA20 和 MA60 斜率都比入场时更偏空，则策略认为空头趋势仍在加强，允许延后止损：

```text
ma20_more_bearish =
  current_ma20_slope != nil
  && entry_ma20_slope != nil
  && current_ma20_slope < entry_ma20_slope

ma60_more_bearish =
  current_ma60_slope != nil
  && entry_ma60_slope != nil
  && current_ma60_slope < entry_ma60_slope

stop_deferred =
  in_stop_zone
  && ma20_more_bearish
  && ma60_more_bearish

hit_stop =
  in_stop_zone
  && !stop_deferred
```

如果 `stop_deferred` 为真，当前 K 线不会因为动态止损区平仓。

### 6.3 连续未创新低止损

亏损后，如果连续若干根没有创出持仓以来新低，说明空头推进失败：

```text
no_new_low_stop =
  not profitable
  && signal_no_new_low_bars >= strength_exit_bars
```

默认 `strength_exit_bars = 3`。默认组合中该值是 6。

这里的“创新低”使用 K 线 low 判断。只要当前 low 没有低于 `signal_lowest_low`，就累计一次未创新低。

### 6.4 收盘突破反弹高点止损

开仓前反弹触碰 MA20 时记录了 `touch_high`。持仓亏损后，如果收盘价突破这个反弹高点，认为原来的弱反弹结构被收复：

```text
break_rebound_high_stop =
  not profitable
  && touch_high != nil
  && close > touch_high
```

触发后平空。

### 6.5 连续站稳 MA20 止损

亏损状态下，若 K 线开盘和收盘都在 MA20 上方，则累计站稳根数：

```text
stood_above_ma20 =
  open > ma20
  && close > ma20
```

如果连续达到 `reclaim_confirm_bars`：

```text
stood_above_stop =
  not profitable
  && reclaim_above_bars >= reclaim_confirm_bars
```

则触发止损。

默认 `reclaim_confirm_bars = 2`。也就是说，单根站上 MA20 不会立刻止损，第二根确认后才止损。

### 6.6 MA60 空头保护

止损条件还会被 MA60 空头保护拦截：

```text
bearish_ma60_stop_guard =
  ma60 != nil
  && ma20 < ma60
  && high < ma60
```

当 MA20 位于 MA60 下方，且当前 K 线最高价仍未触碰 MA60，策略认为空头结构仍完整。此时即使以下条件成立，也暂时不止损：

- 动态 TR 止损区。
- 连续未创新低。
- 收盘突破反弹高点。
- 连续站稳 MA20。
- 观察窗口结束且仍亏损。

这个保护只拦截止损，不拦截盈利止盈。

### 6.7 观察窗口结束

如果持仓观察根数达到 `observation_bars`，并且当前仍不盈利，则通过 `STOP_LOSS` 路径结束：

```text
signal_bars >= observation_bars
&& not profitable
&& !bearish_ma60_stop_guard
```

这种退出的 `signal_result` 是 `unresolved`，不是普通失败：

```text
signal_result = unresolved
reason = observation window ended without structural exit
```

默认 `observation_bars = 240`。默认组合中是 360。

### 6.8 止损优先级

如果同一根 K 线同时满足多个止损条件，源码按以下顺序选择原因：

1. 动态 TR 止损：`dynamic stop zone hit`
2. 连续未创新低：`loss holding failed to make new lows`
3. 收盘突破反弹高点：`loss holding closed above rebound high`
4. 连续站稳 MA20：`loss holding confirmed MA20 reclaim`
5. 观察窗口结束：`observation window ended without structural exit`

普通止损返回：

```text
target_position = 0
signal = CLOSE_SHORT
signal_result = failure
```

观察窗口结束返回：

```text
target_position = 0
signal = CLOSE_SHORT
signal_result = unresolved
```

## 7. 止盈算法

止盈只在盈利状态下触发：

```text
profitable = close < entry
```

当前实现有两套止盈逻辑：

1. ZigZag 波谷止盈。
2. MA20/MA60 反弹止盈。

最终是否启用 MA20/MA60 反弹止盈，受 `prefer_zigzag_trough_take_profit` 控制。

### 7.1 ZigZag 波谷止盈

Go 侧会把 ZigZag 指标确认后的波谷注入：

```json
{
  "features": {
    "zigzag_atr26": {
      "troughs": [
        {
          "pivot_index": 33,
          "pivot_time": "...",
          "pivot_price": 98.0,
          "confirmed_time": "...",
          "confirmed_index": 38
        }
      ]
    }
  }
}
```

策略判断：

```text
zigzag_trough_take =
  profitable
  && use_zigzag_trough_take_profit
  && has_confirmed_trough_after_signal
```

`has_confirmed_trough_after_signal` 要求：

```text
signal_bar_index != nil
&& trough.pivot_index >= signal_bar_index
&& trough.confirmed_index >= trough.pivot_index
```

这表示：空单开仓之后形成了一个已确认 ZigZag 波谷。因为空单盈利通常来自价格下跌，确认波谷意味着下跌段可能结束或进入反弹段，所以策略先兑现利润。

ZigZag 波谷止盈不要求 `signal_ma20_touched == true`。只要当前空单盈利，并且有入场后的已确认波谷，就可以止盈。

触发后：

```text
target_position = 0
signal = CLOSE_SHORT
signal_result = success
reason = ATR ZigZag trough confirmed after short profit
```

### 7.2 prefer_zigzag_trough_take_profit 的影响

源码最终止盈表达式：

```text
ma_reclaim_take =
  close_above_ma60_take
  || below_ma60_take

prefer_zigzag_take =
  use_zigzag_trough_take_profit
  && prefer_zigzag_trough_take_profit

take_profit =
  zigzag_trough_take
  || (ma_reclaim_take && !prefer_zigzag_take)
```

因此：

| 配置 | 行为 |
| --- | --- |
| `use_zigzag_trough_take_profit=true` 且 `prefer_zigzag_trough_take_profit=true` | 只要没有 ZigZag 波谷，MA20/MA60 反弹止盈不会触发 |
| `use_zigzag_trough_take_profit=true` 且 `prefer_zigzag_trough_take_profit=false` | ZigZag 波谷和 MA20/MA60 反弹都可以止盈 |
| `use_zigzag_trough_take_profit=false` | 只使用 MA20/MA60 反弹止盈 |

默认源码和默认组合都把 `prefer_zigzag_trough_take_profit` 设为 `true`。所以默认运行时，盈利空单会优先等待入场后的 ZigZag 波谷；MA20/MA60 反弹止盈被关闭。

### 7.3 MA20/MA60 反弹止盈的前置条件

MA 反弹止盈要求持仓后价格至少触碰过 MA20：

```text
signal_ma20_touched = true
```

这个标记在任意持仓 K 线满足下面条件时置为真：

```text
high >= ma20
```

没有触碰 MA20 前，即使空单已有浮盈，也不会通过 MA20/MA60 反弹规则止盈。

### 7.4 收盘站上 MA60 止盈

如果空单盈利，且持仓后已经触碰过 MA20，当前收盘价进一步站上 MA60，则触发 MA 反弹止盈：

```text
close_above_ma60_take =
  profitable
  && signal_ma20_touched
  && ma60 != nil
  && close > ma60
```

含义：盈利后反弹强度已经大到收复 MA60，继续持有空单的趋势优势下降，先止盈。

该规则只有在 `prefer_zigzag_take == false` 时才会真正触发。

### 7.5 MA60 下方站上 MA20 的反弹强度止盈

如果还没有站上 MA60，但已经站上 MA20，则进一步看反弹距离和均线斜率。

前置：

```text
below_ma60_after_ma20_reclaim =
  profitable
  && signal_ma20_touched
  && ma60 != nil
  && close <= ma60
  && close > ma20
```

反弹距离：

```text
ma20_reclaim_distance = close - ma20
```

如果反弹距离小于 ATR26，不止盈：

```text
ma20_reclaim_distance < atr26
```

如果反弹距离达到 ATR26，再看 MA20 和 MA60 斜率：

```text
ma20_slope_down = ma20_slope < 0
ma60_slope_down = ma60_slope < 0
```

当 MA20 和 MA60 双双向下时，不止盈，让利润继续奔跑：

```text
ma_slope_down_take_block =
  ma20_reclaim_distance >= atr26
  && ma20_slope_down
  && ma60_slope_down
```

当反弹距离达到 ATR26，且不是双均线向下时，触发止盈：

```text
below_ma60_take =
  below_ma60_after_ma20_reclaim
  && atr26 != nil
  && ma20_reclaim_distance >= atr26
  && !(ma20_slope_down && ma60_slope_down)
```

该规则同样只有在 `prefer_zigzag_take == false` 时才会真正触发。

### 7.6 止盈优先级

源码先计算止盈，再计算止损。

止盈内部优先级：

1. ZigZag 波谷止盈。
2. 收盘站上 MA60 止盈。
3. MA60 下方站上 MA20 且反弹距离达到 ATR26，且 MA20/MA60 不是双双向下。

但在默认配置下，因为 `prefer_zigzag_trough_take_profit=true`，第 2 和第 3 类 MA 反弹止盈不会触发，只会作为等待状态里的解释性检查。

止盈返回：

```text
target_position = 0
signal = CLOSE_SHORT
signal_result = success
```

## 8. ZigZag feature 在本策略中的作用

ZigZag 是独立指标策略 `indicator.zigzag_atr26` 产生的 feature，不是 `ma20.state_diagram_short` 自己计算的。

Go 侧缓存最近最多 64 个 ZigZag 结构点，向策略请求注入最近最多 3 个波峰和 3 个波谷：

```json
{
  "features": {
    "zigzag_atr26": {
      "peaks": [],
      "troughs": []
    }
  }
}
```

当前 MA20 状态图中的用途：

| ZigZag 类型 | 当前用途 |
| --- | --- |
| 波峰 `PEAK` | 可选入场过滤，要求二次跌破时最近 N 根内出现过波峰 |
| 波谷 `TROUGH` | 可选止盈，盈利后若入场之后出现已确认波谷则平空 |

缺失 ZigZag feature 时：

- 如果入场波峰区间仍是默认无限区间，则开仓流程不受影响。
- 如果配置了有限 `entry_zigzag_peak_max_bars`，缺失波峰会导致入场过滤失败。
- 如果启用 ZigZag 波谷优先止盈但没有波谷，则不会触发 ZigZag 止盈。

## 9. 一轮交易的伪代码

```text
on_bar(bar):
  update current_bar_index
  update zigzag features
  append close/TR
  compute ma20, ma60, slopes, atr26, stop_tr_avg

  if ma20 not ready:
    wait

  if state == INIT:
    if close > ma20:
      state = ABOVE_MA20
    else:
      wait

  if state == ABOVE_MA20:
    if previous_close >= previous_ma && close < ma20:
      state = BELOW_MA20
    else:
      wait

  if state == BELOW_MA20:
    if open > ma20 && close > ma20 for enough bars:
      reset to ABOVE_MA20
    else if high >= ma20:
      record touch_open/touch_close/touch_high/touch_ma20
      state = REBOUND_TO_HIGH
    else:
      wait

  if state == REBOUND_TO_HIGH:
    wait_bars += 1
    if open > ma20 && close > ma20 for enough bars:
      reset to ABOVE_MA20
    else if close < ma20:
      if entry filters fail:
        reset to INIT
      else:
        entry = close
        emit SHORT target_position=-1
        state = LOSS_HOLDING
    else if wait_bars > max_wait_bars:
      clear touch info
      state = BELOW_MA20
    else:
      wait

  if state in PROFIT_HOLDING / LOSS_HOLDING:
    update signal_bars, lowest low, no-new-low count, ma20 touch flag
    profitable = close < entry

    if profitable and zigzag trough after signal:
      emit CLOSE_SHORT TAKE_PROFIT
      reset to INIT

    if profitable and !prefer_zigzag_take:
      if touched MA20 and close > ma60:
        emit CLOSE_SHORT TAKE_PROFIT
      else if touched MA20 and close > ma20 and close <= ma60:
        if close - ma20 >= atr26 and not (ma20_slope < 0 and ma60_slope < 0):
          emit CLOSE_SHORT TAKE_PROFIT

    if not profitable:
      compute hit_stop / no_new_low_stop / break_rebound_high_stop / stood_above_stop
      if bearish_ma60_stop_guard:
        wait
      else if any stop condition:
        emit CLOSE_SHORT STOP_LOSS
        reset to INIT
      else if signal_bars >= observation_bars:
        emit CLOSE_SHORT STOP_LOSS unresolved
        reset to INIT
      else:
        wait
```

## 10. 容易误解的点

1. 二次入场不是跌破 `trigger_line`，而是 `close < ma20`。
2. `trigger_line = min(touch_open, touch_close)` 当前只用于记录和展示。
3. 单根 K 线站上 MA20 不会直接止损，需要连续达到 `reclaim_confirm_bars`。
4. 默认源码启用 ZigZag 波谷优先止盈；在 `prefer_zigzag_trough_take_profit=true` 时，MA20/MA60 反弹止盈不会真正平仓。
5. ZigZag 波峰默认不限制入场；只有配置有限区间后，才会要求近期波峰。
6. MA60 空头保护会拦截亏损止损和观察窗口结束，但不会拦截盈利止盈。
7. 策略按 K 线收盘推进，不按 tick 噪声提前开仓或平仓。
