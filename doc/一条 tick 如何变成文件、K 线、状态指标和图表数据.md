# Tick 到落库链路导读

这份文档按源码实际调用顺序梳理本工程从 CTP tick 接收到最后落库的全过程。

文档里的文件链接使用相对路径编写，适合在 VSCode 中打开本文件后通过 Markdown 预览点击，直接跳到对应源文件。

## 总览

主链路可以概括为：

`CTP 回调 -> tick 时间归位 -> 合约 1m 聚合 -> 合约 mm 聚合 -> L9 1m 聚合 -> L9 mm 聚合 -> dbBatchWriter 批量落库`

关键文件：

- [quotes/service.go](../internal/quotes/service.go)
- [quotes/md_spi.go](../internal/quotes/md_spi.go)
- [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go)
- [quotes/l9_async.go](../internal/quotes/l9_async.go)
- [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go)
- [quotes/kline_store.go](../internal/quotes/kline_store.go)
- [klineagg/aggregate.go](../internal/klineagg/aggregate.go)
- [sessiontime/sessiontime.go](../internal/sessiontime/sessiontime.go)
- [klineclock/clock.go](../internal/klineclock/clock.go)

最终落到四类表：

- 合约 1m：`future_kline_instrument_1m_<variety>`
- 合约 mm：`future_kline_instrument_mm_<variety>`
- L9 1m：`future_kline_l9_1m_<variety>`
- L9 mm：`future_kline_l9_mm_<variety>`

## 1. 入口：CTP 回调进入 runtime

入口在 [quotes/md_spi.go](../internal/quotes/md_spi.go) 的 `OnRtnDepthMarketData`。

它做的事情很轻：

- 从 CTP 回调结构里提取 `InstrumentID`、`TradingDay`、`UpdateTime`、`LastPrice`、`Volume`、`OpenInterest` 等字段
- 对价格和结算价做基础清洗
- 封装成 `tickInputData`
- 交给 `marketDataRuntime.onLiveTick`

真正的分钟线聚合不在回调线程里做，而是继续交给 runtime。

## 2. 路由：按合约分 shard

路由入口在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `enqueue`。

这里的设计目标是：

- 同一合约的 tick 保持顺序
- 不同合约并行处理
- 回调线程尽量不被阻塞

具体做法：

- 用 `shardForInstrument(instrumentID)` 对合约做 hash
- 同一合约固定路由到同一个 `marketDataShard`
- 先旁路异步写 tick CSV
- 再把 `runtimeTick` 非阻塞投递到 shard 队列
- 队列满时直接丢弃 tick，并记录告警和指标

## 3. 时间归位：tick 怎么映射到 1m 标签时间

真正的时间归位发生在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `processTick`，内部调用 [quotes/md_spi.go](../internal/quotes/md_spi.go) 的 `parseTickTimesWithMillis`。

这里会产出三个时间：

- `MinuteTime`：业务标签时间，对应后续落库的 `DataTime`
- `AdjustedTime`：跨夜修正后的连续时间轴
- `adjustedTickTime`：带毫秒的真实 tick 时刻，用于延迟和漂移分析

### 3.1 为什么 09:00 会归到 09:01

规则在 [sessiontime/sessiontime.go](../internal/sessiontime/sessiontime.go) 的 `LabelMinute`。

本工程的 1m 标签使用“结束标签分钟”：

- `09:00:xx` 这一分钟记为 `09:01`
- `09:01:xx` 这一分钟记为 `09:02`

因此 1m 的 `DataTime` 表示的是“这根分钟线结束时刻”，不是开始时刻。

### 3.2 为什么夜盘要有 AdjustedTime

规则在 [klineclock/clock.go](../internal/klineclock/clock.go) 的 `BuildBarTimes`。

如果直接把夜盘分钟挂在 `tradingDay` 上，会出现时间轴错位。例如：

- 业务交易日是 `2026-01-20`
- 夜盘 `21:00` 实际发生在自然日 `2026-01-19` 晚上
- 凌晨 `00:30` 实际发生在自然日 `2026-01-20` 凌晨

为了让图表和聚合逻辑看到一条连续时间轴，系统引入 `AdjustedTime`：

- `21:00 ~ 23:59` 挂到“上一交易日”的自然日晚间
- `00:00 ~ 07:xx` 挂到“上一交易日晚盘之后”的自然日凌晨

这样 `21:00 -> 23:59 -> 00:00 -> 02:30` 在 `AdjustedTime` 上是连续的。

## 4. 合约 1m：tick 怎么聚成当前分钟 bar

核心逻辑在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `processTick`。

每个合约对应一个 `instrumentRuntimeState`，里面最重要的是：

- 当前正在构造的 `bar`
- 上一个有效 tick 的快照 `lastTick`
- 上一分钟结束时累计成交量基准
- 一个专门做多周期聚合的 `closedBarAggregator`

同一分钟内，更新规则是：

- `Open`：第一笔 tick 价格
- `High/Low`：分钟内极值
- `Close`：最后一笔 tick 价格
- `Volume`：当前累计成交量减去本分钟起点累计量/这儿应该是错的，应该是当前累计成交量-上分钟终点成交量
- `OpenInterest`：使用最后一笔 tick 的持仓量
- `SettlementPrice`：使用最后一笔 tick 的结算价

这里特别重要的是：

- CTP 的 `Volume` 是累计成交量，不是逐笔成交量
- 所以系统用 `computeBucketVolume` 做差分，得到当前分钟成交量

## 5. 分钟切换：旧 1m bar 在哪里封口

还是在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `processTick`。

当当前 tick 归到的 `minuteTime` 和 `state.bar.MinuteTime` 不同时，说明进入了新分钟：

- 旧 `state.bar` 封口，成为 `closedBar`
- 构造 `runtimeTrace`
- 调用 `buildPersistTasks(closedBar, trace, false)`
- 当前 tick 同时初始化下一根新 1m bar

这一刻是整条链路最关键的分叉点，因为后面的 1m/mm/L9 都从这根“刚封口的 1m”继续展开。

## 6. 合约 mm：1m 怎么继续变成 5m/15m/30m/1h/120m/1d

入口在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `buildPersistTasks`，聚合执行在 `closedBarAggregator.Consume`。

### 6.1 实时链路里生成哪些周期

实时链路里，`closedBarAggregator` 会继续尝试生成：

- `5m`
- `15m`
- `30m`
- `1h`
- `120m`
- `1d`

### 6.2 切桶规则在哪里

核心算法在 [klineagg/aggregate.go](../internal/klineagg/aggregate.go) 的 `Aggregate`。

它不是按自然时间直接整除，而是按“可交易标签分钟序列”切桶：

- 先把 session 展开成标签分钟序列
- 给每个分钟一个 `GlobalSeq`、`SessionID`、`SessionSeq`
- 再据此决定该分钟属于哪个桶

这意味着：

- `5m/15m` 严格在各自 session 内切桶
- `30m/1h` 可以按配置跨 session 拼接
- session 末尾可能出现短桶

### 6.3 桶完整性怎么判断

`Aggregate` 同时输出：

- `ExpectedMinutes`：该桶理论上应包含多少个 1m
- `ActualMinutes`：当前实际已经聚合到多少个 1m

实时链路里，上层只会发射已经完整闭合的桶；不完整的桶继续留待后续分钟补齐。

## 7. L9 1m：同品种多合约怎样算成加权指数

入口仍然在 [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go) 的 `buildPersistTasks`：

- 先把封口的合约 1m 用 `ObserveMinuteBar` 记入 L9 缓存
- 再调用 `Submit(variety, minuteTime)` 投递异步任务

L9 计算逻辑在 [quotes/l9_async.go](../internal/quotes/l9_async.go)：

- `ObserveMinuteBar`
- `Submit`
- `computeAndStore`
- `snapshotBarsForMinute`

### 7.1 为什么 L9 是异步的

因为 L9 的输入不是单个合约，而是“同品种该分钟所有合约的 1m 快照”。

如果在主 shard 里同步做，会拖慢主 tick 路径，所以这里单独有 `l9AsyncCalculator` worker。

### 7.2 权重怎么算

`computeAndStore` 按 `OpenInterest` 做加权：

- `Open/High/Low/Close/SettlementPrice`：`sum(value * OI) / sum(OI)`
- `Volume`：直接求和
- `OpenInterest`：各合约 OI 总和

生成后的 L9 合约号固定是：

- `InstrumentID = variety + "l9"`
- `Exchange = "L9"`

### 7.3 某个合约该分钟没成交怎么办

规则在 [quotes/l9_async.go](../internal/quotes/l9_async.go) 的 `snapshotBarsForMinute`。

如果某个合约该分钟没有新的 1m bar：

- 用最近一根 1m 的价格、持仓量、结算价补齐
- 该分钟 `Volume = 0`

这样做的目的是避免 L9 因为某个合约短时间没有成交而断档。

## 8. L9 mm：L9 1m 继续变成多周期

L9 1m 生成之后，并不会停在 1m。

`computeAndStore` 内部会继续调用一个专属于该品种 L9 的 `closedBarAggregator`，复用和普通合约相同的 mm 聚合逻辑，再生成：

- `5m`
- `15m`
- `30m`
- `1h`
- `120m`
- `1d`

最终形成 L9 mm 的持久化任务。

## 9. 落库：四类表怎么进入 dbBatchWriter

所有落库任务都会先变成 `persistTask`，再进入 [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go) 的 `dbBatchWriter`。

这里分成两条车道：

- 普通合约 1m：走 `minuteWorkers`
- mm 和 L9：先进入 `mmDeferred` 去重，再分发到 `mmWorkers`/这儿去重有意义吗？

关键函数：

- [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go) `Enqueue`
- [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go) `runMMDeferred`
- [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go) `flush`
- [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go) `buildUpsertStatement`

写库策略：

- 按表聚合成批
- 使用多值 `INSERT ... ON DUPLICATE KEY UPDATE`
- 对 mm/L9 任务按 `表 + 合约 + 周期 + 分钟` 去重

这样能显著减少 SQL 次数，也避免 mm/L9 抢占 1m 的低延迟写入通道。

## 10. 一条链路串起来看

建议按下面顺序阅读源码：

1. [quotes/md_spi.go](../internal/quotes/md_spi.go)
2. [quotes/market_data_runtime.go](../internal/quotes/market_data_runtime.go)
3. [sessiontime/sessiontime.go](../internal/sessiontime/sessiontime.go)
4. [klineclock/clock.go](../internal/klineclock/clock.go)
5. [quotes/l9_async.go](../internal/quotes/l9_async.go)
6. [klineagg/aggregate.go](../internal/klineagg/aggregate.go)
7. [quotes/market_data_db_writer.go](../internal/quotes/market_data_db_writer.go)
8. [quotes/kline_store.go](../internal/quotes/kline_store.go)

顺着读下来，就是：

- tick 时间归位
- 1m 标签时间确定
- AdjustedTime 连续时间轴修正
- 合约 1m 聚合
- 合约 mm 聚合
- L9 1m 聚合
- L9 mm 聚合
- 批量落库
