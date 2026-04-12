# 一条 tick 如何变成文件、K 线、状态指标和图表数据

这份文档专门回答一个问题：

**一条来自 CTP 的实时 tick，是如何在本工程里一路变成文件、K 线、状态指标和图表数据的？**

相比 [tick_to_storage_pipeline.md](../tick_to_storage_pipeline.md)，这份文档更强调“同一条 tick 在系统里分叉成哪些产物”，适合新接手开发者先抓主线。

## 总览

从结果上看，一条 tick 进入系统后，最终会产生 4 类产物：

1. **文件产物**
   实时 tick CSV，按合约拆分保存到 `flow/ticks/<instrument>.csv`
2. **K 线产物**
   普通合约 1m、普通合约 mm、L9 1m、L9 mm，最终落到 MySQL
3. **状态产物**
   连接状态、延迟、队列深度、DB flush、文件 flush 等运行态指标
4. **图表产物**
   图表实时 partial/final bar、图表最新行情快照和最近 tick 列表

如果只看调用顺序，这条链路大致是：

`CTP 回调 -> mdSpi -> marketDataRuntime -> shard -> fileWriter / 聚合 / dbWriter -> RuntimeStatusCenter / ChartStream`

关键文件：

- [quotes/md_spi.go](../../internal/quotes/md_spi.go)
- [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)
- [quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)
- [quotes/runtime_status.go](../../internal/quotes/runtime_status.go)
- [quotes/chart_stream.go](../../internal/quotes/chart_stream.go)

---

## 1. 第一步：CTP 回调把交易所数据送进系统

入口在 [quotes/md_spi.go](../../internal/quotes/md_spi.go) 的 `OnRtnDepthMarketData`。

这里做的事情并不复杂，但非常关键：

- 从 CTP 回调结构里提取行情字段
- 对所有交易所浮点字段做基础清洗
  - `NaN`
  - `+Inf`
  - `-Inf`
  - 极大异常值
  会被统一置为 `0`
- 封装成内部 `tickInputData`
- 调用 `marketDataRuntime.onLiveTick`

这一步的定位可以理解为：

- **CTP 协议层 -> 系统内部统一 tick 格式**

这一步还没有开始分钟线聚合，也没有写库。它只是把“交易所原始输入”转换成“系统可消费的输入”。

建议先看：

- [quotes/md_spi.go](../../internal/quotes/md_spi.go)

---

## 2. 第二步：runtime 接住 tick，并先分叉出“文件落盘”旁路

入口在 [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go) 的 `onLiveTick`。

这里会把 `tickInputData` 转成内部 `tickEvent`。`tickEvent` 比回调原始结构更接近业务处理，除了行情字段，还会带上：

- `ReceivedAt`
- `CallbackAt`
- `ProcessStartedAt`
- 旁路处理相关时间戳

接着 `onLiveTick` 会进入 `enqueue(runtimeTick)`。

在 `enqueue` 里，系统做了两件并行的事情：

1. **先把 tick 异步投递给 shard 对应的文件 writer**
2. **再把 tick 投递进 shard 队列，进入正式聚合链路**

这段逻辑在 [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go) 的 `enqueue` 很关键：

- `fileWriter.Enqueue(t.tickEvent)` 是文件侧旁路
- `shard.in <- t` 是主处理链路

这里的设计目的很明确：

- tick 文件记录要尽量完整
- 但文件 I/O 不能阻塞主行情处理线程

所以 tick CSV 是“旁路异步产物”，不是主链路上的同步步骤。

建议先看：

- [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)

---

## 3. 第三步：这条 tick 如何变成文件

文件侧入口在 [quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)。

每个 shard 有一个 `shardFileWriter`，收到 `tickEvent` 后会：

- 按合约拆文件
- 写入 `flow/ticks/<instrument>.csv`
- 用缓冲写降低频繁 I/O
- 周期性 flush
- 周期性 fsync

### 3.1 文件路径

tick 文件目录来自 `flow_path/ticks`，每个合约一个文件，例如：

- `flow/ticks/rb2505.csv`
- `flow/ticks/ag2606.csv`

### 3.2 写入时机

写入不是每条 tick 都立刻 `fsync`。

当前实现是：

- 先写入内存缓冲
- 定期 `flush`
- 更长周期再 `fsync`

这样能兼顾性能和可追溯性。

### 3.3 写入字段

tick CSV 当前写入的字段有：

- `received_at`
- `instrument_id`
- `exchange_id`
- `exchange_inst_id`
- `trading_day`
- `action_day`
- `update_time`
- `last_price`
- `pre_settlement_price`
- `pre_close_price`
- `pre_open_interest`
- `open_price`
- `highest_price`
- `lowest_price`
- `volume`
- `turnover`
- `open_interest`
- `close_price`
- `settlement_price`
- `upper_limit_price`
- `lower_limit_price`
- `average_price`
- `pre_delta`
- `curr_delta`
- `bid_price1`
- `ask_price1`
- `update_millisec`
- `bid_volume1`
- `ask_volume1`

这些字段都是“接近交易所原始输入”的 tick 主字段，不包含系统内部调试时间戳。

### 3.4 数值规则

当前 tick 文件落盘有两个保护规则：

- 所有交易所浮点字段在进入系统时都做非有限值清洗，异常值统一写成 `0`
- 文件中的浮点数统一保留 **3 位小数**

所以 tick 文件是“清洗后的原始输入记录”，不是未经处理的原始二进制镜像。

建议先看：

- [quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)

---

## 4. 第四步：这条 tick 如何进入 shard，并开始分钟线聚合

主处理链路从 shard 开始。

系统会按合约把 tick hash 到固定 shard，目标是：

- 同一合约保持顺序
- 不同合约并行处理
- 回调线程尽量少阻塞

进入 shard 后，`processTick` 会完成几件核心事情：

- 解析交易时间和标签时间
- 做 tick 去重
- 做漂移检测
- 更新当前合约正在构造的 1m bar
- 判断是否发生“分钟切换”

### 4.1 这条 tick 不是立刻生成一根 K 线

tick 自己不会直接产出一根完整 1m bar。

系统会先把它并入“当前分钟正在构造中的 bar”：

- 第一笔 tick 决定 `Open`
- 后续 tick 持续更新 `High/Low`
- 最后一笔 tick 不断更新 `Close`
- 成交量用累计量做差分
- 持仓和结算价取最后一笔

### 4.2 真正产出 K 线的时机

当下一条 tick 落到“新分钟”时，上一分钟的 bar 才会封口，变成 `closedBar`。

也就是说：

- tick A 落到 09:30 分钟，只是在更新“09:31 标签”的当前 bar
- 直到后续 tick B 进入下一分钟，09:31 那根 bar 才真正封口

这一步决定了后续所有 K 线产物的起点。

建议先看：

- [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [sessiontime/sessiontime.go](../../internal/sessiontime/sessiontime.go)
- [klineclock/clock.go](../../internal/klineclock/clock.go)

---

## 5. 第五步：这条 tick 如何变成 K 线

tick 自己不会直接写进 K 线表，而是先推动 1m bar 封口，再从封口 bar 扩展出更多周期。

### 5.1 普通合约 1m

当 `closedBar` 生成后，系统首先构造普通合约 1m 的 `persistTask`。

这根 bar 最终写入：

- `future_kline_instrument_1m_<variety>`

### 5.2 普通合约 mm

同一根已经封口的 1m bar 会继续交给多周期聚合器，尝试生成：

- `5m`
- `15m`
- `30m`
- `1h`
- `120m`
- `1d`

这些 mm bar 最终写入：

- `future_kline_instrument_mm_<variety>`

### 5.3 L9 1m

封口后的合约 1m bar 还会被记入 L9 异步计算器。

L9 的输入不是一条 tick，也不是一个合约，而是：

- **同品种、同一分钟、多个合约的 1m 快照**

L9 1m 最终写入：

- `future_kline_l9_1m_<variety>`

### 5.4 L9 mm

L9 1m 再继续做高周期聚合，形成：

- `future_kline_l9_mm_<variety>`

### 5.5 真正写库在哪里发生

上述所有 `persistTask` 最终都会进入 `dbBatchWriter`，再批量 upsert 到 MySQL。

也就是说：

- tick 本身不直接写 K 线表
- tick 先推动 bar 形成
- bar 再推动持久化任务
- 持久化任务最终由 DB writer 批量落库

建议先看：

- [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [quotes/l9_async.go](../../internal/quotes/l9_async.go)
- [quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)
- [quotes/kline_store.go](../../internal/quotes/kline_store.go)

---

## 6. 第六步：这条 tick 如何变成状态指标

除了文件和 K 线，这条 tick 还会持续更新运行状态。

统一状态中心在 [quotes/runtime_status.go](../../internal/quotes/runtime_status.go)。

它负责汇总：

- 查询前置、MD 前置、登录、订阅状态
- tick 去重和漂移状态
- 路由排队、shard 排队、DB 排队
- DB flush、文件 flush
- 队列深度
- 端到端耗时

### 6.1 tick 自己贡献的状态

一条 tick 会直接或间接推动这些指标变化：

- `LastTickTime`
- `ServerTime`
- `RouterQueueMS`
- `ShardQueueMS`
- `PersistQueueMS`
- `EndToEndMS`

### 6.2 为什么状态指标不是“后补”的

这些指标不是离线扫描日志得出来的，而是在实时主链路各阶段被直接打点：

- 回调时打点
- 入 shard 时打点
- 入 DB 队列时打点
- flush 时打点
- 文件写盘时打点

所以状态页本质上是主链路运行时的侧写。

建议先看：

- [quotes/runtime_status.go](../../internal/quotes/runtime_status.go)
- [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)

---

## 7. 第七步：这条 tick 如何变成图表数据

tick 和 bar 并不只写文件、写库，它们还会进入图表实时流。

图表相关核心在 [quotes/chart_stream.go](../../internal/quotes/chart_stream.go)。

### 7.1 tick 侧图表产物

实时 tick 会推动图表侧更新：

- 最新行情快照
- 买一卖一
- 最新价格
- 最近若干条 tick 行

也就是前端行情面板能实时看到的 quote 数据。

### 7.2 bar 侧图表产物

bar 会形成两种图表事件：

- **partial bar**
  当前分钟尚未封口，但要实时给图表预览
- **final bar**
  已封口，作为正式 K 线广播给图表

### 7.3 为什么图表和 DB 不是同一条路径

图表要求低延迟，而 DB 写库允许异步批量。

所以系统把图表更新放在更靠前的位置，保证：

- 图表能尽快看到行情变化
- DB 可以用更合理的批量策略提高吞吐

这也是为什么用户看到图表变化，不代表那根 bar 已经落到 MySQL。

建议先看：

- [quotes/chart_stream.go](../../internal/quotes/chart_stream.go)
- [quotes/service.go](../../internal/quotes/service.go)

---

## 8. 一条 tick 在系统里的四类终点

把整条链路压缩成一句话，可以这样理解：

### 8.1 文件终点

这条 tick 会被异步记录进：

- `flow/ticks/<instrument>.csv`

### 8.2 K 线终点

这条 tick 不直接写 K 线，但会推动生成：

- 合约 1m
- 合约 mm
- L9 1m
- L9 mm

最终进入 MySQL。

### 8.3 状态终点

这条 tick 会更新运行时状态中心里的：

- 连接状态
- 延迟
- 队列
- flush
- 端到端耗时

最终通过 HTTP / WebSocket 暴露到前端。

### 8.4 图表终点

这条 tick 会推动图表侧形成：

- 最新行情快照
- 最近 tick 列表
- partial bar
- final bar

最终成为用户在图表和行情面板上看到的实时数据。

---

## 9. 新人最容易误解的地方

### 9.1 不是“来一条 tick 就写一条 K 线”

tick 是 bar 的原料，不是 bar 本身。K 线要在分钟切换时才真正封口。

### 9.2 图表快，不等于数据库已经写完

图表更新是更靠前的实时侧产物，DB 写入是后面的异步批量产物。

### 9.3 tick 文件是旁路，不决定聚合逻辑

tick CSV 主要用于排障、核对和 replay，不是主链路的业务判断依据。

### 9.4 replay 也是走同一套主链路

回放不是另写一遍 K 线逻辑，而是把历史 tick 重新送进 `quotes` 主链路。

---

## 10. 建议阅读顺序

如果你要把“这条 tick 如何一路变成 4 类产物”真正看懂，建议按这个顺序读：

1. [quotes/md_spi.go](../../internal/quotes/md_spi.go)
2. [quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
3. [quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)
4. [quotes/l9_async.go](../../internal/quotes/l9_async.go)
5. [quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)
6. [quotes/runtime_status.go](../../internal/quotes/runtime_status.go)
7. [quotes/chart_stream.go](../../internal/quotes/chart_stream.go)

如果再配合 [tick_to_storage_pipeline.md](../tick_to_storage_pipeline.md) 一起看，会更容易把“业务主线”和“产物分叉点”对起来。
