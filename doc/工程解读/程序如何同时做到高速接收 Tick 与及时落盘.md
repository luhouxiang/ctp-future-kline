# 程序如何同时做到高速接收 Tick 与及时落盘

这篇文档只回答一个问题：

在 CTP 高频 tick 持续涌入时，程序为什么既能尽量快地把 tick 接进来，又能把数据及时写到文件和数据库，而不是因为 I/O 或聚合计算把主链路拖垮。

结论先说：

- 接收侧靠的是“回调轻量化 + 分片串行 + 多级异步队列”
- 文件侧靠的是“按 shard 异步写文件 + 定期 flush/fsync”
- 数据库侧靠的是“持久化任务异步化 + 分 worker 批量 flush + 溢写到磁盘”
- 兜底靠的是“队列观测、延迟指标、磁盘 spool、必要时阻塞而不是静默丢失”

如果只记一句话，可以记成：

`CTP 回调尽快把 tick 交给 runtime，runtime 尽快把 I/O 交给专门 writer，writer 用批量和定时刷盘把吞吐与时效折中起来。`

## 1. 主思路：把“接收”和“落盘”解耦

这个工程没有把“收到一个 tick 就立刻同步写文件、同步写数据库”作为处理模式。

如果这么做，CTP 回调线程会被磁盘 I/O、数据库事务、CSV flush、索引更新这些慢操作卡住；一旦上游流量抬升，后果通常是：

- 回调线程处理不过来
- 内部积压快速扩大
- tick 延迟越来越高
- 最终开始丢数据或长时间阻塞

所以这里采用的是典型的生产者/消费者拆分：

1. CTP 回调线程只做最必要的数据整理和投递
2. `marketDataRuntime` 按合约把 tick 路由到固定 shard
3. shard 内只串行完成“必须依赖顺序”的状态更新和 1m 聚合
4. 文件输出和数据库写入都交给独立的异步 writer
5. 当内存队列顶满时，优先溢写到磁盘 spool，避免直接丢失

对应主链路在代码里大致是：

`OnRtnDepthMarketData -> mdSpi -> marketDataRuntime -> marketDataShard -> fileWriter / dbBatchWriter`

关键文件：

- [internal/quotes/md_spi.go](../../internal/quotes/md_spi.go)
- [internal/quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [internal/quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)
- [internal/quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)
- [internal/queuewatch/spool.go](../../internal/queuewatch/spool.go)

## 2. 为什么高速 tick 接收不会马上被拖慢

### 2.1 CTP 回调不直接做重活

`mdSpi` 的职责是把 CTP 原始结构整理成内部 tick 输入，再尽快交给 `marketDataRuntime`。

这里的核心思想是：回调线程不承担文件写入、数据库写入这类慢操作。这样做的直接收益是，接收能力主要受内存队列和 CPU 轻计算影响，而不是受磁盘和数据库抖动影响。

也就是说，程序先确保“接得住”，再通过后面的异步链路保证“写得出去”。

### 2.2 按合约 hash 到 shard，局部串行，整体并行

`marketDataRuntime` 会按合约把 tick 分配到固定 shard。这样做同时解决了两个问题：

- 同一合约总是进入同一个 shard，顺序天然可控
- 不同合约可以分散到多个 shard，被多个 goroutine 并行处理

这就是“单合约串行、全局并行”的设计。

如果不分 shard，而是所有合约共用一把大锁或一个总队列，高峰时很容易互相阻塞；而现在每个 shard 都有自己的输入队列、状态和文件 writer，热点只会局部放大，不会把全局都拖死。

相关实现见：

- [internal/quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)

重点可以看这些位置：

- `newMarketDataRuntime`：初始化 shard、队列、db writer、file writer
- `SubmitTick` 附近逻辑：把 tick 投递到 shard
- `shardForInstrument`：按合约稳定映射到 shard
- `marketDataShard.run`：串行消费 shard 队列

### 2.3 shard 内只做必须同步的事

进入 shard 后，程序会在 shard goroutine 内完成这些必须顺序一致的处理：

- 时间规整
- 去重
- 漂移检测
- 合约级聚合状态更新
- 1m bar 封口

这部分必须在同一条串行链路里完成，因为它依赖同一合约 tick 的先后关系。

但更重的 I/O 工作并不在这里完成：

- tick 文件写入由 `shardFileWriter` 异步处理
- 数据库持久化由 `dbBatchWriter` 异步处理
- L9 计算也被拆到 `l9AsyncCalculator` 异步处理，避免拖慢主 shard

这意味着 shard 线程做的是“实时性要求最高且必须按序的那一小段”，而不是把所有事情都扛在自己身上。

## 3. 为什么文件落盘既及时又不阻塞主链路

### 3.1 文件写入是旁路异步的

在 `marketDataRuntime` 中，实时 tick 会优先被投递给对应 shard 的 `fileWriter`，但这个投递只是入队，不是同步写文件。

这样主处理链路只承担一次 channel enqueue 的成本，不承担实际磁盘写成本。

对应实现见：

- [internal/quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [internal/quotes/market_data_file_writer.go](../../internal/quotes/market_data_file_writer.go)

### 3.2 每个 shard 一个文件 writer，减少锁竞争

`shardFileWriter` 是按 shard 建立的，不是全局共用一个 writer。

这样做有两个直接好处：

- 各 shard 文件写入互不抢同一条执行通道
- 文件侧压力被分摊，不容易出现“所有合约争一个写线程”的瓶颈

### 3.3 flush 和 fsync 分层处理，兼顾时效与吞吐

文件 writer 并不是每写一行都立刻 `Flush + fsync`，而是采用两级时钟：

- 较短周期做 `Flush`
- 较长周期做 `fsync`

从实现上看，`market_data_file_writer.go` 默认是：

- `flushInterval = 500ms`
- `fsyncInterval = 5s`

这是一种典型折中：

- `Flush` 让应用层缓冲尽快推到内核缓冲，保证“看起来已经持续落盘”
- `fsync` 不必每条都做，避免把磁盘同步写放大成吞吐瓶颈

所以“及时性”并不是理解成“每条 tick 到达后立刻同步刷到物理介质”，而是理解成“在可接受的短周期内持续把文件缓冲推下去，同时避免 fsync 把实时链路拖死”。

### 3.4 文件队列满了先告警，再尽量兜底

文件 writer 自己也有队列深度监控。当队列打满时，系统会记录告警和状态指标，帮助使用者及时发现“文件侧已经跟不上”。

这里的重点不是承诺“永不积压”，而是：

- 平时靠异步写和定时 flush 保持低延迟
- 高峰时靠队列和监控吸收瞬时抖动
- 一旦真的顶到容量边界，系统会显式暴露风险，而不是悄悄失真

## 4. 为什么数据库写入既及时又能扛住高吞吐

### 4.1 shard 不直接写库，而是生成 `persistTask`

当 shard 内完成 1m/mm bar 计算后，不会直接执行 SQL，而是把待落库数据包装成 `persistTask`，再交给 `dbBatchWriter`。

这一步非常关键。它把“业务计算完成”与“真正写入数据库”拆开了。

这样一来：

- shard 可以快速回去继续吃下一个 tick
- 数据库抖动不会立刻拖慢聚合线程
- 持久化链路可以独立做批量、分发、溢写和指标统计

相关代码：

- [internal/quotes/market_data_runtime.go](../../internal/quotes/market_data_runtime.go)
- [internal/quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)

### 4.2 DB writer 不是单线程单条写，而是多 worker 批量写

`dbBatchWriter` 启动后，会创建多个 worker。任务进入后会被分发到具体 worker，每个 worker 维护自己的队列和批量缓冲。

worker flush 的触发条件通常有两个：

- 缓冲条数达到 `flushBatch`
- 到达 `flushInterval`

也就是说，数据库写入采用的是“批量优先、超时也刷”的策略。

这比“每条都 insert/update 一次”更适合高频行情，原因很直接：

- SQL 往返次数更少
- 事务提交次数更少
- 表锁/索引更新压力更平滑
- 在同样数据库资源下吞吐更高

同时，定时 flush 又保证了即使当前流量不大，数据也不会因为凑不满 batch 而长时间滞留。

### 4.3 mm/L9 走延迟去重队列，避免重复和瞬时风暴

项目里对 mm/L9 做了单独处理。`dbBatchWriter` 内部有 `mmDeferredQueue`，会按周期把延迟队列里的任务整理后再分发。

这样做主要是为了：

- 避免同一时间窗内重复写很多等价任务
- 把高频更新压缩成更平滑的持久化流量
- 不让 mm/L9 的额外写入放大主 1m 链路压力

另外，L9 本身的计算也不在主 shard goroutine 里同步执行，而是交给 `l9AsyncCalculator`，算完后再复用统一的 `persistTask -> dbBatchWriter` 链路。

相关代码：

- [internal/quotes/l9_async.go](../../internal/quotes/l9_async.go)
- [internal/quotes/market_data_db_writer.go](../../internal/quotes/market_data_db_writer.go)

### 4.4 内存队列满了先 spill 到磁盘，不急着丢

这是“及时性”和“可靠性”之间最关键的一层兜底。

无论是：

- shard 输入队列
- DB worker 队列
- mm/L9 deferred 队列

当内存队列满了以后，程序都会优先尝试把任务写到 `JSONSpool`。

`JSONSpool` 的本质是一个很简单的磁盘 FIFO 队列。内存吃不下时，先把对象序列化到 spool 目录里；消费者空出来以后，再按顺序从磁盘捞回来继续处理。

这样做的意义是：

- 吸收瞬时峰值，减少直接丢失
- 把“数据库暂时慢一点”与“主链路彻底打挂”隔离开
- 在进程重启后仍有机会从 spool 恢复未处理任务

对应实现：

- [internal/queuewatch/spool.go](../../internal/queuewatch/spool.go)

这层设计说明系统对“高峰时的及时性”定义得很务实：

- 正常情况下依靠内存队列和异步批量，低延迟落库
- 异常情况下允许短暂排队和落盘缓冲，但尽量不直接丢

### 4.5 真正无处可 spill 时，宁可阻塞也不静默丢失

在某些极端情况下，如果内存队列已满、磁盘 spool 也不可用，代码会退化到阻塞 enqueue，并伴随明确告警日志。

这体现的是明确的取舍：

- 不把“吞吐好看”建立在“悄悄丢数据”之上
- 当系统资源真的不足时，让背压显式传导出来

这不是说系统不会卡，而是说系统在顶到硬边界时选择“诚实失败”，便于排障和扩容。

## 5. 及时性不是口号，而是有指标持续观测

如果只有异步队列，没有观测，系统很容易变成“看起来没报错，但其实已经严重滞后”。

这个工程在 `RuntimeStatusCenter` 里持续记录运行态指标，用来判断链路是不是还健康。

重点包括：

- `router_queue_ms`：tick 从回调进入 runtime 路由前后的排队延迟
- `shard_queue_ms`：tick 在 shard 队列里的等待时间
- `db queue depth` / `inflight depth`：数据库持久化积压
- `MarkDBFlush`：单次批量 flush 行数和耗时
- `MarkFileFlush`：文件 flush 耗时和队列深度
- `ShardBacklog`：各 shard 当前积压
- `QueueSpillingCount`：有多少队列已经开始 spill 到磁盘

相关实现：

- [internal/quotes/runtime_status.go](../../internal/quotes/runtime_status.go)

这套指标的意义在于，它让“是否及时”变成了一个可观察问题：

- 如果 `router_queue_ms` 上升，说明入口就开始顶住了
- 如果 `shard_queue_ms` 上升，说明 shard 处理跟不上
- 如果 DB queue 深度持续增加，说明写库速度落后于生产速度
- 如果频繁进入 spool，说明系统已经开始依赖磁盘缓冲过峰

所以这个系统不是简单地“相信异步一定够快”，而是通过状态中心持续检验每一级是否仍然及时。

## 6. 可以把整套机制理解成四层缓冲

从接收 tick 到最终落盘，可以把程序理解成四层缓冲和限流：

1. CTP 回调到 runtime 的快速交接
2. runtime 到 shard 的分片队列
3. shard 到 file/db writer 的异步任务队列
4. 内存队列满载后的磁盘 spool

每一层都在解决不同问题：

- 第 1 层解决“不要让外部回调线程做重活”
- 第 2 层解决“既保序又并行”
- 第 3 层解决“I/O 与计算解耦”
- 第 4 层解决“峰值时不要直接崩或直接丢”

而“及时性”则主要由下面几个手段共同保证：

- shard 线程尽量只做必须的顺序计算
- 文件写采用短周期 flush
- 数据库写采用 batch + interval 双触发
- L9 和 mm 做异步/延迟处理，避免挤占主链路
- 状态中心持续暴露延迟和积压

## 7. 一句话总结这套设计

这个程序并不是靠“某个函数特别快”来解决高速 tick 与及时落盘的矛盾，而是靠分层解耦来解决：

- 用轻量回调和 shard 并行把 tick 快速接进来
- 用异步文件 writer 和批量 DB writer 把慢 I/O 从主链路剥离出去
- 用 flush 周期、批量阈值和磁盘 spool 在吞吐、时效、可靠性之间做工程化折中
- 用运行态指标持续确认这套折中在当前负载下仍然成立

因此，它本质上不是“既要又要”的魔法，而是一套明确承认 I/O 很慢、并围绕这个事实做出的并发与缓冲设计。
