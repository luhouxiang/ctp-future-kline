### 标题

实时模拟（live_paper）实施方案：实时行情 + 交易所元数据同步 + 回放撮合复用

### Summary

基于你确认的策略，`live_paper` 采用“**交易所请求实时行情/合约/手续费/保证金**，但**不请求实盘资金/持仓/委托/成交**，且**不向交易所发下单撤单**”。
交易行为完全复用 `replay_paper` 的撮合内核（排队、按 bid/ask+限价撮合），只是行情源从回放 tick 改为实时 tick。
手续费/保证金补齐连接按“缺口非空才启动，缺口清空即停；仅交易日切换或手动刷新再重启”，节流延续“只升不降”。

### Key Changes

1. **模式职责重定义（live_paper）**

- `live_paper` 交易后端继续用本地 `paper_live` 账户库，不走实盘下单链路。
- 禁止在 `live_paper` 中调用任何实盘交易指令发送（`ReqOrderInsert/ReqOrderAction`）。
- `live_paper` 不再执行实盘查询：`QryTradingAccount/QryInvestorPosition/QryOrder/QryTrade`。

2. **实时模拟撮合复用 replay 逻辑**

- 将 `trade.Service` 的 paper 撮合拆成统一“paper matching core”，由 `replay_paper` 和 `live_paper` 共用。
- `live_paper` 从“即时成交”改为“挂单+行情驱动撮合”：
  - 下单进入 pending 队列；
  - 按实时 bid/ask 与限价判断成交；
  - 成交后更新本地订单/成交/持仓/资金快照。
- 成交时间统一使用行情数据时间（交易所数据时间），不使用本机当前时间。

3. **交易所元数据同步（仅合约+手续费+保证金）**

- 新增 `live_paper` 专用“元数据同步编排器”（只读交易查询，不承载下单）：
  - 启动时通过交易接口登录拿 `TradingDay`；
  - 以当前 `TradingDay` 做缺口计算；
  - 缺口非空时启动 `fee_lane`/`margin_lane`；
  - 缺口清空后关闭两 lane（按你的选择）。
- 缺口定义（当前交易日）：
  - 手续费缺口：`ctp_instruments(TradingDay=TD)` - `ctp_commission_rates(sync_trading_day=TD)`
  - 保证金缺口：`ctp_instruments(TradingDay=TD)` - `ctp_margin_rates(sync_trading_day=TD)`
- 节流规则沿用：初始 1.0s，遇流控错误 `-2/-3` 或同类错误 `+0.2s`，上限 3.0s，成功后不下降。

4. **lane 生命周期（按你选项）**

- `fee_lane/margin_lane` 仅在以下时机重建并可能重启：
  - 交易日切换；
  - 手动触发 `/api/trade/query/refresh`。
- 若当次计算缺口为空，不启动 lane；已启动 lane 在缺口清空后退出。
- `live_paper` 下不启用 `auto_pos`（因不请求持仓/委托/成交/资金）。

5. **现有接口与状态可观测性**

- 保持现有 `/api/commission-rates`、`/api/margin-rates` 查询接口不变，数据按 `TradingDay` 可见。
- `trade status` 增加/扩展元数据同步状态字段（建议）：`meta_sync.trading_day`, `fee_gap`, `margin_gap`, `fee_lane_running`, `margin_lane_running`, `throttle_ms`。
- 与交易所通信失败日志强制带上下文：`lane/query_type/req_id/instrument/exchange/error_id/error_msg/throttle_ms`。

### Public Interfaces / Types

- `trade.Service` 增加统一 paper 行情撮合入口（例如 `ConsumePaperMarketTick(...)`），供 `replay_paper` 与 `live_paper` 共用。
- `web` 层在实时行情分发路径上，将合适的实时 tick/quote 事件喂给 `tradePaperLive`（仅 `live_paper` 模式启用）。
- `/api/trade/query/refresh` 在 `live_paper` 下语义扩展为“重建 TD 缺口并按需重启 fee/margin lane”。

### Test Plan

1. **模式行为**

- `live_paper` 下单后不触发任何 `ReqOrderInsert/ReqOrderAction`。
- `live_paper` 不触发 `QryTradingAccount/QryInvestorPosition/QryOrder/QryTrade`。
- `live_paper` 仍可持续刷新实时行情、合约、手续费、保证金。

2. **撮合一致性**

- 同一组行情驱动下，`live_paper` 与 `replay_paper` 的挂单撮合结果口径一致（状态迁移、成交价、可平数量、资金变化）。
- 成交时间字段来自行情数据时间，且展示到秒，不带时区尾缀。

3. **缺口补齐**

- 当日数据齐全时不启动 fee/margin lane。
- 缺口存在时按每连接节流查询并逐条落库，清空后 lane 自动停止。
- 手动 `refresh` 与交易日切换可重新触发缺口补齐。

4. **节流与日志**

- 注入 `-2/-3` 后间隔按 1.0→1.2→…→3.0 增长，成功后保持当前值不回落。
- 错误日志包含完整定位字段，便于排障。

### Assumptions

- `ctp_instruments` 已含 `trading_day`，并在同步时按交易所返回 `TradingDay` 写入。
- 元数据同步只针对 `live_paper` 与 `live_real` 的实时链路；`replay_paper` 保持现有回放逻辑。
- 首版不引入滑点/部分成交模型，严格复用当前回放撮合规则。




已按你给的方案完成实现，核心改动已落地：

* **live_paper** 现在不走实盘下单/撤单链路，交易撮合改为“挂单 + 行情驱动”，并复用回放撮合内核（不再即时全成）。
* 新增统一 paper 行情入口 **ConsumePaperMarketTick(...)**，**replay_paper** 和 **live_paper** 共用；成交时间改为使用行情数据时间（**TradingDay + UpdateTime**）。
* **web** 已把实时 **quote_snapshot_update** 喂给 **paper_live** 撮合（仅 **live_paper** 模式）。
* **live_paper** 新增元数据同步编排：只做合约相关元数据补齐（手续费/保证金），不做资金/持仓/委托/成交查询；缺口空不启动 lane，手动刷新或交易日切换才重建。
* **trade status** 增加了元数据可观测字段（**meta_sync_trading_day/fee_gap/margin_gap/fee_lane_running/margin_lane_running/*_throttle_ms**）。

主要代码位置：

* **service.go**
* **paper_meta_sync.go**
* **types.go**
* **server.go**
