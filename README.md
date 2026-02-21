# ctp-future-kline

一个基于 Go 的 CTP 期货分钟线采集与历史导入工具，提供 Web 控制台。

## 功能概览

- CTP 实时行情采集，聚合为 1 分钟 K 线并写入 MySQL 8.4
- 主力加权指数（L9）异步计算
- 通达信历史数据导入（含冲突决策）
- 交易日历导入、刷新与启动自动维护
- K 线检索与图表展示（前端）
- 运行状态实时推送（HTTP + WebSocket）

## 目录结构

- `main.go`: 程序入口
- `internal/trader`: CTP 连接、行情聚合、L9 计算、状态管理
- `internal/web`: HTTP API、WebSocket、静态资源服务
- `internal/importer`: 通达信历史数据导入
- `internal/klinequery`: K 线查询与指标计算
- `internal/calendar`: 交易日历维护
- `internal/searchindex`: 查询索引管理
- `web`: Vue3 + Vite 前端工程

## 运行环境

- Go `1.25.7+`
- Node.js `18+`（前端构建）
- Windows 运行 CTP 需确保以下 DLL 可访问：
  - `wrap.dll`
  - `thosttraderapi_se.dll`
  - `thostmduserapi_se.dll`

## 快速开始

### 1. 安装依赖并构建前端

```bash
go mod tidy
cd web
npm install
npm run build
cd ..
```

### 2. 启动服务

```bash
go run . -config ../ctp-future-resources/config/config.json
```

可选参数：

```bash
go run . -config ../ctp-future-resources/config/config.json -no-open
```

- `-no-open`: 不自动打开浏览器

默认访问地址：`http://127.0.0.1:8080`

## 配置说明

配置结构包含 4 部分：`ctp`、`db`、`web`、`calendar`。

### 示例 `config.json`

```json
{
  "ctp": {
    "flow_path": "./flow",
    "trader_front_addr": "tcp://180.166.37.142:43205",
    "md_front_addr": "tcp://180.166.37.142:43213",
    "broker_id": "8030",
    "app_id": "client_qhtrade_1.0.0",
    "auth_code": "your_auth_code",
    "user_product_info": "",
    "user_id": "your_user_id",
    "password": "your_password",
    "subscribe_instruments": ["rb"],
    "enable_l9_async": true,
    "connect_wait_seconds": 5,
    "authenticate_wait_seconds": 5,
    "login_wait_seconds": 5,
    "md_connect_wait_seconds": 5,
    "md_login_wait_seconds": 5,
    "md_receive_seconds": 30,

    "md_reconnect_enabled": true,
    "md_reconnect_initial_backoff_ms": 1000,
    "md_reconnect_max_backoff_ms": 30000,
    "md_reconnect_jitter_ratio": 0.2,
    "md_relogin_wait_seconds": 3,
    "tick_dedup_window_seconds": 2,
    "drift_threshold_seconds": 5,
    "drift_resume_consecutive_ticks": 3,
    "no_tick_warn_seconds": 60
  },
  "web": {
    "listen_addr": "127.0.0.1:8080",
    "auto_open_browser": true,
    "market_open_stale_seconds": 60,
    "draw_debug_default": 0,
    "browser_log_default": 0
  },
  "db": {
    "driver": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_mysql_password",
    "database": "future_kline",
    "params": "parseTime=true&loc=Local&multiStatements=false"
  },
  "calendar": {
    "auto_update_on_start": true,
    "min_future_open_days": 60,
    "source_url": "",
    "source_csv_path": "",
    "check_interval_hours": 24,
    "browser_fallback": true,
    "browser_path": "",
    "browser_headless": true
  }
}
```

### 关键默认值与校验（ctp）

- `md_reconnect_enabled` 默认 `true`
- `md_reconnect_initial_backoff_ms` 默认 `1000`，必须 `> 0`
- `md_reconnect_max_backoff_ms` 默认 `30000`，必须 `> 0` 且 `>= initial`
- `md_reconnect_jitter_ratio` 默认 `0.2`，范围 `[0,1]`
- `md_relogin_wait_seconds` 默认 `3`，必须 `> 0`
- `tick_dedup_window_seconds` 默认 `2`，必须 `> 0`
- `drift_threshold_seconds` 默认 `5`，必须 `> 0`
- `drift_resume_consecutive_ticks` 默认 `3`，必须 `>= 1`
- `no_tick_warn_seconds` 默认回退到 `web.market_open_stale_seconds`，且最小 `30`

## 行情与授时可靠性策略

当前实现（按代码行为）：

- 断线：MD 前置断开后进入自动重连流程
- 退避：指数退避 + 抖动（jitter）
- 补订阅：重连登录成功后对订阅目标全量补订阅
- 去重：
  - 实时层：按 tick 指纹做短窗口去重
  - 存储层：分钟线 `upsert` 幂等兜底
- 断网疑似：连接看似在线但长时间无 tick，仅标记 `network_suspect` 并告警，不强制重连
- 漂移：检测本机时间与调整后行情时间漂移
  - 超阈值：暂停写入
  - 恢复：连续 N 条正常 tick 后自动恢复

## 数据存储

- 数据库：MySQL（`db.database`，默认 `future_kline`）
- 合约分钟线表：`future_kline_instrument_1m_<variety>`
- L9 表：`future_kline_l9_1m_<variety>`
- 图表布局表：`chart_layouts`
- 绘图对象表：`chart_drawings`

## 图表布局与绘图 API

- `GET /api/chart/layout?symbol=&type=&variety=&timeframe=`
  - 获取图表布局与绘图对象快照
- `PUT /api/chart/layout`
  - 全量保存布局（含 drawings）
- `POST /api/chart/drawings`
  - 新增或更新单个绘图对象
- `DELETE /api/chart/drawings/:id?symbol=&type=&variety=&timeframe=`
  - 删除单个绘图对象

## SQLite 迁移到 MySQL

```bash
go run ./cmd/migrate_sqlite_to_mysql \
  --sqlite-path flow/future_kline.db \
  --mysql-host localhost \
  --mysql-port 3306 \
  --mysql-user root \
  --mysql-password your_mysql_password \
  --mysql-db future_kline \
  --truncate
```

## API

### HTTP

- `GET /api/status`
  - 返回运行状态快照
- `POST /api/server/start`
  - 启动 CTP 运行流程
- `POST /api/import/session`
  - 创建通达信导入会话（`multipart/form-data`）
- `POST /api/import/session/{id}/decision`
  - 提交导入冲突决策
- `GET /api/kline/search`
  - K 线检索
- `GET /api/kline/bars`
  - 拉取图表数据（bars + macd）
- `GET /api/instruments`
  - 合约列表分页
- `GET /api/calendar/status`
  - 交易日历状态
- `POST /api/calendar/import`
  - 上传 CSV 导入交易日历
- `POST /api/calendar/import/tdx-daily`
  - 上传通达信日线文件导入交易日
- `POST /api/calendar/refresh`
  - 按配置刷新交易日历

### WebSocket

- `GET /ws`
- 事件类型：
  - `status_update`
  - `import_progress`
  - `import_conflict`
  - `import_done`
  - `import_error`

## 运行状态字段（核心）

`/api/status` 与 `status_update` 中 `status` 包含（节选）：

- 基础连接：`state`、`trader_front`、`trader_login`、`md_front`、`md_login`、`md_subscribed`
- 订阅与时间：`subscribe_count`、`server_time`、`trading_day`、`last_tick_time`
- 市场活跃：`is_market_open`
- 错误：`last_error`
- 可靠性新增：
  - `md_front_disconnected`
  - `md_disconnect_reason`
  - `md_reconnect_attempt`
  - `md_next_retry_at`
  - `network_suspect`
  - `tick_dedup_dropped`
  - `drift_seconds`
  - `drift_paused`
  - `drift_pause_count`

## 前端开发

```bash
cd web
npm run dev
```

代理配置见 `web/vite.config.js`：

- `/api` -> `http://127.0.0.1:8080`
- `/ws` -> `ws://127.0.0.1:8080`

## 测试

建议命令：

```bash
go test ./tests/internal/config ./tests/internal/importer ./tests/internal/searchindex ./tests/internal/klinequery
```

说明：`internal/trader` 与 `tests/internal/trader` 在部分环境下运行测试可能依赖本机 CTP DLL。

## 依赖

- CTP SDK: `github.com/kkqy/ctp-go`
- MySQL 驱动: `github.com/go-sql-driver/mysql`
- SQLite 迁移工具读取: `modernc.org/sqlite`
- WebSocket: `github.com/gorilla/websocket`
- 编码处理: `golang.org/x/text`
- 图表库: `lightweight-charts`

