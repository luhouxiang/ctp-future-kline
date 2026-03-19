<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { FRONTEND_VERSION } from './version'

const APP_VERSION = FRONTEND_VERSION

const status = reactive({
  state: 'idle',
  trader_front: false,
  trader_login: false,
  md_front: false,
  md_login: false,
  md_subscribed: false,
  md_front_disconnected: false,
  md_disconnect_reason: 0,
  md_reconnect_attempt: 0,
  md_next_retry_at: '',
  network_suspect: false,
  tick_dedup_dropped: 0,
  drift_seconds: 0,
  drift_paused: false,
  drift_pause_count: 0,
  last_drift_instrument: '',
  last_drift_at: '',
  upstream_lag_ms: 0,
  callback_to_process_ms: 0,
  router_queue_ms: 0,
  shard_queue_ms: 0,
  persist_queue_ms: 0,
  db_flush_ms: 0,
  db_flush_rows: 0,
  db_queue_depth: 0,
  file_flush_ms: 0,
  file_queue_depth: 0,
  end_to_end_ms: 0,
  server_time: '',
  trading_day: '',
  is_market_open: false,
  subscribe_count: 0,
  last_error: '',
})

const importState = reactive({
  session_id: '',
  total_files: 0,
  processed_files: 0,
  total_lines: 0,
  inserted_rows: 0,
  overwritten_rows: 0,
  skipped_rows: 0,
  skipped_files: 0,
  error_count: 0,
  done: false,
  canceled: false,
})

const replayState = reactive({
  task_id: '',
  status: '',
  mode: '',
  speed: 1,
  topics: [],
  sources: [],
  start_time: '',
  end_time: '',
  from_cursor: null,
  last_cursor: null,
  tick_dir: '',
  tick_files: 0,
  instruments: 0,
  total_ticks: 0,
  processed_ticks: 0,
  current_instrument_id: '',
  current_sim_time: '',
  first_sim_time: '',
  last_sim_time: '',
  dispatched: 0,
  skipped: 0,
  errors: 0,
  last_error: '',
  created_at: '',
  started_at: '',
  finished_at: '',
})

const replayForm = reactive({
  topicsText: '',
  sourcesText: 'replay.tickcsv',
  start: '',
  end: '',
  mode: 'realtime',
  speed: 1,
  tickDir: 'flow/ticks',
  fromFile: '',
  fromOffset: 0,
  fullReplay: true,
})

const replayLoading = reactive({
  starting: false,
  pausing: false,
  resuming: false,
  stopping: false,
  fetchingStatus: false,
})

const strategyStatus = reactive({
  enabled: false,
  process_running: false,
  connected: false,
  grpc_addr: '',
  python_entry: '',
  last_error: '',
  last_health_at: '',
  definitions: 0,
  instances: 0,
  running_count: 0,
  signal_count: 0,
  audit_count: 0,
  backtest_run_count: 0,
})

const strategyState = reactive({
  definitions: [],
  instances: [],
  signals: [],
  backtests: [],
  orderAudits: [],
  ordersStatus: { mode: 'simulated', positions: {}, updated_at: '' },
})

const strategyForm = reactive({
  instance_id: '',
  strategy_id: '',
  display_name: '',
  mode: 'realtime',
  account_id: 'paper',
  symbols_text: '',
  timeframe: '1m',
  params_text: '{}',
})

const tradeStatus = reactive({
  enabled: false,
  account_id: '',
  trader_front: false,
  trader_login: false,
  settlement_confirmed: false,
  trading_day: '',
  front_id: 0,
  session_id: 0,
  last_error: '',
  last_query_at: '',
})

const tradeState = reactive({
  account: null,
  positions: [],
  orders: [],
  trades: [],
  audits: [],
})

const tradeForm = reactive({
  account_id: 'default',
  symbol: '',
  exchange_id: '',
  direction: 'buy',
  offset_flag: 'open',
  limit_price: '',
  volume: 1,
  client_tag: '',
})

const backtestForm = reactive({
  instance_id: '',
  symbol: '',
  timeframe: '1m',
  start_time: '',
  end_time: '',
})

const searchForm = reactive({
  keyword: '',
  start: '',
  end: '',
  page: 1,
  pageSize: 100,
})

const searchState = reactive({
  loading: false,
  total: 0,
  items: [],
})

const files = ref([])
const tradingDayFile = ref(null)
const tradingDayImportResult = ref(null)
const logs = ref([])
const wsConnected = ref(false)
const loadingImport = ref(false)
const loadingTradingDayImport = ref(false)
const conflict = ref(null)
const replaySupported = ref(false)
let ws
let statusPollTimer = null
let tradePollTimer = null
let lastLargeDriftLogKey = ''

const marketBadgeClass = computed(() => (status.is_market_open ? 'badge open' : 'badge closed'))
const marketBadgeText = computed(() => (status.is_market_open ? '开市中' : '非开市'))
const formattedTradingDay = computed(() => {
  const raw = String(status.trading_day || '').trim()
  if (!raw) return ''
  if (/^\d{8}$/.test(raw)) {
    return `${raw.slice(0, 4)}年${Number(raw.slice(4, 6))}月${Number(raw.slice(6, 8))}日`
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const [y, m, d] = raw.split('-')
    return `${y}年${Number(m)}月${Number(d)}日`
  }
  return raw
})
const totalPages = computed(() => {
  if (searchState.total <= 0) return 1
  return Math.ceil(searchState.total / searchForm.pageSize)
})
const replayStatus = computed(() => String(replayState.status || '').trim())
const replayEnabled = computed(() => replaySupported.value)
const strategyEnabled = computed(() => !!strategyStatus.enabled)
const canStartReplay = computed(() => replayEnabled.value && ['idle', 'done', 'error', 'stopped', ''].includes(replayStatus.value) && !replayLoading.starting)
const canPauseReplay = computed(() => replayEnabled.value && replayStatus.value === 'running' && !replayLoading.pausing)
const canResumeReplay = computed(() => replayEnabled.value && replayStatus.value === 'paused' && !replayLoading.resuming)
const canStopReplay = computed(() => replayEnabled.value && ['running', 'paused'].includes(replayStatus.value) && !replayLoading.stopping)
const replayProgressText = computed(() => {
  const total = Number(replayState.total_ticks || 0)
  const processed = Number(replayState.processed_ticks || 0)
  if (total <= 0) return '--'
  return `${processed}/${total} (${((processed / total) * 100).toFixed(1)}%)`
})
const replayRemainingText = computed(() => {
  const current = Date.parse(String(replayState.current_sim_time || ''))
  const last = Date.parse(String(replayState.last_sim_time || ''))
  const speed = Number(replayState.speed || 1)
  if (!Number.isFinite(current) || !Number.isFinite(last) || !Number.isFinite(speed) || speed <= 0 || last < current) {
    return '--'
  }
  return formatDurationMs(last - current)
})
const replayEtaText = computed(() => {
  const current = Date.parse(String(replayState.current_sim_time || ''))
  const last = Date.parse(String(replayState.last_sim_time || ''))
  const speed = Number(replayState.speed || 1)
  if (!Number.isFinite(current) || !Number.isFinite(last) || !Number.isFinite(speed) || speed <= 0 || last < current) {
    return '--'
  }
  const eta = new Date(Date.now() + (last - current) / speed)
  return Number.isNaN(eta.getTime()) ? '--' : eta.toLocaleString()
})

function addLog(message) {
  logs.value.unshift(`${new Date().toLocaleTimeString()} ${message}`)
  if (logs.value.length > 80) {
    logs.value = logs.value.slice(0, 80)
  }
}

function applyStatusSnapshot(snapshot) {
  Object.assign(status, snapshot || {})
  const driftSeconds = Number(status.drift_seconds || 0)
  if (!Number.isFinite(driftSeconds) || driftSeconds <= 86400) {
    return
  }
  const instrumentID = String(status.last_drift_instrument || '').trim() || 'unknown'
  const driftAt = String(status.last_drift_at || '').trim() || 'unknown'
  const logKey = `${instrumentID}|${driftAt}`
  if (logKey === lastLargeDriftLogKey) {
    return
  }
  lastLargeDriftLogKey = logKey
  addLog(`漂移超过86400秒: 合约=${instrumentID} drift_seconds=${driftSeconds.toFixed(3)} drift_at=${driftAt}`)
}

function formatBytes(bytes) {
  const n = Number(bytes || 0)
  if (!Number.isFinite(n) || n <= 0) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(2)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MB`
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function formatDurationMs(ms) {
  const totalSeconds = Math.max(0, Math.floor(Number(ms || 0) / 1000))
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function toDateTimeLocal(date) {
  const pad = (n) => String(n).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function defaultTimeRange() {
  searchForm.end = ''
  searchForm.start = ''
}

function normalizeDateTimeInput(value) {
  return value ? value.replace('T', ' ') : ''
}

function parseCSVInput(text) {
  return String(text || '')
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
}

function localDateTimeToISO(value) {
  if (!value) return ''
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return ''
  }
  return date.toISOString()
}

function applyReplayState(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return
  Object.assign(replayState, snapshot)
}

function applyStrategyStatus(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return
  Object.assign(strategyStatus, snapshot)
}

function applyTradeStatus(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return
  Object.assign(tradeStatus, snapshot)
  if (snapshot.account_id) {
    tradeForm.account_id = snapshot.account_id
  }
}

async function fetchStatus() {
  const resp = await fetch('/api/status')
  if (!resp.ok) {
    throw new Error(`status http ${resp.status}`)
  }
  const data = await resp.json()
  applyStatusSnapshot(data.status || {})
  if (data.replay) {
    replaySupported.value = true
    applyReplayState(data.replay)
  } else {
    replaySupported.value = false
  }
  if (data.strategy) {
    applyStrategyStatus(data.strategy)
  }
  if (data.trade) {
    applyTradeStatus(data.trade)
  }
}

async function fetchReplayStatus() {
  replayLoading.fetchingStatus = true
  try {
    const resp = await fetch('/api/replay/status')
    if (!resp.ok) {
      replaySupported.value = false
      return
    }
    const data = await resp.json()
    replaySupported.value = true
    applyReplayState(data.task || {})
  } finally {
    replayLoading.fetchingStatus = false
  }
}

async function fetchStrategyBundle() {
  const endpoints = [
    ['/api/strategy/status', (data) => applyStrategyStatus(data.status || {})],
    ['/api/strategy/definitions', (data) => { strategyState.definitions = data.items || [] }],
    ['/api/strategy/instances', (data) => { strategyState.instances = data.items || [] }],
    ['/api/strategy/signals?limit=20', (data) => { strategyState.signals = data.items || [] }],
    ['/api/strategy/backtests?limit=20', (data) => { strategyState.backtests = data.items || [] }],
    ['/api/orders/status', (data) => { strategyState.ordersStatus = data || { mode: 'simulated', positions: {} } }],
    ['/api/orders/audit?limit=20', (data) => { strategyState.orderAudits = data.items || [] }],
  ]
  for (const [url, apply] of endpoints) {
    try {
      const resp = await fetch(url)
      if (!resp.ok) continue
      apply(await resp.json())
    } catch {
      // ignore strategy endpoints when service is disabled
    }
  }
}

async function fetchTradeBundle() {
  const endpoints = [
    ['/api/trade/status', (data) => applyTradeStatus(data.status || {})],
    ['/api/trade/account', (data) => { tradeState.account = data || null }],
    ['/api/trade/positions', (data) => { tradeState.positions = data.items || [] }],
    ['/api/trade/orders?limit=50', (data) => { tradeState.orders = data.items || [] }],
    ['/api/trade/trades?limit=50', (data) => { tradeState.trades = data.items || [] }],
  ]
  for (const [url, apply] of endpoints) {
    try {
      const resp = await fetch(url)
      if (!resp.ok) continue
      apply(await resp.json())
    } catch {
      // ignore when trade disabled
    }
  }
}

async function refreshTradeData() {
  const resp = await fetch('/api/trade/query/refresh', { method: 'POST' })
  if (!resp.ok) {
    addLog(`刷新交易查询失败: ${await resp.text()}`)
    return
  }
  addLog('已刷新交易查询')
  await fetchTradeBundle()
}

async function submitTradeOrder() {
  const payload = {
    account_id: tradeForm.account_id,
    symbol: tradeForm.symbol,
    exchange_id: tradeForm.exchange_id,
    direction: tradeForm.direction,
    offset_flag: tradeForm.offset_flag,
    limit_price: Number(tradeForm.limit_price),
    volume: Number(tradeForm.volume),
    client_tag: tradeForm.client_tag,
    reason: 'manual',
  }
  const resp = await fetch('/api/trade/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) {
    addLog(`手工下单失败: ${await resp.text()}`)
    return
  }
  const data = await resp.json()
  addLog(`手工下单已提交: ${data.command_id || '--'} ${data.symbol || payload.symbol}`)
  await fetchTradeBundle()
}

async function cancelTradeOrder(item) {
  const payload = {
    account_id: tradeForm.account_id || tradeStatus.account_id,
    order_ref: item.order_ref,
    exchange_id: item.exchange_id,
    order_sys_id: item.order_sys_id,
    front_id: item.front_id,
    session_id: item.session_id,
    reason: 'manual_cancel',
  }
  const resp = await fetch(`/api/trade/orders/${item.command_id}/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) {
    addLog(`撤单失败: ${await resp.text()}`)
    return
  }
  addLog(`撤单请求已提交: ${item.command_id}`)
  await fetchTradeBundle()
}

async function saveStrategyInstance() {
  let params = {}
  try {
    params = JSON.parse(strategyForm.params_text || '{}')
  } catch {
    addLog('策略实例保存失败: 参数 JSON 无效')
    return
  }
  const payload = {
    instance_id: strategyForm.instance_id || undefined,
    strategy_id: strategyForm.strategy_id,
    display_name: strategyForm.display_name,
    mode: strategyForm.mode,
    account_id: strategyForm.account_id,
    symbols: parseCSVInput(strategyForm.symbols_text),
    timeframe: strategyForm.timeframe,
    params,
  }
  const resp = await fetch('/api/strategy/instances', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) {
    addLog(`策略实例保存失败: ${await resp.text()}`)
    return
  }
  addLog('策略实例已保存')
  strategyForm.instance_id = ''
  await fetchStrategyBundle()
}

async function startStrategyInstance(instanceId) {
  const resp = await fetch(`/api/strategy/instances/${instanceId}/start`, { method: 'POST' })
  if (!resp.ok) {
    addLog(`策略实例启动失败: ${await resp.text()}`)
    return
  }
  addLog(`策略实例已启动: ${instanceId}`)
  await fetchStrategyBundle()
}

async function stopStrategyInstance(instanceId) {
  const resp = await fetch(`/api/strategy/instances/${instanceId}/stop`, { method: 'POST' })
  if (!resp.ok) {
    addLog(`策略实例停止失败: ${await resp.text()}`)
    return
  }
  addLog(`策略实例已停止: ${instanceId}`)
  await fetchStrategyBundle()
}

async function runBacktest() {
  const selected = strategyState.instances.find((item) => item.instance_id === backtestForm.instance_id)
  if (!selected) {
    addLog('回测启动失败: 请选择策略实例')
    return
  }
  const payload = {
    instance: selected,
    symbol: backtestForm.symbol,
    timeframe: backtestForm.timeframe,
    start_time: backtestForm.start_time ? new Date(backtestForm.start_time).toISOString() : '',
    end_time: backtestForm.end_time ? new Date(backtestForm.end_time).toISOString() : '',
    parameters: selected.params || {},
  }
  const resp = await fetch('/api/strategy/backtests', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) {
    addLog(`回测启动失败: ${await resp.text()}`)
    return
  }
  addLog('回测任务已提交')
  await fetchStrategyBundle()
}

async function startReplay() {
  if (!canStartReplay.value) return
  if (Number(replayForm.speed) <= 0) {
    addLog('回放启动失败: speed 必须大于 0')
    return
  }
  const startISO = localDateTimeToISO(replayForm.start)
  const endISO = localDateTimeToISO(replayForm.end)
  if (replayForm.start && !startISO) {
    addLog('回放启动失败: 开始时间格式无效')
    return
  }
  if (replayForm.end && !endISO) {
    addLog('回放启动失败: 结束时间格式无效')
    return
  }
  if (startISO && endISO && new Date(endISO).getTime() < new Date(startISO).getTime()) {
    addLog('回放启动失败: 结束时间不能早于开始时间')
    return
  }

  const tickDir = String(replayForm.tickDir || '').trim() || 'flow/ticks'
  const sources = parseCSVInput(replayForm.sourcesText)
  if (tickDir && !sources.includes('replay.tickcsv')) {
    sources.push('replay.tickcsv')
  }
  const payload = {
    topics: parseCSVInput(replayForm.topicsText),
    sources,
    mode: replayForm.mode || 'realtime',
    speed: Number(replayForm.speed) || 1,
    tick_dir: tickDir,
    full_replay: replayForm.fullReplay !== false,
  }
  if (startISO) payload.start_time = startISO
  if (endISO) payload.end_time = endISO
  if (replayForm.fromFile.trim()) {
    payload.from_cursor = {
      file: replayForm.fromFile.trim(),
      offset: Number(replayForm.fromOffset) || 0,
    }
  }

  replayLoading.starting = true
  try {
    const resp = await fetch('/api/replay/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    if (!resp.ok) {
      const message = (await resp.text()) || `http ${resp.status}`
      addLog(`回放启动失败: ${message}`)
      return
    }
    const data = await resp.json()
    replaySupported.value = true
    applyReplayState(data.task || {})
    addLog(`回放已启动: task=${data.task?.task_id || '--'} mode=${data.task?.mode || payload.mode}`)
  } catch (error) {
    addLog(`回放启动失败: ${error instanceof Error ? error.message : String(error)}`)
  } finally {
    replayLoading.starting = false
  }
}

async function pauseReplay() {
  if (!canPauseReplay.value) return
  replayLoading.pausing = true
  try {
    const resp = await fetch('/api/replay/pause', { method: 'POST' })
    if (!resp.ok) {
      const message = (await resp.text()) || `http ${resp.status}`
      addLog(`回放暂停失败: ${message}`)
      return
    }
    const data = await resp.json()
    applyReplayState(data.task || {})
    addLog(`回放已暂停: task=${data.task?.task_id || '--'}`)
  } catch (error) {
    addLog(`回放暂停失败: ${error instanceof Error ? error.message : String(error)}`)
  } finally {
    replayLoading.pausing = false
  }
}

async function resumeReplay() {
  if (!canResumeReplay.value) return
  replayLoading.resuming = true
  try {
    const resp = await fetch('/api/replay/resume', { method: 'POST' })
    if (!resp.ok) {
      const message = (await resp.text()) || `http ${resp.status}`
      addLog(`回放继续失败: ${message}`)
      return
    }
    const data = await resp.json()
    applyReplayState(data.task || {})
    addLog(`回放已继续: task=${data.task?.task_id || '--'}`)
  } catch (error) {
    addLog(`回放继续失败: ${error instanceof Error ? error.message : String(error)}`)
  } finally {
    replayLoading.resuming = false
  }
}

async function stopReplay() {
  if (!canStopReplay.value) return
  replayLoading.stopping = true
  try {
    const resp = await fetch('/api/replay/stop', { method: 'POST' })
    if (!resp.ok) {
      const message = (await resp.text()) || `http ${resp.status}`
      addLog(`回放停止失败: ${message}`)
      return
    }
    const data = await resp.json()
    applyReplayState(data.task || {})
    addLog(`回放已停止: task=${data.task?.task_id || '--'}`)
  } catch (error) {
    addLog(`回放停止失败: ${error instanceof Error ? error.message : String(error)}`)
  } finally {
    replayLoading.stopping = false
  }
}

async function startServer() {
  const resp = await fetch('/api/server/start', { method: 'POST' })
  if (!resp.ok) {
    addLog('启动服务器失败')
    return
  }
  addLog('已触发启动服务器')
}

function onSelectFiles(event) {
  const selected = Array.from(event.target.files || [])
  files.value = selected
  addLog(`已选择目录文件 ${selected.length} 个`)
}

function onSelectTradingDayFile(event) {
  const selected = Array.from(event.target.files || [])
  tradingDayFile.value = selected[0] || null
  tradingDayImportResult.value = null
  addLog(`已选择交易日历文件 ${tradingDayFile.value ? tradingDayFile.value.name : '无'}`)
}

async function startImport() {
  if (files.value.length === 0) {
    addLog('未选择目录文件')
    return
  }

  loadingImport.value = true
  try {
    addLog(`开始上传文件，共 ${files.value.length} 个`)
    for (const file of files.value) {
      const name = file.webkitRelativePath || file.name
      addLog(`导入文件: ${name}，大小 ${formatBytes(file.size)}`)
    }

    const formData = new FormData()
    for (const file of files.value) {
      formData.append('files', file, file.webkitRelativePath || file.name)
    }

    const resp = await fetch('/api/import/session', {
      method: 'POST',
      body: formData,
    })
    if (!resp.ok) {
      addLog('创建导入会话失败')
      return
    }

    const data = await resp.json()
    importState.session_id = data.session_id || ''
    addLog(`文件上传完成，共 ${files.value.length} 个`)
    addLog(`导入会话已创建: ${importState.session_id}`)
  } finally {
    loadingImport.value = false
  }
}

async function importTradingDays() {
  if (!tradingDayFile.value) {
    addLog('未选择交易日历文件')
    return
  }

  loadingTradingDayImport.value = true
  try {
    const formData = new FormData()
    formData.append('file', tradingDayFile.value, tradingDayFile.value.name)
    const resp = await fetch('/api/calendar/import/tdx-daily', {
      method: 'POST',
      body: formData,
    })
    if (!resp.ok) {
      const message = (await resp.text()) || `http ${resp.status}`
      alert(message)
      addLog(`加载历史交易日历失败: ${message}`)
      return
    }
    const data = await resp.json()
    tradingDayImportResult.value = {
      minDate: data.min_date || '--',
      maxDate: data.max_date || '--',
      importedDays: Number(data.imported_days || 0),
    }
    addLog(`加载历史交易日历成功: 首日=${data.min_date} 末日=${data.max_date} 交易日=${data.imported_days} 表=${data.table_name || 'trading_calendar'} 写入成功=${data.write_success === true}`)
    addLog(`加载历史交易日历详情: 删除区间记录 ${data.deleted_range_rows} 条`)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    alert(message)
    addLog(`加载历史交易日历失败: ${message}`)
  } finally {
    loadingTradingDayImport.value = false
  }
}

async function submitDecision(action) {
  if (!conflict.value || !importState.session_id) return

  const payload = {
    action,
    overwrite_instrument: !!conflict.value.overwriteInstrument,
    overwrite_all_contracts: !!conflict.value.overwriteAll,
  }

  const resp = await fetch(`/api/import/session/${importState.session_id}/decision`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) {
    addLog('提交冲突决策失败')
    return
  }
  conflict.value = null
}

async function runSearch(resetPage = false) {
  if (resetPage) {
    searchForm.page = 1
  }

  searchState.loading = true
  try {
    const params = new URLSearchParams({
      keyword: searchForm.keyword.trim(),
      start: normalizeDateTimeInput(searchForm.start),
      end: normalizeDateTimeInput(searchForm.end),
      page: String(searchForm.page),
      page_size: String(searchForm.pageSize),
    })
    const resp = await fetch(`/api/kline/search?${params}`)
    if (!resp.ok) {
      throw new Error(`查询失败: ${resp.status}`)
    }
    const data = await resp.json()
    searchState.items = data.items || []
    searchState.total = data.total || 0
  } catch (error) {
    addLog(`K线查询失败: ${error.message}`)
  } finally {
    searchState.loading = false
  }
}

function prevPage() {
  if (searchForm.page <= 1) return
  searchForm.page -= 1
  void runSearch(false)
}

function nextPage() {
  if (searchForm.page >= totalPages.value) return
  searchForm.page += 1
  void runSearch(false)
}

function openChart(item) {
  const params = new URLSearchParams({
    symbol: item.symbol,
    type: item.type,
    variety: item.variety || '',
    start: normalizeDateTimeInput(searchForm.start),
    end: normalizeDateTimeInput(searchForm.end),
  })
  const url = `/chart?${params.toString()}`
  console.log('[openChart] params', {
    symbol: item.symbol,
    type: item.type,
    variety: item.variety || '',
    start: normalizeDateTimeInput(searchForm.start),
    end: normalizeDateTimeInput(searchForm.end),
    url,
  })
  addLog(`openChart symbol=${item.symbol} type=${item.type} variety=${item.variety || ''} start=${normalizeDateTimeInput(searchForm.start)} end=${normalizeDateTimeInput(searchForm.end)}`)
  window.open(url, '_blank')
}

function connectWS() {
  const protocol = location.protocol === 'https:' ? 'wss' : 'ws'
  ws = new WebSocket(`${protocol}://${location.host}/ws`)

  ws.onopen = () => {
    wsConnected.value = true
    addLog('WebSocket 已连接')
  }

  ws.onclose = () => {
    wsConnected.value = false
    addLog('WebSocket 已断开，2秒后重连')
    setTimeout(connectWS, 2000)
  }

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data)
    if (msg.type === 'status_update' && msg.data?.status) {
      applyStatusSnapshot(msg.data.status)
      if (msg.data.replay) {
        replaySupported.value = true
        applyReplayState(msg.data.replay)
      } else {
        replaySupported.value = false
      }
      return
    }
    if (msg.type === 'strategy_status_update' && msg.data) {
      applyStrategyStatus(msg.data)
      fetchStrategyBundle()
      return
    }
    if (msg.type === 'strategy_signal' && msg.data) {
      strategyState.signals = [msg.data, ...(strategyState.signals || [])].slice(0, 20)
      return
    }
    if (msg.type === 'strategy_backtest_done' && msg.data) {
      strategyState.backtests = [msg.data, ...(strategyState.backtests || [])].slice(0, 20)
      return
    }
    if (msg.type === 'trade_status_update' && msg.data) {
      applyTradeStatus(msg.data)
      return
    }
    if (msg.type === 'trade_account_update' && msg.data) {
      tradeState.account = msg.data
      return
    }
    if (msg.type === 'trade_position_update' && msg.data) {
      tradeState.positions = msg.data.items || tradeState.positions
      return
    }
    if (msg.type === 'trade_order_update' && msg.data) {
      fetchTradeBundle()
      return
    }
    if (msg.type === 'trade_trade_update' && msg.data) {
      fetchTradeBundle()
      return
    }
    if (msg.type === 'trade_command_audit' && msg.data) {
      tradeState.audits = [msg.data, ...(tradeState.audits || [])].slice(0, 20)
      return
    }
    if (msg.type === 'order_audit_update' && msg.data) {
      strategyState.orderAudits = [msg.data, ...(strategyState.orderAudits || [])].slice(0, 20)
      fetch('/api/orders/status').then((resp) => resp.ok ? resp.json() : null).then((data) => {
        if (data) strategyState.ordersStatus = data
      }).catch(() => {})
      return
    }
    if (msg.type === 'import_progress' && msg.data?.progress) {
      if (msg.data.session_id === importState.session_id || !importState.session_id) {
        Object.assign(importState, msg.data.progress)
      }
      return
    }
    if (msg.type === 'import_conflict' && msg.data?.conflict) {
      if (msg.data.session_id === importState.session_id) {
        conflict.value = {
          ...msg.data.conflict,
          overwriteInstrument: false,
          overwriteAll: false,
        }
      }
      return
    }
    if (msg.type === 'import_done' && msg.data?.progress) {
      if (msg.data.session_id === importState.session_id) {
        Object.assign(importState, msg.data.progress)
        addLog(
          `导入完成: 文件 ${importState.processed_files}/${importState.total_files}，数据行 ${importState.total_lines}，新增 ${importState.inserted_rows}，覆盖 ${importState.overwritten_rows}，跳过 ${importState.skipped_rows}`,
        )
      }
      return
    }
    if (msg.type === 'import_error' && msg.data?.error) {
      addLog(`导入错误: ${msg.data.error}`)
    }
  }
}

onMounted(async () => {
  defaultTimeRange()
  try {
    await fetchStatus()
  } catch (error) {
    addLog(`状态获取失败: ${error.message}`)
  }
  await fetchReplayStatus()
  await fetchStrategyBundle()
  await fetchTradeBundle()
  await runSearch(true)
  connectWS()
  statusPollTimer = setInterval(() => {
    fetchStatus().catch(() => {})
  }, 5000)
  tradePollTimer = setInterval(() => {
    fetchTradeBundle().catch(() => {})
  }, 5000)
})

onUnmounted(() => {
  if (statusPollTimer) {
    clearInterval(statusPollTimer)
    statusPollTimer = null
  }
  if (tradePollTimer) {
    clearInterval(tradePollTimer)
    tradePollTimer = null
  }
  if (ws) {
    ws.close()
  }
})
</script>

<template>
  <div class="container">
    <div class="panel">
      <h2>CTP Kline 控制台</h2>
      <p>版本号: {{ APP_VERSION }}</p>
      <div class="row">
        <button @click="startServer">启动服务器</button>
        <span>WebSocket: {{ wsConnected ? '已连接' : '未连接' }}</span>
        <span :class="marketBadgeClass">{{ marketBadgeText }}</span>
      </div>
    </div>

    <div class="panel">
      <h3>连接状态</h3>
      <div class="status-grid">
        <div class="status-item">状态: {{ status.state }}</div>
        <div class="status-item">Trader 前置: {{ status.trader_front ? '已连接' : '未连接' }}</div>
        <div class="status-item">Trader 登录: {{ status.trader_login ? '已登录' : '未登录' }}</div>
        <div class="status-item">MD 前置: {{ status.md_front ? '已连接' : '未连接' }}</div>
        <div class="status-item">MD 登录: {{ status.md_login ? '已登录' : '未登录' }}</div>
        <div class="status-item">订阅状态: {{ status.md_subscribed ? '已订阅' : '未订阅' }}</div>
        <div class="status-item">订阅数量: {{ status.subscribe_count }}</div>
        <div class="status-item">服务器时间: {{ status.server_time || '--' }}</div>
        <div class="status-item">MD断线: {{ status.md_front_disconnected ? `是(原因 ${status.md_disconnect_reason})` : '否' }}</div>
        <div class="status-item">重连尝试: {{ status.md_reconnect_attempt || 0 }}</div>
        <div class="status-item">下次重试: {{ status.md_next_retry_at || '--' }}</div>
        <div class="status-item">网络疑似断开: {{ status.network_suspect ? '是' : '否' }}</div>
        <div class="status-item">去重丢弃: {{ status.tick_dedup_dropped || 0 }}</div>
        <div class="status-item">漂移秒数: {{ Number(status.drift_seconds || 0).toFixed(3) }}</div>
        <div class="status-item">漂移暂停: {{ status.drift_paused ? '是' : '否' }}</div>
        <div class="status-item">漂移暂停次数: {{ status.drift_pause_count || 0 }}</div>
      </div>
      <p v-if="formattedTradingDay">当前交易日：{{ formattedTradingDay }}</p>
      <p v-if="status.last_error" style="color: #b22222">错误: {{ status.last_error }}</p>
      <p v-if="status.network_suspect" style="color: #b22222">告警: 长时间无Tick，疑似断网</p>
      <p v-if="status.drift_paused" style="color: #b22222">告警: 行情时钟漂移超阈值，已暂停写入</p>
    </div>

    <div class="panel">
      <h3>链路耗时</h3>
      <div class="status-grid">
        <div class="status-item">上游时间差: {{ Number(status.upstream_lag_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">回调到入队: {{ Number(status.callback_to_process_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">路由排队时间: {{ Number(status.router_queue_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">分片排队时间: {{ Number(status.shard_queue_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">DB 排队时间: {{ Number(status.persist_queue_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">DB 批量执行时间: {{ Number(status.db_flush_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">DB 本批写入行数: {{ status.db_flush_rows || 0 }}</div>
        <div class="status-item">DB 队列深度: {{ status.db_queue_depth || 0 }}</div>
        <div class="status-item">文件批量落盘时间: {{ Number(status.file_flush_ms || 0).toFixed(1) }} ms</div>
        <div class="status-item">文件队列深度: {{ status.file_queue_depth || 0 }}</div>
        <div class="status-item">端到端总耗时: {{ Number(status.end_to_end_ms || 0).toFixed(1) }} ms</div>
      </div>
      <p>说明: 排队时间看堆积，批量执行时间看数据库写入本身，总耗时看单条数据从接收到落库的整体延迟。</p>
    </div>

    <div class="panel">
      <h3>Replay 控制与监控</h3>
      <p v-if="!replayEnabled" class="error-text">Replay 未启用</p>
      <div class="row">
        <button :disabled="!canStartReplay" @click="startReplay">
          {{ replayLoading.starting ? '启动中...' : '开始回放' }}
        </button>
        <button class="secondary" :disabled="!canPauseReplay" @click="pauseReplay">
          {{ replayLoading.pausing ? '暂停中...' : '暂停' }}
        </button>
        <button class="secondary" :disabled="!canResumeReplay" @click="resumeReplay">
          {{ replayLoading.resuming ? '继续中...' : '继续' }}
        </button>
        <button class="secondary" :disabled="!canStopReplay" @click="stopReplay">
          {{ replayLoading.stopping ? '停止中...' : '停止' }}
        </button>
      </div>

      <div class="replay-form-grid">
        <label>Topics(逗号分隔)</label>
        <input v-model="replayForm.topicsText" :disabled="!replayEnabled" placeholder="tick,bar,order_command,order_status" />
        <label>Sources(逗号分隔)</label>
        <input v-model="replayForm.sourcesText" :disabled="!replayEnabled" placeholder="replay.tickcsv" />
        <label>开始时间</label>
        <input v-model="replayForm.start" :disabled="!replayEnabled" type="datetime-local" />
        <label>结束时间</label>
        <input v-model="replayForm.end" :disabled="!replayEnabled" type="datetime-local" />
        <label>模式</label>
        <select v-model="replayForm.mode" :disabled="!replayEnabled">
          <option value="fast">fast</option>
          <option value="realtime">realtime</option>
        </select>
        <label>速度</label>
        <input v-model.number="replayForm.speed" :disabled="!replayEnabled" type="number" min="0.1" step="0.1" />
        <label>Tick目录</label>
        <input v-model="replayForm.tickDir" :disabled="!replayEnabled" placeholder="flow/ticks" />
        <label>全部回放</label>
        <label class="checkbox-field">
          <input v-model="replayForm.fullReplay" :disabled="!replayEnabled" type="checkbox" />
          <span>启动前清空记账表并完整重放</span>
        </label>
        <label>起始文件</label>
        <input v-model="replayForm.fromFile" :disabled="!replayEnabled" placeholder="rb2505.csv" />
        <label>起始偏移</label>
        <input v-model.number="replayForm.fromOffset" :disabled="!replayEnabled" type="number" min="0" step="1" />
      </div>

      <div class="status-grid">
        <div class="status-item">状态: {{ replayState.status || '--' }}</div>
        <div class="status-item">任务ID: {{ replayState.task_id || '--' }}</div>
        <div class="status-item">模式: {{ replayState.mode || '--' }}</div>
        <div class="status-item">速度: {{ replayState.speed || 1 }}</div>
        <div class="status-item">合约数: {{ replayState.instruments || 0 }}</div>
        <div class="status-item">Tick文件数: {{ replayState.tick_files || 0 }}</div>
        <div class="status-item">总Tick数: {{ replayState.total_ticks || 0 }}</div>
        <div class="status-item">已处理Tick: {{ replayState.processed_ticks || 0 }}</div>
        <div class="status-item">回放进度: {{ replayProgressText }}</div>
        <div class="status-item">当前回放合约: {{ replayState.current_instrument_id || '--' }}</div>
        <div class="status-item">模拟当前时间: {{ replayState.current_sim_time || '--' }}</div>
        <div class="status-item">模拟开始时间: {{ replayState.first_sim_time || '--' }}</div>
        <div class="status-item">模拟结束时间: {{ replayState.last_sim_time || '--' }}</div>
        <div class="status-item">剩余模拟时长: {{ replayRemainingText }}</div>
        <div class="status-item">ETA: {{ replayEtaText }}</div>
        <div class="status-item">主题: {{ (replayState.topics || []).join(', ') || '--' }}</div>
        <div class="status-item">来源: {{ (replayState.sources || []).join(', ') || '--' }}</div>
        <div class="status-item">开始过滤: {{ replayState.start_time || '--' }}</div>
        <div class="status-item">结束过滤: {{ replayState.end_time || '--' }}</div>
        <div class="status-item">Tick目录: {{ replayState.tick_dir || '--' }}</div>
        <div class="status-item">起始游标: {{ replayState.from_cursor ? `${replayState.from_cursor.file}@${replayState.from_cursor.offset}` : '--' }}</div>
        <div class="status-item">当前游标: {{ replayState.last_cursor ? `${replayState.last_cursor.file}@${replayState.last_cursor.offset}` : '--' }}</div>
        <div class="status-item">分发数: {{ replayState.dispatched || 0 }}</div>
        <div class="status-item">跳过数: {{ replayState.skipped || 0 }}</div>
        <div class="status-item">错误数: {{ replayState.errors || 0 }}</div>
        <div class="status-item">最后错误: {{ replayState.last_error || '--' }}</div>
        <div class="status-item">创建时间: {{ replayState.created_at || '--' }}</div>
        <div class="status-item">开始执行: {{ replayState.started_at || '--' }}</div>
        <div class="status-item">完成时间: {{ replayState.finished_at || '--' }}</div>
      </div>
    </div>

    <div class="panel">
      <h3>策略管理</h3>
      <p v-if="!strategyEnabled" class="error-text">策略子系统未启用</p>
      <div class="status-grid">
        <div class="status-item">启用: {{ strategyStatus.enabled ? '是' : '否' }}</div>
        <div class="status-item">Python 进程: {{ strategyStatus.process_running ? '运行中' : '未运行' }}</div>
        <div class="status-item">gRPC 连接: {{ strategyStatus.connected ? '已连接' : '未连接' }}</div>
        <div class="status-item">gRPC 地址: {{ strategyStatus.grpc_addr || '--' }}</div>
        <div class="status-item">Python 入口: {{ strategyStatus.python_entry || '--' }}</div>
        <div class="status-item">策略定义数: {{ strategyStatus.definitions || 0 }}</div>
        <div class="status-item">实例数: {{ strategyStatus.instances || 0 }}</div>
        <div class="status-item">运行中实例: {{ strategyStatus.running_count || 0 }}</div>
        <div class="status-item">信号数: {{ strategyStatus.signal_count || 0 }}</div>
        <div class="status-item">审计数: {{ strategyStatus.audit_count || 0 }}</div>
        <div class="status-item">回测数: {{ strategyStatus.backtest_run_count || 0 }}</div>
        <div class="status-item">最近健康检查: {{ strategyStatus.last_health_at || '--' }}</div>
      </div>
      <p v-if="strategyStatus.last_error" class="error-text">策略错误: {{ strategyStatus.last_error }}</p>

      <div class="replay-form-grid">
        <label>策略定义</label>
        <select v-model="strategyForm.strategy_id">
          <option value="">请选择</option>
          <option v-for="item in strategyState.definitions" :key="item.strategy_id" :value="item.strategy_id">
            {{ item.display_name }} ({{ item.strategy_id }})
          </option>
        </select>
        <label>实例名称</label>
        <input v-model="strategyForm.display_name" placeholder="my-strategy-instance" />
        <label>运行模式</label>
        <select v-model="strategyForm.mode">
          <option value="realtime">realtime</option>
          <option value="replay">replay</option>
          <option value="paper">paper</option>
        </select>
        <label>账户</label>
        <input v-model="strategyForm.account_id" placeholder="paper" />
        <label>合约(逗号分隔)</label>
        <input v-model="strategyForm.symbols_text" placeholder="rb2505,ag2605" />
        <label>周期</label>
        <input v-model="strategyForm.timeframe" placeholder="1m" />
        <label>参数 JSON</label>
        <textarea v-model="strategyForm.params_text" rows="4" placeholder='{"threshold": 0.2}' />
      </div>
      <div class="row">
        <button @click="saveStrategyInstance">保存实例</button>
      </div>

      <table class="table query-table">
        <thead>
          <tr>
            <th>实例</th>
            <th>策略</th>
            <th>模式</th>
            <th>状态</th>
            <th>合约</th>
            <th>周期</th>
            <th>目标仓位</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in strategyState.instances" :key="item.instance_id">
            <td>{{ item.display_name || item.instance_id }}</td>
            <td>{{ item.strategy_id }}</td>
            <td>{{ item.mode }}</td>
            <td>{{ item.status }}</td>
            <td>{{ (item.symbols || []).join(', ') || '--' }}</td>
            <td>{{ item.timeframe || '--' }}</td>
            <td>{{ item.last_target_position ?? '--' }}</td>
            <td>
              <button class="secondary" @click="startStrategyInstance(item.instance_id)">启动</button>
              <button class="secondary" @click="stopStrategyInstance(item.instance_id)">停止</button>
            </td>
          </tr>
          <tr v-if="strategyState.instances.length === 0">
            <td colspan="8">无策略实例</td>
          </tr>
        </tbody>
      </table>

      <h4>回测</h4>
      <div class="replay-form-grid">
        <label>实例</label>
        <select v-model="backtestForm.instance_id">
          <option value="">请选择</option>
          <option v-for="item in strategyState.instances" :key="item.instance_id" :value="item.instance_id">
            {{ item.display_name || item.instance_id }}
          </option>
        </select>
        <label>合约</label>
        <input v-model="backtestForm.symbol" placeholder="rb2505" />
        <label>周期</label>
        <input v-model="backtestForm.timeframe" placeholder="1m" />
        <label>开始时间</label>
        <input v-model="backtestForm.start_time" type="datetime-local" />
        <label>结束时间</label>
        <input v-model="backtestForm.end_time" type="datetime-local" />
      </div>
      <div class="row">
        <button @click="runBacktest">提交回测</button>
      </div>

      <div class="status-grid">
        <div class="status-item">执行模式: {{ strategyState.ordersStatus.mode || '--' }}</div>
        <div class="status-item">持仓: {{ JSON.stringify(strategyState.ordersStatus.positions || {}) }}</div>
      </div>

      <div class="row">
        <div style="flex: 1">
          <h4>最近信号</h4>
          <div class="log-box">
            <div v-for="item in strategyState.signals" :key="`sig-${item.id || item.event_time}`">
              {{ item.event_time }} | {{ item.instance_id }} | {{ item.symbol }} | 目标={{ item.target_position }} | {{ item.reason }}
            </div>
          </div>
        </div>
        <div style="flex: 1">
          <h4>最近审计</h4>
          <div class="log-box">
            <div v-for="item in strategyState.orderAudits" :key="`audit-${item.id || item.event_time}`">
              {{ item.event_time }} | {{ item.instance_id }} | Δ={{ item.planned_delta }} | 风控={{ item.risk_status }} | 订单={{ item.order_status }}
            </div>
          </div>
        </div>
      </div>

      <h4>最近回测</h4>
      <div class="log-box">
        <div v-for="item in strategyState.backtests" :key="item.run_id">
          {{ item.started_at || '--' }} | {{ item.run_id }} | {{ item.symbol }} | {{ item.status }} | {{ item.output_path || '--' }}
        </div>
      </div>
    </div>

    <div class="panel">
      <h3>实盘交易</h3>
      <div class="row">
        <button class="secondary" @click="refreshTradeData">刷新查询</button>
      </div>
      <div class="status-grid">
        <div class="status-item">启用: {{ tradeStatus.enabled ? '是' : '否' }}</div>
        <div class="status-item">账户: {{ tradeStatus.account_id || '--' }}</div>
        <div class="status-item">Trader 前置: {{ tradeStatus.trader_front ? '已连接' : '未连接' }}</div>
        <div class="status-item">Trader 登录: {{ tradeStatus.trader_login ? '已登录' : '未登录' }}</div>
        <div class="status-item">结算确认: {{ tradeStatus.settlement_confirmed ? '已确认' : '未确认' }}</div>
        <div class="status-item">交易日: {{ tradeStatus.trading_day || '--' }}</div>
        <div class="status-item">FrontID: {{ tradeStatus.front_id || 0 }}</div>
        <div class="status-item">SessionID: {{ tradeStatus.session_id || 0 }}</div>
        <div class="status-item">最近查询: {{ tradeStatus.last_query_at || '--' }}</div>
      </div>
      <p v-if="tradeStatus.last_error" class="error-text">交易错误: {{ tradeStatus.last_error }}</p>

      <div class="status-grid">
        <div class="status-item">权益: {{ tradeState.account?.balance ?? '--' }}</div>
        <div class="status-item">可用: {{ tradeState.account?.available ?? '--' }}</div>
        <div class="status-item">保证金: {{ tradeState.account?.margin ?? '--' }}</div>
        <div class="status-item">冻结现金: {{ tradeState.account?.frozen_cash ?? '--' }}</div>
        <div class="status-item">手续费: {{ tradeState.account?.commission ?? '--' }}</div>
        <div class="status-item">平仓盈亏: {{ tradeState.account?.close_profit ?? '--' }}</div>
        <div class="status-item">持仓盈亏: {{ tradeState.account?.position_profit ?? '--' }}</div>
        <div class="status-item">资金更新时间: {{ tradeState.account?.updated_at || '--' }}</div>
      </div>

      <div class="replay-form-grid">
        <label>账户</label>
        <input v-model="tradeForm.account_id" placeholder="default" />
        <label>合约</label>
        <input v-model="tradeForm.symbol" placeholder="ag2605" />
        <label>交易所</label>
        <input v-model="tradeForm.exchange_id" placeholder="SHFE" />
        <label>方向</label>
        <select v-model="tradeForm.direction">
          <option value="buy">buy</option>
          <option value="sell">sell</option>
        </select>
        <label>开平</label>
        <select v-model="tradeForm.offset_flag">
          <option value="open">open</option>
          <option value="close">close</option>
          <option value="close_today">close_today</option>
          <option value="close_yesterday">close_yesterday</option>
        </select>
        <label>限价</label>
        <input v-model="tradeForm.limit_price" type="number" min="0" step="0.01" />
        <label>手数</label>
        <input v-model.number="tradeForm.volume" type="number" min="1" step="1" />
        <label>标签</label>
        <input v-model="tradeForm.client_tag" placeholder="manual-ui" />
      </div>
      <div class="row">
        <button @click="submitTradeOrder">提交限价单</button>
      </div>

      <h4>持仓</h4>
      <table class="table query-table">
        <thead>
          <tr>
            <th>合约</th>
            <th>方向</th>
            <th>总仓</th>
            <th>今仓</th>
            <th>昨仓</th>
            <th>保证金</th>
            <th>更新时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in tradeState.positions" :key="`${item.symbol}-${item.direction}-${item.hedge_flag}`">
            <td>{{ item.symbol }}</td>
            <td>{{ item.direction }}</td>
            <td>{{ item.position }}</td>
            <td>{{ item.today_position }}</td>
            <td>{{ item.yd_position }}</td>
            <td>{{ item.use_margin }}</td>
            <td>{{ item.updated_at }}</td>
          </tr>
          <tr v-if="tradeState.positions.length === 0">
            <td colspan="7">无持仓</td>
          </tr>
        </tbody>
      </table>

      <h4>订单</h4>
      <table class="table query-table">
        <thead>
          <tr>
            <th>命令</th>
            <th>合约</th>
            <th>方向</th>
            <th>开平</th>
            <th>价格</th>
            <th>原始手数</th>
            <th>已成</th>
            <th>状态</th>
            <th>提交</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in tradeState.orders" :key="item.command_id">
            <td>{{ item.command_id }}</td>
            <td>{{ item.symbol }}</td>
            <td>{{ item.direction }}</td>
            <td>{{ item.offset_flag }}</td>
            <td>{{ item.limit_price }}</td>
            <td>{{ item.volume_total_original }}</td>
            <td>{{ item.volume_traded }}</td>
            <td>{{ item.order_status }}</td>
            <td>{{ item.submit_status }}</td>
            <td><button class="secondary" @click="cancelTradeOrder(item)">撤单</button></td>
          </tr>
          <tr v-if="tradeState.orders.length === 0">
            <td colspan="10">无订单</td>
          </tr>
        </tbody>
      </table>

      <h4>成交</h4>
      <div class="log-box">
        <div v-for="item in tradeState.trades" :key="`${item.trade_id}-${item.received_at}`">
          {{ item.trade_time }} | {{ item.symbol }} | {{ item.direction }} | {{ item.offset_flag }} | {{ item.price }} x {{ item.volume }}
        </div>
      </div>
    </div>

    <div class="panel">
      <h3>K线查询</h3>
      <div class="row">
        <input v-model="searchForm.keyword" placeholder="关键字（代码/品种/交易所）" />
        <input v-model="searchForm.start" type="datetime-local" />
        <input v-model="searchForm.end" type="datetime-local" />
        <button :disabled="searchState.loading" @click="runSearch(true)">
          {{ searchState.loading ? '查询中...' : '查询' }}
        </button>
      </div>
      <table class="table query-table">
        <thead>
          <tr>
            <th>类型</th>
            <th>代码</th>
            <th>品种</th>
            <th>交易所</th>
            <th>开始</th>
            <th>结束</th>
            <th>数量</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in searchState.items" :key="`${item.type}-${item.symbol}`">
            <td>{{ item.type }}</td>
            <td>{{ item.symbol }}</td>
            <td>{{ item.variety }}</td>
            <td>{{ item.exchange }}</td>
            <td>{{ item.min_time }}</td>
            <td>{{ item.max_time }}</td>
            <td>{{ item.bar_count }}</td>
            <td>
              <button class="secondary" @click="openChart(item)">打开图表</button>
            </td>
          </tr>
          <tr v-if="searchState.items.length === 0">
            <td colspan="8">无数据</td>
          </tr>
        </tbody>
      </table>
      <div class="row">
        <button class="secondary" :disabled="searchForm.page <= 1" @click="prevPage">上一页</button>
        <span>第 {{ searchForm.page }} / {{ totalPages }} 页（总 {{ searchState.total }} 条）</span>
        <button class="secondary" :disabled="searchForm.page >= totalPages" @click="nextPage">下一页</button>
      </div>
    </div>

    <div class="panel">
      <h3>加载历史交易日历</h3>
      <div class="row">
        <input type="file" accept=".txt" @change="onSelectTradingDayFile" />
        <button :disabled="loadingTradingDayImport" @click="importTradingDays">
          {{ loadingTradingDayImport ? '加载中...' : '开始加载交易日历' }}
        </button>
      </div>
      <p>建议文件: 42#T001.txt（日线）</p>
      <p>已选择文件: {{ tradingDayFile ? tradingDayFile.name : '--' }}</p>
      <p v-if="tradingDayImportResult">
        开始交易日: {{ tradingDayImportResult.minDate }}，结束交易日: {{ tradingDayImportResult.maxDate }}，写入交易日数量: {{ tradingDayImportResult.importedDays }}
      </p>
    </div>

    <div class="panel">
      <h3>加载历史数据</h3>
      <div class="row">
        <input type="file" webkitdirectory directory multiple @change="onSelectFiles" />
        <button :disabled="loadingImport" @click="startImport">{{ loadingImport ? '上传中...' : '开始导入' }}</button>
      </div>
      <p>已选择文件数: {{ files.length }}</p>
      <div class="status-grid">
        <div class="status-item">会话: {{ importState.session_id || '--' }}</div>
        <div class="status-item">文件进度: {{ importState.processed_files }}/{{ importState.total_files }}</div>
        <div class="status-item">解析行数: {{ importState.total_lines }}</div>
        <div class="status-item">新增: {{ importState.inserted_rows }}</div>
        <div class="status-item">覆盖: {{ importState.overwritten_rows }}</div>
        <div class="status-item">跳过: {{ importState.skipped_rows }}</div>
        <div class="status-item">跳过文件: {{ importState.skipped_files }}</div>
        <div class="status-item">错误数: {{ importState.error_count }}</div>
      </div>
    </div>

    <div class="panel">
      <h3>事件日志</h3>
      <div class="log-box">
        <div v-for="item in logs" :key="item">{{ item }}</div>
      </div>
    </div>
  </div>

  <div v-if="conflict" class="modal-backdrop">
    <div class="modal">
      <h3>检测到重复数据</h3>
      <p v-if="conflict.batch">
        文件 {{ conflict.file_name }}，合约 {{ conflict.instrument_id }}。新数据 {{ conflict.new_count }} 条，重复数据 {{ conflict.duplicate_count }} 条。
      </p>
      <p v-else>
        文件 {{ conflict.file_name }} 第 {{ conflict.line_number }} 行，合约 {{ conflict.instrument_id }}，时间 {{ conflict.minute_time }}
      </p>
      <table v-if="!conflict.batch" class="table">
        <thead>
          <tr>
            <th>字段</th>
            <th>数据库已有</th>
            <th>新导入</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>开盘</td>
            <td>{{ conflict.existing.open }}</td>
            <td>{{ conflict.incoming.open }}</td>
          </tr>
          <tr>
            <td>最高</td>
            <td>{{ conflict.existing.high }}</td>
            <td>{{ conflict.incoming.high }}</td>
          </tr>
          <tr>
            <td>最低</td>
            <td>{{ conflict.existing.low }}</td>
            <td>{{ conflict.incoming.low }}</td>
          </tr>
          <tr>
            <td>收盘</td>
            <td>{{ conflict.existing.close }}</td>
            <td>{{ conflict.incoming.close }}</td>
          </tr>
          <tr>
            <td>成交量</td>
            <td>{{ conflict.existing.volume }}</td>
            <td>{{ conflict.incoming.volume }}</td>
          </tr>
        </tbody>
      </table>

      <div class="row">
        <label>
          <input v-model="conflict.overwriteInstrument" type="checkbox" /> 此合约全部覆盖
        </label>
        <label>
          <input v-model="conflict.overwriteAll" type="checkbox" /> 所有合约全部覆盖
        </label>
      </div>

      <div class="row" style="margin-top: 10px">
        <button @click="submitDecision('overwrite')">覆盖</button>
        <button class="secondary" @click="submitDecision('skip')">跳过</button>
        <button class="secondary" @click="submitDecision('cancel')">取消</button>
      </div>
    </div>
  </div>
</template>
