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
  sourcesText: '',
  start: '',
  end: '',
  mode: 'fast',
  speed: 1,
  fromFile: '',
  fromOffset: 0,
})

const replayLoading = reactive({
  starting: false,
  pausing: false,
  resuming: false,
  stopping: false,
  fetchingStatus: false,
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
const canStartReplay = computed(() => replayEnabled.value && ['idle', 'done', 'error', 'stopped', ''].includes(replayStatus.value) && !replayLoading.starting)
const canPauseReplay = computed(() => replayEnabled.value && replayStatus.value === 'running' && !replayLoading.pausing)
const canResumeReplay = computed(() => replayEnabled.value && replayStatus.value === 'paused' && !replayLoading.resuming)
const canStopReplay = computed(() => replayEnabled.value && ['running', 'paused'].includes(replayStatus.value) && !replayLoading.stopping)

function addLog(message) {
  logs.value.unshift(`${new Date().toLocaleTimeString()} ${message}`)
  if (logs.value.length > 80) {
    logs.value = logs.value.slice(0, 80)
  }
}

function formatBytes(bytes) {
  const n = Number(bytes || 0)
  if (!Number.isFinite(n) || n <= 0) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(2)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MB`
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function toDateTimeLocal(date) {
  const pad = (n) => String(n).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function defaultTimeRange() {
  const end = new Date()
  const start = new Date(end.getTime())
  start.setMonth(start.getMonth() - 3)
  searchForm.end = toDateTimeLocal(end)
  searchForm.start = toDateTimeLocal(start)
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

async function fetchStatus() {
  const resp = await fetch('/api/status')
  if (!resp.ok) {
    throw new Error(`status http ${resp.status}`)
  }
  const data = await resp.json()
  Object.assign(status, data.status || {})
  if (data.replay) {
    replaySupported.value = true
    applyReplayState(data.replay)
  } else {
    replaySupported.value = false
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

  const payload = {
    topics: parseCSVInput(replayForm.topicsText),
    sources: parseCSVInput(replayForm.sourcesText),
    mode: replayForm.mode || 'fast',
    speed: Number(replayForm.speed) || 1,
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
      Object.assign(status, msg.data.status)
      if (msg.data.replay) {
        replaySupported.value = true
        applyReplayState(msg.data.replay)
      } else {
        replaySupported.value = false
      }
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
  await runSearch(true)
  connectWS()
})

onUnmounted(() => {
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
        <input v-model="replayForm.sourcesText" :disabled="!replayEnabled" placeholder="trader.md,trader.bar,replay" />
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
        <label>起始文件</label>
        <input v-model="replayForm.fromFile" :disabled="!replayEnabled" placeholder="events-YYYYMMDD.log" />
        <label>起始偏移</label>
        <input v-model.number="replayForm.fromOffset" :disabled="!replayEnabled" type="number" min="0" step="1" />
      </div>

      <div class="status-grid">
        <div class="status-item">状态: {{ replayState.status || '--' }}</div>
        <div class="status-item">任务ID: {{ replayState.task_id || '--' }}</div>
        <div class="status-item">模式: {{ replayState.mode || '--' }}</div>
        <div class="status-item">速度: {{ replayState.speed || 1 }}</div>
        <div class="status-item">主题: {{ (replayState.topics || []).join(', ') || '--' }}</div>
        <div class="status-item">来源: {{ (replayState.sources || []).join(', ') || '--' }}</div>
        <div class="status-item">开始过滤: {{ replayState.start_time || '--' }}</div>
        <div class="status-item">结束过滤: {{ replayState.end_time || '--' }}</div>
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
