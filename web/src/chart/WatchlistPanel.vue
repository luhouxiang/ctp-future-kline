<script setup>
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { DEFAULT_CHANNEL_SETTINGS, normalizeChannelSettingsV2 } from './analysis/channelDetector'

const props = defineProps({
  open: { type: Boolean, default: true },
  items: { type: Array, default: () => [] },
  current: { type: String, default: '' },
  currentTimeframe: { type: String, default: '1m' },
  quoteSnapshot: { type: Object, default: () => ({}) },
  quoteTicks: { type: Array, default: () => [] },
  drawings: { type: Array, default: () => [] },
  lineOrders: { type: Array, default: () => [] },
  strategy: { type: Object, default: () => ({ status: {}, instances: [], traces: [], backtests: [] }) },
  selectedDrawingId: { type: String, default: '' },
  activeTab: { type: String, default: 'quote' },
  channels: { type: Object, default: () => ({ rows: [], selected_id: '', settings: {}, detail: null }) },
  reversal: { type: Object, default: () => ({ settings: {}, results: { lines: [], events: [] }, selected_id: '' }) },
  lightweightOnly: { type: Boolean, default: false },
  selectedStrategyInstanceId: { type: String, default: '' },
})

const emit = defineEmits([
  'toggle',
  'select',
  'set-active-tab',
  'select-drawing',
  'toggle-drawing-visible',
  'delete-drawing',
  'arm-line-order',
  'disable-line-order',
  'stop-line-orders',
  'strategy-trace-focus',
  'strategy-instance-select',
  'strategy-instance-stop',
  'strategy-run-click',
  'channel-action',
  'channel-settings',
  'reversal-action',
])

const hoverRowId = ref('')
const rowRefs = new Map()
const selectedChannelRows = ref([])
const filterStatus = ref('all')
const filterMethod = ref('all')
const scoreMin = ref(0)

function cloneSettings(v) {
  return JSON.parse(JSON.stringify(v))
}

const draftSettings = reactive(normalizeChannelSettingsV2(DEFAULT_CHANNEL_SETTINGS))

function applyDraftSettings(v) {
  const norm = normalizeChannelSettingsV2(v)
  Object.assign(draftSettings.display, norm.display)
  Object.assign(draftSettings.common, norm.common)
  Object.assign(draftSettings.algorithms.extrema, norm.algorithms.extrema)
  Object.assign(draftSettings.algorithms.ransac, norm.algorithms.ransac)
  Object.assign(draftSettings.algorithms.regression, norm.algorithms.regression)
}

function setRowRef(id, el) {
  if (!id) return
  if (el) rowRefs.set(id, el)
  else rowRefs.delete(id)
}

watch(
  () => [props.selectedDrawingId, props.activeTab, props.open],
  async () => {
    if (!props.open || props.activeTab !== 'object_tree' || !props.selectedDrawingId) return
    await nextTick()
    const el = rowRefs.get(props.selectedDrawingId)
    if (el && typeof el.scrollIntoView === 'function') {
      el.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'smooth' })
    }
  },
  { immediate: true },
)

watch(
  () => props.channels?.settings,
  (v) => {
    applyDraftSettings(v || {})
  },
  { deep: true, immediate: true },
)

function iconType(row) {
  const t = String(row?.type || '').toLowerCase()
  if (t === 'trendline') return 'trendline'
  if (t === 'hline') return 'hline'
  if (t === 'vline') return 'vline'
  if (t === 'rect') return 'rect'
  if (t === 'text') return 'text'
  return 'default'
}

function lineOrderForDrawing(row) {
  const id = String(row?.id || '')
  if (!id) return null
  return (props.lineOrders || []).find((item) => String(item?.drawing_id || '') === id && String(item?.status || '') === 'armed') || null
}

function canLineOrder(row) {
  const t = String(row?.type || '').toLowerCase()
  return t === 'hline' || t === 'trendline'
}

const filteredChannels = computed(() => {
  let rows = Array.isArray(props.channels?.rows) ? props.channels.rows.slice() : []
  if (filterStatus.value !== 'all') rows = rows.filter((x) => String(x.status || '') === filterStatus.value)
  if (filterMethod.value !== 'all') rows = rows.filter((x) => String(x.method || '') === filterMethod.value)
  rows = rows.filter((x) => Number(x.score || 0) >= Number(scoreMin.value || 0))
  return rows
})

const reversalRows = computed(() => {
  const events = Array.isArray(props.reversal?.results?.events) ? props.reversal.results.events : []
  return events
    .slice()
    .sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0))
    .map((ev, idx) => ({
      id: String(ev?.id || idx),
      direction: String(ev?.direction || ''),
      confirmed: !!ev?.confirmed,
      invalidated: !!ev?.invalidated,
      score: Number(ev?.score || 0),
      p1: Number(ev?.p1?.index ?? -1),
      p2: Number(ev?.p2?.index ?? -1),
      p3: Number(ev?.p3?.index ?? -1),
      label: String(ev?.label || '1-2-3 反转'),
      active: String(props.reversal?.selected_id || '') === String(ev?.id || ''),
    }))
})

const reversalDiagnostics = computed(() => {
  const lines = Array.isArray(props.reversal?.results?.lines) ? props.reversal.results.lines : []
  const events = Array.isArray(props.reversal?.results?.events) ? props.reversal.results.events : []
  const debug = Array.isArray(props.reversal?.results?.debug) ? props.reversal.results.debug : []
  const rows = []
  rows.push(`mid_lines=${lines.length} events=${events.length}`)
  if (!lines.length) rows.push('当前为何无线: 当前可视区未产出中趋势线')
  debug.forEach((x) => rows.push(String(x || '')))
  return rows
})

const armedLineOrderCount = computed(() => (
  (props.lineOrders || []).filter((item) => String(item?.status || '') === 'armed').length
))

function emitSettingsUpdate(force = false) {
  emit('channel-settings', { settings: cloneSettings(draftSettings), force })
}

function onNumberInput(section, key, evt) {
  const v = Number(evt?.target?.value)
  if (!Number.isFinite(v)) return
  draftSettings[section][key] = v
  if (draftSettings.common.liveApply) emitSettingsUpdate(false)
}

function onAlgoNumberInput(algo, key, evt) {
  const v = Number(evt?.target?.value)
  if (!Number.isFinite(v)) return
  draftSettings.algorithms[algo][key] = v
  if (draftSettings.common.liveApply) emitSettingsUpdate(false)
}

function onCommonBoolInput(key, evt) {
  draftSettings.common[key] = !!evt?.target?.checked
  if (draftSettings.common.liveApply || key === 'liveApply') emitSettingsUpdate(false)
}

function onDisplayBoolInput(key, evt) {
  draftSettings.display[key] = !!evt?.target?.checked
  emitSettingsUpdate(false)
}

function onRowToggleChecked(id, evt) {
  const checked = !!evt?.target?.checked
  if (checked) {
    if (!selectedChannelRows.value.includes(id)) selectedChannelRows.value = [...selectedChannelRows.value, id]
  } else {
    selectedChannelRows.value = selectedChannelRows.value.filter((x) => x !== id)
  }
}

function isRowChecked(id) {
  return selectedChannelRows.value.includes(id)
}

function clearSelection() {
  selectedChannelRows.value = []
}

function statusLabel(s) {
  const v = String(s || 'auto')
  if (v === 'accepted') return '已接受'
  if (v === 'edited') return '已编辑'
  if (v === 'rejected') return '已拒绝'
  return '自动'
}

function displayNumber(v, digits = 0) {
  const num = Number(v)
  if (!Number.isFinite(num)) return '-'
  return digits > 0 ? num.toFixed(digits) : String(num)
}

function displayPrice(v) {
  const num = Number(v)
  if (!Number.isFinite(num)) return '-'
  return num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function displaySignedPrice(v) {
  const num = Number(v)
  if (!Number.isFinite(num)) return '-'
  const text = displayPrice(num)
  return num > 0 ? `+${text}` : text
}

function displayPercent(v) {
  const num = Number(v)
  if (!Number.isFinite(num)) return '-'
  const pct = num * 100
  const text = pct.toFixed(2)
  return pct > 0 ? `+${text}%` : `${text}%`
}

function quoteNatureTone(nature, oiDelta) {
  const text = String(nature || '').trim()
  if (['多开', '空开', '双开'].includes(text)) return 'up'
  if (['多平', '空平', '双平', '多换', '空换'].includes(text)) return 'down'
  const delta = Number(oiDelta)
  if (Number.isFinite(delta)) {
    return delta > 0 ? 'up' : 'down'
  }
  return 'neutral'
}

const quoteDisplay = computed(() => {
  const snapshot = props.quoteSnapshot || {}
  const oiDeltaValue = Number(snapshot.oi_delta)
  return {
    askVolume: displayNumber(snapshot.ask_volume1),
    ask: displayPrice(snapshot.ask_price1),
    bidVolume: displayNumber(snapshot.bid_volume1),
    bid: displayPrice(snapshot.bid_price1),
    last: displayPrice(snapshot.latest_price),
    totalVolume: displayNumber(snapshot.volume),
    currentVolume: displayNumber(snapshot.current_volume),
    totalAmount: displayPrice(snapshot.turnover),
    openInterest: displayNumber(snapshot.open_interest),
    oiDelta: displaySignedPrice(snapshot.oi_delta),
    oiDeltaTone: quoteNatureTone('', oiDeltaValue),
    open: displayPrice(snapshot.open),
    high: displayPrice(snapshot.high),
    low: displayPrice(snapshot.low),
    preSettlement: displayPrice(snapshot.pre_settlement_price),
    preClose: displayPrice(snapshot.pre_close_price),
    settlement: displayPrice(snapshot.settlement_price),
    average: displayPrice(snapshot.average_price),
    upperLimit: displayPrice(snapshot.upper_limit_price),
    lowerLimit: displayPrice(snapshot.lower_limit_price),
    change: displaySignedPrice(snapshot.change),
    changePct: displayPercent(snapshot.change_pct),
  }
})

const quoteTicks = computed(() => {
  const rows = Array.isArray(props.quoteTicks) ? props.quoteTicks : []
  return rows.map((row) => ({
    time: row?.time || '-',
    price: displayPrice(row?.price),
    volume: displayNumber(row?.volume),
    oiDelta: displaySignedPrice(row?.oi_delta),
    nature: row?.nature || '-',
    tone: quoteNatureTone(row?.nature, row?.oi_delta),
  }))
})

const runningStrategyInstances = computed(() => (
  (Array.isArray(props.strategy?.instances) ? props.strategy.instances : [])
    .filter((item) => String(item?.status || '') === 'running')
))

const strategyInstances = computed(() => (
  (Array.isArray(props.strategy?.instances) ? props.strategy.instances : [])
    .filter((item) => {
      const currentSymbol = String(props.current || '').trim().toLowerCase()
      const currentTimeframe = String(props.currentTimeframe || '1m').trim().toLowerCase()
      const itemTimeframe = String(item?.timeframe || '').trim().toLowerCase()
      const symbols = Array.isArray(item?.symbols)
        ? item.symbols.map((v) => String(v || '').trim().toLowerCase()).filter(Boolean)
        : []
      if (currentTimeframe && itemTimeframe && itemTimeframe !== currentTimeframe) return false
      if (!currentSymbol || !symbols.length) return true
      return symbols.includes(currentSymbol)
    })
    .slice()
    .sort((a, b) => Date.parse(String(b?.updated_at || b?.created_at || '')) - Date.parse(String(a?.updated_at || a?.created_at || '')))
))

const activeStrategyInstanceId = computed(() => {
  const selected = String(props.selectedStrategyInstanceId || '').trim()
  if (selected && strategyInstances.value.some((item) => String(item?.instance_id || '') === selected)) return selected
  const running = strategyInstances.value.find((item) => String(item?.status || '') === 'running')
  if (running) return String(running.instance_id || '')
  return String(strategyInstances.value[0]?.instance_id || '')
})

const strategyTraceRows = computed(() => {
  const instanceID = activeStrategyInstanceId.value
  const currentSymbol = String(props.current || '').trim().toLowerCase()
  const currentTimeframe = String(props.currentTimeframe || '1m').trim().toLowerCase()
  return (Array.isArray(props.strategy?.traces) ? props.strategy.traces : [])
    .filter((row) => {
      const rowSymbol = String(row?.symbol || '').trim().toLowerCase()
      const rowTimeframe = String(row?.timeframe || '').trim().toLowerCase()
      const rowInstanceID = String(row?.instance_id || '').trim()
      if (currentSymbol && rowSymbol && rowSymbol !== currentSymbol) return false
      if (currentTimeframe && rowTimeframe && rowTimeframe !== currentTimeframe) return false
      if (instanceID && rowInstanceID && rowInstanceID !== instanceID) return false
      return true
    })
    .map((row) => ({
      ...row,
      timeText: formatTraceTime(row?.event_time),
      eventLabel: traceEventLabel(row?.event_type),
      statusLabel: traceStatusLabel(row?.status),
      checks: Array.isArray(row?.checks) ? row.checks : [],
    }))
})

const latestStrategyTrace = computed(() => {
  return strategyTraceRows.value[0] || null
})

const latestStrategyTimeText = computed(() => formatTraceTime(latestStrategyTrace.value?.event_time))

const ma20Steps = [
  { key: 'WAIT_BREAK_BELOW_MA20', label: '等待跌破 MA20', index: 1 },
  { key: 'BROKEN_BELOW_MA20', label: '已跌破，等待反抽触碰 MA20', index: 2 },
  { key: 'WAIT_BREAK_TOUCH_OPEN', label: '等待跌破触碰K开盘价', index: 3 },
  { key: 'SHORT_SIGNAL', label: '做空信号', index: 4 },
  { key: 'SIGNAL_ACTIVE', label: '信号观察中', index: 5 },
  { key: 'SIGNAL_RESULT', label: '信号结果', index: 5 },
]

const ma20WeakSteps = [
  { key: 'WAIT_BREAK_BELOW_MA20', label: '等待跌破 MA20', index: 1 },
  { key: 'TREND_STRUCTURE_FILTER', label: '趋势/结构过滤', index: 2 },
  { key: 'WAIT_PULLBACK_TOUCH_MA20', label: '等待弱反触碰 MA20', index: 3 },
  { key: 'WAIT_BREAK_REACTION_LOW', label: '等待跌破反抽低点', index: 4 },
  { key: 'SHORT_SIGNAL', label: '做空信号', index: 5 },
  { key: 'SIGNAL_ACTIVE', label: '信号观察中', index: 6 },
  { key: 'SIGNAL_RESULT', label: '信号结果', index: 6 },
]

const strategyStepRows = computed(() => {
  const trace = latestStrategyTrace.value || {}
  const currentIndex = Number(trace.step_index || 0)
  const currentKey = String(trace.step_key || '')
  const activeInstance = strategyInstances.value.find((item) => String(item?.instance_id || '') === activeStrategyInstanceId.value) || runningStrategyInstances.value[0] || null
  const strategyID = String(trace.strategy_id || activeInstance?.strategy_id || '')
  let base = [{ key: currentKey || 'CURRENT', label: trace.step_label || currentKey || '当前步骤', index: currentIndex || 1 }]
  if (strategyID === 'ma20.weak_pullback_short' || strategyID.startsWith('ma20.weak_pullback_short.')) base = ma20WeakSteps
  else if (strategyID === 'ma20.pullback_short' || !strategyID) base = ma20Steps
  return base.map((step) => {
    let state = 'waiting'
    if (currentIndex > step.index || String(trace.status || '') === 'done') state = 'done'
    else if (currentIndex === step.index || currentKey === step.key) state = String(trace.status || 'waiting')
    return { ...step, state, active: currentIndex === step.index || currentKey === step.key }
  })
})

const strategyBacktestRows = computed(() => (
  (Array.isArray(props.strategy?.backtests) ? props.strategy.backtests : [])
    .filter((row) => String(row?.strategy_id || '').startsWith('ma20.weak_pullback_short'))
    .slice(0, 6)
))

function formatTraceTime(value) {
  const ts = Date.parse(String(value || ''))
  if (!Number.isFinite(ts)) return '--'
  return formatHms(ts)
}

function formatHms(ts) {
  const d = new Date(ts)
  const p = (v) => String(v).padStart(2, '0')
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

function formatInstanceAnchor(item) {
  const params = item?.params && typeof item.params === 'object' ? item.params : {}
  return String(params.chart_start_time || params.chart_anchor?.data_time || params.chart_anchor?.plot_time || '--')
}

function strategyInstanceName(item) {
  const base = String(item?.display_name || item?.instance_id || '').trim()
  const timeframe = String(item?.timeframe || '').trim()
  if (!timeframe) return base || '--'
  return base.includes(`-${timeframe}`) || base.includes(`· ${timeframe}`) ? base : `${base} · ${timeframe}`
}

function instanceStatusLabel(value) {
  const v = String(value || '')
  if (v === 'running') return '运行中'
  if (v === 'stopped') return '已停止'
  if (v === 'error') return '错误'
  return v || '--'
}

function traceEventLabel(value) {
  const v = String(value || '')
  if (v === 'bar') return 'K线'
  if (v === 'key_tick') return '关键Tick'
  if (v === 'signal') return '信号'
  if (v === 'order_plan') return '计划'
  if (v === 'order_result') return '订单'
  return v || '--'
}

function traceStatusLabel(value) {
  const v = String(value || '')
  if (v === 'passed' || v === 'allowed' || v === 'simulated_submitted') return '满足'
  if (v === 'failed' || v === 'blocked') return '阻断'
  if (v === 'done') return '完成'
  if (v === 'waiting') return '等待'
  return v || '--'
}

function stepStateLabel(value) {
  const v = String(value || '')
  if (v === 'done') return '完成'
  if (v === 'passed' || v === 'allowed' || v === 'simulated_submitted') return '满足'
  if (v === 'failed' || v === 'blocked') return '阻断'
  return '等待'
}

function formatBacktestSummary(summary) {
  const stats = summary && typeof summary === 'object' ? summary.stats || {} : {}
  const rows = Object.entries(stats)
  if (!rows.length) return '--'
  return rows.map(([algo, item]) => {
    const success = Number(item?.success || 0)
    const signals = Number(item?.signals || 0)
    const signalRate = Number(item?.signal_success_rate || 0)
    const formationRate = Number(item?.signal_formation_rate || 0)
    const rateText = Number.isFinite(signalRate) ? `${(signalRate * 100).toFixed(1)}%` : '--'
    const formationText = Number.isFinite(formationRate) ? `${(formationRate * 100).toFixed(1)}%` : '--'
    return `${algo}: ${success}/${signals} 成功率${rateText} 成形${formationText}`
  }).join(' | ')
}
</script>

<template>
  <aside class="tv-watchlist" :class="{ collapsed: !props.open }">
    <div v-if="props.open && props.activeTab === 'quote'" class="tv-watchlist-body tv-quote-tab">
      <div class="tv-quote-card">
        <div class="tv-quote-strip">
          <span class="tv-quote-strip-label">卖出</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.ask }}</span>
          <span class="tv-quote-strip-label">卖量</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.askVolume }}</span>
        </div>
        <div class="tv-quote-strip">
          <span class="tv-quote-strip-label">买入</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.bid }}</span>
          <span class="tv-quote-strip-label">买量</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.bidVolume }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">现价</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.last }}</span>
          <span class="tv-quote-strip-label">总量</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.totalVolume }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">现量</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.currentVolume }}</span>
          <span class="tv-quote-strip-label">总额</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.totalAmount }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">持仓</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.openInterest }}</span>
          <span class="tv-quote-strip-label">仓差</span>
          <span class="tv-quote-strip-value" :class="`is-${quoteDisplay.oiDeltaTone}`">{{ quoteDisplay.oiDelta }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">今开</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.open }}</span>
          <span class="tv-quote-strip-label">最高</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.high }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">最低</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.low }}</span>
          <span class="tv-quote-strip-label">昨结</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.preSettlement }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">昨收</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.preClose }}</span>
          <span class="tv-quote-strip-label">结算</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.settlement }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">均价</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.average }}</span>
          <span class="tv-quote-strip-label">涨跌</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.change }}</span>
        </div>
        <div class="tv-quote-strip tv-quote-strip-split">
          <span class="tv-quote-strip-label">涨跌幅</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.changePct }}</span>
          <span class="tv-quote-strip-label">涨停</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.upperLimit }}</span>
          <span class="tv-quote-strip-label">跌停</span>
          <span class="tv-quote-strip-value">{{ quoteDisplay.lowerLimit }}</span>
        </div>
      </div>

      <div class="tv-quote-card">
        <div class="tv-quote-tick-head">
          <span>时间</span>
          <span>价格</span>
          <span>现量</span>
          <span>仓差</span>
          <span>性质</span>
        </div>
        <div v-if="quoteTicks.length" class="tv-quote-tick-list">
          <div v-for="(row, idx) in quoteTicks" :key="`${row.time}-${idx}`" class="tv-quote-tick-row">
            <span>{{ row.time }}</span>
            <span>{{ row.price }}</span>
            <span>{{ row.volume }}</span>
            <span :class="`is-${row.tone}`">{{ row.oiDelta }}</span>
            <span :class="`is-${row.tone}`">{{ row.nature }}</span>
          </div>
        </div>
        <div v-else class="tv-object-empty">暂无 tick 明细</div>
      </div>
    </div>

    <div v-else-if="props.open && props.activeTab === 'watchlist'" class="tv-watchlist-body">
      <button
        v-for="item in props.items"
        :key="`${item.type}-${item.symbol}`"
        class="tv-watch-item"
        :class="{ active: item.symbol === props.current }"
        @click="emit('select', item)"
      >
        <span>{{ item.symbol }}</span>
        <small>{{ item.exchange }}</small>
      </button>
    </div>

    <div v-else-if="props.open && props.activeTab === 'strategy'" class="tv-watchlist-body tv-strategy-tab">
      <div class="tv-quote-card">
        <div class="tv-channel-settings-head">
          <span>策略运行</span>
          <button type="button" class="tv-strategy-run-btn" @click.stop="emit('strategy-run-click', $event)">运行策略</button>
        </div>
        <div class="tv-quote-strip">
          <span class="tv-quote-strip-label">连接</span>
          <span class="tv-quote-strip-value">{{ props.strategy?.status?.connected ? '已连接' : '未连接' }}</span>
          <span class="tv-quote-strip-label">运行</span>
          <span class="tv-quote-strip-value">{{ runningStrategyInstances.length }}</span>
          <span class="tv-quote-strip-label">最近</span>
          <span class="tv-quote-strip-value">{{ latestStrategyTimeText }}</span>
        </div>
        <div v-if="latestStrategyTrace" class="tv-strategy-current">
          <div class="tv-strategy-step-head">
            <span>{{ latestStrategyTrace.step_label || '--' }}</span>
            <span>{{ latestStrategyTrace.step_index || 0 }}/{{ latestStrategyTrace.step_total || 0 }}</span>
          </div>
          <div class="tv-strategy-progress">
            <div
              class="tv-strategy-progress-fill"
              :style="{ width: `${Math.max(0, Math.min(100, Number(latestStrategyTrace.step_index || 0) / Math.max(1, Number(latestStrategyTrace.step_total || 1)) * 100))}%` }"
            ></div>
          </div>
          <div class="mono">{{ latestStrategyTrace.event_type }} · {{ latestStrategyTrace.reason || '--' }}</div>
          <div class="tv-strategy-checks">
            <div v-for="(check, idx) in latestStrategyTrace.checks || []" :key="`latest-check-${idx}`" class="tv-strategy-check" :class="{ passed: check.passed }">
              <span>{{ check.passed ? '✓' : '○' }}</span>
              <span>{{ check.name }}</span>
              <small>{{ check.current ?? '-' }} / {{ check.target ?? '-' }} <template v-if="check.delta !== undefined">差 {{ check.delta }}</template></small>
              <em v-if="check.description">{{ check.description }}</em>
            </div>
          </div>
        </div>
        <div v-else class="tv-object-empty">暂无策略过程事件</div>
      </div>

      <div class="tv-quote-card">
        <div class="tv-channel-settings-head">策略步骤</div>
        <div class="tv-strategy-step-list">
          <div
            v-for="step in strategyStepRows"
            :key="step.key"
            class="tv-strategy-step-row"
            :class="{ active: step.active, done: step.state === 'done' }"
          >
            <span class="tv-strategy-step-index">{{ step.index }}</span>
            <span class="tv-strategy-step-name">{{ step.label }}</span>
            <span class="tv-strategy-step-state">{{ stepStateLabel(step.state) }}</span>
          </div>
        </div>
      </div>

      <div class="tv-quote-card">
        <div class="tv-channel-settings-head">运行实例</div>
        <div class="tv-strategy-instance-list">
          <div
            v-for="item in strategyInstances"
            :key="item.instance_id"
            class="tv-strategy-instance-row"
            :class="{ active: String(item.instance_id || '') === activeStrategyInstanceId }"
            @click="emit('strategy-instance-select', item.instance_id)"
          >
            <div class="tv-strategy-instance-main">
              <strong>{{ strategyInstanceName(item) }}</strong>
              <small>{{ item.strategy_id }} · {{ (item.symbols || []).join(',') || '--' }} · {{ formatInstanceAnchor(item) }}</small>
            </div>
            <span class="tv-strategy-instance-status">{{ instanceStatusLabel(item.status) }}</span>
            <button
              v-if="String(item.status || '') === 'running'"
              type="button"
              class="tv-strategy-stop-btn"
              @click.stop
              @click="emit('strategy-instance-stop', item.instance_id)"
            >
              停止
            </button>
          </div>
        </div>
        <div v-if="!strategyInstances.length" class="tv-object-empty">暂无策略实例</div>
      </div>

      <div class="tv-quote-card">
        <div class="tv-channel-settings-head">回放报告</div>
        <div class="tv-strategy-backtest-list">
          <div v-for="item in strategyBacktestRows" :key="item.run_id" class="tv-strategy-backtest-row">
            <strong>{{ item.symbol || 'all' }} · {{ item.timeframe || '--' }}</strong>
            <small>{{ formatBacktestSummary(item.summary) }}</small>
          </div>
        </div>
        <div v-if="!strategyBacktestRows.length" class="tv-object-empty">暂无回放报告</div>
      </div>

      <div class="tv-quote-card">
        <div class="tv-channel-settings-head">审计流</div>
        <div class="tv-strategy-trace-list">
          <button
            v-for="row in strategyTraceRows"
            :key="row.trace_id || `${row.instance_id}-${row.event_time}-${row.event_type}`"
            class="tv-strategy-trace-row"
            @click="emit('strategy-trace-focus', row)"
          >
            <span class="tv-strategy-trace-time">{{ row.timeText }}</span>
            <span class="tv-strategy-trace-event">{{ row.eventLabel }}</span>
            <span class="tv-strategy-trace-status">{{ row.statusLabel }}</span>
            <span class="tv-strategy-trace-step">{{ row.step_label || row.step_key || '--' }}</span>
          </button>
        </div>
        <div v-if="!strategyTraceRows.length" class="tv-object-empty">暂无审计记录</div>
      </div>
    </div>

    <div v-else-if="props.open && props.activeTab === 'object_tree'" class="tv-watchlist-body tv-object-tree">
      <div v-if="armedLineOrderCount > 0" class="tv-object-toolbar">
        <span>画线下单 {{ armedLineOrderCount }}</span>
        <button type="button" @click="emit('stop-line-orders')">全部停止</button>
      </div>
      <div
        v-for="row in props.drawings"
        :key="row.id"
        class="tv-object-row"
        :class="{ active: row.id === props.selectedDrawingId, hidden: row.visible === false }"
        :ref="(el) => setRowRef(row.id, el)"
        @pointerenter="hoverRowId = row.id"
        @pointerleave="hoverRowId = ''"
        @click="emit('select-drawing', row.id)"
      >
        <div class="tv-object-main">
          <span class="tv-object-title">
            <span class="tv-object-icon" :class="`kind-${iconType(row)}`" aria-hidden="true">
              <svg v-if="iconType(row) === 'trendline'" viewBox="0 0 24 24"><circle cx="6" cy="17" r="1.8" /><circle cx="18" cy="7" r="1.8" /><line x1="7.5" y1="15.5" x2="16.5" y2="8.5" /></svg>
              <svg v-else-if="iconType(row) === 'hline'" viewBox="0 0 24 24"><line x1="4" y1="12" x2="20" y2="12" /></svg>
              <svg v-else-if="iconType(row) === 'vline'" viewBox="0 0 24 24"><line x1="12" y1="4" x2="12" y2="20" /></svg>
              <svg v-else-if="iconType(row) === 'rect'" viewBox="0 0 24 24"><rect x="5" y="6" width="14" height="12" /></svg>
              <svg v-else-if="iconType(row) === 'text'" viewBox="0 0 24 24"><line x1="6" y1="6" x2="18" y2="6" /><line x1="12" y1="6" x2="12" y2="18" /></svg>
              <svg v-else viewBox="0 0 24 24"><circle cx="12" cy="12" r="6" /></svg>
            </span>
            {{ row.typeLabel }}
          </span>
          <span class="tv-object-sub">已修改 {{ row.updatedAtLabel }}</span>
        </div>
        <div v-if="hoverRowId === row.id" class="tv-object-actions">
          <button
            v-if="canLineOrder(row) && !lineOrderForDrawing(row)"
            class="tv-icon-btn"
            title="画线下单"
            @click.stop="emit('arm-line-order', row.id)"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 16h16" /><path d="M8 8h8" /><path d="M12 4v16" /></svg>
          </button>
          <button
            v-else-if="canLineOrder(row)"
            class="tv-icon-btn active"
            title="停止画线下单"
            @click.stop="emit('disable-line-order', lineOrderForDrawing(row)?.id)"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5l14 14" /><path d="M4 16h16" /></svg>
          </button>
          <button class="tv-icon-btn" :title="row.visible === false ? '显示' : '隐藏'" @click.stop="emit('toggle-drawing-visible', row.id)">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z" /><circle cx="12" cy="12" r="3.1" /><line v-if="row.visible === false" x1="4" y1="20" x2="20" y2="4" /></svg>
          </button>
          <button class="tv-icon-btn" title="移除" @click.stop="emit('delete-drawing', row.id)">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16" /><path d="M9 7V5h6v2" /><path d="M7 7l1 12h8l1-12" /><line x1="10" y1="10" x2="10" y2="17" /><line x1="14" y1="10" x2="14" y2="17" /></svg>
          </button>
        </div>
      </div>
      <div v-if="!props.drawings.length" class="tv-object-empty">暂无对象</div>
    </div>

    <div v-else-if="!props.lightweightOnly && props.open && props.activeTab === 'channel'" class="tv-watchlist-body tv-channel-tab">
      <details class="tv-channel-help" open>
        <summary>参数说明与操作语义</summary>
        <div class="tv-channel-help-content">
          <div class="help-item"><b>通用参数</b>：window/步长/inside/minTouches/NMS 等对三种算法共用。</div>
          <div class="help-item"><b>极值通道</b>：按低点与次低点/高点与次高点连线，再平行复制对侧边界。</div>
          <div class="help-item"><b>RANSAC 通道</b>：在枢轴点上做鲁棒拟合，适合离群较多场景。</div>
          <div class="help-item"><b>Regression 通道</b>：对收盘中心线回归并估计包络宽度，响应更平滑。</div>
          <div class="help-item"><b>算法勾选框</b>：控制“参与检测+显示”，未勾选即不计算也不展示。</div>
          <div class="help-item"><b>liveApply</b>：关闭后参数先暂存，点击“应用并重算”生效。</div>
        </div>
      </details>

      <div class="tv-channel-settings">
        <div class="tv-channel-settings-head">通用参数</div>
        <div class="tv-channel-grid">
          <label>1m窗<input type="number" :value="draftSettings.common.windowSizeMinute" @change="onNumberInput('common', 'windowSizeMinute', $event)" /></label>
          <label>1h窗<input type="number" :value="draftSettings.common.windowSizeHour" @change="onNumberInput('common', 'windowSizeHour', $event)" /></label>
          <label>1d窗<input type="number" :value="draftSettings.common.windowSizeDay" @change="onNumberInput('common', 'windowSizeDay', $event)" /></label>
          <label>步长因子<input type="number" :value="draftSettings.common.stepDivisor" @change="onNumberInput('common', 'stepDivisor', $event)" /></label>
          <label>inside最小<input type="number" step="0.01" :value="draftSettings.common.insideRatioMin" @change="onNumberInput('common', 'insideRatioMin', $event)" /></label>
          <label>触碰最小<input type="number" :value="draftSettings.common.minTouches" @change="onNumberInput('common', 'minTouches', $event)" /></label>
          <label>NMS IoU<input type="number" step="0.01" :value="draftSettings.common.nmsIoU" @change="onNumberInput('common', 'nmsIoU', $event)" /></label>
          <label>NMS斜率<input type="number" step="0.1" :value="draftSettings.common.nmsSlopeFactor" @change="onNumberInput('common', 'nmsSlopeFactor', $event)" /></label>
          <label>最多段<input type="number" :value="draftSettings.common.maxSegments" @change="onNumberInput('common', 'maxSegments', $event)" /></label>
          <label>自动显示N<input type="number" :value="draftSettings.common.showTopAutoN" @change="onNumberInput('common', 'showTopAutoN', $event)" /></label>
        </div>
        <div class="tv-channel-bools">
          <label><input type="checkbox" :checked="draftSettings.common.liveApply" @change="onCommonBoolInput('liveApply', $event)" />实时应用</label>
          <label><input type="checkbox" :checked="draftSettings.common.hideAuto" @change="onCommonBoolInput('hideAuto', $event)" />隐藏自动候选</label>
          <label><input type="checkbox" :checked="draftSettings.common.showLabels" @change="onCommonBoolInput('showLabels', $event)" />显示标签</label>
        </div>
      </div>

      <details class="tv-channel-help" open>
        <summary><label><input type="checkbox" :checked="draftSettings.display.showExtrema" @change="onDisplayBoolInput('showExtrema', $event)" /> 极值通道</label></summary>
        <div class="tv-channel-grid">
          <label>pivot_m<input type="number" :value="draftSettings.algorithms.extrema.pivotKMinute" @change="onAlgoNumberInput('extrema', 'pivotKMinute', $event)" /></label>
          <label>pivot_h<input type="number" :value="draftSettings.algorithms.extrema.pivotKHour" @change="onAlgoNumberInput('extrema', 'pivotKHour', $event)" /></label>
          <label>pivot_d<input type="number" :value="draftSettings.algorithms.extrema.pivotKDay" @change="onAlgoNumberInput('extrema', 'pivotKDay', $event)" /></label>
          <label>最小跨度<input type="number" :value="draftSettings.algorithms.extrema.minPairSpanBars" @change="onAlgoNumberInput('extrema', 'minPairSpanBars', $event)" /></label>
          <label>最大跨度<input type="number" :value="draftSettings.algorithms.extrema.maxPairSpanBars" @change="onAlgoNumberInput('extrema', 'maxPairSpanBars', $event)" /></label>
          <label>次点ATR<input type="number" step="0.01" :value="draftSettings.algorithms.extrema.secondPointAtrFactor" @change="onAlgoNumberInput('extrema', 'secondPointAtrFactor', $event)" /></label>
          <label>突破容忍ATR<input type="number" step="0.01" :value="draftSettings.algorithms.extrema.breakToleranceAtrFactor" @change="onAlgoNumberInput('extrema', 'breakToleranceAtrFactor', $event)" /></label>
        </div>
      </details>

      <details class="tv-channel-help" open>
        <summary><label><input type="checkbox" :checked="draftSettings.display.showRansac" @change="onDisplayBoolInput('showRansac', $event)" /> RANSAC 通道</label></summary>
        <div class="tv-channel-grid">
          <label>pivot_m<input type="number" :value="draftSettings.algorithms.ransac.pivotKMinute" @change="onAlgoNumberInput('ransac', 'pivotKMinute', $event)" /></label>
          <label>pivot_h<input type="number" :value="draftSettings.algorithms.ransac.pivotKHour" @change="onAlgoNumberInput('ransac', 'pivotKHour', $event)" /></label>
          <label>pivot_d<input type="number" :value="draftSettings.algorithms.ransac.pivotKDay" @change="onAlgoNumberInput('ransac', 'pivotKDay', $event)" /></label>
          <label>残差ATR<input type="number" step="0.01" :value="draftSettings.algorithms.ransac.residualAtrFactor" @change="onAlgoNumberInput('ransac', 'residualAtrFactor', $event)" /></label>
          <label>斜率容差<input type="number" step="0.01" :value="draftSettings.algorithms.ransac.slopeTolAtrFactor" @change="onAlgoNumberInput('ransac', 'slopeTolAtrFactor', $event)" /></label>
        </div>
      </details>

      <details class="tv-channel-help" open>
        <summary><label><input type="checkbox" :checked="draftSettings.display.showRegression" @change="onDisplayBoolInput('showRegression', $event)" /> Regression 通道</label></summary>
        <div class="tv-channel-grid">
          <label>残差ATR<input type="number" step="0.01" :value="draftSettings.algorithms.regression.residualAtrFactor" @change="onAlgoNumberInput('regression', 'residualAtrFactor', $event)" /></label>
        </div>
      </details>

      <div class="tv-channel-actions-row">
        <button class="tv-btn" @click="emitSettingsUpdate(true)">应用并重算</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'reset-settings' })">恢复默认</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'recalc' })">重算</button>
      </div>
      <div class="tv-channel-actions-row">
        <button class="tv-btn" @click="emit('channel-action', { type: 'load-sample', sample: 'parallel' })">平行样本</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'load-sample', sample: 'outlier' })">离群样本</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'load-sample', sample: 'wedge' })">楔形样本</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'restore-live' })">恢复实盘</button>
      </div>

      <div class="tv-channel-filter">
        <label>状态
          <select v-model="filterStatus"><option value="all">全部</option><option value="auto">自动</option><option value="accepted">已接受</option><option value="edited">已编辑</option><option value="rejected">已拒绝</option></select>
        </label>
        <label>方法
          <select v-model="filterMethod"><option value="all">全部</option><option value="extrema">Extrema</option><option value="ransac">RANSAC</option><option value="regression">Regression</option></select>
        </label>
        <label>score>=<input type="number" step="0.01" v-model.number="scoreMin" /></label>
      </div>

      <div class="tv-channel-actions-row">
        <button class="tv-btn" @click="emit('channel-action', { type: 'accept-selected', ids: selectedChannelRows })">接受所选</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'reject-selected', ids: selectedChannelRows })">拒绝所选</button>
        <button class="tv-btn" @click="emit('channel-action', { type: 'clear-rejected' })">清空拒绝</button>
        <button class="tv-btn" @click="clearSelection">清空勾选</button>
      </div>

      <div class="tv-channel-list">
        <div v-for="row in filteredChannels" :key="row.id" class="tv-channel-row" :class="[`status-${row.status}`, { active: row.id === props.channels?.selected_id }]">
          <div class="tv-channel-row-main" @click="emit('channel-action', { type: 'select', id: row.id })">
            <input type="checkbox" :checked="isRowChecked(row.id)" @change="onRowToggleChecked(row.id, $event)" @click.stop />
            <span class="mono">{{ statusLabel(row.status) }}</span>
            <span class="mono">{{ row.methodLabel || row.method || '-' }}</span>
            <span class="mono">S {{ Number(row.score || 0).toFixed(3) }}</span>
            <span class="mono">I {{ Number(row.insideRatio || 0).toFixed(3) }}</span>
            <span class="mono">T {{ Number(row.touchCount || 0) }}</span>
            <span class="mono">K {{ Number(row.slope || 0).toFixed(4) }}</span>
          </div>
          <div class="tv-channel-row-actions">
            <button class="tv-icon-btn" title="接受" @click.stop="emit('channel-action', { type: 'accept', id: row.id })">A</button>
            <button class="tv-icon-btn" title="拒绝" @click.stop="emit('channel-action', { type: 'reject', id: row.id })">R</button>
            <button class="tv-icon-btn" title="恢复自动" @click.stop="emit('channel-action', { type: 'restore', id: row.id })">&#8634;</button>
            <button class="tv-icon-btn" title="锁定" @click.stop="emit('channel-action', { type: 'toggle-lock', id: row.id })">L</button>
            <button class="tv-icon-btn" title="定位" @click.stop="emit('channel-action', { type: 'focus', id: row.id })">◎</button>
          </div>
        </div>
      </div>

      <div v-if="props.channels?.detail" class="tv-channel-detail">
        <div class="tv-channel-settings-head">调整细节</div>
        <div class="mono">ID: {{ props.channels.detail.id }}</div>
        <div class="mono">来源: {{ props.channels.detail.source || '-' }}</div>
        <div class="mono">操作人: {{ props.channels.detail.operator || 'admin' }}</div>
        <div v-if="props.channels.detail.selected" class="mono">状态: {{ props.channels.detail.selected.status }} | 方法: {{ props.channels.detail.selected.methodLabel || props.channels.detail.selected.method }} | 质量: {{ props.channels.detail.selected.qualityTier }}</div>
        <div v-if="props.channels.detail.selected" class="mono">起始时间: {{ props.channels.detail.selected.startTimeText }} ({{ props.channels.detail.selected.startTime }})</div>
        <div v-if="props.channels.detail.selected" class="mono">结束时间: {{ props.channels.detail.selected.endTimeText }} ({{ props.channels.detail.selected.endTime }})</div>
        <div v-if="props.channels.detail.selected" class="mono">上轨: {{ Number(props.channels.detail.selected.upperStart || 0).toFixed(3) }} -> {{ Number(props.channels.detail.selected.upperEnd || 0).toFixed(3) }}</div>
        <div v-if="props.channels.detail.selected" class="mono">下轨: {{ Number(props.channels.detail.selected.lowerStart || 0).toFixed(3) }} -> {{ Number(props.channels.detail.selected.lowerEnd || 0).toFixed(3) }}</div>
        <div v-if="props.channels.detail.selected" class="mono">分数: {{ Number(props.channels.detail.selected.score || 0).toFixed(3) }} | inside: {{ Number(props.channels.detail.selected.insideRatio || 0).toFixed(3) }} | touches: {{ Number(props.channels.detail.selected.touchCount || 0) }}</div>
        <div class="mono">斜率: {{ Number(props.channels.detail.before?.slope || 0).toFixed(5) }} -> {{ Number(props.channels.detail.after?.slope || 0).toFixed(5) }}</div>
        <div class="mono">宽度: {{ Number(props.channels.detail.before?.widthMean || 0).toFixed(3) }} -> {{ Number(props.channels.detail.after?.widthMean || 0).toFixed(3) }}</div>
        <div class="mono">时间: {{ props.channels.detail.updated_at || '-' }}</div>
      </div>
    </div>

    <div v-else-if="!props.lightweightOnly && props.open" class="tv-watchlist-body tv-channel-tab">
      <div class="tv-channel-settings-head">1-2-3 反转信号</div>
      <div class="tv-channel-actions-row">
        <button class="tv-btn" @click="emit('reversal-action', { type: 'recalc' })">重算</button>
      </div>
      <div class="tv-reversal-diagnostics">
        <div class="tv-reversal-diagnostics-title">当前为何无线</div>
        <div v-for="(row, idx) in reversalDiagnostics" :key="`rev-diag-${idx}`" class="mono tv-reversal-diagnostics-row">
          {{ row }}
        </div>
      </div>
      <div class="tv-channel-list">
        <div
          v-for="row in reversalRows"
          :key="row.id"
          class="tv-channel-row"
          :class="{ active: row.active }"
        >
          <div class="tv-channel-row-main" @click="emit('reversal-action', { type: 'select', id: row.id })">
            <div class="tv-channel-row-top">
              <span class="mono">{{ row.direction }}</span>
              <span class="mono">S {{ row.score.toFixed(3) }}</span>
            </div>
            <div class="mono">P1 {{ row.p1 }} · P2 {{ row.p2 }} · P3 {{ row.p3 }}</div>
            <div class="mono">确认 {{ row.confirmed ? 'Y' : 'N' }} · 失效 {{ row.invalidated ? 'Y' : 'N' }}</div>
            <div class="mono">{{ row.label }}</div>
          </div>
          <div class="tv-channel-row-actions">
            <button class="tv-icon-btn" title="定位" @click.stop="emit('reversal-action', { type: 'focus', id: row.id })">◎</button>
          </div>
        </div>
      </div>
      <div v-if="!reversalRows.length" class="tv-object-empty">暂无反转信号</div>
    </div>
  </aside>
</template>

<style scoped>
.is-up {
  color: #d84b45;
}

.is-down {
  color: #2f9e5b;
}

.is-neutral {
  color: inherit;
}

.tv-object-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 6px 8px;
  border-bottom: 1px solid rgba(130, 156, 188, 0.26);
  font-size: 12px;
}

.tv-object-toolbar button {
  min-height: 24px;
  padding: 0 8px;
  border: 1px solid rgba(130, 156, 188, 0.45);
  background: transparent;
  color: inherit;
  cursor: pointer;
}

.tv-strategy-tab {
  gap: 8px;
}

.tv-strategy-current {
  display: grid;
  gap: 8px;
  padding-top: 6px;
}

.tv-strategy-step-head {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 12px;
  font-weight: 600;
}

.tv-strategy-progress {
  height: 6px;
  overflow: hidden;
  background: rgba(130, 156, 188, 0.24);
}

.tv-strategy-progress-fill {
  height: 100%;
  background: #4ea1ff;
}

.tv-strategy-checks {
  display: grid;
  gap: 4px;
}

.tv-strategy-check {
  display: grid;
  grid-template-columns: 16px minmax(0, 1fr) auto;
  gap: 6px;
  align-items: center;
  font-size: 12px;
  color: rgba(210, 223, 238, 0.72);
}

.tv-strategy-check em {
  grid-column: 2 / 4;
  font-style: normal;
  color: rgba(210, 223, 238, 0.56);
}

.tv-strategy-check.passed {
  color: #70d38b;
}

.tv-strategy-step-list,
.tv-strategy-instance-list {
  display: grid;
  gap: 4px;
}

.tv-strategy-backtest-list {
  display: grid;
  gap: 4px;
}

.tv-strategy-step-row {
  display: grid;
  grid-template-columns: 22px minmax(0, 1fr) 42px;
  align-items: center;
  gap: 6px;
  padding: 6px 4px;
  border-bottom: 1px solid rgba(130, 156, 188, 0.14);
  font-size: 12px;
  color: rgba(210, 223, 238, 0.72);
}

.tv-strategy-step-row.active {
  background: rgba(78, 161, 255, 0.12);
  color: #d8e6f7;
}

.tv-strategy-step-row.done {
  color: #70d38b;
}

.tv-strategy-step-index {
  display: inline-grid;
  place-items: center;
  width: 18px;
  height: 18px;
  border: 1px solid rgba(130, 156, 188, 0.36);
  font-size: 11px;
}

.tv-strategy-step-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tv-strategy-step-state {
  text-align: right;
}

.tv-strategy-instance-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto auto;
  align-items: center;
  gap: 8px;
  padding: 7px 4px;
  border-bottom: 1px solid rgba(130, 156, 188, 0.14);
  font-size: 12px;
  cursor: pointer;
}

.tv-strategy-instance-row.active {
  background: rgba(78, 161, 255, 0.12);
}

.tv-strategy-backtest-row {
  display: grid;
  gap: 3px;
  padding: 7px 4px;
  border-bottom: 1px solid rgba(130, 156, 188, 0.14);
  font-size: 12px;
}

.tv-strategy-backtest-row strong,
.tv-strategy-backtest-row small {
  overflow: hidden;
  text-overflow: ellipsis;
}

.tv-strategy-backtest-row small {
  color: rgba(210, 223, 238, 0.62);
  line-height: 1.35;
}

.tv-strategy-instance-main {
  display: grid;
  gap: 2px;
  min-width: 0;
}

.tv-strategy-instance-main strong,
.tv-strategy-instance-main small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tv-strategy-instance-main small,
.tv-strategy-instance-status {
  color: rgba(210, 223, 238, 0.62);
}

.tv-strategy-stop-btn {
  min-height: 24px;
  padding: 0 8px;
  border: 1px solid rgba(216, 75, 69, 0.55);
  background: transparent;
  color: #ff7a73;
  cursor: pointer;
}

.tv-strategy-run-btn {
  min-height: 24px;
  padding: 0 8px;
  border: 1px solid rgba(56, 189, 248, 0.45);
  background: rgba(14, 116, 144, 0.24);
  color: #e0f2fe;
  cursor: pointer;
}

.tv-strategy-trace-list {
  display: grid;
  gap: 4px;
}

.tv-strategy-trace-row {
  display: grid;
  grid-template-columns: 48px 58px 42px minmax(0, 1fr);
  gap: 6px;
  align-items: center;
  width: 100%;
  padding: 6px 4px;
  border: 0;
  border-bottom: 1px solid rgba(130, 156, 188, 0.16);
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
  font-size: 12px;
}

.tv-strategy-trace-row:hover {
  background: rgba(78, 161, 255, 0.08);
}

.tv-strategy-trace-time,
.tv-strategy-trace-event,
.tv-strategy-trace-status {
  white-space: nowrap;
  color: rgba(210, 223, 238, 0.72);
}

.tv-strategy-trace-step {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
