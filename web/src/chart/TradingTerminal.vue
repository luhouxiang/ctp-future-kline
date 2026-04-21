<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import TopToolbar from './TopToolbar.vue'
import LeftDrawToolbar from './LeftDrawToolbar.vue'
import WatchlistPanel from './WatchlistPanel.vue'
import PriceChartPane from './PriceChartPane.vue'
import KeyboardSprite from './KeyboardSprite.vue'
import TradeDockWindow from './TradeDockWindow.vue'
import { bootKlineComposerWASM } from './klineComposeBridge'
import { DEFAULT_CHANNEL_SETTINGS, normalizeChannelSettingsV2 } from './analysis/channelDetector'
import { DEFAULT_REVERSAL_SETTINGS, normalizeReversalSettings } from './analysis/reversalDetector'

const paneRef = ref(null)
const owner = ref('admin')
const lightweightOnly = true

const scope = reactive({
  symbol: '',
  type: '',
  variety: '',
  timeframe: '1m',
  end: '',
})

const layout = reactive({
  theme: 'dark',
  panes: {
    right_watchlist_open: true,
    bottom_panel_open: false,
  },
  indicators: {
    ma20: true,
    macd: true,
    volume: true,
  },
})

const drawings = ref([])
const watchlist = ref([])
const activeRightTab = ref('quote')
const quoteSnapshot = ref({})
const quoteTicks = ref([])
const selectedDrawingId = ref('')
const saveStatus = ref('idle')
const activeTool = ref('cursor')
const channelDebug = ref(false)
const channelState = reactive({
  settings: {},
  decisions: [],
  selected_id: '',
  rows: [],
  detail: null,
  persistVersion: 0,
})
const reversalState = reactive({
  settings: normalizeReversalSettings(DEFAULT_REVERSAL_SETTINGS),
  results: { lines: [], events: [] },
  persistVersion: 0,
  selected_id: '',
})
const watchlistWidth = ref(340)
const minWatchlistWidth = 180
const maxWatchlistWidth = 520
let resizeMeta = null
let saveTimer = null
let beforeUnloadHandler = null
let spriteQueryTimer = null
let spriteKeyHandler = null
let chartWS = null
let chartWSReconnectTimer = null
let chartWSReconnectAttempt = 0
let chartScopeSyncSeq = 0
const realtimeStatus = ref('connecting')
const activeChartSubscriptionKey = ref('')
const dataMode = ref('realtime')
const chartSubscribedTicket = ref(null)
const autoOpenTradeWindow = ref(false)
const chartHealth = reactive({
  lastBarUpdateAt: 0,
  lastQuoteUpdateAt: 0,
  lastWindowResyncAt: 0,
})
const quoteOnlyResyncThresholdMS = 3000
const quoteOnlyWindowResyncIntervalMS = 5000
const tradeWindow = reactive({
  visible: false,
  x: 0,
  y: 74,
  width: 1322,
  height: 268,
  activeTab: 'positions',
  dragging: false,
  resizing: false,
  symbolLocked: false,
})
const tradeTerminal = reactive({
  summary: {},
  order_entry_defaults: { account_id: '', symbol: '', exchange_id: '', volume: 1, limit_price: 0 },
  working_orders: [],
  positions: [],
  orders: [],
  trades: [],
  funds: {},
})
const tradeForm = reactive({
  account_id: '',
  symbol: '',
  exchange_id: '',
  direction: 'buy',
  offset_flag: 'open',
  limit_price: '',
  volume: 1,
  client_tag: 'chart-trade-dock',
})
let tradeWindowDrag = null
let tradeWindowResize = null
const DRAWING_TYPE_LABELS = {
  trendline: '趋势线',
  hline: '水平线',
  vline: '垂直线',
  rect: '矩形',
  text: '文本',
}

function normalizeChannelSettingsWithDisplayOff(raw) {
  const out = normalizeChannelSettingsV2(raw || DEFAULT_CHANNEL_SETTINGS)
  out.display.showExtrema = false
  out.display.showRansac = false
  out.display.showRegression = false
  return out
}

function normalizeReversalSettingsOff(raw) {
  return normalizeReversalSettings({
    ...(raw || DEFAULT_REVERSAL_SETTINGS),
    enabled: false,
  })
}

function applyLightweightDefaults(layoutData = {}) {
  layout.theme = layoutData.theme || 'dark'
  layout.panes.right_watchlist_open = layoutData.panes?.right_watchlist_open ?? true
  watchlistWidth.value = Number(layoutData.panes?.right_watchlist_width || 340)
  if (!Number.isFinite(watchlistWidth.value)) watchlistWidth.value = 340
  if (watchlistWidth.value < minWatchlistWidth) watchlistWidth.value = minWatchlistWidth
  if (watchlistWidth.value > maxWatchlistWidth) watchlistWidth.value = maxWatchlistWidth
  layout.indicators.ma20 = true
  layout.indicators.macd = true
  layout.indicators.volume = true
  channelDebug.value = false
  channelState.settings = normalizeChannelSettingsWithDisplayOff(layoutData.channels?.settings || DEFAULT_CHANNEL_SETTINGS)
  channelState.decisions = []
  channelState.selected_id = ''
  channelState.rows = []
  channelState.detail = null
  channelState.persistVersion = 0
  reversalState.settings = normalizeReversalSettingsOff(layoutData.reversal?.settings || DEFAULT_REVERSAL_SETTINGS)
  reversalState.results = { lines: [], events: [] }
  reversalState.persistVersion = 0
  reversalState.selected_id = ''
  activeRightTab.value = String(layoutData.panes?.right_active_tab || 'quote')
  if (!['quote', 'watchlist', 'object_tree', 'channel', 'reversal'].includes(activeRightTab.value)) {
    activeRightTab.value = 'quote'
  }
  drawings.value = (layoutData.drawings || []).map((d) => normalizeDrawingForSave(d))
  selectedDrawingId.value = ''
  quoteSnapshot.value = {}
  quoteTicks.value = []
}

const keyboardSprite = reactive({
  visible: false,
  loading: false,
  query: '',
  items: [],
  activeIndex: 0,
  requestSeq: 0,
})

function normalizeDrawingForSave(d) {
  const out = {
    owner: owner.value || 'admin',
    object_class: d.object_class || (d.type === 'trendline' ? 'trendline' : 'general'),
    visible_range: d.visible_range || 'all',
    line_style: d.line_style || 'solid',
    left_cap: d.left_cap || 'plain',
    right_cap: d.right_cap || 'plain',
    label_pos: d.label_pos || 'middle',
    label_align: d.label_align || 'center',
    ...d,
  }
  out.line_color = out.line_color || out.style?.color || ''
  out.line_width = out.line_width ?? out.style?.width ?? null
  out.label_text = out.label_text || out.text || ''
  if (out.type === 'trendline' && Array.isArray(out.points) && out.points.length >= 2) {
    out.start_time = Number(out.start_time || out.points[0]?.time || 0)
    out.end_time = Number(out.end_time || out.points[1]?.time || 0)
    if (out.start_price == null) out.start_price = out.points[0]?.price ?? null
    if (out.end_price == null) out.end_price = out.points[1]?.price ?? null
  }
  return out
}

function getParams() {
  const p = new URLSearchParams(location.search)
  const symbol = String(p.get('symbol') || '').trim().toLowerCase()
  const kind = String(p.get('type') || '').trim().toLowerCase()
  const variety = String(p.get('variety') || '').trim().toLowerCase()
  scope.symbol = symbol
  scope.type = kind || inferKlineTypeBySymbol(symbol)
  scope.variety = variety || inferVarietyBySymbol(symbol, scope.type)
  scope.timeframe = p.get('timeframe') || '1m'
  scope.end = p.get('end') || ''
  autoOpenTradeWindow.value = p.get('open_trade') === '1'
}

function inferKlineTypeBySymbol(symbol) {
  const s = String(symbol || '').trim().toLowerCase()
  if (!s) return ''
  if (s === 'l9' || s.endsWith('l9')) return 'l9'
  return 'contract'
}

function inferVarietyBySymbol(symbol, kind) {
  const s = String(symbol || '').trim().toLowerCase()
  if (!s) return ''
  if (kind === 'l9') {
    if (s === 'l9') return ''
    return s.endsWith('l9') ? s.slice(0, -2) : s
  }
  const m = s.match(/^[a-z]+/)
  return m ? m[0] : ''
}

async function fetchWatchlist() {
  try {
    const resp = await fetch('/api/instruments?page=1&page_size=200')
    if (!resp.ok) return
    const data = await resp.json()
    watchlist.value = data.items || []
  } catch {
    // ignore
  }
}

const layoutPayload = computed(() => ({
  owner: owner.value || 'admin',
  symbol: scope.symbol,
  type: scope.type,
  variety: scope.variety,
  timeframe: scope.timeframe,
  theme: layout.theme,
  panes: {
    ...layout.panes,
    right_watchlist_width: watchlistWidth.value,
    right_active_tab: activeRightTab.value,
  },
  indicators: layout.indicators,
  channels: {
    settings: channelState.settings,
    decisions: channelState.decisions,
    selected_id: channelState.selected_id,
  },
  reversal: {
    settings: reversalState.settings,
    results: reversalState.results,
    persistVersion: reversalState.persistVersion,
    selected_id: reversalState.selected_id,
  },
  drawings: drawings.value,
}))

function parseDrawingSortTs(d, fallbackIndex = 0) {
  const t1 = Date.parse(d?.created_at || '')
  if (Number.isFinite(t1)) return t1
  const t2 = Date.parse(d?.updated_at || '')
  if (Number.isFinite(t2)) return t2
  const z = Number(d?.z)
  if (Number.isFinite(z)) return z
  return -1_000_000_000_000 + fallbackIndex
}

function parseDrawingDisplayTs(d) {
  const t1 = Date.parse(d?.created_at || '')
  if (Number.isFinite(t1)) return t1
  const t2 = Date.parse(d?.updated_at || '')
  if (Number.isFinite(t2)) return t2
  return null
}

function parseDrawingUpdatedTs(d) {
  const t1 = Date.parse(d?.updated_at || '')
  if (Number.isFinite(t1)) return t1
  const t2 = Date.parse(d?.created_at || '')
  if (Number.isFinite(t2)) return t2
  return null
}

function formatHms(ts) {
  if (!Number.isFinite(ts)) return '--'
  const d = new Date(ts)
  const p = (n) => String(n).padStart(2, '0')
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

const objectTreeRows = computed(() => {
  return (drawings.value || [])
    .map((d, idx) => {
      const displayTs = parseDrawingDisplayTs(d)
      const updatedTs = parseDrawingUpdatedTs(d)
      return {
        id: d.id,
        type: d.type,
        typeLabel: DRAWING_TYPE_LABELS[d.type] || d.type || '对象',
        createdAtLabel: formatHms(displayTs),
        updatedAtLabel: formatHms(updatedTs),
        visible: d.visible !== false,
        _sortTs: parseDrawingSortTs(d, idx),
        _idx: idx,
      }
    })
    .sort((a, b) => {
      if (b._sortTs !== a._sortTs) return b._sortTs - a._sortTs
      return a._idx - b._idx
    })
})

async function loadLayout() {
  if (!scope.symbol || !scope.type) return
  try {
    const qs = new URLSearchParams({ owner: owner.value || 'admin', symbol: scope.symbol, type: scope.type, variety: scope.variety || '', timeframe: scope.timeframe || '1m', data_mode: dataMode.value })
    const resp = await fetch(`/api/chart/layout?${qs.toString()}`)
    if (!resp.ok) return
    const data = await resp.json()
    applyLightweightDefaults(data)
  } catch {
    // ignore
  }
}

async function flushSave() {
  if (!scope.symbol || !scope.type) return
  saveStatus.value = 'saving'
  try {
    const resp = await fetch(`/api/chart/layout?data_mode=${encodeURIComponent(dataMode.value)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(layoutPayload.value),
    })
    if (!resp.ok) {
      saveStatus.value = 'error'
      return
    }
    saveStatus.value = 'saved'
  } catch {
    saveStatus.value = 'error'
  }
}

function flushSaveOnUnload() {
  if (!scope.symbol || !scope.type) return
  try {
    const body = JSON.stringify(layoutPayload.value)
    const url = `/api/chart/layout?data_mode=${encodeURIComponent(dataMode.value)}`
    navigator.sendBeacon(url, new Blob([body], { type: 'application/json' }))
  } catch {
    // ignore
  }
}

function scheduleSave() {
  saveStatus.value = 'pending'
  if (saveTimer) clearTimeout(saveTimer)
  saveTimer = setTimeout(() => {
    void flushSave()
  }, 800)
}

function onSetDrawings(next) {
  drawings.value = (Array.isArray(next) ? next : []).map((d) => normalizeDrawingForSave(d))
  if (selectedDrawingId.value && !drawings.value.some((d) => d.id === selectedDrawingId.value)) {
    selectedDrawingId.value = ''
  }
  scheduleSave()
}

function onSelectWatch(item) {
  scope.symbol = String(item?.symbol || '').trim().toLowerCase()
  scope.type = item.type || 'contract'
  scope.variety = item.variety || ''
  if (!tradeWindow.symbolLocked) {
    tradeForm.symbol = scope.symbol
  }
  const qs = new URLSearchParams({
    symbol: scope.symbol,
    type: scope.type,
    variety: scope.variety,
    timeframe: scope.timeframe,
  })
  history.replaceState({}, '', `/chart?${qs.toString()}`)
}

function resetKeyboardSprite() {
  keyboardSprite.visible = false
  keyboardSprite.loading = false
  keyboardSprite.query = ''
  keyboardSprite.items = []
  keyboardSprite.activeIndex = 0
  keyboardSprite.requestSeq += 1
}

function isEditableTarget(target) {
  const el = target instanceof HTMLElement ? target : document.activeElement
  if (!(el instanceof HTMLElement)) return false
  if (el.isContentEditable) return true
  const tag = String(el.tagName || '').toLowerCase()
  if (tag === 'input' || tag === 'textarea' || tag === 'select' || tag === 'button') return true
  return !!el.closest('input, textarea, select, button, [contenteditable="true"]')
}

function pickQuoteNumber(...values) {
  for (const item of values) {
    const n = Number(item)
    if (Number.isFinite(n) && n > 0) return n
  }
  return 0
}

function currentTradeSymbol() {
  return String(scope.symbol || '').trim()
}

function syncTradeFormWithScope(force = false) {
  const symbol = currentTradeSymbol()
  if (!symbol) return
  if (force || !tradeWindow.symbolLocked || !String(tradeForm.symbol || '').trim()) {
    tradeForm.symbol = symbol
    tradeWindow.symbolLocked = false
  }
  const defaults = tradeTerminal.order_entry_defaults || {}
  if (!String(tradeForm.account_id || '').trim()) tradeForm.account_id = defaults.account_id || ''
  if (!String(tradeForm.exchange_id || '').trim()) tradeForm.exchange_id = defaults.exchange_id || ''
  if (!String(tradeForm.limit_price || '').trim()) {
    const price = pickQuoteNumber(defaults.limit_price, quoteSnapshot.value?.latest_price, quoteSnapshot.value?.ask_price1, quoteSnapshot.value?.bid_price1)
    tradeForm.limit_price = price > 0 ? String(price) : ''
  }
  tradeForm.volume = 1
}

const MIN_TRADE_WINDOW_WIDTH = 980
const MIN_TRADE_WINDOW_HEIGHT = 220

function tradeWindowHostRect() {
  const host = document.querySelector('.tv-terminal')
  return host?.getBoundingClientRect?.() || null
}

function clampTradeWindowPosition(nextX, nextY, width = tradeWindow.width, height = tradeWindow.height) {
  const hostRect = tradeWindowHostRect()
  if (!hostRect) return
  const boundedMaxX = Math.max(0, Number(hostRect.width || 0) - width - 12)
  const boundedMaxY = Math.max(24, Number(hostRect.height || 0) - height - 12)
  tradeWindow.x = Math.max(0, Math.min(Math.round(nextX), boundedMaxX))
  tradeWindow.y = Math.max(22, Math.min(Math.round(nextY), boundedMaxY))
}

function applyTradeWindowRect(nextX, nextY, nextWidth, nextHeight) {
  const hostRect = tradeWindowHostRect()
  if (!hostRect) return
  const width = Math.max(MIN_TRADE_WINDOW_WIDTH, Math.round(nextWidth))
  const height = Math.max(MIN_TRADE_WINDOW_HEIGHT, Math.round(nextHeight))
  const maxWidth = Math.max(MIN_TRADE_WINDOW_WIDTH, Math.floor(hostRect.width - 12))
  const maxHeight = Math.max(MIN_TRADE_WINDOW_HEIGHT, Math.floor(hostRect.height - 22))
  tradeWindow.width = Math.min(width, maxWidth)
  tradeWindow.height = Math.min(height, maxHeight)
  clampTradeWindowPosition(nextX, nextY, tradeWindow.width, tradeWindow.height)
}

function placeTradeWindowDefault() {
  const hostRect = tradeWindowHostRect()
  const maxX = Math.max(0, Number(hostRect?.width || 0) - tradeWindow.width - 16)
  tradeWindow.x = Math.max(2, Math.min(maxX, 2))
  tradeWindow.y = 1
}

function openTradeWindow(forceSync = true) {
  if (!tradeWindow.visible) {
    if (tradeWindow.x === 0 && tradeWindow.y <= 24) placeTradeWindowDefault()
    tradeWindow.visible = true
  }
  syncTradeFormWithScope(forceSync)
  void fetchTradeTerminal().catch(() => {})
}

function closeTradeWindow() {
  tradeWindow.visible = false
}

async function fetchTradeTerminal() {
  const params = new URLSearchParams()
  if (currentTradeSymbol()) params.set('symbol', currentTradeSymbol())
  const resp = await fetch(`/api/trade/terminal?${params.toString()}`)
  if (!resp.ok) throw new Error(`trade terminal http ${resp.status}`)
  const data = await resp.json()
  tradeTerminal.summary = data.summary || {}
  tradeTerminal.order_entry_defaults = data.order_entry_defaults || {}
  tradeTerminal.working_orders = data.working_orders || []
  tradeTerminal.positions = data.positions || []
  tradeTerminal.orders = data.orders || []
  tradeTerminal.trades = data.trades || []
  tradeTerminal.funds = data.funds || {}
  if (!String(tradeForm.account_id || '').trim()) tradeForm.account_id = tradeTerminal.order_entry_defaults.account_id || ''
  if (!tradeWindow.symbolLocked && currentTradeSymbol()) tradeForm.symbol = currentTradeSymbol()
  if (!String(tradeForm.exchange_id || '').trim()) tradeForm.exchange_id = tradeTerminal.order_entry_defaults.exchange_id || ''
  if (!String(tradeForm.limit_price || '').trim()) {
    const price = pickQuoteNumber(tradeTerminal.order_entry_defaults.limit_price, quoteSnapshot.value?.latest_price, quoteSnapshot.value?.ask_price1, quoteSnapshot.value?.bid_price1)
    tradeForm.limit_price = price > 0 ? String(price) : ''
  }
}

function updateTradeFormField(field, value) {
  if (!(field in tradeForm)) return
  if (field === 'volume') {
    const n = Number(value)
    tradeForm.volume = Number.isFinite(n) && n > 0 ? Math.floor(n) : 1
    return
  }
  tradeForm[field] = typeof value === 'string' ? value : String(value ?? '')
  if (field === 'symbol') {
    tradeWindow.symbolLocked = String(value || '').trim().toLowerCase() !== currentTradeSymbol().toLowerCase()
  }
}

function showTradeError(prefix, err) {
  const msg = err instanceof Error ? err.message : String(err || 'unknown error')
  console.warn(`[trade-dock] ${prefix} failed`, err)
  if (typeof window !== 'undefined' && typeof window.alert === 'function') {
    window.alert(`${prefix}失败：${msg}`)
  }
}

async function adjustPaperAccount(payload, actionLabel) {
  try {
    const resp = await fetch('/api/trade/account/adjust', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
        ...payload,
      }),
    })
    if (!resp.ok) throw new Error(await resp.text())
    await fetchTradeTerminal()
  } catch (err) {
    showTradeError(actionLabel, err)
  }
}

async function adjustCashflow() {
  const depositRaw = window.prompt('入金金额（>=0）', '0')
  if (depositRaw == null) return
  const withdrawRaw = window.prompt('出金金额（>=0）', '0')
  if (withdrawRaw == null) return
  const premiumRaw = window.prompt('权利金调整（可正可负，默认0）', '0')
  if (premiumRaw == null) return
  const deposit = Number(depositRaw || 0)
  const withdraw = Number(withdrawRaw || 0)
  const premium = Number(premiumRaw || 0)
  if (!Number.isFinite(deposit) || !Number.isFinite(withdraw) || !Number.isFinite(premium) || deposit < 0 || withdraw < 0) {
    window.alert('输入无效，请输入数字；入金/出金需>=0')
    return
  }
  await adjustPaperAccount({
    deposit_delta: deposit,
    withdraw_delta: withdraw,
    premium_delta: premium,
  }, '出入金调整')
}

async function adjustFee() {
  const otherFeeRaw = window.prompt('其他费用调整（正数=增加费用，负数=减少）', '0')
  if (otherFeeRaw == null) return
  const frozenCommissionRaw = window.prompt('冻结手续费调整（可正可负）', '0')
  if (frozenCommissionRaw == null) return
  const frozenPremiumRaw = window.prompt('冻结权利金调整（可正可负）', '0')
  if (frozenPremiumRaw == null) return
  const otherFee = Number(otherFeeRaw || 0)
  const frozenCommission = Number(frozenCommissionRaw || 0)
  const frozenPremium = Number(frozenPremiumRaw || 0)
  if (!Number.isFinite(otherFee) || !Number.isFinite(frozenCommission) || !Number.isFinite(frozenPremium)) {
    window.alert('输入无效，请输入数字')
    return
  }
  await adjustPaperAccount({
    other_fee_delta: otherFee,
    frozen_commission_delta: frozenCommission,
    frozen_premium_delta: frozenPremium,
  }, '费用调整')
}

async function submitTradeOrder() {
  try {
    const payload = {
      account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
      symbol: String(tradeForm.symbol || '').trim(),
      exchange_id: String(tradeForm.exchange_id || '').trim(),
      direction: tradeForm.direction,
      offset_flag: tradeForm.offset_flag,
      limit_price: Number(tradeForm.limit_price),
      volume: Number(tradeForm.volume || 1),
      client_tag: tradeForm.client_tag,
      reason: 'manual',
    }
    const resp = await fetch('/api/trade/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    if (!resp.ok) throw new Error(await resp.text())
    await fetchTradeTerminal()
  } catch (err) {
    showTradeError('下单', err)
  }
}

async function cancelTradeOrderRequest(item, options = {}) {
  try {
    const payload = {
      account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
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
    if (!resp.ok) throw new Error(await resp.text())
    if (!options.skipRefresh) await fetchTradeTerminal()
    return true
  } catch (err) {
    if (!options.silentError) showTradeError('撤单', err)
    return false
  }
}

async function cancelTradeOrder(item) {
  await cancelTradeOrderRequest(item)
}

function closeVolumeByRatio(closableRaw, ratioRaw = 1) {
  const closable = Math.max(0, Math.floor(Number(closableRaw || 0)))
  if (closable <= 0) return 0
  const ratio = Number(ratioRaw)
  if (!Number.isFinite(ratio) || ratio >= 1) return closable
  if (ratio <= 0) return 0
  return Math.max(1, Math.min(closable, Math.floor(closable * ratio)))
}

async function closeTradePosition(item, options = {}) {
  try {
    const direction = String(item?.direction || '').trim().toLowerCase()
    const volume = closeVolumeByRatio(item?.closable, options.ratio ?? 1)
    if (!item?.symbol || volume <= 0) return
    const defaultPrice = direction === 'short'
      ? pickQuoteNumber(quoteSnapshot.value?.ask_price1, quoteSnapshot.value?.latest_price, item.market_price, item.avg_price)
      : pickQuoteNumber(quoteSnapshot.value?.bid_price1, quoteSnapshot.value?.latest_price, item.market_price, item.avg_price)
    const price = pickQuoteNumber(options.limitPrice, defaultPrice)
    const resp = await fetch(`/api/trade/positions/${encodeURIComponent(item.symbol)}/close`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
        exchange_id: item.exchange,
        direction,
        offset_flag: 'close',
        price,
        volume,
      }),
    })
    if (!resp.ok) throw new Error(await resp.text())
    if (!options.skipRefresh) await fetchTradeTerminal()
    return true
  } catch (err) {
    showTradeError('平仓', err)
    return false
  }
}

async function submitOpenOrder(payload) {
  const resp = await fetch('/api/trade/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!resp.ok) throw new Error(await resp.text())
}

async function onPositionClose(payload) {
  const item = payload?.position
  const ratio = Number(payload?.ratio ?? 1)
  const limitPrice = Number(payload?.limit_price)
  await closeTradePosition(item, { ratio, limitPrice })
}

async function onPositionReverse(payload) {
  const item = payload?.position
  if (!item?.symbol) return
  const volume = Math.max(1, closeVolumeByRatio(payload?.volume ?? item?.closable, 1))
  const closeLimitPrice = Number(payload?.close_limit_price)
  const openLimitPrice = Number(payload?.open_limit_price)
  const openDirection = String(payload?.open_direction || '').trim().toLowerCase()
  if (!openDirection || volume <= 0) return
  try {
    const closed = await closeTradePosition(item, { ratio: 1, limitPrice: closeLimitPrice, skipRefresh: true })
    if (!closed) return
    await submitOpenOrder({
      account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
      symbol: String(item.symbol || '').trim(),
      exchange_id: String(item.exchange || tradeForm.exchange_id || tradeTerminal.order_entry_defaults?.exchange_id || '').trim(),
      direction: openDirection,
      offset_flag: 'open',
      limit_price: pickQuoteNumber(openLimitPrice, quoteSnapshot.value?.latest_price, tradeTerminal.order_entry_defaults?.limit_price),
      volume,
      client_tag: tradeForm.client_tag,
      reason: 'manual',
    })
    await fetchTradeTerminal()
  } catch (err) {
    showTradeError('反手', err)
  }
}

async function onAmendOrder(payload) {
  const oldOrder = payload?.order
  const limitPrice = Number(payload?.limit_price)
  const volume = Number(payload?.volume)
  if (!oldOrder?.command_id) return
  if (!Number.isFinite(limitPrice) || limitPrice <= 0) return
  if (!Number.isFinite(volume) || volume <= 0) return
  try {
    const canceled = await cancelTradeOrderRequest(oldOrder, { skipRefresh: true, silentError: false })
    if (!canceled) return
    await submitOpenOrder({
      account_id: tradeForm.account_id || tradeTerminal.order_entry_defaults?.account_id || '',
      symbol: String(oldOrder.symbol || '').trim(),
      exchange_id: String(oldOrder.exchange_id || tradeForm.exchange_id || tradeTerminal.order_entry_defaults?.exchange_id || '').trim(),
      direction: String(oldOrder.direction || '').trim(),
      offset_flag: String(oldOrder.offset_flag || '').trim(),
      limit_price: limitPrice,
      volume: Math.max(1, Math.floor(volume)),
      client_tag: tradeForm.client_tag,
      reason: 'manual',
    })
    await fetchTradeTerminal()
  } catch (err) {
    showTradeError('改价', err)
  }
}

function normalizeQuickOrderPayload(raw) {
  if (typeof raw === 'string') return { kind: raw, limitPrice: 0 }
  const kind = String(raw?.kind || '').trim()
  const price = Number(raw?.limit_price ?? raw?.limitPrice)
  return {
    kind,
    limitPrice: Number.isFinite(price) && price > 0 ? price : 0,
  }
}

async function quickTradeOrder(raw) {
  const { kind, limitPrice } = normalizeQuickOrderPayload(raw)
  const lastPrice = pickQuoteNumber(
    limitPrice,
    quoteSnapshot.value?.latest_price,
    quoteSnapshot.value?.ask_price1,
    quoteSnapshot.value?.bid_price1,
    tradeTerminal.order_entry_defaults?.limit_price,
  )
  if (kind === 'buy_open') {
    tradeForm.direction = 'buy'
    tradeForm.offset_flag = 'open'
    if (lastPrice > 0) tradeForm.limit_price = String(lastPrice)
    await submitTradeOrder()
    return
  }
  if (kind === 'sell_open') {
    tradeForm.direction = 'sell'
    tradeForm.offset_flag = 'open'
    if (lastPrice > 0) tradeForm.limit_price = String(lastPrice)
    await submitTradeOrder()
    return
  }
  if (kind === 'close') {
    if (tradeTerminal.positions?.length) {
      const target = tradeTerminal.positions.find((item) => Number(item?.closable || 0) > 0) || tradeTerminal.positions[0]
      await closeTradePosition(target)
      return
    }
    tradeForm.offset_flag = 'close'
    if (lastPrice > 0) tradeForm.limit_price = String(lastPrice)
    await submitTradeOrder()
  }
}

function startTradeWindowDrag(evt) {
  if (!(evt.target instanceof HTMLElement) || !evt.target.closest('.trade-classic-titlebar')) return
  const hostRect = tradeWindowHostRect()
  if (!hostRect) return
  evt.preventDefault()
  tradeWindow.dragging = true
  tradeWindowDrag = {
    hostRect,
    offsetX: evt.clientX - hostRect.left - tradeWindow.x,
    offsetY: evt.clientY - hostRect.top - tradeWindow.y,
  }
}

function startTradeWindowResize(direction, evt) {
  const hostRect = tradeWindowHostRect()
  if (!hostRect) return
  evt.preventDefault()
  evt.stopPropagation()
  tradeWindow.resizing = true
  tradeWindowResize = {
    direction,
    hostRect,
    startX: evt.clientX,
    startY: evt.clientY,
    startLeft: tradeWindow.x,
    startTop: tradeWindow.y,
    startWidth: tradeWindow.width,
    startHeight: tradeWindow.height,
  }
}

function onTradeWindowPointerMove(evt) {
  if (tradeWindowResize) {
    const dx = evt.clientX - tradeWindowResize.startX
    const dy = evt.clientY - tradeWindowResize.startY
    let nextX = tradeWindowResize.startLeft
    let nextY = tradeWindowResize.startTop
    let nextWidth = tradeWindowResize.startWidth
    let nextHeight = tradeWindowResize.startHeight
    const dir = tradeWindowResize.direction

    if (dir.includes('e')) nextWidth = tradeWindowResize.startWidth + dx
    if (dir.includes('s')) nextHeight = tradeWindowResize.startHeight + dy
    if (dir.includes('w')) {
      nextWidth = tradeWindowResize.startWidth - dx
      nextX = tradeWindowResize.startLeft + dx
    }
    if (dir.includes('n')) {
      nextHeight = tradeWindowResize.startHeight - dy
      nextY = tradeWindowResize.startTop + dy
    }

    if (nextWidth < MIN_TRADE_WINDOW_WIDTH) {
      if (dir.includes('w')) nextX -= MIN_TRADE_WINDOW_WIDTH - nextWidth
      nextWidth = MIN_TRADE_WINDOW_WIDTH
    }
    if (nextHeight < MIN_TRADE_WINDOW_HEIGHT) {
      if (dir.includes('n')) nextY -= MIN_TRADE_WINDOW_HEIGHT - nextHeight
      nextHeight = MIN_TRADE_WINDOW_HEIGHT
    }

    applyTradeWindowRect(nextX, nextY, nextWidth, nextHeight)
    return
  }
  if (!tradeWindowDrag) return
  const nextX = evt.clientX - tradeWindowDrag.hostRect.left - tradeWindowDrag.offsetX
  const nextY = evt.clientY - tradeWindowDrag.hostRect.top - tradeWindowDrag.offsetY
  clampTradeWindowPosition(nextX, nextY)
}

function stopTradeWindowDrag() {
  tradeWindow.dragging = false
  tradeWindowDrag = null
  tradeWindow.resizing = false
  tradeWindowResize = null
}

function isSpriteKey(evt) {
  return !evt.ctrlKey && !evt.metaKey && !evt.altKey && evt.key.length === 1 && /^[a-zA-Z0-9]$/.test(evt.key)
}

async function fetchKeyboardSpriteItems() {
  const query = String(keyboardSprite.query || '').trim().toLowerCase()
  if (!query) {
    keyboardSprite.items = []
    keyboardSprite.loading = false
    keyboardSprite.activeIndex = 0
    return
  }
  const requestSeq = keyboardSprite.requestSeq + 1
  keyboardSprite.requestSeq = requestSeq
  keyboardSprite.loading = true
  try {
    const params = new URLSearchParams({
      keyword: query,
      page: '1',
      page_size: '20',
      data_mode: dataMode.value,
    })
    const resp = await fetch(`/api/kline/search?${params.toString()}`)
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    const data = await resp.json()
    if (keyboardSprite.requestSeq !== requestSeq) return
    keyboardSprite.items = Array.isArray(data.items) ? data.items : []
    if (keyboardSprite.activeIndex >= keyboardSprite.items.length) keyboardSprite.activeIndex = 0
  } catch {
    if (keyboardSprite.requestSeq !== requestSeq) return
    keyboardSprite.items = []
    keyboardSprite.activeIndex = 0
  } finally {
    if (keyboardSprite.requestSeq === requestSeq) keyboardSprite.loading = false
  }
}

function openKeyboardSprite(initialChar = '') {
  const next = String(initialChar || '').trim().toLowerCase()
  keyboardSprite.visible = true
  keyboardSprite.query = next
  keyboardSprite.activeIndex = 0
}

function chooseKeyboardSprite(item) {
  if (!item) return
  resetKeyboardSprite()
  onSelectWatch(item)
}

function onKeyboardSpriteKeydown(evt) {
  if (!isEditableTarget(evt.target) && evt.code === 'Space') {
    evt.preventDefault()
    openTradeWindow(true)
    return
  }
  if (!isEditableTarget(evt.target) && evt.key === 'Escape' && tradeWindow.visible) {
    evt.preventDefault()
    closeTradeWindow()
    return
  }
  if (isEditableTarget(evt.target)) return
  if (!keyboardSprite.visible) {
    if (!isSpriteKey(evt)) return
    evt.preventDefault()
    openKeyboardSprite(evt.key)
    return
  }

  if (evt.key === 'Escape') {
    evt.preventDefault()
    resetKeyboardSprite()
    return
  }
  if (evt.key === 'Backspace') {
    evt.preventDefault()
    keyboardSprite.query = keyboardSprite.query.slice(0, -1)
    if (!keyboardSprite.query) resetKeyboardSprite()
    return
  }
  if (evt.key === 'ArrowDown') {
    evt.preventDefault()
    if (!keyboardSprite.items.length) return
    keyboardSprite.activeIndex = (keyboardSprite.activeIndex + 1) % keyboardSprite.items.length
    return
  }
  if (evt.key === 'ArrowUp') {
    evt.preventDefault()
    if (!keyboardSprite.items.length) return
    keyboardSprite.activeIndex = (keyboardSprite.activeIndex - 1 + keyboardSprite.items.length) % keyboardSprite.items.length
    return
  }
  if (evt.key === 'Enter') {
    evt.preventDefault()
    chooseKeyboardSprite(keyboardSprite.items[keyboardSprite.activeIndex] || null)
    return
  }
  if (isSpriteKey(evt)) {
    evt.preventDefault()
    keyboardSprite.query += evt.key.toLowerCase()
    return
  }
}

function onDeleteSelected() {
  if (selectedDrawingId.value) {
    const target = selectedDrawingId.value
    drawings.value = drawings.value.filter((d) => d.id !== target)
    selectedDrawingId.value = ''
    scheduleSave()
    return
  }
  paneRef.value?.onDeleteSelected?.()
}

function onSelectDrawing(id) {
  selectedDrawingId.value = id || ''
}

function onToggleDrawingVisible(id) {
  if (!id) return
  drawings.value = drawings.value.map((d) => {
    if (d.id !== id) return d
    return normalizeDrawingForSave({ ...d, visible: d.visible === false })
  })
  scheduleSave()
}

function onDeleteDrawingById(id) {
  if (!id) return
  drawings.value = drawings.value.filter((d) => d.id !== id)
  if (selectedDrawingId.value === id) selectedDrawingId.value = ''
  scheduleSave()
}

function onSetTool(v) {
  activeTool.value = v
}

function onSetTimeframe(v) {  // K线顶部按钮切换周期时会把周期传到这儿
  scope.timeframe = v
}

function onSetTheme(v) {
  layout.theme = v
}

function currentChartSubscription() {
  if (!scope.symbol || !scope.type || !scope.timeframe) return null
  return {
    symbol: String(scope.symbol || '').trim().toLowerCase(),
    type: String(scope.type || '').trim().toLowerCase(),
    variety: String(scope.variety || '').trim().toLowerCase(),
    timeframe: String(scope.timeframe || '1m').trim().toLowerCase(),
    data_mode: String(dataMode.value || 'realtime').trim().toLowerCase(),
  }
}

function chartSubscriptionKey(sub) {
  return [sub?.symbol || '', sub?.type || '', sub?.variety || '', sub?.timeframe || '', sub?.data_mode || 'realtime'].join('|')
}

function applyAppModeSnapshot(snapshot) {
  const nextMode = String(snapshot?.kline_data_mode || snapshot?.chart_data_mode || 'realtime').trim().toLowerCase()
  if (!nextMode || dataMode.value === nextMode) return
  dataMode.value = nextMode
}

function resetQuotePanel() {
  quoteSnapshot.value = {}
  quoteTicks.value = []
  chartSubscribedTicket.value = null
  chartHealth.lastBarUpdateAt = 0
  chartHealth.lastQuoteUpdateAt = 0
  chartHealth.lastWindowResyncAt = 0
}

function sendChartWS(type, data) {
  if (!chartWS || chartWS.readyState !== WebSocket.OPEN) return false
  chartWS.send(JSON.stringify({ type, data }))
  return true
}

function syncChartSubscription() {
  const next = currentChartSubscription()
  const nextKey = next ? chartSubscriptionKey(next) : ''
  if (activeChartSubscriptionKey.value && activeChartSubscriptionKey.value !== nextKey) {
    const [symbol, type, variety, timeframe, data_mode] = activeChartSubscriptionKey.value.split('|')
    sendChartWS('chart_unsubscribe', { symbol, type, variety, timeframe, data_mode })
    sendChartWS('quote_unsubscribe', { symbol, type, variety, timeframe, data_mode })
    activeChartSubscriptionKey.value = ''
    resetQuotePanel()
  }
  if (!next || !sendChartWS('chart_subscribe', next)) return
  sendChartWS('quote_subscribe', next)
  activeChartSubscriptionKey.value = nextKey
}

async function syncChartScopeAndReload() {
  const seq = ++chartScopeSyncSeq
  resetKeyboardSprite()
  await loadLayout()
  if (seq !== chartScopeSyncSeq) return
  syncChartSubscription()
  if (seq !== chartScopeSyncSeq) return
  await paneRef.value?.reload?.()
}

function scheduleChartWSReconnect() {
  if (chartWSReconnectTimer) return
  const delay = Math.min(5000, 1000 * Math.max(1, chartWSReconnectAttempt + 1))
  chartWSReconnectTimer = setTimeout(() => {
    chartWSReconnectTimer = null
    connectChartWS()
  }, delay)
}

function connectChartWS() {
  if (chartWS && (chartWS.readyState === WebSocket.OPEN || chartWS.readyState === WebSocket.CONNECTING)) return
  realtimeStatus.value = 'connecting'
  const protocol = location.protocol === 'https:' ? 'wss' : 'ws'
  chartWS = new WebSocket(`${protocol}://${location.host}/ws`)
  chartWS.addEventListener('open', async () => {
    realtimeStatus.value = 'live'
    chartWSReconnectAttempt = 0
    if (activeChartSubscriptionKey.value) {
      activeChartSubscriptionKey.value = ''
      resetQuotePanel()
    }
    syncChartSubscription()
    if (!paneRef.value?.reload) return
    await paneRef.value.reload()
  })
  chartWS.addEventListener('message', (evt) => {
    let msg = null
    try {
      msg = JSON.parse(String(evt.data || '{}'))
    } catch {
      return
    }
    if (msg?.type === 'chart_bar_update' && msg.data) {
      chartHealth.lastBarUpdateAt = Date.now()
      paneRef.value?.applyRealtimeBarUpdate?.(msg.data)
      return
    }
    if (msg?.type === 'chart_subscribed' && msg.data) {
      const sub = msg.data?.subscription || {}
      const key = chartSubscriptionKey({
        symbol: sub.symbol,
        type: sub.type,
        variety: sub.variety,
        timeframe: sub.timeframe,
        data_mode: sub.data_mode,
      })
      if (key === activeChartSubscriptionKey.value) {
        const prevTicketID = String(chartSubscribedTicket.value?.ticket_id || '')
        chartSubscribedTicket.value = msg.data
        const nextTicketID = String(msg.data?.ticket_id || '')
        if (nextTicketID && nextTicketID !== prevTicketID) {
          paneRef.value?.resetComposeSession?.(nextTicketID)
          chartHealth.lastBarUpdateAt = 0
        }
        paneRef.value?.setComposeTicket?.(msg.data)
      }
      return
    }
    if (msg?.type === 'quote_snapshot_update' && msg.data) {
      chartHealth.lastQuoteUpdateAt = Date.now()
      const sub = msg.data?.subscription || {}
      const key = chartSubscriptionKey({
        symbol: sub.symbol,
        type: sub.type,
        variety: sub.variety,
        timeframe: sub.timeframe,
        data_mode: sub.data_mode,
      })
      if (key === activeChartSubscriptionKey.value) {
        quoteSnapshot.value = msg.data?.snapshot || {}
        quoteTicks.value = Array.isArray(msg.data?.ticks) ? msg.data.ticks : []
        paneRef.value?.applyQuoteSynthesis?.(msg.data, chartSubscribedTicket.value)
        const now = Date.now()
        if (now - chartHealth.lastBarUpdateAt >= quoteOnlyResyncThresholdMS && now - chartHealth.lastWindowResyncAt >= quoteOnlyWindowResyncIntervalMS) {
          chartHealth.lastWindowResyncAt = now
          paneRef.value?.reloadRecentWindow?.()
        }
      }
      return
    }
    if (msg?.type === 'app_mode_update' && msg.data) {
      applyAppModeSnapshot(msg.data)
      return
    }
    if (
      tradeWindow.visible &&
      ['trade_status_update', 'trade_account_update', 'trade_position_update', 'trade_order_update', 'trade_trade_update'].includes(msg?.type)
    ) {
      void fetchTradeTerminal().catch(() => {})
      return
    }
    if (msg?.type === 'chart_subscription_error') {
      realtimeStatus.value = 'error'
    }
  })
  chartWS.addEventListener('close', () => {
    realtimeStatus.value = 'offline'
    chartWS = null
    chartWSReconnectAttempt += 1
    scheduleChartWSReconnect()
  })
  chartWS.addEventListener('error', () => {
    realtimeStatus.value = 'error'
  })
}

function closeChartWS() {
  if (chartWSReconnectTimer) {
    clearTimeout(chartWSReconnectTimer)
    chartWSReconnectTimer = null
  }
  const current = currentChartSubscription()
  if (current) {
    sendChartWS('chart_unsubscribe', current)
    sendChartWS('quote_unsubscribe', current)
  }
  activeChartSubscriptionKey.value = ''
  resetQuotePanel()
  if (chartWS) {
    try {
      chartWS.close()
    } catch {
      // ignore
    }
    chartWS = null
  }
}

function onToggleChannelDebug() {
  if (lightweightOnly) return
  channelDebug.value = !channelDebug.value
}

function onToggleReversal() {
  if (lightweightOnly) return
  reversalState.settings = normalizeReversalSettings({
    ...reversalState.settings,
    enabled: !reversalState.settings?.enabled,
  })
  paneRef.value?.applyReversalSettings?.({ settings: reversalState.settings, force: true })
  reversalState.persistVersion += 1
  scheduleSave()
}

function onChannelViewChange(payload) {
  if (!payload || typeof payload !== 'object') return
  channelState.rows = Array.isArray(payload.rows) ? payload.rows : []
  channelState.detail = payload.detail || null
  if (payload.selected_id !== undefined) channelState.selected_id = payload.selected_id || ''
  const pv = Number(payload.persistVersion || 0)
  if (Number.isFinite(pv) && pv !== Number(channelState.persistVersion || 0)) {
    if (payload.settings) channelState.settings = payload.settings
    if (Array.isArray(payload.decisions)) channelState.decisions = payload.decisions
    channelState.persistVersion = pv
    scheduleSave()
  }
}

function onChannelAction(action) {
  if (lightweightOnly) return
  paneRef.value?.applyChannelAction?.(action || null)
}

function onChannelSettings(payload) {
  if (lightweightOnly) return
  paneRef.value?.applyChannelSettings?.(payload || null)
}

function onReversalViewChange(payload) {
  if (!payload || typeof payload !== 'object') return
  if (payload.results) reversalState.results = payload.results
  if (payload.selected_id !== undefined) reversalState.selected_id = String(payload.selected_id || '')
  const pv = Number(payload.persistVersion || payload.persist_version || 0)
  if (Number.isFinite(pv) && pv !== Number(reversalState.persistVersion || 0)) {
    if (payload.settings) reversalState.settings = normalizeReversalSettings(payload.settings)
    reversalState.persistVersion = pv
    scheduleSave()
  }
}

function onSetReversalSettings(payload) {
  if (lightweightOnly) return
  paneRef.value?.applyReversalSettings?.(payload || null)
}

function onRecalcReversal() {
  if (lightweightOnly) return
  paneRef.value?.recalcReversalNow?.()
}

function onReversalAction(action) {
  if (lightweightOnly) return
  const type = String(action?.type || '')
  if (type === 'recalc') {
    onRecalcReversal()
    return
  }
  paneRef.value?.applyReversalAction?.(action || null)
}

const bodyStyle = computed(() => {
  const rightCol = layout.panes.right_watchlist_open ? `${watchlistWidth.value}px` : '48px'
  const resizerCol = layout.panes.right_watchlist_open ? '4px' : '0px'
  return {
    gridTemplateColumns: `84px minmax(0, 1fr) ${resizerCol} ${rightCol}`,
  }
})

function stopResizeWatchlist() {
  resizeMeta = null
  window.removeEventListener('pointermove', onResizeWatchlistMove)
  window.removeEventListener('pointerup', stopResizeWatchlist)
}

function onResizeWatchlistMove(evt) {
  if (!resizeMeta || !layout.panes.right_watchlist_open) return
  const delta = evt.clientX - resizeMeta.startX
  let next = resizeMeta.startWidth - delta
  if (next < minWatchlistWidth) next = minWatchlistWidth
  if (next > maxWatchlistWidth) next = maxWatchlistWidth
  watchlistWidth.value = Math.floor(next)
}

function startResizeWatchlist(evt) {
  if (!layout.panes.right_watchlist_open) return
  evt.preventDefault()
  resizeMeta = {
    startX: evt.clientX,
    startWidth: watchlistWidth.value,
  }
  window.addEventListener('pointermove', onResizeWatchlistMove)
  window.addEventListener('pointerup', stopResizeWatchlist)
}

watch(
  () => [scope.symbol, scope.type, scope.variety, scope.timeframe],
  async () => {
    if (!tradeWindow.symbolLocked) {
      tradeForm.symbol = currentTradeSymbol()
    }
    await syncChartScopeAndReload()
    if (tradeWindow.visible) {
      try {
        await fetchTradeTerminal()
      } catch {
        // ignore trade terminal refresh failures when hidden backend is unavailable
      }
    }
  },
)

watch(
  () => dataMode.value,
  async () => {
    await syncChartScopeAndReload()
    if (tradeWindow.visible) {
      try {
        await fetchTradeTerminal()
      } catch {
        // ignore
      }
    }
  },
)

watch(
  () => [layout.theme, layout.panes.right_watchlist_open, watchlistWidth.value, layout.indicators.ma20, layout.indicators.macd, layout.indicators.volume],
  () => {
    scheduleSave()
  },
)

watch(
  () => activeRightTab.value,
  () => {
    scheduleSave()
  },
)

watch(
  () => keyboardSprite.query,
  () => {
    if (spriteQueryTimer) clearTimeout(spriteQueryTimer)
    if (!keyboardSprite.visible || !keyboardSprite.query) {
      keyboardSprite.items = []
      keyboardSprite.loading = false
      keyboardSprite.activeIndex = 0
      return
    }
    spriteQueryTimer = setTimeout(() => {
      void fetchKeyboardSpriteItems()
    }, 100)
  },
)

watch(
  () => quoteSnapshot.value,
  () => {
    if (String(tradeForm.limit_price || '').trim()) return
    const price = pickQuoteNumber(
      tradeTerminal.order_entry_defaults?.limit_price,
      quoteSnapshot.value?.latest_price,
      quoteSnapshot.value?.ask_price1,
      quoteSnapshot.value?.bid_price1,
    )
    if (price > 0) tradeForm.limit_price = String(price)
  },
  { deep: true },
)

onMounted(async () => {
  document.getElementById('app')?.classList.add('chart-app-root')
  void bootKlineComposerWASM().catch(() => {})
  getParams()
  tradeForm.symbol = currentTradeSymbol()
  try {
    const resp = await fetch('/api/app-mode')
    if (resp.ok) {
      const data = await resp.json()
      dataMode.value = data?.kline_data_mode || 'realtime'
    }
  } catch {
    dataMode.value = 'realtime'
  }
  await fetchWatchlist()
  await loadLayout()
  connectChartWS()
  spriteKeyHandler = (evt) => onKeyboardSpriteKeydown(evt)
  window.addEventListener('keydown', spriteKeyHandler)
  window.addEventListener('pointermove', onTradeWindowPointerMove)
  window.addEventListener('pointerup', stopTradeWindowDrag)
  beforeUnloadHandler = () => {
    closeChartWS()
    flushSaveOnUnload()
    if (saveTimer) clearTimeout(saveTimer)
  }
  window.addEventListener('beforeunload', beforeUnloadHandler)
  if (autoOpenTradeWindow.value) {
    setTimeout(() => openTradeWindow(true), 30)
  }
})

onUnmounted(() => {
  document.getElementById('app')?.classList.remove('chart-app-root')
  stopResizeWatchlist()
  closeChartWS()
  stopTradeWindowDrag()
  if (spriteQueryTimer) clearTimeout(spriteQueryTimer)
  if (spriteKeyHandler) window.removeEventListener('keydown', spriteKeyHandler)
  window.removeEventListener('pointermove', onTradeWindowPointerMove)
  window.removeEventListener('pointerup', stopTradeWindowDrag)
  if (beforeUnloadHandler) window.removeEventListener('beforeunload', beforeUnloadHandler)
})
</script>

<template>
  <div class="tv-terminal" :class="layout.theme === 'light' ? 'theme-light' : 'theme-dark'">
    <TopToolbar
      :symbol="scope.symbol"
      :type="scope.type"
      :variety="scope.variety"
      :timeframe="scope.timeframe"
      :theme="layout.theme"
      :right-panel-open="layout.panes.right_watchlist_open"
      :active-right-tab="activeRightTab"
      :save-status="saveStatus"
      :active-tool="activeTool"
      :channel-debug="channelDebug"
      :reversal-settings="reversalState.settings"
      :lightweight-only="lightweightOnly"
      @set-timeframe="onSetTimeframe"
      @set-theme="onSetTheme"
      @set-active-right-tab="activeRightTab = $event"
      @toggle-right-sidebar="layout.panes.right_watchlist_open = !layout.panes.right_watchlist_open"
      @set-tool="onSetTool"
      @delete-selected="onDeleteSelected"
      @toggle-channel-debug="onToggleChannelDebug"
      @toggle-reversal="onToggleReversal"
      @set-reversal-settings="onSetReversalSettings"
      @recalc-reversal="onRecalcReversal"
    />

    <div class="tv-body" :style="bodyStyle">
      <LeftDrawToolbar :active-tool="activeTool" @set-tool="onSetTool" />

      <div class="tv-center">
        <PriceChartPane
          ref="paneRef"
          :scope="scope"
          :data-mode="dataMode"
          :theme="layout.theme"
          :active-tool="activeTool"
          :drawings="drawings"
          :selected-drawing-id="selectedDrawingId"
          :indicators="layout.indicators"
          :show-channel-debug="channelDebug"
          :channel-state="channelState"
          :reversal-state="reversalState"
          @set-drawings="onSetDrawings"
          @select-drawing="onSelectDrawing"
          @channel-view-change="onChannelViewChange"
          @reversal-view-change="onReversalViewChange"
        />
      </div>

      <div
        class="tv-col-resizer"
        :class="{ disabled: !layout.panes.right_watchlist_open }"
        title="左右拖拽调整图表区/观察列表宽度"
        @pointerdown="startResizeWatchlist"
      ></div>

      <div class="tv-right-sidebar">
        <WatchlistPanel
          :open="layout.panes.right_watchlist_open"
          :items="watchlist"
          :current="scope.symbol"
          :quote-snapshot="quoteSnapshot"
          :quote-ticks="quoteTicks"
          :drawings="objectTreeRows"
          :channels="channelState"
          :reversal="{
            settings: reversalState.settings,
            results: reversalState.results,
            selected_id: reversalState.selected_id || ''
          }"
          :lightweight-only="lightweightOnly"
          :selected-drawing-id="selectedDrawingId"
          :active-tab="activeRightTab"
          @select="onSelectWatch"
          @set-active-tab="activeRightTab = $event"
          @select-drawing="onSelectDrawing"
          @toggle-drawing-visible="onToggleDrawingVisible"
          @delete-drawing="onDeleteDrawingById"
          @channel-action="onChannelAction"
          @channel-settings="onChannelSettings"
          @reversal-action="onReversalAction"
        />
      </div>
    </div>

    <TradeDockWindow
      :visible="tradeWindow.visible"
      :x="tradeWindow.x"
      :y="tradeWindow.y"
      :width="tradeWindow.width"
      :height="tradeWindow.height"
      :dragging="tradeWindow.dragging"
      :resizing="tradeWindow.resizing"
      :active-tab="tradeWindow.activeTab"
      :order-form="tradeForm"
      :terminal="tradeTerminal"
      :quote-snapshot="quoteSnapshot"
      @close="closeTradeWindow"
      @start-drag="startTradeWindowDrag"
      @start-resize="startTradeWindowResize"
      @set-tab="tradeWindow.activeTab = $event"
      @update-order-field="updateTradeFormField"
      @cancel-order="cancelTradeOrder"
      @amend-order="onAmendOrder"
      @quick-order="quickTradeOrder"
      @position-close="onPositionClose"
      @position-reverse="onPositionReverse"
      @adjust-cashflow="adjustCashflow"
      @adjust-fee="adjustFee"
    />

    <KeyboardSprite
      :visible="keyboardSprite.visible"
      :query="keyboardSprite.query"
      :items="keyboardSprite.items"
      :loading="keyboardSprite.loading"
      :active-index="keyboardSprite.activeIndex"
      @select="chooseKeyboardSprite"
      @close="resetKeyboardSprite"
      @set-active-index="keyboardSprite.activeIndex = $event"
    />
  </div>
</template>
