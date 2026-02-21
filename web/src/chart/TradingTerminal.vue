<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import TopToolbar from './TopToolbar.vue'
import LeftDrawToolbar from './LeftDrawToolbar.vue'
import WatchlistPanel from './WatchlistPanel.vue'
import PriceChartPane from './PriceChartPane.vue'

const paneRef = ref(null)
const owner = ref('admin')

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
const activeRightTab = ref('watchlist')
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
const watchlistWidth = ref(280)
const minWatchlistWidth = 180
const maxWatchlistWidth = 520
let resizeMeta = null
let saveTimer = null
let beforeUnloadHandler = null
const DRAWING_TYPE_LABELS = {
  trendline: '趋势线',
  hline: '水平线',
  vline: '垂直线',
  rect: '矩形',
  text: '文本',
}

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
  scope.symbol = String(p.get('symbol') || '').trim().toLowerCase()
  scope.type = p.get('type') || ''
  scope.variety = p.get('variety') || ''
  scope.timeframe = p.get('timeframe') || '1m'
  scope.end = p.get('end') || ''
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
  },
  indicators: layout.indicators,
  channels: {
    settings: channelState.settings,
    decisions: channelState.decisions,
    selected_id: channelState.selected_id,
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
    const qs = new URLSearchParams({ owner: owner.value || 'admin', symbol: scope.symbol, type: scope.type, variety: scope.variety || '', timeframe: scope.timeframe || '1m' })
    const resp = await fetch(`/api/chart/layout?${qs.toString()}`)
    if (!resp.ok) return
    const data = await resp.json()
    layout.theme = data.theme || 'dark'
    layout.panes.right_watchlist_open = data.panes?.right_watchlist_open ?? true
    watchlistWidth.value = Number(data.panes?.right_watchlist_width || 280)
    if (!Number.isFinite(watchlistWidth.value)) watchlistWidth.value = 280
    if (watchlistWidth.value < minWatchlistWidth) watchlistWidth.value = minWatchlistWidth
    if (watchlistWidth.value > maxWatchlistWidth) watchlistWidth.value = maxWatchlistWidth
    layout.indicators.ma20 = data.indicators?.ma20 ?? true
    layout.indicators.macd = data.indicators?.macd ?? true
    layout.indicators.volume = data.indicators?.volume ?? true
    channelState.settings = data.channels?.settings || {}
    channelState.decisions = Array.isArray(data.channels?.decisions) ? data.channels.decisions : []
    channelState.selected_id = data.channels?.selected_id || ''
    drawings.value = (data.drawings || []).map((d) => normalizeDrawingForSave(d))
    selectedDrawingId.value = ''
  } catch {
    // ignore
  }
}

async function flushSave() {
  if (!scope.symbol || !scope.type) return
  saveStatus.value = 'saving'
  try {
    const resp = await fetch('/api/chart/layout', {
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
    navigator.sendBeacon('/api/chart/layout', new Blob([body], { type: 'application/json' }))
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
  const qs = new URLSearchParams({
    symbol: scope.symbol,
    type: scope.type,
    variety: scope.variety,
    timeframe: scope.timeframe,
  })
  history.replaceState({}, '', `/chart?${qs.toString()}`)
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

function onToggleChannelDebug() {
  channelDebug.value = !channelDebug.value
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
  paneRef.value?.applyChannelAction?.(action || null)
}

function onChannelSettings(payload) {
  paneRef.value?.applyChannelSettings?.(payload || null)
}

const bodyStyle = computed(() => {
  const rightCol = layout.panes.right_watchlist_open ? `${watchlistWidth.value}px` : '48px'
  const resizerCol = layout.panes.right_watchlist_open ? '8px' : '0px'
  return {
    gridTemplateColumns: `88px minmax(0, 1fr) ${resizerCol} ${rightCol}`,
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
    await loadLayout()
  },
)

watch(
  () => [layout.theme, layout.panes.right_watchlist_open, watchlistWidth.value, layout.indicators.ma20, layout.indicators.macd, layout.indicators.volume],
  () => {
    scheduleSave()
  },
)

onMounted(async () => {
  getParams()
  await fetchWatchlist()
  await loadLayout()
  beforeUnloadHandler = () => {
    flushSaveOnUnload()
    if (saveTimer) clearTimeout(saveTimer)
  }
  window.addEventListener('beforeunload', beforeUnloadHandler)
})

onUnmounted(() => {
  stopResizeWatchlist()
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
      :save-status="saveStatus"
      :active-tool="activeTool"
      :channel-debug="channelDebug"
      @set-timeframe="onSetTimeframe"
      @set-theme="onSetTheme"
      @set-tool="onSetTool"
      @delete-selected="onDeleteSelected"
      @toggle-channel-debug="onToggleChannelDebug"
    />

    <div class="tv-body" :style="bodyStyle">
      <LeftDrawToolbar :active-tool="activeTool" @set-tool="onSetTool" />

      <div class="tv-center">
        <PriceChartPane
          ref="paneRef"
          :scope="scope"
          :theme="layout.theme"
          :active-tool="activeTool"
          :drawings="drawings"
          :selected-drawing-id="selectedDrawingId"
          :indicators="layout.indicators"
          :show-channel-debug="channelDebug"
          :channel-state="channelState"
          @set-drawings="onSetDrawings"
          @select-drawing="onSelectDrawing"
          @channel-view-change="onChannelViewChange"
        />
      </div>

      <div
        class="tv-col-resizer"
        :class="{ disabled: !layout.panes.right_watchlist_open }"
        title="左右拖拽调整图表区/观察列表宽度"
        @pointerdown="startResizeWatchlist"
      ></div>

      <WatchlistPanel
        :open="layout.panes.right_watchlist_open"
        :items="watchlist"
        :current="scope.symbol"
        :drawings="objectTreeRows"
        :channels="channelState"
        :selected-drawing-id="selectedDrawingId"
        :active-tab="activeRightTab"
        @toggle="layout.panes.right_watchlist_open = !layout.panes.right_watchlist_open"
        @select="onSelectWatch"
        @set-active-tab="activeRightTab = $event"
        @select-drawing="onSelectDrawing"
        @toggle-drawing-visible="onToggleDrawingVisible"
        @delete-drawing="onDeleteDrawingById"
        @channel-action="onChannelAction"
        @channel-settings="onChannelSettings"
      />
    </div>
  </div>
</template>
