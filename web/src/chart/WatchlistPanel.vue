<script setup>
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { DEFAULT_CHANNEL_SETTINGS, normalizeChannelSettingsV2 } from './analysis/channelDetector'

const props = defineProps({
  open: { type: Boolean, default: true },
  items: { type: Array, default: () => [] },
  current: { type: String, default: '' },
  drawings: { type: Array, default: () => [] },
  selectedDrawingId: { type: String, default: '' },
  activeTab: { type: String, default: 'watchlist' },
  channels: { type: Object, default: () => ({ rows: [], selected_id: '', settings: {}, detail: null }) },
})

const emit = defineEmits([
  'toggle',
  'select',
  'set-active-tab',
  'select-drawing',
  'toggle-drawing-visible',
  'delete-drawing',
  'channel-action',
  'channel-settings',
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

const filteredChannels = computed(() => {
  let rows = Array.isArray(props.channels?.rows) ? props.channels.rows.slice() : []
  if (filterStatus.value !== 'all') rows = rows.filter((x) => String(x.status || '') === filterStatus.value)
  if (filterMethod.value !== 'all') rows = rows.filter((x) => String(x.method || '') === filterMethod.value)
  rows = rows.filter((x) => Number(x.score || 0) >= Number(scoreMin.value || 0))
  return rows
})

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
</script>

<template>
  <aside class="tv-watchlist" :class="{ collapsed: !props.open }">
    <div class="tv-watchlist-head">
      <div v-if="props.open" class="tv-watchlist-tabs">
        <button class="tv-watch-tab" :class="{ active: props.activeTab === 'watchlist' }" @click="emit('set-active-tab', 'watchlist')">
          观察列表
        </button>
        <button class="tv-watch-tab" :class="{ active: props.activeTab === 'object_tree' }" @click="emit('set-active-tab', 'object_tree')">
          对象树
        </button>
        <button class="tv-watch-tab" :class="{ active: props.activeTab === 'channel' }" @click="emit('set-active-tab', 'channel')">
          通道
        </button>
      </div>
      <button class="tv-btn" @click="emit('toggle')">{{ props.open ? '收起' : '展开' }}</button>
    </div>

    <div v-if="props.open && props.activeTab === 'watchlist'" class="tv-watchlist-body">
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

    <div v-else-if="props.open && props.activeTab === 'object_tree'" class="tv-watchlist-body tv-object-tree">
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

    <div v-else-if="props.open" class="tv-watchlist-body tv-channel-tab">
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
  </aside>
</template>

