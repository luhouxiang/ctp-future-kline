<script setup>
import { reactive, ref, watch } from 'vue'

const props = defineProps({
  symbol: { type: String, default: '' },
  type: { type: String, default: '' },
  variety: { type: String, default: '' },
  timeframe: { type: String, default: '1m' },
  saveStatus: { type: String, default: 'idle' },
  activeTool: { type: String, default: 'cursor' },
  theme: { type: String, default: 'dark' },
  channelDebug: { type: Boolean, default: false },
  reversalSettings: { type: Object, default: () => ({}) },
})

const emit = defineEmits([
  'set-timeframe',
  'set-theme',
  'set-tool',
  'delete-selected',
  'toggle-channel-debug',
  'toggle-reversal',
  'set-reversal-settings',
  'recalc-reversal',
])

const frames = ['1m', '5m', '15m', '1h', '1d']
const reversalPanelOpen = ref(false)
const draft = reactive({
  midTrendMinBars: 50,
  midTrendMaxBars: 2000,
  pivotKMinute: 3,
  pivotKHour: 5,
  pivotKDay: 8,
  lineToleranceAtrFactor: 1,
  breakThresholdPct: 3,
  minSwingAmplitudeAtr: 1,
  showLabels: true,
})

watch(
  () => props.reversalSettings,
  (v) => {
    const s = v || {}
    draft.midTrendMinBars = Number(s.midTrendMinBars ?? 50)
    draft.midTrendMaxBars = Number(s.midTrendMaxBars ?? 2000)
    draft.pivotKMinute = Number(s.pivotKMinute ?? 3)
    draft.pivotKHour = Number(s.pivotKHour ?? 5)
    draft.pivotKDay = Number(s.pivotKDay ?? 8)
    draft.lineToleranceAtrFactor = Number(s.lineToleranceAtrFactor ?? 1)
    draft.breakThresholdPct = Number(s.breakThresholdPct ?? 3)
    draft.minSwingAmplitudeAtr = Number(s.minSwingAmplitudeAtr ?? 1)
    draft.showLabels = s.showLabels !== false
  },
  { deep: true, immediate: true },
)

function applyReversalSettings(force = false) {
  emit('set-reversal-settings', {
    settings: {
      ...draft,
      enabled: !!props.reversalSettings?.enabled,
      confirmOnClose: true,
    },
    force,
  })
}
</script>

<template>
  <div class="tv-topbar">
    <div class="tv-brand">{{ props.symbol || '--' }} · {{ props.type }} <small>{{ props.variety }}</small></div>
    <div class="tv-group">
      <button v-for="f in frames" :key="f" class="tv-btn" :class="{ active: props.timeframe === f }" @click="emit('set-timeframe', f)">{{ f }}</button>
    </div>
    <div class="tv-group">
      <button class="tv-btn" :class="{ active: props.activeTool === 'cursor' }" @click="emit('set-tool', 'cursor')">光标</button>
      <button class="tv-btn danger" @click="emit('delete-selected')">删除</button>
      <button class="tv-btn" :class="{ active: props.channelDebug }" @click="emit('toggle-channel-debug')">通道调试</button>
      <div class="tv-reversal-wrap">
        <button class="tv-btn" :class="{ active: props.reversalSettings?.enabled }" @click="emit('toggle-reversal')">中趋势反转</button>
        <button class="tv-btn" @click="reversalPanelOpen = !reversalPanelOpen">参数</button>
        <div v-if="reversalPanelOpen" class="tv-reversal-panel">
          <label>最小窗口<input type="number" :value="draft.midTrendMinBars" @change="draft.midTrendMinBars = Number($event.target.value || 50)" /></label>
          <label>最大窗口<input type="number" :value="draft.midTrendMaxBars" @change="draft.midTrendMaxBars = Number($event.target.value || 2000)" /></label>
          <label>pivot_m<input type="number" :value="draft.pivotKMinute" @change="draft.pivotKMinute = Number($event.target.value || 3)" /></label>
          <label>pivot_h<input type="number" :value="draft.pivotKHour" @change="draft.pivotKHour = Number($event.target.value || 5)" /></label>
          <label>pivot_d<input type="number" :value="draft.pivotKDay" @change="draft.pivotKDay = Number($event.target.value || 8)" /></label>
          <label>线容差ATR<input type="number" step="0.1" :value="draft.lineToleranceAtrFactor" @change="draft.lineToleranceAtrFactor = Number($event.target.value || 1)" /></label>
          <label>突破阈值%<input type="number" step="0.1" :value="draft.breakThresholdPct" @change="draft.breakThresholdPct = Number($event.target.value || 3)" /></label>
          <label>最小摆幅ATR<input type="number" step="0.1" :value="draft.minSwingAmplitudeAtr" @change="draft.minSwingAmplitudeAtr = Number($event.target.value || 1)" /></label>
          <label class="tv-reversal-check"><input type="checkbox" :checked="draft.showLabels" @change="draft.showLabels = !!$event.target.checked" />显示标签</label>
          <div class="tv-reversal-actions">
            <button class="tv-btn" @click="applyReversalSettings(false)">应用</button>
            <button class="tv-btn" @click="applyReversalSettings(true); emit('recalc-reversal')">立即重算</button>
          </div>
        </div>
      </div>
      <button class="tv-btn" :class="{ active: props.theme === 'dark' }" @click="emit('set-theme', 'dark')">深色</button>
      <button class="tv-btn" :class="{ active: props.theme === 'light' }" @click="emit('set-theme', 'light')">浅色</button>
    </div>
    <div class="tv-save">保存状态: {{ props.saveStatus }}</div>
  </div>
</template>
