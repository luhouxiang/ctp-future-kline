<script setup>
const props = defineProps({
  symbol: { type: String, default: '' },
  type: { type: String, default: '' },
  variety: { type: String, default: '' },
  timeframe: { type: String, default: '1m' },
  saveStatus: { type: String, default: 'idle' },
  activeTool: { type: String, default: 'cursor' },
  theme: { type: String, default: 'dark' },
  channelDebug: { type: Boolean, default: false },
})

const emit = defineEmits(['set-timeframe', 'set-theme', 'set-tool', 'delete-selected', 'toggle-channel-debug'])

const frames = ['1m', '5m', '15m', '1h', '1d']
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
      <button class="tv-btn" :class="{ active: props.theme === 'dark' }" @click="emit('set-theme', 'dark')">深色</button>
      <button class="tv-btn" :class="{ active: props.theme === 'light' }" @click="emit('set-theme', 'light')">浅色</button>
    </div>
    <div class="tv-save">保存状态: {{ props.saveStatus }}</div>
  </div>
</template>
