<script setup>
const props = defineProps({ activeTool: { type: String, default: 'cursor' } })
const emit = defineEmits(['set-tool'])

const tools = [
  { id: 'cursor', label: '选择' },
  { id: 'hline', label: '水平线' },
  { id: 'vline', label: '垂直线' },
  { id: 'trendline', label: '趋势线' },
  { id: 'rect', label: '矩形' },
  { id: 'text', label: '文本' },
]
</script>

<template>
  <aside class="tv-left-tools">
    <button
      v-for="t in tools"
      :key="t.id"
      class="tv-tool-btn"
      :class="{ active: props.activeTool === t.id }"
      @click="emit('set-tool', t.id)"
    >
      {{ t.label }}
    </button>
  </aside>
</template>
