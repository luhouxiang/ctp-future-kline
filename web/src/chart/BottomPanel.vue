<script setup>
const props = defineProps({
  open: { type: Boolean, default: true },
  logs: { type: Array, default: () => [] },
  stats: { type: Object, default: () => ({ bars: 0, symbol: '', loading: false }) },
  replay: { type: Object, default: () => ({}) },
})
const emit = defineEmits(['toggle'])
</script>

<template>
  <section class="tv-bottom" :class="{ collapsed: !props.open }">
    <div class="tv-bottom-head">
      <strong>底部面板</strong>
      <button class="tv-btn" @click="emit('toggle')">{{ props.open ? '收起' : '展开' }}</button>
    </div>
    <div v-if="props.open" class="tv-bottom-body">
      <div class="tv-bottom-col">
        <h4>行情状态</h4>
        <p>Symbol: {{ props.stats.symbol || '--' }}</p>
        <p>Bars: {{ props.stats.bars || 0 }}</p>
        <p>Loading: {{ props.stats.loading ? '是' : '否' }}</p>
      </div>
      <div class="tv-bottom-col">
        <h4>回放状态</h4>
        <p>Status: {{ props.replay.status || '--' }}</p>
        <p>Task: {{ props.replay.task_id || '--' }}</p>
        <p>Dispatched: {{ props.replay.dispatched || 0 }}</p>
      </div>
      <div class="tv-bottom-col">
        <h4>事件日志</h4>
        <div class="tv-log-list">
          <div v-for="(line, idx) in props.logs" :key="idx">{{ line }}</div>
        </div>
      </div>
    </div>
  </section>
</template>
