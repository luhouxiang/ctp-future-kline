<script setup>
const props = defineProps({
  visible: { type: Boolean, default: false },
  query: { type: String, default: '' },
  items: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  activeIndex: { type: Number, default: 0 },
})

const emit = defineEmits(['close', 'select', 'set-active-index'])

function onChoose(item) {
  emit('select', item)
}
</script>

<template>
  <div v-if="props.visible" class="tv-keyboard-sprite" @click.stop>
    <div class="tv-keyboard-sprite-head">
      <div class="tv-keyboard-sprite-title">键盘精灵</div>
      <button class="tv-icon-btn" title="关闭" @click="emit('close')">×</button>
    </div>
    <div class="tv-keyboard-sprite-query">{{ props.query || '_' }}</div>
    <div class="tv-keyboard-sprite-hint">Enter 选中 · Esc 关闭 · ↑↓ 切换</div>
    <div class="tv-keyboard-sprite-list">
      <div v-if="props.loading" class="tv-keyboard-sprite-empty">检索中...</div>
      <div v-else-if="!props.items.length" class="tv-keyboard-sprite-empty">无匹配结果</div>
      <button
        v-for="(item, idx) in props.items"
        :key="`${item.type}-${item.symbol}-${idx}`"
        class="tv-keyboard-sprite-item"
        :class="{ active: idx === props.activeIndex }"
        @mouseenter="emit('set-active-index', idx)"
        @click="onChoose(item)"
      >
        <span class="sprite-main">
          <span class="sprite-symbol">{{ item.symbol }}</span>
          <span class="sprite-type">{{ item.type }}</span>
        </span>
        <span class="sprite-sub">
          <span>{{ item.variety || '--' }}</span>
          <span>{{ item.exchange || '--' }}</span>
          <span>{{ item.bar_count || 0 }}</span>
        </span>
      </button>
    </div>
  </div>
</template>
