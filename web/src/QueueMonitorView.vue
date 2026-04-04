<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { FRONTEND_VERSION } from './version'

const summary = reactive({
  total_queues: 0,
  active_alerts: 0,
  critical_queues: 0,
  spilling_queues: 0,
})
const queues = ref([])
const alerts = ref([])
const wsConnected = ref(false)
const loading = ref(false)
let ws
let pollTimer = null

const criticalQueues = computed(() => queues.value.filter((q) => q.criticality === 'critical'))
const sidecarQueues = computed(() => queues.value.filter((q) => q.criticality !== 'critical'))

function levelClass(level) {
  switch (String(level || 'normal')) {
    case 'emergency':
      return 'queue-level emergency'
    case 'critical':
      return 'queue-level critical'
    case 'warn':
      return 'queue-level warn'
    default:
      return 'queue-level normal'
  }
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString('zh-CN')
}

function applyPayload(payload) {
  if (!payload) return
  Object.assign(summary, payload.summary || {})
  queues.value = Array.isArray(payload.queues) ? payload.queues : []
  alerts.value = Array.isArray(payload.alerts) ? payload.alerts : []
}

async function fetchQueues() {
  loading.value = true
  try {
    const resp = await fetch('/api/queues')
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    applyPayload(await resp.json())
  } finally {
    loading.value = false
  }
}

function connectWS() {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  ws = new WebSocket(`${proto}://${window.location.host}/ws`)
  ws.onopen = () => {
    wsConnected.value = true
  }
  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data)
      if (msg.type === 'queue_status_update' && msg.data) {
        applyPayload(msg.data)
      }
    } catch {
      // noop
    }
  }
  ws.onclose = () => {
    wsConnected.value = false
    if (ws) {
      ws.onclose = null
      ws = null
    }
    setTimeout(connectWS, 1500)
  }
}

onMounted(() => {
  fetchQueues().catch(() => {})
  connectWS()
  pollTimer = setInterval(() => {
    fetchQueues().catch(() => {})
  }, 5000)
})

onUnmounted(() => {
  if (pollTimer) clearInterval(pollTimer)
  if (ws) ws.close()
})
</script>

<template>
  <div class="container queue-monitor-page">
    <div class="panel">
      <h2>队列监控</h2>
      <p>版本号: {{ FRONTEND_VERSION }} | WebSocket: {{ wsConnected ? '已连接' : '未连接' }} | {{ loading ? '刷新中' : '已同步' }}</p>
      <div class="row">
        <a href="/">返回控制台</a>
      </div>
      <div class="status-grid">
        <div class="status-item">队列总数: {{ formatNumber(summary.total_queues) }}</div>
        <div class="status-item">活跃告警: {{ formatNumber(summary.active_alerts) }}</div>
        <div class="status-item">严重队列: {{ formatNumber(summary.critical_queues) }}</div>
        <div class="status-item">溢写中队列: {{ formatNumber(summary.spilling_queues) }}</div>
      </div>
    </div>

    <div class="panel" v-if="alerts.length">
      <h3>当前告警</h3>
      <div class="queue-alert-list">
        <div v-for="item in alerts" :key="item.name" class="queue-alert-card" :class="levelClass(item.alert_level)">
          <div><strong>{{ item.name }}</strong></div>
          <div>级别: {{ item.alert_level }}</div>
          <div>深度: {{ formatNumber(item.current_depth) }} / {{ formatNumber(item.capacity) }}</div>
          <div>占用: {{ Number(item.usage_percent || 0).toFixed(1) }}%</div>
          <div>策略: {{ item.loss_policy }}</div>
        </div>
      </div>
    </div>

    <div class="panel">
      <h3>关键队列</h3>
      <div class="queue-table-wrap">
        <table class="queue-table">
          <thead>
            <tr>
              <th>队列</th>
              <th>类别</th>
              <th>级别</th>
              <th>容量</th>
              <th>当前深度</th>
              <th>高水位</th>
              <th>丢弃</th>
              <th>溢写</th>
              <th>策略</th>
              <th>依据</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in criticalQueues" :key="item.name" :class="levelClass(item.alert_level)">
              <td>{{ item.name }}</td>
              <td>{{ item.category }}</td>
              <td>{{ item.alert_level }}</td>
              <td>{{ formatNumber(item.capacity) }}</td>
              <td>{{ formatNumber(item.current_depth) }} / {{ Number(item.usage_percent || 0).toFixed(1) }}%</td>
              <td>{{ formatNumber(item.high_watermark) }}</td>
              <td>{{ formatNumber(item.drop_total) }}</td>
              <td>{{ formatNumber(item.spill_total) }} / {{ formatNumber(item.spill_depth) }}</td>
              <td>{{ item.loss_policy }}</td>
              <td>{{ item.basis_text }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h3>旁路与展示队列</h3>
      <div class="queue-table-wrap">
        <table class="queue-table">
          <thead>
            <tr>
              <th>队列</th>
              <th>类别</th>
              <th>级别</th>
              <th>容量</th>
              <th>当前深度</th>
              <th>高水位</th>
              <th>丢弃</th>
              <th>策略</th>
              <th>依据</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in sidecarQueues" :key="item.name" :class="levelClass(item.alert_level)">
              <td>{{ item.name }}</td>
              <td>{{ item.category }}</td>
              <td>{{ item.alert_level }}</td>
              <td>{{ formatNumber(item.capacity) }}</td>
              <td>{{ formatNumber(item.current_depth) }} / {{ Number(item.usage_percent || 0).toFixed(1) }}%</td>
              <td>{{ formatNumber(item.high_watermark) }}</td>
              <td>{{ formatNumber(item.drop_total) }}</td>
              <td>{{ item.loss_policy }}</td>
              <td>{{ item.basis_text }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>