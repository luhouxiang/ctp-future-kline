<script setup>
import { CandlestickSeries, createChart, HistogramSeries, LineSeries } from 'lightweight-charts'
import { FRONTEND_VERSION } from './version'
import { onMounted, onUnmounted, reactive, ref } from 'vue'

const CHUNK_SIZE = 2000
const APP_VERSION = FRONTEND_VERSION

const state = reactive({
  loading: false,
  loadingMore: false,
  hasMore: true,
  error: '',
  meta: {
    symbol: '',
    type: '',
    variety: '',
    exchange: '',
  },
  bars: [],
  selected: {
    timeText: '--',
    open: '--',
    high: '--',
    low: '--',
    close: '--',
    volume: '--',
    openInterest: '--',
  },
  debug: {
    symbol: '',
    type: '',
    start: '',
    end: '',
    lastRequestEnd: '',
    lastBars: 0,
  },
})

const candleRef = ref(null)
const macdRef = ref(null)
const volumeRef = ref(null)
const chartRefs = {
  candle: null,
  macd: null,
  volume: null,
}
const seriesRefs = {
  candle: null,
  ma20: null,
  volume: null,
  dif: null,
  dea: null,
  hist: null,
}
let isSyncing = false
let crosshairHandler = null
const DISPLAY_TZ_OFFSET_SECONDS = 8 * 60 * 60

function getParams() {
  const params = new URLSearchParams(location.search)
  const out = {
    symbol: params.get('symbol') || '',
    type: params.get('type') || '',
    variety: params.get('variety') || '',
    start: params.get('start') || '',
    end: params.get('end') || '',
  }
  state.debug.symbol = out.symbol
  state.debug.type = out.type
  state.debug.start = out.start
  state.debug.end = out.end
  console.log('[ChartView] URL params', out)
  return out
}

function buildChart(container, height) {
  return createChart(container, {
    width: container.clientWidth,
    height,
    layout: {
      background: { color: '#f8fbff' },
      textColor: '#1c2b3a',
    },
    grid: {
      vertLines: { color: '#e3ebf2' },
      horzLines: { color: '#e3ebf2' },
    },
    rightPriceScale: {
      borderColor: '#d2dfeb',
    },
    localization: {
      timeFormatter: (time) => formatTimeInUTC8(normalizeTimeToUnix(time)),
    },
    timeScale: {
      borderColor: '#d2dfeb',
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: (time) => formatTickInUTC8(normalizeTimeToUnix(time)),
    },
  })
}

function syncCharts() {
  const link = (source, target1, target2) => {
    source.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (!range || isSyncing) return
      isSyncing = true
      target1.timeScale().setVisibleLogicalRange(range)
      target2.timeScale().setVisibleLogicalRange(range)
      isSyncing = false

      if (range.from < 30) {
        void loadOlderChunk()
      }
    })
  }
  link(chartRefs.candle, chartRefs.macd, chartRefs.volume)
  link(chartRefs.macd, chartRefs.candle, chartRefs.volume)
  link(chartRefs.volume, chartRefs.candle, chartRefs.macd)
}

function initCharts() {
  chartRefs.candle = buildChart(candleRef.value, 420)
  chartRefs.macd = buildChart(macdRef.value, 180)
  chartRefs.volume = buildChart(volumeRef.value, 180)

  seriesRefs.candle = chartRefs.candle.addSeries(CandlestickSeries, {
    upColor: '#e53935',
    downColor: '#2e7d32',
    borderVisible: false,
    wickUpColor: '#e53935',
    wickDownColor: '#2e7d32',
  })
  seriesRefs.ma20 = chartRefs.candle.addSeries(LineSeries, {
    color: '#c2185b',
    lineWidth: 1,
    crosshairMarkerVisible: false,
  })
  seriesRefs.volume = chartRefs.volume.addSeries(HistogramSeries, {
    color: '#6b88a6',
    priceFormat: { type: 'volume' },
  })
  seriesRefs.hist = chartRefs.macd.addSeries(HistogramSeries, {
    color: '#90caf9',
  })
  seriesRefs.dif = chartRefs.macd.addSeries(LineSeries, {
    color: '#ff7043',
    lineWidth: 2,
  })
  seriesRefs.dea = chartRefs.macd.addSeries(LineSeries, {
    color: '#42a5f5',
    lineWidth: 2,
  })

  syncCharts()
  bindCrosshairLegend()
}

function bindCrosshairLegend() {
  if (!chartRefs.candle || !seriesRefs.candle) return
  crosshairHandler = (param) => {
    if (!param || !param.seriesData) return
    const candle = param.seriesData.get(seriesRefs.candle)
    if (!candle || typeof candle.time !== 'number') return
    applySelectedByTime(candle.time, candle)
  }
  chartRefs.candle.subscribeCrosshairMove(crosshairHandler)
}

function unbindCrosshairLegend() {
  if (chartRefs.candle && crosshairHandler) {
    chartRefs.candle.unsubscribeCrosshairMove(crosshairHandler)
  }
  crosshairHandler = null
}

function resizeCharts() {
  if (!chartRefs.candle) return
  chartRefs.candle.applyOptions({ width: candleRef.value.clientWidth })
  chartRefs.macd.applyOptions({ width: macdRef.value.clientWidth })
  chartRefs.volume.applyOptions({ width: volumeRef.value.clientWidth })
}

function normalizeTimeToUnix(timeValue) {
  if (typeof timeValue === 'number') return timeValue
  if (typeof timeValue === 'string') return Math.floor(Date.parse(`${timeValue}T00:00:00Z`) / 1000)
  if (timeValue && typeof timeValue === 'object' && 'year' in timeValue && 'month' in timeValue && 'day' in timeValue) {
    return Math.floor(Date.UTC(timeValue.year, timeValue.month - 1, timeValue.day, 0, 0, 0) / 1000)
  }
  return 0
}

function formatTimeInUTC8(unixTs, withDate = true) {
  if (!unixTs) return '--'
  const d = new Date((unixTs + DISPLAY_TZ_OFFSET_SECONDS) * 1000)
  const pad = (n) => String(n).padStart(2, '0')
  if (!withDate) return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
}

function formatTickInUTC8(unixTs) {
  if (!unixTs) return ''
  return formatTimeInUTC8(unixTs, false)
}

function formatEndParamByUnix(unixTs) {
  return formatTimeInUTC8(unixTs)
}

function formatDisplayTime(unixTs) {
  return formatTimeInUTC8(unixTs)
}

function formatNumber(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return '--'
  return Number(v).toFixed(digits)
}

function formatInt(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return '--'
  return Number(v).toLocaleString('zh-CN')
}

function applySelectedByTime(ts, candleData) {
  const bar = state.bars.find((x) => x.time === ts)
  const openValue = candleData?.open ?? bar?.open
  const highValue = candleData?.high ?? bar?.high
  const lowValue = candleData?.low ?? bar?.low
  const closeValue = candleData?.close ?? bar?.close
  const volumeValue = bar?.volume
  const oiValue = bar?.open_interest

  const displayTs = bar?.data_time ?? ts
  state.selected.timeText = formatDisplayTime(displayTs)
  state.selected.open = formatNumber(openValue)
  state.selected.high = formatNumber(highValue)
  state.selected.low = formatNumber(lowValue)
  state.selected.close = formatNumber(closeValue)
  state.selected.volume = formatInt(volumeValue)
  state.selected.openInterest = formatInt(oiValue)
}

function selectLatestBar() {
  const n = state.bars.length
  if (n === 0) {
    state.selected.timeText = '--'
    state.selected.open = '--'
    state.selected.high = '--'
    state.selected.low = '--'
    state.selected.close = '--'
    state.selected.volume = '--'
    state.selected.openInterest = '--'
    return
  }
  const bar = state.bars[n - 1]
  applySelectedByTime(bar.time, {
    time: bar.time,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  })
}

function buildCandleData(bars) {
  return bars.map((bar) => ({
    time: bar.time,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }))
}

function buildVolumeData(bars) {
  return bars.map((bar) => ({
    time: bar.time,
    value: bar.volume,
    color: bar.close >= bar.open ? '#e53935' : '#2e7d32',
  }))
}

function buildMA20Data(bars) {
  const out = []
  let sum = 0
  for (let i = 0; i < bars.length; i += 1) {
    sum += bars[i].close
    if (i >= 20) {
      sum -= bars[i - 20].close
    }
    out.push({
      time: bars[i].time,
      value: i >= 19 ? sum / 20 : null,
    })
  }
  return out
}

function calcMACDLocally(bars) {
  const closes = bars.map((bar) => bar.close)
  if (closes.length === 0) {
    return { difData: [], deaData: [], histData: [] }
  }
  const shortK = 2 / 13
  const longK = 2 / 27
  const signalK = 2 / 10
  let shortEMA = closes[0]
  let longEMA = closes[0]
  let signalEMA = 0

  const difData = []
  const deaData = []
  const histData = []
  for (let i = 0; i < closes.length; i += 1) {
    const close = closes[i]
    if (i > 0) {
      shortEMA = close * shortK + shortEMA * (1 - shortK)
      longEMA = close * longK + longEMA * (1 - longK)
    }
    const dif = shortEMA - longEMA
    if (i === 0) {
      signalEMA = dif
    } else {
      signalEMA = dif * signalK + signalEMA * (1 - signalK)
    }
    const hist = (dif - signalEMA) * 2
    const ts = bars[i].time
    difData.push({ time: ts, value: dif })
    deaData.push({ time: ts, value: signalEMA })
    histData.push({ time: ts, value: hist, color: hist >= 0 ? '#4fc3f7' : '#ef9a9a' })
  }
  return { difData, deaData, histData }
}

function renderSeries() {
  const bars = state.bars
  seriesRefs.candle.setData(buildCandleData(bars))
  seriesRefs.ma20.setData(buildMA20Data(bars))
  seriesRefs.volume.setData(buildVolumeData(bars))
  const { difData, deaData, histData } = calcMACDLocally(bars)
  seriesRefs.dif.setData(difData)
  seriesRefs.dea.setData(deaData)
  seriesRefs.hist.setData(histData)
  selectLatestBar()
}

async function fetchChunk(endParam, isInitial) {
  const p = getParams()
  const query = new URLSearchParams({
    symbol: p.symbol,
    type: p.type,
    variety: p.variety || '',
    end: endParam,
    limit: String(CHUNK_SIZE),
  })
  console.log('[ChartView] fetch /api/kline/bars', {
    symbol: p.symbol,
    type: p.type,
    variety: p.variety || '',
    end: endParam,
    limit: CHUNK_SIZE,
    isInitial,
  })
  state.debug.lastRequestEnd = endParam
  const resp = await fetch(`/api/kline/bars?${query}`)
  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status}`)
  }
  const data = await resp.json()
  console.log('[ChartView] fetch result', {
    symbol: p.symbol,
    type: p.type,
    bars: (data.bars || []).length,
    meta: data.meta,
  })
  state.debug.lastBars = (data.bars || []).length
  if (isInitial) {
    state.meta = data.meta || state.meta
  }
  return data.bars || []
}

async function loadInitialChunk() {
  const p = getParams()
  if (!p.symbol || !p.type) {
    state.error = '缺少 symbol 或 type 参数'
    return
  }
  state.loading = true
  state.error = ''
  try {
    const endParam = p.end || formatEndParamByUnix(Math.floor(Date.now() / 1000))
    const bars = await fetchChunk(endParam, true)
    state.bars = bars
    state.hasMore = bars.length >= CHUNK_SIZE
    if (bars.length === 0) {
      state.error = '当前参数未查询到K线数据'
    }
    renderSeries()
  } catch (error) {
    state.error = `加载图表失败: ${error.message}`
  } finally {
    state.loading = false
  }
}

async function loadOlderChunk() {
  if (state.loading || state.loadingMore || !state.hasMore || state.bars.length === 0) {
    return
  }
  state.loadingMore = true
  try {
    const oldest = state.bars[0].time
    const nextEnd = formatEndParamByUnix(oldest - 60)
    const olderBars = await fetchChunk(nextEnd, false)
    if (olderBars.length === 0) {
      state.hasMore = false
      return
    }
    const seen = new Set(state.bars.map((x) => x.time))
    const toPrepend = olderBars.filter((x) => !seen.has(x.time))
    if (toPrepend.length === 0) {
      state.hasMore = false
      return
    }
    state.bars = [...toPrepend, ...state.bars]
    state.hasMore = olderBars.length >= CHUNK_SIZE
    renderSeries()
  } catch (error) {
    state.error = `加载更多失败: ${error.message}`
  } finally {
    state.loadingMore = false
  }
}

function disposeCharts() {
  unbindCrosshairLegend()
  for (const key of Object.keys(chartRefs)) {
    if (chartRefs[key]) {
      chartRefs[key].remove()
      chartRefs[key] = null
    }
  }
}

onMounted(async () => {
  try {
    initCharts()
    await loadInitialChunk()
  } catch (error) {
    console.error('[ChartView] init failed', error)
    state.error = `图表初始化失败: ${error.message || error}`
  }
  window.addEventListener('resize', resizeCharts)
})

onUnmounted(() => {
  window.removeEventListener('resize', resizeCharts)
  disposeCharts()
})
</script>

<template>
  <div class="chart-page">
    <div class="chart-header">
      <h2>{{ state.meta.symbol || 'K线图表' }} · {{ state.meta.type }} <small>v{{ APP_VERSION.replace('v', '') }}</small></h2>
      <div class="chart-meta quote-line">
        <span>时间: {{ state.selected.timeText }}</span>
        <span>开: {{ state.selected.open }}</span>
        <span>高: {{ state.selected.high }}</span>
        <span>低: {{ state.selected.low }}</span>
        <span>收: {{ state.selected.close }}</span>
        <span>量: {{ state.selected.volume }}</span>
        <span>持仓: {{ state.selected.openInterest }}</span>
      </div>
      <p v-if="state.loading">加载中...</p>
      <p v-if="state.loadingMore">向前加载更多...</p>
      <p v-if="state.error" class="error-text">{{ state.error }}</p>
    </div>
    <div ref="candleRef" class="chart-box main"></div>
    <div ref="macdRef" class="chart-box sub"></div>
    <div ref="volumeRef" class="chart-box sub"></div>
  </div>
</template>
