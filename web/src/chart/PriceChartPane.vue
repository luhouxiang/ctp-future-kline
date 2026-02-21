<script setup>
import {
  CandlestickSeries,
  createChart,
  HistogramSeries,
  LineSeries,
  LineStyle,
  CrosshairMode,
} from "lightweight-charts";
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, watch } from "vue";
import { createDrawController } from "./draw/controller";
import { drawingToScreenGeometry } from "./draw/overlay";
import {
  DEFAULT_CHANNEL_SETTINGS,
  detectChannelsInVisibleRange,
  normalizeChannelSettingsV2,
} from "./analysis/channelDetector";

const CHUNK_SIZE = 2000;
const DISPLAY_TZ_OFFSET_SECONDS = 8 * 60 * 60;
const RESIZER_H = 12;
const MIN_CANDLE_H = 140;
const MIN_MACD_H = 70;
const MIN_VOLUME_H = 120;
const DATA_ZONE_WIDTH_PX = 64;
const COLOR_UP = "#ff2f2f";
const COLOR_DOWN_KLINE_VOLUME = "#49e9f8";
const COLOR_DOWN_MACD = "#2e7d32";

const props = defineProps({
  scope: { type: Object, required: true },
  theme: { type: String, default: "dark" },
  activeTool: { type: String, default: "cursor" },
  drawings: { type: Array, default: () => [] },
  selectedDrawingId: { type: String, default: "" },
  indicators: { type: Object, default: () => ({ ma20: true, macd: true, volume: true }) },
  showChannelDebug: { type: Boolean, default: false },
  channelState: {
    type: Object,
    default: () => ({ settings: {}, decisions: [], selected_id: "" }),
  },
});

const emit = defineEmits(["set-drawings", "select-drawing", "channel-view-change"]);

const state = reactive({
  loading: false,
  loadingMore: false,
  error: "",
  bars: [],
  macdHist: [],
  channelSegments: [],
  channelAutoCandidates: [],
  channelDecisions: [],
  selectedChannelId: "",
  channelDetail: null,
  channelPersistVersion: 0,
  channelSettings: normalizeChannelSettingsV2(DEFAULT_CHANNEL_SETTINGS),
  channelDebug: {
    lastRecalcAt: 0,
    visibleRange: { start: -1, end: -1 },
    params: null,
  },
  hasMore: true,
  resolvedSymbol: "",
});

const containerRef = ref(null);
const candleSlotRef = ref(null);
const macdSlotRef = ref(null);
const volumeSlotRef = ref(null);
const mainWrapRef = ref(null);
const candleRef = ref(null);
const macdRef = ref(null);
const volumeRef = ref(null);
const overlayRef = ref(null);
const overlayMetrics = reactive({ width: 0, height: 0 });

const paneHeights = reactive({
  candle: 420,
  macd: 180,
  volume: 180,
});
const syncCrosshair = reactive({
  visible: false,
  xCandle: 0,
  xMacd: 0,
  xVolume: 0,
  time: 0,
});
const rightValueOverlay = reactive({
  candleVisible: false,
  candleY: 0,
  candleValue: 0,
  macdVisible: false,
  macdY: 0,
  macdValue: 0,
  volumeVisible: false,
  volumeY: 0,
  volumeValue: 0,
});
const viewVersion = ref(0);

const chartRefs = { candle: null, macd: null, volume: null };
const seriesRefs = {
  candle: null,
  ma20: null,
  volume: null,
  dif: null,
  dea: null,
  hist: null,
};
let syncGuard = false;
let draggingDraw = false;
let resizeObserver = null;
let activeResize = null;
let keydownHandler = null;
let candleCrosshairHandler = null;
let macdCrosshairHandler = null;
let volumeCrosshairHandler = null;
let pointerDownMeta = null;
let dragPointerId = null;
let globalPointerMoveHandler = null;
let channelRecalcTimer = null;
let channelDrag = null;

const drawState = reactive({
  activeTool: "cursor",
  selectedId: "",
  hoveredId: "",
  drawings: [],
  dirty: false,
});
const pointerCursor = ref("");
const dragMode = ref("");
let controller = null;

watch(
  () => props.drawings,
  (val) => {
    if (!draggingDraw) drawState.drawings = JSON.parse(JSON.stringify(val || []));
    if (
      drawState.selectedId &&
      !drawState.drawings.some((d) => d.id === drawState.selectedId)
    ) {
      drawState.selectedId = "";
      emit("select-drawing", "");
    }
  },
  { deep: true, immediate: true }
);
watch(
  () => props.selectedDrawingId,
  (val) => {
    const id = val || "";
    if (drawState.selectedId === id) return;
    drawState.selectedId = id;
    drawState.hoveredId = "";
    viewVersion.value += 1;
  },
  { immediate: true }
);
watch(
  () => props.activeTool,
  (val) => {
    drawState.activeTool = val || "cursor";
    if ((val || "cursor") !== "trendline") {
      controller?.clearDraft?.();
      drawState.selectedId = "";
      drawState.hoveredId = "";
      pointerCursor.value = "";
      emit("select-drawing", "");
      viewVersion.value += 1;
    }
  },
  { immediate: true }
);
watch(
  () => [
    props.channelState?.settings,
    props.channelState?.decisions,
    props.channelState?.selected_id,
    props.scope.symbol,
    props.scope.type,
    props.scope.variety,
    props.scope.timeframe,
  ],
  () => {
    const val = props.channelState || {};
    state.channelSettings = normalizeChannelSettingsV2(val.settings || {});
    state.channelDecisions = Array.isArray(val.decisions)
      ? JSON.parse(JSON.stringify(val.decisions))
      : [];
    state.selectedChannelId = String(val.selected_id || "");
    scheduleChannelRecalc();
    publishChannelView();
  },
  { deep: true, immediate: true }
);

const overlaySize = computed(() => ({
  width: overlayMetrics.width,
  height: overlayMetrics.height,
}));
const dataZoneMaskStyle = computed(() => ({
  width: `${DATA_ZONE_WIDTH_PX}px`,
  background: props.theme !== "light" ? "#142033" : "#f8fbff",
}));

const overlayInteractive = computed(() => props.activeTool !== "cursor");

function fmtTime(unixTs) {
  if (!unixTs) return "--";
  const d = new Date((unixTs + DISPLAY_TZ_OFFSET_SECONDS) * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(
    d.getUTCHours()
  )}:${p(d.getUTCMinutes())}`;
}

function normalizeTimeToUnix(timeValue) {
  if (typeof timeValue === "number") return timeValue;
  if (typeof timeValue === "string")
    return Math.floor(Date.parse(`${timeValue}T00:00:00Z`) / 1000);
  if (timeValue && typeof timeValue === "object" && "year" in timeValue) {
    return Math.floor(
      Date.UTC(timeValue.year, timeValue.month - 1, timeValue.day, 0, 0, 0) / 1000
    );
  }
  return 0;
}

function toFiniteNumberOrNull(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function parseTimeframeSeconds(tf) {
  const s = String(tf || "")
    .trim()
    .toLowerCase();
  if (!s) return 60;
  if (s.endsWith("m")) {
    const n = Number(s.slice(0, -1));
    if (Number.isFinite(n) && n > 0) return Math.floor(n * 60);
  }
  if (s.endsWith("h")) {
    const n = Number(s.slice(0, -1));
    if (Number.isFinite(n) && n > 0) return Math.floor(n * 3600);
  }
  if (s.endsWith("d")) {
    const n = Number(s.slice(0, -1));
    if (Number.isFinite(n) && n > 0) return Math.floor(n * 86400);
  }
  return 60;
}

function timeframeBucket(tf) {
  const s = String(tf || "")
    .trim()
    .toLowerCase();
  if (!s) return "minute";
  if (s.endsWith("d")) return "day";
  if (s.endsWith("h")) return "hour";
  if (s.endsWith("m")) {
    const n = Number(s.slice(0, -1));
    if (!Number.isFinite(n) || n <= 0) return "minute";
    if (n < 60) return "minute";
    if (n < 24 * 60) return "hour";
    return "day";
  }
  return "minute";
}

function buildChart(container, height) {
  const dark = props.theme !== "light";
  return createChart(container, {
    width: container.clientWidth,
    height,
    layout: {
      background: { color: dark ? "#142033" : "#f8fbff" },
      textColor: dark ? "#d3ddec" : "#1c2b3a",
      attributionLogo: false,
    },
    grid: {
      vertLines: { color: dark ? "#1f2a3b" : "#e3ebf2" },
      horzLines: { color: dark ? "#1f2a3b" : "#e3ebf2" },
    },
    crosshair: {
      mode: CrosshairMode.Normal,
      horzLine: {
        visible: true,
        labelVisible: true,
        style: LineStyle.Dashed,
        width: 1,
        color: dark ? "#8ea5bf" : "#5c7897",
      },
      vertLine: {
        visible: true,
        labelVisible: true,
        style: LineStyle.Dashed,
        width: 1,
        color: dark ? "#8ea5bf" : "#5c7897",
      },
    },
    rightPriceScale: { borderColor: dark ? "#263449" : "#d2dfeb" },
    timeScale: {
      borderColor: dark ? "#263449" : "#d2dfeb",
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: (time) => fmtTime(normalizeTimeToUnix(time)).slice(11),
    },
    localization: { timeFormatter: (time) => fmtTime(normalizeTimeToUnix(time)) },
  });
}

function applyPaneScaleVisibility() {
  if (!chartRefs.candle || !chartRefs.macd || !chartRefs.volume) return;
  // Keep time label only on the bottom pane (volume), same as TradingView multi-pane behavior.
  chartRefs.candle.applyOptions({
    timeScale: { visible: false, rightOffset: 0 },
    rightPriceScale: {
      borderColor: props.theme !== "light" ? "#263449" : "#d2dfeb",
      minimumWidth: DATA_ZONE_WIDTH_PX,
      scaleMargins: { top: 0.06, bottom: 0.38 },
    },
  });
  chartRefs.macd.applyOptions({
    timeScale: { visible: false, rightOffset: 0 },
    rightPriceScale: {
      borderColor: props.theme !== "light" ? "#263449" : "#d2dfeb",
      minimumWidth: DATA_ZONE_WIDTH_PX,
    },
  });
  chartRefs.volume.applyOptions({
    timeScale: { visible: true, rightOffset: 0 },
    rightPriceScale: {
      borderColor: props.theme !== "light" ? "#263449" : "#d2dfeb",
      minimumWidth: DATA_ZONE_WIDTH_PX,
    },
  });
}

function normalizePaneHeights() {
  const hostH = containerRef.value?.clientHeight || 0;
  if (hostH <= 0) return;
  const fixed = RESIZER_H * 2;
  const available = Math.max(hostH - fixed, MIN_CANDLE_H + MIN_MACD_H + MIN_VOLUME_H);
  const total = paneHeights.candle + paneHeights.macd + paneHeights.volume;
  if (total <= 0) {
    paneHeights.candle = Math.floor(available * 0.62);
    paneHeights.macd = Math.floor(available * 0.23);
    paneHeights.volume = available - paneHeights.candle - paneHeights.macd;
    return;
  }
  paneHeights.candle = Math.max(
    MIN_CANDLE_H,
    Math.floor((paneHeights.candle / total) * available)
  );
  paneHeights.macd = Math.max(
    MIN_MACD_H,
    Math.floor((paneHeights.macd / total) * available)
  );
  paneHeights.volume = Math.max(
    MIN_VOLUME_H,
    available - paneHeights.candle - paneHeights.macd
  );
  const diff = available - (paneHeights.candle + paneHeights.macd + paneHeights.volume);
  paneHeights.volume += diff;
}

function applyChartSizes() {
  if (!chartRefs.candle || !candleRef.value || !macdRef.value || !volumeRef.value) return;
  chartRefs.candle.applyOptions({
    width: candleRef.value.clientWidth,
    height: Math.max(50, paneHeights.candle),
  });
  chartRefs.macd.applyOptions({
    width: macdRef.value.clientWidth,
    height: Math.max(50, paneHeights.macd),
  });
  chartRefs.volume.applyOptions({
    width: volumeRef.value.clientWidth,
    height: Math.max(50, paneHeights.volume),
  });
  refreshOverlayMetrics();
  viewVersion.value += 1;
}

function refreshOverlayMetrics() {
  const w =
    overlayRef.value?.clientWidth ||
    mainWrapRef.value?.clientWidth ||
    candleRef.value?.clientWidth ||
    0;
  const h =
    overlayRef.value?.clientHeight ||
    mainWrapRef.value?.clientHeight ||
    candleRef.value?.clientHeight ||
    Math.max(0, paneHeights.candle);
  overlayMetrics.width = Math.max(0, Math.floor(w));
  overlayMetrics.height = Math.max(0, Math.floor(h));
}

function initCharts() {
  chartRefs.candle = buildChart(candleRef.value, Math.max(100, paneHeights.candle));
  chartRefs.macd = buildChart(macdRef.value, Math.max(80, paneHeights.macd));
  chartRefs.volume = buildChart(volumeRef.value, Math.max(80, paneHeights.volume));

  seriesRefs.candle = chartRefs.candle.addSeries(CandlestickSeries, {
    upColor: COLOR_UP,
    downColor: COLOR_DOWN_KLINE_VOLUME,
    borderVisible: true,
    borderUpColor: COLOR_UP,
    borderDownColor: COLOR_DOWN_KLINE_VOLUME,
    wickUpColor: COLOR_UP,
    wickDownColor: COLOR_DOWN_KLINE_VOLUME,
  });
  seriesRefs.ma20 = chartRefs.candle.addSeries(LineSeries, {
    color: "#f5c242",
    lineWidth: 1,
    crosshairMarkerVisible: false,
    priceLineVisible: false,
    lastValueVisible: false,
  });
  seriesRefs.volume = chartRefs.volume.addSeries(HistogramSeries, {
    color: "#6b88a6",
    priceFormat: { type: "volume" },
  });
  seriesRefs.hist = chartRefs.macd.addSeries(HistogramSeries, { color: "#90caf9" });
  seriesRefs.dif = chartRefs.macd.addSeries(LineSeries, {
    color: "#ff7043",
    lineWidth: 2,
  });
  seriesRefs.dea = chartRefs.macd.addSeries(LineSeries, {
    color: "#42a5f5",
    lineWidth: 2,
  });

  const link = (source, target1, target2) => {
    source.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (!range || syncGuard) return;
      syncGuard = true;
      target1.timeScale().setVisibleLogicalRange(range);
      target2.timeScale().setVisibleLogicalRange(range);
      syncGuard = false;
      scheduleChannelRecalc();
      viewVersion.value += 1;
      if (range.from < 30) void loadOlderChunk();
    });
  };
  link(chartRefs.candle, chartRefs.macd, chartRefs.volume);
  link(chartRefs.macd, chartRefs.candle, chartRefs.volume);
  link(chartRefs.volume, chartRefs.candle, chartRefs.macd);
  applyPaneScaleVisibility();

  const updateSyncFrom = (source, param) => {
    if (source === "candle" && param?.point && Number.isFinite(Number(param.point.y))) {
      const py = Number(param.point.y);
      let p = Number.NaN;
      if (seriesRefs.candle?.coordinateToPrice) {
        p = Number(seriesRefs.candle.coordinateToPrice(py));
      }
      if (!Number.isFinite(p)) {
        p = Number(mapYToPriceWithFallback(py));
      }
      if (Number.isFinite(p)) {
        rightValueOverlay.candleVisible = true;
        rightValueOverlay.candleY = Math.round(py);
        rightValueOverlay.candleValue = p;
      } else {
        rightValueOverlay.candleVisible = false;
      }
    } else if (source === "candle") {
      rightValueOverlay.candleVisible = false;
    }

    if (source === "macd" && param?.point && Number.isFinite(Number(param.point.y))) {
      const py = Number(param.point.y);
      let p = Number.NaN;
      const s = seriesRefs.dea || seriesRefs.hist;
      if (s?.coordinateToPrice) {
        p = Number(s.coordinateToPrice(py));
      }
      if (Number.isFinite(p)) {
        rightValueOverlay.macdVisible = true;
        rightValueOverlay.macdY = Math.round(py);
        rightValueOverlay.macdValue = p;
      } else {
        rightValueOverlay.macdVisible = false;
      }
    } else if (source === "macd") {
      rightValueOverlay.macdVisible = false;
    }

    if (source === "volume" && param?.point && Number.isFinite(Number(param.point.y))) {
      const py = Number(param.point.y);
      let p = Number.NaN;
      if (seriesRefs.volume?.coordinateToPrice) {
        p = Number(seriesRefs.volume.coordinateToPrice(py));
      }
      if (Number.isFinite(p)) {
        rightValueOverlay.volumeVisible = true;
        rightValueOverlay.volumeY = Math.round(py);
        rightValueOverlay.volumeValue = p;
      } else {
        rightValueOverlay.volumeVisible = false;
      }
    } else if (source === "volume") {
      rightValueOverlay.volumeVisible = false;
    }

    if (!param || !param.time) {
      syncCrosshair.visible = false;
      syncCrosshair.time = 0;
      return;
    }
    const ts = normalizeTimeToUnix(param.time);
    if (!ts) {
      syncCrosshair.visible = false;
      syncCrosshair.time = 0;
      return;
    }
    const candleX = chartRefs.candle?.timeScale().timeToCoordinate(ts);
    const macdX = chartRefs.macd?.timeScale().timeToCoordinate(ts);
    const volumeX = chartRefs.volume?.timeScale().timeToCoordinate(ts);
    if (
      candleX === null ||
      candleX === undefined ||
      macdX === null ||
      macdX === undefined ||
      volumeX === null ||
      volumeX === undefined
    ) {
      syncCrosshair.visible = false;
      syncCrosshair.time = 0;
      return;
    }
    syncCrosshair.visible = true;
    syncCrosshair.time = ts;
    // Render helper lines on non-active panes to keep one shared time cursor.
    syncCrosshair.xMacd = source === "macd" ? -1 : macdX;
    syncCrosshair.xVolume = source === "volume" ? -1 : volumeX;
    syncCrosshair.xCandle = source === "candle" ? -1 : candleX;
  };
  candleCrosshairHandler = (param) => {
    updateSyncFrom("candle", param);
    viewVersion.value += 1;
  };
  macdCrosshairHandler = (param) => {
    updateSyncFrom("macd", param);
    viewVersion.value += 1;
  };
  volumeCrosshairHandler = (param) => {
    updateSyncFrom("volume", param);
    viewVersion.value += 1;
  };
  chartRefs.candle.subscribeCrosshairMove(candleCrosshairHandler);
  chartRefs.macd.subscribeCrosshairMove(macdCrosshairHandler);
  chartRefs.volume.subscribeCrosshairMove(volumeCrosshairHandler);
}

function rebuildChartsForTheme() {
  if (chartRefs.candle && candleCrosshairHandler) {
    chartRefs.candle.unsubscribeCrosshairMove(candleCrosshairHandler);
  }
  if (chartRefs.macd && macdCrosshairHandler) {
    chartRefs.macd.unsubscribeCrosshairMove(macdCrosshairHandler);
  }
  if (chartRefs.volume && volumeCrosshairHandler) {
    chartRefs.volume.unsubscribeCrosshairMove(volumeCrosshairHandler);
  }
  for (const key of Object.keys(chartRefs)) {
    chartRefs[key]?.remove();
    chartRefs[key] = null;
  }
  initCharts();
  applyChartSizes();
  renderSeries();
}

function buildMA20Data(bars) {
  const out = [];
  let sum = 0;
  for (let i = 0; i < bars.length; i += 1) {
    const close = Number(bars[i]?.close);
    sum += Number.isFinite(close) ? close : 0;
    if (i >= 20) {
      const drop = Number(bars[i - 20]?.close);
      sum -= Number.isFinite(drop) ? drop : 0;
    }
    const t = Number(bars[i]?.adjusted_time || 0);
    // lightweight-charts LineSeries uses whitespace points (without `value`)
    // for gaps; `value: null` can throw and break following series updates.
    if (i >= 19) out.push({ time: t, value: sum / 20 });
    else out.push({ time: t });
  }
  return out;
}

function calcMACD(bars) {
  const closes = bars.map((x) => Number(x?.close));
  if (closes.length === 0) return { dif: [], dea: [], hist: [] };
  const firstClose = Number.isFinite(closes[0]) ? closes[0] : 0;
  const shortK = 2 / 13;
  const longK = 2 / 27;
  const signalK = 2 / 10;
  let shortEMA = firstClose;
  let longEMA = firstClose;
  let signalEMA = 0;
  const dif = [];
  const dea = [];
  const hist = [];
  let lastClose = firstClose;
  for (let i = 0; i < closes.length; i += 1) {
    const close = Number.isFinite(closes[i]) ? closes[i] : lastClose;
    lastClose = close;
    if (i > 0) {
      shortEMA = close * shortK + shortEMA * (1 - shortK);
      longEMA = close * longK + longEMA * (1 - longK);
    }
    const d = shortEMA - longEMA;
    signalEMA = i === 0 ? d : d * signalK + signalEMA * (1 - signalK);
    const h = (d - signalEMA) * 2;
    const ts = Number(bars[i]?.adjusted_time || 0);
    dif.push({ time: ts, value: d });
    dea.push({ time: ts, value: signalEMA });
    hist.push({ time: ts, value: h, color: h >= 0 ? "#4fc3f7" : "#ef9a9a" });
  }
  return { dif, dea, hist };
}

function renderSeries() {
  const bars = state.bars;
  if (!seriesRefs.candle) return;
  const candleData = bars
    .map((bar) => ({
      time: Number(bar?.adjusted_time || 0),
      open: Number(bar?.open),
      high: Number(bar?.high),
      low: Number(bar?.low),
      close: Number(bar?.close),
    }))
    .filter(
      (x) =>
        Number.isFinite(x.time) &&
        Number.isFinite(x.open) &&
        Number.isFinite(x.high) &&
        Number.isFinite(x.low) &&
        Number.isFinite(x.close)
    );
  const ma20Data = buildMA20Data(bars);
  const volumeData = bars.map((bar) => ({
    time: Number(bar?.adjusted_time || 0),
    value: Number.isFinite(Number(bar?.volume)) ? Number(bar?.volume) : 0,
    color: "rgba(0,0,0,0)",
  }));

  try {
    seriesRefs.candle.setData(candleData);
  } catch (e) {
    console.error("[chart] candle setData failed", e);
  }
  try {
    seriesRefs.ma20.setData(ma20Data);
  } catch (e) {
    console.error("[chart] ma20 setData failed", e);
  }
  try {
    seriesRefs.volume.setData(volumeData);
  } catch (e) {
    console.error("[chart] volume setData failed", e);
  }
  const m = calcMACD(bars);
  state.macdHist = m.hist;
  try {
    seriesRefs.dif.setData(m.dif);
    seriesRefs.dea.setData(m.dea);
    seriesRefs.hist.setData(m.hist.map((x) => ({ ...x, color: "rgba(0,0,0,0)" })));
  } catch (e) {
    console.error("[chart] macd setData failed", e);
  }
  scheduleChannelRecalc();
  viewVersion.value += 1;
}

function currentVisibleIndexRange() {
  if (!state.bars.length) return null;
  const range = chartRefs.candle?.timeScale()?.getVisibleLogicalRange?.();
  if (
    !range ||
    !Number.isFinite(Number(range.from)) ||
    !Number.isFinite(Number(range.to))
  ) {
    return { start: 0, end: state.bars.length - 1 };
  }
  const start = Math.max(0, Math.floor(Math.min(Number(range.from), Number(range.to))));
  const end = Math.min(
    state.bars.length - 1,
    Math.ceil(Math.max(Number(range.from), Number(range.to)))
  );
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return null;
  return { start, end };
}

function channelSegmentMetrics(seg) {
  const slope = Number(seg?.slope || 0);
  const widthMean =
    (Math.abs(Number(seg?.upperStart || 0) - Number(seg?.lowerStart || 0)) +
      Math.abs(Number(seg?.upperEnd || 0) - Number(seg?.lowerEnd || 0))) *
    0.5;
  return { slope, widthMean };
}

function normalizeMethodName(method) {
  const m = String(method || "")
    .trim()
    .toLowerCase();
  if (m === "extrema") return "extrema";
  if (m === "ransac") return "ransac";
  if (m === "regression") return "regression";
  return m;
}

function methodDisplayName(method) {
  const m = normalizeMethodName(method);
  if (m === "extrema") return "极值(extrema)";
  if (m === "ransac") return "RANSAC(ransac)";
  if (m === "regression") return "回归(regression)";
  return m || "-";
}

function isMethodEnabled(method) {
  const m = normalizeMethodName(method);
  const d = state.channelSettings?.display || {};
  if (m === "extrema") return !!d.showExtrema;
  if (m === "ransac") return !!d.showRansac;
  if (m === "regression") return !!d.showRegression;
  return true;
}

function buildChannelDetailFromSegment(seg, source = "select") {
  if (!seg) return null;
  const m = channelSegmentMetrics(seg);
  return {
    id: seg.id,
    source,
    operator: "admin",
    before: m,
    after: m,
    updated_at: new Date().toISOString(),
    selected: {
      id: seg.id,
      status: seg.status || "auto",
      method: seg.method || "-",
      methodLabel: methodDisplayName(seg.method),
      qualityTier: seg.qualityTier || "-",
      score: Number(seg.score || 0),
      insideRatio: Number(seg.insideRatio || 0),
      touchCount: Number(seg.touchCount || 0),
      startTime: Number(seg.startTime || 0),
      endTime: Number(seg.endTime || 0),
      startTimeText: fmtTime(Number(seg.startTime || 0)),
      endTimeText: fmtTime(Number(seg.endTime || 0)),
      upperStart: Number(seg.upperStart || 0),
      upperEnd: Number(seg.upperEnd || 0),
      lowerStart: Number(seg.lowerStart || 0),
      lowerEnd: Number(seg.lowerEnd || 0),
      slope: Number(m.slope || 0),
      widthMean: Number(m.widthMean || 0),
      locked: !!seg.locked,
      manual: !!seg.manual,
    },
  };
}

function normalizeSegmentStatus(seg) {
  const score = Number(seg?.score || 0);
  const qualityTier = score >= 0.92 ? "A" : score >= 0.84 ? "B" : "C";
  return {
    ...seg,
    status: seg?.status || "auto",
    manual: !!seg?.manual,
    locked: !!seg?.locked,
    hidden: !!seg?.hidden,
    widthMean: Number.isFinite(Number(seg?.widthMean))
      ? Number(seg.widthMean)
      : channelSegmentMetrics(seg).widthMean,
    qualityTier: seg?.qualityTier || qualityTier,
  };
}

function segmentIoU(a, b) {
  const a0 = Number(a?.startTime ?? a?.start_time ?? 0);
  const a1 = Number(a?.endTime ?? a?.end_time ?? 0);
  const b0 = Number(b?.startTime ?? b?.start_time ?? 0);
  const b1 = Number(b?.endTime ?? b?.end_time ?? 0);
  const left = Math.max(Math.min(a0, a1), Math.min(b0, b1));
  const right = Math.min(Math.max(a0, a1), Math.max(b0, b1));
  const inter = Math.max(0, right - left);
  const union =
    Math.max(Math.max(a0, a1), Math.max(b0, b1)) -
    Math.min(Math.min(a0, a1), Math.min(b0, b1));
  if (!Number.isFinite(union) || union <= 0) return 0;
  return inter / union;
}

function mergeChannels(autoCandidates) {
  const decisions = Array.isArray(state.channelDecisions) ? state.channelDecisions : [];
  const auto = (autoCandidates || []).map((x) =>
    normalizeSegmentStatus({ ...x, status: "auto", manual: false })
  );
  const decisionByID = new Map(
    decisions.map((d) => [String(d.id || d.base_id || ""), d])
  );
  const rendered = [];
  const usedDecision = new Set();

  for (const seg of auto) {
    if (!isMethodEnabled(seg.method)) continue;
    const key = String(seg.id || seg.baseId || "");
    let decision = decisionByID.get(key);
    if (!decision && decisions.length > 0) {
      decision = decisions.find(
        (d) =>
          segmentIoU(d, seg) > 0.8 &&
          Math.abs(Number(d.slope || 0) - Number(seg.slope || 0)) <= 0.0008
      );
    }
    if (!decision) {
      rendered.push(seg);
      continue;
    }
    usedDecision.add(decision);
    if (!isMethodEnabled(decision.method || seg.method)) continue;
    const status = String(decision.status || "auto");
    if (status === "rejected") continue;
    if (status === "edited" || status === "accepted") {
      const merged = normalizeSegmentStatus({
        ...seg,
        ...decision,
        id: decision.id || seg.id,
        status,
        manual: true,
        locked: !!decision.locked,
      });
      rendered.push(merged);
      continue;
    }
    rendered.push(normalizeSegmentStatus({ ...seg, status: "auto" }));
  }

  for (const d of decisions) {
    if (usedDecision.has(d)) continue;
    if (!isMethodEnabled(d.method)) continue;
    const status = String(d.status || "auto");
    if (status === "rejected") continue;
    if (status === "accepted" || status === "edited" || d.locked) {
      rendered.push(normalizeSegmentStatus({ ...d, manual: true }));
    }
  }

  rendered.sort((a, b) => {
    const pri = (v) => (v === "edited" ? 3 : v === "accepted" ? 2 : 1);
    const pd = pri(String(b.status || "auto")) - pri(String(a.status || "auto"));
    if (pd !== 0) return pd;
    return Number(b.score || 0) - Number(a.score || 0);
  });

  const topAutoN = Number(state.channelSettings?.common?.showTopAutoN ?? 2);
  let autoSeen = 0;
  const out = [];
  for (const seg of rendered) {
    if (seg.hidden) continue;
    if (state.channelSettings?.common?.hideAuto && seg.status === "auto") continue;
    if (seg.status === "auto") {
      autoSeen += 1;
      if (autoSeen > topAutoN) continue;
    }
    out.push(seg);
  }
  return out;
}

function buildDecisionSnapshot(seg, status, source) {
  return {
    id: String(seg?.id || seg?.baseId || ""),
    base_id: String(seg?.baseId || seg?.id || ""),
    status,
    locked: !!seg?.locked,
    hidden: !!seg?.hidden,
    method: seg?.method || "",
    start_time: Number(seg?.startTime || 0),
    end_time: Number(seg?.endTime || 0),
    upper_start: Number(seg?.upperStart || 0),
    upper_end: Number(seg?.upperEnd || 0),
    lower_start: Number(seg?.lowerStart || 0),
    lower_end: Number(seg?.lowerEnd || 0),
    slope: Number(seg?.slope || 0),
    score: Number(seg?.score || 0),
    updated_at: new Date().toISOString(),
    operator: "admin",
    source: source || "",
  };
}

function upsertChannelDecision(decision) {
  const id = String(decision?.id || "");
  if (!id) return;
  const arr = Array.isArray(state.channelDecisions) ? state.channelDecisions.slice() : [];
  const idx = arr.findIndex((x) => String(x.id || x.base_id || "") === id);
  if (idx >= 0) arr[idx] = { ...arr[idx], ...decision };
  else arr.push({ ...decision });
  state.channelDecisions = arr;
  state.channelPersistVersion += 1;
}

function removeChannelDecision(id) {
  const sid = String(id || "");
  if (!sid) return;
  const before = Array.isArray(state.channelDecisions)
    ? state.channelDecisions.length
    : 0;
  state.channelDecisions = (state.channelDecisions || []).filter(
    (x) => String(x.id || x.base_id || "") !== sid
  );
  if (state.channelDecisions.length !== before) state.channelPersistVersion += 1;
}

function buildPanelRows() {
  const rows = [];
  for (const seg of state.channelSegments || []) {
    rows.push({
      id: seg.id,
      status: seg.status,
      method: seg.method,
      methodLabel: methodDisplayName(seg.method),
      score: seg.score,
      insideRatio: seg.insideRatio,
      touchCount: seg.touchCount,
      slope: seg.slope,
      widthMean: seg.widthMean,
      timeRange: `${fmtTime(seg.startTime)} ~ ${fmtTime(seg.endTime)}`,
      locked: !!seg.locked,
    });
  }
  for (const d of state.channelDecisions || []) {
    if (String(d.status || "") !== "rejected") continue;
    if (!isMethodEnabled(d.method)) continue;
    rows.push({
      id: String(d.id || d.base_id || ""),
      status: "rejected",
      method: d.method || "-",
      methodLabel: methodDisplayName(d.method),
      score: Number(d.score || 0),
      insideRatio: 0,
      touchCount: 0,
      slope: Number(d.slope || 0),
      widthMean:
        (Math.abs(Number(d.upper_start || 0) - Number(d.lower_start || 0)) +
          Math.abs(Number(d.upper_end || 0) - Number(d.lower_end || 0))) *
        0.5,
      timeRange: `${fmtTime(Number(d.start_time || 0))} ~ ${fmtTime(
        Number(d.end_time || 0)
      )}`,
      locked: !!d.locked,
    });
  }
  return rows.sort((a, b) => {
    const pri = (v) => (v === "edited" ? 4 : v === "accepted" ? 3 : v === "auto" ? 2 : 1);
    const p = pri(String(b.status || "auto")) - pri(String(a.status || "auto"));
    if (p !== 0) return p;
    return Number(b.score || 0) - Number(a.score || 0);
  });
}

function publishChannelView() {
  emit("channel-view-change", {
    rows: buildPanelRows(),
    detail: state.channelDetail || null,
    selected_id: state.selectedChannelId || "",
    settings: state.channelSettings,
    decisions: state.channelDecisions,
    persistVersion: state.channelPersistVersion,
  });
}

function recalcChannelsNow() {
  if (!state.bars.length) {
    state.channelSegments = [];
    state.channelAutoCandidates = [];
    state.channelDebug.visibleRange = { start: -1, end: -1 };
    state.channelDebug.lastRecalcAt = Date.now();
    publishChannelView();
    viewVersion.value += 1;
    return;
  }
  const vr = currentVisibleIndexRange();
  if (!vr) return;
  state.channelAutoCandidates = detectChannelsInVisibleRange({
    bars: state.bars,
    visibleStartIndex: vr.start,
    visibleEndIndex: vr.end,
    timeframe: props.scope?.timeframe || "1m",
    settings: state.channelSettings,
  });
  state.channelSegments = mergeChannels(state.channelAutoCandidates);
  if (state.selectedChannelId) {
    const sel = findSegmentByID(state.selectedChannelId);
    if (!sel) {
      state.selectedChannelId = "";
      state.channelDetail = null;
    } else {
      state.channelDetail = buildChannelDetailFromSegment(sel, "recalc-sync");
    }
  }
  state.channelDebug.lastRecalcAt = Date.now();
  state.channelDebug.visibleRange = { start: vr.start, end: vr.end };
  state.channelDebug.params = {
    timeframe: props.scope?.timeframe || "1m",
    settings: state.channelSettings,
  };
  publishChannelView();
  viewVersion.value += 1;
}

function scheduleChannelRecalc() {
  if (channelRecalcTimer) clearTimeout(channelRecalcTimer);
  channelRecalcTimer = setTimeout(() => {
    channelRecalcTimer = null;
    recalcChannelsNow();
  }, 80);
}

async function fetchChunk(endParam) {
  const query = new URLSearchParams({
    symbol: props.scope.symbol,
    type: props.scope.type,
    variety: props.scope.variety || "",
    end: endParam,
    limit: String(CHUNK_SIZE),
  });
  const resp = await fetch(`/api/kline/bars?${query.toString()}`);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const data = await resp.json();
  const bars = (data.bars || []).map((bar) => ({
    ...bar,
    adjusted_time: Number(bar.adjusted_time ?? bar.time ?? 0),
  }));
  return { bars, meta: data.meta || {} };
}

async function loadInitialChunk() {
  if (!props.scope.symbol || !props.scope.type) return;
  state.loading = true;
  state.error = "";
  try {
    const endParam = props.scope.end || fmtTime(Math.floor(Date.now() / 1000));
    const chunk = await fetchChunk(endParam);
    state.bars = chunk.bars;
    state.hasMore = chunk.bars.length >= CHUNK_SIZE;
    const metaSymbol = String(chunk.meta?.symbol || "")
      .trim()
      .toLowerCase();
    state.resolvedSymbol =
      metaSymbol ||
      String(props.scope?.symbol || "")
        .trim()
        .toLowerCase();
    renderSeries();
  } catch (err) {
    state.error = `闂傚倷绀侀幉鈥愁潖缂佹ɑ鍙忛柟顖ｇ亹瑜版帒鐐婃い鎺嗗亾闁绘劕锕弻鐔烘喆閸曨偄顫囬梺閫炲苯澧柛鐕佸灠椤曘儵宕熼浣虹Ф濡炪倖鍔戦崕? ${
      err.message || err
    }`;
  } finally {
    state.loading = false;
  }
}

async function loadOlderChunk() {
  if (state.loading || state.loadingMore || !state.hasMore || state.bars.length === 0)
    return;
  state.loadingMore = true;
  try {
    const oldest = state.bars[0].adjusted_time;
    const nextEnd = fmtTime(oldest - 60);
    const chunk = await fetchChunk(nextEnd);
    const olderBars = chunk.bars;
    if (olderBars.length === 0) {
      state.hasMore = false;
      return;
    }
    if (!state.resolvedSymbol) {
      const metaSymbol = String(chunk.meta?.symbol || "")
        .trim()
        .toLowerCase();
      if (metaSymbol) state.resolvedSymbol = metaSymbol;
    }
    const seen = new Set(state.bars.map((x) => x.adjusted_time));
    const toPrepend = olderBars.filter((x) => !seen.has(x.adjusted_time));
    if (toPrepend.length === 0) {
      state.hasMore = false;
      return;
    }
    state.bars = [...toPrepend, ...state.bars];
    state.hasMore = olderBars.length >= CHUNK_SIZE;
    renderSeries();
    scheduleChannelRecalc();
  } finally {
    state.loadingMore = false;
  }
}

function generateChannelSampleBars(kind = "parallel") {
  const sec = parseTimeframeSeconds(props.scope?.timeframe || "1m");
  const count = 320;
  const now = Math.floor(Date.now() / 1000);
  const start = now - count * sec;
  const bars = [];
  for (let i = 0; i < count; i += 1) {
    const t = start + i * sec;
    let center = 5200 + i * 0.08 + Math.sin(i * 0.12) * 0.6;
    let width = 5.5;
    if (kind === "wedge") {
      center = 5200 + i * 0.03;
      width = Math.max(0.8, 7.2 - i * 0.018);
    }
    const jitter = Math.sin(i * 0.33) * 0.15;
    let high = center + width * 0.5 + jitter;
    let low = center - width * 0.5 - jitter;
    if (kind === "outlier" && (i === 46 || i === 108 || i === 184 || i === 260)) {
      if (i % 2 === 0) high += 7.8;
      else low -= 8.5;
    }
    let open = center + Math.sin(i * 0.17) * 0.45;
    let close = center + Math.cos(i * 0.15) * 0.45;
    if (high < low) {
      const tmp = high;
      high = low;
      low = tmp;
    }
    bars.push({
      adjusted_time: t,
      data_time: t,
      open,
      high,
      low,
      close,
      volume: Math.round(900 + Math.sin(i * 0.09) * 400 + (kind === "outlier" ? 100 : 0)),
      open_interest: 0,
    });
  }
  return bars;
}

function edgeProjectionMeta() {
  const scale = chartRefs.candle?.timeScale();
  if (!scale || state.bars.length < 2) return null;
  const n = state.bars.length;
  const firstTime = Number(state.bars[0]?.adjusted_time || 0);
  const secondTime = Number(state.bars[1]?.adjusted_time || 0);
  const prevTime = Number(state.bars[n - 2]?.adjusted_time || 0);
  const lastTime = Number(state.bars[n - 1]?.adjusted_time || 0);
  const xFirst = toFiniteNumberOrNull(scale.timeToCoordinate(firstTime));
  const xSecond = toFiniteNumberOrNull(scale.timeToCoordinate(secondTime));
  const xPrev = toFiniteNumberOrNull(scale.timeToCoordinate(prevTime));
  const xLast = toFiniteNumberOrNull(scale.timeToCoordinate(lastTime));
  const stepTimeLeft =
    secondTime - firstTime || parseTimeframeSeconds(props.scope?.timeframe || "1m");
  const stepTimeRight =
    lastTime - prevTime || parseTimeframeSeconds(props.scope?.timeframe || "1m");
  if (xFirst === null || xSecond === null || xPrev === null || xLast === null)
    return null;
  const stepXLeft = xSecond - xFirst;
  const stepXRight = xLast - xPrev;
  if (
    !Number.isFinite(stepXLeft) ||
    !Number.isFinite(stepXRight) ||
    stepTimeLeft <= 0 ||
    stepTimeRight <= 0
  )
    return null;
  return {
    firstTime,
    secondTime,
    prevTime,
    lastTime,
    xFirst,
    xSecond,
    xPrev,
    xLast,
    stepTimeLeft,
    stepTimeRight,
    stepXLeft,
    stepXRight,
  };
}

function mapTimeToXWithFallback(time) {
  const scale = chartRefs.candle?.timeScale();
  if (!scale) return null;
  const raw = toFiniteNumberOrNull(scale.timeToCoordinate(time));
  if (raw !== null) return raw;
  const meta = edgeProjectionMeta();
  if (!meta || !Number.isFinite(time)) return null;
  if (time <= meta.firstTime) {
    const ratio = (time - meta.firstTime) / meta.stepTimeLeft;
    return meta.xFirst + ratio * meta.stepXLeft;
  }
  if (time >= meta.lastTime) {
    // Do not extrapolate to the right without available time scale labels.
    return meta.xLast;
  }
  return null;
}

function mapXToTimeWithFallback(x) {
  const scale = chartRefs.candle?.timeScale();
  if (!scale) return null;
  const raw = scale.coordinateToTime(x);
  const ts = normalizeTimeToUnix(raw);
  if (Number.isFinite(ts) && ts > 0) return ts;
  const meta = edgeProjectionMeta();
  if (!meta || !Number.isFinite(x)) return null;
  if (x <= meta.xFirst) {
    const ratio = (x - meta.xFirst) / meta.stepXLeft;
    return Math.round(meta.firstTime + ratio * meta.stepTimeLeft);
  }
  if (x >= meta.xLast) {
    // Clamp to max available time on the right edge.
    return Math.round(meta.lastTime);
  }
  return null;
}

function priceProjectionMeta() {
  const h = overlaySize.value?.height || candleRef.value?.clientHeight || 0;
  if (h <= 1) return null;
  const scaleApi = chartRefs.candle?.priceScale?.("right");
  const range = scaleApi?.getVisibleRange?.();
  const opts = scaleApi?.options?.();
  if (range && Number.isFinite(range.from) && Number.isFinite(range.to)) {
    const invert = !!opts?.invertScale;
    const pTop = invert ? Number(range.from) : Number(range.to);
    const pBottom = invert ? Number(range.to) : Number(range.from);
    if (Number.isFinite(pTop) && Number.isFinite(pBottom) && pTop !== pBottom) {
      return { pTop, slope: (pBottom - pTop) / Math.max(1, h - 1) };
    }
  }
  const series = seriesRefs.candle;
  if (!series) return null;
  const pTop = Number(series.coordinateToPrice(0));
  const pBottom = Number(series.coordinateToPrice(h - 1));
  if (!Number.isFinite(pTop) || !Number.isFinite(pBottom) || pTop === pBottom)
    return null;
  return { pTop, slope: (pBottom - pTop) / Math.max(1, h - 1) };
}

function mapYToPriceWithFallback(y) {
  const meta = priceProjectionMeta();
  if (!meta || !Number.isFinite(y)) return null;
  return Math.max(0, meta.pTop + meta.slope * y);
}

function mapPriceToYWithFallback(price) {
  const meta = priceProjectionMeta();
  if (!meta || !Number.isFinite(price) || meta.slope === 0) return null;
  const p = Math.max(0, price);
  return (p - meta.pTop) / meta.slope;
}

const mapper = {
  timeToX(time) {
    return mapTimeToXWithFallback(time);
  },
  xToTime(x) {
    return mapXToTimeWithFallback(x);
  },
  maxTimeX() {
    const meta = edgeProjectionMeta();
    return meta ? meta.xLast : null;
  },
  maxTime() {
    const meta = edgeProjectionMeta();
    return meta ? meta.lastTime : null;
  },
  priceToY(price) {
    return mapPriceToYWithFallback(price);
  },
  yToPrice(y) {
    return mapYToPriceWithFallback(y);
  },
  priceDeltaFromYDelta(dy) {
    const meta = priceProjectionMeta();
    if (!meta || !Number.isFinite(dy)) return null;
    return meta.slope * dy;
  },
};

function mergeDrawing(drawing) {
  const idx = drawState.drawings.findIndex((x) => x.id === drawing.id);
  if (idx >= 0) drawState.drawings[idx] = { ...drawing };
  else drawState.drawings.push({ ...drawing });
}

controller = createDrawController({
  getState: () => drawState,
  emitChange: (drawing, inPlace = false) => {
    if (drawing) mergeDrawing(drawing);
    if (!inPlace) {
      drawState.dirty = true;
      emit("set-drawings", JSON.parse(JSON.stringify(drawState.drawings)));
    }
  },
  getScope: () => ({
    symbol: props.scope.symbol,
    type: props.scope.type,
    variety: props.scope.variety || "",
    timeframe: props.scope.timeframe || "1m",
  }),
  getMapper: () => mapper,
  getSize: () => overlaySize.value,
  promptText: () => window.prompt("Input text", "") || "",
});

function pointerPos(evt) {
  const rect = overlayRef.value.getBoundingClientRect();
  return { x: evt.clientX - rect.left, y: evt.clientY - rect.top };
}

function onPointerDown(evt) {
  if (!overlayRef.value || !overlayInteractive.value) return;
  const { x, y } = pointerPos(evt);
  draggingDraw = true;
  controller.onPointerDown(x, y);
}

function onPointerMove(evt) {
  if (!overlayRef.value || !overlayInteractive.value) return;
  const draft = controller.getDraft?.();
  if (!draggingDraw && !(draft && draft.type === "trendline")) return;
  const { x, y } = pointerPos(evt);
  controller.onPointerMove(x, y);
  viewVersion.value += 1;
}

function onPointerUp(evt) {
  if (dragPointerId !== null && mainWrapRef.value?.releasePointerCapture) {
    try {
      mainWrapRef.value.releasePointerCapture(dragPointerId);
    } catch {
      // ignore
    }
  }
  dragPointerId = null;
  if (channelDrag) {
    channelDrag = null;
    pointerCursor.value = "";
    pointerDownMeta = null;
    state.channelSegments = mergeChannels(state.channelAutoCandidates);
    publishChannelView();
    viewVersion.value += 1;
    return;
  }
  const wasDragging = draggingDraw;
  if (wasDragging) {
    draggingDraw = false;
    dragMode.value = "";
    controller.onPointerUp();
    emit("set-drawings", JSON.parse(JSON.stringify(drawState.drawings)));
  }
  if (
    props.activeTool === "cursor" &&
    candleRef.value &&
    evt &&
    typeof evt.clientX === "number"
  ) {
    const rect = candleRef.value.getBoundingClientRect();
    const x = evt.clientX - rect.left;
    const y = evt.clientY - rect.top;
    if (x >= 0 && y >= 0 && x <= rect.width && y <= rect.height) {
      const info = controller.inspectAt?.(x, y);
      drawState.hoveredId = info?.id || "";
      if (info?.mode === "line") pointerCursor.value = "pointer";
      else if (info?.mode === "anchor") pointerCursor.value = "default";
      else pointerCursor.value = "";
    } else {
      drawState.hoveredId = "";
      pointerCursor.value = "";
    }
  } else if (wasDragging) {
    pointerCursor.value = "";
  }
  pointerDownMeta = null;
  viewVersion.value += 1;
}

function onDeleteSelected() {
  if (state.selectedChannelId) {
    removeChannelDecision(state.selectedChannelId);
    state.channelDetail = null;
    state.channelSegments = mergeChannels(state.channelAutoCandidates);
    publishChannelView();
    viewVersion.value += 1;
    return;
  }
  const id = controller.onDeleteSelected();
  if (!id) return;
  drawState.drawings = drawState.drawings.filter((x) => x.id !== id);
  emit("select-drawing", "");
  emit("set-drawings", JSON.parse(JSON.stringify(drawState.drawings)));
}

function clearSelection() {
  state.selectedChannelId = "";
  drawState.selectedId = "";
  drawState.hoveredId = "";
  pointerCursor.value = "";
  dragMode.value = "";
  emit("select-drawing", "");
  publishChannelView();
  viewVersion.value += 1;
}

function setSelectedDrawing(id) {
  drawState.selectedId = id || "";
  drawState.hoveredId = "";
  viewVersion.value += 1;
}

function findSegmentByID(id) {
  const sid = String(id || "");
  if (!sid) return null;
  return (state.channelSegments || []).find((x) => String(x.id || "") === sid) || null;
}

function setChannelSelected(id) {
  state.selectedChannelId = String(id || "");
  const seg = findSegmentByID(id);
  state.channelDetail = buildChannelDetailFromSegment(seg, "select");
  publishChannelView();
  viewVersion.value += 1;
}

function applyChannelAction(action) {
  if (!action || typeof action !== "object") return;
  const type = String(action.type || "");
  const id = String(action.id || state.selectedChannelId || "");
  const seg = findSegmentByID(id);
  if (type === "select") {
    setChannelSelected(id);
    return;
  }
  if (type === "focus") {
    setChannelSelected(id);
    return;
  }
  if (type === "recalc") {
    scheduleChannelRecalc();
    return;
  }
  if (type === "load-sample") {
    const sample = String(action.sample || "parallel");
    state.bars = generateChannelSampleBars(sample);
    state.hasMore = false;
    state.channelDecisions = [];
    state.channelDetail = null;
    state.selectedChannelId = "";
    renderSeries();
    scheduleChannelRecalc();
    publishChannelView();
    return;
  }
  if (type === "restore-live") {
    state.channelDetail = null;
    state.selectedChannelId = "";
    void loadInitialChunk();
    publishChannelView();
    return;
  }
  if (type === "reset-settings") {
    state.channelSettings = normalizeChannelSettingsV2(DEFAULT_CHANNEL_SETTINGS);
    state.channelPersistVersion += 1;
    scheduleChannelRecalc();
    publishChannelView();
    return;
  }
  if (type === "clear-rejected") {
    const before = state.channelDecisions.length;
    state.channelDecisions = (state.channelDecisions || []).filter(
      (x) => String(x.status || "") !== "rejected"
    );
    if (state.channelDecisions.length !== before) state.channelPersistVersion += 1;
    scheduleChannelRecalc();
    publishChannelView();
    return;
  }
  if (type === "accept-selected" || type === "reject-selected") {
    const ids = Array.isArray(action.ids) ? action.ids : [];
    for (const it of ids) {
      const s = findSegmentByID(it);
      if (!s) continue;
      const nextStatus = type === "accept-selected" ? "accepted" : "rejected";
      upsertChannelDecision(buildDecisionSnapshot(s, nextStatus, `auto->${nextStatus}`));
    }
    scheduleChannelRecalc();
    publishChannelView();
    return;
  }
  if (!seg) return;
  if (type === "accept") {
    upsertChannelDecision(
      buildDecisionSnapshot(seg, "accepted", `${seg.status || "auto"}->accepted`)
    );
    state.channelDetail = {
      id: seg.id,
      source: `${seg.status || "auto"}->accepted`,
      operator: "admin",
      before: channelSegmentMetrics(seg),
      after: channelSegmentMetrics(seg),
      updated_at: new Date().toISOString(),
    };
  } else if (type === "reject") {
    upsertChannelDecision(
      buildDecisionSnapshot(seg, "rejected", `${seg.status || "auto"}->rejected`)
    );
    state.channelDetail = {
      id: seg.id,
      source: `${seg.status || "auto"}->rejected`,
      operator: "admin",
      before: channelSegmentMetrics(seg),
      after: channelSegmentMetrics(seg),
      updated_at: new Date().toISOString(),
    };
  } else if (type === "restore") {
    removeChannelDecision(id);
  } else if (type === "toggle-lock") {
    const d =
      (state.channelDecisions || []).find(
        (x) => String(x.id || x.base_id || "") === id
      ) || buildDecisionSnapshot(seg, seg.status || "accepted", "toggle-lock");
    upsertChannelDecision({
      ...d,
      locked: !d.locked,
      updated_at: new Date().toISOString(),
    });
  } else if (type === "delete-manual") {
    removeChannelDecision(id);
  }
  scheduleChannelRecalc();
  publishChannelView();
}

function applyChannelSettings(payload) {
  const settings = normalizeChannelSettingsV2(payload?.settings || {});
  state.channelSettings = settings;
  state.channelPersistVersion += 1;
  if (payload?.force || settings?.common?.liveApply) scheduleChannelRecalc();
  publishChannelView();
}

function getChannelSegments() {
  return JSON.parse(JSON.stringify(state.channelSegments || []));
}

function getChannelPanelState() {
  return {
    rows: buildPanelRows(),
    detail: state.channelDetail || null,
    selected_id: state.selectedChannelId || "",
    settings: state.channelSettings,
    decisions: state.channelDecisions,
    persistVersion: state.channelPersistVersion,
  };
}

defineExpose({
  onDeleteSelected,
  reload: loadInitialChunk,
  setSelectedDrawing,
  getChannelSegments,
  getChannelPanelState,
  applyChannelAction,
  applyChannelSettings,
});

const channelOverlayShapes = computed(() => {
  void viewVersion.value;
  if (!state.channelSegments.length || !seriesRefs.candle) return [];
  const w = overlaySize.value?.width || 0;
  const h = overlaySize.value?.height || 0;
  if (w <= 0 || h <= 0) return [];
  return state.channelSegments
    .map((seg, idx) => {
      const t0 = Number(seg?.startTime || 0);
      const t1 = Number(seg?.endTime || 0);
      const x0 = mapTimeToXWithFallback(t0);
      const x1 = mapTimeToXWithFallback(t1);
      const yUpper0 = Number(
        seriesRefs.candle.priceToCoordinate(Number(seg?.upperStart))
      );
      const yUpper1 = Number(seriesRefs.candle.priceToCoordinate(Number(seg?.upperEnd)));
      const yLower0 = Number(
        seriesRefs.candle.priceToCoordinate(Number(seg?.lowerStart))
      );
      const yLower1 = Number(seriesRefs.candle.priceToCoordinate(Number(seg?.lowerEnd)));
      if (
        x0 === null ||
        x1 === null ||
        !Number.isFinite(yUpper0) ||
        !Number.isFinite(yUpper1) ||
        !Number.isFinite(yLower0) ||
        !Number.isFinite(yLower1)
      ) {
        return null;
      }
      const status = String(seg.status || "auto");
      const selected = String(state.selectedChannelId || "") === String(seg.id || "");
      const color =
        status === "edited"
          ? "#ffd166"
          : status === "accepted"
          ? "#ff9f43"
          : seg.method === "ransac"
          ? "#ff6d3a"
          : seg.method === "regression"
          ? "#4ea1ff"
          : "#ff7f50";
      const strokeWidth = selected ? 1.8 : status === "auto" ? 1 : 1.4;
      const centerDash =
        seg.qualityTier === "C" ? "2 3" : seg.qualityTier === "B" ? "4 3" : "6 3";
      const opacity = status === "auto" ? 0.52 : 0.92;
      return {
        id: `${seg.id || idx}`,
        method: seg.method,
        status,
        selected,
        color,
        opacity,
        strokeWidth,
        x0,
        x1,
        yUpper0,
        yUpper1,
        yLower0,
        yLower1,
        points: `${x0},${yUpper0} ${x1},${yUpper1} ${x1},${yLower1} ${x0},${yLower0}`,
        centerDash,
        centerY0: (yUpper0 + yLower0) * 0.5,
        centerY1: (yUpper1 + yLower1) * 0.5,
        label: `${methodDisplayName(seg.method)} S${Number(seg.score || 0).toFixed(
          3
        )} I${Number(seg.insideRatio || 0).toFixed(3)} T${Number(seg.touchCount || 0)}`,
      };
    })
    .filter((x) => !!x);
});

const overlayShapes = computed(() => {
  // Depend on viewVersion so drawings reproject when chart pans/zooms/crosshair moves.
  void viewVersion.value;
  const size = overlaySize.value;
  const draft = controller.getDraft?.();
  const base = (drawState.drawings || []).slice();
  if (
    draft &&
    draft.type === "trendline" &&
    Array.isArray(draft.points) &&
    draft.points.length >= 2
  ) {
    base.push({
      id: "__draft_trendline__",
      type: "trendline",
      points: draft.points.slice(0, 2),
      style: { color: "#4ea1ff", width: 1, opacity: 0.9 },
      visible: true,
    });
  }
  return base
    .filter((d) => d.visible !== false)
    .filter((d) => d.type !== "vline")
    .filter((d) => {
      const vr = String(d.visible_range || "all").toLowerCase();
      if (vr === "all" || vr === "") return true;
      return vr === timeframeBucket(props.scope?.timeframe || "1m");
    })
    .map((d) => ({
      drawing: d,
      geom: drawingToScreenGeometry(d, mapper, size),
      showAnchors:
        d.id === drawState.selectedId ||
        d.id === drawState.hoveredId ||
        d.id === "__draft_trendline__",
    }))
    .filter((x) => !!x.geom);
});

const candleVLines = computed(() => {
  void viewVersion.value;
  return (drawState.drawings || [])
    .filter(
      (d) =>
        d.visible !== false &&
        d.type === "vline" &&
        Array.isArray(d.points) &&
        d.points.length > 0
    )
    .filter((d) => {
      const vr = String(d.visible_range || "all").toLowerCase();
      if (vr === "all" || vr === "") return true;
      return vr === timeframeBucket(props.scope?.timeframe || "1m");
    })
    .map((d) => {
      const x = mapper.timeToX(Number(d.points[0]?.time || 0));
      if (x === null || x === undefined || Number.isNaN(x)) return null;
      return {
        id: d.id,
        x,
        color: d.style?.color || "#4ea1ff",
        width: d.style?.width || 1,
      };
    })
    .filter((x) => !!x);
});

function optimalCandlestickWidth(barSpacing, pixelRatio) {
  const from = 2.5;
  const to = 4;
  const coeffSpecial = 3;
  if (barSpacing >= from && barSpacing <= to) {
    return Math.floor(coeffSpecial * pixelRatio);
  }
  const reducing = 0.2;
  const coeff =
    1 - (reducing * Math.atan(Math.max(to, barSpacing) - to)) / (Math.PI * 0.5);
  const res = Math.floor(barSpacing * coeff * pixelRatio);
  const scaled = Math.floor(barSpacing * pixelRatio);
  const optimal = Math.min(res, scaled);
  return Math.max(Math.floor(pixelRatio), optimal);
}

function candleBodyGeometryCss(centerX, barSpacing, pixelRatio) {
  if (!Number.isFinite(centerX)) return null;
  let barWidthPx = optimalCandlestickWidth(barSpacing, pixelRatio);
  if (barWidthPx >= 2) {
    const wickWidthPx = Math.floor(pixelRatio);
    if (wickWidthPx % 2 !== barWidthPx % 2) barWidthPx -= 1;
  }
  const centerPx = Math.round(centerX * pixelRatio);
  const leftPx = centerPx - Math.floor(barWidthPx * 0.5);
  const rightPx = leftPx + barWidthPx - 1;
  return {
    x: leftPx / pixelRatio,
    w: (rightPx - leftPx + 1) / pixelRatio,
  };
}

const macdStemLines = computed(() => {
  void viewVersion.value;
  if (!chartRefs.macd || !chartRefs.candle || !seriesRefs.hist) return [];
  const hostW = macdRef.value?.clientWidth || 0;
  const hostH = macdRef.value?.clientHeight || 0;
  const zeroY = Number(seriesRefs.hist.priceToCoordinate(0));
  if (!Number.isFinite(zeroY)) return [];
  return (state.macdHist || [])
    .map((item) => {
      const x = Number(chartRefs.candle.timeScale().timeToCoordinate(item.time));
      const y = Number(seriesRefs.hist.priceToCoordinate(item.value));
      if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
      if (x < -4 || x > hostW + 4) return null;
      const y1 = Math.max(-2, Math.min(hostH + 2, zeroY));
      const y2 = Math.max(-2, Math.min(hostH + 2, y));
      return {
        id: item.time,
        x,
        y1,
        y2,
        color: item.value >= 0 ? COLOR_UP : COLOR_DOWN_MACD,
      };
    })
    .filter((x) => !!x);
});

const volumeBars = computed(() => {
  void viewVersion.value;
  if (!chartRefs.volume || !chartRefs.candle || !seriesRefs.volume) return [];
  const hostW = volumeRef.value?.clientWidth || 0;
  const hostH = volumeRef.value?.clientHeight || 0;
  const zeroY = Number(seriesRefs.volume.priceToCoordinate(0));
  if (!Number.isFinite(zeroY)) return [];
  const barSpacing = Number(chartRefs.candle.timeScale().options?.().barSpacing || 6);
  const pixelRatio = Math.max(1, Number(window.devicePixelRatio || 1));
  return state.bars
    .map((bar) => {
      const x = Number(chartRefs.candle.timeScale().timeToCoordinate(bar.adjusted_time));
      const topY = Number(seriesRefs.volume.priceToCoordinate(bar.volume));
      if (!Number.isFinite(x) || !Number.isFinite(topY)) return null;
      const geom = candleBodyGeometryCss(x, barSpacing, pixelRatio);
      if (!geom) return null;
      if (geom.x > hostW + 4 || geom.x + geom.w < -4) return null;
      const y = Math.max(-2, Math.min(hostH + 2, Math.min(topY, zeroY)));
      const h = Math.max(1, Math.abs(zeroY - topY));
      return {
        id: bar.adjusted_time,
        x: geom.x,
        y: Math.round(y) + 0.5,
        w: Math.max(1 / pixelRatio, geom.w),
        h: Math.max(1, Math.round(h)),
        up: Number(bar.close) >= Number(bar.open),
      };
    })
    .filter((x) => !!x);
});

const syncTimeLabelText = computed(() => {
  if (!syncCrosshair.visible || !syncCrosshair.time) return "";
  return fmtTime(syncCrosshair.time);
});

const syncTimeLabelStyle = computed(() => {
  const hostW = volumeRef.value?.clientWidth || 0;
  const x = Number(syncCrosshair.xVolume || 0);
  // Keep label centered on crosshair and clamped inside pane.
  let left = x - 74;
  if (left < 6) left = 6;
  if (left > hostW - 148 - 6) left = Math.max(6, hostW - 148 - 6);
  return { left: `${left}px` };
});

function findBarIndexByTime(ts) {
  if (!Number.isFinite(ts) || state.bars.length === 0) return -1;
  let lo = 0;
  let hi = state.bars.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const mt = Number(state.bars[mid]?.adjusted_time || 0);
    if (mt === ts) return mid;
    if (mt < ts) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (best >= 0) return best;
  return 0;
}

function formatPrice(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  const abs = Math.abs(n);
  const decimals = abs >= 1000 ? 2 : abs >= 1 ? 3 : 4;
  return n.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

function formatVolume(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return n.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function formatSignedChange(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  if (n > 0) return `+${formatPrice(Math.abs(n))}`;
  if (n < 0) return `-${formatPrice(Math.abs(n))}`;
  return "0";
}

function formatAxisValue(mode, v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  if (mode === "volume") {
    return n.toLocaleString("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    });
  }
  return n.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function buildRightAxisTicks(chart, hostEl, mode = "price") {
  if (!chart || !hostEl) return [];
  const h = Number(hostEl.clientHeight || 0);
  if (!Number.isFinite(h) || h <= 20) return [];
  const series =
    mode === "volume"
      ? seriesRefs.volume
      : mode === "macd"
      ? seriesRefs.dea || seriesRefs.hist
      : seriesRefs.candle;
  if (!series || typeof series.coordinateToPrice !== "function") return [];

  const count = Math.max(4, Math.floor(h / 42));

  const out = [];
  for (let i = 0; i <= count; i += 1) {
    const y = Math.round((i / count) * (h - 1));
    const value = Number(series.coordinateToPrice(y));
    if (!Number.isFinite(value)) continue;
    let text = "";
    if (mode === "volume") {
      text = Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
    } else {
      text = formatAxisValue(mode, value);
    }
    out.push({ id: `${mode}-${i}`, y, text });
  }
  return out;
}

const candleAxisTicks = computed(() => {
  void viewVersion.value;
  return buildRightAxisTicks(chartRefs.candle, candleRef.value, "price");
});

const macdAxisTicks = computed(() => {
  void viewVersion.value;
  return buildRightAxisTicks(chartRefs.macd, macdRef.value, "macd");
});

const volumeAxisTicks = computed(() => {
  void viewVersion.value;
  return buildRightAxisTicks(chartRefs.volume, volumeRef.value, "volume");
});

const latestPriceTag = computed(() => {
  void viewVersion.value;
  if (!seriesRefs.candle) return null;
  if (!state.bars.length) return null;
  const price = Number(state.bars[state.bars.length - 1]?.close);
  const y = Number(seriesRefs.candle.priceToCoordinate(price));
  if (!Number.isFinite(price) || !Number.isFinite(y)) return null;
  return { y: Math.round(y), text: formatAxisValue("price", price) };
});

const selectedBarIndex = computed(() => {
  if (state.bars.length === 0) return -1;
  if (syncCrosshair.visible && syncCrosshair.time) {
    return findBarIndexByTime(Number(syncCrosshair.time));
  }
  return state.bars.length - 1;
});

const selectedBar = computed(() => {
  const idx = selectedBarIndex.value;
  if (idx < 0 || idx >= state.bars.length) return null;
  return state.bars[idx];
});

const selectedPrevBar = computed(() => {
  const idx = selectedBarIndex.value;
  if (idx <= 0 || idx > state.bars.length - 1) return null;
  return state.bars[idx - 1];
});

const quoteHeader = computed(() => {
  const bar = selectedBar.value;
  const prev = selectedPrevBar.value;
  const barIndex = selectedBarIndex.value;
  const changeRaw =
    bar &&
    prev &&
    Number.isFinite(Number(bar.close)) &&
    Number.isFinite(Number(prev.close))
      ? Number(bar.close) - Number(prev.close)
      : null;
  const changeClass = !Number.isFinite(changeRaw)
    ? "flat"
    : changeRaw > 0
    ? "up"
    : changeRaw < 0
    ? "down"
    : "flat";
  return {
    symbol:
      state.resolvedSymbol ||
      String(props.scope?.symbol || "")
        .trim()
        .toLowerCase() ||
      "--",
    timeframe: props.scope?.timeframe || "--",
    open: bar ? formatPrice(bar.open) : "--",
    high: bar ? formatPrice(bar.high) : "--",
    low: bar ? formatPrice(bar.low) : "--",
    close: bar ? formatPrice(bar.close) : "--",
    volume: bar ? formatVolume(bar.volume) : "--",
    change: Number.isFinite(changeRaw) ? formatSignedChange(changeRaw) : "--",
    index: barIndex >= 0 ? String(barIndex) : "--",
    changeClass,
  };
});

const channelDebugRows = computed(() => {
  void viewVersion.value;
  if (!props.showChannelDebug) return [];
  const segs = Array.isArray(state.channelSegments) ? state.channelSegments : [];
  return segs
    .slice()
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, 6)
    .map((s, idx) => ({
      id: s.id || `${idx}`,
      text: `${idx + 1}. ${String(s.method || "-").padEnd(10, " ")} score:${Number(
        s.score || 0
      ).toFixed(3)} in:${Number(s.insideRatio || 0).toFixed(3)} t:${Number(
        s.touchCount || 0
      )}`,
    }));
});

function candlePointerPos(evt) {
  const rect = candleRef.value.getBoundingClientRect();
  return { x: evt.clientX - rect.left, y: evt.clientY - rect.top };
}

function distPointToSeg(px, py, x1, y1, x2, y2) {
  const vx = x2 - x1;
  const vy = y2 - y1;
  const wx = px - x1;
  const wy = py - y1;
  const c1 = vx * wx + vy * wy;
  if (c1 <= 0) return Math.hypot(px - x1, py - y1);
  const c2 = vx * vx + vy * vy;
  if (c2 <= c1) return Math.hypot(px - x2, py - y2);
  const b = c1 / c2;
  const bx = x1 + b * vx;
  const by = y1 + b * vy;
  return Math.hypot(px - bx, py - by);
}

function findChannelHit(x, y) {
  const shapes = channelOverlayShapes.value || [];
  if (!shapes.length) return null;
  let best = null;
  const anchorThreshold = 7;
  const lineThreshold = 6;
  for (let i = shapes.length - 1; i >= 0; i -= 1) {
    const s = shapes[i];
    const a0 = Math.hypot(x - s.x0, y - s.yUpper0);
    const a1 = Math.hypot(x - s.x1, y - s.yUpper1);
    const b0 = Math.hypot(x - s.x0, y - s.yLower0);
    const b1 = Math.hypot(x - s.x1, y - s.yLower1);
    const startHit = Math.min(a0, b0);
    const endHit = Math.min(a1, b1);
    if (startHit <= anchorThreshold)
      return { id: s.id, mode: "start_anchor", dist: startHit };
    if (endHit <= anchorThreshold) return { id: s.id, mode: "end_anchor", dist: endHit };
    const du = distPointToSeg(x, y, s.x0, s.yUpper0, s.x1, s.yUpper1);
    const dl = distPointToSeg(x, y, s.x0, s.yLower0, s.x1, s.yLower1);
    const dc = distPointToSeg(x, y, s.x0, s.centerY0, s.x1, s.centerY1);
    const candidates = [
      { mode: "upper", dist: du },
      { mode: "lower", dist: dl },
      { mode: "center", dist: dc },
    ];
    for (const c of candidates) {
      if (c.dist > lineThreshold) continue;
      if (!best || c.dist < best.dist) best = { id: s.id, mode: c.mode, dist: c.dist };
    }
  }
  return best;
}

function segmentFromDecision(d) {
  return {
    id: String(d.id || d.base_id || ""),
    baseId: String(d.base_id || d.id || ""),
    method: d.method || "manual",
    startTime: Number(d.start_time || d.startTime || 0),
    endTime: Number(d.end_time || d.endTime || 0),
    upperStart: Number(d.upper_start || d.upperStart || 0),
    upperEnd: Number(d.upper_end || d.upperEnd || 0),
    lowerStart: Number(d.lower_start || d.lowerStart || 0),
    lowerEnd: Number(d.lower_end || d.lowerEnd || 0),
    slope: Number(d.slope || 0),
    score: Number(d.score || 0),
    insideRatio: Number(d.insideRatio || 0),
    touchCount: Number(d.touchCount || 0),
    status: String(d.status || "edited"),
    locked: !!d.locked,
    manual: true,
  };
}

function clampSegmentToBars(seg) {
  if (!state.bars.length) return seg;
  const minT = Number(state.bars[0]?.adjusted_time || 0);
  const maxT = Number(state.bars[state.bars.length - 1]?.adjusted_time || minT);
  let startTime = Math.max(minT, Math.min(maxT, Number(seg.startTime || minT)));
  let endTime = Math.max(minT, Math.min(maxT, Number(seg.endTime || maxT)));
  if (endTime <= startTime)
    endTime = Math.min(
      maxT,
      startTime + parseTimeframeSeconds(props.scope?.timeframe || "1m")
    );
  return { ...seg, startTime, endTime };
}

function applyChannelDrag(x, y) {
  if (!channelDrag) return;
  const target = findSegmentByID(channelDrag.id);
  if (!target) return;
  const startT = mapXToTimeWithFallback(channelDrag.startX);
  const nowT = mapXToTimeWithFallback(x);
  const dt =
    Number.isFinite(startT) && Number.isFinite(nowT) ? Math.round(nowT - startT) : 0;
  const dpRaw = mapper.priceDeltaFromYDelta?.(y - channelDrag.startY);
  const dp = Number.isFinite(dpRaw) ? dpRaw : 0;
  const base = { ...channelDrag.original };
  let next = { ...base };
  if (channelDrag.mode === "upper") {
    next.upperStart = Number(base.upperStart) + dp;
    next.upperEnd = Number(base.upperEnd) + dp;
  } else if (channelDrag.mode === "lower") {
    next.lowerStart = Number(base.lowerStart) + dp;
    next.lowerEnd = Number(base.lowerEnd) + dp;
  } else if (channelDrag.mode === "center") {
    next.upperStart = Number(base.upperStart) + dp;
    next.upperEnd = Number(base.upperEnd) + dp;
    next.lowerStart = Number(base.lowerStart) + dp;
    next.lowerEnd = Number(base.lowerEnd) + dp;
    next.startTime = Number(base.startTime) + dt;
    next.endTime = Number(base.endTime) + dt;
  } else if (channelDrag.mode === "start_anchor") {
    next.startTime = Number(base.startTime) + dt;
    next.upperStart = Number(base.upperStart) + dp;
    next.lowerStart = Number(base.lowerStart) + dp;
  } else if (channelDrag.mode === "end_anchor") {
    next.endTime = Number(base.endTime) + dt;
    next.upperEnd = Number(base.upperEnd) + dp;
    next.lowerEnd = Number(base.lowerEnd) + dp;
  }
  next = clampSegmentToBars(next);
  if (next.upperStart <= next.lowerStart) next.upperStart = next.lowerStart + 0.2;
  if (next.upperEnd <= next.lowerEnd) next.upperEnd = next.lowerEnd + 0.2;
  next.slope =
    (Number(next.upperEnd) -
      Number(next.upperStart) +
      Number(next.lowerEnd) -
      Number(next.lowerStart)) /
    (2 * Math.max(1, Number(next.endTime) - Number(next.startTime)));
  next.status = "edited";
  next.manual = true;
  next.locked = !!base.locked;
  next.id = base.id;
  next.baseId = base.baseId || base.id;
  const before = channelSegmentMetrics(base);
  const after = channelSegmentMetrics(next);
  state.channelDetail = {
    id: next.id,
    source: `${base.status || "auto"}->edited`,
    operator: "admin",
    before,
    after,
    updated_at: new Date().toISOString(),
  };
  upsertChannelDecision(buildDecisionSnapshot(next, "edited", "drag->edited"));
  state.channelSegments = mergeChannels(state.channelAutoCandidates);
  setChannelSelected(next.id);
}

function onCandlePointerDown(evt) {
  if (props.activeTool !== "cursor") return;
  refreshOverlayMetrics();
  if (mainWrapRef.value?.setPointerCapture && typeof evt?.pointerId === "number") {
    try {
      mainWrapRef.value.setPointerCapture(evt.pointerId);
      dragPointerId = evt.pointerId;
    } catch {
      dragPointerId = null;
    }
  }
  const { x, y } = candlePointerPos(evt);
  const ch = findChannelHit(x, y);
  if (ch?.id) {
    const seg = findSegmentByID(ch.id);
    if (seg) {
      evt.preventDefault();
      setChannelSelected(ch.id);
      channelDrag = {
        id: ch.id,
        mode: ch.mode,
        startX: x,
        startY: y,
        original: JSON.parse(JSON.stringify(seg)),
      };
      pointerDownMeta = { x, y, moved: false };
      pointerCursor.value = ch.mode === "center" ? "grabbing" : "default";
      viewVersion.value += 1;
      return;
    }
  }
  const info = controller.inspectAt?.(x, y);
  if (!info || !info.id) {
    state.selectedChannelId = "";
    drawState.selectedId = "";
    drawState.hoveredId = "";
    pointerCursor.value = "";
    emit("select-drawing", "");
    publishChannelView();
    return;
  }
  evt.preventDefault();
  controller.onPointerDown(x, y);
  draggingDraw = controller.isDragging?.() || false;
  pointerDownMeta = { x, y, moved: false };
  drawState.selectedId = info.id;
  drawState.hoveredId = info.id;
  emit("select-drawing", info.id);
  dragMode.value = info.mode || "";
  // TradingView-like feel: keep pointer on down, switch to grabbing only after real drag starts.
  pointerCursor.value = info.mode === "line" ? "pointer" : "default";
  viewVersion.value += 1;
}

function onCandlePointerMove(evt) {
  if (props.activeTool !== "cursor") return;
  const { x, y } = candlePointerPos(evt);
  if (channelDrag) {
    applyChannelDrag(x, y);
    if (pointerDownMeta) {
      const moved =
        Math.abs(x - pointerDownMeta.x) > 2 || Math.abs(y - pointerDownMeta.y) > 2;
      if (moved) pointerDownMeta.moved = true;
    }
    pointerCursor.value = channelDrag.mode === "center" ? "grabbing" : "default";
    publishChannelView();
    viewVersion.value += 1;
    return;
  }
  if (controller.isDragging?.()) {
    updateDraggingCursorMove(x, y);
    return;
  }
  pointerDownMeta = null;
  const ch = findChannelHit(x, y);
  if (ch?.id) {
    setChannelSelected(ch.id);
    pointerCursor.value = ch.mode === "center" ? "grab" : "default";
    viewVersion.value += 1;
    return;
  }
  const info = controller.inspectAt?.(x, y);
  drawState.hoveredId = info?.id || "";
  if (info?.mode === "line") {
    pointerCursor.value = "pointer";
  } else if (info?.mode === "anchor") {
    pointerCursor.value = "default";
  } else {
    pointerCursor.value = "";
  }
  viewVersion.value += 1;
}

function updateDraggingCursorMove(x, y) {
  controller.onPointerMove(x, y);
  if (pointerDownMeta) {
    const moved =
      Math.abs(x - pointerDownMeta.x) > 2 || Math.abs(y - pointerDownMeta.y) > 2;
    if (moved) pointerDownMeta.moved = true;
  }
  if (dragMode.value === "line") {
    pointerCursor.value = pointerDownMeta?.moved ? "grabbing" : "pointer";
  } else {
    pointerCursor.value = "default";
  }
  viewVersion.value += 1;
}

function onGlobalPointerMove(evt) {
  if (props.activeTool !== "cursor") return;
  if (channelDrag) {
    if (!candleRef.value || !evt || typeof evt.clientX !== "number") return;
    refreshOverlayMetrics();
    const { x, y } = candlePointerPos(evt);
    applyChannelDrag(x, y);
    publishChannelView();
    viewVersion.value += 1;
    return;
  }
  if (!controller.isDragging?.()) return;
  if (!candleRef.value || !evt || typeof evt.clientX !== "number") return;
  refreshOverlayMetrics();
  const { x, y } = candlePointerPos(evt);
  updateDraggingCursorMove(x, y);
}

function onCandlePointerLeave() {
  if (props.activeTool !== "cursor") return;
  if (controller.isDragging?.()) return;
  pointerDownMeta = null;
  drawState.hoveredId = "";
  pointerCursor.value = "";
  viewVersion.value += 1;
}

function startResize(kind, evt) {
  evt.preventDefault();
  activeResize = {
    kind,
    startY: evt.clientY,
    candle: paneHeights.candle,
    macd: paneHeights.macd,
    volume: paneHeights.volume,
  };
  window.addEventListener("pointermove", onResizeMove);
  window.addEventListener("pointerup", stopResize);
}

function onResizeMove(evt) {
  if (!activeResize) return;
  const delta = evt.clientY - activeResize.startY;
  if (activeResize.kind === "candle-macd") {
    const pair = activeResize.candle + activeResize.macd;
    let nextCandle = activeResize.candle + delta;
    if (nextCandle < MIN_CANDLE_H) nextCandle = MIN_CANDLE_H;
    if (nextCandle > pair - MIN_MACD_H) nextCandle = pair - MIN_MACD_H;
    paneHeights.candle = Math.floor(nextCandle);
    paneHeights.macd = Math.floor(pair - nextCandle);
  } else if (activeResize.kind === "macd-volume") {
    const pair = activeResize.macd + activeResize.volume;
    let nextMacd = activeResize.macd + delta;
    if (nextMacd < MIN_MACD_H) nextMacd = MIN_MACD_H;
    if (nextMacd > pair - MIN_VOLUME_H) nextMacd = pair - MIN_VOLUME_H;
    paneHeights.macd = Math.floor(nextMacd);
    paneHeights.volume = Math.floor(pair - nextMacd);
  }
  applyChartSizes();
  viewVersion.value += 1;
}

function stopResize() {
  activeResize = null;
  window.removeEventListener("pointermove", onResizeMove);
  window.removeEventListener("pointerup", stopResize);
}

async function syncLayoutAndChartSizes() {
  normalizePaneHeights();
  await nextTick();
  refreshOverlayMetrics();
  applyChartSizes();
  requestAnimationFrame(() => {
    refreshOverlayMetrics();
    applyChartSizes();
  });
}

watch(
  () => [
    props.scope.symbol,
    props.scope.type,
    props.scope.variety,
    props.scope.timeframe,
  ],
  () => {
    state.resolvedSymbol = "";
    loadInitialChunk();
  }
);

watch(
  () => props.theme,
  () => {
    rebuildChartsForTheme();
  }
);

onMounted(async () => {
  await syncLayoutAndChartSizes();
  initCharts();
  applyChartSizes();
  refreshOverlayMetrics();
  await loadInitialChunk();
  resizeObserver = new ResizeObserver(() => {
    void syncLayoutAndChartSizes();
  });
  if (containerRef.value) resizeObserver.observe(containerRef.value);
  if (mainWrapRef.value) resizeObserver.observe(mainWrapRef.value);
  if (overlayRef.value) resizeObserver.observe(overlayRef.value);
  window.addEventListener("pointerup", onPointerUp);
  globalPointerMoveHandler = (evt) => onGlobalPointerMove(evt);
  window.addEventListener("pointermove", globalPointerMoveHandler);
  keydownHandler = (e) => {
    if (state.selectedChannelId) {
      const k = String(e.key || "").toLowerCase();
      if (k === "a") {
        applyChannelAction({ type: "accept", id: state.selectedChannelId });
        return;
      }
      if (k === "r") {
        applyChannelAction({ type: "reject", id: state.selectedChannelId });
        return;
      }
      if (k === "l") {
        applyChannelAction({ type: "toggle-lock", id: state.selectedChannelId });
        return;
      }
      if (k === "delete" || k === "backspace") {
        applyChannelAction({ type: "delete-manual", id: state.selectedChannelId });
        return;
      }
    }
    if (e.key === "Delete" || e.key === "Backspace") {
      onDeleteSelected();
      return;
    }
    if (e.key === "Escape") {
      clearSelection();
    }
  };
  window.addEventListener("keydown", keydownHandler);
});

onUnmounted(() => {
  stopResize();
  if (channelRecalcTimer) {
    clearTimeout(channelRecalcTimer);
    channelRecalcTimer = null;
  }
  if (resizeObserver) resizeObserver.disconnect();
  window.removeEventListener("pointerup", onPointerUp);
  if (globalPointerMoveHandler)
    window.removeEventListener("pointermove", globalPointerMoveHandler);
  if (keydownHandler) window.removeEventListener("keydown", keydownHandler);
  if (chartRefs.candle && candleCrosshairHandler) {
    chartRefs.candle.unsubscribeCrosshairMove(candleCrosshairHandler);
  }
  if (chartRefs.macd && macdCrosshairHandler) {
    chartRefs.macd.unsubscribeCrosshairMove(macdCrosshairHandler);
  }
  if (chartRefs.volume && volumeCrosshairHandler) {
    chartRefs.volume.unsubscribeCrosshairMove(volumeCrosshairHandler);
  }
  for (const key of Object.keys(chartRefs)) {
    chartRefs[key]?.remove();
    chartRefs[key] = null;
  }
});
</script>

<template>
  <div ref="containerRef" class="tv-chart-pane">
    <div
      ref="candleSlotRef"
      class="pane-slot"
      :style="{ height: `${paneHeights.candle}px` }"
    >
      <div
        ref="mainWrapRef"
        class="tv-main-wrap"
        :style="{ cursor: pointerCursor || undefined }"
        @pointerdown="onCandlePointerDown"
        @pointermove="onCandlePointerMove"
        @pointerleave="onCandlePointerLeave"
      >
        <div ref="candleRef" class="chart-box main"></div>
        <div class="right-data-mask" :style="dataZoneMaskStyle"></div>
        <div class="right-data-text-layer" :style="{ width: `${DATA_ZONE_WIDTH_PX}px` }">
          <div
            v-for="t in candleAxisTicks"
            :key="t.id"
            class="axis-tick-text"
            :style="{ top: `${t.y}px` }"
          >
            {{ t.text }}
          </div>
          <div
            v-if="latestPriceTag"
            class="axis-price-badge latest"
            :style="{ top: `${latestPriceTag.y}px` }"
          >
            {{ latestPriceTag.text }}
          </div>
          <div
            v-if="rightValueOverlay.candleVisible"
            class="axis-price-badge cross"
            :style="{ top: `${rightValueOverlay.candleY}px` }"
          >
            {{ formatAxisValue("price", rightValueOverlay.candleValue) }}
          </div>
        </div>
        <div class="tv-quote-header">
          <span class="dim">Symbol</span>
          <span>{{ quoteHeader.symbol }}</span>
          <span class="dim">TF</span>
          <span>{{ quoteHeader.timeframe }}</span>
          <span class="dim">Open</span>
          <span>{{ quoteHeader.open }}</span>
          <span class="dim">High</span>
          <span>{{ quoteHeader.high }}</span>
          <span class="dim">Low</span>
          <span>{{ quoteHeader.low }}</span>
          <span class="dim">Close</span>
          <span>{{ quoteHeader.close }}</span>
          <span class="dim">Vol</span>
          <span>{{ quoteHeader.volume }}</span>
          <span class="dim">Change</span>
          <span :class="quoteHeader.changeClass">{{ quoteHeader.change }}</span>
          <span class="dim">Index</span>
          <span>{{ quoteHeader.index }}</span>
        </div>
        <div v-if="props.showChannelDebug" class="channel-debug-panel">
          <div class="channel-debug-title">
            CH Debug | seg={{ state.channelSegments.length }} | vr={{
              state.channelDebug.visibleRange.start
            }}-{{ state.channelDebug.visibleRange.end }}
          </div>
          <div v-for="row in channelDebugRows" :key="row.id" class="channel-debug-row">
            {{ row.text }}
          </div>
        </div>
        <div
          v-if="syncCrosshair.visible && syncCrosshair.xCandle >= 0"
          class="sync-vline"
          :style="{ left: `${syncCrosshair.xCandle}px` }"
        ></div>
        <div class="candle-vline-layer">
          <div
            v-for="item in candleVLines"
            :key="item.id"
            class="candle-vline"
            :style="{
              left: `${item.x}px`,
              borderLeftColor: item.color,
              borderLeftWidth: `${item.width}px`,
            }"
          ></div>
        </div>
        <div class="channel-overlay-layer">
          <svg :width="overlaySize.width" :height="overlaySize.height">
            <template v-for="seg in channelOverlayShapes" :key="seg.id">
              <polygon
                class="channel-fill"
                :points="seg.points"
                :fill="seg.color"
                :fill-opacity="seg.selected ? 0.09 : 0.06"
              ></polygon>
              <line
                class="channel-line"
                :class="{ selected: seg.selected }"
                :x1="seg.x0"
                :y1="seg.yUpper0"
                :x2="seg.x1"
                :y2="seg.yUpper1"
                :stroke="seg.color"
                :stroke-opacity="seg.opacity"
                :stroke-width="seg.strokeWidth"
              />
              <line
                class="channel-line"
                :class="{ selected: seg.selected }"
                :x1="seg.x0"
                :y1="seg.yLower0"
                :x2="seg.x1"
                :y2="seg.yLower1"
                :stroke="seg.color"
                :stroke-opacity="seg.opacity"
                :stroke-width="seg.strokeWidth"
              />
              <line
                class="channel-mid-line"
                :x1="seg.x0"
                :y1="seg.centerY0"
                :x2="seg.x1"
                :y2="seg.centerY1"
                :stroke="seg.color"
                :stroke-dasharray="seg.centerDash"
                :stroke-opacity="seg.opacity"
              />
              <circle
                v-if="seg.selected"
                class="channel-anchor"
                :cx="seg.x0"
                :cy="seg.yUpper0"
                r="3.5"
              />
              <circle
                v-if="seg.selected"
                class="channel-anchor"
                :cx="seg.x0"
                :cy="seg.yLower0"
                r="3.5"
              />
              <circle
                v-if="seg.selected"
                class="channel-anchor"
                :cx="seg.x1"
                :cy="seg.yUpper1"
                r="3.5"
              />
              <circle
                v-if="seg.selected"
                class="channel-anchor"
                :cx="seg.x1"
                :cy="seg.yLower1"
                r="3.5"
              />
              <text
                v-if="state.channelSettings?.common?.showLabels"
                class="channel-label"
                :x="seg.x0 + 4"
                :y="Math.min(seg.yUpper0, seg.yLower0) - 6"
              >
                {{ seg.label }}
              </text>
            </template>
          </svg>
        </div>
        <div
          ref="overlayRef"
          class="draw-overlay"
          :class="{ interactive: overlayInteractive }"
          @pointerdown.prevent="onPointerDown"
          @pointermove.prevent="onPointerMove"
        >
          <svg :width="overlaySize.width" :height="overlaySize.height">
            <template v-for="item in overlayShapes" :key="item.drawing.id">
              <line
                v-if="item.geom.type === 'line'"
                :x1="item.geom.x1"
                :y1="item.geom.y1"
                :x2="item.geom.x2"
                :y2="item.geom.y2"
                :stroke="item.drawing.style?.color || '#4ea1ff'"
                :stroke-width="item.drawing.style?.width || 1"
              />
              <circle
                v-if="
                  item.drawing.type === 'trendline' &&
                  item.geom.type === 'line' &&
                  item.showAnchors
                "
                :cx="item.geom.x1"
                :cy="item.geom.y1"
                r="4"
                :stroke="item.drawing.style?.color || '#4ea1ff'"
                stroke-width="1.5"
                :fill="props.theme === 'light' ? '#ffffff' : '#0b1220'"
              />
              <circle
                v-if="
                  item.drawing.type === 'trendline' &&
                  item.geom.type === 'line' &&
                  item.showAnchors
                "
                :cx="item.geom.x2"
                :cy="item.geom.y2"
                r="4"
                :stroke="item.drawing.style?.color || '#4ea1ff'"
                stroke-width="1.5"
                :fill="props.theme === 'light' ? '#ffffff' : '#0b1220'"
              />
              <rect
                v-if="item.geom.type === 'rect'"
                :x="item.geom.x"
                :y="item.geom.y"
                :width="item.geom.w"
                :height="item.geom.h"
                :stroke="item.drawing.style?.color || '#4ea1ff'"
                :stroke-width="item.drawing.style?.width || 1"
                :fill="item.drawing.style?.fill || '#4ea1ff'"
                :fill-opacity="item.drawing.style?.opacity ?? 0.12"
              />
              <text
                v-if="item.geom.type === 'text'"
                :x="item.geom.x"
                :y="item.geom.y"
                :fill="item.drawing.style?.color || '#dce7f5'"
                font-size="12"
              >
                {{ item.drawing.text || "Text" }}
              </text>
            </template>
          </svg>
        </div>
      </div>
    </div>

    <div
      class="pane-resizer"
      title="Drag to resize Kline/MACD panes"
      @pointerdown="startResize('candle-macd', $event)"
    >
      <span class="pane-resizer-grip">...</span>
    </div>

    <div ref="macdSlotRef" class="pane-slot" :style="{ height: `${paneHeights.macd}px` }">
      <div ref="macdRef" class="chart-box sub"></div>
      <div class="right-data-mask" :style="dataZoneMaskStyle"></div>
      <div class="right-data-text-layer" :style="{ width: `${DATA_ZONE_WIDTH_PX}px` }">
        <div
          v-for="t in macdAxisTicks"
          :key="t.id"
          class="axis-tick-text"
          :style="{ top: `${t.y}px` }"
        >
          {{ t.text }}
        </div>
        <div
          v-if="rightValueOverlay.macdVisible"
          class="axis-price-badge cross"
          :style="{ top: `${rightValueOverlay.macdY}px` }"
        >
          {{ formatAxisValue("macd", rightValueOverlay.macdValue) }}
        </div>
      </div>
      <div class="macd-stem-layer">
        <svg :width="macdRef?.clientWidth || 0" :height="macdRef?.clientHeight || 0">
          <line
            v-for="item in macdStemLines"
            :key="item.id"
            :x1="item.x"
            :x2="item.x"
            :y1="item.y1"
            :y2="item.y2"
            :stroke="item.color"
            stroke-width="1"
            shape-rendering="crispEdges"
          />
        </svg>
      </div>
      <div
        v-if="syncCrosshair.visible && syncCrosshair.xMacd >= 0"
        class="sync-vline"
        :style="{ left: `${syncCrosshair.xMacd}px` }"
      ></div>
    </div>

    <div
      class="pane-resizer"
      title="Drag to resize MACD/Volume panes"
      @pointerdown="startResize('macd-volume', $event)"
    >
      <span class="pane-resizer-grip">...</span>
    </div>

    <div
      ref="volumeSlotRef"
      class="pane-slot"
      :style="{ height: `${paneHeights.volume}px` }"
    >
      <div ref="volumeRef" class="chart-box sub"></div>
      <div class="right-data-mask" :style="dataZoneMaskStyle"></div>
      <div class="right-data-text-layer" :style="{ width: `${DATA_ZONE_WIDTH_PX}px` }">
        <div
          v-for="t in volumeAxisTicks"
          :key="t.id"
          class="axis-tick-text"
          :style="{ top: `${t.y}px` }"
        >
          {{ t.text }}
        </div>
        <div
          v-if="rightValueOverlay.volumeVisible"
          class="axis-price-badge cross"
          :style="{ top: `${rightValueOverlay.volumeY}px` }"
        >
          {{ formatAxisValue("volume", rightValueOverlay.volumeValue) }}
        </div>
      </div>
      <div class="volume-outline-layer">
        <svg :width="volumeRef?.clientWidth || 0" :height="volumeRef?.clientHeight || 0">
          <rect
            v-for="item in volumeBars"
            :key="item.id"
            :x="item.x"
            :y="item.y"
            :width="item.w"
            :height="item.h"
            :fill="item.up ? 'none' : COLOR_DOWN_KLINE_VOLUME"
            :stroke="item.up ? COLOR_UP : COLOR_DOWN_KLINE_VOLUME"
            stroke-width="1"
            shape-rendering="crispEdges"
          />
        </svg>
      </div>
      <div
        v-if="syncCrosshair.visible && syncCrosshair.xVolume >= 0"
        class="sync-vline"
        :style="{ left: `${syncCrosshair.xVolume}px` }"
      ></div>
      <div v-if="syncTimeLabelText" class="sync-time-label" :style="syncTimeLabelStyle">
        {{ syncTimeLabelText }}
      </div>
    </div>
  </div>
</template>
