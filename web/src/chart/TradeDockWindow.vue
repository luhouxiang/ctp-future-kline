<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps({
  visible: { type: Boolean, default: false },
  x: { type: Number, default: 0 },
  y: { type: Number, default: 0 },
  width: { type: Number, default: 1322 },
  height: { type: Number, default: 268 },
  dragging: { type: Boolean, default: false },
  resizing: { type: Boolean, default: false },
  activeTab: { type: String, default: 'positions' },
  orderForm: { type: Object, required: true },
  terminal: { type: Object, default: () => ({}) },
  quoteSnapshot: { type: Object, default: () => ({}) },
  replayKlineMode: { type: Boolean, default: false },
})

const emit = defineEmits([
  'close',
  'start-drag',
  'start-resize',
  'set-tab',
  'update-order-field',
  'cancel-order',
  'amend-order',
  'quick-order',
  'position-close',
  'position-reverse',
  'adjust-cashflow',
  'adjust-fee',
])

const symbolInputRef = ref(null)
const priceModeMenuRef = ref(null)
const selectedPriceMode = ref('latest')
const priceModeMenuOpen = ref(false)
const priceFrozen = ref(false)
const frozenLatestPrice = ref(0)
const manualPriceInput = ref('')
const selectedPositionKey = ref('')
const selectedOrderKey = ref('')
const selectedTradeKey = ref('')
const selectedWorkingOrderKey = ref('')
const positionMenuOpen = ref(false)
const positionMenuX = ref(0)
const positionMenuY = ref(0)
const ordersFilterMode = ref('all')
const ordersMenuOpen = ref(false)
const ordersMenuX = ref(0)
const ordersMenuY = ref(0)
const workingOrderMenuOpen = ref(false)
const workingOrderMenuX = ref(0)
const workingOrderMenuY = ref(0)
const amendDialogOpen = ref(false)
const amendOrderTarget = ref(null)
const amendForm = ref({ price: '', volume: 1 })

const PRICE_MODE_OPTIONS = [
  { key: 'latest', label: '最新价' },
  { key: 'queue', label: '排队价' },
  { key: 'opponent', label: '对手价' },
  { key: 'market', label: '市价' },
]

const priceModeOptions = computed(() => (
  props.replayKlineMode ? [{ key: 'latest', label: '最新价' }] : PRICE_MODE_OPTIONS
))

watch(
  () => props.replayKlineMode,
  (enabled) => {
    if (enabled && !priceFrozen.value) selectedPriceMode.value = 'latest'
  },
  { immediate: true },
)

watch(
  () => props.visible,
  async (visible) => {
    if (!visible) {
      priceModeMenuOpen.value = false
      positionMenuOpen.value = false
      ordersMenuOpen.value = false
      workingOrderMenuOpen.value = false
      amendDialogOpen.value = false
      return
    }
    await nextTick()
    symbolInputRef.value?.focus?.()
    symbolInputRef.value?.select?.()
  },
)

function pickQuoteNumber(...values) {
  for (const item of values) {
    const n = Number(item)
    if (Number.isFinite(n) && n > 0) return n
  }
  return 0
}

const topTabs = [
  { key: 'positions', label: '持仓' },
  { key: 'orders', label: '委托' },
  { key: 'trades', label: '成交' },
  { key: 'funds', label: '资金表' },
]

const rootStyle = computed(() => ({
  transform: `translate(${Math.round(props.x)}px, ${Math.round(props.y)}px)`,
  width: `${Math.round(props.width)}px`,
  minHeight: `${Math.round(props.height)}px`,
}))

const titleText = computed(() => {
  const account = String(props.terminal?.summary?.account_id || '----')
  const balance = fmtNumber(props.terminal?.summary?.balance, 0)
  const available = fmtNumber(props.terminal?.summary?.available, 0)
  const ratio = fmtPct(props.terminal?.summary?.risk_ratio)
  return `国信期货 - ${account} 权益：${balance}，可用资金：${available}，资金使用率：${ratio}`
})

const instrumentInfoText = computed(() => {
  const symbol = String(props.orderForm?.symbol || props.terminal?.summary?.symbol || '').trim()
  if (!symbol) return '--'
  const price = fmtNumber(props.quoteSnapshot?.latest_price, 0)
  const volume = fmtInt(props.orderForm?.volume || 1)
  return `${symbol}，${volume}手，现价${price}`
})

function fmtNumber(value, digits = 2) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  return n.toFixed(digits)
}

function fmtInt(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  return String(Math.trunc(n))
}

function fmtPct(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '0.00%'
  return `${(n * 100).toFixed(2)}%`
}

function toWholeNumber(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return null
  return Math.max(0, Math.floor(n))
}

function orderVolumeOriginal(item) {
  const v = toWholeNumber(item?.volume_total_original)
  return v == null ? 0 : v
}

function orderVolumeTraded(item) {
  const v = toWholeNumber(item?.volume_traded)
  return v == null ? 0 : v
}

function orderVolumeCanceled(item) {
  const v = toWholeNumber(item?.volume_canceled)
  return v == null ? 0 : v
}

function orderVolumeRemaining(item) {
  const direct = toWholeNumber(item?.remaining_volume)
  if (direct != null) return direct
  return Math.max(0, orderVolumeOriginal(item) - orderVolumeTraded(item) - orderVolumeCanceled(item))
}

function fmtTime(value) {
  const raw = String(value || '').trim()
  if (!raw) return '--'
  let text = raw.replace('T', ' ').replace('Z', '')
  text = text.replace(/([+-]\d{2}:\d{2})$/, '')
  text = text.replace(/\.\d+$/, '')
  const m = text.match(/^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})(?!:)/)
  if (m) text = `${m[1]}:00`
  return text
}

function updateField(field, evt) {
  emit('update-order-field', field, evt?.target?.value)
}

function updateNumberField(field, evt) {
  emit('update-order-field', field, Number(evt?.target?.value || 0))
}

function onHeaderPointerDown(evt) {
  emit('start-drag', evt)
}

function onResizePointerDown(direction, evt) {
  emit('start-resize', direction, evt)
}

function quickOrder(kind) {
  let limitPrice = 0
  if (kind === 'buy_open') limitPrice = resolveSidePrice('buy')
  if (kind === 'sell_open') limitPrice = resolveSidePrice('sell')
  emit('quick-order', {
    kind,
    limit_price: limitPrice,
    price_mode: priceFrozen.value ? 'frozen_latest' : selectedPriceMode.value,
  })
}

function positionKey(item) {
  return `${item?.symbol || ''}|${item?.direction || ''}|${item?.hedge_flag || ''}`
}

function orderKey(item) {
  return String(item?.command_id || '')
}

function tradeKey(item) {
  return `${item?.trade_id || ''}|${item?.received_at || ''}`
}

const selectedPosition = computed(() => {
  const rows = props.terminal?.positions || []
  return rows.find((item) => positionKey(item) === selectedPositionKey.value) || null
})

const filteredOrders = computed(() => {
  const rows = Array.isArray(props.terminal?.orders) ? props.terminal.orders : []
  if (ordersFilterMode.value === 'cancelable') return rows.filter((item) => isOrderCancelable(item))
  if (ordersFilterMode.value === 'canceled') return rows.filter((item) => isOrderCanceled(item))
  return rows
})

const hasWorkingOrders = computed(() => {
  const rows = Array.isArray(props.terminal?.working_orders) ? props.terminal.working_orders : []
  return rows.length > 0
})

const selectedWorkingOrder = computed(() => {
  const rows = Array.isArray(props.terminal?.working_orders) ? props.terminal.working_orders : []
  return rows.find((item) => orderKey(item) === selectedWorkingOrderKey.value) || null
})

const amendDirectionLabel = computed(() => {
  const text = String(amendOrderTarget.value?.direction || '').trim().toLowerCase()
  if (text === 'buy') return '买'
  if (text === 'sell') return '卖'
  return amendOrderTarget.value?.direction || '--'
})

const amendOffsetLabel = computed(() => {
  const text = String(amendOrderTarget.value?.offset_flag || '').trim().toLowerCase()
  if (text === 'open') return '开仓'
  if (text === 'close') return '平仓'
  if (text === 'close_today') return '平今'
  if (text === 'close_yesterday') return '平昨'
  return amendOrderTarget.value?.offset_flag || '--'
})

const selectedPositionClosable = computed(() => {
  const n = Number(selectedPosition.value?.closable || 0)
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0
})

function latestPriceNow() {
  return pickQuoteNumber(
    props.quoteSnapshot?.latest_price,
    props.quoteSnapshot?.ask_price1,
    props.quoteSnapshot?.bid_price1,
  )
}

function resolveSidePrice(kind) {
  if (priceFrozen.value) return pickQuoteNumber(frozenLatestPrice.value)
  const quote = props.quoteSnapshot || {}
  const latest = latestPriceNow()
  if (props.replayKlineMode) return latest
  const bid1 = pickQuoteNumber(quote?.bid_price1)
  const ask1 = pickQuoteNumber(quote?.ask_price1)
  const lowerLimit = pickQuoteNumber(quote?.lower_limit_price)
  const upperLimit = pickQuoteNumber(quote?.upper_limit_price)
  const side = kind === 'sell' ? 'sell' : 'buy'
  if (selectedPriceMode.value === 'opponent') {
    return side === 'buy'
      ? pickQuoteNumber(ask1, latest, bid1)
      : pickQuoteNumber(bid1, latest, ask1)
  }
  if (selectedPriceMode.value === 'queue') {
    return side === 'buy'
      ? pickQuoteNumber(bid1, latest, ask1)
      : pickQuoteNumber(ask1, latest, bid1)
  }
  if (selectedPriceMode.value === 'market') {
    return side === 'buy'
      ? pickQuoteNumber(upperLimit, latest, ask1, bid1)
      : pickQuoteNumber(lowerLimit, latest, bid1, ask1)
  }
  return pickQuoteNumber(latest, ask1, bid1)
}

function sidePrice(kind) {
  const v = resolveSidePrice(kind)
  return v > 0 ? fmtNumber(v, tickDigits()) : '--'
}

function sideDeltaText(kind) {
  const latest = latestPriceNow()
  const side = resolveSidePrice(kind)
  if (latest <= 0 || side <= 0) return 'Δ --'
  const delta = side - latest
  if (Math.abs(delta) < 1e-9) return 'Δ 0'
  return `Δ ${delta > 0 ? '+' : ''}${fmtNumber(delta, 0)}`
}

const selectedPriceModeLabel = computed(() => (
  priceModeOptions.value.find((item) => item.key === selectedPriceMode.value)?.label || '最新价'
))

function priceStepTick() {
  const quote = props.quoteSnapshot || {}
  return pickQuoteNumber(quote?.price_tick)
}

function tickDigits() {
  const tick = priceStepTick()
  if (!(tick > 0)) return 0
  const text = String(tick)
  if (!text.includes('.')) return 0
  return Math.min(6, text.length - text.indexOf('.') - 1)
}

function formatPriceForInput(value) {
  const v = Number(value)
  if (!Number.isFinite(v) || v <= 0) return ''
  return fmtNumber(v, tickDigits())
}

const priceAnchorDisplay = computed(() => {
  if (priceFrozen.value) {
    if (String(manualPriceInput.value || '').trim()) return String(manualPriceInput.value)
    return formatPriceForInput(frozenLatestPrice.value)
  }
  return selectedPriceModeLabel.value
})

const upperLimitDisplay = computed(() => {
  const v = pickQuoteNumber(props.quoteSnapshot?.upper_limit_price)
  return v > 0 ? fmtNumber(v, 0) : '--'
})

const lowerLimitDisplay = computed(() => {
  const v = pickQuoteNumber(props.quoteSnapshot?.lower_limit_price)
  return v > 0 ? fmtNumber(v, 0) : '--'
})

function togglePriceModeMenu() {
  if (props.replayKlineMode) {
    selectedPriceMode.value = 'latest'
    priceFrozen.value = false
    manualPriceInput.value = ''
  }
  priceModeMenuOpen.value = !priceModeMenuOpen.value
}

function selectPriceMode(mode) {
  selectedPriceMode.value = mode
  priceFrozen.value = false
  manualPriceInput.value = ''
  priceModeMenuOpen.value = false
}

function freezeManualFromLatest(stepDirection = 0) {
  const tick = priceStepTick()
  if (tick <= 0) return
  const latest = latestPriceNow()
  const base = priceFrozen.value
    ? pickQuoteNumber(frozenLatestPrice.value)
    : pickQuoteNumber(latest)
  if (base <= 0) return
  const offset = stepDirection > 0 ? 1 : stepDirection < 0 ? -1 : 0
  frozenLatestPrice.value = Math.max(0, base + tick * offset)
  manualPriceInput.value = formatPriceForInput(frozenLatestPrice.value)
  priceFrozen.value = true
}

function onPriceAnchorClick() {
  if (priceFrozen.value) return
  freezeManualFromLatest(0)
}

function bumpFrozenPrice(stepDirection) {
  freezeManualFromLatest(stepDirection)
}

function onPriceAnchorInput(evt) {
  if (!priceFrozen.value) return
  const raw = String(evt?.target?.value || '').trim()
  manualPriceInput.value = raw
  const n = Number(raw)
  if (Number.isFinite(n) && n > 0) frozenLatestPrice.value = n
}

function closePriceModeMenuOnOutside(evt) {
  if (!priceModeMenuOpen.value) return
  const target = evt.target
  if (!(target instanceof Node)) return
  if (priceModeMenuRef.value?.contains?.(target)) return
  priceModeMenuOpen.value = false
}

function closePositionMenuOnOutside(evt) {
  if (!positionMenuOpen.value) return
  const target = evt.target
  if (!(target instanceof HTMLElement)) return
  if (target.closest('.trade-classic-position-menu')) return
  positionMenuOpen.value = false
}

function closeOrdersMenuOnOutside(evt) {
  if (!ordersMenuOpen.value) return
  const target = evt.target
  if (!(target instanceof HTMLElement)) return
  if (target.closest('.trade-classic-orders-menu')) return
  ordersMenuOpen.value = false
}

function closeWorkingOrderMenuOnOutside(evt) {
  if (!workingOrderMenuOpen.value) return
  const target = evt.target
  if (!(target instanceof HTMLElement)) return
  if (target.closest('.trade-classic-working-menu')) return
  workingOrderMenuOpen.value = false
}

function closeSideByPosition(item) {
  const direction = String(item?.direction || '').trim().toLowerCase()
  return direction === 'short' ? 'buy' : 'sell'
}

function reverseOpenDirectionByPosition(item) {
  const direction = String(item?.direction || '').trim().toLowerCase()
  return direction === 'short' ? 'buy' : 'sell'
}

function resolveSidePriceByMode(mode, side) {
  const quote = props.quoteSnapshot || {}
  const latest = latestPriceNow()
  if (props.replayKlineMode) return latest
  const bid1 = pickQuoteNumber(quote?.bid_price1)
  const ask1 = pickQuoteNumber(quote?.ask_price1)
  const lowerLimit = pickQuoteNumber(quote?.lower_limit_price)
  const upperLimit = pickQuoteNumber(quote?.upper_limit_price)
  if (mode === 'opponent') {
    return side === 'buy'
      ? pickQuoteNumber(ask1, latest, bid1)
      : pickQuoteNumber(bid1, latest, ask1)
  }
  if (mode === 'queue') {
    return side === 'buy'
      ? pickQuoteNumber(bid1, latest, ask1)
      : pickQuoteNumber(ask1, latest, bid1)
  }
  if (mode === 'market') {
    return side === 'buy'
      ? pickQuoteNumber(upperLimit, latest, ask1, bid1)
      : pickQuoteNumber(lowerLimit, latest, bid1, ask1)
  }
  return pickQuoteNumber(latest, ask1, bid1)
}

function resolveClosePrice(item, mode = 'active') {
  const side = closeSideByPosition(item)
  if (mode === 'active') return resolveSidePrice(side)
  return resolveSidePriceByMode(mode, side)
}

function resolveReverseOpenPrice(item) {
  const side = reverseOpenDirectionByPosition(item)
  return resolveSidePriceByMode('opponent', side)
}

function selectPositionRow(item) {
  selectedPositionKey.value = positionKey(item)
}

function onPositionRowClick(item) {
  selectPositionRow(item)
  positionMenuOpen.value = false
}

function onPositionRowContextMenu(item, evt) {
  evt.preventDefault()
  selectPositionRow(item)
  positionMenuX.value = evt.clientX
  positionMenuY.value = evt.clientY
  positionMenuOpen.value = true
}

function onOrderRowClick(item) {
  selectedOrderKey.value = orderKey(item)
  ordersMenuOpen.value = false
}

function onOrderRowContextMenu(item, evt) {
  evt.preventDefault()
  onOrderRowClick(item)
  ordersMenuX.value = evt.clientX
  ordersMenuY.value = evt.clientY
  ordersMenuOpen.value = true
}

function onOrdersTableContextMenu(evt) {
  evt.preventDefault()
  ordersMenuX.value = evt.clientX
  ordersMenuY.value = evt.clientY
  ordersMenuOpen.value = true
}

function onTradeRowClick(item) {
  selectedTradeKey.value = tradeKey(item)
}

function onTradeRowContextMenu(item, evt) {
  evt.preventDefault()
  onTradeRowClick(item)
}

function onWorkingOrderRowClick(item) {
  selectedWorkingOrderKey.value = orderKey(item)
  workingOrderMenuOpen.value = false
}

function onWorkingOrderRowContextMenu(item, evt) {
  evt.preventDefault()
  onWorkingOrderRowClick(item)
  workingOrderMenuX.value = evt.clientX
  workingOrderMenuY.value = evt.clientY
  workingOrderMenuOpen.value = true
}

function isOrderCancelable(item) {
  return orderVolumeRemaining(item) > 0
}

function isOrderCanceled(item) {
  if (orderVolumeCanceled(item) > 0) return true
  const status = String(item?.order_status || '').toLowerCase()
  const msg = String(item?.status_msg || '').toLowerCase()
  return status.includes('cancel') || status.includes('撤') || msg.includes('cancel') || msg.includes('撤')
}

function setOrdersFilterMode(mode) {
  ordersFilterMode.value = mode
  ordersMenuOpen.value = false
}

function cancelSelectedWorkingOrder() {
  if (!selectedWorkingOrder.value) return
  emit('cancel-order', selectedWorkingOrder.value)
  workingOrderMenuOpen.value = false
}

function openAmendDialogForSelectedWorkingOrder() {
  if (!selectedWorkingOrder.value) return
  amendOrderTarget.value = selectedWorkingOrder.value
  amendForm.value = {
    price: String(Number(selectedWorkingOrder.value.limit_price || 0)),
    volume: Math.max(1, orderVolumeRemaining(selectedWorkingOrder.value) || orderVolumeOriginal(selectedWorkingOrder.value) || 1),
  }
  amendDialogOpen.value = true
  workingOrderMenuOpen.value = false
}

function closeAmendDialog() {
  amendDialogOpen.value = false
}

function submitAmendOrder() {
  if (!amendOrderTarget.value) return
  const price = Number(amendForm.value.price)
  const volume = Number(amendForm.value.volume)
  if (!Number.isFinite(price) || price <= 0) return
  if (!Number.isFinite(volume) || volume <= 0) return
  emit('amend-order', {
    order: amendOrderTarget.value,
    limit_price: price,
    volume: Math.max(1, Math.floor(volume)),
  })
  amendDialogOpen.value = false
}

function emitCloseSelectedPosition(ratio = 1, mode = 'active') {
  if (!selectedPosition.value || selectedPositionClosable.value <= 0) return
  emit('position-close', {
    position: selectedPosition.value,
    ratio,
    limit_price: resolveClosePrice(selectedPosition.value, mode),
  })
  positionMenuOpen.value = false
}

function emitReverseSelectedPosition() {
  if (!selectedPosition.value || selectedPositionClosable.value <= 0) return
  emit('position-reverse', {
    position: selectedPosition.value,
    volume: selectedPositionClosable.value,
    close_limit_price: resolveClosePrice(selectedPosition.value, 'market'),
    open_limit_price: resolveReverseOpenPrice(selectedPosition.value),
    open_direction: reverseOpenDirectionByPosition(selectedPosition.value),
  })
  positionMenuOpen.value = false
}

onMounted(() => {
  document.addEventListener('pointerdown', closePriceModeMenuOnOutside)
  document.addEventListener('pointerdown', closePositionMenuOnOutside)
  document.addEventListener('pointerdown', closeOrdersMenuOnOutside)
  document.addEventListener('pointerdown', closeWorkingOrderMenuOnOutside)
})

onBeforeUnmount(() => {
  document.removeEventListener('pointerdown', closePriceModeMenuOnOutside)
  document.removeEventListener('pointerdown', closePositionMenuOnOutside)
  document.removeEventListener('pointerdown', closeOrdersMenuOnOutside)
  document.removeEventListener('pointerdown', closeWorkingOrderMenuOnOutside)
})

function fundsRows() {
  const funds = props.terminal?.funds || {}
  return [
    ['币种', 'CNY', '交易日', props.terminal?.summary?.trading_day || '--'],
    ['静态权益', fmtNumber(funds.static_balance), '动态权益', fmtNumber(funds.dynamic_balance)],
    ['冻结资金', fmtNumber(funds.frozen_cash), '可用资金', fmtNumber(funds.available)],
    ['手续费', fmtNumber(funds.commission), '保证金', fmtNumber(funds.margin)],
    ['平仓盈亏', fmtNumber(funds.close_profit), '持仓盈亏', fmtNumber(funds.position_profit)],
    ['资金使用率', fmtPct(funds.risk_ratio), '更新时间', fmtTime(funds.updated_at)],
  ]
}
</script>

<template>
  <div class="trade-classic-layer">
    <section v-if="visible" class="trade-classic-window" :style="rootStyle">
      <span class="trade-classic-resize trade-classic-resize-n" @pointerdown="onResizePointerDown('n', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-s" @pointerdown="onResizePointerDown('s', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-w" @pointerdown="onResizePointerDown('w', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-e" @pointerdown="onResizePointerDown('e', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-nw" @pointerdown="onResizePointerDown('nw', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-ne" @pointerdown="onResizePointerDown('ne', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-sw" @pointerdown="onResizePointerDown('sw', $event)"></span>
      <span class="trade-classic-resize trade-classic-resize-se" @pointerdown="onResizePointerDown('se', $event)"></span>

      <header class="trade-classic-titlebar" :class="{ dragging, resizing }" @pointerdown="onHeaderPointerDown">
        <div class="trade-classic-title">
          <span>{{ titleText }}</span>
        </div>
        <button class="trade-classic-close" type="button" title="隐藏" aria-label="隐藏" @click="emit('close')">
          <span class="trade-classic-hide-icon" aria-hidden="true"></span>
        </button>
      </header>

      <div class="trade-classic-shell">
        <div class="trade-classic-main">
          <section class="trade-classic-order-pane">
            <div class="trade-classic-entry-grid">
              <label class="trade-classic-field trade-classic-field-symbol">
                <span>合约</span>
                <div class="trade-classic-input-row">
                  <input ref="symbolInputRef" :value="orderForm.symbol" @input="updateField('symbol', $event)" />
                  <button type="button" class="trade-classic-search">Q</button>
                </div>
              </label>

              <label class="trade-classic-field trade-classic-field-small">
                <span>手数 ↩</span>
                <input type="number" min="1" step="1" :value="orderForm.volume" @input="updateNumberField('volume', $event)" />
              </label>

              <label class="trade-classic-field trade-classic-field-price">
                <div ref="priceModeMenuRef" class="trade-classic-price-control">
                  <button
                    type="button"
                    class="trade-classic-price-trigger"
                    :class="{ open: priceModeMenuOpen }"
                    @click.stop="togglePriceModeMenu"
                    title="选择价格类型"
                  >
                    价格 ...
                  </button>
                  <div class="trade-classic-price-box">
                    <input
                      type="text"
                      class="trade-classic-price-anchor"
                      :value="priceAnchorDisplay"
                      :readonly="!priceFrozen"
                      title="点击后按最新价转为手动价"
                      @click.stop="onPriceAnchorClick"
                      @input="onPriceAnchorInput"
                      :class="{ readonly: !priceFrozen, frozen: priceFrozen }"
                    />
                    <div class="trade-classic-price-stepper">
                      <button type="button" class="trade-classic-price-step" title="加一跳" @click.stop="bumpFrozenPrice(1)">▲</button>
                      <button type="button" class="trade-classic-price-step" title="减一跳" @click.stop="bumpFrozenPrice(-1)">▼</button>
                    </div>
                    <div class="trade-classic-limit-box">
                      <span class="trade-classic-limit-up">{{ upperLimitDisplay }}</span>
                      <span class="trade-classic-limit-down">{{ lowerLimitDisplay }}</span>
                    </div>
                  </div>
                  <div v-if="priceModeMenuOpen" class="trade-classic-price-menu">
                    <button
                      v-for="mode in priceModeOptions"
                      :key="mode.key"
                      type="button"
                      class="trade-classic-price-menu-item"
                      :class="{ active: !priceFrozen && selectedPriceMode === mode.key }"
                      @click="selectPriceMode(mode.key)"
                    >
                      {{ mode.label }}
                    </button>
                  </div>
                </div>
              </label>
            </div>

            <div class="trade-classic-actions">
              <button type="button" class="trade-classic-action trade-classic-buy" @click="quickOrder('buy_open')">
                <span class="trade-classic-action-price">{{ sidePrice('buy') }}</span>
                <span class="trade-classic-action-text">买多</span>
                <span class="trade-classic-action-foot">{{ sideDeltaText('buy') }}</span>
              </button>
              <button type="button" class="trade-classic-action trade-classic-sell" @click="quickOrder('sell_open')">
                <span class="trade-classic-action-price">{{ sidePrice('sell') }}</span>
                <span class="trade-classic-action-text">卖空</span>
                <span class="trade-classic-action-foot">{{ sideDeltaText('sell') }}</span>
              </button>
              <button type="button" class="trade-classic-action trade-classic-closeall" @click="quickOrder('close')">
                <span class="trade-classic-action-text trade-classic-action-text-only">平仓</span>
              </button>
            </div>

            <div class="trade-classic-sub-actions">
              <button type="button" @click="emit('set-tab', 'orders')">撤单</button>
              <button type="button" disabled>对价跟</button>
              <button type="button" disabled>排队跟</button>
              <button type="button" disabled>连续追</button>
            </div>

            <div class="trade-classic-instrument">
              {{ instrumentInfoText }}
              <span v-if="props.replayKlineMode" class="trade-classic-mode-note">K线回放：仅最新价/指定价</span>
            </div>
            <div class="trade-classic-placeholder-box"></div>

            <div class="trade-classic-links">
              <button type="button" class="linkish" :disabled="props.replayKlineMode">止损开仓</button>
              <button type="button" class="linkish" :disabled="props.replayKlineMode">画线下单</button>
              <button type="button" class="linkish" :disabled="props.replayKlineMode">添加条件单</button>
              <button type="button" class="linkish" @click="emit('adjust-cashflow')">出入金</button>
              <button type="button" class="linkish" @click="emit('adjust-fee')">费用调整</button>
            </div>
          </section>

          <section class="trade-classic-right">
            <div class="trade-classic-top-tabs">
              <button
                v-for="item in topTabs"
                :key="item.key"
                type="button"
                class="trade-classic-tab"
                :class="{ active: activeTab === item.key }"
                @click="emit('set-tab', item.key)"
              >
                {{ item.label }}
              </button>
            </div>

            <div class="trade-classic-gridbox trade-classic-gridbox-top">
              <table v-if="activeTab === 'positions'" class="trade-classic-table">
                <thead>
                  <tr>
                    <th>品种</th>
                    <th>合约</th>
                    <th>方向</th>
                    <th>总仓</th>
                    <th>可用</th>
                    <th>开仓均价</th>
                    <th>盈亏价差</th>
                    <th>逐笔浮盈</th>
                    <th>浮盈比例</th>
                    <th>保证金</th>
                    <th>资金占比</th>
                    <th>持仓均价</th>
                    <th>盯市浮盈</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="item in terminal?.positions || []"
                    :key="`${item.symbol}-${item.direction}-${item.hedge_flag}`"
                    class="trade-classic-row-selectable"
                    :class="{ selected: selectedPositionKey === positionKey(item) }"
                    @click="onPositionRowClick(item)"
                    @contextmenu="onPositionRowContextMenu(item, $event)"
                  >
                    <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                    <td>{{ item.symbol }}</td>
                    <td>{{ item.direction }}</td>
                    <td>{{ fmtInt(item.position) }}</td>
                    <td>{{ fmtInt(item.closable) }}</td>
                    <td>{{ fmtNumber(item.avg_price, 0) }}</td>
                    <td>{{ fmtNumber(Number(item.market_price || 0) - Number(item.avg_price || 0), 2) }}</td>
                    <td>{{ fmtNumber(item.float_pnl, 2) }}</td>
                    <td>{{ item.use_margin ? fmtPct(Number(item.float_pnl || 0) / Number(item.use_margin || 1)) : '0.00%' }}</td>
                    <td>{{ fmtNumber(item.use_margin, 2) }}</td>
                    <td>{{ terminal?.summary?.balance ? fmtPct(Number(item.use_margin || 0) / Number(terminal.summary.balance || 1)) : '0.00%' }}</td>
                    <td>{{ fmtNumber(item.avg_price, 0) }}</td>
                    <td>{{ fmtNumber(item.float_pnl, 2) }}</td>
                  </tr>
                  <tr v-if="!(terminal?.positions || []).length">
                    <td colspan="13">无持仓</td>
                  </tr>
                </tbody>
              </table>

              <table v-else-if="activeTab === 'orders'" class="trade-classic-table" @contextmenu="onOrdersTableContextMenu($event)">
                <thead>
                  <tr>
                    <th>时间 ▲</th>
                    <th>品种</th>
                    <th>合约</th>
                    <th>状态</th>
                    <th>买卖</th>
                    <th>开平</th>
                    <th>委托价</th>
                    <th>委托量</th>
                    <th>可撤</th>
                    <th>已成交</th>
                    <th>已撤单</th>
                    <th>状态说明</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="item in filteredOrders"
                    :key="item.command_id"
                    class="trade-classic-row-selectable"
                    :class="{ selected: selectedOrderKey === orderKey(item) }"
                    @click="onOrderRowClick(item)"
                    @contextmenu="onOrderRowContextMenu(item, $event)"
                  >
                    <td>{{ fmtTime(item.updated_at || item.inserted_at) }}</td>
                    <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                    <td>{{ item.symbol }}</td>
                    <td>{{ item.order_status }}</td>
                    <td>{{ item.direction }}</td>
                    <td>{{ item.offset_flag }}</td>
                    <td>{{ fmtNumber(item.limit_price, 0) }}</td>
                    <td>{{ fmtInt(orderVolumeOriginal(item)) }}</td>
                    <td>{{ fmtInt(orderVolumeRemaining(item)) }}</td>
                    <td>{{ fmtInt(orderVolumeTraded(item)) }}</td>
                    <td>{{ fmtInt(orderVolumeCanceled(item)) }}</td>
                    <td>{{ item.status_msg || '--' }}</td>
                  </tr>
                  <tr v-if="!filteredOrders.length">
                    <td colspan="12">无委托</td>
                  </tr>
                </tbody>
              </table>

              <table v-else-if="activeTab === 'trades'" class="trade-classic-table">
                <thead>
                  <tr>
                    <th>时间 ▲</th>
                    <th>品种</th>
                    <th>合约</th>
                    <th>买卖</th>
                    <th>开平</th>
                    <th>成交价</th>
                    <th>成交量</th>
                    <th>流水</th>
                    <th>手续费</th>
                    <th>按保</th>
                    <th>合约号</th>
                    <th>主场号</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="item in terminal?.trades || []"
                    :key="`${item.trade_id}-${item.received_at}`"
                    class="trade-classic-row-selectable"
                    :class="{ selected: selectedTradeKey === tradeKey(item) }"
                    @click="onTradeRowClick(item)"
                    @contextmenu="onTradeRowContextMenu(item, $event)"
                  >
                    <td>{{ fmtTime(item.trade_time) }}</td>
                    <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                    <td>{{ item.symbol }}</td>
                    <td>{{ item.direction }}</td>
                    <td>{{ item.offset_flag }}</td>
                    <td>{{ fmtNumber(item.price, 0) }}</td>
                    <td>{{ fmtInt(item.volume) }}</td>
                    <td>{{ item.trade_id }}</td>
                    <td>--</td>
                    <td>--</td>
                    <td>{{ item.order_sys_id || '--' }}</td>
                    <td>{{ item.exchange_id || '--' }}</td>
                  </tr>
                  <tr v-if="!(terminal?.trades || []).length">
                    <td colspan="12">无成交</td>
                  </tr>
                </tbody>
              </table>

              <table v-else class="trade-classic-table trade-classic-funds-table">
                <tbody>
                  <tr v-for="(row, idx) in fundsRows()" :key="idx">
                    <th>{{ row[0] }}</th>
                    <td>{{ row[1] }}</td>
                    <th>{{ row[2] }}</th>
                    <td>{{ row[3] }}</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <template v-if="activeTab === 'positions'">
              <div class="trade-classic-bottom-actions">
                <button type="button" :disabled="selectedPositionClosable <= 0" @click="emitCloseSelectedPosition(0.33, 'active')">平33%</button>
                <button type="button" :disabled="selectedPositionClosable <= 0" @click="emitCloseSelectedPosition(0.5, 'active')">平50%</button>
                <button type="button" :disabled="selectedPositionClosable <= 0" @click="emitCloseSelectedPosition(1, 'active')">平100%</button>
                <button type="button" :disabled="selectedPositionClosable <= 0" @click="emitReverseSelectedPosition">反手</button>
                <button type="button" disabled>损盈</button>
              </div>

              <div v-if="hasWorkingOrders" class="trade-classic-gridbox trade-classic-gridbox-bottom">
                <table class="trade-classic-table">
                  <thead>
                    <tr>
                      <th>时间 ▲</th>
                      <th>品种</th>
                      <th>合约</th>
                      <th>状态</th>
                      <th>买卖</th>
                      <th>开平</th>
                      <th>委托价</th>
                      <th>委托量</th>
                      <th>可撤</th>
                      <th>已成交</th>
                      <th>按保</th>
                      <th>预止损</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr
                      v-for="item in terminal?.working_orders || []"
                      :key="item.command_id"
                      class="trade-classic-row-selectable"
                      :class="{ selected: selectedWorkingOrderKey === orderKey(item) }"
                      @click="onWorkingOrderRowClick(item)"
                      @contextmenu="onWorkingOrderRowContextMenu(item, $event)"
                    >
                      <td>{{ fmtTime(item.updated_at || item.inserted_at) }}</td>
                      <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                      <td>{{ item.symbol }}</td>
                      <td>{{ item.order_status }}</td>
                      <td>{{ item.direction }}</td>
                      <td>{{ item.offset_flag }}</td>
                      <td>{{ fmtNumber(item.limit_price, 0) }}</td>
                      <td>{{ fmtInt(orderVolumeOriginal(item)) }}</td>
                      <td>
                        <button type="button" class="linkish-inline" @click="emit('cancel-order', item)">
                          {{ fmtInt(orderVolumeRemaining(item)) }}
                        </button>
                      </td>
                      <td>{{ fmtInt(orderVolumeTraded(item)) }}</td>
                      <td>{{ fmtNumber(item.limit_price * Math.max(0, orderVolumeRemaining(item)), 0) }}</td>
                      <td>--</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </template>
          </section>
        </div>
      </div>
    </section>
    <div
      v-if="positionMenuOpen && selectedPosition"
      class="trade-classic-position-menu"
      :style="{ left: `${positionMenuX}px`, top: `${positionMenuY}px` }"
    >
      <button type="button" @click="emitCloseSelectedPosition(1, 'opponent')">对手价平仓</button>
      <button type="button" @click="emitCloseSelectedPosition(1, 'latest')">最新价平仓</button>
      <button type="button" @click="emitCloseSelectedPosition(1, 'market')">市价平仓</button>
    </div>
    <div
      v-if="ordersMenuOpen && activeTab === 'orders'"
      class="trade-classic-orders-menu"
      :style="{ left: `${ordersMenuX}px`, top: `${ordersMenuY}px` }"
    >
      <button type="button" :class="{ active: ordersFilterMode === 'all' }" @click="setOrdersFilterMode('all')">显示全部</button>
      <button type="button" :class="{ active: ordersFilterMode === 'cancelable' }" @click="setOrdersFilterMode('cancelable')">显示可撤</button>
      <button type="button" :class="{ active: ordersFilterMode === 'canceled' }" @click="setOrdersFilterMode('canceled')">显示已撤</button>
    </div>
    <div
      v-if="workingOrderMenuOpen && activeTab === 'positions' && selectedWorkingOrder"
      class="trade-classic-working-menu"
      :style="{ left: `${workingOrderMenuX}px`, top: `${workingOrderMenuY}px` }"
    >
      <button type="button" @click="cancelSelectedWorkingOrder">撤单</button>
      <button type="button" @click="openAmendDialogForSelectedWorkingOrder">改价</button>
    </div>
    <div v-if="amendDialogOpen" class="trade-classic-amend-mask" @click.self="closeAmendDialog">
      <section class="trade-classic-amend-dialog" role="dialog" aria-modal="true" aria-label="改价">
        <header class="trade-classic-amend-title">
          <span>改价</span>
          <button type="button" @click="closeAmendDialog">×</button>
        </header>
        <div class="trade-classic-amend-body">
          <div class="trade-classic-amend-row">
            <span class="trade-classic-amend-symbol">{{ amendOrderTarget?.symbol || '--' }}</span>
            <span>{{ amendDirectionLabel }}，{{ amendOffsetLabel }}</span>
          </div>
          <div class="trade-classic-amend-row">
            <label>手</label>
            <input v-model.number="amendForm.volume" type="number" min="1" step="1" />
            <label>价格</label>
            <input v-model="amendForm.price" type="number" min="0" step="0.01" />
          </div>
        </div>
        <footer class="trade-classic-amend-actions">
          <button type="button" class="confirm" @click="submitAmendOrder">确定</button>
          <button type="button" @click="closeAmendDialog">取消</button>
        </footer>
      </section>
    </div>
  </div>
</template>

<style scoped>
.trade-classic-layer {
  position: absolute;
  inset: 0;
  pointer-events: none;
  z-index: 45;
}

.trade-classic-window {
  position: absolute;
  top: 0;
  left: 0;
  pointer-events: auto;
  display: flex;
  flex-direction: column;
  background: #f8f8f8;
  border: 1px solid #7f9db9;
  box-shadow: 0 5px 14px rgba(0, 0, 0, 0.2);
  color: #111;
  overflow: hidden;
}

.trade-classic-resize {
  position: absolute;
  pointer-events: auto;
  z-index: 3;
}

.trade-classic-resize-n,
.trade-classic-resize-s {
  left: 8px;
  right: 8px;
  height: 6px;
  cursor: ns-resize;
}

.trade-classic-resize-n {
  top: -3px;
}

.trade-classic-resize-s {
  bottom: -3px;
}

.trade-classic-resize-w,
.trade-classic-resize-e {
  top: 8px;
  bottom: 8px;
  width: 6px;
  cursor: ew-resize;
}

.trade-classic-resize-w {
  left: -3px;
}

.trade-classic-resize-e {
  right: -3px;
}

.trade-classic-resize-nw,
.trade-classic-resize-se,
.trade-classic-resize-ne,
.trade-classic-resize-sw {
  width: 10px;
  height: 10px;
}

.trade-classic-resize-nw {
  top: -4px;
  left: -4px;
  cursor: nwse-resize;
}

.trade-classic-resize-se {
  right: -4px;
  bottom: -4px;
  cursor: nwse-resize;
}

.trade-classic-resize-ne {
  top: -4px;
  right: -4px;
  cursor: nesw-resize;
}

.trade-classic-resize-sw {
  left: -4px;
  bottom: -4px;
  cursor: nesw-resize;
}

.trade-classic-titlebar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 22px;
  padding: 0 5px 0 4px;
  background: #eaf4ff;
  border-bottom: 1px solid #8eb4d8;
  cursor: move;
  user-select: none;
  font-size: 11px;
  line-height: 1;
}

.trade-classic-titlebar.dragging {
  cursor: grabbing;
}

.trade-classic-title {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  padding-top: 1px;
}

.trade-classic-title > span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.trade-classic-close {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 16px;
  padding: 0;
  border: none;
  background: transparent;
  cursor: pointer;
  color: #111;
  line-height: 1;
}

.trade-classic-hide-icon {
  display: inline-block;
  width: 11px;
  height: 7px;
  position: relative;
}

.trade-classic-hide-icon::before,
.trade-classic-hide-icon::after {
  content: '';
  position: absolute;
  left: 0;
  right: 0;
  margin: auto;
  display: block;
  background: #111;
}

.trade-classic-hide-icon::before {
  bottom: 0;
  width: 11px;
  height: 1px;
}

.trade-classic-hide-icon::after {
  top: 0;
  width: 7px;
  height: 7px;
  background: transparent;
  border-right: 1px solid #111;
  border-bottom: 1px solid #111;
  transform: rotate(45deg);
  transform-origin: center;
}

.trade-classic-shell {
  display: flex;
  flex-direction: column;
  min-height: 0;
  background: #f3f3f3;
}

.trade-classic-main {
  display: grid;
  grid-template-columns: 384px minmax(0, 1fr);
  min-height: 0;
}

.trade-classic-order-pane {
  border-right: 1px solid #bccddd;
  padding: 7px 8px 8px;
  background: #f7f7f7;
}

.trade-classic-entry-grid {
  display: grid;
  grid-template-columns: 108px 56px 1fr;
  gap: 5px 8px;
  align-items: end;
}

.trade-classic-field {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 11px;
  color: #24384c;
}

.trade-classic-field-symbol {
  grid-column: 1;
}

.trade-classic-field-small {
  grid-column: 2;
}

.trade-classic-field-price {
  grid-column: 3;
}

.trade-classic-input-row,
.trade-classic-price-box {
  display: grid;
  grid-template-columns: 1fr 22px;
  gap: 4px;
}

.trade-classic-price-control {
  position: relative;
  display: grid;
  gap: 4px;
}

.trade-classic-price-box {
  display: grid;
  grid-template-columns: minmax(0, 82px) 16px 40px;
  gap: 4px;
  align-items: stretch;
}

.trade-classic-price-trigger {
  min-height: 20px;
  width: 56px;
  padding: 0 8px;
  border: 1px solid #b6c5d5;
  border-radius: 0;
  background: #efefef;
  color: #253f59;
  font-size: 12px;
  cursor: pointer;
}

.trade-classic-price-trigger:hover,
.trade-classic-price-trigger.open {
  border-color: #7fa3c9;
  background: #e8f0fa;
}

.trade-classic-window input,
.trade-classic-window select {
  width: 100%;
  min-width: 0;
  min-height: 26px;
  padding: 1px 5px;
  border: 1px solid #b6c5d5;
  border-radius: 0;
  background: #fff;
  font-size: 12px;
  color: #111;
}

.trade-classic-search {
  min-height: 26px;
  border: 1px solid #b6c5d5;
  background: #f6f8fb;
  border-radius: 0;
  font-size: 11px;
  cursor: pointer;
  color: #111;
}

.trade-classic-price-anchor {
  min-height: 26px;
  border: 1px solid #b6c5d5;
  border-radius: 0;
  background: #f9fbfd;
  color: #111;
  cursor: pointer;
  text-align: right;
  padding: 0 5px;
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.2px;
}

.trade-classic-price-anchor:hover,
.trade-classic-price-trigger:hover {
  border-color: #7fa3c9;
  background: #eef5fc;
}

.trade-classic-price-anchor-label {
  font-size: 11px;
  color: #2c4864;
}

.trade-classic-price-anchor.readonly {
  background: #f1f4f8;
  color: #3a5067;
  cursor: default;
}

.trade-classic-price-anchor.frozen {
  background: #fff;
  color: #111;
  border-color: #8da6c1;
  cursor: text;
}

.trade-classic-limit-box {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: flex-end;
  padding: 1px 0;
  font-size: 12px;
  line-height: 1.1;
  user-select: none;
}

.trade-classic-price-stepper {
  display: grid;
  grid-template-rows: 1fr 1fr;
  gap: 2px;
}

.trade-classic-price-step {
  min-height: 12px;
  padding: 0;
  border: 1px solid #b6c5d5;
  border-radius: 0;
  background: #f4f7fb;
  color: #253f59;
  font-size: 9px;
  line-height: 1;
  cursor: pointer;
}

.trade-classic-price-step:hover {
  border-color: #7fa3c9;
  background: #e8f0fa;
}

.trade-classic-limit-up {
  color: #d71c1c;
  font-weight: 700;
}

.trade-classic-limit-down {
  color: #10980e;
  font-weight: 700;
}

.trade-classic-price-menu {
  position: absolute;
  left: 60px;
  top: 0;
  z-index: 4;
  display: flex;
  gap: 4px;
  min-width: 0;
  padding: 4px;
  border: 1px solid #9bb2c9;
  background: #f3f6fa;
  box-shadow: 0 4px 9px rgba(0, 0, 0, 0.16);
}

.trade-classic-price-menu-item {
  min-height: 24px;
  min-width: 62px;
  padding: 0 10px;
  border: 1px solid #b6c5d5;
  background: #fff;
  font-size: 12px;
  color: #1e2f40;
  text-align: center;
  white-space: nowrap;
  cursor: pointer;
}

.trade-classic-price-menu-item:hover {
  background: #edf4fb;
}

.trade-classic-price-menu-item.active {
  border-color: #6f93b9;
  background: #e8f0fa;
  font-weight: 700;
}

.trade-classic-actions {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 12px;
  margin-top: 8px;
}

.trade-classic-action {
  min-height: 56px;
  border: 1px solid #8eaad1;
  background: #e7eef6;
  font-weight: 700;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 1px;
  border-radius: 2px;
  padding: 3px 0;
}

.trade-classic-buy {
  color: #d71c1c;
}

.trade-classic-sell {
  color: #10980e;
}

.trade-classic-closeall {
  color: #2744bf;
}

.trade-classic-action-price {
  font-size: 15px;
  line-height: 1;
}

.trade-classic-action-text {
  font-size: 17px;
  line-height: 1.05;
}

.trade-classic-action-text-only {
  margin-top: 8px;
}

.trade-classic-action-foot {
  font-size: 10px;
  font-weight: 400;
  color: #111;
}

.trade-classic-sub-actions,
.trade-classic-links,
.trade-classic-bottom-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.trade-classic-sub-actions {
  margin-top: 6px;
}

.trade-classic-sub-actions button,
.trade-classic-bottom-actions button,
.trade-classic-tab {
  color: #111;
}

.trade-classic-sub-actions button,
.trade-classic-bottom-actions button {
  min-height: 20px;
  padding: 0 9px;
  border: 1px solid #b5c6d7;
  background: #f9f9f9;
  border-radius: 6px;
  font-size: 11px;
}

.trade-classic-sub-actions button:disabled,
.trade-classic-bottom-actions button:disabled {
  color: #111;
  opacity: 0.55;
  cursor: not-allowed;
}

.trade-classic-instrument {
  margin-top: 7px;
  min-height: 20px;
  padding: 3px 2px;
  font-size: 11px;
  border-top: 1px solid #d6dee6;
  color: #1b2a38;
}

.trade-classic-mode-note {
  margin-left: 10px;
  color: #8a5a00;
}

.trade-classic-placeholder-box {
  height: 30px;
  margin-top: 4px;
  border: 1px solid #d0d8e0;
  background: #fff;
}

.trade-classic-links {
  margin-top: 4px;
}

.linkish,
.linkish-inline {
  border: none;
  background: transparent;
  color: #0039c6;
  padding: 0;
  min-height: 18px;
  font-size: 11px;
  cursor: pointer;
}

.linkish:disabled,
.linkish-inline:disabled {
  color: #6b7280;
  cursor: not-allowed;
  opacity: 0.65;
}

.trade-classic-right {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto minmax(0, 0.86fr);
  min-height: 0;
  padding: 0 3px 3px;
  background: #f4f4f4;
}

.trade-classic-top-tabs {
  display: flex;
  gap: 1px;
  padding: 1px 0 0;
}

.trade-classic-tab {
  min-height: 21px;
  padding: 0 9px;
  border: 1px solid #b8c8d8;
  border-bottom: none;
  background: #e7edf3;
  font-size: 11px;
  cursor: pointer;
  border-radius: 0;
  flex: 0 0 auto;
}

.trade-classic-tab.active {
  background: #fff;
  color: #111;
  font-weight: 700;
}

.trade-classic-gridbox {
  background: #fff;
  border: 1px solid #b8c8d8;
  overflow: auto;
}

.trade-classic-gridbox-top {
  border-top: none;
}

.trade-classic-gridbox-bottom {
  margin-top: 3px;
}

.trade-classic-bottom-actions {
  justify-content: flex-end;
  padding: 3px 0;
}

.trade-classic-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}

.trade-classic-table th,
.trade-classic-table td {
  height: 20px;
  padding: 0 4px;
  border-right: 1px solid #d6e2ee;
  border-bottom: 1px solid #d6e2ee;
  white-space: nowrap;
  text-align: left;
}

.trade-classic-table thead th {
  background: #edf3f8;
  color: #24384c;
  position: sticky;
  top: 0;
  z-index: 1;
}

.trade-classic-row-selectable {
  cursor: default;
}

.trade-classic-row-selectable:hover td {
  background: #f2f7fd;
}

.trade-classic-row-selectable.selected td {
  background: #dcecff;
}

.trade-classic-position-menu {
  position: fixed;
  z-index: 70;
  min-width: 132px;
  display: grid;
  gap: 1px;
  border: 1px solid #8ea5be;
  background: #e8edf3;
  box-shadow: 0 6px 16px rgba(0, 0, 0, 0.22);
  pointer-events: auto;
}

.trade-classic-position-menu button {
  min-height: 26px;
  border: none;
  background: #fff;
  padding: 0 10px;
  text-align: left;
  font-size: 12px;
  color: #1e2f40;
  cursor: pointer;
}

.trade-classic-position-menu button:hover {
  background: #eaf3fd;
}

.trade-classic-orders-menu,
.trade-classic-working-menu {
  position: fixed;
  z-index: 70;
  min-width: 132px;
  display: grid;
  gap: 1px;
  border: 1px solid #8ea5be;
  background: #e8edf3;
  box-shadow: 0 6px 16px rgba(0, 0, 0, 0.22);
  pointer-events: auto;
}

.trade-classic-orders-menu button,
.trade-classic-working-menu button {
  min-height: 26px;
  border: none;
  background: #fff;
  padding: 0 10px;
  text-align: left;
  font-size: 12px;
  color: #1e2f40;
  cursor: pointer;
}

.trade-classic-orders-menu button:hover,
.trade-classic-working-menu button:hover {
  background: #eaf3fd;
}

.trade-classic-orders-menu button.active {
  background: #dcecff;
  font-weight: 700;
}

.trade-classic-amend-mask {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.08);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 72;
  pointer-events: auto;
}

.trade-classic-amend-dialog {
  width: 400px;
  border: 1px solid #9db2c7;
  background: #f4f5f7;
  box-shadow: 0 8px 18px rgba(0, 0, 0, 0.25);
}

.trade-classic-amend-title {
  min-height: 26px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 10px;
  border-bottom: 1px solid #c3cfdb;
  background: #e8edf3;
  font-size: 12px;
}

.trade-classic-amend-title button {
  border: none;
  background: transparent;
  font-size: 18px;
  line-height: 1;
  cursor: pointer;
}

.trade-classic-amend-body {
  padding: 16px 16px 10px;
  display: grid;
  gap: 10px;
}

.trade-classic-amend-row {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
}

.trade-classic-amend-symbol {
  min-width: 70px;
  font-weight: 700;
}

.trade-classic-amend-row label {
  color: #44596d;
}

.trade-classic-amend-row input {
  width: 92px;
  min-height: 24px;
  padding: 0 5px;
  border: 1px solid #aebfd0;
  background: #fff;
}

.trade-classic-amend-actions {
  display: flex;
  justify-content: center;
  gap: 12px;
  padding: 4px 0 12px;
}

.trade-classic-amend-actions button {
  min-width: 74px;
  min-height: 24px;
  border: 1px solid #aebfd0;
  background: #f5f7f9;
  cursor: pointer;
}

.trade-classic-amend-actions .confirm {
  border-color: #4d84c8;
  background: #f2f8ff;
}

.trade-classic-funds-table th {
  width: 100px;
  background: #edf3f8;
}

.trade-classic-funds-table td {
  width: 180px;
}

@media (max-width: 1280px) {
  .trade-classic-window {
    width: min(96vw, 1320px) !important;
  }

  .trade-classic-main {
    grid-template-columns: 320px minmax(0, 1fr);
  }
}
</style>
