<script setup>
import { computed, nextTick, ref, watch } from 'vue'

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
})

const emit = defineEmits([
  'close',
  'start-drag',
  'start-resize',
  'set-tab',
  'update-order-field',
  'cancel-order',
  'quick-order',
])

const symbolInputRef = ref(null)

watch(
  () => props.visible,
  async (visible) => {
    if (!visible) return
    await nextTick()
    symbolInputRef.value?.focus?.()
    symbolInputRef.value?.select?.()
  },
)

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
  emit('quick-order', kind)
}

function sidePrice(kind) {
  if (kind === 'buy') {
    return fmtNumber(props.quoteSnapshot?.ask_price1 ?? props.quoteSnapshot?.latest_price, 0)
  }
  return fmtNumber(props.quoteSnapshot?.bid_price1 ?? props.quoteSnapshot?.latest_price, 0)
}

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
                <span>价格 ...</span>
                <div class="trade-classic-price-box">
                  <input type="number" step="0.01" :value="orderForm.limit_price" @input="updateField('limit_price', $event)" />
                  <select :value="orderForm.exchange_id" @change="updateField('exchange_id', $event)">
                    <option value="">最新价</option>
                    <option :value="orderForm.exchange_id || ''">{{ orderForm.exchange_id || '交易所' }}</option>
                  </select>
                </div>
              </label>
            </div>

            <div class="trade-classic-actions">
              <button type="button" class="trade-classic-action trade-classic-buy" @click="quickOrder('buy_open')">
                <span class="trade-classic-action-price">{{ sidePrice('buy') }}</span>
                <span class="trade-classic-action-text">买多</span>
                <span class="trade-classic-action-foot">&lt;= {{ fmtInt(orderForm.volume) }}</span>
              </button>
              <button type="button" class="trade-classic-action trade-classic-sell" @click="quickOrder('sell_open')">
                <span class="trade-classic-action-price">{{ sidePrice('sell') }}</span>
                <span class="trade-classic-action-text">卖空</span>
                <span class="trade-classic-action-foot">&lt;= {{ fmtInt(orderForm.volume) }}</span>
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

            <div class="trade-classic-instrument">{{ instrumentInfoText }}</div>
            <div class="trade-classic-placeholder-box"></div>

            <div class="trade-classic-links">
              <button type="button" class="linkish">止损开仓</button>
              <button type="button" class="linkish">画线下单</button>
              <button type="button" class="linkish">添加条件单</button>
              <button type="button" class="linkish">出入金</button>
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
                  <tr v-for="item in terminal?.positions || []" :key="`${item.symbol}-${item.direction}-${item.hedge_flag}`">
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

              <table v-else-if="activeTab === 'orders'" class="trade-classic-table">
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
                  <tr v-for="item in terminal?.orders || []" :key="item.command_id">
                    <td>{{ fmtTime(item.updated_at || item.inserted_at) }}</td>
                    <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                    <td>{{ item.symbol }}</td>
                    <td>{{ item.order_status }}</td>
                    <td>{{ item.direction }}</td>
                    <td>{{ item.offset_flag }}</td>
                    <td>{{ fmtNumber(item.limit_price, 0) }}</td>
                    <td>{{ fmtInt(item.volume_total_original) }}</td>
                    <td>{{ fmtInt(item.remaining_volume) }}</td>
                    <td>{{ fmtInt(item.volume_traded) }}</td>
                    <td>{{ fmtInt(item.volume_canceled) }}</td>
                    <td>{{ item.status_msg || '--' }}</td>
                  </tr>
                  <tr v-if="!(terminal?.orders || []).length">
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
                  <tr v-for="item in terminal?.trades || []" :key="`${item.trade_id}-${item.received_at}`">
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
                <button type="button" disabled>平33%</button>
                <button type="button" disabled>平50%</button>
                <button type="button" disabled>平100%</button>
                <button type="button" disabled>反手</button>
                <button type="button" disabled>损盈</button>
              </div>

              <div class="trade-classic-gridbox trade-classic-gridbox-bottom">
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
                    <tr v-for="item in terminal?.working_orders || []" :key="item.command_id">
                      <td>{{ fmtTime(item.updated_at || item.inserted_at) }}</td>
                      <td>{{ item.symbol?.replace(/[0-9]/g, '') || '--' }}</td>
                      <td>{{ item.symbol }}</td>
                      <td>{{ item.order_status }}</td>
                      <td>{{ item.direction }}</td>
                      <td>{{ item.offset_flag }}</td>
                      <td>{{ fmtNumber(item.limit_price, 0) }}</td>
                      <td>{{ fmtInt(item.volume_total_original) }}</td>
                      <td>
                        <button type="button" class="linkish-inline" @click="emit('cancel-order', item)">
                          {{ fmtInt(item.remaining_volume) }}
                        </button>
                      </td>
                      <td>{{ fmtInt(item.volume_traded) }}</td>
                      <td>{{ fmtNumber(item.limit_price * Math.max(0, Number(item.remaining_volume || 0)), 0) }}</td>
                      <td>--</td>
                    </tr>
                    <tr v-if="!(terminal?.working_orders || []).length">
                      <td colspan="12">无待成交单</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </template>
          </section>
        </div>
      </div>
    </section>
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

.trade-classic-price-box {
  grid-template-columns: 1fr 82px;
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
  opacity: 1;
}

.trade-classic-instrument {
  margin-top: 7px;
  min-height: 20px;
  padding: 3px 2px;
  font-size: 11px;
  border-top: 1px solid #d6dee6;
  color: #1b2a38;
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
