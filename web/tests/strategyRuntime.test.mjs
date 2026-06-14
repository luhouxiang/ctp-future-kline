import test from 'node:test'
import assert from 'node:assert/strict'
import {
  buildChartStrategyCompositionInstances,
  featureKeyForHelperStrategy,
  saveAndStartStrategyComposition,
} from '../src/chart/strategyRuntime.js'

const definitions = [
  {
    strategy_id: 'indicator.zigzag_atr26',
    display_name: 'ZigZag',
    default_params: { atr_period: 26 },
  },
  {
    strategy_id: 'ma20.state_diagram_short',
    display_name: 'MA20',
    default_params: { ma_period: 20 },
  },
]

const anchor = {
  adjusted_time: 1767229200,
  plot_time: 1767229200,
}

const scope = {
  symbol: 'rb2601',
  timeframe: '1m',
}

const composition = {
  composition_id: 'ma20_state_zigzag',
  display_name: 'MA20 + ZigZag',
  primary_strategy_id: 'ma20.state_diagram_short',
  helper_strategy_ids: ['indicator.zigzag_atr26'],
  primary_params: {},
  helper_params_by_strategy: {
    'indicator.zigzag_atr26': { atr_multiple: 2 },
  },
}

test('featureKeyForHelperStrategy strips indicator prefix', () => {
  assert.equal(featureKeyForHelperStrategy('indicator.zigzag_atr26'), 'zigzag_atr26')
  assert.equal(featureKeyForHelperStrategy('custom.signal'), 'custom.signal')
})

test('buildChartStrategyCompositionInstances creates helper before primary metadata', () => {
  const built = buildChartStrategyCompositionInstances({
    anchor,
    composition,
    definitions,
    scope,
    accountID: 'paper',
    now: 123,
  })

  assert.equal(built.helpers.length, 1)
  assert.equal(built.primary.strategy_id, 'ma20.state_diagram_short')
  assert.deepEqual(built.primary.params.feature_dependencies, ['zigzag_atr26'])
  assert.deepEqual(built.primary.params.helper_instance_ids, built.helperInstanceIDs)
  assert.equal(built.helpers[0].params.parent_instance_id, built.primaryInstanceID)
  assert.equal(built.helpers[0].params.composition_role, 'helper')
  assert.equal(built.primary.params.composition_role, 'primary')
})

test('saveAndStartStrategyComposition starts helper then primary', async () => {
  const calls = []
  const fetcher = async (url, options = {}) => {
    calls.push({ url, method: options.method || 'GET', body: options.body ? JSON.parse(options.body) : null })
    return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }

  const result = await saveAndStartStrategyComposition({
    anchor,
    composition,
    definitions,
    scope,
    accountID: 'paper',
    now: 123,
  }, fetcher)

  assert.deepEqual(calls.map((item) => item.method), ['POST', 'POST', 'POST', 'POST'])
  assert.equal(calls[0].body.strategy_id, 'indicator.zigzag_atr26')
  assert.match(calls[1].url, /indicator\.zigzag_atr26/)
  assert.equal(calls[2].body.strategy_id, 'ma20.state_diagram_short')
  assert.match(calls[3].url, /primary/)
  assert.equal(result.startedInstanceIDs.length, 2)
})

