import test from 'node:test'
import assert from 'node:assert/strict'
import { buildInitialVisibleLogicalRange, shouldRenderReversal } from '../src/chart/lightweightMode.js'

test('buildInitialVisibleLogicalRange shows recent window for large datasets', () => {
  const range = buildInitialVisibleLogicalRange(1728)
  assert.deepEqual(range, { from: 1488, to: 1733 })
})

test('buildInitialVisibleLogicalRange clamps to dataset size for small datasets', () => {
  const range = buildInitialVisibleLogicalRange(80)
  assert.deepEqual(range, { from: 0, to: 85 })
})

test('shouldRenderReversal only when explicitly enabled', () => {
  assert.equal(shouldRenderReversal({ enabled: false }), false)
  assert.equal(shouldRenderReversal({ enabled: true }), true)
  assert.equal(shouldRenderReversal({}), false)
  assert.equal(shouldRenderReversal(null), false)
})
