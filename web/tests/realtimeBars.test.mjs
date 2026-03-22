import test from 'node:test'
import assert from 'node:assert/strict'
import { mergeRealtimeBarUpdate } from '../src/chart/realtimeBars.js'

test('mergeRealtimeBarUpdate inserts partial bar', () => {
  const out = mergeRealtimeBarUpdate([], {
    phase: 'partial',
    bar: {
      adjusted_time: 100,
      data_time: 100,
      open: 1,
      high: 2,
      low: 1,
      close: 2,
      volume: 3,
      open_interest: 4,
    },
  })
  assert.equal(out.length, 1)
  assert.equal(out[0].adjusted_time, 100)
  assert.equal(out[0].close, 2)
})

test('mergeRealtimeBarUpdate replaces existing partial with final', () => {
  const partial = mergeRealtimeBarUpdate([], {
    phase: 'partial',
    bar: {
      adjusted_time: 100,
      data_time: 100,
      open: 1,
      high: 2,
      low: 1,
      close: 2,
    },
  })
  const out = mergeRealtimeBarUpdate(partial, {
    phase: 'final',
    bar: {
      adjusted_time: 100,
      data_time: 100,
      open: 1,
      high: 3,
      low: 1,
      close: 3,
    },
  })
  assert.equal(out.length, 1)
  assert.equal(out[0].high, 3)
  assert.equal(out[0].close, 3)
})

test('mergeRealtimeBarUpdate appends newer bar in order', () => {
  const existing = [{
    adjusted_time: 100,
    data_time: 100,
    plot_time: 100,
    open: 1,
    high: 2,
    low: 1,
    close: 2,
    volume: 1,
    open_interest: 1,
  }]
  const out = mergeRealtimeBarUpdate(existing, {
    phase: 'partial',
    bar: {
      adjusted_time: 160,
      data_time: 160,
      open: 2,
      high: 4,
      low: 2,
      close: 3,
    },
  })
  assert.deepEqual(out.map((item) => item.adjusted_time), [100, 160])
})

