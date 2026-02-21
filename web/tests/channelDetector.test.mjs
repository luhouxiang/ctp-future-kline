import test from 'node:test'
import assert from 'node:assert/strict'
import { detectChannelsInVisibleRange, normalizeChannelSettingsV2 } from '../src/chart/analysis/channelDetector.js'

function makeChannelBars(count, opts = {}) {
  const slope = Number(opts.slope ?? 0.12)
  const base = Number(opts.base ?? 100)
  const width = Number(opts.width ?? 2.4)
  const noise = Number(opts.noise ?? 0.25)
  const swing = Number(opts.swing ?? 1.5)
  const outlierMap = new Map((opts.outliers || []).map((x) => [x.index, x]))
  const bars = []
  for (let i = 0; i < count; i += 1) {
    const center = base + slope * i + Math.sin(i * 0.6) * swing
    let high = center + width + Math.sin(i * 0.37) * noise
    let low = center - width - Math.cos(i * 0.31) * noise
    let open = center + Math.sin(i * 0.15) * 0.25
    let close = center + Math.cos(i * 0.17) * 0.25
    const outlier = outlierMap.get(i)
    if (outlier?.highDelta) high += outlier.highDelta
    if (outlier?.lowDelta) low -= outlier.lowDelta
    if (high < low) {
      const t = high
      high = low
      low = t
    }
    bars.push({
      adjusted_time: 1_700_000_000 + i * 60,
      open,
      high,
      low,
      close,
      volume: 100 + i,
    })
  }
  return bars
}

function makeWedgeBars(count) {
  const bars = []
  for (let i = 0; i < count; i += 1) {
    const center = 120 + i * 0.06
    const width = Math.max(0.6, 7 - i * 0.12)
    const high = center + width + Math.sin(i * 0.2) * 0.4
    const low = center - width + Math.cos(i * 0.2) * 0.4
    const close = center + Math.sin(i * 0.5) * Math.max(0.2, width * 0.8)
    bars.push({
      adjusted_time: 1_700_100_000 + i * 60,
      open: center,
      high,
      low,
      close,
      volume: 200 + i,
    })
  }
  return bars
}

function detectWith(settings, bars = makeChannelBars(260)) {
  return detectChannelsInVisibleRange({
    bars,
    visibleStartIndex: 0,
    visibleEndIndex: bars.length - 1,
    timeframe: '1m',
    settings,
  })
}

test('default settings produce extrema channels only', () => {
  const segments = detectWith(undefined)
  assert.ok(Array.isArray(segments))
  assert.ok(segments.length >= 1)
  assert.ok(segments.every((x) => x.method === 'extrema'))
})

test('display switch: only ransac', () => {
  const settings = {
    display: { showExtrema: false, showRansac: true, showRegression: false },
  }
  const segments = detectWith(settings)
  assert.ok(segments.length >= 1)
  assert.ok(segments.every((x) => x.method === 'ransac'))
})

test('display switch: only regression', () => {
  const settings = {
    display: { showExtrema: false, showRansac: false, showRegression: true },
  }
  const segments = detectWith(settings)
  assert.ok(segments.length >= 1)
  assert.ok(segments.every((x) => x.method === 'regression'))
})

test('display switch: all enabled can coexist', () => {
  const settings = {
    display: { showExtrema: true, showRansac: true, showRegression: true },
    common: { maxSegments: 12 },
  }
  const segments = detectWith(settings)
  assert.ok(segments.length >= 1)
  const methods = new Set(segments.map((x) => x.method))
  assert.ok(methods.has('extrema'))
  assert.ok(methods.has('ransac') || methods.has('regression'))
})

test('legacy flat settings are migrated to grouped structure', () => {
  const out = normalizeChannelSettingsV2({
    windowSizeMinute: 180,
    residualAtrFactor: 1.2,
    pivotKMinute: 7,
    slopeTolAtrFactor: 0.2,
    hideAuto: true,
  })
  assert.equal(out.common.windowSizeMinute, 180)
  assert.equal(out.algorithms.ransac.residualAtrFactor, 1.2)
  assert.equal(out.algorithms.ransac.pivotKMinute, 7)
  assert.equal(out.algorithms.extrema.pivotKMinute, 7)
  assert.equal(out.algorithms.ransac.slopeTolAtrFactor, 0.2)
  assert.equal(out.common.hideAuto, true)
  assert.equal(out.display.showExtrema, true)
  assert.equal(out.display.showRansac, false)
  assert.equal(out.display.showRegression, false)
})

test('wedge-like narrowing structure is filtered aggressively', () => {
  const bars = makeWedgeBars(120)
  const segments = detectWith(undefined, bars)
  assert.ok(Array.isArray(segments))
  assert.ok(segments.length <= 2)
  if (segments.length > 0) {
    assert.ok(segments.every((x) => x.insideRatio >= 0.78))
  }
})

test('returns empty on insufficient bars', () => {
  const bars = makeChannelBars(20)
  const segments = detectWith(undefined, bars)
  assert.deepEqual(segments, [])
})
