function clamp(v, min, max) {
  const n = Number(v)
  if (!Number.isFinite(n)) return min
  if (n < min) return min
  if (n > max) return max
  return n
}

function toObj(v) {
  return v && typeof v === 'object' ? v : {}
}

function deepClone(v) {
  return JSON.parse(JSON.stringify(v))
}

function timeframeBucket(tf) {
  const s = String(tf || '').trim().toLowerCase()
  if (s.endsWith('d')) return 'day'
  if (s.endsWith('h')) return 'hour'
  return 'minute'
}

function safeTime(bar) {
  const t = Number(bar?.data_time ?? bar?.adjusted_time ?? bar?.time ?? 0)
  return Number.isFinite(t) ? t : 0
}

function calcAtr(bars, period = 14) {
  if (!Array.isArray(bars) || bars.length < 2) return 0
  const trs = []
  for (let i = 0; i < bars.length; i += 1) {
    const high = Number(bars[i]?.high)
    const low = Number(bars[i]?.low)
    if (!Number.isFinite(high) || !Number.isFinite(low)) continue
    if (i === 0) {
      trs.push(high - low)
      continue
    }
    const prevClose = Number(bars[i - 1]?.close)
    if (!Number.isFinite(prevClose)) {
      trs.push(high - low)
      continue
    }
    trs.push(Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose)))
  }
  if (!trs.length) return 0
  const take = trs.slice(Math.max(0, trs.length - period))
  return take.reduce((acc, x) => acc + x, 0) / Math.max(1, take.length)
}

function twoPointLine(a, b) {
  const x1 = Number(a?.x)
  const y1 = Number(a?.y)
  const x2 = Number(b?.x)
  const y2 = Number(b?.y)
  if (![x1, y1, x2, y2].every(Number.isFinite)) return null
  const dx = x2 - x1
  if (Math.abs(dx) < 1e-9) return null
  const slope = (y2 - y1) / dx
  const intercept = y1 - slope * x1
  if (!Number.isFinite(slope) || !Number.isFinite(intercept)) return null
  return { slope, intercept }
}

function findPivots(bars, pivotK, startIndex) {
  const highs = []
  const lows = []
  if (!Array.isArray(bars) || bars.length < 2 * pivotK + 1) return { highs, lows }
  for (let i = pivotK; i < bars.length - pivotK; i += 1) {
    const h = Number(bars[i]?.high)
    const l = Number(bars[i]?.low)
    if (!Number.isFinite(h) || !Number.isFinite(l)) continue
    let isHigh = true
    let isLow = true
    for (let j = i - pivotK; j <= i + pivotK; j += 1) {
      if (j === i) continue
      const hj = Number(bars[j]?.high)
      const lj = Number(bars[j]?.low)
      if (!Number.isFinite(hj) || !Number.isFinite(lj)) {
        isHigh = false
        isLow = false
        break
      }
      if (hj > h) isHigh = false
      if (lj < l) isLow = false
      if (!isHigh && !isLow) break
    }
    if (isHigh) highs.push({ x: startIndex + i, y: h })
    if (isLow) lows.push({ x: startIndex + i, y: l })
  }
  return { highs, lows }
}

export const DEFAULT_REVERSAL_SETTINGS = {
  enabled: true,
  midTrendMinBars: 50,
  midTrendMaxBars: 2000,
  pivotKMinute: 3,
  pivotKHour: 5,
  pivotKDay: 8,
  lineToleranceAtrFactor: 1.0,
  breakThresholdPct: 3.0,
  minSwingAmplitudeAtr: 1.0,
  confirmOnClose: true,
  showLabels: true,
}

export function normalizeReversalSettings(raw) {
  const out = deepClone(DEFAULT_REVERSAL_SETTINGS)
  const src = toObj(raw)
  if (Object.prototype.hasOwnProperty.call(src, 'enabled')) {
    out.enabled = src.enabled === true || src.enabled === 1 || src.enabled === '1'
  }
  out.midTrendMinBars = Math.round(clamp(src.midTrendMinBars, 20, 500))
  out.midTrendMaxBars = Math.round(clamp(src.midTrendMaxBars, out.midTrendMinBars, 200000))
  out.pivotKMinute = Math.round(clamp(src.pivotKMinute, 2, 20))
  out.pivotKHour = Math.round(clamp(src.pivotKHour, 2, 24))
  out.pivotKDay = Math.round(clamp(src.pivotKDay, 2, 30))
  out.lineToleranceAtrFactor = clamp(src.lineToleranceAtrFactor, 0.05, 5)
  out.breakThresholdPct = clamp(src.breakThresholdPct, 0.1, 20)
  out.minSwingAmplitudeAtr = clamp(src.minSwingAmplitudeAtr, 0, 10)
  out.confirmOnClose = src.confirmOnClose !== false
  out.showLabels = src.showLabels !== false
  return out
}

function pickPivotK(bucket, settings) {
  if (bucket === 'day') return settings.pivotKDay
  if (bucket === 'hour') return settings.pivotKHour
  return settings.pivotKMinute
}

function scoreTrendCandidate(line, bars, startIndex, endIndex, side, tol, atr) {
  let touches = 0
  let broken = false
  let meanErr = 0
  let nErr = 0
  const minSwing = atr * 0.1
  for (let i = 0; i <= endIndex - startIndex; i += 1) {
    const bar = bars[i]
    const x = startIndex + i
    const yLine = line.slope * x + line.intercept
    const high = Number(bar?.high)
    const low = Number(bar?.low)
    const close = Number(bar?.close)
    if (!Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) continue
    if (side === 'up') {
      if (close < yLine - tol) broken = true
      const err = Math.abs(low - yLine)
      if (err <= Math.max(tol, minSwing)) touches += 1
      meanErr += err
      nErr += 1
    } else {
      if (close > yLine + tol) broken = true
      const err = Math.abs(high - yLine)
      if (err <= Math.max(tol, minSwing)) touches += 1
      meanErr += err
      nErr += 1
    }
  }
  meanErr = nErr > 0 ? meanErr / nErr : 1e9
  const span = Math.max(1, endIndex - startIndex)
  const score = touches * 2 + span * 0.02 - meanErr * 0.1 - (broken ? 2 : 0)
  return { touches, broken, meanErr, score }
}

function bestTrendLineFromPivots({ pivots, bars, startIndex, endIndex, minBars, maxBars, side, atr, tol }) {
  const points = side === 'up' ? pivots.lows : pivots.highs
  if (!Array.isArray(points) || points.length < 2) return null
  // 中趋势 50-200 是“趋势窗口”定义，不应当强制等同于锚点间隔。
  // 锚点允许更短间距，否则在真实走势中会频繁找不到线。
  const anchorMinSpan = Math.max(5, Math.floor(minBars * 0.25))
  const anchorMaxSpan = Number.isFinite(maxBars) && maxBars > 0
    ? Math.max(anchorMinSpan, Math.floor(maxBars))
    : Number.POSITIVE_INFINITY
  let best = null
  for (let i = 0; i < points.length - 1; i += 1) {
    for (let j = i + 1; j < points.length; j += 1) {
      const a = points[i]
      const b = points[j]
      const span = Number(b.x) - Number(a.x)
      if (!Number.isFinite(span) || span < anchorMinSpan || span > anchorMaxSpan) continue
      const line = twoPointLine(a, b)
      if (!line) continue
      const m = scoreTrendCandidate(line, bars, startIndex, endIndex, side, tol, atr)
      if (!best || m.score > best.score) {
        best = {
          id: `mid-${side}-${safeTime(bars[0])}-${a.x}-${b.x}`,
          side,
          startIndex: Number(a.x),
          endIndex: Number(b.x),
          startPrice: Number(a.y),
          endPrice: Number(b.y),
          slope: line.slope,
          intercept: line.intercept,
          touches: m.touches,
          meanErr: m.meanErr,
          score: m.score,
        }
      }
    }
  }
  return best
}

function fallbackMidTrendLine({ bars, startIndex, endIndex, side }) {
  const len = endIndex - startIndex + 1
  if (len < 20) return null
  const mid = startIndex + Math.floor(len / 2)
  let pA = null
  let pB = null
  if (side === 'up') {
    let min1 = Number.POSITIVE_INFINITY
    let idx1 = -1
    let min2 = Number.POSITIVE_INFINITY
    let idx2 = -1
    for (let i = startIndex; i <= mid; i += 1) {
      const v = Number(bars[i]?.low)
      if (Number.isFinite(v) && v < min1) {
        min1 = v
        idx1 = i
      }
    }
    for (let i = Math.max(mid + 1, startIndex + 1); i <= endIndex; i += 1) {
      const v = Number(bars[i]?.low)
      if (Number.isFinite(v) && v < min2) {
        min2 = v
        idx2 = i
      }
    }
    if (idx1 >= 0 && idx2 > idx1) {
      pA = { x: idx1, y: min1 }
      pB = { x: idx2, y: min2 }
    }
  } else {
    let max1 = Number.NEGATIVE_INFINITY
    let idx1 = -1
    let max2 = Number.NEGATIVE_INFINITY
    let idx2 = -1
    for (let i = startIndex; i <= mid; i += 1) {
      const v = Number(bars[i]?.high)
      if (Number.isFinite(v) && v > max1) {
        max1 = v
        idx1 = i
      }
    }
    for (let i = Math.max(mid + 1, startIndex + 1); i <= endIndex; i += 1) {
      const v = Number(bars[i]?.high)
      if (Number.isFinite(v) && v > max2) {
        max2 = v
        idx2 = i
      }
    }
    if (idx1 >= 0 && idx2 > idx1) {
      pA = { x: idx1, y: max1 }
      pB = { x: idx2, y: max2 }
    }
  }
  if (!pA || !pB) return null
  const line = twoPointLine(pA, pB)
  if (!line) return null
  return {
    id: `mid-fallback-${side}-${safeTime(bars[startIndex])}-${pA.x}-${pB.x}`,
    side,
    startIndex: Number(pA.x),
    endIndex: Number(pB.x),
    startPrice: Number(pA.y),
    endPrice: Number(pB.y),
    slope: Number(line.slope),
    intercept: Number(line.intercept),
    touches: 0,
    meanErr: 0,
    score: -999,
  }
}

function findFirstBreak(barSlice, startIndex, line, side, thresholdFn) {
  for (let i = 0; i < barSlice.length; i += 1) {
    const bar = barSlice[i]
    const close = Number(bar?.close)
    if (!Number.isFinite(close)) continue
    const x = startIndex + i
    const yLine = line.slope * x + line.intercept
    const th = thresholdFn(close)
    if (side === 'up' && close < yLine - th) return { idx: x, time: safeTime(bar), price: close, yLine }
    if (side === 'down' && close > yLine + th) return { idx: x, time: safeTime(bar), price: close, yLine }
  }
  return null
}

function findLastPivotBefore(points, idx) {
  let out = null
  for (const p of points || []) {
    if (Number(p.x) < idx) out = p
    else break
  }
  return out
}

function findFirstPivotAfter(points, idx) {
  for (const p of points || []) {
    if (Number(p.x) > idx) return p
  }
  return null
}

function detect123ForLine({ line, pivots, bars, startIndex, endIndex, atr, settings }) {
  const barSlice = bars.slice(startIndex, endIndex + 1)
  const thresholdFn = (close) => Math.max(atr * settings.lineToleranceAtrFactor, Math.abs(close) * (settings.breakThresholdPct / 100))
  if (line.side === 'up') {
    const c1 = findFirstBreak(barSlice, startIndex, line, 'up', thresholdFn)
    if (!c1) return null
    const prevHigh = findLastPivotBefore(pivots.highs, c1.idx)
    const p2 = findFirstPivotAfter(pivots.highs, c1.idx)
    const swingOk = !!(p2 && Math.abs(Number(p2.y) - Number(c1.price || 0)) >= atr * settings.minSwingAmplitudeAtr)
    const c2Met = !!(prevHigh && p2 && Number(p2.y) <= Number(prevHigh.y) && swingOk)
    const keyLow = findLastPivotBefore(pivots.lows, p2 ? p2.x : c1.idx)
    let c3 = null
    if (c2Met && keyLow) {
      for (let i = Math.max(0, (p2 ? p2.x : c1.idx) - startIndex + 1); i < barSlice.length; i += 1) {
        const close = Number(barSlice[i]?.close)
        if (!Number.isFinite(close)) continue
        const th = thresholdFn(close)
        if (close < Number(keyLow.y) - th) {
          const idx = startIndex + i
          c3 = { idx, time: safeTime(barSlice[i]), price: close }
          break
        }
      }
    }
    const confirmed = !!(c1 && c2Met && c3)
    let invalidated = false
    if (confirmed && p2) {
      for (let i = Math.max(0, c3.idx - startIndex + 1); i < barSlice.length; i += 1) {
        const close = Number(barSlice[i]?.close)
        if (Number.isFinite(close) && close > Number(p2.y)) {
          invalidated = true
          break
        }
      }
    }
    return {
      id: `${line.id}-bear-123`,
      lineId: line.id,
      direction: 'bearish_reversal',
      p1: c1 ? { index: c1.idx, time: c1.time, price: c1.price } : null,
      p2: p2 ? { index: Number(p2.x), time: safeTime(bars[Number(p2.x)]), price: Number(p2.y) } : null,
      p3: c3 ? { index: c3.idx, time: c3.time, price: c3.price } : null,
      conditions: {
        c1: { met: !!c1, at: c1 ? c1.idx : -1 },
        c2: { met: c2Met, at: p2 ? Number(p2.x) : -1 },
        c3: { met: !!c3, at: c3 ? c3.idx : -1 },
      },
      confirmed,
      invalidated,
      score: line.score + (confirmed ? 5 : 0),
      label: '1-2-3 反转(空)',
    }
  }

  const c1 = findFirstBreak(barSlice, startIndex, line, 'down', thresholdFn)
  if (!c1) return null
  const prevLow = findLastPivotBefore(pivots.lows, c1.idx)
  const p2 = findFirstPivotAfter(pivots.lows, c1.idx)
  const swingOk = !!(p2 && Math.abs(Number(p2.y) - Number(c1.price || 0)) >= atr * settings.minSwingAmplitudeAtr)
  const c2Met = !!(prevLow && p2 && Number(p2.y) >= Number(prevLow.y) && swingOk)
  const keyHigh = findLastPivotBefore(pivots.highs, p2 ? p2.x : c1.idx)
  let c3 = null
  if (c2Met && keyHigh) {
    for (let i = Math.max(0, (p2 ? p2.x : c1.idx) - startIndex + 1); i < barSlice.length; i += 1) {
      const close = Number(barSlice[i]?.close)
      if (!Number.isFinite(close)) continue
      const th = thresholdFn(close)
      if (close > Number(keyHigh.y) + th) {
        const idx = startIndex + i
        c3 = { idx, time: safeTime(barSlice[i]), price: close }
        break
      }
    }
  }
  const confirmed = !!(c1 && c2Met && c3)
  let invalidated = false
  if (confirmed && p2) {
    for (let i = Math.max(0, c3.idx - startIndex + 1); i < barSlice.length; i += 1) {
      const close = Number(barSlice[i]?.close)
      if (Number.isFinite(close) && close < Number(p2.y)) {
        invalidated = true
        break
      }
    }
  }
  return {
    id: `${line.id}-bull-123`,
    lineId: line.id,
    direction: 'bullish_reversal',
    p1: c1 ? { index: c1.idx, time: c1.time, price: c1.price } : null,
    p2: p2 ? { index: Number(p2.x), time: safeTime(bars[Number(p2.x)]), price: Number(p2.y) } : null,
    p3: c3 ? { index: c3.idx, time: c3.time, price: c3.price } : null,
    conditions: {
      c1: { met: !!c1, at: c1 ? c1.idx : -1 },
      c2: { met: c2Met, at: p2 ? Number(p2.x) : -1 },
      c3: { met: !!c3, at: c3 ? c3.idx : -1 },
    },
    confirmed,
    invalidated,
    score: line.score + (confirmed ? 5 : 0),
    label: '1-2-3 反转(多)',
  }
}

export function detectReversalsInVisibleRange(input) {
  const bars = Array.isArray(input?.bars) ? input.bars : []
  if (bars.length < 60) {
    return {
      lines: [],
      events: [],
      debug: ['当前为何无线: K线总数不足(需要>=60)', `bars_total=${bars.length}`],
    }
  }
  const settings = normalizeReversalSettings(input?.settings)
  if (!settings.enabled) {
    return {
      lines: [],
      events: [],
      debug: ['当前为何无线: 功能未启用(enabled=false)'],
    }
  }
  const rawStart = Number(input?.visibleStartIndex ?? 0)
  const rawEnd = Number(input?.visibleEndIndex ?? bars.length - 1)
  const start = clamp(Math.floor(Math.min(rawStart, rawEnd)), 0, bars.length - 1)
  const end = clamp(Math.ceil(Math.max(rawStart, rawEnd)), 0, bars.length - 1)
  const debug = []
  const visibleLen = end - start + 1
  debug.push(`visible_range=[${start},${end}] len=${visibleLen}`)
  const effectiveMaxBars = Math.max(
    settings.midTrendMinBars,
    visibleLen,
    Number.isFinite(settings.midTrendMaxBars) ? settings.midTrendMaxBars : visibleLen,
  )
  debug.push(`mid_window=${settings.midTrendMinBars}-${effectiveMaxBars}`)
  if (visibleLen < settings.midTrendMinBars) {
    return {
      lines: [],
      events: [],
      debug: [
        ...debug,
        `当前为何无线: 可视区太短(${visibleLen}<${settings.midTrendMinBars})`,
      ],
    }
  }
  const bucket = timeframeBucket(input?.timeframe || '1m')
  const pivotK = pickPivotK(bucket, settings)
  const winBars = bars.slice(start, end + 1)
  const atr = Math.max(1e-6, calcAtr(winBars, 14))
  const pivots = findPivots(winBars, pivotK, start)
  debug.push(`pivotK=${pivotK}`)
  debug.push(`pivot_highs=${pivots.highs.length}`)
  debug.push(`pivot_lows=${pivots.lows.length}`)
  debug.push(`atr=${Number(atr).toFixed(4)}`)
  const tol = atr * settings.lineToleranceAtrFactor

  const upLine = bestTrendLineFromPivots({
    pivots,
    bars: winBars,
    startIndex: start,
    endIndex: end,
    minBars: settings.midTrendMinBars,
    maxBars: effectiveMaxBars,
    side: 'up',
    atr,
    tol,
  })
  const downLine = bestTrendLineFromPivots({
    pivots,
    bars: winBars,
    startIndex: start,
    endIndex: end,
    minBars: settings.midTrendMinBars,
    maxBars: effectiveMaxBars,
    side: 'down',
    atr,
    tol,
  })
  if (!upLine) debug.push('up_line: none(pivot/score)')
  else debug.push(`up_line: ok score=${Number(upLine.score || 0).toFixed(3)} touches=${Number(upLine.touches || 0)}`)
  if (!downLine) debug.push('down_line: none(pivot/score)')
  else debug.push(`down_line: ok score=${Number(downLine.score || 0).toFixed(3)} touches=${Number(downLine.touches || 0)}`)
  const closeStart = Number(winBars[0]?.close)
  const closeEnd = Number(winBars[winBars.length - 1]?.close)
  const dominantSide = Number.isFinite(closeStart) && Number.isFinite(closeEnd) && closeEnd >= closeStart ? 'up' : 'down'
  let lines = [upLine, downLine].filter(Boolean)
  if (!lines.length) {
    const fb = fallbackMidTrendLine({
      bars,
      startIndex: start,
      endIndex: end,
      side: dominantSide,
    })
    if (fb) {
      lines = [fb]
      debug.push(`fallback_line: ${dominantSide}`)
    } else {
      debug.push('fallback_line: none')
    }
  }
  if (!lines.length) {
    debug.push('当前为何无线: 未找到有效中趋势线(含fallback)')
  } else {
    debug.push(`mid_lines=${lines.length}`)
  }
  const events = lines
    .map((line) => detect123ForLine({ line, pivots, bars, startIndex: start, endIndex: end, atr, settings }))
    .filter(Boolean)
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
  if (!events.length) {
    debug.push('event: none(C1/C2/C3 not all met)')
  } else {
    debug.push(`event: ${events.length}`)
  }
  return { lines, events, debug }
}
