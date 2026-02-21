export const DEFAULT_CHANNEL_SETTINGS = {
  display: {
    showExtrema: true,
    showRansac: false,
    showRegression: false,
  },
  common: {
    windowSizeMinute: 120,
    windowSizeHour: 160,
    windowSizeDay: 200,
    stepDivisor: 4,
    insideRatioMin: 0.78,
    minTouches: 2,
    nmsIoU: 0.6,
    nmsSlopeFactor: 1.5,
    maxSegments: 6,
    showTopAutoN: 2,
    hideAuto: false,
    liveApply: true,
    showLabels: true,
  },
  algorithms: {
    extrema: {
      // 分钟级图表（1m/5m/15m/30m）使用的 pivot 半窗大小。
      // 例如 K=3 时，某点要在左右各 3 根内相对更高/更低，才会被识别为局部极值。
      pivotKMinute: 3,
      // 小时级图表（1h/2h/4h）使用的 pivot 半窗大小。
      pivotKHour: 5,
      // 日级及以上图表（1d/1w）使用的 pivot 半窗大小。
      pivotKDay: 8,
      // 两个锚点极值之间最小 bar 跨度，防止过近点连线导致噪声通道。
      minPairSpanBars: 12,
      // 两个锚点极值之间最大 bar 跨度，防止跨越过长连接到不相关波段。
      maxPairSpanBars: 120,
      // 第二锚点相对第一锚点的最小位移阈值（ATR 倍数）。
      // 上升通道要求第二低点高于第一低点至少该阈值；下降通道相反。
      secondPointAtrFactor: 0.25,
      // 通道边界校验的突破容差（ATR 倍数），越大越容忍边界附近噪声/假突破。
      breakToleranceAtrFactor: 0.3,
    },
    ransac: {
      pivotKMinute: 3,
      pivotKHour: 5,
      pivotKDay: 8,
      residualAtrFactor: 0.8,
      slopeTolAtrFactor: 0.12,
    },
    regression: {
      residualAtrFactor: 0.8,
    },
  },
}

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

function pick(obj, keys, fallback) {
  const o = toObj(obj)
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(o, key)) return o[key]
  }
  return fallback
}

function readBool(v, fallback) {
  if (typeof v === 'boolean') return v
  if (v === 1 || v === '1') return true
  if (v === 0 || v === '0') return false
  return fallback
}

function timeframeBucket(tf) {
  const s = String(tf || '').trim().toLowerCase()
  if (s.endsWith('d')) return 'day'
  if (s.endsWith('h')) return 'hour'
  return 'minute'
}

function commonWindowSize(bucket, common) {
  if (bucket === 'day') return common.windowSizeDay
  if (bucket === 'hour') return common.windowSizeHour
  return common.windowSizeMinute
}

function pivotByBucket(bucket, algo) {
  // 根据当前周期桶选择对应 pivot 参数，三者只会取其一。
  if (bucket === 'day') return algo.pivotKDay
  if (bucket === 'hour') return algo.pivotKHour
  return algo.pivotKMinute
}

function mergeCommonFromLegacy(target, legacy) {
  target.windowSizeMinute = Math.round(clamp(pick(legacy, ['windowSizeMinute', 'window_size_minute'], target.windowSizeMinute), 40, 800))
  target.windowSizeHour = Math.round(clamp(pick(legacy, ['windowSizeHour', 'window_size_hour'], target.windowSizeHour), 40, 800))
  target.windowSizeDay = Math.round(clamp(pick(legacy, ['windowSizeDay', 'window_size_day'], target.windowSizeDay), 40, 1200))
  target.stepDivisor = clamp(pick(legacy, ['stepDivisor', 'step_divisor'], target.stepDivisor), 2, 10)
  target.insideRatioMin = clamp(pick(legacy, ['insideRatioMin', 'inside_ratio_min'], target.insideRatioMin), 0.5, 0.99)
  target.minTouches = Math.round(clamp(pick(legacy, ['minTouches', 'min_touches'], target.minTouches), 1, 20))
  target.nmsIoU = clamp(pick(legacy, ['nmsIoU', 'nms_iou'], target.nmsIoU), 0.1, 0.95)
  target.nmsSlopeFactor = clamp(pick(legacy, ['nmsSlopeFactor', 'nms_slope_factor'], target.nmsSlopeFactor), 0.5, 10)
  target.maxSegments = Math.round(clamp(pick(legacy, ['maxSegments', 'max_segments'], target.maxSegments), 1, 30))
  target.showTopAutoN = Math.round(clamp(pick(legacy, ['showTopAutoN', 'show_top_auto_n'], target.showTopAutoN), 0, 20))
  target.hideAuto = readBool(pick(legacy, ['hideAuto', 'hide_auto'], target.hideAuto), target.hideAuto)
  target.liveApply = readBool(pick(legacy, ['liveApply', 'live_apply'], target.liveApply), target.liveApply)
  target.showLabels = readBool(pick(legacy, ['showLabels', 'show_labels'], target.showLabels), target.showLabels)
}

export function normalizeChannelSettingsV2(raw) {
  const out = deepClone(DEFAULT_CHANNEL_SETTINGS)
  const source = toObj(raw)
  const hasGrouped = !!(source.display || source.common || source.algorithms)

  const displayRaw = toObj(source.display)
  const commonRaw = toObj(source.common)
  const algRaw = toObj(source.algorithms)
  const extRaw = toObj(algRaw.extrema)
  const ranRaw = toObj(algRaw.ransac)
  const regRaw = toObj(algRaw.regression)

  if (!hasGrouped) {
    mergeCommonFromLegacy(out.common, source)
    const legacyPivotM = pick(source, ['pivotKMinute', 'pivot_k_minute'], out.algorithms.ransac.pivotKMinute)
    const legacyPivotH = pick(source, ['pivotKHour', 'pivot_k_hour'], out.algorithms.ransac.pivotKHour)
    const legacyPivotD = pick(source, ['pivotKDay', 'pivot_k_day'], out.algorithms.ransac.pivotKDay)
    out.algorithms.ransac.pivotKMinute = Math.round(clamp(legacyPivotM, 2, 20))
    out.algorithms.ransac.pivotKHour = Math.round(clamp(legacyPivotH, 2, 24))
    out.algorithms.ransac.pivotKDay = Math.round(clamp(legacyPivotD, 2, 30))
    out.algorithms.extrema.pivotKMinute = out.algorithms.ransac.pivotKMinute
    out.algorithms.extrema.pivotKHour = out.algorithms.ransac.pivotKHour
    out.algorithms.extrema.pivotKDay = out.algorithms.ransac.pivotKDay
    out.algorithms.ransac.residualAtrFactor = clamp(pick(source, ['residualAtrFactor', 'residual_atr_factor'], out.algorithms.ransac.residualAtrFactor), 0.1, 5)
    out.algorithms.ransac.slopeTolAtrFactor = clamp(pick(source, ['slopeTolAtrFactor', 'slope_tol_atr_factor'], out.algorithms.ransac.slopeTolAtrFactor), 0.001, 2)
    out.algorithms.regression.residualAtrFactor = clamp(
      pick(source, ['regressionResidualAtrFactor', 'regression_residual_atr_factor', 'residualAtrFactor', 'residual_atr_factor'], out.algorithms.regression.residualAtrFactor),
      0.1,
      5,
    )
    out.display.showExtrema = readBool(pick(source, ['showExtrema', 'show_extrema', 'useExtremaChannel', 'use_extrema_channel'], out.display.showExtrema), out.display.showExtrema)
    out.display.showRansac = readBool(pick(source, ['showRansac', 'show_ransac', 'useRansacChannel', 'use_ransac_channel'], out.display.showRansac), out.display.showRansac)
    out.display.showRegression = readBool(
      pick(source, ['showRegression', 'show_regression', 'useRegressionChannel', 'use_regression_channel'], out.display.showRegression),
      out.display.showRegression,
    )
    return out
  }

  out.display.showExtrema = readBool(pick(displayRaw, ['showExtrema', 'show_extrema'], out.display.showExtrema), out.display.showExtrema)
  out.display.showRansac = readBool(pick(displayRaw, ['showRansac', 'show_ransac'], out.display.showRansac), out.display.showRansac)
  out.display.showRegression = readBool(pick(displayRaw, ['showRegression', 'show_regression'], out.display.showRegression), out.display.showRegression)

  mergeCommonFromLegacy(out.common, commonRaw)

  out.algorithms.extrema.pivotKMinute = Math.round(clamp(pick(extRaw, ['pivotKMinute', 'pivot_k_minute'], out.algorithms.extrema.pivotKMinute), 2, 20))
  out.algorithms.extrema.pivotKHour = Math.round(clamp(pick(extRaw, ['pivotKHour', 'pivot_k_hour'], out.algorithms.extrema.pivotKHour), 2, 24))
  out.algorithms.extrema.pivotKDay = Math.round(clamp(pick(extRaw, ['pivotKDay', 'pivot_k_day'], out.algorithms.extrema.pivotKDay), 2, 30))
  out.algorithms.extrema.minPairSpanBars = Math.round(clamp(pick(extRaw, ['minPairSpanBars', 'min_pair_span_bars'], out.algorithms.extrema.minPairSpanBars), 2, 400))
  out.algorithms.extrema.maxPairSpanBars = Math.round(clamp(pick(extRaw, ['maxPairSpanBars', 'max_pair_span_bars'], out.algorithms.extrema.maxPairSpanBars), 3, 800))
  if (out.algorithms.extrema.maxPairSpanBars < out.algorithms.extrema.minPairSpanBars) {
    out.algorithms.extrema.maxPairSpanBars = out.algorithms.extrema.minPairSpanBars
  }
  out.algorithms.extrema.secondPointAtrFactor = clamp(
    pick(extRaw, ['secondPointAtrFactor', 'second_point_atr_factor'], out.algorithms.extrema.secondPointAtrFactor),
    0,
    3,
  )
  out.algorithms.extrema.breakToleranceAtrFactor = clamp(
    pick(extRaw, ['breakToleranceAtrFactor', 'break_tolerance_atr_factor'], out.algorithms.extrema.breakToleranceAtrFactor),
    0.01,
    5,
  )

  out.algorithms.ransac.pivotKMinute = Math.round(clamp(pick(ranRaw, ['pivotKMinute', 'pivot_k_minute'], out.algorithms.ransac.pivotKMinute), 2, 20))
  out.algorithms.ransac.pivotKHour = Math.round(clamp(pick(ranRaw, ['pivotKHour', 'pivot_k_hour'], out.algorithms.ransac.pivotKHour), 2, 24))
  out.algorithms.ransac.pivotKDay = Math.round(clamp(pick(ranRaw, ['pivotKDay', 'pivot_k_day'], out.algorithms.ransac.pivotKDay), 2, 30))
  out.algorithms.ransac.residualAtrFactor = clamp(pick(ranRaw, ['residualAtrFactor', 'residual_atr_factor'], out.algorithms.ransac.residualAtrFactor), 0.1, 5)
  out.algorithms.ransac.slopeTolAtrFactor = clamp(pick(ranRaw, ['slopeTolAtrFactor', 'slope_tol_atr_factor'], out.algorithms.ransac.slopeTolAtrFactor), 0.001, 2)

  out.algorithms.regression.residualAtrFactor = clamp(
    pick(regRaw, ['residualAtrFactor', 'residual_atr_factor'], out.algorithms.regression.residualAtrFactor),
    0.1,
    5,
  )

  return out
}

export const normalizeChannelSettings = normalizeChannelSettingsV2

function safeTime(bar) {
  const t = Number(bar?.adjusted_time ?? bar?.time ?? 0)
  return Number.isFinite(t) ? t : 0
}

function quantile(arr, q) {
  if (!Array.isArray(arr) || arr.length === 0) return 0
  const s = arr.slice().sort((a, b) => a - b)
  const pos = clamp((s.length - 1) * q, 0, s.length - 1)
  const lo = Math.floor(pos)
  const hi = Math.ceil(pos)
  if (lo === hi) return s[lo]
  const w = pos - lo
  return s[lo] * (1 - w) + s[hi] * w
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
  const sum = take.reduce((acc, x) => acc + x, 0)
  return sum / Math.max(1, take.length)
}

function olsSlopeIntercept(points) {
  if (!Array.isArray(points) || points.length < 2) return null
  let sx = 0
  let sy = 0
  let sxx = 0
  let sxy = 0
  let n = 0
  for (const p of points) {
    const x = Number(p?.x)
    const y = Number(p?.y)
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue
    sx += x
    sy += y
    sxx += x * x
    sxy += x * y
    n += 1
  }
  if (n < 2) return null
  const den = n * sxx - sx * sx
  if (!Number.isFinite(den) || Math.abs(den) < 1e-9) return null
  const slope = (n * sxy - sx * sy) / den
  const intercept = (sy - slope * sx) / n
  if (!Number.isFinite(slope) || !Number.isFinite(intercept)) return null
  return { slope, intercept }
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
  // 逐根扫描，i 位置在 [i-K, i+K] 范围内比较：
  // - 若 high 不小于邻域所有 high，则记为 pivot high
  // - 若 low  不大于邻域所有 low，则记为 pivot low
  // x 使用全局索引（startIndex + i），便于后续跨窗口统一计算斜率与截距。
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

function rng(seed) {
  let s = (seed >>> 0) || 1
  return () => {
    s ^= s << 13
    s ^= s >>> 17
    s ^= s << 5
    return ((s >>> 0) % 1_000_000) / 1_000_000
  }
}

function ransacFitLine(points, threshold, iterations, seed) {
  if (!Array.isArray(points) || points.length < 2) return null
  const random = rng(seed)
  let best = null
  const n = points.length
  for (let i = 0; i < Math.max(24, iterations); i += 1) {
    const ia = Math.floor(random() * n)
    let ib = Math.floor(random() * n)
    if (ia === ib) ib = (ib + 1) % n
    const a = points[ia]
    const b = points[ib]
    const dx = Number(b.x) - Number(a.x)
    if (!Number.isFinite(dx) || Math.abs(dx) < 1e-9) continue
    const slope = (Number(b.y) - Number(a.y)) / dx
    const intercept = Number(a.y) - slope * Number(a.x)
    if (!Number.isFinite(slope) || !Number.isFinite(intercept)) continue
    const inliers = []
    for (const p of points) {
      const pred = slope * Number(p.x) + intercept
      const residual = Math.abs(Number(p.y) - pred)
      if (Number.isFinite(residual) && residual <= threshold) inliers.push(p)
    }
    if (inliers.length < 2) continue
    const refined = olsSlopeIntercept(inliers)
    if (!refined) continue
    const score = inliers.length / points.length
    if (!best || score > best.score || (score === best.score && inliers.length > best.inliers.length)) {
      best = { ...refined, inliers, score }
    }
  }
  return best
}

function lineInterceptsBySlope(points, slope) {
  const values = []
  for (const p of points) {
    const x = Number(p?.x)
    const y = Number(p?.y)
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue
    values.push(y - slope * x)
  }
  if (!values.length) return null
  return quantile(values, 0.5)
}

function segmentFromRaw(raw) {
  const widthMean = (Math.abs(Number(raw.upperStart) - Number(raw.lowerStart)) + Math.abs(Number(raw.upperEnd) - Number(raw.lowerEnd))) * 0.5
  const tier = raw.score >= 0.92 ? 'A' : raw.score >= 0.84 ? 'B' : 'C'
  return {
    ...raw,
    widthMean,
    qualityTier: tier,
  }
}

function evaluateCandidate({ common, method, bars, startIndex, endIndex, slope, bUp, bLow, atr, slopeDelta, slopeTol, residualAtrFactor }) {
  if (!Number.isFinite(slope) || !Number.isFinite(bUp) || !Number.isFinite(bLow)) return null
  if (bUp <= bLow) return null
  // threshold 是“边界命中/包含”判定容差。
  // 对 extrema 来说这里传入的是 breakToleranceAtrFactor；
  // 对 regression/ransac 则是各自 residualAtrFactor。
  const threshold = Math.max(atr * residualAtrFactor, 1e-6)
  let inside = 0
  let touches = 0
  const len = endIndex - startIndex + 1
  for (let i = 0; i < len; i += 1) {
    const bar = bars[i]
    const x = startIndex + i
    const upper = slope * x + bUp
    const lower = slope * x + bLow
    const high = Number(bar?.high)
    const low = Number(bar?.low)
    if (!Number.isFinite(high) || !Number.isFinite(low)) continue
    if (high <= upper + threshold && low >= lower - threshold) inside += 1
    if (Math.abs(high - upper) <= threshold) touches += 1
    if (Math.abs(low - lower) <= threshold) touches += 1
  }
  const insideRatio = inside / Math.max(1, len)
  const touchCount = touches
  const touchScore = clamp(touchCount / (2 * common.minTouches), 0, 1)
  const parallelScore = clamp(1 - Math.abs(slopeDelta || 0) / Math.max(slopeTol || 1e-9, 1e-9), 0, 1)
  const score = 0.55 * insideRatio + 0.25 * touchScore + 0.2 * parallelScore
  if (touchCount < common.minTouches) return null
  if (insideRatio < common.insideRatioMin) return null

  const first = bars[0]
  const last = bars[bars.length - 1]
  const startTime = safeTime(first)
  const endTime = safeTime(last)
  const baseId = `${method}-${startTime}-${endTime}`

  return segmentFromRaw({
    id: baseId,
    baseId,
    method,
    startTime,
    endTime,
    upperStart: slope * startIndex + bUp,
    upperEnd: slope * endIndex + bUp,
    lowerStart: slope * startIndex + bLow,
    lowerEnd: slope * endIndex + bLow,
    slope,
    slopeDelta: Number.isFinite(slopeDelta) ? slopeDelta : 0,
    insideRatio,
    touchCount,
    score,
    windowStartIndex: startIndex,
    windowEndIndex: endIndex,
    _slopeTol: slopeTol,
  })
}

function regressionChannelCandidate(args) {
  const { common, settings, bars, startIndex, endIndex, atr, slopeTol } = args
  const points = []
  const residuals = []
  for (let i = 0; i < bars.length; i += 1) {
    const close = Number(bars[i]?.close)
    if (!Number.isFinite(close)) continue
    points.push({ x: startIndex + i, y: close })
  }
  const fit = olsSlopeIntercept(points)
  if (!fit) return null
  for (let i = 0; i < bars.length; i += 1) {
    const bar = bars[i]
    const x = startIndex + i
    const center = fit.slope * x + fit.intercept
    const high = Number(bar?.high)
    const low = Number(bar?.low)
    if (!Number.isFinite(high) || !Number.isFinite(low)) continue
    residuals.push(Math.abs(high - center))
    residuals.push(Math.abs(center - low))
  }
  const halfWidth = quantile(residuals, 0.85) + 0.15 * atr * settings.residualAtrFactor
  if (!Number.isFinite(halfWidth) || halfWidth <= 0) return null
  return evaluateCandidate({
    common,
    method: 'regression',
    bars,
    startIndex,
    endIndex,
    slope: fit.slope,
    bUp: fit.intercept + halfWidth,
    bLow: fit.intercept - halfWidth,
    atr,
    slopeDelta: 0,
    slopeTol,
    residualAtrFactor: settings.residualAtrFactor,
  })
}

function ransacChannelCandidate(args) {
  const { common, settings, bars, startIndex, endIndex, atr, pivotK, slopeTol, seed } = args
  const pivots = findPivots(bars, pivotK, startIndex)
  if (pivots.highs.length < 2 || pivots.lows.length < 2) return null
  const threshold = Math.max(atr * settings.residualAtrFactor, 1e-6)
  const upFit = ransacFitLine(pivots.highs, threshold, 72, seed + 11)
  const lowFit = ransacFitLine(pivots.lows, threshold, 72, seed + 29)
  if (!upFit || !lowFit) return null
  const slopeDelta = upFit.slope - lowFit.slope
  if (Math.abs(slopeDelta) > slopeTol) return null
  const slope = (upFit.slope + lowFit.slope) / 2
  const bUp = lineInterceptsBySlope(upFit.inliers, slope)
  const bLow = lineInterceptsBySlope(lowFit.inliers, slope)
  if (!Number.isFinite(bUp) || !Number.isFinite(bLow)) return null
  return evaluateCandidate({
    common,
    method: 'ransac',
    bars,
    startIndex,
    endIndex,
    slope,
    bUp,
    bLow,
    atr,
    slopeDelta,
    slopeTol,
    residualAtrFactor: settings.residualAtrFactor,
  })
}

function pickExtremaPair(points, direction, atr, settings, windowLimit) {
  if (!Array.isArray(points) || points.length < 2) return null
  // 配对约束：
  // 1) 跨度必须在 [minPairSpanBars, maxPairSpanBars]
  // 2) 第二点相对第一点的方向位移需超过 ATR 阈值
  //    - up:   dy >= ATR * secondPointAtrFactor（第二低点更高）
  //    - down: -dy >= ATR * secondPointAtrFactor（第二高点更低）
  // 评分目前偏向“更长跨度 + 更大位移”的锚点组合。
  const minSpan = Math.max(2, Math.floor(settings.minPairSpanBars))
  const maxSpan = Math.max(minSpan, Math.min(Math.floor(settings.maxPairSpanBars), windowLimit))
  let best = null
  for (let i = 0; i < points.length - 1; i += 1) {
    for (let j = i + 1; j < points.length; j += 1) {
      const a = points[i]
      const b = points[j]
      const span = Number(b.x) - Number(a.x)
      if (!Number.isFinite(span) || span < minSpan || span > maxSpan) continue
      const dy = Number(b.y) - Number(a.y)
      if (!Number.isFinite(dy)) continue
      if (direction === 'up' && dy < atr * settings.secondPointAtrFactor) continue
      if (direction === 'down' && -dy < atr * settings.secondPointAtrFactor) continue
      const score = span + Math.abs(dy) * 0.2
      if (!best || score > best.score) best = { a, b, score }
    }
  }
  return best
}

function extremaChannelCandidate(args) {
  const { common, settings, bars, startIndex, endIndex, atr, pivotK, slopeTol } = args
  // 第一步：提取窗口内局部极值点（高点/低点）。
  const pivots = findPivots(bars, pivotK, startIndex)
  const len = endIndex - startIndex + 1
  const out = []
  console.log('[extrema] pivots:', { 'startIndex':startIndex, 'highs': pivots.highs, 'lows': pivots.lows })

  // 第二步（上升通道）：在 lows 中选两点锚定支撑线斜率。
  const upPair = pickExtremaPair(pivots.lows, 'up', atr, settings, len - 1)
  if (upPair) {
    const line = twoPointLine(upPair.a, upPair.b)
    if (line) {
      // 第三步：固定斜率后，用整窗数据推导“最小包络”截距：
      // - 上轨 bUp 取所有 high 对应截距最大值（保证上轨不压住 K 线）
      // - 下轨 bLow 取所有 low  对应截距最小值（保证下轨不抬高 K 线）
      // 同时确保下轨不高于锚点线截距（保留“低点锚定”的语义）。
      let bLow = Number.POSITIVE_INFINITY
      let bUp = Number.NEGATIVE_INFINITY
      for (let i = 0; i < bars.length; i += 1) {
        const x = startIndex + i
        const h = Number(bars[i]?.high)
        const l = Number(bars[i]?.low)
        if (!Number.isFinite(h)) continue
        bUp = Math.max(bUp, h - line.slope * x)
        if (Number.isFinite(l)) bLow = Math.min(bLow, l - line.slope * x)
      }
      if (Number.isFinite(bUp) && Number.isFinite(bLow)) {
        bLow = Math.min(bLow, line.intercept)
        // 第四步：按 inside ratio / touches / parallel score 进行质量打分与过滤。
        const cand = evaluateCandidate({
          common,
          method: 'extrema',
          bars,
          startIndex,
          endIndex,
          slope: line.slope,
          bUp,
          bLow,
          atr,
          slopeDelta: 0,
          slopeTol,
          residualAtrFactor: settings.breakToleranceAtrFactor,
        })
        if (cand) out.push(cand)
      }
    }
  }

  // 第二步（下降通道）：在 highs 中选两点锚定阻力线斜率。
  const downPair = pickExtremaPair(pivots.highs, 'down', atr, settings, len - 1)
  if (downPair) {
    const line = twoPointLine(downPair.a, downPair.b)
    if (line) {
      // 固定斜率后同样做最小包络截距推导。
      let bUp = Number.NEGATIVE_INFINITY
      let bLow = Number.POSITIVE_INFINITY
      for (let i = 0; i < bars.length; i += 1) {
        const x = startIndex + i
        const h = Number(bars[i]?.high)
        const l = Number(bars[i]?.low)
        if (Number.isFinite(h)) bUp = Math.max(bUp, h - line.slope * x)
        if (!Number.isFinite(l)) continue
        bLow = Math.min(bLow, l - line.slope * x)
      }
      if (Number.isFinite(bLow) && Number.isFinite(bUp)) {
        bUp = Math.max(bUp, line.intercept)
        const cand = evaluateCandidate({
          common,
          method: 'extrema',
          bars,
          startIndex,
          endIndex,
          slope: line.slope,
          bUp,
          bLow,
          atr,
          slopeDelta: 0,
          slopeTol,
          residualAtrFactor: settings.breakToleranceAtrFactor,
        })
        if (cand) out.push(cand)
      }
    }
  }

  if (!out.length) return null
  // 同一窗口可能同时产生上升/下降候选，这里只保留得分最高者。
  return out.sort((a, b) => Number(b.score) - Number(a.score))[0]
}

function intervalIoU(a0, a1, b0, b1) {
  const left = Math.max(a0, b0)
  const right = Math.min(a1, b1)
  const inter = Math.max(0, right - left)
  const union = Math.max(a1, b1) - Math.min(a0, b0)
  if (union <= 0) return 0
  return inter / union
}

function nmsSegments(candidates, common) {
  const sorted = candidates.slice().sort((a, b) => (Number(b.score) - Number(a.score)) || (Number(b.insideRatio) - Number(a.insideRatio)))
  const out = []
  for (const seg of sorted) {
    let skip = false
    for (const kept of out) {
      const iou = intervalIoU(seg.windowStartIndex, seg.windowEndIndex, kept.windowStartIndex, kept.windowEndIndex)
      const slopeClose = Math.abs(seg.slope - kept.slope) <= Math.max(seg._slopeTol, kept._slopeTol) * common.nmsSlopeFactor
      if (iou > common.nmsIoU && slopeClose) {
        skip = true
        break
      }
    }
    if (!skip) out.push(seg)
    if (out.length >= common.maxSegments) break
  }
  return out
}

function normalizeOutput(segments) {
  return segments.map((x) => ({
    ...x,
    status: 'auto',
    manual: false,
    locked: false,
    hidden: false,
    recalcVersion: 0,
  }))
}

/**
 * 在当前可见区内检测通道候选（极值 / RANSAC / 回归），并返回最终自动结果。
 *
 * 设计目标：
 * 1) 只在可见范围计算，避免全量历史带来的噪声和开销；
 * 2) 通过滑窗+多算法并行生成候选；
 * 3) 先在“算法内”做 NMS 去重，再跨算法合并后按质量分数排序；
 * 4) 输出统一补充自动状态字段，供前端“自动结果 + 人工决策”流程消费。
 *
 * @param {object} input
 * @param {Array<object>} input.bars 全量K线（至少包含 high/low/close/time）
 * @param {number} input.visibleStartIndex 当前可见区起始索引（允许乱序，内部会纠正）
 * @param {number} input.visibleEndIndex 当前可见区结束索引（允许乱序，内部会纠正）
 * @param {string} input.timeframe 周期字符串（如 1m/5m/1h/1d）
 * @param {object} input.settings 通道参数（支持新旧格式，内部归一化）
 * @returns {Array<object>} 排序后的通道结果（已注入 status/manual/locked/hidden/recalcVersion）
 */
export function detectChannelsInVisibleRange(input) {
  // 1) 基础输入兜底：bars 不合法直接视为空。
  const bars = Array.isArray(input?.bars) ? input.bars : []
  // 数据过少时不做检测：避免统计指标和斜率估计不稳定。
  if (bars.length < 40) return []

  // 2) 参数归一化：兼容 legacy 字段并落地默认值。
  const settings = normalizeChannelSettingsV2(input?.settings)
  const display = settings.display
  const common = settings.common
  // 三类算法都关闭时直接返回空结果。
  if (!display.showExtrema && !display.showRansac && !display.showRegression) return []

  // 3) 可见区索引规范化：
  // - 支持 start/end 颠倒；
  // - 强制截断到 [0, bars.length-1]；
  // - 可见区过短不检测。
  const rawStart = Number(input?.visibleStartIndex ?? 0)
  const rawEnd = Number(input?.visibleEndIndex ?? (bars.length - 1))
  const visibleStart = clamp(Math.floor(Math.min(rawStart, rawEnd)), 0, bars.length - 1)
  const visibleEnd = clamp(Math.ceil(Math.max(rawStart, rawEnd)), 0, bars.length - 1)
  if (visibleEnd - visibleStart + 1 < 40) return []

  // 4) 根据周期桶选择窗口和 pivot 参数，确定滑窗步长。
  // step 越小越密集，结果更多但计算更重；设置下限 8 防止过密遍历。
  const bucket = timeframeBucket(input?.timeframe || '1m')
  const windowSize = commonWindowSize(bucket, common)
  const step = Math.max(8, Math.floor(windowSize / Math.max(1, common.stepDivisor)))
  // slopeBase 用于把 ATR 量纲归一到斜率容差，避免不同窗口大小下容差失真。
  const slopeBase = Math.max(1, windowSize)
  // 按算法分桶收集候选，后续各自 NMS。
  const byMethod = {
    extrema: [],
    ransac: [],
    regression: [],
  }

  // 5) 在可见区内按滑窗扫描。
  for (let winStart = visibleStart; winStart <= visibleEnd; winStart += step) {
    const winEnd = Math.min(visibleEnd, winStart + windowSize - 1)
    const winBars = bars.slice(winStart, winEnd + 1)
    // 过短窗口跳过：窗口太小时触点/包络统计不可靠。
    if (winBars.length < Math.max(40, Math.floor(windowSize * 0.45))) continue

    // 计算该窗口局部波动（ATR），作为多处容差基准。
    const atr = calcAtr(winBars, 14)
    // RANSAC/回归共享斜率容差：ATR 越大，允许的斜率差适当放宽。
    const ransacSlopeTol = Math.max(1e-6, (settings.algorithms.ransac.slopeTolAtrFactor * Math.max(atr, 1e-6)) / slopeBase)
    // 稳定随机种子：保证同一窗口在同一数据下结果可复现。
    const seed = safeTime(winBars[0]) + winStart * 31

    // 5.1 回归通道候选
    if (display.showRegression) {
      const reg = regressionChannelCandidate({
        common,
        settings: settings.algorithms.regression,
        bars: winBars,
        startIndex: winStart,
        endIndex: winEnd,
        atr,
        slopeTol: ransacSlopeTol,
      })
      if (reg) byMethod.regression.push(reg)
    }

    // 5.2 RANSAC 通道候选
    if (display.showRansac) {
      const ran = ransacChannelCandidate({
        common,
        settings: settings.algorithms.ransac,
        bars: winBars,
        startIndex: winStart,
        endIndex: winEnd,
        atr,
        pivotK: pivotByBucket(bucket, settings.algorithms.ransac),
        slopeTol: ransacSlopeTol,
        seed,
      })
      if (ran) byMethod.ransac.push(ran)
    }

    // 5.3 极值通道候选
    if (display.showExtrema) {
      const ext = extremaChannelCandidate({
        common,
        settings: settings.algorithms.extrema,
        bars: winBars,
        startIndex: winStart,
        endIndex: winEnd,
        atr,
        pivotK: pivotByBucket(bucket, settings.algorithms.extrema),
        slopeTol: ransacSlopeTol,
      })
      if (ext) byMethod.extrema.push(ext)
    }

    // 覆盖到可见区尾部时提前结束，避免无意义迭代。
    if (winEnd === visibleEnd) break
  }

  // 6) 先做“算法内”NMS，去除同类高度重叠候选，保留高质量代表。
  const merged = []
  if (display.showExtrema) merged.push(...nmsSegments(byMethod.extrema, common))
  if (display.showRansac) merged.push(...nmsSegments(byMethod.ransac, common))
  if (display.showRegression) merged.push(...nmsSegments(byMethod.regression, common))

  // 7) 跨算法统一排序：
  // 主排序 score，次排序 insideRatio，最后截断到 maxSegments。
  const ranked = merged
    .slice()
    .sort((a, b) => (Number(b.score) - Number(a.score)) || (Number(b.insideRatio) - Number(a.insideRatio)))
    .slice(0, common.maxSegments)

  // 8) 输出归一化：补充自动通道状态字段，便于 UI 与人工决策链路直接消费。
  return normalizeOutput(ranked)
}
