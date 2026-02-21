function normalizeUnix(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.floor(value)
  if (typeof value === 'string') {
    const ts = Date.parse(value)
    if (!Number.isNaN(ts)) return Math.floor(ts / 1000)
  }
  if (value && typeof value === 'object' && 'year' in value) {
    return Math.floor(Date.UTC(value.year, value.month - 1, value.day, 0, 0, 0) / 1000)
  }
  return null
}

export function mapperToScreen(mapper, point) {
  const x = mapper.timeToX(point.time)
  const y = point.price === undefined || point.price === null ? null : mapper.priceToY(point.price)
  return { x, y }
}

export function mapperFromScreen(mapper, x, y, withPrice = true) {
  const rawTime = mapper.xToTime(x)
  const time = normalizeUnix(rawTime)
  const rawPrice = withPrice ? mapper.yToPrice(y) : null
  const price = withPrice && Number.isFinite(rawPrice) ? Math.max(0, rawPrice) : rawPrice
  if (time === null || (withPrice && (price === null || !Number.isFinite(price)))) {
    return null
  }
  return withPrice ? { time, price } : { time }
}

export function normalizeTimeValue(value) {
  return normalizeUnix(value)
}
