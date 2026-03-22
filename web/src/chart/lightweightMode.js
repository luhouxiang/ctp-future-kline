export function buildInitialVisibleLogicalRange(totalBars, minWindow = 120, defaultWindow = 240, rightPad = 5) {
  const total = Number(totalBars || 0)
  if (!Number.isFinite(total) || total <= 0) return null
  const size = Math.min(Math.max(minWindow, defaultWindow), total)
  return {
    from: Math.max(0, total - size),
    to: Math.max(size, total + rightPad),
  }
}

export function shouldRenderReversal(settings) {
  return !!settings?.enabled
}
