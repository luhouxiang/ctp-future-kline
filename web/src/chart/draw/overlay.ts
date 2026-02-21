import { DRAW_TYPES } from './types'
import { mapperToScreen } from './coord'

const HIT_EPS = 6

function pointDist(ax, ay, bx, by) {
  const dx = ax - bx
  const dy = ay - by
  return Math.sqrt(dx * dx + dy * dy)
}

function segDist(px, py, x1, y1, x2, y2) {
  const vx = x2 - x1
  const vy = y2 - y1
  const wx = px - x1
  const wy = py - y1
  const c1 = vx * wx + vy * wy
  if (c1 <= 0) return pointDist(px, py, x1, y1)
  const c2 = vx * vx + vy * vy
  if (c2 <= c1) return pointDist(px, py, x2, y2)
  const b = c1 / c2
  return pointDist(px, py, x1 + b * vx, y1 + b * vy)
}

export function drawingToScreenGeometry(drawing, mapper, size) {
  if (!drawing?.points?.length) return null
  const pts = drawing.points.map((p) => mapperToScreen(mapper, p))
  if (pts.some((p) => p.x === null || Number.isNaN(p.x))) return null
  switch (drawing.type) {
    case DRAW_TYPES.HLINE:
      return { type: 'line', x1: 0, y1: pts[0].y, x2: size.width, y2: pts[0].y }
    case DRAW_TYPES.VLINE:
      return { type: 'line', x1: pts[0].x, y1: 0, x2: pts[0].x, y2: size.height }
    case DRAW_TYPES.TRENDLINE:
      if (pts.length < 2) return null
      return { type: 'line', x1: pts[0].x, y1: pts[0].y, x2: pts[1].x, y2: pts[1].y }
    case DRAW_TYPES.RECT:
      if (pts.length < 2) return null
      return {
        type: 'rect',
        x: Math.min(pts[0].x, pts[1].x),
        y: Math.min(pts[0].y, pts[1].y),
        w: Math.abs(pts[0].x - pts[1].x),
        h: Math.abs(pts[0].y - pts[1].y),
      }
    case DRAW_TYPES.TEXT:
      return { type: 'text', x: pts[0].x, y: pts[0].y }
    default:
      return null
  }
}

export function hitTestDrawing(drawing, mapper, size, px, py) {
  const g = drawingToScreenGeometry(drawing, mapper, size)
  if (!g) return false
  switch (g.type) {
    case 'line':
      return segDist(px, py, g.x1, g.y1, g.x2, g.y2) <= HIT_EPS
    case 'rect':
      return px >= g.x - HIT_EPS && px <= g.x + g.w + HIT_EPS && py >= g.y - HIT_EPS && py <= g.y + g.h + HIT_EPS
    case 'text':
      return pointDist(px, py, g.x, g.y) <= 12
    default:
      return false
  }
}
