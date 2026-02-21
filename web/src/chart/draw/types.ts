export const DRAW_TYPES = {
  CURSOR: 'cursor',
  HLINE: 'hline',
  VLINE: 'vline',
  TRENDLINE: 'trendline',
  RECT: 'rect',
  TEXT: 'text',
}

export function defaultStyle(type) {
  switch (type) {
    case DRAW_TYPES.RECT:
      return { color: '#4ea1ff', width: 1, fill: '#4ea1ff', opacity: 0.12 }
    case DRAW_TYPES.TEXT:
      return { color: '#dce7f5', width: 1, opacity: 1 }
    default:
      return { color: '#4ea1ff', width: 1, opacity: 1 }
  }
}

export function createDrawing(type, scope, points, text = '') {
  const now = new Date().toISOString()
  const style = defaultStyle(type)
  const isTrendline = type === DRAW_TYPES.TRENDLINE && Array.isArray(points) && points.length >= 2
  return {
    id: `drw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    type,
    owner: 'admin',
    symbol: scope.symbol,
    type_kind: scope.type,
    variety: scope.variety || '',
    timeframe: scope.timeframe || '1m',
    object_class: type === DRAW_TYPES.TRENDLINE ? 'trendline' : 'general',
    points,
    text,
    style,
    start_time: isTrendline ? Number(points[0]?.time || 0) : 0,
    end_time: isTrendline ? Number(points[1]?.time || 0) : 0,
    start_price: isTrendline ? points[0]?.price ?? null : null,
    end_price: isTrendline ? points[1]?.price ?? null : null,
    line_color: style.color,
    line_width: style.width,
    line_style: 'solid',
    left_cap: 'plain',
    right_cap: 'plain',
    label_text: text || '',
    label_pos: 'middle',
    label_align: 'center',
    visible_range: 'all',
    locked: false,
    visible: true,
    z: Date.now(),
    created_at: now,
    updated_at: now,
  }
}
