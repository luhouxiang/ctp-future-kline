import { createDrawing, DRAW_TYPES } from './types'
import { hitTestDrawing } from './overlay'
import { mapperFromScreen, mapperToScreen } from './coord'

export function createDrawController({ getState, emitChange, getScope, getMapper, getSize, promptText }) {
  const debugEnabled = (() => {
    try {
      const q = new URLSearchParams(window.location.search).get('draw_debug')
      if (q === '1') return true
      if (q === '0') return false
      const defaults = (window as any).__WEB_DEBUG_DEFAULTS__
      return Number(defaults?.draw_debug) === 1
    } catch {
      const defaults = (window as any).__WEB_DEBUG_DEFAULTS__
      return Number(defaults?.draw_debug) === 1
    }
  })()
  const debug = (...args) => {
    if (!debugEnabled) return
    // eslint-disable-next-line no-console
    console.log('[draw-debug]', ...args)
  }

  const runtime = {
    draft: null,
    drag: null,
    lastLogAt: 0,
  }

  function pickDrawing(x, y) {
    const state = getState()
    const mapper = getMapper()
    const size = getSize()
    const sorted = [...state.drawings].sort((a, b) => (b.z || 0) - (a.z || 0))
    return sorted.find((d) => d.visible !== false && hitTestDrawing(d, mapper, size, x, y))
  }

  function pickTrendlineAnchor(x, y) {
    const state = getState()
    const mapper = getMapper()
    const sorted = [...state.drawings]
      .filter((d) => d.visible !== false && d.type === DRAW_TYPES.TRENDLINE && Array.isArray(d.points) && d.points.length >= 2)
      .sort((a, b) => (b.z || 0) - (a.z || 0))
    for (const d of sorted) {
      const p0 = mapperToScreen(mapper, d.points[0])
      const p1 = mapperToScreen(mapper, d.points[1])
      if (p0.x == null || p0.y == null || p1.x == null || p1.y == null) continue
      const d0 = Math.hypot(x - p0.x, y - p0.y)
      if (d0 <= 6) return { drawing: d, anchorIndex: 0 }
      const d1 = Math.hypot(x - p1.x, y - p1.y)
      if (d1 <= 6) return { drawing: d, anchorIndex: 1 }
    }
    return null
  }

  function inspectAt(x, y) {
    const anchorHit = pickTrendlineAnchor(x, y)
    if (anchorHit) {
      return { id: anchorHit.drawing.id, cursor: 'default', mode: 'anchor', anchorIndex: anchorHit.anchorIndex }
    }
    const lineHit = pickDrawing(x, y)
    if (lineHit) {
      return { id: lineHit.id, cursor: 'pointer', mode: 'line', anchorIndex: -1 }
    }
    return { id: '', cursor: '', mode: '', anchorIndex: -1 }
  }

  function onPointerDown(x, y) {
    const state = getState()
    const mapper = getMapper()
    const tool = state.activeTool

    if (tool === DRAW_TYPES.CURSOR) {
      // If current selection is the same trendline, prefer moving the whole line
      // instead of grabbing an anchor accidentally.
      const lineHit = pickDrawing(x, y)
      const anchorHit = pickTrendlineAnchor(x, y)
      let info
      if (lineHit && state.selectedId && lineHit.id === state.selectedId) {
        info = { id: lineHit.id, cursor: 'pointer', mode: 'line', anchorIndex: -1 }
      } else if (anchorHit) {
        info = { id: anchorHit.drawing.id, cursor: 'default', mode: 'anchor', anchorIndex: anchorHit.anchorIndex }
      } else if (lineHit) {
        info = { id: lineHit.id, cursor: 'pointer', mode: 'line', anchorIndex: -1 }
      } else {
        info = { id: '', cursor: '', mode: '', anchorIndex: -1 }
      }
      state.selectedId = info.id || ''
      if (info.id) {
        const hit = state.drawings.find((d) => d.id === info.id)
        if (!hit) return
        runtime.drag = {
          id: hit.id,
          startX: x,
          startY: y,
          original: JSON.parse(JSON.stringify(hit.points)),
          mode: info.mode,
          anchorIndex: info.anchorIndex,
        }
      }
      return
    }

    if (tool === DRAW_TYPES.HLINE) {
      state.selectedId = ''
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      emitChange(createDrawing(DRAW_TYPES.HLINE, getScope(), [p]))
      return
    }

    if (tool === DRAW_TYPES.VLINE) {
      state.selectedId = ''
      const p = mapperFromScreen(mapper, x, y, false)
      if (!p) return
      emitChange(createDrawing(DRAW_TYPES.VLINE, getScope(), [p]))
      return
    }

    if (tool === DRAW_TYPES.TEXT) {
      state.selectedId = ''
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      const text = promptText()
      if (!text) return
      emitChange(createDrawing(DRAW_TYPES.TEXT, getScope(), [p], text))
      return
    }

    if (tool === DRAW_TYPES.TRENDLINE) {
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      if (!runtime.draft || runtime.draft.type !== DRAW_TYPES.TRENDLINE) {
        state.selectedId = ''
        runtime.draft = { type: DRAW_TYPES.TRENDLINE, points: [p] }
        return
      }
      runtime.draft.points.push(p)
      const created = createDrawing(DRAW_TYPES.TRENDLINE, getScope(), runtime.draft.points.slice(0, 2))
      emitChange(created)
      state.selectedId = created.id
      runtime.draft = null
      return
    }

    if (tool === DRAW_TYPES.RECT) {
      state.selectedId = ''
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      runtime.draft = { type: DRAW_TYPES.RECT, points: [p, p], startX: x, startY: y }
      return
    }
  }

  function onPointerMove(x, y) {
    const state = getState()
    const mapper = getMapper()

    if (runtime.drag) {
      const idx = state.drawings.findIndex((x0) => x0.id === runtime.drag.id)
      if (idx < 0) return
      const d = state.drawings[idx]
      let moved = runtime.drag.original
      if (runtime.drag.mode === 'anchor' && runtime.drag.anchorIndex >= 0) {
        const np = mapperFromScreen(mapper, x, y, true)
        if (!np) return
        moved = runtime.drag.original.map((p, i) => (i === runtime.drag.anchorIndex ? { time: np.time, price: np.price } : p))
      } else {
        // Move geometry in logical space (time/price) to avoid screen-space conversion
        // instability near pane edges or scale limits.
        const start = mapperFromScreen(mapper, runtime.drag.startX, runtime.drag.startY, true)
        const now = mapperFromScreen(mapper, x, y, true)
        if (start && now) {
          let dt = now.time - start.time
          let dpFromDelta =
            typeof mapper.priceDeltaFromYDelta === 'function' ? mapper.priceDeltaFromYDelta(y - runtime.drag.startY) : null
          let dp = Number.isFinite(dpFromDelta) ? dpFromDelta : now.price - start.price
          // Floor at price=0: keep geometry movement but disallow negative prices.
          let minOriginalPrice = Number.POSITIVE_INFINITY
          for (const p of runtime.drag.original) {
            const price = Number(p?.price)
            if (Number.isFinite(price)) minOriginalPrice = Math.min(minOriginalPrice, price)
          }
          if (Number.isFinite(minOriginalPrice) && minOriginalPrice+dp < 0) {
            dp = -minOriginalPrice
            dpFromDelta = dp
          }
          if (dt > 0 && typeof mapper.maxTime === 'function') {
            const maxTime = Number(mapper.maxTime())
            if (Number.isFinite(maxTime)) {
              let maxOriginalTime = Number.NEGATIVE_INFINITY
              for (const p of runtime.drag.original) {
                const t = Number(p?.time)
                if (Number.isFinite(t)) maxOriginalTime = Math.max(maxOriginalTime, t)
              }
              if (Number.isFinite(maxOriginalTime)) {
                const remain = maxTime - maxOriginalTime
                if (dt > remain) dt = Math.max(0, remain)
              }
            }
          }
          moved = runtime.drag.original.map((p) => {
            const nextTime = Math.round(Number(p.time) + dt)
            if (p.price === undefined || p.price === null) return { time: nextTime }
            return { time: nextTime, price: Number(p.price) + dp }
          })
          const nowMs = Date.now()
          if (debugEnabled && nowMs - runtime.lastLogAt > 120) {
            runtime.lastLogAt = nowMs
            const p0 = moved[0]
            const p1 = moved[1]
            const y0 = p0 && p0.price != null && typeof mapper.priceToY === 'function' ? mapper.priceToY(p0.price) : null
            const y1 = p1 && p1.price != null && typeof mapper.priceToY === 'function' ? mapper.priceToY(p1.price) : null
            const size = typeof getSize === 'function' ? getSize() : null
            const topPrice = typeof mapper.yToPrice === 'function' ? mapper.yToPrice(0) : null
            const bottomPrice =
              typeof mapper.yToPrice === 'function' && size && Number.isFinite(size.height) ? mapper.yToPrice(Math.max(0, size.height - 1)) : null
            const sp0 = p0 ? mapperToScreen(mapper, p0) : null
            const sp1 = p1 ? mapperToScreen(mapper, p1) : null
            debug('drag-move-logical', {
              id: d.id,
              pointer: { x, y, startX: runtime.drag.startX, startY: runtime.drag.startY },
              start,
              now,
              dt,
              dp,
              maxTime: typeof mapper.maxTime === 'function' ? mapper.maxTime() : null,
              moved0: p0 ? { time: p0.time, price: p0.price } : null,
              moved1: p1 ? { time: p1.time, price: p1.price } : null,
              y0,
              y1,
              screen0: sp0,
              screen1: sp1,
              clampProbe:
                typeof mapper.clampY === 'function' && size && Number.isFinite(size.height)
                  ? mapper.clampY(size.height + 20)
                  : null,
              paneSize: size,
              topPrice,
              bottomPrice,
            })
          }
        } else {
          debug('drag-move-fallback', {
            id: d.id,
            pointer: { x, y, startX: runtime.drag.startX, startY: runtime.drag.startY },
            start,
            now,
          })
          // Fallback for unexpected mapper failures.
          const dx = x - runtime.drag.startX
          const dy = y - runtime.drag.startY
          moved = runtime.drag.original.map((p) => {
            const sp = mapperToScreen(mapper, p)
            if (sp.x === null || sp.y === null) return p
            const np = mapperFromScreen(mapper, sp.x + dx, sp.y + dy, p.price !== undefined)
            if (!np) return p
            return p.price === undefined ? { time: np.time } : { time: np.time, price: np.price }
          })
        }
      }
      d.points = moved
      d.updated_at = new Date().toISOString()
      emitChange(d, true)
      return
    }

    if (runtime.draft && runtime.draft.type === DRAW_TYPES.RECT) {
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      runtime.draft.points[1] = p
    }
    if (runtime.draft && runtime.draft.type === DRAW_TYPES.TRENDLINE) {
      const p = mapperFromScreen(mapper, x, y, true)
      if (!p) return
      if (runtime.draft.points.length < 2) {
        runtime.draft.points.push(p)
      } else {
        runtime.draft.points[1] = p
      }
    }
  }

  function onPointerUp() {
    const state = getState()
    if (runtime.drag) {
      debug('drag-end', { id: runtime.drag.id })
      runtime.drag = null
      state.dirty = true
    }
    if (runtime.draft && runtime.draft.type === DRAW_TYPES.RECT) {
      emitChange(createDrawing(DRAW_TYPES.RECT, getScope(), runtime.draft.points.slice(0, 2)))
      runtime.draft = null
    }
  }

  function onDeleteSelected() {
    const state = getState()
    if (!state.selectedId) return null
    const id = state.selectedId
    state.selectedId = ''
    return id
  }

  function getDraft() {
    return runtime.draft
  }

  function clearDraft() {
    runtime.draft = null
  }

  function isDragging() {
    return !!runtime.drag
  }

  return { onPointerDown, onPointerMove, onPointerUp, onDeleteSelected, getDraft, clearDraft, inspectAt, isDragging }
}
