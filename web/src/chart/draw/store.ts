import { reactive } from 'vue'

export function createDrawStore() {
  const state = reactive({
    activeTool: 'cursor',
    selectedId: '',
    drawings: [],
    dirty: false,
    lastSavedAt: '',
    saveError: '',
  })

  function setDrawings(items) {
    state.drawings = Array.isArray(items) ? items : []
    state.dirty = false
  }

  function upsertDrawing(item) {
    const idx = state.drawings.findIndex((x) => x.id === item.id)
    if (idx >= 0) {
      state.drawings[idx] = item
    } else {
      state.drawings.push(item)
    }
    state.dirty = true
  }

  function removeDrawing(id) {
    state.drawings = state.drawings.filter((x) => x.id !== id)
    if (state.selectedId === id) state.selectedId = ''
    state.dirty = true
  }

  return { state, setDrawings, upsertDrawing, removeDrawing }
}
