import { createApp } from 'vue'
import App from './App.vue'
import TradingTerminal from './chart/TradingTerminal.vue'
import './style.css'

function resolveDebugFlag(key) {
  let queryValue = null
  try {
    queryValue = new URLSearchParams(window.location.search).get(key)
  } catch {
    queryValue = null
  }
  if (queryValue === '1') return true
  if (queryValue === '0') return false
  const defaults = window.__WEB_DEBUG_DEFAULTS__
  if (defaults && Number(defaults[key]) === 1) return true
  return false
}

function setupClientLogBridge() {
  const enabled = resolveDebugFlag('browser_log')
  if (!enabled) return

  const raw = {
    log: console.log.bind(console),
    info: console.info.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
  }

  const safe = (v) => {
    if (v === null || v === undefined) return v
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v
    try {
      return JSON.parse(JSON.stringify(v))
    } catch {
      try {
        return String(v)
      } catch {
        return '[unserializable]'
      }
    }
  }

  const send = (level, args) => {
    const payload = {
      level,
      ts: new Date().toISOString(),
      path: window.location.pathname + window.location.search,
      args: Array.isArray(args) ? args.map((x) => safe(x)) : [],
    }
    const body = JSON.stringify(payload)
    if (navigator.sendBeacon) {
      navigator.sendBeacon('/api/client-log', new Blob([body], { type: 'application/json' }))
      return
    }
    fetch('/api/client-log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      keepalive: true,
    }).catch(() => {
      // no-op
    })
  }

  console.log = (...args) => {
    raw.log(...args)
    send('log', args)
  }
  console.info = (...args) => {
    raw.info(...args)
    send('info', args)
  }
  console.warn = (...args) => {
    raw.warn(...args)
    send('warn', args)
  }
  console.error = (...args) => {
    raw.error(...args)
    send('error', args)
  }

  raw.info('[client-log] browser_log=1 enabled')
}

setupClientLogBridge()

const component = window.location.pathname === '/chart' ? TradingTerminal : App
createApp(component).mount('#app')
