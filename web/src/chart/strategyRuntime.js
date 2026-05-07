function cleanText(value) {
  return String(value || '').trim()
}

function cleanLower(value) {
  return cleanText(value).toLowerCase()
}

function clonePlainObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  try {
    return JSON.parse(JSON.stringify(value))
  } catch {
    return { ...value }
  }
}

export function formatStrategyAnchorTime(unixSeconds) {
  const n = Number(unixSeconds || 0)
  if (!Number.isFinite(n) || n <= 0) return '--'
  const d = new Date(n * 1000)
  const p = (v) => String(v).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}

export function strategyDefaultParams(definition) {
  return clonePlainObject(definition?.default_params)
}

export function strategyDefaultParamsText(definition) {
  return JSON.stringify(strategyDefaultParams(definition), null, 2)
}

export function parseStrategyParamsText(rawText) {
  const raw = cleanText(rawText)
  if (!raw) return {}
  const parsed = JSON.parse(raw)
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('参数必须是 JSON 对象')
  }
  return parsed
}

export function selectPreferredStrategyDefinition(definitions) {
  const items = Array.isArray(definitions) ? definitions : []
  return items.find((item) => item?.strategy_id === 'ma20.weak_pullback_short.score_filter')
    || items.find((item) => item?.strategy_id === 'ma20.weak_pullback_short')
    || items[0]
    || null
}

export function resolveStrategyRunMode({ replayKlineMode = false, dataMode = '', klineReplayRunning = false } = {}) {
  if (replayKlineMode || cleanLower(dataMode) === 'replay' || klineReplayRunning) return 'replay'
  return 'paper'
}

export function buildChartStrategyInstance({
  anchor,
  definition,
  scope,
  paramsText,
  accountID,
  fallbackAccountID,
  replayKlineMode = false,
  dataMode = '',
  klineReplayRunning = false,
  now = Date.now(),
} = {}) {
  const strategyID = cleanText(definition?.strategy_id)
  if (!strategyID) throw new Error('strategy_id is required')
  if (!anchor) throw new Error('右键位置没有可用K线')

  const symbol = cleanLower(scope?.symbol)
  if (!symbol) throw new Error('当前图表没有合约')
  const timeframe = cleanText(scope?.timeframe || '1m') || '1m'
  const mode = resolveStrategyRunMode({ replayKlineMode, dataMode, klineReplayRunning })
  const displayTime = formatStrategyAnchorTime(anchor.data_time || anchor.adjusted_time || anchor.plot_time)
  if (displayTime === '--') throw new Error('右键位置没有有效K线时间')

  const defaultParams = strategyDefaultParams(definition)
  const userParams = parseStrategyParamsText(paramsText)
  const mergedParams = { ...defaultParams, ...userParams }
  const warmupSource = replayKlineMode ? 'realtime' : ''
  const instanceID = `chart-${symbol}-${cleanLower(timeframe)}-${now}`
  const displayName = `${definition?.display_name || strategyID}@${symbol}-${timeframe}-${displayTime}`

  return {
    instanceID,
    instance: {
      instance_id: instanceID,
      strategy_id: strategyID,
      display_name: displayName,
      mode,
      account_id: cleanText(accountID || fallbackAccountID || mode),
      symbols: [symbol],
      timeframe,
      params: {
        ...mergedParams,
        chart_anchor: anchor,
        chart_start_time: displayTime,
        start_time: displayTime,
        start_source: 'chart_context_menu',
        runtime_factory: 'python',
        replay_mode: replayKlineMode ? 'kline' : '',
        warmup_source: warmupSource,
      },
    },
  }
}

export async function saveStrategyInstance(instance, fetcher = fetch) {
  const resp = await fetcher('/api/strategy/instances', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(instance),
  })
  if (!resp.ok) throw new Error(await resp.text())
  return resp.json()
}

export async function startStrategyInstance(instanceID, fetcher = fetch) {
  const id = cleanText(instanceID)
  if (!id) throw new Error('instance_id is required')
  const resp = await fetcher(`/api/strategy/instances/${encodeURIComponent(id)}/start`, {
    method: 'POST',
  })
  if (!resp.ok) throw new Error(await resp.text())
  return resp.json()
}

export async function saveAndStartStrategyInstance(instance, fetcher = fetch) {
  await saveStrategyInstance(instance, fetcher)
  return startStrategyInstance(instance?.instance_id, fetcher)
}
