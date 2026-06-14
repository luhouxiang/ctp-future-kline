function cleanText(value) {
  return String(value || '').trim()
}

function cleanLower(value) {
  return cleanText(value).toLowerCase()
}

function cleanIDPart(value) {
  return cleanLower(value).replace(/[^a-z0-9_.-]+/g, '_').replace(/^_+|_+$/g, '') || 'item'
}

function clonePlainObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  try {
    return JSON.parse(JSON.stringify(value))
  } catch {
    return { ...value }
  }
}

const STRATEGY_START_TIMEOUT_MS = 45_000

async function fetchWithTimeout(fetcher, url, options = {}, timeoutMS = 0) {
  if (!timeoutMS || typeof AbortController === 'undefined') {
    return fetcher(url, options)
  }
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMS)
  try {
    return await fetcher(url, { ...options, signal: controller.signal })
  } catch (err) {
    // 浏览器 fetch 默认没有超时；如果后端、Python 调试器或 warmup 查询卡住，
    // 不主动 abort 会让“启动策略”按钮一直停在启动中，用户也看不到失败原因。
    if (err?.name === 'AbortError') {
      throw new Error(`策略启动请求超过 ${Math.round(timeoutMS / 1000)} 秒未返回`)
    }
    throw err
  } finally {
    clearTimeout(timer)
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
    || items.find((item) => item?.strategy_id === 'ma20.state_diagram_short')
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
  const displayTime = formatStrategyAnchorTime(anchor.adjusted_time || anchor.plot_time)
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
  const resp = await fetchWithTimeout(fetcher, '/api/strategy/instances', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(instance),
  }, STRATEGY_START_TIMEOUT_MS)
  if (!resp.ok) throw new Error(await resp.text())
  return resp.json()
}

export async function startStrategyInstance(instanceID, fetcher = fetch) {
  const id = cleanText(instanceID)
  if (!id) throw new Error('instance_id is required')
  const resp = await fetchWithTimeout(fetcher, `/api/strategy/instances/${encodeURIComponent(id)}/start`, {
    method: 'POST',
  }, STRATEGY_START_TIMEOUT_MS)
  if (!resp.ok) throw new Error(await resp.text())
  return resp.json()
}

export async function saveAndStartStrategyInstance(instance, fetcher = fetch) {
  await saveStrategyInstance(instance, fetcher)
  return startStrategyInstance(instance?.instance_id, fetcher)
}

export function featureKeyForHelperStrategy(strategyID) {
  const id = cleanLower(strategyID)
  if (id.startsWith('indicator.')) return id.slice('indicator.'.length)
  return id
}

export function normalizeStrategyComposition(input = {}) {
  const compositionID = cleanIDPart(input.composition_id || input.compositionID || 'ma20_state_zigzag')
  const helperIDs = Array.isArray(input.helper_strategy_ids) ? input.helper_strategy_ids.map(cleanText).filter(Boolean) : []
  return {
    composition_id: compositionID,
    display_name: cleanText(input.display_name) || compositionID,
    primary_strategy_id: cleanText(input.primary_strategy_id),
    helper_strategy_ids: helperIDs,
    primary_params: clonePlainObject(input.primary_params),
    helper_params_by_strategy: clonePlainObject(input.helper_params_by_strategy),
    updated_at: cleanText(input.updated_at),
  }
}

export async function fetchStrategyCompositions(fetcher = fetch) {
  const resp = await fetchWithTimeout(fetcher, '/api/strategy/compositions', {}, STRATEGY_START_TIMEOUT_MS)
  if (!resp.ok) throw new Error(await resp.text())
  const data = await resp.json()
  return Array.isArray(data.items) ? data.items.map(normalizeStrategyComposition) : []
}

export async function saveStrategyCompositions(items, fetcher = fetch) {
  const normalized = (Array.isArray(items) ? items : []).map(normalizeStrategyComposition)
  const resp = await fetchWithTimeout(fetcher, '/api/strategy/compositions', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: normalized }),
  }, STRATEGY_START_TIMEOUT_MS)
  if (!resp.ok) throw new Error(await resp.text())
  const data = await resp.json()
  return Array.isArray(data.items) ? data.items.map(normalizeStrategyComposition) : normalized
}

export function buildChartStrategyCompositionInstances({
  anchor,
  composition,
  definitions,
  scope,
  accountID,
  fallbackAccountID,
  primaryParamsText = '',
  helperParamsText = '',
  replayKlineMode = false,
  dataMode = '',
  klineReplayRunning = false,
  now = Date.now(),
} = {}) {
  const normalized = normalizeStrategyComposition(composition)
  const primaryOverrideParams = primaryParamsText ? parseStrategyParamsText(primaryParamsText) : clonePlainObject(normalized.primary_params)
  const helperOverrideParams = helperParamsText ? parseStrategyParamsText(helperParamsText) : clonePlainObject(normalized.helper_params_by_strategy)
  const byID = new Map((Array.isArray(definitions) ? definitions : []).map((item) => [cleanText(item?.strategy_id), item]))
  const primaryDefinition = byID.get(normalized.primary_strategy_id)
  if (!primaryDefinition) throw new Error(`组合主策略不可用: ${normalized.primary_strategy_id}`)
  if (!anchor) throw new Error('右键位置没有可用K线')
  const symbol = cleanLower(scope?.symbol)
  if (!symbol) throw new Error('当前图表没有合约')
  const timeframe = cleanText(scope?.timeframe || '1m') || '1m'
  const suffix = `${cleanIDPart(symbol)}-${cleanIDPart(timeframe)}-${now}`
  const primaryInstanceID = `combo-${normalized.composition_id}-primary-${suffix}`
  const featureDependencies = Array.isArray(normalized.primary_params?.feature_dependencies)
    ? normalized.primary_params.feature_dependencies.map(cleanText).filter(Boolean)
    : normalized.helper_strategy_ids.map(featureKeyForHelperStrategy).filter(Boolean)
  const helperInstanceIDs = normalized.helper_strategy_ids.map((strategyID) => (
    `combo-${normalized.composition_id}-helper-${cleanIDPart(strategyID)}-${suffix}`
  ))

  const helpers = normalized.helper_strategy_ids.map((strategyID, index) => {
    const definition = byID.get(strategyID)
    if (!definition) throw new Error(`组合辅助策略不可用: ${strategyID}`)
    const helperParams = {
      ...strategyDefaultParams(definition),
      ...clonePlainObject(helperOverrideParams?.[strategyID]),
      composition_id: normalized.composition_id,
      composition_role: 'helper',
      parent_instance_id: primaryInstanceID,
    }
    return buildChartStrategyInstance({
      anchor,
      definition,
      scope,
      paramsText: JSON.stringify(helperParams),
      accountID,
      fallbackAccountID,
      replayKlineMode,
      dataMode,
      klineReplayRunning,
      now,
    }).instance
  }).map((instance, index) => ({
    ...instance,
    instance_id: helperInstanceIDs[index],
    display_name: `${normalized.display_name} / ${instance.display_name || instance.strategy_id}`,
  }))

  const primaryParams = {
    ...strategyDefaultParams(primaryDefinition),
    ...clonePlainObject(primaryOverrideParams),
    composition_id: normalized.composition_id,
    composition_role: 'primary',
    feature_dependencies: featureDependencies,
    helper_instance_ids: helperInstanceIDs,
  }
  const primary = buildChartStrategyInstance({
    anchor,
    definition: primaryDefinition,
    scope,
    paramsText: JSON.stringify(primaryParams),
    accountID,
    fallbackAccountID,
    replayKlineMode,
    dataMode,
    klineReplayRunning,
    now,
  }).instance
  primary.instance_id = primaryInstanceID
  primary.display_name = `${normalized.display_name} / 主策略@${symbol}-${timeframe}`

  return {
    primaryInstanceID,
    helperInstanceIDs,
    helpers,
    primary,
    instances: [...helpers, primary],
  }
}

export async function saveAndStartStrategyComposition(args = {}, fetcher = fetch) {
  const built = buildChartStrategyCompositionInstances(args)
  const started = []
  for (const helper of built.helpers) {
    await saveAndStartStrategyInstance(helper, fetcher)
    started.push(helper.instance_id)
  }
  await saveAndStartStrategyInstance(built.primary, fetcher)
  started.push(built.primary.instance_id)
  return { ...built, startedInstanceIDs: started }
}
