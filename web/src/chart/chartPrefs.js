export const CHART_LAST_SESSION_PREF_KEY = "chart_last_session_prefs_v1";
export const TRADE_WINDOW_PREF_KEY = "chart_trade_window_prefs_v1";
export const KLINE_REPLAY_PANEL_PREF_KEY = "chart_kline_replay_panel_prefs_v1";

export function readLocalJSON(key) {
  try {
    if (typeof localStorage === "undefined") return null;
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const data = JSON.parse(raw);
    return data && typeof data === "object" ? data : null;
  } catch {
    return null;
  }
}

export function writeLocalJSON(key, value) {
  try {
    if (typeof localStorage === "undefined") return false;
    localStorage.setItem(key, JSON.stringify(value || {}));
    return true;
  } catch {
    return false;
  }
}

function cleanText(value) {
  return String(value || "").trim();
}

function cleanLower(value) {
  return cleanText(value).toLowerCase();
}

export function normalizeChartSessionPrefs(data = {}) {
  const visible =
    typeof data.trade_window_visible === "boolean"
      ? data.trade_window_visible
      : typeof data.open_trade === "boolean"
        ? data.open_trade
        : null;
  return {
    symbol: cleanLower(data.symbol),
    type: cleanLower(data.type),
    variety: cleanLower(data.variety),
    timeframe: cleanLower(data.timeframe || "1m") || "1m",
    end: cleanText(data.end),
    data_mode: cleanLower(data.data_mode || data.dataMode),
    app_mode: cleanLower(data.app_mode || data.appMode),
    replay_default_mode: cleanLower(data.replay_default_mode || data.replayMode),
    trade_window_visible: visible,
    updated_at: cleanText(data.updated_at),
  };
}

export function readLastChartSessionPrefs() {
  return normalizeChartSessionPrefs(readLocalJSON(CHART_LAST_SESSION_PREF_KEY) || {});
}

export function writeLastChartSessionPrefs(patch = {}) {
  const current = readLocalJSON(CHART_LAST_SESSION_PREF_KEY) || {};
  const next = normalizeChartSessionPrefs({
    ...current,
    ...patch,
    updated_at: new Date().toISOString(),
  });
  return writeLocalJSON(CHART_LAST_SESSION_PREF_KEY, next);
}

export function readTradeWindowPrefs() {
  return readLocalJSON(TRADE_WINDOW_PREF_KEY);
}

export function readKlineReplayPanelPrefs() {
  return readLocalJSON(KLINE_REPLAY_PANEL_PREF_KEY);
}
