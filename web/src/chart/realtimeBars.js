export function normalizeRealtimeBar(bar = {}) {
  const adjustedTime = Number(bar.adjusted_time || 0);
  const dataTime = Number(bar.data_time || adjustedTime || 0);
  return {
    adjusted_time: adjustedTime,
    data_time: dataTime,
    plot_time: adjustedTime,
    open: Number(bar.open || 0),
    high: Number(bar.high || 0),
    low: Number(bar.low || 0),
    close: Number(bar.close || 0),
    volume: Number(bar.volume || 0),
    open_interest: Number(bar.open_interest || 0),
  };
}

export function mergeRealtimeBarUpdate(rawBars, update) {
  const bars = Array.isArray(rawBars) ? [...rawBars] : [];
  const phase = String(update?.phase || "").trim().toLowerCase();
  const bar = normalizeRealtimeBar(update?.bar || {});
  if (!Number.isFinite(bar.adjusted_time) || bar.adjusted_time <= 0) {
    return bars;
  }
  const idx = bars.findIndex((item) => Number(item?.adjusted_time) === bar.adjusted_time);
  if (idx >= 0) {
    bars[idx] = { ...bars[idx], ...bar };
  } else {
    bars.push(bar);
  }
  bars.sort((a, b) => Number(a.adjusted_time || 0) - Number(b.adjusted_time || 0));
  if (phase === "final") {
    return bars;
  }
  if (bars.length <= 1) {
    return bars;
  }
  const last = bars[bars.length - 1];
  const prev = bars[bars.length - 2];
  if (Number(last.adjusted_time || 0) < Number(prev.adjusted_time || 0)) {
    bars.sort((a, b) => Number(a.adjusted_time || 0) - Number(b.adjusted_time || 0));
  }
  return bars;
}

