let initializedTicketID = "";
let wasmReady = false;
let wasmBootPromise = null;
let goRuntime = null;

async function loadScriptOnce(src) {
  const existing = document.querySelector(`script[data-klinecompose-src="${src}"]`);
  if (existing) {
    if (existing.dataset.loaded === "1") return;
    await new Promise((resolve, reject) => {
      existing.addEventListener("load", resolve, { once: true });
      existing.addEventListener("error", reject, { once: true });
    });
    return;
  }
  await new Promise((resolve, reject) => {
    const el = document.createElement("script");
    el.src = src;
    el.async = true;
    el.dataset.klinecomposeSrc = src;
    el.addEventListener("load", () => {
      el.dataset.loaded = "1";
      resolve();
    }, { once: true });
    el.addEventListener("error", reject, { once: true });
    document.head.appendChild(el);
  });
}

export async function bootKlineComposerWASM() {
  if (wasmReady) return true;
  if (wasmBootPromise) return wasmBootPromise;
  wasmBootPromise = (async () => {
    try {
      await loadScriptOnce("/wasm/wasm_exec.js");
      if (typeof globalThis.Go !== "function") return false;
      if (!(globalThis.WebAssembly && typeof globalThis.WebAssembly.instantiateStreaming === "function")) return false;
      goRuntime = new globalThis.Go();
      const result = await globalThis.WebAssembly.instantiateStreaming(fetch("/wasm/klinecompose.wasm"), goRuntime.importObject);
      goRuntime.run(result.instance);
      wasmReady = hasWASMComposer();
      return wasmReady;
    } catch {
      wasmReady = false;
      return false;
    }
  })();
  return wasmBootPromise;
}

function hasWASMComposer() {
  return (
    typeof globalThis.klinecompose_init === "function" &&
    typeof globalThis.klinecompose_push_tick === "function" &&
    typeof globalThis.klinecompose_get_bar === "function" &&
    typeof globalThis.klinecompose_reset === "function"
  );
}

function parseJSON(raw, fallback = null) {
  if (typeof raw !== "string" || !raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

export function initKlineComposer(ticketPayload) {
  if (!hasWASMComposer()) {
    void bootKlineComposerWASM();
    return false;
  }
  const ticketID = String(ticketPayload?.ticket_id || "").trim();
  if (!ticketID) return false;
  if (initializedTicketID === ticketID) return true;
  const ok = globalThis.klinecompose_init(JSON.stringify(ticketPayload || {}));
  if (ok) initializedTicketID = ticketID;
  return !!ok;
}

export function resetKlineComposer(ticketID = "") {
  if (!hasWASMComposer()) return false;
  const ok = globalThis.klinecompose_reset(String(ticketID || ""));
  if (ok) initializedTicketID = "";
  return !!ok;
}

export function pushComposeTick(tickPayload) {
  if (!hasWASMComposer()) return null;
  return parseJSON(globalThis.klinecompose_push_tick(JSON.stringify(tickPayload || {})), null);
}

export function getComposedBar(timeframe) {
  if (!hasWASMComposer()) return null;
  return parseJSON(globalThis.klinecompose_get_bar(String(timeframe || "1m")), null);
}
