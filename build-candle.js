require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");
const { loadScripMaster } = require("./loadScripMaster");
const { fetchHistoricalCandles } = require("./fetchHistoricalCandles");

// Global smartApi instance for session refresh
let globalSmartApi = null;
let globalSession = null;
let sessionRefreshInterval = null;

const STRATEGY_URL = "http://localhost:4000/evaluate";

// ---- Log push to server.js (port 2000) ----
const _bcOrigLog = console.log;
const _bcOrigError = console.error;
let _bcLogBatch = [];

function _bcFormatArgs(args) {
  return args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' ');
}

console.log = (...args) => {
  _bcOrigLog(...args);
  _bcLogBatch.push(`[LOG] ${new Date().toLocaleTimeString()} ${_bcFormatArgs(args)}`);
};

console.error = (...args) => {
  _bcOrigError(...args);
  _bcLogBatch.push(`[ERR] ${new Date().toLocaleTimeString()} ${_bcFormatArgs(args)}`);
};

setInterval(() => {
  if (_bcLogBatch.length === 0) return;
  const batch = _bcLogBatch.splice(0);
  fetch("http://localhost:2000/logs/candle-push", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lines: batch }),
  }).catch(() => {});
}, 2000);

// Per-symbol candle building state (max 2 active strategy symbols)
// candleStateBySymbol[symbol] = {
//   currentCandle: null,
//   lastMinute: null,
//   completedCandles: [],
//   token: null,
// }
const candleStateBySymbol = {};

// ---- Nifty50 Index candle tracking (dedicated, always-on) ----
const NIFTY50_TOKEN = "99926000";
const NIFTY50_EXCHANGE_TYPE = 1; // NSE
const NIFTY50_MAX_CANDLES = 700;
let nifty50State = {
  currentCandle: null,
  lastMinute: null,
  completedCandles: [],
  subscribed: false,
  historyLoaded: false,
};

// Track which strategy symbols are currently subscribed for candle building
let subscribedStrategySymbols = new Set();

// Track all watchlist token subscriptions for LTP
let subscribedWatchlistTokens = new Set();

// Keep symbol <-> token maps in memory
let symbolToTokenMap = {};
let tokenToSymbolMap = {};
let lastHistoryFetchTimeBySymbol = {};
let lastSymbolTokenMapRefresh = 0;
let failedSymbolCooldown = {};

// Prevent overlapping subscribe calls when interval runs again before previous one finishes
let isSubscriptionInProgress = false;

function formatMinute(timestamp) {
  const date = new Date(Number(timestamp));

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}`;
}

function getNextMinute(minuteString) {
  const date = new Date(minuteString.replace(" ", "T") + ":00");

  date.setMinutes(date.getMinutes() + 1);

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}`;
}

// Load scrip master and build fast lookup maps
async function buildSymbolTokenMaps() {
  try {
    const rows = await loadScripMaster();

    symbolToTokenMap = {};
    tokenToSymbolMap = {};

    rows.forEach((item) => {
      const symbol = item.symbol;
      const token = String(item.token);

      symbolToTokenMap[symbol] = token;
      tokenToSymbolMap[token] = symbol;
    });

    lastSymbolTokenMapRefresh = Date.now();
    console.log("Symbol-token maps ready");
  } catch (error) {
    console.error("Build symbol-token maps failed:", error.message);
  }
}

// Refresh maps if stale (older than 6 hours)
async function refreshSymbolTokenMapsIfNeeded() {
  const REFRESH_INTERVAL = 6 * 60 * 60 * 1000;
  if (Date.now() - lastSymbolTokenMapRefresh > REFRESH_INTERVAL) {
    await buildSymbolTokenMaps();
  }
}

// Read the active strategy symbols from local API server (array, max 2)
async function getActiveStrategySymbols() {
  try {
    const response = await fetch("http://localhost:2000/active-strategy-symbols");
    const data = await response.json();
    return Array.isArray(data.symbols) ? data.symbols.map(s => formatSensexSymbolForLookup(s)) : [];
  } catch (error) {
    console.error("Get active strategy symbols failed:", error.message);
    return [];
  }
}

// Fetch symbols explicitly removed by frontend (drains the queue)
async function getPendingRemovals() {
  try {
    const response = await fetch("http://localhost:2000/pending-symbol-removals");
    const data = await response.json();
    return Array.isArray(data.symbols) ? data.symbols.map(s => formatSensexSymbolForLookup(s)) : [];
  } catch (error) {
    return [];
  }
}

// Read full watchlist symbols from local API server
async function getWatchlistSymbols() {
  try {
    const response = await fetch("http://localhost:2000/watchlist-symbols");
    const data = await response.json();

    if (!Array.isArray(data.symbols)) {
      return [];
    }

    return data.symbols.map(s => formatSensexSymbolForLookup(s));
  } catch (error) {
    console.error("Get watchlist symbols failed:", error.message);
    return [];
  }
}

// Find token from in-memory symbol map
function getTokenForSymbol(symbol) {
  const token = symbolToTokenMap[symbol];

  if (!token) {
    console.error("Token not found for symbol:", symbol);
    return null;
  }

  return token;
}

// Convert Sensex DDMMMYY back to YYMDD format for token lookup (e.g., 07MAY26 -> 26507)
function formatSensexSymbolForLookup(symbol) {
  if (!symbol.startsWith("SENSEX")) return symbol;

  const match = symbol.match(/^SENSEX(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})(\d{5})(CE|PE)$/);
  if (!match) return symbol; // Already in YYMDD format or different format

  const [, day, monthName, year, strike, type] = match;
  const months = { 'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                  'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12 };
  const month = String(months[monthName]);

  return `SENSEX${year}${month}${day}${strike}${type}`;
}

// Get exchange type based on symbol (NIFTY=2 for NFO, SENSEX=4 for BFO)
function getExchangeTypeForSymbol(symbol) {
  if (symbol.startsWith("SENSEX")) {
    return 4; // BSE F&O
  }
  return 2; // NSE F&O (NIFTY and others)
}

// Report symbol history fetch status to server.js
async function reportSymbolHistoryStatus(symbol, status, candleCount = 0) {
  try {
    await fetch("http://localhost:2000/symbol-history-status", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol, status, candleCount }),
    });
  } catch (error) {
    // Non-critical — don't block on this
  }
}

// Send latest market time to local API server
async function sendMarketTime(minute) {
  try {
    await fetch("http://localhost:2000/market-time", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        marketTime: minute,
      }),
    });
  } catch (error) {
    console.error("Send market time failed:", error.message);
  }
}

// Send latest live price to local API server
async function sendPriceUpdate(symbol, ltp, marketTime) {
  try {
    if (!symbol) {
      return;
    }

    await fetch("http://localhost:2000/price-update", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol,
        ltp,
        marketTime,
      }),
    });
  } catch (error) {
    console.error("Send price update failed:", error.message);
  }
}

// Send completed candle to strategy for a specific symbol
async function sendCandleToStrategy(symbol, candle) {
  try {
    if (!symbol) {
      console.log("No symbol provided. Skipping candle send.");
      return;
    }

    const response = await fetch(STRATEGY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: symbol,
        candle: candle,
        mode: "live",
      }),
    });

    let data = null;

    try {
      data = await response.json();
    } catch (jsonError) {
      console.error("Strategy response is not valid JSON");
      return;
    }

    if (!response.ok) {
      console.error(`Strategy engine returned status ${response.status}`);
      console.error("Strategy error response:", data);
      return;
    }

    console.log(
      `Sent candle to strategy: ${candle.time} | ${symbol} | signal ${data.signal}`
    );
  } catch (error) {
    console.error("Strategy server unreachable. Candle send skipped.");
    console.error(error.message);
  }
}

async function sendHistoricalCandlesToStrategy(symbol, candles) {
  try {
    if (!symbol) {
      console.log("No symbol provided. Skipping history send.");
      return;
    }

    const response = await fetch(STRATEGY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: symbol,
        candles: candles,
        mode: "history",
      }),
    });

    let data = null;

    try {
      data = await response.json();
    } catch (jsonError) {
      console.error("Strategy response is not valid JSON (history)");
      return;
    }

    if (!response.ok) {
      console.error(`Strategy engine returned status ${response.status}`);
      console.error("Strategy error response:", data);
      return;
    }

    console.log(
      `Sent historical candles: ${candles.length} | ${symbol} | signal ${data.signal}`
    );
  } catch (error) {
    console.error("Strategy server unreachable (history).");
    console.error(error.message);
  }
}

function initCandleStateForSymbol(symbol, token) {
  candleStateBySymbol[symbol] = {
    currentCandle: null,
    lastMinute: null,
    completedCandles: [],
    token: String(token),
    volumeAtCandleStart: null,
  };
}

function removeCandleStateForSymbol(symbol) {
  delete candleStateBySymbol[symbol];
}

function extractTickToken(tick) {
  const rawToken = String(
    tick.token ||
      tick.symboltoken ||
      tick.symbolToken ||
      tick.tk ||
      ""
  );

  return rawToken.replace(/"/g, "").trim();
}

function extractTickPrice(tick) {
  const rawPrice = Number(tick.last_traded_price);

  if (!Number.isFinite(rawPrice)) {
    return null;
  }

  return rawPrice / 100;
}

function extractTickCumulativeVolume(tick) {
  const rawVol = Number(tick.volume_trade_for_the_day);

  if (!Number.isFinite(rawVol) || rawVol < 0) {
    return null;
  }

  return rawVol;
}

function extractTickMinute(tick) {
  if (!tick.exchange_timestamp) {
    return null;
  }

  return formatMinute(tick.exchange_timestamp);
}

// Subscribe all watchlist symbols for LTP,
// and also keep active strategy symbols ready for candle building
async function subscribeToSymbols(ws, smartApi) {
  if (isSubscriptionInProgress) {
    return;
  }

  isSubscriptionInProgress = true;

  try {
    await refreshSymbolTokenMapsIfNeeded();

    const activeStrategySymbols = await getActiveStrategySymbols();
    const watchlistSymbols = await getWatchlistSymbols();

    // --- Watchlist LTP subscriptions ---

    const watchlistTokensToAdd = [];

    for (const symbol of watchlistSymbols) {
      const token = getTokenForSymbol(symbol);

      if (!token) {
        continue;
      }

      if (!subscribedWatchlistTokens.has(token)) {
        watchlistTokensToAdd.push(token);
      }
    }

    if (watchlistTokensToAdd.length > 0) {
      console.log("Subscribing watchlist tokens:", watchlistTokensToAdd);

      // Group tokens by exchange type (NFO=2, BFO=4)
      const tokensByExchange = { 2: [], 4: [] };
      for (const token of watchlistTokensToAdd) {
        const symbol = tokenToSymbolMap[token];
        if (symbol) {
          const exchangeType = getExchangeTypeForSymbol(symbol);
          tokensByExchange[exchangeType].push(token);
        } else {
          // Default to NFO if symbol not found
          tokensByExchange[2].push(token);
        }
      }

      // Subscribe to each exchange separately
      for (const [exchangeType, tokens] of Object.entries(tokensByExchange)) {
        if (tokens.length === 0) continue;

        try {
          const result = await ws.fetchData({
            correlationID: "watchlist-ltp",
            action: 1,
            mode: 2,
            exchangeType: parseInt(exchangeType),
            tokens: tokens,
          });

          console.log(`Watchlist subscription result (exchange ${exchangeType}):`, result);

          tokens.forEach((token) => {
            subscribedWatchlistTokens.add(token);
          });

          console.log(`Watchlist LTP subscription success for exchange ${exchangeType}`);
        } catch (error) {
          console.error(`Watchlist subscription failed for exchange ${exchangeType}:`, error.message);
        }
      }
    }

    // --- Process explicit removals from server queue ---

    const pendingRemovals = await getPendingRemovals();
    for (const sym of pendingRemovals) {
      if (subscribedStrategySymbols.has(sym)) {
        console.log("Removing strategy symbol (explicit removal):", sym);
        removeCandleStateForSymbol(sym);
        subscribedStrategySymbols.delete(sym);
      }
    }

    // --- Add new strategy symbols ---

    for (const symbol of activeStrategySymbols) {
      if (subscribedStrategySymbols.has(symbol)) {
        continue;
      }

      let token = getTokenForSymbol(symbol);

      if (!token) {
        // Force refresh maps and retry once
        console.log(`[${symbol}] Token not found, refreshing scrip master...`);
        await buildSymbolTokenMaps();
        token = getTokenForSymbol(symbol);
      }

      if (!token) {
        console.error("No token found for strategy symbol after refresh:", symbol);
        failedSymbolCooldown[symbol] = Date.now();
        continue;
      }

      // Cooldown for symbols that recently failed (avoid spamming every 1s)
      const lastFail = failedSymbolCooldown[symbol] || 0;
      if (Date.now() - lastFail < 30 * 1000) {
        continue;
      }

      console.log("New strategy symbol detected:", symbol);

      // Report loading status to server
      reportSymbolHistoryStatus(symbol, "loading");

      initCandleStateForSymbol(symbol, token);

      let subscriptionSuccess = false;

      // Fetch historical candles
      try {
        const nowTime = Date.now();
        const lastFetchTime = lastHistoryFetchTimeBySymbol[symbol] || 0;

        if (nowTime - lastFetchTime < 30 * 1000) {
          console.log("Skipping history fetch due to cooldown for:", symbol);
        } else {
          const retryDelays = [0, 10000, 10000, 10000];
          let historicalCandles = [];
          let fetchSuccess = false;

          for (let attempt = 0; attempt < retryDelays.length; attempt++) {
            if (retryDelays[attempt] > 0) {
              console.log(`[${symbol}] History fetch retry ${attempt}/${retryDelays.length - 1} in ${retryDelays[attempt] / 1000}s...`);
              await new Promise((resolve) => setTimeout(resolve, retryDelays[attempt]));
            }

            const now = new Date();

            const toDate =
              now.getFullYear() +
              "-" +
              String(now.getMonth() + 1).padStart(2, "0") +
              "-" +
              String(now.getDate()).padStart(2, "0") +
              " " +
              String(now.getHours()).padStart(2, "0") +
              ":" +
              String(now.getMinutes()).padStart(2, "0");

            const from = new Date(now.getTime() - 10 * 24 * 60 * 60 * 1000);

            const fromDate =
              from.getFullYear() +
              "-" +
              String(from.getMonth() + 1).padStart(2, "0") +
              "-" +
              String(from.getDate()).padStart(2, "0") +
              " " +
              String(from.getHours()).padStart(2, "0") +
              ":" +
              String(from.getMinutes()).padStart(2, "0");

            const fetchResult = await fetchHistoricalCandles({
              smartApi: globalSmartApi,
              symbolToken: token,
              exchange: symbol.startsWith("SENSEX") ? "BFO" : "NFO",
              fromDate,
              toDate,
            });

            // Handle auth error — refresh session and retry
            if (fetchResult && fetchResult.authError) {
              console.log(`[${symbol}] Auth error detected, refreshing session...`);
              await refreshSession();
              continue; // Retry with fresh session
            }

            // Handle invalid token — refresh scrip master and get fresh token
            if (fetchResult && fetchResult.invalidToken) {
              console.log(`[${symbol}] Invalid token detected, refreshing scrip master...`);
              await buildSymbolTokenMaps();
              const newToken = getTokenForSymbol(symbol);
              if (newToken && newToken !== token) {
                console.log(`[${symbol}] Token updated: ${token} -> ${newToken}`);
                token = newToken;
                // Update candle state with new token
                if (candleStateBySymbol[symbol]) {
                  candleStateBySymbol[symbol].token = String(newToken);
                }
              }
              continue; // Retry with new token
            }

            historicalCandles = Array.isArray(fetchResult) ? fetchResult : [];

            if (historicalCandles.length > 0) {
              fetchSuccess = true;
              break;
            }

            console.log(`[${symbol}] History fetch attempt ${attempt + 1} returned 0 candles`);
          }

          console.log(`[${symbol}] Fetched historical candles:`, historicalCandles.length);

          if (fetchSuccess && historicalCandles.length > 0) {
            historicalCandles = historicalCandles.slice(-1200);
            await sendHistoricalCandlesToStrategy(symbol, historicalCandles);
            console.log(`[${symbol}] Historical candles sent to strategy (batch)`);
            lastHistoryFetchTimeBySymbol[symbol] = Date.now();
            reportSymbolHistoryStatus(symbol, "ready", historicalCandles.length);
          } else {
            console.log(`[${symbol}] All history fetch attempts failed or returned 0 candles`);
            reportSymbolHistoryStatus(symbol, "failed", 0);
          }
        }
      } catch (error) {
        console.error(`[${symbol}] Historical load failed:`, error.message);
      }

      // Subscribe token for tick data if not already subscribed
      if (!subscribedWatchlistTokens.has(String(token))) {
        try {
          const result = await ws.fetchData({
            correlationID: symbol,
            action: 1,
            mode: 2,
            exchangeType: getExchangeTypeForSymbol(symbol),
            tokens: [String(token)],
          });

          console.log(`[${symbol}] Subscription result:`, result);

          subscribedWatchlistTokens.add(String(token));
          subscriptionSuccess = true;
          console.log(`[${symbol}] Subscription success:`, token);
        } catch (error) {
          console.error(`[${symbol}] Subscription failed:`, error.message);
        }
      } else {
        // Token already subscribed via watchlist, reuse it
        subscriptionSuccess = true;
      }

      if (subscriptionSuccess) {
        delete failedSymbolCooldown[symbol];
        subscribedStrategySymbols.add(symbol);
        console.log(`[${symbol}] Ready for candle building, token:`, token);
      } else {
        // Clean up so it can be retried on next interval
        removeCandleStateForSymbol(symbol);
        failedSymbolCooldown[symbol] = Date.now();
        console.error(`[${symbol}] Failed to subscribe. Will retry after cooldown.`);
      }
    }
  } catch (error) {
    console.error("Subscribe to symbols failed:", error.message);
  } finally {
    isSubscriptionInProgress = false;
  }
}

function handleTick(tick) {
  const price = extractTickPrice(tick);
  const minute = extractTickMinute(tick);
  const tickToken = extractTickToken(tick);
  const tickSymbol = tokenToSymbolMap[tickToken] || null;

  if (!price || !minute) {
    console.log("Tick ignored due to missing price/time:", tick);
    return [];
  }

  sendMarketTime(minute);
  sendPriceUpdate(tickSymbol, price, minute);

  // Only build candles for active strategy symbols
  if (!tickSymbol || !candleStateBySymbol[tickSymbol]) {
    return [];
  }

  const state = candleStateBySymbol[tickSymbol];

  const cumulativeVolume = extractTickCumulativeVolume(tick);

  if (!state.lastMinute) {
    state.lastMinute = minute;
    state.volumeAtCandleStart = cumulativeVolume;

    state.currentCandle = {
      time: minute,
      open: price,
      high: price,
      low: price,
      close: price,
      volume: 0,
    };

    console.log(`[${tickSymbol}] Started first candle:`, state.currentCandle);
    return [];
  }

  if (minute === state.lastMinute) {
    if (price > state.currentCandle.high) {
      state.currentCandle.high = price;
    }

    if (price < state.currentCandle.low) {
      state.currentCandle.low = price;
    }

    state.currentCandle.close = price;

    if (cumulativeVolume !== null && state.volumeAtCandleStart !== null) {
      state.currentCandle.volume = cumulativeVolume - state.volumeAtCandleStart;
    }

    return [];
  }

  const candlesToSend = [];

  if (cumulativeVolume !== null && state.volumeAtCandleStart !== null) {
    state.currentCandle.volume = cumulativeVolume - state.volumeAtCandleStart;
  }

  const finishedCandle = { ...state.currentCandle };
  state.completedCandles.push(finishedCandle);
  candlesToSend.push({ symbol: tickSymbol, candle: finishedCandle });

  console.log(`[${tickSymbol}] Completed candle:`, finishedCandle);
  console.log(`[${tickSymbol}] Completed candles count:`, state.completedCandles.length);

  let nextMinute = getNextMinute(state.lastMinute);

  while (nextMinute !== minute) {
    const fillerCandle = {
      time: nextMinute,
      open: finishedCandle.close,
      high: finishedCandle.close,
      low: finishedCandle.close,
      close: finishedCandle.close,
      volume: 0,
    };

    state.completedCandles.push(fillerCandle);
    candlesToSend.push({ symbol: tickSymbol, candle: fillerCandle });

    console.log(`[${tickSymbol}] Filled missing candle:`, fillerCandle);

    nextMinute = getNextMinute(nextMinute);
  }

  state.lastMinute = minute;

  state.volumeAtCandleStart = cumulativeVolume;

  state.currentCandle = {
    time: minute,
    open: price,
    high: price,
    low: price,
    close: price,
    volume: 0,
  };

  console.log(`[${tickSymbol}] Started new candle:`, state.currentCandle);

  return candlesToSend;
}

// ---- Nifty50 dedicated candle logic ----

function handleNifty50Tick(tick) {
  const price = extractTickPrice(tick);
  const minute = extractTickMinute(tick);

  if (!price || !minute) return;

  if (!nifty50State.lastMinute) {
    nifty50State.lastMinute = minute;
    nifty50State.currentCandle = {
      time: minute,
      open: price,
      high: price,
      low: price,
      close: price,
      volume: 0,
    };
    console.log("[NIFTY50] Started first candle:", nifty50State.currentCandle);
    pushNifty50Update();
    return;
  }

  if (minute === nifty50State.lastMinute) {
    if (price > nifty50State.currentCandle.high) nifty50State.currentCandle.high = price;
    if (price < nifty50State.currentCandle.low) nifty50State.currentCandle.low = price;
    nifty50State.currentCandle.close = price;
    pushNifty50Update();
    return;
  }

  // Minute changed — finalize current candle
  const finishedCandle = { ...nifty50State.currentCandle };
  nifty50State.completedCandles.push(finishedCandle);

  // Fill gaps
  let nextMinute = getNextMinute(nifty50State.lastMinute);
  while (nextMinute !== minute) {
    const fillerCandle = {
      time: nextMinute,
      open: finishedCandle.close,
      high: finishedCandle.close,
      low: finishedCandle.close,
      close: finishedCandle.close,
      volume: 0,
    };
    nifty50State.completedCandles.push(fillerCandle);
    nextMinute = getNextMinute(nextMinute);
  }

  // Trim to max
  if (nifty50State.completedCandles.length > NIFTY50_MAX_CANDLES) {
    nifty50State.completedCandles = nifty50State.completedCandles.slice(-NIFTY50_MAX_CANDLES);
  }

  nifty50State.lastMinute = minute;
  nifty50State.currentCandle = {
    time: minute,
    open: price,
    high: price,
    low: price,
    close: price,
    volume: 0,
  };

  console.log("[NIFTY50] Completed candle:", finishedCandle.time, "| Total:", nifty50State.completedCandles.length);
  pushNifty50Update();
}

// Throttle: push at most once per 500ms
let _nifty50PushTimeout = null;
function pushNifty50Update() {
  if (_nifty50PushTimeout) return;
  _nifty50PushTimeout = setTimeout(() => {
    _nifty50PushTimeout = null;
    _doPushNifty50();
  }, 500);
}

function _doPushNifty50() {
  const payload = {
    completedCandles: nifty50State.completedCandles,
    currentCandle: nifty50State.currentCandle,
  };
  fetch("http://localhost:2000/nifty50-candle-update", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  }).catch(() => {});
}

async function subscribeNifty50(ws, smartApi) {
  if (nifty50State.subscribed) return;

  // Fetch historical candles for Nifty50 index
  if (!nifty50State.historyLoaded) {
    try {
      const now = new Date();
      const toDate =
        now.getFullYear() + "-" +
        String(now.getMonth() + 1).padStart(2, "0") + "-" +
        String(now.getDate()).padStart(2, "0") + " " +
        String(now.getHours()).padStart(2, "0") + ":" +
        String(now.getMinutes()).padStart(2, "0");

      const from = new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000);
      const fromDate =
        from.getFullYear() + "-" +
        String(from.getMonth() + 1).padStart(2, "0") + "-" +
        String(from.getDate()).padStart(2, "0") + " " +
        String(from.getHours()).padStart(2, "0") + ":" +
        String(from.getMinutes()).padStart(2, "0");

      const historicalCandles = await fetchHistoricalCandles({
        smartApi,
        symbolToken: NIFTY50_TOKEN,
        exchange: "NSE",
        fromDate,
        toDate,
      });

      if (Array.isArray(historicalCandles) && historicalCandles.length > 0) {
        nifty50State.completedCandles = historicalCandles.slice(-NIFTY50_MAX_CANDLES);
        nifty50State.historyLoaded = true;
        console.log("[NIFTY50] Historical candles loaded:", nifty50State.completedCandles.length);
        pushNifty50Update();
      } else {
        console.log("[NIFTY50] No historical candles returned");
      }
    } catch (error) {
      console.error("[NIFTY50] Historical fetch failed:", error.message);
    }
  }

  // Subscribe to Nifty50 tick data
  try {
    const result = await ws.fetchData({
      correlationID: "nifty50-index",
      action: 1,
      mode: 2,
      exchangeType: NIFTY50_EXCHANGE_TYPE,
      tokens: [NIFTY50_TOKEN],
    });
    console.log("[NIFTY50] Subscription result:", result);
    nifty50State.subscribed = true;
  } catch (error) {
    console.error("[NIFTY50] Subscription failed:", error.message);
  }
}

let subscribeIntervalId = null;
let isReconnecting = false;
let isRefreshingSession = false;

async function refreshSession() {
  if (isRefreshingSession) return;
  isRefreshingSession = true;
  console.log("[SESSION] Refreshing Angel One session...");
  try {
    const totp = authenticator.generate(process.env.ANGEL_TOTP_SECRET);
    const session = await globalSmartApi.generateSession(
      process.env.ANGEL_CLIENT_CODE,
      process.env.ANGEL_PASSWORD,
      totp
    );
    globalSession = session;
    console.log("[SESSION] Session refreshed successfully");
    return session;
  } catch (error) {
    console.error("[SESSION] Session refresh failed:", error.message);
    throw error;
  } finally {
    isRefreshingSession = false;
  }
}

async function connectWebSocket() {
  try {
    globalSmartApi = new SmartAPI({
      api_key: process.env.ANGEL_API_KEY,
    });

    const totp = authenticator.generate(process.env.ANGEL_TOTP_SECRET);

    const session = await globalSmartApi.generateSession(
      process.env.ANGEL_CLIENT_CODE,
      process.env.ANGEL_PASSWORD,
      totp
    );
    globalSession = session;

    console.log("Login success");

    // Start periodic session refresh every 12 hours
    if (sessionRefreshInterval) clearInterval(sessionRefreshInterval);
    sessionRefreshInterval = setInterval(() => {
      console.log("[SESSION] Periodic refresh triggered");
      refreshSession().catch((err) => console.error("[SESSION] Periodic refresh failed:", err.message));
    }, 12 * 60 * 60 * 1000);

    const ws = new WebSocketV2({
      clientcode: process.env.ANGEL_CLIENT_CODE,
      jwttoken: globalSession.data.jwtToken,
      apikey: process.env.ANGEL_API_KEY,
      feedtype: globalSession.data.feedToken,
    });

    ws.on("tick", async (tick) => {
      if (tick === "pong") {
        console.log("Heartbeat pong");
        return;
      }

      if (tick && tick.last_traded_price && tick.exchange_timestamp) {
        // Handle Nifty50 index ticks separately
        const tickToken = extractTickToken(tick);
        if (tickToken === NIFTY50_TOKEN) {
          handleNifty50Tick(tick);
          return;
        }

        const candlesToSend = handleTick(tick);

        for (const { symbol, candle } of candlesToSend) {
          console.log(`[${symbol}] Ready to send candle:`, candle);
          await sendCandleToStrategy(symbol, candle);
        }
      } else {
        console.log("Unhandled event shape:", tick);
      }
    });

    ws.on("error", (error) => {
      console.error("WebSocket error:", error);
      scheduleReconnect();
    });

    ws.on("close", (data) => {
      console.log("WebSocket closed:", data);
      scheduleReconnect();
    });

    await ws.connect();
    console.log("WebSocket connected");
    isReconnecting = false;

    // Reset subscription tracking so symbols get re-subscribed on new connection
    subscribedWatchlistTokens = new Set();
    subscribedStrategySymbols = new Set();
    nifty50State.subscribed = false;
    for (const sym in candleStateBySymbol) {
      delete candleStateBySymbol[sym];
    }

    // Subscribe Nifty50 index first (always-on)
    await subscribeNifty50(ws, globalSmartApi);

    await subscribeToSymbols(ws, globalSmartApi);

    if (subscribeIntervalId) {
      clearInterval(subscribeIntervalId);
    }

    subscribeIntervalId = setInterval(() => {
      subscribeToSymbols(ws, globalSmartApi);
    }, 1000);
  } catch (error) {
    console.error("WebSocket connect failed:", error.message || error);
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (isReconnecting) return;
  isReconnecting = true;

  if (subscribeIntervalId) {
    clearInterval(subscribeIntervalId);
    subscribeIntervalId = null;
  }

  console.log("Reconnecting in 5 seconds...");
  setTimeout(() => {
    connectWebSocket();
  }, 5000);
}

async function run() {
  try {
    await buildSymbolTokenMaps();
    await connectWebSocket();
  } catch (error) {
    console.error("Build candle failed:");
    console.error(error);
  }
}

run();