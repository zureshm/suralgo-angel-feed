require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");
const { loadScripMaster } = require("./loadScripMaster");
const { fetchHistoricalCandles } = require("./fetchHistoricalCandles");

const STRATEGY_URL = "http://localhost:4000/evaluate";

// Per-symbol candle building state (max 2 active strategy symbols)
// candleStateBySymbol[symbol] = {
//   currentCandle: null,
//   lastMinute: null,
//   completedCandles: [],
//   token: null,
// }
const candleStateBySymbol = {};

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
    console.log("Refreshing symbol-token maps (stale)...");
    await buildSymbolTokenMaps();
  }
}

// Read the active strategy symbols from local API server (array, max 2)
async function getActiveStrategySymbols() {
  try {
    const response = await fetch("http://localhost:2000/active-strategy-symbols");
    const data = await response.json();
    return Array.isArray(data.symbols) ? data.symbols : [];
  } catch (error) {
    console.error("Get active strategy symbols failed:", error.message);
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

    return data.symbols;
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

      try {
        const result = await ws.fetchData({
          correlationID: "watchlist-ltp",
          action: 1,
          mode: 1,
          exchangeType: 2,
          tokens: watchlistTokensToAdd,
        });

        console.log("Watchlist subscription result:", result);

        watchlistTokensToAdd.forEach((token) => {
          subscribedWatchlistTokens.add(token);
        });

        console.log("Watchlist LTP subscription success");
      } catch (error) {
        console.error("Watchlist subscription failed:", error.message);
      }
    }

    // --- Remove strategy symbols no longer active ---

    for (const sym of subscribedStrategySymbols) {
      if (!activeStrategySymbols.includes(sym)) {
        console.log("Removing inactive strategy symbol:", sym);
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

            const from = new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000);

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

            historicalCandles = await fetchHistoricalCandles({
              smartApi,
              symbolToken: token,
              fromDate,
              toDate,
            });

            if (historicalCandles.length > 0) {
              fetchSuccess = true;
              break;
            }

            console.log(`[${symbol}] History fetch attempt ${attempt + 1} returned 0 candles`);
          }

          console.log(`[${symbol}] Fetched historical candles:`, historicalCandles.length);

          if (fetchSuccess && historicalCandles.length > 0) {
            historicalCandles = historicalCandles.slice(-500);
            await sendHistoricalCandlesToStrategy(symbol, historicalCandles);
            console.log(`[${symbol}] Historical candles sent to strategy (batch)`);
            lastHistoryFetchTimeBySymbol[symbol] = Date.now();
          } else {
            console.log(`[${symbol}] All history fetch attempts failed or returned 0 candles`);
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
            mode: 1,
            exchangeType: 2,
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

  if (!state.lastMinute) {
    state.lastMinute = minute;

    state.currentCandle = {
      time: minute,
      open: price,
      high: price,
      low: price,
      close: price,
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

    return [];
  }

  const candlesToSend = [];

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
    };

    state.completedCandles.push(fillerCandle);
    candlesToSend.push({ symbol: tickSymbol, candle: fillerCandle });

    console.log(`[${tickSymbol}] Filled missing candle:`, fillerCandle);

    nextMinute = getNextMinute(nextMinute);
  }

  state.lastMinute = minute;

  state.currentCandle = {
    time: minute,
    open: price,
    high: price,
    low: price,
    close: price,
  };

  console.log(`[${tickSymbol}] Started new candle:`, state.currentCandle);

  return candlesToSend;
}

let subscribeIntervalId = null;
let isReconnecting = false;

async function connectWebSocket() {
  try {
    const smartApi = new SmartAPI({
      api_key: process.env.ANGEL_API_KEY,
    });

    const totp = authenticator.generate(process.env.ANGEL_TOTP_SECRET);

    const session = await smartApi.generateSession(
      process.env.ANGEL_CLIENT_CODE,
      process.env.ANGEL_PASSWORD,
      totp
    );

    console.log("Login success");

    const ws = new WebSocketV2({
      clientcode: process.env.ANGEL_CLIENT_CODE,
      jwttoken: session.data.jwtToken,
      apikey: process.env.ANGEL_API_KEY,
      feedtype: session.data.feedToken,
    });

    ws.on("tick", async (tick) => {
      if (tick === "pong") {
        console.log("Heartbeat pong");
        return;
      }

      if (tick && tick.last_traded_price && tick.exchange_timestamp) {
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
    for (const sym in candleStateBySymbol) {
      delete candleStateBySymbol[sym];
    }

    await subscribeToSymbols(ws, smartApi);

    if (subscribeIntervalId) {
      clearInterval(subscribeIntervalId);
    }

    subscribeIntervalId = setInterval(() => {
      subscribeToSymbols(ws, smartApi);
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