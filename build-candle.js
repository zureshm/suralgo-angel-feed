require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");
const { loadScripMaster } = require("./loadScripMaster");
const { fetchHistoricalCandles } = require("./fetchHistoricalCandles");

const STRATEGY_URL = "http://localhost:4000/evaluate";

let currentCandle = null;
let lastMinute = null;
let completedCandles = [];

// Track what symbol/token is currently used for candle building
let currentSubscribedSymbol = null;
let currentSubscribedToken = null;

// Track all watchlist token subscriptions for LTP
let subscribedWatchlistTokens = new Set();

// Keep symbol <-> token maps in memory
let symbolToTokenMap = {};
let tokenToSymbolMap = {};
let lastHistoryFetchTimeBySymbol = {};

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

// Load scrip master once and build fast lookup maps
async function buildSymbolTokenMaps() {
  try {
    const rows = await loadScripMaster();

    rows.forEach((item) => {
      const symbol = item.symbol;
      const token = String(item.token);

      symbolToTokenMap[symbol] = token;
      tokenToSymbolMap[token] = symbol;
    });

    console.log("Symbol-token maps ready");
  } catch (error) {
    console.error("Build symbol-token maps failed:", error.message);
  }
}

// Read the currently active symbol from local API server
async function getActiveSymbol() {
  try {
    const response = await fetch("http://localhost:2000/active-symbol");
    const data = await response.json();
    return data.activeSymbol || null;
  } catch (error) {
    console.error("Get active symbol failed:", error.message);
    return null;
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

// Send completed candle to strategy for the currently active symbol
async function sendCandleToStrategy(candle) {
  try {
    const activeSymbol = await getActiveSymbol();

    if (!activeSymbol) {
      console.log("No active symbol yet. Skipping candle send.");
      return;
    }

    const response = await fetch(STRATEGY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: activeSymbol,
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
      `Sent candle to strategy: ${candle.time} | ${activeSymbol} | signal ${data.signal}`
    );
  } catch (error) {
    console.error("Strategy server unreachable. Candle send skipped.");
    console.error(error.message);
  }
}

async function sendHistoricalCandlesToStrategy(candles) {
  try {
    const activeSymbol = await getActiveSymbol();

    if (!activeSymbol) {
      console.log("No active symbol yet. Skipping history send.");
      return;
    }

    const response = await fetch(STRATEGY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: activeSymbol,
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
      `Sent historical candles: ${candles.length} | ${activeSymbol} | signal ${data.signal}`
    );
  } catch (error) {
    console.error("Strategy server unreachable (history).");
    console.error(error.message);
  }
}

function resetCandleStateForNewActiveSymbol() {
  currentCandle = null;
  lastMinute = null;
  completedCandles = [];
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
// and also keep active symbol ready for candle building
async function subscribeToSymbols(ws, smartApi) {
  if (isSubscriptionInProgress) {
    return;
  }

  isSubscriptionInProgress = true;

  try {
    const activeSymbol = await getActiveSymbol();
    const watchlistSymbols = await getWatchlistSymbols();

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

    if (!activeSymbol) {
      return;
    }

    if (activeSymbol === currentSubscribedSymbol) {
      return;
    }

    const activeToken = getTokenForSymbol(activeSymbol);

    if (!activeToken) {
      console.error("No token found for active symbol:", activeSymbol);
      return;
    }

    console.log("New active symbol detected:", activeSymbol);

    currentSubscribedToken = String(activeToken);

    resetCandleStateForNewActiveSymbol();

    try {
      const nowTime = Date.now();
      const lastFetchTime = lastHistoryFetchTimeBySymbol[activeSymbol] || 0;

      if (nowTime - lastFetchTime < 30 * 1000) {
        console.log("Skipping history fetch due to cooldown for:", activeSymbol);
      } else {
        const retryDelays = [0, 2000, 5000, 10000];
        let historicalCandles = [];
        let fetchSuccess = false;

        for (let attempt = 0; attempt < retryDelays.length; attempt++) {
          if (retryDelays[attempt] > 0) {
            console.log(`History fetch retry ${attempt}/${retryDelays.length - 1} in ${retryDelays[attempt] / 1000}s...`);
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
            symbolToken: activeToken,
            fromDate,
            toDate,
          });

          if (historicalCandles.length > 0) {
            fetchSuccess = true;
            break;
          }

          console.log(`History fetch attempt ${attempt + 1} returned 0 candles`);
        }

        console.log("Fetched historical candles:", historicalCandles.length);

        if (fetchSuccess && historicalCandles.length > 0) {
          historicalCandles = historicalCandles.slice(-150);
          await sendHistoricalCandlesToStrategy(historicalCandles);
          console.log("Historical candles sent to strategy (batch)");
          lastHistoryFetchTimeBySymbol[activeSymbol] = Date.now();
        } else {
          console.log("All history fetch attempts failed or returned 0 candles");
        }
      }
    } catch (error) {
      console.error("Historical load failed:", error.message);
    }

    currentSubscribedSymbol = activeSymbol;

    if (!subscribedWatchlistTokens.has(currentSubscribedToken)) {
      try {
        const result = await ws.fetchData({
          correlationID: currentSubscribedSymbol,
          action: 1,
          mode: 1,
          exchangeType: 2,
          tokens: [currentSubscribedToken],
        });

        console.log("Active symbol subscription result:", result);

        subscribedWatchlistTokens.add(currentSubscribedToken);
        console.log("Active symbol subscription success:", currentSubscribedToken);
      } catch (error) {
        console.error("Active symbol subscription failed:", error.message);
      }
    }

    console.log("Active symbol ready for candle building:", currentSubscribedSymbol);
    console.log("Using token:", currentSubscribedToken);
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

  if (!tickSymbol || tickSymbol !== currentSubscribedSymbol) {
    return [];
  }

  if (!lastMinute) {
    lastMinute = minute;

    currentCandle = {
      time: minute,
      open: price,
      high: price,
      low: price,
      close: price,
    };

    console.log("Started first candle:", currentCandle);
    return [];
  }

  if (minute === lastMinute) {
    if (price > currentCandle.high) {
      currentCandle.high = price;
    }

    if (price < currentCandle.low) {
      currentCandle.low = price;
    }

    currentCandle.close = price;

    return [];
  }

  const candlesToSend = [];

  const finishedCandle = { ...currentCandle };
  completedCandles.push(finishedCandle);
  candlesToSend.push(finishedCandle);

  console.log("Completed candle:", finishedCandle);
  console.log("Completed candles count:", completedCandles.length);

  let nextMinute = getNextMinute(lastMinute);

  while (nextMinute !== minute) {
    const fillerCandle = {
      time: nextMinute,
      open: finishedCandle.close,
      high: finishedCandle.close,
      low: finishedCandle.close,
      close: finishedCandle.close,
    };

    completedCandles.push(fillerCandle);
    candlesToSend.push(fillerCandle);

    console.log("Filled missing candle:", fillerCandle);

    nextMinute = getNextMinute(nextMinute);
  }

  lastMinute = minute;

  currentCandle = {
    time: minute,
    open: price,
    high: price,
    low: price,
    close: price,
  };

  console.log("Started new candle:", currentCandle);

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

        for (const candle of candlesToSend) {
          console.log("Ready to send candle:", candle);
          await sendCandleToStrategy(candle);
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
    currentSubscribedSymbol = null;
    currentSubscribedToken = null;

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