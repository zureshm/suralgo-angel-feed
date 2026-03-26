require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");
const { loadScripMaster } = require("./loadScripMaster");

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

// Subscribe all watchlist symbols for LTP,
// and also keep active symbol ready for candle building
async function subscribeToSymbols(ws) {
  // If one subscribe process is already running, skip this interval turn
  if (isSubscriptionInProgress) {
    return;
  }

  isSubscriptionInProgress = true;

  try {
    const activeSymbol = await getActiveSymbol();
    const watchlistSymbols = await getWatchlistSymbols();

    // Subscribe any new watchlist symbols for LTP updates
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

      ws.fetchData({
        correlationID: "watchlist-ltp",
        action: 1,
        mode: 2,
        exchangeType: 2,
        tokens: watchlistTokensToAdd,
      });

      watchlistTokensToAdd.forEach((token) => {
        subscribedWatchlistTokens.add(token);
      });

      console.log("Watchlist LTP subscription request sent");
    }

    // No active symbol selected yet from frontend
    if (!activeSymbol) {
      return;
    }

    // Same active symbol is already set for candle building
    if (activeSymbol === currentSubscribedSymbol) {
      return;
    }

    const activeToken = getTokenForSymbol(activeSymbol);

    if (!activeToken) {
      console.error("No token found for active symbol:", activeSymbol);
      return;
    }

    console.log("New active symbol detected:", activeSymbol);

    currentSubscribedSymbol = activeSymbol;
    currentSubscribedToken = String(activeToken);

    // Reset candle state when active symbol changes
    currentCandle = null;
    lastMinute = null;
    completedCandles = [];

    // If active symbol token was not yet part of watchlist subscription,
    // subscribe it also so ticks definitely arrive
    if (!subscribedWatchlistTokens.has(currentSubscribedToken)) {
      ws.fetchData({
        correlationID: currentSubscribedSymbol,
        action: 1,
        mode: 2,
        exchangeType: 2,
        tokens: [currentSubscribedToken],
      });

      subscribedWatchlistTokens.add(currentSubscribedToken);
    }

    console.log("Active symbol ready for candle building:", currentSubscribedSymbol);
    console.log("Using token:", currentSubscribedToken);
  } catch (error) {
    console.error("Subscribe to symbols failed:", error.message);
  } finally {
    // Release lock so next interval can run
    isSubscriptionInProgress = false;
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
// This lets port 2000 store LTP per symbol for watchlist polling
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

    // Frontend has not selected symbol yet.
    // So do not send candle to strategy.
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
    // Strategy server may be down or not started yet.
    // Worker must continue running and try again on next candle.
    console.error("Strategy server unreachable. Candle send skipped.");
    console.error(error.message);
  }
}

function handleTick(tick) {
  const rawPrice = Number(tick.last_traded_price);
  const price = rawPrice / 100;

  const minute = formatMinute(tick.exchange_timestamp);
  const tickToken = String(tick.token || "");
  const tickSymbol = tokenToSymbolMap[tickToken] || null;

  // Update market time on every tick
  sendMarketTime(minute);

  // Update latest live price for whichever symbol produced this tick
  sendPriceUpdate(tickSymbol, price, minute);

  // Only build candles for the current active symbol
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

    console.log("Updating candle:", currentCandle);
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

async function run() {
  try {
    // Build symbol/token maps once at startup
    await buildSymbolTokenMaps();

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

    await ws.connect();
    console.log("WebSocket connected");

    // Keep worker alive and keep checking for symbol/watchlist from frontend.
    setInterval(() => {
      subscribeToSymbols(ws);
    }, 1000);

    ws.on("tick", async (tick) => {
      if (tick && tick.last_traded_price && tick.exchange_timestamp) {
        const candlesToSend = handleTick(tick);

        for (const candle of candlesToSend) {
          console.log("Ready to send candle:", candle);
          await sendCandleToStrategy(candle);
        }
      } else {
        console.log("Other message:", tick);
      }
    });
  } catch (error) {
    console.error("Build candle failed:");
    console.error(error);
  }
}

run();