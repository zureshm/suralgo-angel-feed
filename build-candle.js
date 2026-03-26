require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");
const { loadScripMaster } = require("./loadScripMaster");

const STRATEGY_URL = "http://localhost:4000/evaluate";


let currentCandle = null;
let lastMinute = null;
let completedCandles = [];

// Track what symbol/token is currently subscribed
let currentSubscribedSymbol = null;
let currentSubscribedToken = null;

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

// Find token for the currently active symbol from Angel scrip master
async function getTokenForSymbol(symbol) {
  try {
    const rows = await loadScripMaster();

    const match = rows.find((item) => item.symbol === symbol);

    if (!match) {
      console.error("Token not found for symbol:", symbol);
      return null;
    }

    return match.token;
  } catch (error) {
    console.error("Get token for symbol failed:", error.message);
    return null;
  }
}

// Subscribe to a symbol only if it changed
async function subscribeToActiveSymbol(ws) {
  const activeSymbol = await getActiveSymbol();

 // No symbol selected yet from frontend
// This is a normal waiting state, not an error
if (!activeSymbol) {
  return;
}

  if (activeSymbol === currentSubscribedSymbol) {
    return;
  }

  const activeToken = await getTokenForSymbol(activeSymbol);

  if (!activeToken) {
    console.error("No token found for active symbol:", activeSymbol);
    return;
  }

  currentSubscribedSymbol = activeSymbol;
  currentSubscribedToken = String(activeToken);

  // Reset candle state when symbol changes
  currentCandle = null;
  lastMinute = null;
  completedCandles = [];

  console.log("Subscribing to active symbol:", currentSubscribedSymbol);
  console.log("Using token:", currentSubscribedToken);

  ws.fetchData({
    correlationID: currentSubscribedSymbol,
    action: 1,
    mode: 2,
    exchangeType: 2,
    tokens: [currentSubscribedToken],
  });
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

// Send completed candle to strategy for the currently active symbol
async function sendCandleToStrategy(candle) {
  try {
    const activeSymbol = await getActiveSymbol();

    if (!activeSymbol) {
      console.error("No active symbol found. Candle not sent to strategy.");
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

    const data = await response.json();

    if (response.ok) {
      console.log(
        `Sent candle to strategy: ${candle.time} | ${activeSymbol} | status ${response.status} | signal ${data.signal}`
      );
      console.log("Strategy response:", data);
      return;
    }

    console.error(`Strategy engine error: status ${response.status}`);
    console.error("Strategy error response:", data);
  } catch (error) {
    console.error("Send candle failed:", error.message);
  }
}

function handleTick(tick) {
  const rawPrice = Number(tick.last_traded_price);
  const price = rawPrice / 100;

  const minute = formatMinute(tick.exchange_timestamp);
  // Update market time on every tick
  sendMarketTime(minute);

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

    // Continuously check if active symbol changed and resubscribe
    setInterval(() => {
      subscribeToActiveSymbol(ws);
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

        // Subscribe using the currently active symbol and its token
    const activeSymbol = await getActiveSymbol();

    if (!activeSymbol) {
      console.error("No active symbol found. WebSocket subscription not started.");
      return;
    }

    const activeToken = await getTokenForSymbol(activeSymbol);

    if (!activeToken) {
      console.error("No token found for active symbol:", activeSymbol);
      return;
    }

    ws.fetchData({
      correlationID: activeSymbol,
      action: 1,
      mode: 2,
      exchangeType: 2,
      tokens: [String(activeToken)],
    });
  } catch (error) {
    console.error("Build candle failed:");
    console.error(error);
  }
}

run();