require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");

let currentCandle = null;
let lastMinute = null;
let completedCandles = [];

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

async function sendCandleToStrategy(candle) {
  try {
    const response = await fetch("http://localhost:4000/evaluate", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: "NIFTY30MAR2623500CE",
        candle,
        }), 
    });

    const data = await response.json();
    console.log("Strategy response:", data);
  } catch (error) {
    console.error("Send candle failed:", error.message);
  }
}

function handleTick(tick) {
  const rawPrice = Number(tick.last_traded_price);
  const price = rawPrice / 100;

  const minute = formatMinute(tick.exchange_timestamp);

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

    ws.fetchData({
      correlationID: "NIFTY30MAR2623500CE",
      action: 1,
      mode: 2,
      exchangeType: 2,
      tokens: ["54518"],
    });
  } catch (error) {
    console.error("Build candle failed:");
    console.error(error);
  }
}

run();