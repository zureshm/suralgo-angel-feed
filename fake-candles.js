// fake-candles.js
// Purpose: Read candles from a CSV file and feed them to:
//   1. Strategy engine (port 4000) via POST /evaluate — so it produces BUY/SELL/WAIT signals
//   2. Angel-feed server (port 2000) via POST /price-update — so frontend gets LTP data
//
// Usage:
//   node fake-candles.js NIFTY28MAR2524500CE
//   node fake-candles.js NIFTY28MAR2524500CE --speed 500
//   node fake-candles.js NIFTY28MAR2524500CE --speed 0        (instant, no delay)
//
// Prerequisites:
//   - angel-feed server.js running on port 2000 (just `node server.js`, no build-candle needed)
//   - strategy1 server.js running on port 4000
//
// The first 30 candles are sent as history batch, then remaining candles drip one per second.

const fs = require("fs");
const path = require("path");

const STRATEGY_URL = "http://localhost:4000/evaluate";
const FEED_URL = "http://localhost:2000";

const CSV_PATH = path.join(__dirname, "temp_feed.csv");

// Parse CLI args
const symbol = process.argv[2];
if (!symbol) {
  console.error("Usage: node fake-candles.js <SYMBOL> [--speed <ms>]");
  console.error("Example: node fake-candles.js NIFTY28MAR2524500CE");
  process.exit(1);
}

let speed = 1000; // ms between live candles
const speedIdx = process.argv.indexOf("--speed");
if (speedIdx !== -1 && process.argv[speedIdx + 1] !== undefined) {
  speed = Number(process.argv[speedIdx + 1]);
}

// Today's date prefix for candle times (strategy expects "YYYY-MM-DD HH:MM")
const now = new Date();
const datePrefix =
  now.getFullYear() +
  "-" +
  String(now.getMonth() + 1).padStart(2, "0") +
  "-" +
  String(now.getDate()).padStart(2, "0");

function parseCSV(filePath) {
  const raw = fs.readFileSync(filePath, "utf-8");
  const lines = raw.trim().split("\n");

  // Skip header
  const candles = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].trim().split(",");
    if (parts.length < 5) continue;

    candles.push({
      time: datePrefix + " " + parts[0].trim(),
      open: Number(parts[1]),
      high: Number(parts[2]),
      low: Number(parts[3]),
      close: Number(parts[4]),
    });
  }

  return candles;
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function setActiveSymbol() {
  try {
    await postJSON(`${FEED_URL}/active-symbol`, { symbol });
    console.log(`Active symbol set: ${symbol}`);
  } catch (e) {
    console.warn("Could not set active symbol on port 2000 (server running?):", e.message);
  }
}

async function sendMarketTime(time) {
  try {
    await postJSON(`${FEED_URL}/market-time`, { marketTime: time });
  } catch {
    // port 2000 not running, skip
  }
}

async function sendPriceUpdate(ltp, marketTime) {
  try {
    await postJSON(`${FEED_URL}/price-update`, { symbol, ltp, marketTime });
  } catch {
    // port 2000 not running, skip
  }
}

async function sendHistoryToStrategy(candles) {
  try {
    const data = await postJSON(STRATEGY_URL, {
      symbol,
      candles,
      mode: "history",
    });
    console.log(`History sent: ${candles.length} candles | Signal: ${data.signal} | Status: ${data.engineStatus}`);
    return data;
  } catch (e) {
    console.error("Strategy server unreachable:", e.message);
    return null;
  }
}

async function sendLiveCandleToStrategy(candle) {
  try {
    const data = await postJSON(STRATEGY_URL, {
      symbol,
      candle,
      mode: "live",
    });
    return data;
  } catch (e) {
    console.error("Strategy server unreachable:", e.message);
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run() {
  const candles = parseCSV(CSV_PATH);
  console.log(`Loaded ${candles.length} candles from CSV`);
  console.log(`Symbol: ${symbol}`);
  console.log(`Speed: ${speed}ms per candle`);
  console.log(`Time range: ${candles[0].time} → ${candles[candles.length - 1].time}`);
  console.log("---");

  // Set active symbol on port 2000
  await setActiveSymbol();

  // Send first 30 candles as history batch (EMA needs ~21 candles to warm up)
  const HISTORY_COUNT = 30;
  const historyCandles = candles.slice(0, HISTORY_COUNT);
  const liveCandles = candles.slice(HISTORY_COUNT);

  await sendHistoryToStrategy(historyCandles);

  // Also update LTP with last history candle
  const lastHistory = historyCandles[historyCandles.length - 1];
  await sendPriceUpdate(lastHistory.close, lastHistory.time);
  await sendMarketTime(lastHistory.time);

  console.log(`\nStarting live feed: ${liveCandles.length} candles remaining\n`);

  // Drip-feed remaining candles one at a time
  for (let i = 0; i < liveCandles.length; i++) {
    const candle = liveCandles[i];

    const data = await sendLiveCandleToStrategy(candle);

    // Update LTP and market time on port 2000
    await sendPriceUpdate(candle.close, candle.time);
    await sendMarketTime(candle.time);

    const signal = data ? data.signal : "???";
    const marker = signal === "BUY" ? " ◄◄◄ BUY" : signal === "SELL" ? " ◄◄◄ SELL" : "";
    console.log(
      `[${i + 1}/${liveCandles.length}] ${candle.time} | O:${candle.open} H:${candle.high} L:${candle.low} C:${candle.close} | ${signal}${marker}`
    );

    if (speed > 0 && i < liveCandles.length - 1) {
      await sleep(speed);
    }
  }

  console.log("\n--- Feed complete ---");
}

run();
