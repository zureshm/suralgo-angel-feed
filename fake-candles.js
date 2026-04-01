// fake-candles.js
// Purpose: Read candles from CSV files and feed them to:
//   1. Strategy engine (port 4000) via POST /evaluate — so it produces BUY/SELL/WAIT signals
//   2. Angel-feed server (port 2000) via POST /price-update — so frontend gets LTP data
//
// Supports 1 or 2 symbols simultaneously (matching the new dual-symbol architecture).
//
// Usage (single symbol — uses temp_feed.csv):
//   node fake-candles.js NIFTY02APR2524500CE
//   node fake-candles.js NIFTY02APR2524500CE --speed 500
//
// Usage (dual symbol — uses temp_feed.csv + temp_feed2.csv):
//   node fake-candles.js NIFTY02APR2524500CE NIFTY02APR2524500PE
//   node fake-candles.js NIFTY02APR2524500CE NIFTY02APR2524500PE --speed 500
//
// Prerequisites:
//   - angel-feed server.js running on port 2000 (just `node server.js`, no build-candle needed)
//   - strategy1 server.js running on port 4000
//
// The first 30 candles per symbol are sent as history batch, then remaining candles drip interleaved.

const fs = require("fs");
const path = require("path");

const STRATEGY_URL = "http://localhost:4000/evaluate";
const FEED_URL = "http://localhost:2000";

const CSV_PATH_1 = path.join(__dirname, "CE.csv");
const CSV_PATH_2 = path.join(__dirname, "PE.csv");

// Parse CLI args — first 1-2 non-flag args are symbols
const args = process.argv.slice(2);
const symbols = [];
for (const arg of args) {
  if (arg.startsWith("--")) break;
  symbols.push(arg);
}

if (symbols.length === 0) {
  console.error("Usage: node fake-candles.js <SYMBOL1> [SYMBOL2] [--speed <ms>]");
  console.error("Example (single): node fake-candles.js NIFTY02APR2524500CE");
  console.error("Example (dual):   node fake-candles.js NIFTY02APR2524500CE NIFTY02APR2524500PE");
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

async function addActiveStrategySymbol(sym) {
  try {
    await postJSON(`${FEED_URL}/active-strategy-symbols`, { symbol: sym });
    console.log(`Active strategy symbol added: ${sym}`);
  } catch (e) {
    console.warn("Could not add active strategy symbol (server running?):", e.message);
  }
}

async function sendMarketTime(time) {
  try {
    await postJSON(`${FEED_URL}/market-time`, { marketTime: time });
  } catch {
    // port 2000 not running, skip
  }
}

async function sendPriceUpdate(sym, ltp, marketTime) {
  try {
    await postJSON(`${FEED_URL}/price-update`, { symbol: sym, ltp, marketTime });
  } catch {
    // port 2000 not running, skip
  }
}

async function sendHistoryToStrategy(sym, candles) {
  try {
    const data = await postJSON(STRATEGY_URL, {
      symbol: sym,
      candles,
      mode: "history",
    });
    console.log(`[${sym}] History sent: ${candles.length} candles | Signal: ${data.signal} | Status: ${data.engineStatus}`);
    return data;
  } catch (e) {
    console.error(`[${sym}] Strategy server unreachable:`, e.message);
    return null;
  }
}

async function sendLiveCandleToStrategy(sym, candle) {
  try {
    const data = await postJSON(STRATEGY_URL, {
      symbol: sym,
      candle,
      mode: "live",
    });
    return data;
  } catch (e) {
    console.error(`[${sym}] Strategy server unreachable:`, e.message);
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function signalMarker(signal) {
  if (signal === "BUY") return " ◄◄◄ BUY";
  if (signal === "SELL") return " ◄◄◄ SELL";
  return "";
}

async function run() {
  // Build feed list: [{symbol, csvPath}]
  const feeds = [{ symbol: symbols[0], csvPath: CSV_PATH_1 }];
  if (symbols.length >= 2) {
    feeds.push({ symbol: symbols[1], csvPath: CSV_PATH_2 });
  }

  const HISTORY_COUNT = 30;

  // Load and parse all CSVs
  const feedData = feeds.map((f) => {
    const candles = parseCSV(f.csvPath);
    console.log(`[${f.symbol}] Loaded ${candles.length} candles from ${path.basename(f.csvPath)}`);
    return {
      symbol: f.symbol,
      history: candles.slice(0, HISTORY_COUNT),
      live: candles.slice(HISTORY_COUNT),
    };
  });

  console.log(`Speed: ${speed}ms per candle tick`);
  console.log("---");

  // Register active strategy symbols on port 2000
  for (const f of feedData) {
    await addActiveStrategySymbol(f.symbol);
  }

  // Send history batch for each symbol
  for (const f of feedData) {
    await sendHistoryToStrategy(f.symbol, f.history);
    const last = f.history[f.history.length - 1];
    await sendPriceUpdate(f.symbol, last.close, last.time);
  }

  // Update market time with last history candle time
  const lastHistTime = feedData[0].history[feedData[0].history.length - 1].time;
  await sendMarketTime(lastHistTime);

  const maxLive = Math.max(...feedData.map((f) => f.live.length));
  console.log(`\nStarting live feed: ${maxLive} candle ticks\n`);

  // Drip-feed live candles interleaved
  for (let i = 0; i < maxLive; i++) {
    for (const f of feedData) {
      if (i >= f.live.length) continue;

      const candle = f.live[i];
      const data = await sendLiveCandleToStrategy(f.symbol, candle);

      await sendPriceUpdate(f.symbol, candle.close, candle.time);

      const signal = data ? data.signal : "???";
      console.log(
        `[${f.symbol}] [${i + 1}/${f.live.length}] ${candle.time} | O:${candle.open} H:${candle.high} L:${candle.low} C:${candle.close} | ${signal}${signalMarker(signal)}`
      );
    }

    // Update market time once per tick (all symbols share same minute)
    const firstCandle = feedData[0].live[Math.min(i, feedData[0].live.length - 1)];
    await sendMarketTime(firstCandle.time);

    if (speed > 0 && i < maxLive - 1) {
      await sleep(speed);
    }
  }

  console.log("\n--- Feed complete ---");
}

run();
