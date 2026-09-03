// server.js
// Purpose:
// 1. Start a small API server on port 2000
// 2. Load Angel scrip master once on startup
// 3. Expose /watchlist?q=... for frontend symbol search
// 4. Expose /prices?symbols=... for frontend LTP polling
// 5. Store latest live price per symbol from build-candle worker
// 6. Store full watchlist symbols for multi-symbol LTP subscription
// 7. WebSocket server for live Nifty50 candle streaming to frontend

const http = require("http");
const express = require("express");
const cors = require("cors");
const { WebSocketServer } = require("ws");
const { loadScripMaster, filterNiftyOptions } = require("./loadScripMaster");

const app = express();
const allowedOrigins = [
  "http://localhost:3000",
  "http://localhost:4200",
  "http://209.38.126.3:3000",
  "http://209.38.126.3:4200",
  "http://144.126.255.14:3000",
  "http://144.126.255.14:4200",
  "https://suralgo.duckdns.org",
  "https://sumalgo.duckdns.org"
];

app.use(cors({
  origin: function (origin, callback) {
    // allow requests with no origin (like Postman, curl)
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    } else {
      return callback(new Error("Not allowed by CORS"));
    }
  },
  credentials: true
}));
app.use(express.json());

const PORT = 2000;

// ---- Log capture system ----
const MAX_LOG_LINES = 500;
const serverLogs = [];
const candleLogs = [];

function pushLog(buffer, line) {
  buffer.push(line);
  if (buffer.length > MAX_LOG_LINES) buffer.shift();
}

const _origLog = console.log;
const _origError = console.error;

console.log = (...args) => {
  _origLog(...args);
  const line = args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' ');
  pushLog(serverLogs, `[LOG] ${new Date().toLocaleTimeString()} ${line}`);
};

console.error = (...args) => {
  _origError(...args);
  const line = args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' ');
  pushLog(serverLogs, `[ERR] ${new Date().toLocaleTimeString()} ${line}`);
};

let allOptionRows = [];

// Store latest market time (updated by build-candle)
let latestMarketTime = null;

// Store the currently selected symbol for live flow (DEPRECATED — use activeStrategySymbols)
let activeSymbol = null;

// Active strategy symbols (max 2 simultaneous, e.g. one CE + one PE)
const MAX_ACTIVE_STRATEGY_SYMBOLS = 8;

let activeStrategySymbols = [];

// Queue of symbols explicitly removed by frontend — build-candle.js drains this
let pendingSymbolRemovals = [];

// Store full watchlist symbols sent by frontend
// Example:
// [
//   "NIFTY07APR2624500CE",
//   "NIFTY29DEC2623000PE"
// ]
let watchlistSymbols = [];

// Store latest price by symbol
// Example:
// {
//   NIFTY07APR2624500CE: {
//     ltp: 58.8,
//     marketTime: "2026-03-26 09:21"
//   }
// }
const latestPricesBySymbol = {};

// Convert Angel scrip master row into frontend watchlist shape
function toWatchlistItem(item) {
  return {
    symbol: item.symbol,
    token: item.token,
    ltp: null,
  };
}

app.get("/", (req, res) => {
  res.send("Angel symbol search server running");
});

// Return the currently selected active symbol (DEPRECATED — use /active-strategy-symbols)
app.get("/active-symbol", (req, res) => {
  res.json({
    activeSymbol: activeSymbol,
  });
});

// Set the active symbol (DEPRECATED — use /active-strategy-symbols)
app.post("/active-symbol", (req, res) => {
  const { symbol } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  activeSymbol = symbol;

  console.log("Active symbol updated:", activeSymbol);

  res.json({
    message: "active symbol set",
    activeSymbol,
  });
});

// ---- Active Strategy Symbols (max 2) ----

// Return the active strategy symbols array
app.get("/active-strategy-symbols", (req, res) => {
  res.json({
    symbols: activeStrategySymbols,
  });
});

// Validate that a symbol looks like a real NIFTY/SENSEX option
// NIFTY:    NIFTY26MAY2624000CE  (DDMMMYY + strike + CE/PE)
// SENSEX monthly: SENSEX23OCT66400CE  (YY + MMM + strike + CE/PE)
// SENSEX weekly:  SENSEX23N0366400CE  (YY + month-letter + DD + strike + CE/PE)
function isValidOptionSymbol(sym) {
  return /^NIFTY\d{2}[A-Z]{3}\d{2}\d+(CE|PE)$/.test(sym) ||
         /^SENSEX\d{2}[A-Z]{3}\d+(CE|PE)$/.test(sym) ||
         /^SENSEX\d{2}[1-9OND]\d{2}\d+(CE|PE)$/.test(sym);
}

// Add a symbol to active strategy symbols (max 2)
app.post("/active-strategy-symbols", (req, res) => {
  const { symbol } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  const angelSymbol = String(symbol).trim();

  if (!isValidOptionSymbol(angelSymbol)) {
    return res.status(400).json({ message: "invalid symbol format" });
  }

  // Already present
  if (activeStrategySymbols.includes(angelSymbol)) {
    return res.json({
      message: "symbol already active",
      symbols: activeStrategySymbols,
    });
  }

  if (activeStrategySymbols.length >= MAX_ACTIVE_STRATEGY_SYMBOLS) {
    return res.status(400).json({
      message: `max ${MAX_ACTIVE_STRATEGY_SYMBOLS} active strategy symbols allowed`,
      symbols: activeStrategySymbols,
    });
  }

  activeStrategySymbols.push(angelSymbol);

  // Keep legacy activeSymbol in sync (last added)
  activeSymbol = angelSymbol;

  console.log("Active strategy symbols updated:", activeStrategySymbols);

  res.json({
    message: "symbol added",
    symbols: activeStrategySymbols,
  });
});

// Remove a symbol from active strategy symbols
app.delete("/active-strategy-symbols", (req, res) => {
  const { symbol } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  const angelSymbol = String(symbol).trim();

  activeStrategySymbols = activeStrategySymbols.filter((s) => s !== angelSymbol);

  // Queue for build-candle.js to pick up
  pendingSymbolRemovals.push(angelSymbol);

  // Keep legacy activeSymbol in sync
  if (activeSymbol === angelSymbol) {
    activeSymbol = activeStrategySymbols[0] || null;
  }

  console.log("Active strategy symbols updated:", activeStrategySymbols);

  res.json({
    message: "symbol removed",
    symbols: activeStrategySymbols,
  });
});

// Return and drain pending symbol removals (polled by build-candle.js)
app.get("/pending-symbol-removals", (req, res) => {
  const removals = pendingSymbolRemovals.splice(0);
  res.json({ symbols: removals });
});

// Return all current watchlist symbols
app.get("/watchlist-symbols", (req, res) => {
  res.json({
    symbols: watchlistSymbols,
  });
});

// Update full watchlist symbols (called from frontend)
// Frontend should send the entire current watchlist after add/remove
app.post("/watchlist-symbols", (req, res) => {
  const { symbols } = req.body;

  if (!Array.isArray(symbols)) {
    return res.status(400).json({
      message: "symbols must be an array",
    });
  }

  const prevSymbols = watchlistSymbols;

  watchlistSymbols = symbols
    .map((symbol) => String(symbol).trim())
    .filter(Boolean);

  // Auto-remove active strategy symbols no longer in the watchlist
  const removed = activeStrategySymbols.filter((s) => !watchlistSymbols.includes(s));
  if (removed.length > 0) {
    activeStrategySymbols = activeStrategySymbols.filter((s) => watchlistSymbols.includes(s));
    if (activeSymbol && !watchlistSymbols.includes(activeSymbol)) {
      activeSymbol = activeStrategySymbols[0] || null;
    }
    console.log("Auto-removed strategy symbols not in watchlist:", removed);
    console.log("Active strategy symbols now:", activeStrategySymbols);
  }

  if (JSON.stringify(prevSymbols) !== JSON.stringify(watchlistSymbols)) {
    console.log("Watchlist symbols updated:", watchlistSymbols);
  }

  res.json({
    message: "watchlist symbols updated",
    symbols: watchlistSymbols,
  });
});

// Endpoint to return current market time
app.get("/market-time", (req, res) => {
  res.json({
    marketTime: latestMarketTime,
  });
});

// Endpoint to update market time (called by build-candle)
app.post("/market-time", (req, res) => {
  const { marketTime } = req.body;

  if (!marketTime) {
    return res.status(400).json({ message: "marketTime required" });
  }

  latestMarketTime = marketTime;

  res.json({ message: "market time updated" });
});

// Endpoint to receive latest live price from build-candle
app.post("/price-update", (req, res) => {
  const { symbol, ltp, marketTime } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  if (ltp === undefined || ltp === null) {
    return res.status(400).json({ message: "ltp is required" });
  }

  const angelSymbol = String(symbol).trim();

  latestPricesBySymbol[angelSymbol] = {
    ltp: Number(ltp),
    marketTime: marketTime || latestMarketTime,
  };

  res.json({
    message: "price updated",
    symbol: angelSymbol,
    price: latestPricesBySymbol[angelSymbol],
  });
});

// ---- Symbol History Status tracking ----
// Tracks per-symbol history fetch status: 'loading' | 'ready' | 'failed'
// Updated by build-candle.js, queried by trade-engine
const symbolHistoryStatus = {};

// GET status for a specific symbol
app.get("/symbol-history-status/:symbol", (req, res) => {
  const symbol = req.params.symbol;
  const status = symbolHistoryStatus[symbol] || { status: "unknown", candleCount: 0 };
  res.json(status);
});

// GET status for all symbols
app.get("/symbol-history-status", (req, res) => {
  res.json(symbolHistoryStatus);
});

// POST to update status (called by build-candle.js)
app.post("/symbol-history-status", (req, res) => {
  const { symbol, status, candleCount } = req.body;
  if (!symbol || !status) {
    return res.status(400).json({ message: "symbol and status required" });
  }
  symbolHistoryStatus[symbol] = { status, candleCount: candleCount || 0, updatedAt: Date.now() };
  res.json({ ok: true });
});

// ---- Log API endpoints ----

app.get("/logs/server", (req, res) => {
  res.json({ logs: serverLogs });
});

app.get("/logs/candle", (req, res) => {
  res.json({ logs: candleLogs });
});

app.post("/logs/candle-push", (req, res) => {
  const { lines } = req.body;
  if (Array.isArray(lines)) {
    for (const line of lines) {
      pushLog(candleLogs, line);
    }
  }
  res.json({ ok: true });
});

// ---- Nifty50 Live Candle WebSocket ----

let nifty50CandleData = {
  completedCandles: [],
  currentCandle: null,
};

// Receive candle updates from build-candle.js
app.post("/nifty50-candle-update", (req, res) => {
  const { completedCandles, currentCandle } = req.body;
  if (Array.isArray(completedCandles)) {
    nifty50CandleData.completedCandles = completedCandles;
  }
  if (currentCandle) {
    nifty50CandleData.currentCandle = currentCandle;
  }
  // Broadcast to all connected WebSocket clients
  broadcastNifty50();
  res.json({ ok: true });
});

// Track connected Nifty50 WebSocket clients
const nifty50Clients = new Set();

function broadcastNifty50() {
  const msg = JSON.stringify({
    type: "update",
    completedCandles: nifty50CandleData.completedCandles,
    currentCandle: nifty50CandleData.currentCandle,
  });
  for (const client of nifty50Clients) {
    if (client.readyState === 1) { // WebSocket.OPEN
      client.send(msg);
    }
  }
}

// Search symbols for watchlist dropdown
app.get("/watchlist", (req, res) => {
  const q = (req.query.q || "").toString().trim().toUpperCase();

  if (!q) {
    return res.json([]);
  }

  const matches = allOptionRows
    .filter((item) => item.symbol && item.symbol.toUpperCase().includes(q))
    .slice(0, 20)
    .map(toWatchlistItem)
    .map((item) => ({
      ...item,
      symbol: item.symbol,
    }));

  res.json(matches);
});

// Return prices for requested symbols
// This compares each requested symbol against latestPricesBySymbol
// so multiple watchlist rows can each get their own LTP
app.get("/prices", (req, res) => {
  const rawSymbols = (req.query.symbols || "").toString().trim();

  if (!rawSymbols) {
    return res.json([]);
  }

  const symbols = rawSymbols
    .split(",")
    .map((symbol) => symbol.trim())
    .filter(Boolean);

  const result = symbols.map((symbol) => {
    const angelSymbol = symbol;
    const priceInfo = latestPricesBySymbol[angelSymbol];

    return {
      symbol,
      ltp: priceInfo ? priceInfo.ltp : null,
      marketTime: priceInfo ? priceInfo.marketTime : latestMarketTime,
      isActiveSymbol: activeStrategySymbols.includes(angelSymbol),
    };
  });

  res.json(result);
});

async function refreshScripMaster() {
  try {
    console.log("Refreshing Angel scrip master...");
    const rows = await loadScripMaster();
    const niftyOptions = filterNiftyOptions(rows);
    allOptionRows = niftyOptions;
    console.log("Scrip master refreshed — NIFTY option rows:", allOptionRows.length);
  } catch (error) {
    console.error("Scrip master refresh failed:", error.message);
  }
}

async function startServer() {
  try {
    console.log("Loading Angel scrip master...");

    const rows = await loadScripMaster();
    const niftyOptions = filterNiftyOptions(rows);

    allOptionRows = niftyOptions;

    console.log("Scrip master loaded");
    console.log("NIFTY option rows:", allOptionRows.length);

    // Refresh scrip master every 12 hours
    const REFRESH_INTERVAL = 12 * 60 * 60 * 1000;
    setInterval(refreshScripMaster, REFRESH_INTERVAL);
    console.log("Scrip master auto-refresh scheduled every 12 hours");

    const server = http.createServer(app);

    // WebSocket server for Nifty50 live chart
    const wss = new WebSocketServer({ server, path: "/ws/nifty50" });

    wss.on("connection", (ws) => {
      console.log("[WS] Nifty50 client connected");
      nifty50Clients.add(ws);

      // Send full snapshot on connect
      const snapshot = JSON.stringify({
        type: "snapshot",
        completedCandles: nifty50CandleData.completedCandles,
        currentCandle: nifty50CandleData.currentCandle,
      });
      ws.send(snapshot);

      ws.on("close", () => {
        nifty50Clients.delete(ws);
        console.log("[WS] Nifty50 client disconnected");
      });

      ws.on("error", () => {
        nifty50Clients.delete(ws);
      });
    });

    server.listen(PORT, () => {
      console.log(`Angel symbol search server running at http://localhost:${PORT}`);
      console.log(`Nifty50 WebSocket available at ws://localhost:${PORT}/ws/nifty50`);
    });
  } catch (error) {
    console.error("Failed to start symbol server:");
    console.error(error);
  }
}

startServer();