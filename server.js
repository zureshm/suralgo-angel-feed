// server.js
// Purpose:
// 1. Start a small API server on port 2000
// 2. Load Angel scrip master once on startup
// 3. Expose /watchlist?q=... for frontend symbol search
// 4. Expose /prices?symbols=... for frontend LTP polling
// 5. Store latest live price per symbol from build-candle worker
// 6. Store full watchlist symbols for multi-symbol LTP subscription

const express = require("express");
const cors = require("cors");
const { loadScripMaster, filterNiftyOptions } = require("./loadScripMaster");

const app = express();
const allowedOrigins = [
  "http://localhost:3000",
  "http://localhost:4200",
  "http://209.38.126.3:3000",
  "https://suralgo.duckdns.org"
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

let allOptionRows = [];

// Convert Sensex YYMDD to DDMMMYY format for display (e.g., 26507 -> 07MAY26)
function formatSensexSymbolForDisplay(symbol) {
  if (!symbol.startsWith("SENSEX")) return symbol;

  const match = symbol.match(/^SENSEX(\d{5})(\d{5})(CE|PE)$/);
  if (!match) return symbol; // Already in DDMMM format or different format

  const [, datePart, strike, type] = match;
  const year = datePart.slice(0, 2); // First 2 digits = year
  const month = parseInt(datePart.slice(2, 3)); // Next 1 digit = month (1-12)
  const day = datePart.slice(3, 5); // Last 2 digits = day
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const monthName = months[month - 1];

  return `SENSEX${day}${monthName}${year}${strike}${type}`;
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

// Store latest market time (updated by build-candle)
let latestMarketTime = null;

// Store the currently selected symbol for live flow (DEPRECATED — use activeStrategySymbols)
let activeSymbol = null;

// Active strategy symbols (max 2 simultaneous, e.g. one CE + one PE)
let activeStrategySymbols = [];
const MAX_ACTIVE_STRATEGY_SYMBOLS = 2;

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
    symbols: activeStrategySymbols.map(s => formatSensexSymbolForDisplay(s)),
  });
});

// Add a symbol to active strategy symbols (max 2)
app.post("/active-strategy-symbols", (req, res) => {
  const { symbol } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  // Convert to Angel format for lookup
  const angelSymbol = formatSensexSymbolForLookup(String(symbol).trim());

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
    symbols: activeStrategySymbols.map(s => formatSensexSymbolForDisplay(s)),
  });
});

// Remove a symbol from active strategy symbols
app.delete("/active-strategy-symbols", (req, res) => {
  const { symbol } = req.body;

  if (!symbol) {
    return res.status(400).json({ message: "symbol is required" });
  }

  // Convert to Angel format for lookup
  const angelSymbol = formatSensexSymbolForLookup(String(symbol).trim());

  activeStrategySymbols = activeStrategySymbols.filter((s) => s !== angelSymbol);

  // Keep legacy activeSymbol in sync
  if (activeSymbol === angelSymbol) {
    activeSymbol = activeStrategySymbols[0] || null;
  }

  console.log("Active strategy symbols updated:", activeStrategySymbols);

  res.json({
    message: "symbol removed",
    symbols: activeStrategySymbols.map(s => formatSensexSymbolForDisplay(s)),
  });
});

// Return all current watchlist symbols
app.get("/watchlist-symbols", (req, res) => {
  res.json({
    symbols: watchlistSymbols.map(s => formatSensexSymbolForDisplay(s)),
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
    .map((symbol) => formatSensexSymbolForLookup(String(symbol).trim()))
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
    symbols: watchlistSymbols.map(s => formatSensexSymbolForDisplay(s)),
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

  latestPricesBySymbol[symbol] = {
    ltp: Number(ltp),
    marketTime: marketTime || latestMarketTime,
  };

  res.json({
    message: "price updated",
    symbol,
    price: latestPricesBySymbol[symbol],
  });
});

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
      symbol: formatSensexSymbolForDisplay(item.symbol),
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
    // Convert display format to Angel format for lookup
    const angelSymbol = formatSensexSymbolForLookup(symbol);
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

    app.listen(PORT, () => {
      console.log(`Angel symbol search server running at http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start symbol server:");
    console.error(error);
  }
}

startServer();