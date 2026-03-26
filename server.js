// server.js
// Purpose:
// 1. Start a small API server on port 2000
// 2. Load Angel scrip master once on startup
// 3. Expose /watchlist?q=... for frontend symbol search
// 4. Expose /prices?symbols=... for frontend LTP polling

const express = require("express");
const cors = require("cors");
const { loadScripMaster, filterNiftyOptions } = require("./loadScripMaster");

const app = express();
app.use(cors());

const PORT = 2000;

let allOptionRows = [];
// Store latest market time (will be updated later)
let latestMarketTime = null;

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

// Endpoint to return current market time
app.get("/market-time", (req, res) => {
  res.json({
    marketTime: latestMarketTime,
  });
});

// Endpoint to update market time (called by build-candle)
app.post("/market-time", express.json(), (req, res) => {
  const { marketTime } = req.body;

  if (!marketTime) {
    return res.status(400).json({ message: "marketTime required" });
  }

  latestMarketTime = marketTime;

  res.json({ message: "market time updated" });
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
    .map(toWatchlistItem);

  res.json(matches);
});

// Return prices for requested symbols
// For now, we return ltp: null until live price lookup is added
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
    return {
      symbol,
      ltp: null,
    };
  });

  res.json(result);
});

async function startServer() {
  try {
    console.log("Loading Angel scrip master...");

    const rows = await loadScripMaster();
    const niftyOptions = filterNiftyOptions(rows);

    allOptionRows = niftyOptions;

    console.log("Scrip master loaded");
    console.log("NIFTY option rows:", allOptionRows.length);

    app.listen(PORT, () => {
      console.log(`Angel symbol search server running at http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start symbol server:");
    console.error(error);
  }
}

startServer();