const { SmartAPI } = require("smartapi-javascript");

async function fetchHistoricalCandles({
  smartApi,
  symbolToken,
  exchange = "NFO",
  interval = "ONE_MINUTE",
  fromDate,
  toDate,
}) {
  try {
    const response = await smartApi.getCandleData({
      exchange,
      symboltoken: String(symbolToken),
      interval,
      fromdate: fromDate,
      todate: toDate,
    });

    if (!response || !response.data || !Array.isArray(response.data)) {
      console.log("Historical candle raw response:", JSON.stringify(response));
      console.log("No historical candles returned");

      // SDK returns { status, message } for HTTP errors (no throw)
      // Check for rate limit (403 or AB1021)
      if (response && (response.status === 403 || response.errorcode === "AB1021" ||
          (response.message && (response.message.includes("Access denied") || response.message.includes("Too many requests"))))) {
        return { rateLimitError: true };
      }

      // Signal invalid token so caller can refresh scrip master
      if (response && (response.errorCode === "AG8001" || response.errorcode === "AG8001")) {
        return { invalidToken: true };
      }

      // Check for auth errors (401/session expired)
      if (response && (response.status === 401 ||
          (response.message && (response.message.includes("Unauthorized") || response.message.includes("Session"))))) {
        return { authError: true };
      }

      return [];
    }

    return response.data.map((item) => {
      return {
        time: item[0],
        open: Number(item[1]),
        high: Number(item[2]),
        low: Number(item[3]),
        close: Number(item[4]),
        volume: Number(item[5]) || 0,
      };
    });
  } catch (error) {
    console.error("Fetch historical candles failed:", error.message);
    // Signal rate limit error so caller can back off longer
    if (error.message && (error.message.includes("Access denied") || error.message.includes("AB1021") || error.message.includes("Too many requests"))) {
      return { rateLimitError: true };
    }
    // Signal auth error to trigger session refresh (not 403 — that's rate limit)
    if (error.message && (error.message.includes("401") || error.message.includes("Unauthorized") || error.message.includes("Session"))) {
      return { authError: true };
    }
    return [];
  }
}

module.exports = { fetchHistoricalCandles };