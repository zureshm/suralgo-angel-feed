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
        console.log("Historical candle raw response:", response);
      console.log("No historical candles returned");
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
    return [];
  }
}

module.exports = { fetchHistoricalCandles };