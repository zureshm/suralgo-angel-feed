const https = require("https");

const SCRIP_MASTER_URL =
  "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json";

function loadScripMaster() {
  const TIMEOUT_MS = 30000;

  return new Promise((resolve, reject) => {
    console.log(`[ScripMaster] Fetching from ${SCRIP_MASTER_URL} (timeout: ${TIMEOUT_MS / 1000}s)...`);
    const startTime = Date.now();
    let receivedBytes = 0;

    const req = https
      .get(SCRIP_MASTER_URL, (res) => {
        console.log(`[ScripMaster] Connected — HTTP ${res.statusCode}`);
        let data = "";

        res.on("data", (chunk) => {
          data += chunk;
          receivedBytes += chunk.length;

          // Log progress every ~5MB
          if (receivedBytes % (5 * 1024 * 1024) < chunk.length) {
            console.log(`[ScripMaster] Downloading... ${(receivedBytes / (1024 * 1024)).toFixed(1)} MB received`);
          }
        });

        res.on("end", () => {
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
          console.log(`[ScripMaster] Download complete — ${(receivedBytes / (1024 * 1024)).toFixed(1)} MB in ${elapsed}s`);

          try {
            const json = JSON.parse(data);
            console.log(`[ScripMaster] Parsed ${json.length} rows`);
            resolve(json);
          } catch (error) {
            console.error(`[ScripMaster] JSON parse failed:`, error.message);
            reject(error);
          }
        });
      })
      .on("error", (error) => {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.error(`[ScripMaster] Fetch failed after ${elapsed}s:`, error.message);
        reject(error);
      });

    req.setTimeout(TIMEOUT_MS, () => {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.error(`[ScripMaster] TIMEOUT after ${elapsed}s (received ${(receivedBytes / (1024 * 1024)).toFixed(1)} MB). Aborting.`);
      req.destroy();
      reject(new Error(`ScripMaster fetch timed out after ${TIMEOUT_MS / 1000}s`));
    });
  });
}

function filterNiftyOptions(rows) {
  const now = new Date();
  now.setHours(0, 0, 0, 0); // Start of today so expiry-day symbols are included
  const maxExpiry = new Date();
  maxExpiry.setDate(now.getDate() + 35); // 5 weeks

  return rows.filter((item) => {
    // Nifty on NFO, Sensex on BFO
    const isNifty = item.exch_seg === "NFO" && item.name === "NIFTY";
    const isSensex = item.exch_seg === "BFO" && item.name === "SENSEX";

    const isValidIndex = (isNifty || isSensex) && item.instrumenttype === "OPTIDX";
    if (!isValidIndex) return false;

    // Parse expiry date (format: "DD-MMM-YYYY" e.g., "29-May-2026")
    const expiryStr = item.expiry || item.expirydate || item.expiry_date;
    if (!expiryStr) return false;

    try {
      const expiryDate = new Date(expiryStr);
      // Valid if expiry is between now and maxExpiry (5 weeks)
      return expiryDate >= now && expiryDate <= maxExpiry;
    } catch {
      return false;
    }
  });
}

module.exports = {
  loadScripMaster,
  filterNiftyOptions,
};