const https = require("https");

const SCRIP_MASTER_URL =
  "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json";

function loadScripMaster() {
  return new Promise((resolve, reject) => {
    https
      .get(SCRIP_MASTER_URL, (res) => {
        let data = "";

        res.on("data", (chunk) => {
          data += chunk;
        });

        res.on("end", () => {
          try {
            const json = JSON.parse(data);
            resolve(json);
          } catch (error) {
            reject(error);
          }
        });
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

function filterNiftyOptions(rows) {
  return rows.filter((item) => {
    return (
      item.exch_seg === "NFO" &&
      item.name === "NIFTY" &&
      item.instrumenttype &&
      item.instrumenttype.includes("OPT")
    );
  });
}

module.exports = {
  loadScripMaster,
  filterNiftyOptions,
};