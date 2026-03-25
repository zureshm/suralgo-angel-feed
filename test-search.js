const { loadScripMaster, filterNiftyOptions } = require("./loadScripMaster");

async function run() {
  try {
    const allRows = await loadScripMaster();
    const niftyOptions = filterNiftyOptions(allRows);

    const matches = niftyOptions.filter((item) => {
      return (
        item.symbol &&
        item.symbol.includes("23500") &&
        (item.symbol.includes("CE") || item.symbol.includes("PE"))
      );
    });

    console.log("Total NIFTY options:", niftyOptions.length);
    console.log("Matching symbols:");
    console.log(matches.slice(0, 20));
  } catch (error) {
    console.error("Search failed:", error.message);
  }
}

run();