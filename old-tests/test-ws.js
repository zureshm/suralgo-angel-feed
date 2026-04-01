require("dotenv").config();

const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const { authenticator } = require("otplib");

async function run() {
  try {
    const smartApi = new SmartAPI({
      api_key: process.env.ANGEL_API_KEY,
    });

    const totp = authenticator.generate(process.env.ANGEL_TOTP_SECRET);

    const session = await smartApi.generateSession(
      process.env.ANGEL_CLIENT_CODE,
      process.env.ANGEL_PASSWORD,
      totp
    );

    console.log("Login success");

    const ws = new WebSocketV2({
      clientcode: process.env.ANGEL_CLIENT_CODE,
      jwttoken: session.data.jwtToken,
      apikey: process.env.ANGEL_API_KEY,
      feedtype: session.data.feedToken,
    });

    await ws.connect();
    console.log("WebSocket connected");

    ws.on("tick", (data) => {
      console.log("Tick:", data);
    });

      ws.fetchData({
          correlationID: "sbin-ltp-1",
          action: 1,
          mode: 2,
          exchangeType: 1,
          tokens: ["3045"]
      });
  } catch (error) {
    console.error("WS failed:");
    console.error(error);
  }
}

run();