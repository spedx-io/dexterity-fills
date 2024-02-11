# Dexterity Fills Webhook Server & API
A minimal Flask web application that serves as an endpoint that can be passed to a [Helius Webhook](https://dev.helius.xyz/) instance to handle transactions streamed in for the **Dexterity Program ID**, proccess them, filter for **OrderFillEvent**s, parse and insert them into a **PostgresSQL Table**

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/ylnG3y?referralCode=thales)

(Check out `db.py` for how to build your `fills` table)

## Helius Webhook
```js
const createWebhook = async () => {
    try {
      const response = await fetch(
        "https://api.helius.xyz/v0/webhooks?api-key=<PASTE YOUR API KEY HERE>",
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            "webhookURL": "https://your-railway-deployment.app/webhooks",
            "accountAddresses": ["FUfpR31LmcP1VSbz5zDaM7nxnH55iBHkpwusgrnhaFjL"],
            "accountAddressOwners": [],
            "encoding": "jsonParsed",
            "webhookType": "raw"
       }),
        }
      );
      const data = await response.json();
      console.log({ data });
    } catch (e) {
      console.error("error", e);
    }
  };
  createWebhook();
```
