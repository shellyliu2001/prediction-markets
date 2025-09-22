import requests
import json

# ----- CONFIG -----
URL = "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
PARAMS = {
    "ticker": "KXMAYORNYCNOMD-25-ZM",
    "min_ts": 1730955600,
    "max_ts": 1751515200,
    "limit": 1000  # Kalshi defaults to pagination, so we grab chunks
}
OUTPUT_FILE = "/Users/wendyliu/Desktop/kalshi_zm_trades.json"
# ------------------

all_trades = []
cursor = None

while True:
    # Update cursor if we have one
    if cursor:
        PARAMS["cursor"] = cursor
    else:
        PARAMS.pop("cursor", None)

    # Call API
    resp = requests.get(URL, params=PARAMS)
    resp.raise_for_status()
    data = resp.json()

    # Collect trades
    trades = data.get("trades", [])
    all_trades.extend(trades)

    print(f"Fetched {len(trades)} trades, total so far: {len(all_trades)}")

    # Check for pagination
    cursor = data.get("cursor")
    if not cursor:
        break  # No more pages

# Save all trades to JSON file
with open(OUTPUT_FILE, "w") as f:
    json.dump(all_trades, f, indent=2)

print(f"✅ Done! Saved {len(all_trades)} trades to {OUTPUT_FILE}")