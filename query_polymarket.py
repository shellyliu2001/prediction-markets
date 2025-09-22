# pip install requests tqdm
import csv, time, requests

CONDITION_ID = "0x6220c4164a293367cd40eba018dd6e67c78e4d48e74158845cc9361230bcb34d".lower()
OUT_CSV      = "/Users/wendyliu/Desktop/mamdani_trades.csv"

# 1) CLOB markets (public) to resolve outcome token IDs (YES/NO) for the condition
CLOB_MARKETS = "https://clob.polymarket.com/markets?next_cursor="

# 2) Goldsky public Polymarket Orderbook subgraph (no key needed)
ORDERBOOK_GQL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# GraphQL page size and pacing
FIRST      = 1000
SLEEP_SEC  = 0.07
TIMEOUT    = 60

def get_market_tokens(condition_id: str):
    sess = requests.Session()
    cursor = ""
    while True:
        r = sess.get(CLOB_MARKETS + cursor, timeout=TIMEOUT)
        r.raise_for_status()
        j = r.json()
        for m in j.get("data", []):
            if m.get("condition_id", "").lower() == condition_id:
                toks = [t["token_id"] for t in m.get("tokens", [])]
                if len(toks) != 2:
                    raise RuntimeError(f"Expected 2 tokens, got {toks}")
                return toks  # [YES_token_id, NO_token_id] (both decimal strings)
        cursor = j.get("next_cursor", "")
        if cursor == "LTE=" or cursor is None:
            break
    raise RuntimeError(f"Condition {condition_id} not found via CLOB markets")

# Query all OrderFilled events where makerAssetId IN ids
Q_MAKER = """
query Page($ids:[BigInt!], $cursor:BigInt, $first:Int!) {
  orderFilledEvents(
    first: $first
    orderBy: timestamp
    orderDirection: desc
    where: { makerAssetId_in: $ids, timestamp_lt: $cursor }
  ) {
    id
    timestamp
    transactionHash
    maker
    taker
    makerAssetId
    makerAmountFilled
    takerAssetId
    takerAmountFilled
    fee
  }
}
"""

# Query all OrderFilled events where takerAssetId IN ids
Q_TAKER = """
query Page($ids:[BigInt!], $cursor:BigInt, $first:Int!) {
  orderFilledEvents(
    first: $first
    orderBy: timestamp
    orderDirection: desc
    where: { takerAssetId_in: $ids, timestamp_lt: $cursor }
  ) {
    id
    timestamp
    transactionHash
    maker
    taker
    makerAssetId
    makerAmountFilled
    takerAssetId
    takerAmountFilled
    fee
  }
}
"""

def gql(endpoint, query, variables):
    r = requests.post(endpoint, json={"query": query, "variables": variables}, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if "errors" in j and j["errors"]:
        raise RuntimeError(str(j["errors"]))
    return j["data"]

def backfill_loop(ids, query, writer, seen):
    total = 0
    cursor = 2_000_000_000  # > current unix seconds, safe upper bound
    while True:
        data = gql(ORDERBOOK_GQL, query, {"ids": ids, "cursor": cursor, "first": FIRST})
        rows = data.get("orderFilledEvents", [])
        if not rows:
            break
        # write + update cursor
        earliest = None
        for r in rows:
            rid = r["id"]
            if rid in seen:
                continue
            seen.add(rid)
            writer.writerow(r)
            total += 1
            try:
                ts = int(r["timestamp"])
                earliest = ts if earliest is None or ts < earliest else earliest
            except:
                pass
        cursor = (earliest - 1) if earliest else (cursor - 60)
        if cursor <= 0:
            break
        time.sleep(SLEEP_SEC)
    return total

def main():
    print(f"Resolving tokens for condition: {CONDITION_ID}")
    yes_no_ids = get_market_tokens(CONDITION_ID)
    print(f"Token IDs: {yes_no_ids}")

    cols = [
        "id","timestamp","transactionHash","maker","taker",
        "makerAssetId","makerAmountFilled","takerAssetId","takerAmountFilled","fee"
    ]
    seen = set()
    total = 0
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()

        print("Backfilling (makerAssetId in market tokens)…")
        total += backfill_loop(yes_no_ids, Q_MAKER, w, seen)

        print("Backfilling (takerAssetId in market tokens)…")
        total += backfill_loop(yes_no_ids, Q_TAKER, w, seen)

    print(f"Done. Wrote {total} unique fills to {OUT_CSV}")

if __name__ == "__main__":
    main()