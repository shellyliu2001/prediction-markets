#!/usr/bin/env python3
# Clean Polymarket fills → ONE row per actual trade (classified by taker), newest first.
# Fixes BUY/SELL, Yes/No, and price by canonicalizing to the taker perspective per tx.

import argparse
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd

USDC_ZERO_ID = "0"
DECIMALS = 1_000_000  # 6 decimals for both tokens and USDC amounts

# Default token IDs for Mamdani market
DEFAULT_YES_TOKEN = "73817598408230683831072353847770809458837920203753987347670649717002095543451"
DEFAULT_NO_TOKEN = "102505737677514435038431832532030540090751572260157019042399710777845176913904"

# ---------- Optional: on-chain derivation of token ids (no API) ----------
def keccak256(data: bytes) -> bytes:
    import sha3  # pip install pysha3
    k = sha3.keccak_256()
    k.update(data)
    return k.digest()

def to_uint256_be(n: int) -> bytes:
    return n.to_bytes(32, "big")

def hexstr_to_bytes32(h: str) -> bytes:
    h = h[2:] if h.startswith("0x") else h
    b = bytes.fromhex(h)
    return b if len(b) == 32 else b.rjust(32, b"\x00")

def address_to_bytes(addr: str) -> bytes:
    a = addr[2:] if addr.startswith("0x") else addr
    b = bytes.fromhex(a)
    if len(b) != 20:
        raise ValueError(f"Bad address length for {addr}")
    return b

def derive_position_id(condition_id: str, outcome_index: int, collateral: str) -> int:
    # collectionId = keccak256(parent=0x00..00, conditionId, indexSet=1<<i)
    parent = b"\x00" * 32
    cond = hexstr_to_bytes32(condition_id)
    index_set = to_uint256_be(1 << outcome_index)
    collection_id = keccak256(parent + cond + index_set)
    # positionId = keccak256(collateral(address) || collectionId)
    pos = keccak256(address_to_bytes(collateral) + collection_id)
    return int.from_bytes(pos, "big")

# ---------- Core logic ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", required=True, help="raw fills CSV")
    p.add_argument("--out", dest="out_path", required=True, help="output CSV")

    # Token-id sources (pick one: explicit OR derive OR infer-from-file)
    p.add_argument("--yes-token", help="explicit YES token id")
    p.add_argument("--no-token", help="explicit NO token id")
    p.add_argument("--condition", help="conditionId (0x...) to derive ids")
    p.add_argument("--collateral", help="collateral address (USDC on the correct chain)")
    p.add_argument("--yes-index", type=int, default=0, help="YES outcome index (default 0)")
    p.add_argument("--no-index", type=int, default=1, help="NO outcome index (default 1)")

    # Cosmetic (optional)
    p.add_argument("--title", default="", help="title column text")
    p.add_argument("--slug", default="", help="slug column text")
    p.add_argument("--event-slug", default="", help="eventSlug column text")
    
    # Perspective (optional)
    p.add_argument("--perspective", choices=["taker", "maker"], default="taker", 
                   help="perspective for buy/sell classification (default: taker)")
    
    return p.parse_args()

def infer_token_ids_from_file(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    # Pick the two most frequent non-zero ids seen in maker/taker asset fields.
    assets = pd.concat([df["makerAssetId"].astype(str), df["takerAssetId"].astype(str)])
    assets = assets[assets != USDC_ZERO_ID]
    uniq = assets.value_counts().index.tolist()
    if len(uniq) >= 2:
        # Heuristic mapping: the one starting with '1025' tends to be NO on Polymarket
        yes = None
        no = None
        for a in uniq[:2]:
            if str(a).startswith("1025"):
                no = a
            else:
                yes = a
        if yes and no:
            return str(yes), str(no)
        # Fallback: return first two; outcome labels later will still be consistent
        return str(uniq[0]), str(uniq[1])
    return None

def determine_token_ids(df: pd.DataFrame, args) -> Tuple[str, str]:
    # 1) explicit
    if args.yes_token and args.no_token:
        return str(args.yes_token), str(args.no_token)
    # 2) derive from chain
    if args.condition and args.collateral:
        try:
            yes = str(derive_position_id(args.condition, args.yes_index, args.collateral))
            no  = str(derive_position_id(args.condition, args.no_index,  args.collateral))
            return yes, no
        except Exception:
            pass
    # 3) use default Mamdani tokens
    return DEFAULT_YES_TOKEN, DEFAULT_NO_TOKEN

def clean_trades(df: pd.DataFrame, YES_TOKEN: str, NO_TOKEN: str,
                 title: str, slug: str, event_slug: str, perspective: str = "taker") -> pd.DataFrame:
    TOKENS = {YES_TOKEN, NO_TOKEN}

    # numeric casts
    for c in ["timestamp", "makerAmountFilled", "takerAmountFilled"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # identify which rows have a token on which side
    df["maker_is_token"] = df["makerAssetId"].isin(TOKENS)
    df["taker_is_token"] = df["takerAssetId"].isin(TOKENS)
    df["maker_is_usdc"]  = df["makerAssetId"].astype(str).eq(USDC_ZERO_ID)
    df["taker_is_usdc"]  = df["takerAssetId"].astype(str).eq(USDC_ZERO_ID)

    # keep only token<->USDC fills
    mask_token_usdc = (
        (df["maker_is_token"] & df["taker_is_usdc"]) |
        (df["taker_is_token"] & df["maker_is_usdc"])
    )
    df = df[mask_token_usdc].copy()
    if df.empty:
        raise RuntimeError("No token↔USDC fills found.")

    # Canonicalize per transactionHash from specified perspective
    # Process each outcome token separately to handle mixed transactions
    def canon_group(g: pd.DataFrame) -> pd.DataFrame:
        results = []
        
        # Process each outcome token separately
        for token_id in [YES_TOKEN, NO_TOKEN]:
            # Get fills involving this specific token
            token_fills = g[
                (g["makerAssetId"].astype(str) == str(token_id)) | 
                (g["takerAssetId"].astype(str) == str(token_id))
            ]
            
            if token_fills.empty:
                continue
                
            # Calculate net position change for this token
            net_tokens = 0
            total_usdc_volume = 0
            
            for _, fill in token_fills.iterrows():
                if str(fill["makerAssetId"]) == str(token_id):
                    # Token on maker side
                    if perspective == "taker":
                        # Taker receives tokens (BUY)
                        net_tokens += fill["makerAmountFilled"] / DECIMALS
                        total_usdc_volume += fill["takerAmountFilled"] / DECIMALS
                    else:  # maker perspective
                        # Maker gives tokens (SELL)
                        net_tokens -= fill["makerAmountFilled"] / DECIMALS
                        total_usdc_volume += fill["takerAmountFilled"] / DECIMALS
                else:
                    # Token on taker side
                    if perspective == "taker":
                        # Taker gives tokens (SELL)
                        net_tokens -= fill["takerAmountFilled"] / DECIMALS
                        total_usdc_volume += fill["makerAmountFilled"] / DECIMALS
                    else:  # maker perspective
                        # Maker receives tokens (BUY)
                        net_tokens += fill["takerAmountFilled"] / DECIMALS
                        total_usdc_volume += fill["makerAmountFilled"] / DECIMALS
            
            # Determine side based on net position change
            if net_tokens > 0:
                side = "BUY"
                size_tokens = net_tokens
            elif net_tokens < 0:
                side = "SELL"
                size_tokens = abs(net_tokens)
            else:
                # Net zero - skip this token
                continue
                
            price = (total_usdc_volume / size_tokens) if size_tokens > 0 else None
            outcome = "Yes" if str(token_id) == YES_TOKEN else "No"
            
            results.append({
                "transactionHash": g["transactionHash"].iloc[0],
                "timestamp":       int(g["timestamp"].max()),
                "side":            side,
                "outcome":         outcome,
                "price":           price,
                "size":            size_tokens,
                "volume_usdc":     total_usdc_volume,
                "asset":           str(token_id),
                "proxyWallet":     g["maker"].iloc[0] if perspective == "maker" else g["taker"].iloc[0],
                "title":           title,
                "slug":            slug,
                "eventSlug":       event_slug,
            })
        
        # If no valid trades found, return empty DataFrame
        if not results:
            return pd.DataFrame()
            
        return pd.DataFrame(results)

    out = (df.sort_values("timestamp")
             .groupby("transactionHash", group_keys=False)
             .apply(canon_group))

    out["datetime_utc"] = pd.to_datetime(out["timestamp"], unit="s", utc=True)
    out = out.sort_values("timestamp", ascending=False)[[
        "timestamp","datetime_utc","side","outcome","price","size","volume_usdc",
        "transactionHash","asset","proxyWallet","title","slug","eventSlug"
    ]]
    return out

def main():
    args = parse_args()
    assert Path(args.in_path).exists(), f"Input not found: {args.in_path}"
    df = pd.read_csv(args.in_path, dtype=str).fillna("")

    YES_TOKEN, NO_TOKEN = determine_token_ids(df, args)

    cleaned = clean_trades(
        df, YES_TOKEN, NO_TOKEN,
        title=args.title, slug=args.slug, event_slug=args.event_slug, perspective=args.perspective
    )
    cleaned.to_csv(args.out_path, index=False)
    print(f"✓ Wrote {args.out_path} with {len(cleaned)} trades (no buy/sell pairs).")
    print(f"Token IDs → YES: {YES_TOKEN} | NO: {NO_TOKEN}")

if __name__ == "__main__":
    main()