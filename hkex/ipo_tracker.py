#!/usr/bin/env python3
"""
hkex/ipo_tracker.py — Fetch recent HK IPOs and their performance from Futu API.

Uses:
    - get_plate_stock('HK.LIST1290') for the "Recent IPOs" list
    - get_market_snapshot() for current prices
    - get_owner_plate() for industry classification
    - get_ipo_list() for IPO offer price (if still available)
"""

import logging
import math
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

# Futu plate code for "Recent IPOs"
RECENT_IPOS_PLATE = "HK.LIST1290"
MAX_DAYS = 90  # Track for 3 months


def fetch_recent_ipos(quote_ctx, market: str = "HK") -> list[dict]:
    """
    Fetch recent IPOs with current performance data.

    Args:
        quote_ctx: An open Futu OpenQuoteContext
        market: Market code (default "HK")

    Returns:
        List of dicts, one per IPO stock, sorted by list date descending.
    """
    # 1. Get stocks in the "Recent IPOs" plate
    ret, plate_data = quote_ctx.get_plate_stock(RECENT_IPOS_PLATE)
    if ret != 0 or plate_data is None or plate_data.empty:
        logger.warning("Failed to fetch Recent IPOs plate: %s", plate_data)
        return []

    codes = plate_data["code"].tolist()
    name_map = dict(zip(plate_data["code"], plate_data["stock_name"]))
    list_date_map = dict(zip(plate_data["code"], plate_data["list_time"]))

    # Filter to last 90 days only
    cutoff = (datetime.now() - timedelta(days=MAX_DAYS)).strftime("%Y-%m-%d")
    codes = [c for c in codes if list_date_map.get(c, "1970-01-01") >= cutoff]

    if not codes:
        return []

    # 2. Get current prices via snapshot (batch 400)
    snap_map = {}
    for i in range(0, len(codes), 400):
        batch = codes[i:i + 400]
        ret, snap = quote_ctx.get_market_snapshot(batch)
        if ret == 0 and snap is not None and not snap.empty:
            for _, row in snap.iterrows():
                snap_map[row["code"]] = row

    # 3. Get industry classification via get_owner_plate (batch 200)
    industry_map = {}
    for i in range(0, len(codes), 200):
        batch = codes[i:i + 200]
        ret, plates = quote_ctx.get_owner_plate(batch)
        if ret == 0 and plates is not None and not plates.empty:
            industry_rows = plates[plates["plate_type"] == "INDUSTRY"]
            for _, row in industry_rows.iterrows():
                code = row["code"]
                if code not in industry_map:
                    industry_map[code] = row["plate_name"]

    # 4. Get IPO/listing prices
    #    - First try get_ipo_list (has official offer price for active IPOs)
    #    - Then fallback to first-day opening price via Yahoo Finance (free, no quota)
    ipo_price_map = {}
    try:
        from futu import Market as FMarket
        mkt = FMarket.HK if market == "HK" else FMarket.US
        ret, ipo_data = quote_ctx.get_ipo_list(mkt)
        if ret == 0 and ipo_data is not None and not ipo_data.empty:
            for _, row in ipo_data.iterrows():
                code = row["code"]
                price = row.get("list_price") or row.get("ipo_price")
                if price and float(price) > 0:
                    ipo_price_map[code] = float(price)
    except Exception as e:
        logger.debug("get_ipo_list failed (non-critical): %s", e)

    # Fallback: fetch first-day open price via Yahoo Finance chart API
    import requests as _requests
    import time
    missing = [c for c in codes if c not in ipo_price_map]
    if missing:
        yahoo_headers = {"User-Agent": "Mozilla/5.0"}
        for code in missing:
            try:
                # Convert HK.00470 -> 0470.HK
                raw = code.replace("HK.", "")
                yahoo_ticker = raw.lstrip("0") + ".HK" if len(raw) > 4 else raw + ".HK"
                if not yahoo_ticker[0].isdigit():
                    continue

                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
                r = _requests.get(url, headers=yahoo_headers,
                                  params={"range": "max", "interval": "1d"}, timeout=8)
                if r.status_code == 200:
                    result = r.json().get("chart", {}).get("result", [])
                    if result:
                        opens = result[0].get("indicators", {}).get("quote", [{}])[0].get("open", [])
                        if opens and opens[0] and not math.isnan(opens[0]):
                            ipo_price_map[code] = round(opens[0], 3)
                time.sleep(0.2)
            except Exception as e:
                logger.debug("Yahoo price fetch failed for %s: %s", code, e)

    # 5. Build result rows
    rows = []
    for code in codes:
        snap = snap_map.get(code)
        if snap is None:
            continue

        def _safe_float(val, default=0.0):
            try:
                v = float(val)
                return default if math.isnan(v) else v
            except (TypeError, ValueError):
                return default

        def _safe_int(val, default=0):
            try:
                v = float(val)
                return default if math.isnan(v) else int(v)
            except (TypeError, ValueError):
                return default

        list_date_str = list_date_map.get(code, "")
        cur_price = _safe_float(snap.get("last_price", 0))
        prev_close = _safe_float(snap.get("prev_close_price", 0))
        volume = _safe_int(snap.get("volume", 0))
        turnover = _safe_float(snap.get("turnover", 0))
        market_cap = _safe_float(snap.get("total_market_val", 0))

        # IPO price: from ipo_list if available
        ipo_price = ipo_price_map.get(code, 0)

        # Calculate return since IPO
        if ipo_price > 0 and cur_price > 0:
            ipo_return = (cur_price - ipo_price) / ipo_price * 100
        else:
            ipo_return = None

        # Daily change
        daily_chg = ((cur_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # Days since IPO
        try:
            days_since = (datetime.now() - datetime.strptime(list_date_str, "%Y-%m-%d")).days
        except (ValueError, TypeError):
            days_since = None

        rows.append({
            "Code": code,
            "Name": name_map.get(code, ""),
            "Industry": industry_map.get(code, ""),
            "IPO Price": round(ipo_price, 3) if ipo_price else "",
            "List Date": list_date_str,
            "Current Price": round(cur_price, 3),
            "Daily Change %": round(daily_chg, 1),
            "Return Since IPO %": round(ipo_return, 1) if ipo_return is not None else "",
            "Days Listed": days_since if days_since is not None else "",
            "Volume": volume,
            "Turnover (HK$)": round(turnover),
            "Market Cap": round(market_cap),
        })

    # Sort by listing date descending (newest first)
    rows.sort(key=lambda r: r.get("List Date", ""), reverse=True)

    logger.info("IPO Tracker: fetched %d stocks from Recent IPOs plate", len(rows))
    return rows
