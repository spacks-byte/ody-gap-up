"""
gap_scorer.py
Drop-in module for TopGainersAnalyzer.

Signals used (all from get_market_snapshot, zero K-line quota):
  1. gap_pct        — overnight gap: (open - prev_close) / prev_close  (cap-adjusted)
  2. continuation   — holding the gap: (last - open) / open
  3. change_5min    — intraday momentum (Futu field)
  4. price_strength — position in intraday range: (last - low) / (high - low)
  5. turnover_delta — % flow since last scan (requires storing prev scan)
  6. unusual_volume — projected today's turnover vs yesterday's turnover

Penny stock friendly: no price floor. Turnover delta + unusual volume are the noise filters.
"""

import json
import os
from datetime import datetime, timedelta

# ─── Resolve data directory (project_root/data/) ──────────────────────────────

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

# ─── Scan history store ────────────────────────────────────────────────────────

SCAN_HISTORY_FILE = os.path.join(_DATA_DIR, "scan_history.json")
ALERT_LOG_FILE    = os.path.join(_DATA_DIR, "alert_log.json")
_PREV_DAY_FILE    = os.path.join(_DATA_DIR, "prev_day_turnover.json")

COOLDOWN_MINUTES  = 60          # min gap between repeat alerts for same stock
MIN_SCORE_ALERT   = 8           # fires Telegram  (0–12 scale)
MIN_SCORE_WATCH   = 4           # logs to watchlist only

# Turnover delta and unusual volume thresholds are now cap-tier-specific
# (defined inline in score_gap_up / score_intraday).


def _load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ─── Inter-scan turnover store ─────────────────────────────────────────────────

def save_scan_snapshot(market: str, snapshot_df):
    """
    Persist turnover for each stock so the next scan can diff it.
    Call this AFTER your get_market_snapshot batch, pass the raw DataFrame.
    No K-line calls involved — this is just writing today's turnover to disk.
    """
    history = _load_json(SCAN_HISTORY_FILE)
    market_data = {}
    for _, row in snapshot_df.iterrows():
        code = row.get("code")
        turnover = row.get("turnover", 0) or 0
        if code:
            market_data[code] = {
                "turnover": float(turnover),
                "last_price": float(row.get("last_price", 0) or 0),
                "scanned_at": datetime.now().isoformat(),
            }
    history[market] = market_data
    _save_json(SCAN_HISTORY_FILE, history)


def get_prev_turnover(market: str, code: str) -> float:
    """Return the turnover value stored from the previous scan, or 0."""
    history = _load_json(SCAN_HISTORY_FILE)
    return history.get(market, {}).get(code, {}).get("turnover", 0.0)


def save_prev_day_turnover(market: str, snapshot_df):
    """
    Persist each stock's closing turnover for use as tomorrow's baseline.
    Call once at market close (16:00 HKT). Merges with existing data so
    other markets are not overwritten.
    """
    existing = _load_json(_PREV_DAY_FILE)
    market_data = {}
    for _, row in snapshot_df.iterrows():
        code = row.get("code")
        turnover = row.get("turnover", 0) or 0
        if code and turnover > 0:
            market_data[code] = float(turnover)
    existing[market] = market_data
    _save_json(_PREV_DAY_FILE, existing)
    print(f"[EOD] Saved {len(market_data)} turnover records for {market}")


def _get_yesterday_turnover(market: str, code: str) -> float:
    """Return yesterday's final turnover for a stock, or 0."""
    prev_day = _load_json(_PREV_DAY_FILE)
    return prev_day.get(market, {}).get(code, 0.0)


def prefetch_yesterday_turnover(market: str = "HK"):
    """
    Use yfinance to download yesterday's volume × close for all HK stocks
    and save to prev_day_turnover.json.  Call once at bot startup (before
    market open) so every stock has a real baseline from day one.

    Downloads in batches of 100 to avoid yfinance hanging on large requests.
    Only fetches stocks that are NOT already in the JSON for today's run.
    """
    import yfinance as yf
    import logging as _logging
    from futu import OpenQuoteContext, Market, SecurityType, RET_OK

    _logging.getLogger("yfinance").setLevel(_logging.CRITICAL)

    host = os.environ.get("FUTU_HOST", "127.0.0.1")
    port = int(os.environ.get("FUTU_PORT", "11111"))

    # 1. Get all stock codes from Futu
    ctx = OpenQuoteContext(host=host, port=port)
    try:
        market_map = {"US": Market.US, "HK": Market.HK, "CN": Market.SH}
        ret, stock_list = ctx.get_stock_basicinfo(
            market=market_map.get(market, Market.HK),
            stock_type=SecurityType.STOCK,
        )
        if ret != RET_OK or stock_list is None or stock_list.empty:
            print("[prefetch] Could not get stock list from Futu")
            return
        futu_codes = stock_list["code"].tolist()  # e.g. "HK.00700"
    finally:
        ctx.close()

    # 2. Skip codes we already have
    existing = _load_json(_PREV_DAY_FILE)
    existing_market = existing.get(market, {})
    missing_futu = [c for c in futu_codes if c not in existing_market]
    if not missing_futu:
        print(f"[prefetch] prev_day_turnover.json already has {len(existing_market)} codes — skipping")
        return

    # 3. Convert Futu codes → yfinance tickers  (HK.00700 → 0700.HK)
    def _to_yf(futu_code: str) -> str:
        parts = futu_code.split(".")
        if len(parts) == 2 and parts[0] == "HK":
            num = parts[1].lstrip("0").zfill(4)
            return f"{num}.HK"
        return futu_code

    yf_to_futu = {}
    for fc in missing_futu:
        yf_to_futu[_to_yf(fc)] = fc

    yf_tickers = list(yf_to_futu.keys())
    total = len(yf_tickers)
    print(f"[prefetch] Fetching yesterday volume for {total} stocks via yfinance...")

    # 4. Download in batches of 100 to avoid hangs
    BATCH_SIZE = 100
    saved = 0

    for i in range(0, total, BATCH_SIZE):
        batch = yf_tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        try:
            df = yf.download(
                batch,
                period="2d",
                group_by="ticker",
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"[prefetch] Batch {batch_num}/{total_batches} failed: {e}")
            continue

        if df is None or df.empty:
            continue

        for yf_tick in batch:
            futu_code = yf_to_futu.get(yf_tick)
            if not futu_code:
                continue
            try:
                if len(batch) == 1:
                    ticker_df = df
                else:
                    ticker_df = df[yf_tick]
                ticker_df = ticker_df.dropna(subset=["Close", "Volume"])
                if len(ticker_df) < 1:
                    continue
                row = ticker_df.iloc[-2] if len(ticker_df) >= 2 else ticker_df.iloc[-1]
                turnover = float(row["Volume"]) * float(row["Close"])
                if turnover > 0:
                    existing_market[futu_code] = turnover
                    saved += 1
            except Exception:
                continue

        # Save after each batch so partial progress is preserved
        existing[market] = existing_market
        _save_json(_PREV_DAY_FILE, existing)

        if batch_num % 10 == 0 or batch_num == total_batches:
            print(f"[prefetch] Batch {batch_num}/{total_batches} done — {saved} saved so far")

    print(f"[prefetch] Complete: {saved} turnover records ({len(existing_market)} total in JSON)")


# ─── Core scoring function ─────────────────────────────────────────────────────

def score_gap_up(row, market: str = "HK", trigger: str = "gap_up",
                 override_yesterday_turnover: float | None = None) -> dict:
    """
    Score a single stock row from get_market_snapshot.

    Args:
        row:      a snapshot row (pd.Series or dict-like)
        market:   market code
        trigger:  'gap_up' or 'intraday_breakout'
        override_yesterday_turnover:  if set, use this instead of the JSON lookup

    Returns a dict:
        score       int   0–12
        signals     dict  breakdown of each component
        alert_tier  str   'alert' | 'watch' | 'skip'
    """
    code          = row.get("code", "")
    last_price    = float(row.get("last_price", 0)    or 0)
    open_price    = float(row.get("open_price", 0)    or 0)
    prev_close    = float(row.get("prev_close_price", 0) or 0)
    high_price    = float(row.get("high_price", 0)    or 0)
    low_price     = float(row.get("low_price", 0)     or 0)
    change_5min   = float(row.get("change_5min", 0)   or 0)  # already in %
    turnover_now  = float(row.get("turnover", 0)      or 0)

    # ── Signal 1: Overnight gap (cap-adjusted) ───────────────────────────────
    # For intraday breakouts, use the intraday move (last vs open) as the
    # effective "gap" — the surge happened mid-session instead of overnight.
    gap_pct = 0.0
    if prev_close > 0 and open_price > 0:
        gap_pct = round((open_price - prev_close) / prev_close * 100, 6)

    if trigger == "intraday_breakout" and open_price > 0:
        effective_gap_pct = round((last_price - open_price) / open_price * 100, 6)
    else:
        effective_gap_pct = gap_pct

    # Cap-tier determines scoring thresholds directly
    market_cap = float(row.get("total_market_val", 0) or 0)
    if market_cap >= 5_000_000_000:        # HKD 5B+ large cap
        cap_tier = "large"
        gap_t1, gap_t2, gap_t3 = 3, 7, 15
        delta_t1, delta_t2, delta_t3 = 3, 8, 20
        delta_veto = 2.0
        vol_t1, vol_t2, vol_t3 = 1.3, 2.0, 3.5
    elif market_cap >= 500_000_000:        # HKD 500M–5B mid cap
        cap_tier = "mid"
        gap_t1, gap_t2, gap_t3 = 5, 12, 22
        delta_t1, delta_t2, delta_t3 = 5, 15, 30
        delta_veto = 3.0
        vol_t1, vol_t2, vol_t3 = 1.5, 3.0, 5.0
    else:                                  # <500M small / penny
        cap_tier = "small"
        gap_t1, gap_t2, gap_t3 = 10, 20, 35
        delta_t1, delta_t2, delta_t3 = 10, 25, 50
        delta_veto = 5.0
        vol_t1, vol_t2, vol_t3 = 2.5, 5.0, 8.0

    gap_score = 0
    if effective_gap_pct >= gap_t3:
        gap_score = 3
    elif effective_gap_pct >= gap_t2:
        gap_score = 2
    elif effective_gap_pct >= gap_t1:
        gap_score = 1

    # ── Signal 2: Continuation (holding or extending the gap) ───────────────
    continuation_pct = 0.0
    if open_price > 0:
        continuation_pct = round((last_price - open_price) / open_price * 100, 6)

    cont_score = 0
    if continuation_pct >= 3:
        cont_score = 2
    elif continuation_pct >= 0:
        cont_score = 1
    elif continuation_pct < -3:
        cont_score = -1     # actively fading — penalise

    # ── Signal 3: 5-minute momentum ─────────────────────────────────────────
    # Provided directly by Futu snapshot, no extra calls
    momentum_score = 0
    if change_5min >= 3:
        momentum_score = 1
    elif change_5min < -1:
        momentum_score = -1

    # ── Signal 4: Price strength in intraday range ───────────────────────────
    # 1.0 = at intraday high, 0.0 = at intraday low
    price_strength = 0.5
    if high_price > low_price:
        price_strength = (last_price - low_price) / (high_price - low_price)

    strength_score = 0
    if price_strength >= 0.75:
        strength_score = 1
    elif price_strength < 0.25:
        strength_score = -1

    # ── Signal 5: Turnover delta — time-normalised ─────────────────────────
    prev_turnover = get_prev_turnover(market, code)
    if prev_turnover == 0.0:
        turnover_delta = turnover_now
    else:
        turnover_delta = max(0.0, turnover_now - prev_turnover)

    # Session elapsed fraction (HK: 09:30–12:00 + 13:00–16:00 = 330 min)
    now_hkt = datetime.utcnow() + timedelta(hours=8)
    open_ref = now_hkt.replace(hour=9, minute=30, second=0, microsecond=0)
    raw_elapsed = max((now_hkt - open_ref).total_seconds() / 60, 1.0)
    elapsed_fraction = min(raw_elapsed, 330.0) / 330.0

    # Projected full-day turnover from today's pace
    projected_today = turnover_now / max(elapsed_fraction, 0.05)

    # Load yesterday's closing turnover as baseline
    if override_yesterday_turnover is not None:
        yesterday_turnover = override_yesterday_turnover
    else:
        yesterday_turnover = _get_yesterday_turnover(market, code)

    # Prefer yesterday as denominator when available — avoids the
    # self-referential problem where projected_today inflates the
    # denominator and suppresses delta_pct on surging stocks.
    if yesterday_turnover > 0:
        expected_turnover = yesterday_turnover
    else:
        expected_turnover = max(projected_today, 1.0)

    # Delta as % of expected full-day
    turnover_delta_pct = (turnover_delta / expected_turnover * 100) if expected_turnover > 0 else 0.0

    delta_score = 0
    if turnover_delta_pct >= delta_t3:
        delta_score = 3
    elif turnover_delta_pct >= delta_t2:
        delta_score = 2
    elif turnover_delta_pct >= delta_t1:
        delta_score = 1
    # 0 for anything below — including ghost moves on zero volume

    # ── Signal 6: Unusual volume vs yesterday ────────────────────────────────
    if yesterday_turnover > 0:
        unusual_multiplier = projected_today / yesterday_turnover
    else:
        unusual_multiplier = 1.0  # no baseline → neutral

    unusual_score = 0
    if unusual_multiplier >= vol_t3:
        unusual_score = 3
    elif unusual_multiplier >= vol_t2:
        unusual_score = 2
    elif unusual_multiplier >= vol_t1:
        unusual_score = 1

    # ── Composite ────────────────────────────────────────────────────────────
    score = (gap_score + cont_score + momentum_score + strength_score
             + delta_score + unusual_score)

    # Hard veto: catches ghost trades and manipulated single-lot prints.
    # Gate 1: New flow since last scan is tiny AND today's total activity is
    #         also low relative to yesterday — real breakouts with substantial
    #         cumulative turnover should not be vetoed just because the delta
    #         between the last two scan intervals was small.
    if (prev_turnover > 0
            and turnover_delta_pct < delta_veto
            and yesterday_turnover > 0
            and (turnover_now / yesterday_turnover) < 0.5
            and score > 3):
        score = 3
    # Gate 2: Total turnover today is absolutely tiny (clear ghost/illiquid).
    if turnover_now < 50_000 and score > 3:
        score = 3

    # ── Breakout bonus (intraday breakouts only) ────────────────────────────
    # Gap_score already uses the intraday move as effective_gap_pct, so the
    # bonus only adds extra credit for very large breakouts (20%+) to avoid
    # double-counting. Also requires minimum turnover to filter penny noise.
    breakout_bonus = 0
    if trigger == "intraday_breakout" and open_price > 0:
        intraday_move = (last_price - open_price) / open_price * 100
        # Only award bonus for exceptional moves
        if intraday_move >= 20:
            breakout_bonus = 2
        # Hard filter: breakouts must have real turnover
        if turnover_now < 500_000:
            score = min(score, 3)
        score += breakout_bonus

    # Clamp to 0–12
    score = max(0, min(12, score))

    # Same thresholds for both paths
    if score >= MIN_SCORE_ALERT:
        tier = "alert"
    elif score >= MIN_SCORE_WATCH:
        tier = "watch"
    else:
        tier = "skip"

    # Check veto state
    veto_applied = None
    raw_score = (gap_score + cont_score + momentum_score + strength_score
                 + delta_score + unusual_score + breakout_bonus)
    if (prev_turnover > 0
            and turnover_delta_pct < delta_veto
            and yesterday_turnover > 0
            and (turnover_now / yesterday_turnover) < 0.5
            and raw_score > 3):
        veto_applied = "ghost_trade"
    elif turnover_now < 50_000 and raw_score > 3:
        veto_applied = "illiquid"
    elif trigger == "intraday_breakout" and turnover_now < 500_000 and raw_score > 3:
        veto_applied = "breakout_low_turnover"

    return {
        "code":              code,
        "score":             score,
        "alert_tier":        tier,
        "signals": {
            "gap_pct":             round(gap_pct, 2),
            "effective_gap_pct":   round(effective_gap_pct, 2),
            "cap_tier":            cap_tier,
            "market_cap":          market_cap,
            "gap_thresholds":      (gap_t1, gap_t2, gap_t3),
            "gap_score":           gap_score,
            "continuation_pct":    round(continuation_pct, 2),
            "cont_score":          cont_score,
            "change_5min":         round(change_5min, 2),
            "momentum_score":      momentum_score,
            "price_strength":      round(price_strength, 2),
            "strength_score":      strength_score,
            "prev_scan_turnover":  round(prev_turnover, 0),
            "turnover_now":        round(turnover_now, 0),
            "turnover_delta_hkd":  round(turnover_delta, 0),
            "elapsed_min":         round(raw_elapsed, 1),
            "elapsed_fraction":    round(elapsed_fraction, 4),
            "expected_turnover":   round(expected_turnover, 0),
            "yesterday_turnover":  round(yesterday_turnover, 0),
            "turnover_delta_pct":  round(turnover_delta_pct, 1),
            "delta_thresholds":    (delta_t1, delta_t2, delta_t3),
            "delta_veto":          delta_veto,
            "delta_score":         delta_score,
            "projected_turnover":  round(projected_today, 0),
            "unusual_multiplier":  round(unusual_multiplier, 1),
            "volume_thresholds":   (vol_t1, vol_t2, vol_t3),
            "unusual_score":       unusual_score,
            "breakout_bonus":     breakout_bonus,
            "raw_score":          raw_score,
            "veto_applied":       veto_applied,
            "trigger":            trigger,
        },
    }


# ─── Intraday scoring (no overnight gap) ──────────────────────────────────────

def score_intraday(row, market: str = "HK",
                   override_yesterday_turnover: float | None = None) -> dict:
    """
    Score a stock based on intraday activity only — ignores overnight gap.

    Signal 1 becomes "Intraday Move" (last vs open) instead of gap.
    Otherwise identical structure to score_gap_up.

    Returns a dict:
        score       int   0–12
        signals     dict  breakdown
        alert_tier  str   'alert' | 'watch' | 'skip'
    """
    code          = row.get("code", "")
    last_price    = float(row.get("last_price", 0)    or 0)
    open_price    = float(row.get("open_price", 0)    or 0)
    prev_close    = float(row.get("prev_close_price", 0) or 0)
    high_price    = float(row.get("high_price", 0)    or 0)
    low_price     = float(row.get("low_price", 0)     or 0)
    change_5min   = float(row.get("change_5min", 0)   or 0)
    turnover_now  = float(row.get("turnover", 0)      or 0)

    # ── Signal 1: Intraday move (cap-tier-specific thresholds) ────────────────
    intraday_pct = 0.0
    if open_price > 0:
        intraday_pct = round((last_price - open_price) / open_price * 100, 6)

    market_cap = float(row.get("total_market_val", 0) or 0)
    if market_cap >= 5_000_000_000:          # HKD 5B+ large cap
        cap_tier = "large"
        move_t1, move_t2, move_t3 = 3, 7, 15
        delta_t1, delta_t2, delta_t3 = 3, 8, 20
        delta_veto = 2.0
        vol_t1, vol_t2, vol_t3 = 1.3, 2.0, 3.5
    elif market_cap >= 500_000_000:          # HKD 500M–5B mid cap
        cap_tier = "mid"
        move_t1, move_t2, move_t3 = 8, 18, 30
        delta_t1, delta_t2, delta_t3 = 5, 15, 30
        delta_veto = 3.0
        vol_t1, vol_t2, vol_t3 = 1.5, 3.0, 5.0
    else:                                    # <500M small / penny
        cap_tier = "small"
        move_t1, move_t2, move_t3 = 15, 30, 50
        delta_t1, delta_t2, delta_t3 = 10, 25, 50
        delta_veto = 5.0
        vol_t1, vol_t2, vol_t3 = 2.5, 5.0, 8.0

    move_score = 0
    if intraday_pct >= move_t3:
        move_score = 3
    elif intraday_pct >= move_t2:
        move_score = 2
    elif intraday_pct >= move_t1:
        move_score = 1

    # ── Signal 2: Daily change strength (cap-adjusted) ──────────────────────
    daily_chg = 0.0
    if prev_close > 0:
        daily_chg = round((last_price - prev_close) / prev_close * 100, 6)

    if cap_tier == "large":
        daily_neg, daily_t1, daily_t2 = -2, 3, 7
    elif cap_tier == "mid":
        daily_neg, daily_t1, daily_t2 = -3, 8, 18
    else:
        daily_neg, daily_t1, daily_t2 = -5, 15, 30

    daily_score = 0
    if daily_chg >= daily_t2:
        daily_score = 2
    elif daily_chg >= daily_t1:
        daily_score = 1
    elif daily_chg < daily_neg:
        daily_score = -1

    # ── Signal 3: 5-minute momentum ─────────────────────────────────────────
    momentum_score = 0
    if change_5min >= 3:
        momentum_score = 1
    elif change_5min < -1:
        momentum_score = -1

    # ── Signal 4: Price strength in intraday range ───────────────────────────
    price_strength = 0.5
    if high_price > low_price:
        price_strength = (last_price - low_price) / (high_price - low_price)

    strength_score = 0
    if price_strength >= 0.75:
        strength_score = 1
    elif price_strength < 0.25:
        strength_score = -1

    # ── Signal 5: Turnover delta ────────────────────────────────────────────
    prev_turnover = get_prev_turnover(market, code)
    if prev_turnover == 0.0:
        turnover_delta = turnover_now
    else:
        turnover_delta = max(0.0, turnover_now - prev_turnover)

    now_hkt = datetime.utcnow() + timedelta(hours=8)
    open_ref = now_hkt.replace(hour=9, minute=30, second=0, microsecond=0)
    raw_elapsed = max((now_hkt - open_ref).total_seconds() / 60, 1.0)
    elapsed_fraction = min(raw_elapsed, 330.0) / 330.0

    projected_today = turnover_now / max(elapsed_fraction, 0.05)

    if override_yesterday_turnover is not None:
        yesterday_turnover = override_yesterday_turnover
    else:
        yesterday_turnover = _get_yesterday_turnover(market, code)

    if yesterday_turnover > 0:
        expected_turnover = yesterday_turnover
    else:
        expected_turnover = max(projected_today, 1.0)

    turnover_delta_pct = (turnover_delta / expected_turnover * 100) if expected_turnover > 0 else 0.0

    delta_score = 0
    if turnover_delta_pct >= delta_t3:
        delta_score = 3
    elif turnover_delta_pct >= delta_t2:
        delta_score = 2
    elif turnover_delta_pct >= delta_t1:
        delta_score = 1

    # ── Signal 6: Unusual volume vs yesterday ────────────────────────────────
    if yesterday_turnover > 0:
        unusual_multiplier = projected_today / yesterday_turnover
    else:
        unusual_multiplier = 1.0

    unusual_score = 0
    if unusual_multiplier >= vol_t3:
        unusual_score = 3
    elif unusual_multiplier >= vol_t2:
        unusual_score = 2
    elif unusual_multiplier >= vol_t1:
        unusual_score = 1

    # ── Composite ────────────────────────────────────────────────────────────
    score = (move_score + daily_score + momentum_score + strength_score
             + delta_score + unusual_score)

    # Hard veto: ghost trades / illiquid
    if (prev_turnover > 0
            and turnover_delta_pct < delta_veto
            and yesterday_turnover > 0
            and (turnover_now / yesterday_turnover) < 0.5
            and score > 3):
        score = 3
    if turnover_now < 50_000 and score > 3:
        score = 3

    score = max(0, min(12, score))

    if score >= MIN_SCORE_ALERT:
        tier = "alert"
    elif score >= MIN_SCORE_WATCH:
        tier = "watch"
    else:
        tier = "skip"

    # Check which veto was applied
    veto_applied = None
    raw_score = (move_score + daily_score + momentum_score + strength_score
                 + delta_score + unusual_score)
    if (prev_turnover > 0
            and turnover_delta_pct < delta_veto
            and yesterday_turnover > 0
            and (turnover_now / yesterday_turnover) < 0.5
            and raw_score > 3):
        veto_applied = "ghost_trade"
    elif turnover_now < 50_000 and raw_score > 3:
        veto_applied = "illiquid"

    return {
        "code":              code,
        "score":             score,
        "alert_tier":        tier,
        "signals": {
            "intraday_pct":        round(intraday_pct, 2),
            "cap_tier":            cap_tier,
            "market_cap":          market_cap,
            "move_thresholds":     (move_t1, move_t2, move_t3),
            "move_score":          move_score,
            "daily_chg":           round(daily_chg, 2),
            "daily_thresholds":    (daily_neg, daily_t1, daily_t2),
            "daily_score":         daily_score,
            "change_5min":         round(change_5min, 2),
            "momentum_score":      momentum_score,
            "price_strength":      round(price_strength, 2),
            "strength_score":      strength_score,
            "prev_scan_turnover":  round(prev_turnover, 0),
            "turnover_now":        round(turnover_now, 0),
            "turnover_delta_hkd":  round(turnover_delta, 0),
            "elapsed_min":         round(raw_elapsed, 1),
            "elapsed_fraction":    round(elapsed_fraction, 4),
            "expected_turnover":   round(expected_turnover, 0),
            "yesterday_turnover":  round(yesterday_turnover, 0),
            "turnover_delta_pct":  round(turnover_delta_pct, 1),
            "delta_thresholds":    (delta_t1, delta_t2, delta_t3),
            "delta_veto":          delta_veto,
            "delta_score":         delta_score,
            "projected_turnover":  round(projected_today, 0),
            "unusual_multiplier":  round(unusual_multiplier, 1),
            "volume_thresholds":   (vol_t1, vol_t2, vol_t3),
            "unusual_score":       unusual_score,
            "raw_score":           raw_score,
            "veto_applied":        veto_applied,
            "trigger":            "intraday",
        },
    }


# ─── Cooldown tracker ─────────────────────────────────────────────────────────

def is_on_cooldown(code: str) -> bool:
    """True if we fired an alert for this stock within COOLDOWN_MINUTES."""
    log = _load_json(ALERT_LOG_FILE)
    entry = log.get(code)
    if not entry:
        return False
    last_alerted = datetime.fromisoformat(entry["last_alerted"])
    return datetime.now() - last_alerted < timedelta(minutes=COOLDOWN_MINUTES)


def mark_alerted(code: str, score: int, name: str):
    log = _load_json(ALERT_LOG_FILE)
    log[code] = {
        "last_alerted": datetime.now().isoformat(),
        "score": score,
        "name": name,
    }
    _save_json(ALERT_LOG_FILE, log)


# ─── Telegram sender ──────────────────────────────────────────────────────────

def _format_large_number(val: float, currency: str = "") -> str:
    """Format a large number with B/M/k suffix."""
    prefix = f"{currency}" if currency else ""
    if val >= 1_000_000_000:
        return f"{prefix}{val/1_000_000_000:.1f}B"
    elif val >= 1_000_000:
        return f"{prefix}{val/1_000_000:.1f}M"
    elif val >= 1_000:
        return f"{prefix}{val/1_000:.0f}k"
    return f"{prefix}{val:.0f}"


def _format_turnover(hkd: float) -> str:
    return _format_large_number(hkd, "HK$")


def build_telegram_message(row, result: dict) -> str:
    """Construct a compact Telegram alert message."""
    sig   = result["signals"]
    code  = result["code"]
    name  = str(row.get("name", ""))[:20]
    price = float(row.get("last_price", 0) or 0)
    daily = float(row.get("change_rate", 0) or 0)
    mkt_cap = float(row.get("total_market_val", 0) or 0)
    volume  = int(row.get("volume", 0) or 0)
    turnover = float(row.get("turnover", 0) or 0)

    trigger    = result.get("trigger", "gap_up")
    tier_emoji = "🔥" if result["alert_tier"] == "alert" else "📊"
    type_label = "Gap-Up" if trigger == "gap_up" else "⚡ Intraday Breakout"

    cap_label = sig.get('cap_tier', 'small').title()

    lines = [
        f"{tier_emoji} *{code}* — {name}  ({type_label}  `[{cap_label}-cap]`)",
        f"",
        f"💰 Price: HK${price:.3f}",
        f"📈 Daily Change: {daily:+.1f}%",
        f"🏢 Market Cap: {_format_large_number(mkt_cap, 'HK$')}",
        f"",
        f"📊 Gap: {sig['gap_pct']:+.1f}%  →  Continuation: {sig['continuation_pct']:+.1f}%",
        f"⚡ 5-min: {sig['change_5min']:+.1f}%  |  Strength: {sig['price_strength']:.0%}",
        f"",
        f"🔄 Volume: {_format_large_number(volume)}",
        f"💵 Turnover: {_format_large_number(turnover, 'HK$')}",
        f"🆕 New Flow: {_format_turnover(sig['turnover_delta_hkd'])} ({sig['turnover_delta_pct']:.0f}% of daily)",
        f"📡 Vol vs Yesterday: {sig['unusual_multiplier']:.1f}x normal",
        f"",
        f"⭐ Score: {result['score']}/12",
    ]
    return "\n".join(lines)


def build_intraday_message(row, result: dict) -> str:
    """Construct a Telegram alert for intraday movers (no gap info)."""
    sig   = result["signals"]
    code  = result["code"]
    name  = str(row.get("name", ""))[:20]
    price = float(row.get("last_price", 0) or 0)
    daily = sig.get("daily_chg", 0)
    mkt_cap = float(row.get("total_market_val", 0) or 0)
    volume  = int(row.get("volume", 0) or 0)
    turnover = float(row.get("turnover", 0) or 0)

    tier_emoji = "🔥" if result["alert_tier"] == "alert" else "📊"

    cap_label = sig.get('cap_tier', 'small').title()

    lines = [
        f"{tier_emoji} *{code}* — {name}  (Intraday Mover  `[{cap_label}-cap]`)",
        f"",
        f"💰 Price: HK${price:.3f}",
        f"📈 Daily Change: {daily:+.1f}%",
        f"🏢 Market Cap: {_format_large_number(mkt_cap, 'HK$')}",
        f"",
        f"📊 Intraday Move: {sig['intraday_pct']:+.1f}%",
        f"⚡ 5-min: {sig['change_5min']:+.1f}%  |  Strength: {sig['price_strength']:.0%}",
        f"",
        f"🔄 Volume: {_format_large_number(volume)}",
        f"💵 Turnover: {_format_large_number(turnover, 'HK$')}",
        f"🆕 New Flow: {_format_turnover(sig['turnover_delta_hkd'])} ({sig['turnover_delta_pct']:.0f}% of daily)",
        f"📡 Vol vs Yesterday: {sig['unusual_multiplier']:.1f}x normal",
        f"",
        f"⭐ Score: {result['score']}/12",
    ]
    return "\n".join(lines)


def send_telegram(bot_token: str, chat_id: str, message: str):
    """
    Fire a Telegram message. Requires: pip install requests
    Get bot_token from @BotFather, chat_id from @userinfobot.
    """
    import requests
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       message,
        "parse_mode": "Markdown",
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Failed to send: {e}")
        return False
