#!/usr/bin/env python3
"""
backtest.py — Backtest the gap-up scoring algorithm against historical data.

Uses yfinance daily OHLCV to simulate the scanner on past trading days,
then checks whether flagged stocks actually continued rising.

Usage:
    python backtest.py              # Run full backtest
    python backtest.py --audit      # Just print scoring audit (no download)
"""

import sys
import warnings
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ──────────────────────────────────────────────────────────────────────────────
# 1.  SCORING ALGORITHM — reproduced here so we can test it in isolation
# ──────────────────────────────────────────────────────────────────────────────

# Thresholds (must match gap_scorer.py)
MIN_SCORE_ALERT = 8
MIN_SCORE_WATCH = 4
DELTA_PCT_VETO  = 3.0
DELTA_PCT_TIER1 = 5.0
DELTA_PCT_TIER2 = 15.0
DELTA_PCT_TIER3 = 30.0


def score_gap_up_backtest(
    open_price: float,
    close_price: float,   # proxy for "last_price" in live
    prev_close: float,
    high_price: float,
    low_price: float,
    turnover_today: float,
    turnover_yesterday: float,
    market_cap: float = 0.0,
    change_5min: float = 0.0,   # unavailable in daily data, default 0
    prev_scan_turnover: float = 0.0,
) -> dict:
    """
    Pure-function replica of score_gap_up() for backtesting.
    Returns the same dict structure as the live scorer.
    """

    # ── Signal 1: Overnight gap (cap-adjusted) ──────────────────────────
    gap_pct = 0.0
    if prev_close > 0 and open_price > 0:
        gap_pct = round((open_price - prev_close) / prev_close * 100, 6)

    if market_cap >= 5_000_000_000:
        gap_adj = gap_pct * 2.5
        cap_tier = "large"
    elif market_cap >= 500_000_000:
        gap_adj = gap_pct * 1.5
        cap_tier = "mid"
    else:
        gap_adj = gap_pct
        cap_tier = "small"

    gap_score = 0
    if gap_adj >= 20:
        gap_score = 3
    elif gap_adj >= 10:
        gap_score = 2
    elif gap_adj >= 5:
        gap_score = 1

    # ── Signal 2: Continuation ──────────────────────────────────────────
    continuation_pct = 0.0
    if open_price > 0:
        continuation_pct = round((close_price - open_price) / open_price * 100, 6)

    cont_score = 0
    if continuation_pct >= 3:
        cont_score = 2
    elif continuation_pct >= 0:
        cont_score = 1
    elif continuation_pct < -3:
        cont_score = -1

    # ── Signal 3: 5-min momentum ────────────────────────────────────────
    momentum_score = 0
    if change_5min >= 3:
        momentum_score = 1
    elif change_5min < -1:
        momentum_score = -1

    # ── Signal 4: Price strength ────────────────────────────────────────
    price_strength = 0.5
    if high_price > low_price:
        price_strength = (close_price - low_price) / (high_price - low_price)

    strength_score = 0
    if price_strength >= 0.75:
        strength_score = 1
    elif price_strength < 0.25:
        strength_score = -1

    # ── Signal 5: Turnover delta ────────────────────────────────────────
    if prev_scan_turnover == 0.0:
        turnover_delta = turnover_today
    else:
        turnover_delta = max(0.0, turnover_today - prev_scan_turnover)

    if turnover_yesterday > 0:
        expected_turnover = turnover_yesterday
    else:
        expected_turnover = max(turnover_today, 1.0)

    turnover_delta_pct = (
        (turnover_delta / expected_turnover * 100) if expected_turnover > 0 else 0.0
    )

    delta_score = 0
    if turnover_delta_pct >= DELTA_PCT_TIER3:
        delta_score = 3
    elif turnover_delta_pct >= DELTA_PCT_TIER2:
        delta_score = 2
    elif turnover_delta_pct >= DELTA_PCT_TIER1:
        delta_score = 1

    # ── Signal 6: Unusual volume ────────────────────────────────────────
    if turnover_yesterday > 0:
        unusual_multiplier = turnover_today / turnover_yesterday
    else:
        unusual_multiplier = 1.0

    unusual_score = 0
    if unusual_multiplier >= 5.0:
        unusual_score = 3
    elif unusual_multiplier >= 3.0:
        unusual_score = 2
    elif unusual_multiplier >= 1.5:
        unusual_score = 1

    # ── Composite ───────────────────────────────────────────────────────
    raw_score = (
        gap_score + cont_score + momentum_score +
        strength_score + delta_score + unusual_score
    )

    # ── Hard veto ───────────────────────────────────────────────────────
    # SPEC: "Either condition caps score at 3" — applied to ALL scores
    vetoed = False
    if turnover_delta_pct < DELTA_PCT_VETO or turnover_delta < 50_000:
        if raw_score > 3:
            raw_score = 3
            vetoed = True

    score = max(0, min(12, raw_score))  # clamp 0–12

    if score >= MIN_SCORE_ALERT:
        tier = "alert"
    elif score >= MIN_SCORE_WATCH:
        tier = "watch"
    else:
        tier = "skip"

    return {
        "score": score,
        "alert_tier": tier,
        "vetoed": vetoed,
        "signals": {
            "gap_pct": round(gap_pct, 2),
            "gap_adj": round(gap_adj, 2),
            "cap_tier": cap_tier,
            "gap_score": gap_score,
            "continuation_pct": round(continuation_pct, 2),
            "cont_score": cont_score,
            "change_5min": round(change_5min, 2),
            "momentum_score": momentum_score,
            "price_strength": round(price_strength, 2),
            "strength_score": strength_score,
            "turnover_delta_pct": round(turnover_delta_pct, 1),
            "delta_score": delta_score,
            "unusual_multiplier": round(unusual_multiplier, 2),
            "unusual_score": unusual_score,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# 2.  AUDIT — compare live scorer to spec
# ──────────────────────────────────────────────────────────────────────────────

def run_audit():
    """Unit-test style check of each signal against the spec."""
    print("=" * 72)
    print("  SCORING ALGORITHM AUDIT — spec vs implementation")
    print("=" * 72)

    bugs = []

    # ── Test 1: Gap scoring thresholds ──────────────────
    print("\n[Signal 1] Overnight Gap (0–3 pts)")
    test_cases_gap = [
        # (gap_adj, expected_score)
        (4.9, 0), (5.0, 1), (9.9, 1), (10.0, 2), (19.9, 2), (20.0, 3), (20.1, 3),
    ]
    for gap_adj, expected in test_cases_gap:
        # Create inputs that produce the desired gap_adj for small-cap
        open_p = 1.0 + gap_adj / 100
        r = score_gap_up_backtest(
            open_price=open_p, close_price=open_p, prev_close=1.0,
            high_price=open_p, low_price=open_p,
            turnover_today=1e8, turnover_yesterday=1e7,
            market_cap=100_000_000,  # small cap → no adjustment
        )
        actual = r["signals"]["gap_score"]
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            bugs.append(f"Gap score: adj={gap_adj}% expected={expected} got={actual}")
        print(f"  adj gap={gap_adj:5.1f}%  expected={expected}  got={actual}  {status}")

    # ── Test 2: Cap adjustment ──────────────────────────
    print("\n[Signal 1b] Cap Adjustment Multipliers")
    # 2% raw gap on large-cap → 2%×2.5=5% → 1 pt
    r = score_gap_up_backtest(
        open_price=1.02, close_price=1.02, prev_close=1.0,
        high_price=1.02, low_price=1.02,
        turnover_today=1e8, turnover_yesterday=1e7,
        market_cap=10_000_000_000,
    )
    expected, actual = 1, r["signals"]["gap_score"]
    status = "✅" if actual == expected else "❌ BUG"
    if actual != expected:
        bugs.append(f"Large-cap adj: 2% raw → 5% adj, expected 1pt, got {actual}")
    print(f"  Large-cap 2% raw → {r['signals']['gap_adj']:.1f}% adj  score={actual}  {status}")

    # 4% raw gap on mid-cap → 4%×1.5=6% → 1 pt
    r = score_gap_up_backtest(
        open_price=1.04, close_price=1.04, prev_close=1.0,
        high_price=1.04, low_price=1.04,
        turnover_today=1e8, turnover_yesterday=1e7,
        market_cap=1_000_000_000,
    )
    expected, actual = 1, r["signals"]["gap_score"]
    status = "✅" if actual == expected else "❌ BUG"
    if actual != expected:
        bugs.append(f"Mid-cap adj: 4% raw → 6% adj, expected 1pt, got {actual}")
    print(f"  Mid-cap 4% raw → {r['signals']['gap_adj']:.1f}% adj  score={actual}  {status}")

    # ── Test 3: Continuation ────────────────────────────
    print("\n[Signal 2] Continuation (-1 to 2 pts)")
    test_cases_cont = [
        # (open, close, expected)
        (1.0, 0.96,  -1),  # -4% → fading
        (1.0, 0.969, -1),  # -3.1% → fading (below -3%)
        (1.0, 0.97,   0),  # -3% → boundary (exactly -3%, not below → neutral)
        (1.0, 0.98,   0),  # -2% → neutral (between -3 and 0)
        (1.0, 1.00,   1),  # 0% → holding
        (1.0, 1.02,   1),  # +2% → holding
        (1.0, 1.03,   2),  # +3% → extending
        (1.0, 1.10,   2),  # +10% → extending
    ]
    for open_p, close_p, expected in test_cases_cont:
        r = score_gap_up_backtest(
            open_price=open_p, close_price=close_p, prev_close=0.9,
            high_price=max(open_p, close_p), low_price=min(open_p, close_p),
            turnover_today=1e8, turnover_yesterday=1e7,
        )
        actual = r["signals"]["cont_score"]
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            cont_pct = (close_p - open_p) / open_p * 100
            bugs.append(f"Cont: {cont_pct:+.1f}% expected={expected} got={actual}")
        print(f"  open={open_p} close={close_p} ({(close_p-open_p)/open_p*100:+.1f}%)  "
              f"expected={expected:+d}  got={actual:+d}  {status}")

    # ── Test 4: 5-min momentum ──────────────────────────
    print("\n[Signal 3] 5-min Momentum (-1 to 1 pt)")
    test_cases_mom = [(-2.0, -1), (-1.1, -1), (-1.0, 0), (-0.5, 0), (0, 0), (2.9, 0), (3.0, 1), (5.0, 1)]
    for chg5, expected in test_cases_mom:
        r = score_gap_up_backtest(
            open_price=1.05, close_price=1.05, prev_close=1.0,
            high_price=1.05, low_price=1.05,
            turnover_today=1e8, turnover_yesterday=1e7,
            change_5min=chg5,
        )
        actual = r["signals"]["momentum_score"]
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            bugs.append(f"Momentum: 5min={chg5}% expected={expected} got={actual}")
        print(f"  5min={chg5:+.1f}%  expected={expected:+d}  got={actual:+d}  {status}")

    # ── Test 5: Price strength ──────────────────────────
    print("\n[Signal 4] Price Strength (-1 to 1 pt)")
    test_cases_str = [
        # (close, high, low, expected)
        (1.00, 1.10, 1.00, -1),  #  0% → at low (bottom quarter)
        (1.01, 1.10, 1.00, -1),  #  10% → bottom quarter
        (1.05, 1.10, 1.00, 0),   #  50% → middle
        (1.08, 1.10, 1.00, 1),   #  80% → top quarter
        (1.10, 1.10, 1.00, 1),   # 100% → at high
    ]
    for close_p, high_p, low_p, expected in test_cases_str:
        r = score_gap_up_backtest(
            open_price=1.05, close_price=close_p, prev_close=1.0,
            high_price=high_p, low_price=low_p,
            turnover_today=1e8, turnover_yesterday=1e7,
        )
        actual = r["signals"]["strength_score"]
        ps = (close_p - low_p) / (high_p - low_p) if high_p > low_p else 0.5
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            bugs.append(f"Strength: ps={ps:.2f} expected={expected} got={actual}")
        print(f"  strength={ps:.2f}  expected={expected:+d}  got={actual:+d}  {status}")

    # ── Test 6: Turnover delta ──────────────────────────
    print("\n[Signal 5] Turnover Delta (0–3 pts)")
    # yesterday=1M, today delta = varying %
    # NOTE: When prev_scan_turnover=0, turnover_delta = turnover_today (entire
    # day's accumulated turnover), not the marginal flow. In a first-scan
    # scenario, delta_pct = turnover_today / turnover_yesterday * 100.
    # So today=1.04M vs yest=1M gives delta_pct of 104%, not 4%.
    # The thresholds (5/15/30%) are designed for inter-scan deltas (a few min),
    # NOT full-day comparisons. The first scan of the day will over-score.
    test_cases_delta = [
        # (today, yesterday, expected) — simulating incremental delta
        # To get a 4% delta of yesterday, we use prev_scan = turnover - 0.04*yesterday
    ]
    # Test using the actual inter-scan scenario (prev_scan_turnover != 0)
    test_cases_delta_incremental = [
        # (today, yesterday, prev_scan, expected_delta_pct_approx, expected_score)
        (1_000_000, 1_000_000, 960_000, 4.0, 0),   # delta=40k, 4% of 1M
        (1_000_000, 1_000_000, 950_000, 5.0, 1),   # delta=50k, 5% of 1M
        (1_000_000, 1_000_000, 850_000, 15.0, 2),  # delta=150k, 15% of 1M
        (1_000_000, 1_000_000, 700_000, 30.0, 3),  # delta=300k, 30% of 1M
    ]
    for today_t, yest_t, prev_scan, expected_pct, expected in test_cases_delta_incremental:
        r = score_gap_up_backtest(
            open_price=1.05, close_price=1.05, prev_close=1.0,
            high_price=1.05, low_price=1.05,
            turnover_today=today_t, turnover_yesterday=yest_t,
            prev_scan_turnover=prev_scan,
        )
        actual = r["signals"]["delta_score"]
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            bugs.append(f"Delta: delta_pct={r['signals']['turnover_delta_pct']}% expected={expected} got={actual}")
        print(f"  today={today_t/1e6:.2f}M prev_scan={prev_scan/1e6:.2f}M yest={yest_t/1e6:.1f}M  "
              f"delta_pct={r['signals']['turnover_delta_pct']:.1f}%  "
              f"expected={expected}  got={actual}  {status}")

    # ── Test 7: Unusual volume ──────────────────────────
    print("\n[Signal 6] Unusual Volume (0–3 pts)")
    test_cases_uv = [
        (1.4, 0), (1.5, 1), (2.9, 1), (3.0, 2), (4.9, 2), (5.0, 3),
    ]
    for mult, expected in test_cases_uv:
        yest = 1_000_000
        today = yest * mult
        r = score_gap_up_backtest(
            open_price=1.05, close_price=1.05, prev_close=1.0,
            high_price=1.05, low_price=1.05,
            turnover_today=today, turnover_yesterday=yest,
        )
        actual = r["signals"]["unusual_score"]
        status = "✅" if actual == expected else "❌ BUG"
        if actual != expected:
            bugs.append(f"Unusual vol: {mult}x expected={expected} got={actual}")
        print(f"  {mult:.1f}x yesterday  expected={expected}  got={actual}  {status}")

    # ── Test 8: Hard veto ───────────────────────────────
    print("\n[Hard Veto] Caps score at 3 when turnover is thin")

    # Case A: High score but thin turnover delta → should be capped at 3
    r = score_gap_up_backtest(
        open_price=1.25, close_price=1.30, prev_close=1.0,
        high_price=1.30, low_price=1.25,
        turnover_today=30,     # effectively zero flow
        turnover_yesterday=100_000,
        change_5min=5.0,
    )
    expected_max = 3
    actual = r["score"]
    status = "✅" if actual <= expected_max else "❌ BUG"
    if actual > expected_max:
        bugs.append(f"Hard veto (thin delta): score={actual}, expected ≤{expected_max}")
    print(f"  Thin delta (<3%): raw would be high, capped={actual}  {status}")

    # Case B: Absolute flow < HK$50k → should be capped at 3
    r = score_gap_up_backtest(
        open_price=1.25, close_price=1.30, prev_close=1.0,
        high_price=1.30, low_price=1.25,
        turnover_today=40_000,
        turnover_yesterday=100_000,
        change_5min=5.0,
    )
    actual = r["score"]
    status = "✅" if actual <= expected_max else "❌ BUG"
    if actual > expected_max:
        bugs.append(f"Hard veto (abs <50k): score={actual}, expected ≤{expected_max}")
    print(f"  Abs flow <50k: score={actual}  {status}")

    # Case C: Watch-tier score (e.g. 6) with thin turnover should also be capped
    r = score_gap_up_backtest(
        open_price=1.10, close_price=1.12, prev_close=1.0,
        high_price=1.12, low_price=1.10,
        turnover_today=20_000,     # <50k absolute
        turnover_yesterday=10_000_000,
        change_5min=0,
    )
    actual = r["score"]
    status = "✅" if actual <= 3 else "❌ BUG"
    if actual > 3:
        bugs.append(f"Hard veto (watch-tier): score={actual}, expected ≤3")
    print(f"  Watch-tier thin turnover: score={actual}  {status}")

    # ── Test 9: Score clamping ──────────────────────────
    print("\n[Clamping] Score should be 0–12")
    # All negative signals with zero positives
    r = score_gap_up_backtest(
        open_price=1.001, close_price=0.95, prev_close=1.0,
        high_price=1.001, low_price=0.94,
        turnover_today=100, turnover_yesterday=1_000_000,
        change_5min=-5.0,
    )
    actual = r["score"]
    status = "✅" if actual >= 0 else "❌ BUG"
    if actual < 0:
        bugs.append(f"Score clamping: negative score {actual}")
    print(f"  All-negative signals: score={actual}  {status}")

    # ── Summary ─────────────────────────────────────────
    print("\n" + "=" * 72)
    if bugs:
        print(f"  ❌ {len(bugs)} BUG(S) FOUND:")
        for b in bugs:
            print(f"     • {b}")
    else:
        print("  ✅ All signal checks passed!")
    print("=" * 72)

    return bugs


# ──────────────────────────────────────────────────────────────────────────────
# 3.  LIVE SCORER AUDIT — test the actual gap_scorer.py code
# ──────────────────────────────────────────────────────────────────────────────

def audit_live_scorer():
    """Run the same tests against the LIVE score_gap_up from gap_scorer.py."""
    from scoring.gap_scorer import score_gap_up as live_score

    print("\n" + "=" * 72)
    print("  LIVE SCORER AUDIT (scoring/gap_scorer.py)")
    print("=" * 72)

    bugs = []

    def make_row(**kw):
        """Build a fake snapshot row (dict) for the live scorer."""
        defaults = {
            "code": "HK.99999",
            "last_price": 1.0,
            "open_price": 1.0,
            "prev_close_price": 1.0,
            "high_price": 1.0,
            "low_price": 1.0,
            "change_5min": 0.0,
            "turnover": 1_000_000,
            "market_val": 100_000_000,
            "total_market_val": 100_000_000,
            "change_rate": 0.0,
            "volume": 100_000,
            "name": "TEST",
        }
        defaults.update(kw)
        return defaults

    # ── Test A: Does change_5min actually get used? ──────────
    print("\n[Live] Signal 3: Does change_5min get used?")
    r0 = live_score(make_row(
        open_price=1.10, last_price=1.10, prev_close_price=1.0,
        high_price=1.10, low_price=1.10,
        change_5min=0.0, turnover=1e8, market_val=1e8,
    ))
    r1 = live_score(make_row(
        open_price=1.10, last_price=1.10, prev_close_price=1.0,
        high_price=1.10, low_price=1.10,
        change_5min=5.0, turnover=1e8, market_val=1e8,
    ))
    if r0["signals"]["momentum_score"] == r1["signals"]["momentum_score"]:
        bugs.append("Signal 3: change_5min has NO effect (both scores identical)")
        print(f"  ❌ BUG: change_5min=0 → momentum={r0['signals']['momentum_score']}, "
              f"change_5min=5 → momentum={r1['signals']['momentum_score']}  (SAME!)")
    else:
        print(f"  ✅ change_5min=0 → {r0['signals']['momentum_score']}, "
              f"change_5min=5 → {r1['signals']['momentum_score']}")

    # ── Test B: Hard veto applies to ALL scores, not just alert tier ──
    print("\n[Live] Hard Veto: should cap ANY score at 3 (not just ≥8)")

    # Create a stock that would score ~5-6 normally, with thin turnover
    r = live_score(make_row(
        code="HK.00001",
        open_price=1.10,
        last_price=1.13,
        prev_close_price=1.0,
        high_price=1.13,
        low_price=1.10,
        change_5min=0,
        turnover=20_000,     # <50k absolute → should trigger veto
        market_val=1e8,
    ))
    if r["score"] > 3:
        bugs.append(f"Hard veto: live scorer lets score={r['score']} through with "
                    f"turnover=20k (should cap at 3)")
        print(f"  ❌ BUG: score={r['score']} with turnover=HK$20k — veto not applied!")
    else:
        print(f"  ✅ score={r['score']} correctly capped")

    # ── Test C: Score clamping ──────────────────────────
    print("\n[Live] Score clamping: should be 0–12")
    r = live_score(make_row(
        code="HK.00002",
        open_price=1.001,
        last_price=0.95,
        prev_close_price=1.0,
        high_price=1.001,
        low_price=0.94,
        change_5min=-5.0,
        turnover=100,
        market_val=1e8,
    ))
    if r["score"] < 0:
        bugs.append(f"Score clamping: live scorer returned {r['score']} (negative!)")
        print(f"  ❌ BUG: score={r['score']} — not clamped to 0")
    else:
        print(f"  ✅ score={r['score']} (≥0)")

    # ── Test D: Maximum theoretical score ───────────────
    print("\n[Live] Max score: theoretical max should be ≤12")
    r = live_score(make_row(
        code="HK.00003",
        open_price=1.25,
        last_price=1.35,
        prev_close_price=1.0,
        high_price=1.35,
        low_price=1.25,
        change_5min=10.0,
        turnover=10_000_000,
        market_val=1e8,        # small cap, gap_pct=25 → 3pts
    ))
    print(f"  Max-signal score = {r['score']}  "
          f"(gap={r['signals']['gap_score']} cont={r['signals']['cont_score']} "
          f"mom={r['signals']['momentum_score']} str={r['signals']['strength_score']} "
          f"delta={r['signals']['delta_score']} vol={r['signals']['unusual_score']})")
    component_sum = (
        r['signals']['gap_score'] + r['signals']['cont_score'] +
        r['signals']['momentum_score'] + r['signals']['strength_score'] +
        r['signals']['delta_score'] + r['signals']['unusual_score']
    )
    if component_sum > 12:
        bugs.append(f"Max raw sum = {component_sum} (exceeds 12), "
                    f"but /algo says 0–12. Fix scale or clamp.")
        print(f"  ⚠️  Component sum = {component_sum} (>12) — scale inconsistency")

    if r["score"] > 12:
        bugs.append(f"Score {r['score']} exceeds 12 — no clamping!")
        print(f"  ❌ BUG: score={r['score']} > 12")

    # Summary
    print("\n" + "=" * 72)
    if bugs:
        print(f"  ❌ {len(bugs)} LIVE SCORER BUG(S):")
        for b in bugs:
            print(f"     • {b}")
    else:
        print("  ✅ Live scorer passed all checks!")
    print("=" * 72)

    return bugs


# ──────────────────────────────────────────────────────────────────────────────
# 4.  HISTORICAL BACKTEST
# ──────────────────────────────────────────────────────────────────────────────

# Top-traded HK stocks for backtest (diverse cap sizes)
BACKTEST_TICKERS = {
    # Large-cap
    "0700.HK": ("Tencent", 4_500_000_000_000),
    "9988.HK": ("Alibaba", 1_800_000_000_000),
    "0005.HK": ("HSBC", 1_200_000_000_000),
    "1299.HK": ("AIA", 700_000_000_000),
    "0941.HK": ("China Mobile", 1_400_000_000_000),
    "3690.HK": ("Meituan", 700_000_000_000),
    "9618.HK": ("JD.com", 350_000_000_000),
    "9888.HK": ("Baidu", 250_000_000_000),
    "2318.HK": ("Ping An", 800_000_000_000),
    "0388.HK": ("HKEX", 350_000_000_000),
    "1810.HK": ("Xiaomi", 600_000_000_000),
    "0027.HK": ("Galaxy Ent", 150_000_000_000),
    "2269.HK": ("WuXi Bio", 80_000_000_000),
    # Mid-cap
    "6060.HK": ("ZhongAn", 15_000_000_000),
    "1024.HK": ("Kuaishou", 200_000_000_000),
    "9626.HK": ("Bilibili", 60_000_000_000),
    "2015.HK": ("Li Auto", 200_000_000_000),
    "9868.HK": ("XPeng", 80_000_000_000),
    "0981.HK": ("SMIC", 350_000_000_000),
    "6618.HK": ("JD Health", 100_000_000_000),
    "9999.HK": ("NetEase", 400_000_000_000),
    "1211.HK": ("BYD", 800_000_000_000),
    "2382.HK": ("Sunny Optical", 70_000_000_000),
    # Small-cap / speculative
    "1585.HK": ("Yadea", 20_000_000_000),
    "2400.HK": ("XD Inc", 3_000_000_000),
    "6969.HK": ("Smoore", 30_000_000_000),
    "9901.HK": ("NIO", 70_000_000_000),
    "0772.HK": ("China Literature", 25_000_000_000),
    "6186.HK": ("China Feihe", 30_000_000_000),
    "1691.HK": ("JS Global", 10_000_000_000),
}


def download_data(tickers: list[str], days: int = 120) -> dict[str, pd.DataFrame]:
    """Download daily OHLCV from yfinance for backtesting."""
    import yfinance as yf

    print(f"\nDownloading {len(tickers)} tickers, last {days} days...")
    data = {}
    # Download in batches to avoid yfinance choking
    batch_size = 10
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            df = yf.download(
                batch,
                period=f"{days}d",
                group_by="ticker",
                progress=False,
                threads=True,
            )
            if df is None or df.empty:
                continue

            for tick in batch:
                try:
                    if len(batch) == 1:
                        ticker_df = df.copy()
                    else:
                        ticker_df = df[tick].copy()
                    ticker_df = ticker_df.dropna(subset=["Close", "Volume"])
                    if len(ticker_df) >= 10:
                        data[tick] = ticker_df
                except Exception:
                    continue
        except Exception as e:
            print(f"  Batch {i//batch_size+1} failed: {e}")

    print(f"  Got data for {len(data)} / {len(tickers)} tickers\n")
    return data


def run_backtest(data: dict[str, pd.DataFrame], ticker_info: dict) -> pd.DataFrame:
    """
    For each trading day, find gap-up stocks, score them,
    and track forward returns (1d, 3d, 5d from close of signal day).
    """
    results = []

    for ticker, df in data.items():
        name, approx_cap = ticker_info.get(ticker, (ticker, 0))
        df = df.reset_index()

        for i in range(1, len(df) - 5):  # need 5 forward days
            row = df.iloc[i]
            prev = df.iloc[i - 1]

            open_p = float(row["Open"])
            close_p = float(row["Close"])
            high_p = float(row["High"])
            low_p = float(row["Low"])
            prev_close = float(prev["Close"])
            vol_today = float(row["Volume"])
            vol_yesterday = float(prev["Volume"])

            if prev_close <= 0 or open_p <= 0:
                continue

            # Gap-up filter: open above prev close
            gap_pct = (open_p - prev_close) / prev_close * 100
            if gap_pct < 1.0:
                continue  # only test gap-ups ≥1%

            # Turnover approximation (volume × close)
            turnover_today = vol_today * close_p
            turnover_yesterday = vol_yesterday * float(prev["Close"])

            result = score_gap_up_backtest(
                open_price=open_p,
                close_price=close_p,
                prev_close=prev_close,
                high_price=high_p,
                low_price=low_p,
                turnover_today=turnover_today,
                turnover_yesterday=turnover_yesterday,
                market_cap=approx_cap,
                change_5min=0.0,   # not available in daily data
                prev_scan_turnover=0.0,  # simulate first scan of the day
            )

            # Forward returns (close-to-close)
            ret_1d = (float(df.iloc[i + 1]["Close"]) / close_p - 1) * 100
            ret_3d = (float(df.iloc[i + 3]["Close"]) / close_p - 1) * 100
            ret_5d = (float(df.iloc[i + 5]["Close"]) / close_p - 1) * 100

            # Intraday return (same day open→close)
            intraday_ret = (close_p / open_p - 1) * 100

            results.append({
                "date": row["Date"] if "Date" in df.columns else row.get("index", i),
                "ticker": ticker,
                "name": name,
                "cap_tier": result["signals"]["cap_tier"],
                "gap_pct": gap_pct,
                "score": result["score"],
                "tier": result["alert_tier"],
                "vetoed": result["vetoed"],
                "gap_score": result["signals"]["gap_score"],
                "cont_score": result["signals"]["cont_score"],
                "strength_score": result["signals"]["strength_score"],
                "delta_score": result["signals"]["delta_score"],
                "unusual_score": result["signals"]["unusual_score"],
                "continuation_pct": result["signals"]["continuation_pct"],
                "unusual_mult": result["signals"]["unusual_multiplier"],
                "intraday_ret": round(intraday_ret, 2),
                "ret_1d": round(ret_1d, 2),
                "ret_3d": round(ret_3d, 2),
                "ret_5d": round(ret_5d, 2),
            })

    return pd.DataFrame(results)


def print_backtest_report(df: pd.DataFrame):
    """Print comprehensive backtest statistics."""
    if df.empty:
        print("No gap-up signals found in backtest data.")
        return

    print("\n" + "=" * 80)
    print("  BACKTEST RESULTS")
    print("=" * 80)

    print(f"\n  Total gap-up signals (≥1%): {len(df)}")
    print(f"  Date range: {df['date'].min()} → {df['date'].max()}")
    print(f"  Tickers with signals: {df['ticker'].nunique()}")

    # ── Tier breakdown ──────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  TIER BREAKDOWN")
    print(f"{'─' * 60}")
    for tier in ["alert", "watch", "skip"]:
        subset = df[df["tier"] == tier]
        if subset.empty:
            print(f"\n  {tier.upper()} (score {'8+' if tier == 'alert' else '4-7' if tier == 'watch' else '<4'}): "
                  f"0 signals")
            continue

        n = len(subset)
        pct_pos_1d = (subset["ret_1d"] > 0).mean() * 100
        pct_pos_3d = (subset["ret_3d"] > 0).mean() * 100
        pct_pos_5d = (subset["ret_5d"] > 0).mean() * 100
        avg_1d = subset["ret_1d"].mean()
        avg_3d = subset["ret_3d"].mean()
        avg_5d = subset["ret_5d"].mean()
        med_1d = subset["ret_1d"].median()
        avg_intra = subset["intraday_ret"].mean()

        tier_emoji = "🔥" if tier == "alert" else "📊" if tier == "watch" else "⏭️"
        score_range = "8+" if tier == "alert" else "4–7" if tier == "watch" else "<4"

        print(f"\n  {tier_emoji} {tier.upper()} (score {score_range}):  "
              f"{n} signals  ({n/len(df)*100:.1f}%)")
        print(f"     Avg intraday (open→close): {avg_intra:+.2f}%")
        print(f"     Next-1d:  avg={avg_1d:+.2f}%  median={med_1d:+.2f}%  "
              f"win-rate={pct_pos_1d:.0f}%")
        print(f"     Next-3d:  avg={avg_3d:+.2f}%  win-rate={pct_pos_3d:.0f}%")
        print(f"     Next-5d:  avg={avg_5d:+.2f}%  win-rate={pct_pos_5d:.0f}%")

    # ── Score distribution ──────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  SCORE DISTRIBUTION")
    print(f"{'─' * 60}")
    for s in sorted(df["score"].unique()):
        sub = df[df["score"] == s]
        bar = "█" * max(1, len(sub) * 40 // len(df))
        avg_ret = sub["ret_1d"].mean()
        win = (sub["ret_1d"] > 0).mean() * 100
        print(f"  Score {s:2d}: {len(sub):4d} signals  "
              f"1d avg={avg_ret:+5.2f}%  win={win:4.0f}%  {bar}")

    # ── Signal contribution analysis ────────────────────
    print(f"\n{'─' * 60}")
    print(f"  SIGNAL CONTRIBUTION (correlation with next-1d return)")
    print(f"{'─' * 60}")
    for signal_col, label in [
        ("gap_score", "Gap Score"),
        ("cont_score", "Continuation"),
        ("strength_score", "Price Strength"),
        ("delta_score", "Turnover Delta"),
        ("unusual_score", "Unusual Volume"),
        ("score", "COMPOSITE"),
    ]:
        if signal_col in df.columns and len(df) > 10:
            corr = df[signal_col].corr(df["ret_1d"])
            corr_3d = df[signal_col].corr(df["ret_3d"])
            print(f"  {label:18s}:  1d corr={corr:+.3f}   3d corr={corr_3d:+.3f}")

    # ── Veto analysis ───────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  VETO ANALYSIS")
    print(f"{'─' * 60}")
    vetoed = df[df["vetoed"]]
    not_vetoed = df[~df["vetoed"]]
    print(f"  Vetoed signals: {len(vetoed)}")
    if not vetoed.empty:
        print(f"    Avg 1d return of vetoed:     {vetoed['ret_1d'].mean():+.2f}%")
    if not not_vetoed.empty:
        print(f"    Avg 1d return of non-vetoed:  {not_vetoed['ret_1d'].mean():+.2f}%")
    if not vetoed.empty and not not_vetoed.empty:
        diff = not_vetoed['ret_1d'].mean() - vetoed['ret_1d'].mean()
        print(f"    Veto effectiveness (return gap): {diff:+.2f}%")

    # ── Cap tier analysis ───────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  CAP TIER ANALYSIS")
    print(f"{'─' * 60}")
    for cap in ["large", "mid", "small"]:
        sub = df[df["cap_tier"] == cap]
        if sub.empty:
            continue
        avg_gap = sub["gap_pct"].mean()
        avg_ret = sub["ret_1d"].mean()
        win = (sub["ret_1d"] > 0).mean() * 100
        alerts = (sub["tier"] == "alert").sum()
        print(f"  {cap:6s}: {len(sub):4d} signals  "
              f"avg gap={avg_gap:+.1f}%  1d avg={avg_ret:+.2f}%  "
              f"win={win:.0f}%  alerts={alerts}")

    # ── Top alert examples ──────────────────────────────
    alerts = df[df["tier"] == "alert"].sort_values("score", ascending=False)
    if not alerts.empty:
        print(f"\n{'─' * 60}")
        print(f"  TOP ALERT EXAMPLES (score ≥8)")
        print(f"{'─' * 60}")
        print(f"  {'Date':<12} {'Ticker':<10} {'Name':<14} {'Gap%':>6} {'Score':>6} "
              f"{'Intra':>7} {'1d':>7} {'3d':>7} {'5d':>7}")
        for _, r in alerts.head(20).iterrows():
            date_str = str(r["date"])[:10]
            print(f"  {date_str:<12} {r['ticker']:<10} {r['name'][:13]:<14} "
                  f"{r['gap_pct']:+5.1f}% {r['score']:5d}  "
                  f"{r['intraday_ret']:+6.2f}% {r['ret_1d']:+6.2f}% "
                  f"{r['ret_3d']:+6.2f}% {r['ret_5d']:+6.2f}%")

    # ── Worst missed opportunities ──────────────────────
    skipped = df[(df["tier"] == "skip") & (df["ret_1d"] > 5)]
    if not skipped.empty:
        print(f"\n{'─' * 60}")
        print(f"  MISSED OPPORTUNITIES (skipped but rose >5% next day)")
        print(f"{'─' * 60}")
        skipped_sorted = skipped.sort_values("ret_1d", ascending=False)
        print(f"  {'Date':<12} {'Ticker':<10} {'Name':<14} {'Gap%':>6} {'Score':>6} "
              f"{'1d':>7} {'3d':>7}")
        for _, r in skipped_sorted.head(10).iterrows():
            date_str = str(r["date"])[:10]
            print(f"  {date_str:<12} {r['ticker']:<10} {r['name'][:13]:<14} "
                  f"{r['gap_pct']:+5.1f}% {r['score']:5d}  "
                  f"{r['ret_1d']:+6.2f}% {r['ret_3d']:+6.2f}%")

    print(f"\n{'=' * 80}\n")


# ──────────────────────────────────────────────────────────────────────────────
# 5.  MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    if "--audit" in sys.argv:
        run_audit()
        audit_live_scorer()
        return

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║          GAP-UP SCORING ALGORITHM BACKTEST                  ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    # Step 1: Audit
    print("\n▶ STEP 1/3: Algorithm Audit")
    audit_bugs = run_audit()
    live_bugs = audit_live_scorer()

    # Step 2: Download data
    print("\n▶ STEP 2/3: Download Historical Data")
    tickers = list(BACKTEST_TICKERS.keys())
    data = download_data(tickers, days=120)

    if not data:
        print("❌ No data downloaded. Check network connection.")
        return

    # Step 3: Run backtest
    print("▶ STEP 3/3: Run Backtest")
    results_df = run_backtest(data, BACKTEST_TICKERS)
    print_backtest_report(results_df)

    # Final summary
    total_bugs = len(audit_bugs) + len(live_bugs)
    print("=" * 80)
    print(f"  FINAL SUMMARY")
    print(f"  Algorithm bugs found: {total_bugs}")
    if audit_bugs:
        print(f"    Backtest scorer bugs: {len(audit_bugs)}")
    if live_bugs:
        print(f"    Live scorer bugs:     {len(live_bugs)}")
        for b in live_bugs:
            print(f"      → {b}")
    print("=" * 80)


if __name__ == "__main__":
    main()
