I have three files: `gap_scorer.py`, `top_gainers.py`, and `bot.py`. Apply the changes below exactly as described. Do not refactor, rename, or restructure anything not explicitly listed.

---

## File: gap_scorer.py

### Change 1 — Cap-adjust the overnight gap score in `score_gap_up()`

After computing `gap_pct` (line ~106), add the following before `gap_score` is assigned. Use `market_val` from the snapshot row (Futu field name). Keep `gap_pct` raw for display; use `gap_pct_for_scoring` only for the score calculation:

```python
market_cap = float(row.get("market_val", 0) or 0)
if market_cap >= 5_000_000_000:        # HKD 5B+ blue chip
    gap_pct_for_scoring = gap_pct * 2.5
    cap_tier = "large"
elif market_cap >= 500_000_000:        # HKD 500M–5B mid
    gap_pct_for_scoring = gap_pct * 1.5
    cap_tier = "mid"
else:
    gap_pct_for_scoring = gap_pct      # small / penny — no adjustment
    cap_tier = "small"
```

Replace the `gap_score` if/elif block to use `gap_pct_for_scoring` instead of `gap_pct`. Store both in the signals dict:

```python
"gap_pct":             round(gap_pct, 2),           # raw, for display
"gap_pct_adjusted":    round(gap_pct_for_scoring, 2), # cap-adjusted
"cap_tier":            cap_tier,
```

---

### Change 2 — Time-normalize the turnover delta in `score_gap_up()`

Replace the existing turnover delta section (from `prev_turnover = get_prev_turnover(...)` through `delta_score = 0` and the scoring if/elif block) with the following:

```python
# ── Signal 5: Turnover delta — time-normalised ───────────────────────────
from datetime import timezone

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
prev_day = _load_json("prev_day_turnover.json")
yesterday_turnover = prev_day.get(market, {}).get(code, 0.0)
expected_turnover = max(yesterday_turnover, projected_today, 1.0)

# Delta as % of expected full-day
turnover_delta_pct = (turnover_delta / expected_turnover * 100) if expected_turnover > 0 else 0.0

delta_score = 0
if turnover_delta_pct >= DELTA_PCT_TIER3:    # 30%+
    delta_score = 3
elif turnover_delta_pct >= DELTA_PCT_TIER2:  # 15%+
    delta_score = 2
elif turnover_delta_pct >= DELTA_PCT_TIER1:  # 5%+
    delta_score = 1
```

Also update the hard veto block to use the new `turnover_delta_pct` and add a penny-stock dual gate:

```python
# Hard veto: thin relative volume → cap score
if turnover_delta_pct < DELTA_PCT_VETO and score >= MIN_SCORE_ALERT:
    score = MIN_SCORE_WATCH - 1

# Dual gate: also require a minimum absolute flow (catches ghost trades)
if turnover_delta < 50_000 and score >= MIN_SCORE_ALERT:
    score = MIN_SCORE_WATCH - 1
```

Update the `signals` dict to include:

```python
"turnover_delta_hkd":  round(turnover_delta, 0),
"turnover_delta_pct":  round(turnover_delta_pct, 1),
"delta_score":         delta_score,
"projected_turnover":  round(projected_today, 0),
```

---

### Change 3 — Add Signal 6: unusual volume vs yesterday

After computing `delta_score`, add the following block. It reuses `projected_today` and `yesterday_turnover` already computed above:

```python
# ── Signal 6: Unusual volume vs yesterday ────────────────────────────────
if yesterday_turnover > 0:
    unusual_multiplier = projected_today / yesterday_turnover
else:
    unusual_multiplier = 1.0

unusual_score = 0
if unusual_multiplier >= 5.0:    unusual_score = 3
elif unusual_multiplier >= 3.0:  unusual_score = 2
elif unusual_multiplier >= 1.5:  unusual_score = 1
```

Add `unusual_score` to the composite:

```python
score = gap_score + cont_score + momentum_score + strength_score + delta_score + unusual_score
```

Add to signals dict:

```python
"unusual_multiplier":  round(unusual_multiplier, 1),
"unusual_score":       unusual_score,
```

Update the docstring return annotation from `0–9` to `0–12`.

Update the module-level constants:

```python
MIN_SCORE_ALERT   = 8    # was 6
MIN_SCORE_WATCH   = 4    # was 3
```

---

### Change 4 — Add `save_prev_day_turnover()` function

Add this new function after `save_scan_snapshot()`:

```python
def save_prev_day_turnover(market: str, snapshot_df):
    """
    Persist each stock's closing turnover for use as tomorrow's baseline.
    Call once at market close (16:00 HKT). Merges with existing data so
    other markets are not overwritten.
    """
    existing = _load_json("prev_day_turnover.json")
    market_data = {}
    for _, row in snapshot_df.iterrows():
        code = row.get("code")
        turnover = row.get("turnover", 0) or 0
        if code and turnover > 0:
            market_data[code] = float(turnover)
    existing[market] = market_data
    _save_json("prev_day_turnover.json", market_data)
    print(f"[EOD] Saved {len(market_data)} turnover records for {market}")
```

---

### Change 5 — Update `build_telegram_message()`

Replace the entire `lines` list with the following. Keep the `tier_emoji` logic as-is. The score is now out of 12:

```python
sig     = result["signals"]
code    = result["code"]
name    = str(row.get("name", ""))[:20]
price   = float(row.get("last_price", 0) or 0)
daily   = float(row.get("change_rate", 0) or 0)
mkt_cap = float(row.get("total_market_val", 0) or 0)
volume  = int(row.get("volume", 0) or 0)
turnover = float(row.get("turnover", 0) or 0)

trigger   = result.get("trigger", "gap_up")
tier_emoji = "🔥" if result["alert_tier"] == "alert" else "📊"
type_label = "Gap-Up" if trigger == "gap_up" else "⚡ Intraday Breakout"

cap_note = ""
if sig.get("cap_tier") == "large":
    cap_note = "  `[Large-cap ×2.5]`"
elif sig.get("cap_tier") == "mid":
    cap_note = "  `[Mid-cap ×1.5]`"

lines = [
    f"{tier_emoji} *{code}* — {name}  ({type_label})",
    f"",
    f"💰 Price: HK${price:.3f}",
    f"📈 Daily Change: {daily:+.1f}%",
    f"🏢 Market Cap: {_format_large_number(mkt_cap, 'HK$')}",
    f"",
    f"📊 Gap: {sig['gap_pct']:+.1f}%{cap_note}  →  Continuation: {sig['continuation_pct']:+.1f}%",
    f"⚡ 5-min: {sig['change_5min']:+.1f}%  |  Strength: {sig['price_strength']:.0%}",
    f"",
    f"🔄 Volume: {_format_large_number(volume)}",
    f"💵 Turnover: {_format_large_number(turnover, 'HK$')}",
    f"🆕 New Flow: {_format_turnover(sig['turnover_delta_hkd'])} ({sig['turnover_delta_pct']:.0f}% of daily)",
    f"📡 Vol vs Yesterday: {sig['unusual_multiplier']:.1f}x normal",
    f"",
    f"⭐ Score: {result['score']}/12",
]
```

---

## File: top_gainers.py

### Change 6 — Add intraday breakout path in `scan_gap_ups()`

In `scan_gap_ups()`, after the existing `alerts, watches = [], []` loop that processes `candidates`, add a second loop for intraday breakouts. Deduplicate by code so a stock only appears once (highest score wins):

```python
# ── Second path: intraday breakouts (opened flat, spiked mid-session) ────
already_seen = {r['code'] for _, r in alerts + watches}

if 'open_price' in snapshot.columns and 'last_price' in snapshot.columns:
    with pd.option_context('mode.chained_assignment', None):
        flat_open = snapshot[
            (snapshot['open_price'] > 0) &
            (snapshot['prev_close_price'] > 0) &
            (snapshot['open_price'] <= snapshot['prev_close_price'] * 1.02)
        ].copy()

        flat_open['intraday_move'] = (
            (flat_open['last_price'] - flat_open['open_price'])
            / flat_open['open_price'].replace(0, float('nan')) * 100
        )
        breakouts = flat_open[flat_open['intraday_move'] >= 5.0]

    for _, row in breakouts.iterrows():
        if row['code'] in already_seen:
            continue
        result = score_gap_up(row, market=market)
        result['trigger'] = 'intraday_breakout'
        if result['alert_tier'] == 'alert':
            alerts.append((row, result))
            already_seen.add(row['code'])
        elif result['alert_tier'] == 'watch':
            watches.append((row, result))
            already_seen.add(row['code'])

# Tag gap-up candidates with their trigger type
for row, result in alerts + watches:
    if 'trigger' not in result:
        result['trigger'] = 'gap_up'
```

---

### Change 7 — Add suspension veto in `scan_gap_ups()`

Before the loop that iterates over `candidates`, add:

```python
SUSPENDED_STATUS_KEYWORDS = {"SUSPEND", "HALT", "DELIST"}

def _is_suspended(row) -> bool:
    status = str(row.get("sec_status", "") or row.get("listing_status", "") or "")
    return any(k in status.upper() for k in SUSPENDED_STATUS_KEYWORDS)
```

Define this as a module-level helper inside `top_gainers.py`. Then in both the `candidates` loop and the breakouts loop, skip and log suspended stocks:

```python
if _is_suspended(row):
    print(f"  [RESUMPTION] {row.get('code')} — skipped (post-suspension), manual review recommended")
    continue
```

---

### Change 8 — Call `save_prev_day_turnover` at EOD in `run_scheduled_scan()`

In `run_scheduled_scan()`, add a flag to track whether EOD save has been done for today. Inside the loop, after the session check, add:

```python
from gap_scorer import save_prev_day_turnover

eod_saved_date = None  # declare before the while loop

# Inside the while loop, after in_session check:
today_date = now_hkt.date()
if hm >= 1600 and eod_saved_date != today_date:
    try:
        snapshot = self._fetch_full_snapshot(market)
        if not snapshot.empty:
            save_prev_day_turnover(market, snapshot)
            eod_saved_date = today_date
    except Exception as e:
        print(f"[EOD] Failed to save daily turnover: {e}")
```

---

## File: bot.py

### Change 9 — Update `/algo` command text

In `cmd_algo()`, update `part2` to reflect:
- Score is now out of 12 (not 9)
- Add Signal 6 description (Unusual Volume, 0–3 pts: `projected today's turnover / yesterday's turnover`. 1.5x → 1pt, 3x → 2pts, 5x → 3pts)
- Update Hard Veto description: "turnover delta < 3% of projected daily turnover, OR less than HK$50k absolute flow"
- Update Alert Tiers: Score 8+ → 🔥 Alert, Score 4–7 → 📊 Watch, below 4 → Skipped
- Add a line about intraday breakouts: "⚡ Intraday Breakout alerts fire when a stock opens flat but spikes 5%+ mid-session"
- Update the Report Fields line: "Composite Score /12, Vol vs Yesterday (x normal)"

Do not change any other part of `bot.py`.

---

## Summary of what NOT to change

- Do not touch `COOLDOWN_MINUTES`, `is_on_cooldown()`, `mark_alerted()`, or `send_telegram()` in `gap_scorer.py`
- Do not change any method signatures in `TopGainersAnalyzer` — `bot.py` calls them by name
- Do not change `scan_and_alert()`, `get_eod_recap()`, `get_watchlist_summary()`, `load_tracked_stocks()` logic
- Do not change `bot.py` command handlers other than `cmd_algo()`
- Do not rename any existing JSON file names (`scan_history.json`, `alert_log.json`, `tracked_movers.json`)
