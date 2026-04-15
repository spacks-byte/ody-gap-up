# Proposal: Bot UX Restructure

**Date:** 2026-04-01  
**Status:** Draft — pending review  
**Scope:** Telegram command structure, output format, watchlist rules. No algo logic changes.

---

## Current State — What's Wrong

The bot has **16 commands** with overlapping responsibilities and unclear naming:

| Problem | Example |
|---------|---------|
| `/scan` and `/gapup` do almost the same thing with different triggers | Users don't know which to run |
| `/tracked` and `/attention` are separate watchlists with no clear distinction | "Where are my stocks?" |
| `/joslist`, `/joadd`, `/joremove` are Jo-specific and clutter the menu | Internal tooling mixed with main features |
| `/debug` is developer-facing but sits alongside user commands | Confusing for non-dev users |
| No main menu or navigation — just a wall of `/help` text | New users are lost |
| Scan output is an Excel file — can't preview without downloading | Friction on mobile |

---

## Proposed Command Structure

### Tier 1 — Daily Use (4 commands)

| Command | Purpose | What it replaces |
|---------|---------|------------------|
| `/scan` | **One unified scan** — runs both gap-up + intraday, sends summary message + Excel | `/scan` + `/gapup` merged |
| `/watchlist` | **One unified watchlist view** — shows tracked stocks + attention stocks in one place | `/tracked` + `/attention` merged |
| `/news` | HKEX announcements (unchanged) | `/news` |
| `/monitor` | Toggle auto-scanning (unchanged) | `/monitor` |

### Tier 2 — Deep Dive (4 commands)

| Command | Purpose | Notes |
|---------|---------|-------|
| `/debug <code>` | Score breakdown for a stock | Change from conversation → single-command with arg |
| `/ccass` | CCASS shareholding report | Unchanged (multi-step flow is fine here) |
| `/chat` | AI stock chat | Unchanged |
| `/algo` | Scoring algorithm explainer | Unchanged |

### Tier 3 — Admin / Internal (3 commands)

| Command | Purpose | Notes |
|---------|---------|-------|
| `/joslist` | Run Jo's tracker | Unchanged |
| `/joadd <code>` | Add to Jo's list | Unchanged |
| `/joremove <code>` | Remove from Jo's list | Unchanged |

### Navigation — `/help` or `/start`

Replace the current wall of text with a **grouped, clean menu**:

```
📊 SCAN & MONITOR
  /scan        — Run market scan now
  /monitor     — Toggle auto-scan (ON/OFF)
  /watchlist   — View all tracked stocks

📢 NEWS & RESEARCH
  /news        — HKEX announcements
  /debug CODE  — Score breakdown
  /ccass       — Shareholding report
  /chat        — AI stock analyst

⚙️ ADMIN
  /joslist     — Run Jo's tracker
  /joadd CODE  — Add to Jo's list
  /joremove CODE — Remove from Jo's list

ℹ️ INFO
  /status      — Bot status
  /algo        — How scoring works
```

---

## Proposed `/scan` Output

Currently: sends Excel only. Proposed: **send a Telegram summary first**, then Excel as attachment.

### Summary Message Format

```
📊 Market Scan — 01 Apr 11:45 HKT

🔥 ALERTS (score ≥ 8)
┌─────────────────────────────────────
│ HK.01705  CMMB VISION       10/12
│ +15.2% gap │ $0.285 │ TO: $2.1M
│ Large-cap │ Continuation: +3.1%
├─────────────────────────────────────
│ HK.02899  ZIJIN MINING       9/12
│ +8.7% intraday │ $18.50 │ TO: $45M
│ Mid-cap │ 5-min: +2.3%
└─────────────────────────────────────

👀 WATCH (score 4-7)
  HK.00388 HKEX          7/12  +4.2%
  HK.01810 XIAOMI        5/12  +3.8%
  HK.09988 ALIBABA       4/12  +2.1%

📡 TRACKED (still gaining)
  HK.06862 HAIDILAO     +12.3% (since 3d ago)
  HK.02618 JD.COM        +5.1% (since 1d ago)

📄 Full details in Excel below ↓
```

**Key changes:**
- Alert stocks get full detail (score, price, turnover, trigger type)
- Watch stocks get one-line summary (enough to decide if worth checking)
- Tracked stocks show performance since flagged
- Excel still attached for spreadsheet users

---

## Proposed `/watchlist` Output

Merge `/tracked` and `/attention` into one unified view:

### Summary Message Format

```
📋 Watchlist — 01 Apr 2026

📌 TRACKED MOVERS (auto-tracked from scans)
┌─ 🟢 UP ──────────────────────────
│ HK.01705  CMMB VISION
│   Entry: $0.248 → Now: $0.285 (+14.9%)
│   Flagged: 30 Mar │ Expires: 02 Apr
│
│ HK.06862  HAIDILAO
│   Entry: $15.20 → Now: $17.07 (+12.3%)
│   Flagged: 29 Mar │ Expires: 01 Apr
├─ 🔴 DOWN ─────────────────────────
│ HK.02899  ZIJIN MINING
│   Entry: $19.50 → Now: $18.20 (-6.7%)
│   Flagged: 31 Mar │ Expires: 03 Apr
└───────────────────────────────────

⚠️ ATTENTION LIST (from HKEX announcements)
  🔴 HK.01029 IRC LTD — Trading Halt
     Added: 28 Mar │ Expires: 26 Jun
  🟢 HK.06693 CHIFENG GOLD — Resumption
     Added: 23 Mar │ Expires: 21 Jun
  🏛️ HK.00703 FUTONG TECH — Privatisation
     Added: 15 Mar │ Expires: 13 Jun

Total: 5 tracked │ 3 attention │ 8 stocks
```

---

## Watchlist Rules — Proposed vs Current

### Auto-Tracked Stocks (from scans)

| Rule | Current | Proposed | Rationale |
|------|---------|----------|-----------|
| **Entry trigger** | Score ≥ 4 (alert + watch) | Score ≥ 4 (no change) | Working well |
| **Retention** | 3 days, then auto-removed | **5 trading days** (1 week) | 3 days too short — big movers often take a week to play out |
| **Re-alert** | +5% from initial, then resets baseline | No change | Working well |
| **Re-alert cooldown** | 60 minutes | No change | Working well |
| **Pin/unpin** | `permanent` flag exists but no command to toggle | Add `/pin <code>` and `/unpin <code>` | Let users keep stocks they care about |
| **Max tracked** | Unlimited | **Cap at 30** — oldest auto-tracked removed first | Prevents noise buildup on active days |

### Attention List (from HKEX announcements)

| Rule | Current | Proposed | Rationale |
|------|---------|----------|-----------|
| **Entry trigger** | Auto-added from `/news` scan results | No change | Working well |
| **Retention** | 90 days (3 months) | No change | Corporate actions take months to resolve |
| **Movement alert** | ≥3% intraday move | No change | Working well |
| **Alert cooldown** | 10 minutes | **60 minutes** (match tracked cooldown) | 10 min too spammy for attention stocks |
| **Cleanup** | Auto-pruned on access | No change | Working well |

### Static Watchlist (watchlist_data.py)

| Rule | Current | Proposed | Rationale |
|------|---------|----------|-----------|
| **Content** | 112 hardcoded stocks from ABCI/Futu | Move to JSON config file | Easier to update without code changes |
| **Visibility** | Only in `/tracked` Excel Sheet 2 | Include in `/watchlist` output (top movers only) | Users should see what's on their radar |
| **Update** | Manual code edit | `/watchadd` / `/watchremove` commands | Self-service |

---

## Backend Logic Reference

For the reviewer — here's how the scoring engine works (no changes proposed):

### Scoring Algorithm (12-point scale)

```
Signal 1: Gap/Move Size        0–3 pts   (cap-tier adjusted thresholds)
Signal 2: Continuation/Change  -1–2 pts  (fade penalty if dropping)
Signal 3: 5-min Momentum       -1–1 pt   (recent direction)
Signal 4: Price Strength        -1–1 pt   (position in day's range)
Signal 5: Turnover Delta        0–3 pts   (new money flow since last scan)
Signal 6: Unusual Volume        0–3 pts   (today's pace vs yesterday)
─────────────────────────────────────────
Max: 12 pts + 2 bonus for breakouts = 14 theoretical max
```

### Cap-Tier Thresholds

| Metric | Large (≥5B HKD) | Mid (500M–5B) | Small (<500M) |
|--------|-----------------|---------------|----------------|
| Gap/Move (1/2/3 pts) | 3% / 7% / 15% | 5% / 12% / 22% | 10% / 20% / 35% |
| Intraday Move | 3% / 7% / 15% | 8% / 18% / 30% | 15% / 30% / 50% |
| Turnover Delta | 3% / 8% / 20% | 5% / 15% / 30% | 10% / 25% / 50% |
| Unusual Volume | 1.3x / 2x / 3.5x | 1.5x / 3x / 5x | 2.5x / 5x / 8x |

### Veto Rules (caps score at 3)

1. **Ghost Trade**: New flow too small relative to daily + cumulative turnover below 50% of yesterday
2. **Illiquidity**: Total turnover < HKD 50,000

### Alert Tiers

| Score | Tier | Action |
|-------|------|--------|
| ≥ 8 | ALERT | Telegram message + Excel + auto-track |
| 4–7 | WATCH | Excel only + auto-track |
| < 4 | SKIP | Ignored |

### Data Flow Per Scan Cycle (every 5 min during market hours)

```
1. Fetch snapshot (all stocks, batched 400)
2. Calculate 6 signals per candidate
3. Score → Alert / Watch / Skip
4. Check 60-min cooldown
5. Send Telegram for new alerts
6. Save snapshot for next scan's turnover delta
7. Check attention stocks for ≥3% moves
8. At 16:00 HKT: save EOD turnover for tomorrow's baseline
```

### Data Files

| File | Purpose | Retention |
|------|---------|-----------|
| `data/tracked_movers.json` | Auto-tracked stocks | 3 days (proposed: 5 trading days) |
| `data/scan_history.json` | Turnover snapshots between scans | Overwritten each scan |
| `data/prev_day_turnover.json` | Yesterday's turnover baseline | Overwritten daily at 16:00 |
| `data/alert_log.json` | Alert cooldown timestamps | 60-min cooldown per stock |
| `data/attention_stocks.json` | HKEX announcement watchlist | 90 days |

---

## Implementation Priority

If approved, suggested order:

| # | Change | Effort | Impact |
|---|--------|--------|--------|
| 1 | Merge `/scan` + `/gapup` into unified `/scan` | Small | High — simplifies daily use |
| 2 | Add Telegram summary message to `/scan` output | Small | High — mobile-friendly preview |
| 3 | Merge `/tracked` + `/attention` into `/watchlist` | Medium | High — one place for everything |
| 4 | Clean up `/help` text with grouped layout | Small | Medium — better onboarding |
| 5 | Add `/pin` and `/unpin` commands | Small | Medium — user control over watchlist |
| 6 | Extend auto-track retention to 5 trading days | Tiny | Low — config change |
| 7 | Cap tracked stocks at 30 | Tiny | Low — prevents noise |
| 8 | Move static watchlist to JSON config | Medium | Low — developer QoL |

---

## What This Does NOT Change

- Scoring algorithm (all 6 signals, thresholds, vetoes)
- HKEX announcement scanning logic
- CCASS tracking
- AI chat functionality
- Jo's tracker script
- FutuOpenD integration
- Scheduled job timing
- Excel report generation (still available as attachment)
