# Dev Bot Setup & Bug Fixes — Change Log

**Date:** 2026-04-01  
**Author:** Claude (assisted setup & debugging)  
**Reviewer:** Pending

---

## 1. Environment Setup (no code changes)

- Created `.env` from `.env.example` with dev bot credentials
- `.env` is gitignored — no risk to production

## 2. Dependency Fix: `cryptography` package

**Problem:** `/joslist` crashed on import — `cryptography` v39.0.1 was compiled against OpenSSL 1.1 (`libssl.1.1.dylib`) which no longer exists on this Mac.

**Fix:** `pip3 install --force-reinstall cryptography` → upgraded to v46.0.6 which bundles its own OpenSSL.

**Files changed:** None (pip package only)

## 3. Bug Fix: `/news` command — "message too long"

**Problem:** `format_alerts()` output can exceed Telegram's 4096 character limit. The message was sent as a single `reply_text()` call, causing Telegram API to reject it.

**Fix:** Added message chunking (split at 4000 chars) — same pattern already used elsewhere in the codebase (e.g. `deal_analysis_callback` at line ~1589).

**File changed:** `bot/telegram_bot.py`

**Locations (3 places):**

| Location | Function | What changed |
|----------|----------|--------------|
| ~Line 1471 | `cmd_news()` — with date arg | Single `reply_text()` → chunked loop |
| ~Line 1499 | `cmd_news()` — no date arg | Single `reply_text()` → chunked loop |
| ~Line 2156 | `scheduled_news_check()` | Single `send_message()` → chunked loop |

**Pattern applied (identical in all 3 places):**
```python
# Before:
await update.message.reply_text(text, parse_mode="Markdown", ...)

# After:
for i in range(0, len(text), 4000):
    chunk = text[i:i + 4000]
    await update.message.reply_text(chunk, parse_mode="Markdown", ...)
```

## 4. No Issue: `/monitor` command

**Observation:** `/monitor` appeared to not scan, but this is expected behaviour — `scheduled_scan()` only runs during HK market hours (9:30–12:00, 13:00–16:00 HKT). Testing was done after market close.

**Files changed:** None

## 5. Dependency Fix: `pyOpenSSL` package

**Problem:** Google Sheets auth (`oauth2client`) crashed with `AttributeError: module 'lib' has no attribute 'X509_V_FLAG_NOTIFY_POLICY'` — old pyOpenSSL 23.0.0 incompatible with new cryptography 46.0.6.

**Fix:** `pip3 install --force-reinstall pyOpenSSL` → upgraded to v26.0.0.

**Files changed:** None (pip package only)

## 6. Google Sheets Integration — Watchlist Restructure

**Problem:** Three confusingly-named lists (Watchlist, Tracked, Attention) outputting as Excel files. Watchlist was just a duplicate of Jo's data.

**Changes:**

| What | Before | After |
|------|--------|-------|
| `/tracked` command | Excel file with 2 sheets (Tracked + Watchlist) | `/scannerhits` → writes to Google Sheets "Scanner Hits" tab |
| `/attention` command | Telegram text message | `/corpactions` → writes to Google Sheets "Corporate Actions" tab |
| Hardcoded watchlist | `WATCHLIST_STOCKS` imported from `watchlist_data.py` | Removed — was duplicate of Jo's list |
| `/help` text | Flat list of 16 commands | Grouped into Scan, Lists, News, Jo's Tracker, Info |
| `/status` label | "Tracked stocks" | "Scanner hits" |

**Files changed:**

| File | Change | Risk |
|------|--------|------|
| `bot/sheets_writer.py` | **New file** — Google Sheets auth + write helpers | Low |
| `bot/telegram_bot.py` | Replaced `cmd_tracked` → `cmd_scanner_hits`, `cmd_attention` → `cmd_corp_actions`, removed `WATCHLIST_STOCKS` import, updated `/help` text, updated command registrations | Medium — commands renamed |
| `watchlist_data.py` | **No change** — file left in place, just no longer imported | None |

**No changes to:** scoring algorithm, scanner logic, HKEX tracker, attention_list.py data layer, scheduled jobs, FutuOpenD integration.

## 7. Code Review Fix — Minimal `/corpactions` approach

**Problem:** Code reviewer flagged that `cmd_attention` was fully rewritten (~35 lines replaced) when only ~10 lines needed adding. The rewrite removed the original Telegram message and replaced it with only a Google Sheets link — losing the inline preview.

**Fix:** Reverted `cmd_corp_actions` to preserve the original `cmd_attention` Telegram output (emoji list with stock details). Added Google Sheets sync as ~10 lines on top, with a Sheets link appended at the bottom. Falls back gracefully if Sheets write fails.

**`cmd_scanner_hits`** (was `cmd_tracked`) was kept as-is — the ~200 lines of Excel/openpyxl formatting HAD to be removed since we're replacing the output mechanism. Data-fetching logic is preserved from original.

**File changed:** `bot/telegram_bot.py` — `cmd_corp_actions` function only

**Risk:** Low — restores original behaviour, adds sheets sync as an addon

## 8. `/corpactions` — Add announcement links + fix column widths

**Problem:** Google Sheets columns too narrow to display content. Announcement links from HKEX were available in the data but not shown.

**Changes:**

| File | What changed |
|------|-------------|
| `bot/telegram_bot.py` | Added `[Announcement](link)` line to each stock in Telegram output; added `"Announcement"` field to sheet rows |
| `bot/sheets_writer.py` | Added `"Announcement"` column to Corporate Actions headers; added `_set_column_widths()` and `_bold_header()` helpers; set readable pixel widths for both Scanner Hits and Corporate Actions tabs |

**Risk:** Low — additive changes only

## 9. Delete `/gapup`, merge into `/scan` + `/news` date picker

**Problem:** `/scan` and `/gapup` were near-identical commands running different scanners. `/news` and `/news DD/MM/YYYY` were the same command but users didn't know the date arg existed.

**Changes:**

| What | Before | After |
|------|--------|-------|
| `/scan` | Ran intraday scanner only | Runs both intraday + gap-up (same as `scheduled_scan`) |
| `/gapup` | Separate command | **Deleted** — merged into `/scan` |
| `/news` (no args) | Immediately scanned last 24h | Shows 2 buttons: "Today (last 24h)" or "Custom date" |
| `/news DD/MM/YYYY` | Direct date scan | Still works as before (unchanged) |
| `_offer_deal_analysis` | Old helper taking `Update` | Replaced with `_offer_deal_analysis_from_msg` taking `message` (works for both commands and callbacks) |

**Files changed:**

| File | What changed |
|------|-------------|
| `bot/telegram_bot.py` | Merged gap-up into `cmd_scan` (~10 lines added for dedup); deleted `cmd_gapup` (~60 lines); rewrote `cmd_news` to show inline buttons; added `news_callback`, `news_date_handler`, `_news_scan_date`, `_offer_deal_analysis_from_msg`; removed old `_offer_deal_analysis`; removed gapup handler registration; added news callback + date handler registrations; updated `/help` text |

**Risk:** Medium — `/scan` now runs 2 scanners (takes slightly longer), `/news` UX changed

## 10. IPO Tracker — new `/ipo` command

**Feature:** Track recent HK IPOs and their performance for 90 days after listing.

**Data sources (Futu API):**
- `get_plate_stock('HK.LIST1290')` — "Recent IPOs" plate (stocks + listing dates)
- `get_market_snapshot()` — current prices, volume, turnover, market cap
- `get_owner_plate()` — industry classification per stock
- `get_ipo_list()` — IPO offer price (only for very recent listings)

**Files:**

| File | Change | Risk |
|------|--------|------|
| `ipo/__init__.py` | **New** — package init | None |
| `ipo/ipo_tracker.py` | **New** — Futu API fetch logic | Low |
| `bot/sheets_writer.py` | Added `write_ipo_tracker()` | Low |
| `bot/telegram_bot.py` | Added `/ipo` command, handler, help text | Low |

**Limitation:** IPO offer price only available from `get_ipo_list()` for very recent/active IPOs. Older IPOs show blank for IPO Price and Return Since IPO.

---

## Summary

| Change | Files modified | Risk |
|--------|---------------|------|
| `.env` created | `.env` (gitignored) | None |
| cryptography reinstall | pip package | Low |
| pyOpenSSL reinstall | pip package | Low |
| News message chunking | `bot/telegram_bot.py` | Low — uses existing codebase pattern |
| /monitor | No change | N/A |
| Google Sheets watchlist | `bot/sheets_writer.py` (new), `bot/telegram_bot.py` | Medium — commands renamed |
| IPO Tracker | `ipo/` (new folder), `bot/sheets_writer.py`, `bot/telegram_bot.py` | Low |
