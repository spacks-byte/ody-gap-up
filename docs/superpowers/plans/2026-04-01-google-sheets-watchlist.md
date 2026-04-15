# Google Sheets Watchlist Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Excel file outputs for `/tracked` and `/attention` with Google Sheets tabs on the existing Jo's spreadsheet, rename lists to clearer names, remove the redundant hardcoded watchlist, and clean up the `/help` menu.

**Architecture:** Create a new `sheets_writer.py` module that handles all Google Sheets writes (authenticate, find-or-create tab, clear & write rows, add description footer). The bot commands (`/scannerhits`, `/corpactions`) call this module instead of building Excel. The old `/tracked` and `/attention` commands are replaced. The static `WATCHLIST_STOCKS` import and Sheet 2 watchlist code in `cmd_tracked` are removed.

**Tech Stack:** gspread, oauth2client, existing service account auth (`hsi-turnover-auth.json`)

**Constraints:**
- Minimal changes to existing files — no algo/scanner logic touched
- All changes logged in `CHANGELOG_DEV.md` for reviewer
- No new dependencies (gspread + oauth2client already installed)

---

### File Map

| Action | File | Responsibility |
|--------|------|----------------|
| **Create** | `bot/sheets_writer.py` | Google Sheets auth + write helper functions |
| **Modify** | `bot/telegram_bot.py` | Replace `/tracked` → `/scannerhits`, `/attention` → `/corpactions`, remove watchlist code, update `/help` |
| **No change** | `hkex/attention_list.py` | Data source for corporate actions (read-only) |
| **No change** | `scanner/market_scanner.py` | Data source for tracked stocks (read-only) |
| **No change** | `watchlist_data.py` | Will stop being imported but file left in place |

---

### Task 1: Create `bot/sheets_writer.py` — Google Sheets helper

**Files:**
- Create: `bot/sheets_writer.py`

- [ ] **Step 1: Create the sheets_writer module**

```python
#!/usr/bin/env python3
"""
bot/sheets_writer.py — Write scanner and corporate action data to Google Sheets.

Uses the same service account credentials as joslist/stock_tracker.py.
Each function clears the target tab and rewrites all rows + a description footer.
"""

import os
import logging
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── Auth ──────────────────────────────────────────────────────────────────────

_gc = None  # cached gspread client


def _get_client() -> gspread.Client:
    """Authenticate with Google Sheets using the service account."""
    global _gc
    if _gc is not None:
        return _gc

    creds_file = os.environ.get("GOOGLE_CREDENTIALS_FILE", "hsi-turnover-auth.json").strip()
    creds_path = Path(creds_file)
    if not creds_path.is_absolute():
        root_candidate = PROJECT_ROOT / creds_path
        local_candidate = PROJECT_ROOT / "joslist" / creds_path
        creds_path = root_candidate if root_candidate.exists() else local_candidate

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    _gc = gspread.authorize(creds)
    return _gc


def _get_sheet() -> gspread.Spreadsheet:
    """Open the shared Google Spreadsheet."""
    url = os.environ.get("GOOGLE_SHEET_URL", "").strip()
    if not url:
        raise ValueError("GOOGLE_SHEET_URL not set in .env")
    return _get_client().open_by_url(url)


def _get_or_create_tab(sheet: gspread.Spreadsheet, title: str, cols: int = 15) -> gspread.Worksheet:
    """Find existing tab or create a new one."""
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return sheet.add_worksheet(title=title, rows="500", cols=str(cols))


# ── Writers ───────────────────────────────────────────────────────────────────

def write_scanner_hits(rows: list[dict], description: str = "") -> str:
    """
    Write scanner-flagged stocks to the 'Scanner Hits' tab.

    Args:
        rows: list of dicts with keys matching the header columns.
        description: footer text explaining the sheet contents.

    Returns:
        URL of the spreadsheet.
    """
    sheet = _get_sheet()
    ws = _get_or_create_tab(sheet, "Scanner Hits")

    headers = [
        "Code", "Name", "Initial Price", "Current Price",
        "Since Tracked %", "Daily Change %", "Volume",
        "Turnover (HK$)", "Days Tracked", "First Seen", "Pinned",
    ]

    # Clear and write
    ws.clear()
    all_rows = [headers]
    for r in rows:
        all_rows.append([r.get(h, "") for h in headers])

    # Add blank row + description
    if description:
        all_rows.append([])
        for line in description.split("\n"):
            all_rows.append([line])

    ws.update(range_name="A1", values=all_rows)
    logger.info("Scanner Hits: wrote %d rows to Google Sheets", len(rows))
    return sheet.url


def write_corporate_actions(rows: list[dict], description: str = "") -> str:
    """
    Write HKEX announcement-tracked stocks to the 'Corporate Actions' tab.

    Args:
        rows: list of dicts with keys matching the header columns.
        description: footer text explaining the sheet contents.

    Returns:
        URL of the spreadsheet.
    """
    sheet = _get_sheet()
    ws = _get_or_create_tab(sheet, "Corporate Actions")

    headers = [
        "Code", "Name", "Category", "Key Info",
        "Added", "Expires",
    ]

    ws.clear()
    all_rows = [headers]
    for r in rows:
        all_rows.append([r.get(h, "") for h in headers])

    if description:
        all_rows.append([])
        for line in description.split("\n"):
            all_rows.append([line])

    ws.update(range_name="A1", values=all_rows)
    logger.info("Corporate Actions: wrote %d rows to Google Sheets", len(rows))
    return sheet.url
```

- [ ] **Step 2: Verify module imports correctly**

Run: `cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "from bot.sheets_writer import write_scanner_hits, write_corporate_actions; print('OK')"`

Expected: `OK`

- [ ] **Step 3: Test Google Sheets connection**

Run:
```bash
cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "
from bot.sheets_writer import _get_sheet
sheet = _get_sheet()
print('Connected:', sheet.title)
print('Tabs:', [ws.title for ws in sheet.worksheets()])
"
```

Expected: `Connected: <sheet name>` and tab list including Turnover, Prices.

- [ ] **Step 4: Commit**

```bash
git add bot/sheets_writer.py
git commit -m "feat: add sheets_writer module for Google Sheets integration"
```

---

### Task 2: Replace `/tracked` with `/scannerhits`

**Files:**
- Modify: `bot/telegram_bot.py` — `cmd_tracked` function (~lines 533-795), command registration (~line 2091), `/help` text (~line 1623)

- [ ] **Step 1: Add import for sheets_writer at top of telegram_bot.py**

After the existing `from hkex.attention_list import ...` line (~line 59), add:

```python
from bot.sheets_writer import write_scanner_hits, write_corporate_actions
```

- [ ] **Step 2: Replace cmd_tracked with cmd_scanner_hits**

Replace the entire `cmd_tracked` function (lines 533 through ~795) with:

```python
async def cmd_scanner_hits(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Write scanner-flagged stocks to Google Sheets 'Scanner Hits' tab."""
    import pandas as pd

    try:
        a = get_analyzer()
        tracked = a.load_tracked_stocks()
        market_stocks = {c: d for c, d in tracked.items()
                         if d.get('market') == MARKET}

        if not market_stocks:
            await update.message.reply_text("No scanner hits right now.")
            return

        await update.message.reply_text("Updating Scanner Hits on Google Sheets...")

        # Fetch live prices
        stock_codes = list(market_stocks.keys())
        all_snaps = []
        for i in range(0, len(stock_codes), 400):
            batch = stock_codes[i:i + 400]
            ret, snap = a.quote_ctx.get_market_snapshot(batch)
            if ret == 0 and snap is not None and not snap.empty:
                all_snaps.append(snap)

        snap_map = {}
        if all_snaps:
            snapshot = pd.concat(all_snaps, ignore_index=True)
            for _, row in snapshot.iterrows():
                snap_map[row['code']] = row

        rows = []
        for code, td in market_stocks.items():
            ini = td['initial_price']
            first_seen = datetime.fromisoformat(td['first_seen'])
            days = (datetime.now() - first_seen).days
            pinned = 'Yes' if td.get('permanent') else ''
            r = snap_map.get(code)
            cur = float(r['last_price']) if r is not None else ini
            daily_chg = float(r.get('change_rate', 0) or 0) if r is not None else 0.0
            volume = int(r.get('volume', 0) or 0) if r is not None else 0
            turnover = float(r.get('turnover', 0) or 0) if r is not None else 0.0
            total_chg = ((cur - ini) / ini * 100) if ini > 0 else 0

            rows.append({
                'Code': code,
                'Name': td['name'],
                'Initial Price': round(ini, 3),
                'Current Price': round(cur, 3),
                'Since Tracked %': round(total_chg, 1),
                'Daily Change %': round(daily_chg, 1),
                'Volume': volume,
                'Turnover (HK$)': round(turnover),
                'Days Tracked': days,
                'First Seen': first_seen.strftime('%Y-%m-%d %H:%M'),
                'Pinned': pinned,
            })

        rows.sort(key=lambda r: abs(r['Since Tracked %']), reverse=True)

        description = (
            "--- SCANNER HITS ---",
            "Stocks automatically flagged by the gap-up / intraday scanner (score >= 4).",
            "Auto-tracked for 3 days, then removed. Pinned stocks stay indefinitely.",
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} HKT",
        )
        url = write_scanner_hits(rows, "\n".join(description))

        n_up = sum(1 for r in rows if r['Since Tracked %'] > 0)
        n_down = sum(1 for r in rows if r['Since Tracked %'] < 0)
        await update.message.reply_text(
            f"*Scanner Hits* — {len(rows)} stocks\n"
            f"🟢 {n_up} up  🔴 {n_down} down since flagged\n\n"
            f"[Open in Google Sheets]({url})",
            parse_mode='Markdown',
            disable_web_page_preview=True,
        )

    except Exception as e:
        logger.exception('Scanner hits failed')
        await update.message.reply_text(f'Error: {e}')
```

- [ ] **Step 3: Remove the `WATCHLIST_STOCKS` import**

Remove this line (~line 56):
```python
from watchlist_data import WATCHLIST_STOCKS
```

- [ ] **Step 4: Update command registration**

Change (~line 2091):
```python
# Old:
app.add_handler(CommandHandler("tracked", cmd_tracked))
# New:
app.add_handler(CommandHandler("scannerhits", cmd_scanner_hits))
```

- [ ] **Step 5: Verify bot starts without errors**

Run:
```bash
cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "
import bot.telegram_bot as tb
print('Import OK')
print('WATCHLIST_STOCKS' in dir(tb))  # should print False
"
```

Expected: `Import OK` and `False`

- [ ] **Step 6: Commit**

```bash
git add bot/telegram_bot.py
git commit -m "feat: replace /tracked with /scannerhits, output to Google Sheets"
```

---

### Task 3: Replace `/attention` with `/corpactions`

**Files:**
- Modify: `bot/telegram_bot.py` — `cmd_attention` function (~lines 1949-1981), command registration (~line 2098)

- [ ] **Step 1: Replace cmd_attention with cmd_corp_actions**

Replace the entire `cmd_attention` function with:

```python
async def cmd_corp_actions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Write HKEX corporate action stocks to Google Sheets 'Corporate Actions' tab."""
    attention = get_attention_stocks()
    if not attention:
        await update.message.reply_text("No corporate actions being tracked.")
        return

    await update.message.reply_text("Updating Corporate Actions on Google Sheets...")

    emojis = {
        "Trading Halt": "HALT", "Trading Resumption": "RESUMPTION",
        "Rights Issue": "RIGHTS", "Share Placement": "PLACEMENT",
        "Privatisation": "PRIVATISATION", "Takeover": "TAKEOVER", "M&A": "M&A",
    }

    rows = []
    for code, info in sorted(attention.items()):
        cat = info.get("category", "")
        rows.append({
            "Code": code,
            "Name": info.get("stock_name", ""),
            "Category": emojis.get(cat, cat),
            "Key Info": info.get("key_info", "")[:120],
            "Added": info.get("added", "")[:10],
            "Expires": info.get("expires", "")[:10],
        })

    description = (
        "--- CORPORATE ACTIONS ---",
        "Stocks flagged from HKEX announcements (halts, takeovers, placements, M&A, etc).",
        "Auto-added by /news scans. Retained for 90 days (3 months), then auto-removed.",
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} HKT",
    )
    url = write_corporate_actions(rows, "\n".join(description))

    await update.message.reply_text(
        f"*Corporate Actions* — {len(rows)} stocks tracked\n\n"
        f"[Open in Google Sheets]({url})",
        parse_mode='Markdown',
        disable_web_page_preview=True,
    )
```

- [ ] **Step 2: Update command registration**

Change (~line 2098):
```python
# Old:
app.add_handler(CommandHandler("attention", cmd_attention))
# New:
app.add_handler(CommandHandler("corpactions", cmd_corp_actions))
```

- [ ] **Step 3: Commit**

```bash
git add bot/telegram_bot.py
git commit -m "feat: replace /attention with /corpactions, output to Google Sheets"
```

---

### Task 4: Update `/help` text and `/status`

**Files:**
- Modify: `bot/telegram_bot.py` — `cmd_help` (~lines 1612-1645), `cmd_status` (~lines 1446-1458)

- [ ] **Step 1: Replace the /help text**

Replace the entire help text in `cmd_help` with:

```python
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    monitor_str = "ON" if monitoring_active else "OFF"
    text = (
        "*Commands*\n"
        "\n"
        "*Scan & Monitor*\n"
        "/scan — Intraday movers scan\n"
        "/gapup — Overnight gap-up scan\n"
        "/monitor — Toggle auto-scan (" + monitor_str + ")\n"
        "\n"
        "*Lists (Google Sheets)*\n"
        "/scannerhits — Scanner-flagged stocks (3-day)\n"
        "/corpactions — HKEX corporate actions (90-day)\n"
        "\n"
        "*News & Research*\n"
        "/news — HKEX announcements\n"
        "/news DD/MM/YYYY — Check a specific date\n"
        "/debug — Score breakdown for any stock\n"
        "/ccass — CCASS shareholding report\n"
        "/chat — AI stock analyst\n"
        "\n"
        "*Jo's Tracker*\n"
        "/joslist — Run Jo's tracker script\n"
        "/joadd CODE — Add stock to Jo's list\n"
        "/joremove CODE — Remove from Jo's list\n"
        "\n"
        "*Info*\n"
        "/status — Bot status\n"
        "/algo — How scoring works\n"
        "/help — This message"
    )
    await update.message.reply_text(text, parse_mode="Markdown")
```

- [ ] **Step 2: Update /status to use new names**

In `cmd_status` (~line 1446), change `"Tracked stocks"` label to `"Scanner hits"`:

```python
# Old:
f"Tracked stocks: {len(tracked)}",
# New:
f"Scanner hits: {len(tracked)}",
```

- [ ] **Step 3: Commit**

```bash
git add bot/telegram_bot.py
git commit -m "feat: update /help menu with new command names"
```

---

### Task 5: Update `CHANGELOG_DEV.md`

**Files:**
- Modify: `CHANGELOG_DEV.md`

- [ ] **Step 1: Add entry for this batch of changes**

Append to `CHANGELOG_DEV.md`:

```markdown

## 5. Google Sheets Integration — Watchlist Restructure

**Problem:** Three confusingly-named lists (Watchlist, Tracked, Attention) outputting as Excel files. Watchlist was just a duplicate of Jo's data.

**Changes:**

| What | Before | After |
|------|--------|-------|
| `/tracked` command | Excel file with 2 sheets (Tracked + Watchlist) | `/scannerhits` → writes to Google Sheets "Scanner Hits" tab |
| `/attention` command | Telegram text message | `/corpactions` → writes to Google Sheets "Corporate Actions" tab |
| Hardcoded watchlist | `WATCHLIST_STOCKS` imported from `watchlist_data.py` | Removed — was duplicate of Jo's list |
| `/help` text | Flat list of 16 commands | Grouped into Scan, Lists, News, Jo's Tracker, Info |

**Files changed:**

| File | Change | Risk |
|------|--------|------|
| `bot/sheets_writer.py` | **New file** — Google Sheets auth + write helpers | Low |
| `bot/telegram_bot.py` | Replaced `cmd_tracked` → `cmd_scanner_hits`, `cmd_attention` → `cmd_corp_actions`, removed `WATCHLIST_STOCKS` import, updated `/help` text, updated command registrations | Medium — commands renamed |
| `watchlist_data.py` | **No change** — file left in place, just no longer imported | None |

**No changes to:** scoring algorithm, scanner logic, HKEX tracker, attention_list.py data layer, scheduled jobs, FutuOpenD integration.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG_DEV.md
git commit -m "docs: update changelog with Google Sheets watchlist restructure"
```

---

### Task 6: Manual Verification

- [ ] **Step 1: Restart bot and test /scannerhits**

```bash
kill <bot_pid>; python3 main.py &
```

Send `/scannerhits` to the bot in Telegram. Verify:
- Bot replies with "Updating Scanner Hits on Google Sheets..."
- Then replies with stock count + Google Sheets link
- The "Scanner Hits" tab exists in the spreadsheet with headers + data + description footer

- [ ] **Step 2: Test /corpactions**

Send `/corpactions` to the bot. Verify:
- Bot replies with "Updating Corporate Actions on Google Sheets..."
- Then replies with stock count + Google Sheets link
- The "Corporate Actions" tab exists with headers + data + description footer

- [ ] **Step 3: Test /help**

Send `/help`. Verify the new grouped menu renders correctly in Telegram.

- [ ] **Step 4: Test /scan and /gapup still work**

Send `/scan`. Verify it still produces Excel output as before (these commands are unchanged).

- [ ] **Step 5: Verify /joslist still works**

Send `/joslist`. Verify it runs without errors (Google Sheets auth is not broken).
