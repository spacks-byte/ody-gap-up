# IPO Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `/ipo` command that fetches recent HK IPOs from Futu API, tracks their performance for 3 months, and writes results to a Google Sheets "IPO Tracker" tab.

**Architecture:** New `ipo/` folder with `ipo_tracker.py` containing the Futu API fetch logic. The bot calls this module from a new `/ipo` command, then writes to Google Sheets via the existing `sheets_writer.py`. Data sources: `get_plate_stock('HK.LIST1290')` for recent IPOs, `get_market_snapshot()` for current prices, `get_owner_plate()` for industry classification, `get_ipo_list()` for IPO price.

**Tech Stack:** futu-api (already installed), gspread (existing `sheets_writer.py`)

**Constraints:** Minimal changes to `telegram_bot.py` — only add command handler + registration. All IPO logic in separate folder.

---

### File Map

| Action | File | Responsibility |
|--------|------|----------------|
| **Create** | `ipo/__init__.py` | Package init |
| **Create** | `ipo/ipo_tracker.py` | Fetch IPO data from Futu API, build rows for sheets |
| **Modify** | `bot/sheets_writer.py` | Add `write_ipo_tracker()` function |
| **Modify** | `bot/telegram_bot.py` | Add `/ipo` command handler + registration + help text |

---

### Task 1: Create `ipo/ipo_tracker.py` — IPO data fetcher

**Files:**
- Create: `ipo/__init__.py`
- Create: `ipo/ipo_tracker.py`

- [ ] **Step 1: Create the package init**

```python
# ipo/__init__.py
from ipo.ipo_tracker import fetch_recent_ipos
```

- [ ] **Step 2: Create ipo_tracker.py**

```python
#!/usr/bin/env python3
"""
ipo/ipo_tracker.py — Fetch recent HK IPOs and their performance from Futu API.

Uses:
    - get_plate_stock('HK.LIST1290') for the "Recent IPOs" list
    - get_market_snapshot() for current prices
    - get_owner_plate() for industry classification
    - get_ipo_list() for IPO offer price (if still available)
"""

import logging
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

    # 4. Try to get IPO prices from get_ipo_list (only has active/very recent IPOs)
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

    # 5. Build result rows
    rows = []
    for code in codes:
        snap = snap_map.get(code)
        if snap is None:
            continue

        list_date_str = list_date_map.get(code, "")
        cur_price = float(snap.get("last_price", 0) or 0)
        open_price = float(snap.get("open_price", 0) or 0)
        prev_close = float(snap.get("prev_close_price", 0) or 0)
        high = float(snap.get("high_price", 0) or 0)
        low = float(snap.get("low_price", 0) or 0)
        volume = int(snap.get("volume", 0) or 0)
        turnover = float(snap.get("turnover", 0) or 0)
        market_cap = float(snap.get("total_market_val", 0) or 0)

        # IPO price: prefer ipo_list, fallback to first-day open (not ideal)
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
            "Industry": industry_map.get(code, "—"),
            "IPO Price": round(ipo_price, 3) if ipo_price else "—",
            "List Date": list_date_str,
            "Current Price": round(cur_price, 3),
            "Daily Change %": round(daily_chg, 1),
            "Return Since IPO %": round(ipo_return, 1) if ipo_return is not None else "—",
            "Days Listed": days_since if days_since is not None else "—",
            "Volume": volume,
            "Turnover (HK$)": round(turnover),
            "Market Cap": round(market_cap),
        })

    # Sort by listing date descending (newest first)
    rows.sort(key=lambda r: r.get("List Date", ""), reverse=True)

    logger.info("IPO Tracker: fetched %d stocks from Recent IPOs plate", len(rows))
    return rows
```

- [ ] **Step 3: Verify import works**

Run:
```bash
cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "from ipo.ipo_tracker import fetch_recent_ipos; print('OK')"
```

Expected: `OK`

---

### Task 2: Add `write_ipo_tracker()` to sheets_writer

**Files:**
- Modify: `bot/sheets_writer.py` — add new function at end of file

- [ ] **Step 1: Add the writer function**

Append to `bot/sheets_writer.py`:

```python
def write_ipo_tracker(rows: list[dict], description: str = "") -> str:
    """
    Write recent IPO data to the 'IPO Tracker' tab.

    Args:
        rows: list of dicts with keys matching the header columns.
        description: footer text explaining the sheet contents.

    Returns:
        URL of the spreadsheet.
    """
    sheet = _get_sheet()
    ws = _get_or_create_tab(sheet, "IPO Tracker")

    headers = [
        "Code", "Name", "Industry", "IPO Price", "List Date",
        "Current Price", "Daily Change %", "Return Since IPO %",
        "Days Listed", "Volume", "Turnover (HK$)", "Market Cap",
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

    # Format: bold header + readable column widths
    #         Code  Name  Industry IPO    ListDt  CurP   Daily  Return Days  Vol    TO     MCap
    _set_column_widths(ws, [110, 200, 180, 80, 100, 100, 100, 120, 80, 100, 120, 120])
    _bold_header(ws, len(headers))

    logger.info("IPO Tracker: wrote %d rows to Google Sheets", len(rows))
    return sheet.url
```

- [ ] **Step 2: Verify import**

Run:
```bash
cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "from bot.sheets_writer import write_ipo_tracker; print('OK')"
```

---

### Task 3: Add `/ipo` command to telegram_bot.py

**Files:**
- Modify: `bot/telegram_bot.py` — add import, command handler, registration, help text

- [ ] **Step 1: Add import**

After the existing `from bot.sheets_writer import ...` line, update it to include `write_ipo_tracker`:

```python
from bot.sheets_writer import write_scanner_hits, write_corporate_actions, write_ipo_tracker
```

And add:

```python
from ipo import fetch_recent_ipos
```

- [ ] **Step 2: Add cmd_ipo function**

Add this function before the `cmd_monitor` function:

```python
async def cmd_ipo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Fetch recent HK IPOs and write performance to Google Sheets."""
    await update.message.reply_text("Fetching recent IPOs from Futu...")

    try:
        a = get_analyzer()
        rows = fetch_recent_ipos(a.quote_ctx, market=MARKET)

        if not rows:
            await update.message.reply_text("No recent IPOs found in the last 90 days.")
            return

        description = (
            "--- IPO TRACKER ---",
            "Recent HK IPOs tracked for 90 days after listing.",
            "Data: Futu 'Recent IPOs' plate + market snapshot + industry plates.",
            "IPO Price only available for very recent listings (from Futu IPO list).",
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} HKT",
        )
        url = write_ipo_tracker(rows, "\n".join(description))

        # Count stats
        with_return = [r for r in rows if isinstance(r.get("Return Since IPO %"), (int, float))]
        n_up = sum(1 for r in with_return if r["Return Since IPO %"] > 0)
        n_down = sum(1 for r in with_return if r["Return Since IPO %"] < 0)

        await update.message.reply_text(
            f"*IPO Tracker* — {len(rows)} recent IPOs\n"
            f"📈 {n_up} up  📉 {n_down} down since listing\n\n"
            f"[Open in Google Sheets]({url})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    except Exception as e:
        logger.exception("IPO tracker failed")
        await update.message.reply_text(f"Error: {e}")
```

- [ ] **Step 3: Add command registration**

In the `main()` function, after the `corpactions` handler, add:

```python
    app.add_handler(CommandHandler("ipo", cmd_ipo))
```

- [ ] **Step 4: Update /help text**

In the `cmd_help` function, in the "Lists (Google Sheets)" section, add a line:

```python
        "/ipo — Recent IPOs performance (90-day)\n"
```

- [ ] **Step 5: Verify bot imports**

Run:
```bash
cd /Users/papasmac/Desktop/Work_Odysseus/ody-gap-up-main && python3 -c "import bot.telegram_bot; print('OK')"
```

---

### Task 4: Update CHANGELOG_DEV.md

**Files:**
- Modify: `CHANGELOG_DEV.md`

- [ ] **Step 1: Append changelog entry**

```markdown

## 10. IPO Tracker — new `/ipo` command

**Feature:** Track recent HK IPOs and their performance for 90 days after listing.

**Data sources (Futu API):**
- `get_plate_stock('HK.LIST1290')` — "Recent IPOs" plate (list of stocks + listing dates)
- `get_market_snapshot()` — current prices, volume, turnover, market cap
- `get_owner_plate()` — industry classification per stock
- `get_ipo_list()` — IPO offer price (only available for very recent listings)

**Files:**

| File | Change | Risk |
|------|--------|------|
| `ipo/__init__.py` | **New** — package init | None |
| `ipo/ipo_tracker.py` | **New** — Futu API fetch logic | Low |
| `bot/sheets_writer.py` | Added `write_ipo_tracker()` | Low |
| `bot/telegram_bot.py` | Added `/ipo` command, handler, help text | Low |

**Limitation:** IPO offer price is only available from `get_ipo_list()` for very recent/active IPOs. Older IPOs will show "—" for IPO Price and Return Since IPO.
```

---

### Task 5: Manual Verification

- [ ] **Step 1: Restart bot and test /ipo**

```bash
kill <pid>; python3 main.py &
```

Send `/ipo` to the bot. Verify:
- Bot replies "Fetching recent IPOs from Futu..."
- Then replies with count + Google Sheets link
- "IPO Tracker" tab exists in Google Sheets with 12 columns + description footer
- Column widths are readable

- [ ] **Step 2: Verify /help shows new command**

Send `/help`. Verify `/ipo` appears under "Lists (Google Sheets)".
