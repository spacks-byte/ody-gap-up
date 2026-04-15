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
from gspread.utils import rowcol_to_a1
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


def _set_column_widths(ws: gspread.Worksheet, widths: list[int]):
    """Set pixel widths for columns. widths[0] = column A, etc."""
    requests = []
    for i, px in enumerate(widths):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": i,
                    "endIndex": i + 1,
                },
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })
    ws.spreadsheet.batch_update({"requests": requests})


def _bold_header(ws: gspread.Worksheet, num_cols: int):
    """Bold the first row (header)."""
    ws.format(f"A1:{rowcol_to_a1(1, num_cols)}", {
        "textFormat": {"bold": True},
    })


def _set_number_format(ws: gspread.Worksheet, col_indices: list[int],
                        num_data_rows: int, pattern: str = "#,##0"):
    """Apply a number format (with comma separators) to specific columns."""
    if num_data_rows <= 0:
        return
    requests = []
    for col_idx in col_indices:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "endRowIndex": 1 + num_data_rows,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": pattern},
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        })
    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


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

    # Format: bold header + readable column widths
    #         Code  Name  InitP  CurP   Since  Daily  Vol    TO     Days  First  Pin
    _set_column_widths(ws, [110, 180, 90, 90, 100, 100, 100, 120, 80, 140, 60])
    _bold_header(ws, len(headers))

    # Number formats: prices (col 2,3), percentages (4,5), volume (6), turnover (7)
    _set_number_format(ws, [2, 3], len(rows), "#,##0.000")  # prices
    _set_number_format(ws, [4, 5], len(rows), "#,##0.00")   # percentages
    _set_number_format(ws, [6, 7], len(rows), "#,##0")      # volume, turnover

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
    ws = _get_or_create_tab(sheet, "Announcements")

    headers = [
        "Code", "Name", "Category", "Key Info",
        "Added", "Expires", "Announcement",
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
    #         Code  Name  Category  KeyInfo  Added  Expires  Announcement
    _set_column_widths(ws, [110, 180, 120, 300, 100, 100, 350])
    _bold_header(ws, len(headers))

    logger.info("Corporate Actions: wrote %d rows to Google Sheets", len(rows))
    return sheet.url


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
    ws = _get_or_create_tab(sheet, "Recent IPOs")

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

    # Number formats: prices (col 3,5), percentages (6,7), volume (9), turnover (10), market cap (11)
    _set_number_format(ws, [3, 5], len(rows), "#,##0.000")   # prices
    _set_number_format(ws, [6, 7], len(rows), "#,##0.00")    # percentages
    _set_number_format(ws, [9, 10, 11], len(rows), "#,##0")  # volume, turnover, market cap

    # Color-code "Return Since IPO %" (col H) and "Daily Change %" (col G)
    # Green for positive, red for negative
    num_data_rows = len(rows)
    if num_data_rows > 0:
        green_bg = {"red": 0.776, "green": 0.937, "blue": 0.808}  # #C6EFCE
        green_fg = {"red": 0, "green": 0.38, "blue": 0}            # #006100
        red_bg = {"red": 1, "green": 0.78, "blue": 0.808}          # #FFC7CE
        red_fg = {"red": 0.61, "green": 0, "blue": 0.024}          # #9C0006

        def _conditional_rule(col_idx, color_bg, color_fg, formula_type):
            """Build a conditional format rule for a column."""
            return {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": 1 + num_data_rows,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": formula_type,
                                "values": [{"userEnteredValue": "0"}],
                            },
                            "format": {
                                "backgroundColor": color_bg,
                                "textFormat": {"foregroundColor": color_fg},
                            },
                        },
                    },
                    "index": 0,
                }
            }

        requests_batch = []
        for col_idx in [6, 7]:  # G=Daily Change %, H=Return Since IPO %
            requests_batch.append(_conditional_rule(col_idx, green_bg, green_fg, "NUMBER_GREATER"))
            requests_batch.append(_conditional_rule(col_idx, red_bg, red_fg, "NUMBER_LESS"))

        # Freeze header row
        requests_batch.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        })

        sheet.batch_update({"requests": requests_batch})

    logger.info("IPO Tracker: wrote %d rows to Google Sheets", len(rows))
    return sheet.url
