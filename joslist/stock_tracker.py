"""
Hong Kong and US Stock Price Tracker
Updates Google Sheets with stock data including opening/closing prices and percentage changes
"""

from datetime import datetime, timedelta
import time
import os
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# Resolve all local paths from this script directory.
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

# Load root .env first, then optional local joslist/.env overrides.
ROOT_ENV_FILE = PROJECT_ROOT / ".env"
LOCAL_ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ROOT_ENV_FILE)
load_dotenv(dotenv_path=LOCAL_ENV_FILE, override=True)

GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "").strip()
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
WORKSHEET_TITLE = os.getenv("STOCK_TRACKER_WORKSHEET", "Prices").strip() or "Prices"

if GOOGLE_CREDENTIALS_FILE:
    credentials_path = Path(GOOGLE_CREDENTIALS_FILE)
    if not credentials_path.is_absolute():
        # Prefer project root for repo-level secrets (e.g. hsi-turnover-auth.json),
        # then fall back to joslist/ for local script-specific files.
        root_candidate = PROJECT_ROOT / credentials_path
        local_candidate = BASE_DIR / credentials_path
        credentials_path = root_candidate if root_candidate.exists() else local_candidate
    GOOGLE_CREDENTIALS_FILE = str(credentials_path)

missing_env_vars = []
if not GOOGLE_SHEET_URL:
    missing_env_vars.append("GOOGLE_SHEET_URL")
if not GOOGLE_CREDENTIALS_FILE:
    missing_env_vars.append("GOOGLE_CREDENTIALS_FILE")

if missing_env_vars:
    print("ERROR: Missing required environment variables in .env")
    print(f"Missing: {', '.join(missing_env_vars)}")
    print("Set them in the project root .env (or joslist/.env for local overrides).")
    exit(1)

# Yahoo Finance Configuration
RATE_LIMIT_DELAY = 0.5  # Seconds between API requests
MAX_RETRIES = 2
FETCH_DAYS = 120


class StockTracker:
    def __init__(self, rate_limit_delay=0.5, max_retries=2):
        """
        Initialize Yahoo Finance + Google Sheets tracker

        Args:
            rate_limit_delay: Delay between API calls in seconds
            max_retries: Maximum number of retries for failed requests
        """
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries

        # Google Sheets connection
        self.gc = self.authenticate_google_sheets()
        self.sheet = self.open_or_create_sheet()

        print("Connected to Google Sheets and Yahoo Finance")
        print(f"Rate limit: {rate_limit_delay}s between requests, Max retries: {max_retries}")

    def get_yahoo_symbol_candidates(self, stock_code):
        """
        Build Yahoo Finance symbol candidates from internal stock code format.

        Examples:
            HK.00020 -> [0020.HK, 00020.HK]
            HK.44288 -> [44288.HK, 4288.HK]
            US.AAPL -> [AAPL]
        """
        if stock_code.startswith("HK."):
            raw_code = stock_code.split(".", 1)[1]
            digits = "".join(ch for ch in raw_code if ch.isdigit())
            if not digits:
                return [f"{raw_code}.HK"]

            candidates = []

            # Keep original numeric code (useful for some products using 5-digit codes).
            candidates.append(digits)

            # For 5-digit Futu-style codes with leading zero, Yahoo often uses the last 4 digits.
            if len(digits) == 5 and digits.startswith("0"):
                candidates.append(digits[1:])

            # For shorter codes, left-pad to standard 4-digit HK ticker format.
            if len(digits) < 4:
                candidates.append(digits.zfill(4))

            # Deduplicate while preserving order.
            unique = []
            for code in candidates:
                code = code.strip()
                if code and code not in unique:
                    unique.append(code)

            return [f"{code}.HK" for code in unique]

        if stock_code.startswith("US."):
            return [stock_code.split(".", 1)[1].upper()]

        return [stock_code]

    def to_yahoo_symbol(self, stock_code):
        """Return the first Yahoo Finance symbol candidate for display/backward compatibility."""
        return self.get_yahoo_symbol_candidates(stock_code)[0]

    def classify_unavailable_reason(self, stock_code, symbols, last_error):
        """Build a clearer unavailable reason for symbols not covered by Yahoo Finance."""
        tried = ", ".join(symbols)
        error_lower = (last_error or "").lower()

        if stock_code.startswith("HK."):
            raw_code = stock_code.split(".", 1)[1]
            digits = "".join(ch for ch in raw_code if ch.isdigit())

            # Many 5-digit HK product codes (without leading zero) are warrants/CBBC/options.
            if len(digits) == 5 and not digits.startswith("0"):
                return (
                    "Likely non-equity HK product code (warrant/CBBC/option) not covered by Yahoo Finance "
                    f"(tried: {tried})."
                )

        if "404" in error_lower or "not found" in error_lower or "no timezone" in error_lower:
            return f"Ticker unavailable on Yahoo Finance (tried: {tried})."

        if "no data" in error_lower or "possibly delisted" in error_lower:
            return f"No price history available on Yahoo Finance (tried: {tried})."

        return (
            f"No data returned from Yahoo Finance (tried: {tried}). "
            f"Last error: {last_error or 'N/A'}"
        )

    def get_stock_name(self, stock_code):
        """
        Fetch stock name from Yahoo Finance

        Args:
            stock_code: Stock code (e.g., 'HK.00700' or 'US.AAPL')

        Returns:
            Stock name or 'N/A' if not found
        """
        for ticker_symbol in self.get_yahoo_symbol_candidates(stock_code):
            try:
                ticker = yf.Ticker(ticker_symbol)
                info = ticker.fast_info
                if info and isinstance(info, dict):
                    short_name = info.get("shortName")
                    if short_name:
                        return short_name

                info = ticker.info
                if info and isinstance(info, dict):
                    name = info.get("shortName") or info.get("longName")
                    if name:
                        return name
            except Exception:
                continue

        return "N/A"

    def get_stock_data(self, stock_code, days=120):
        """
        Fetch stock data from Yahoo Finance with retry logic

        Args:
            stock_code: Stock code (e.g., 'HK.00700' for Tencent)
            days: Number of days of historical data to fetch

        Returns:
            Tuple of (DataFrame with historical data, error_message)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)  # Extra buffer for market holidays
        symbols = self.get_yahoo_symbol_candidates(stock_code)
        last_error = None

        for ticker_symbol in symbols:
            print(f"  Fetching Yahoo data for {ticker_symbol}: {start_date.date()} to {end_date.date()}")

            for attempt in range(self.max_retries + 1):
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    data = ticker.history(
                        start=start_date.strftime("%Y-%m-%d"),
                        end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                        interval="1d",
                        auto_adjust=False,
                        actions=False,
                    )

                    if data is None or data.empty:
                        last_error = f"No data for {ticker_symbol}"
                        # Empty data for a symbol is usually not transient; try next candidate.
                        break

                    # Normalize column names to match existing processing flow.
                    data = data.reset_index()
                    data = data.rename(
                        columns={
                            "Date": "time_key",
                            "Open": "open",
                            "Close": "close",
                        }
                    )

                    if "time_key" in data.columns:
                        data["time_key"] = pd.to_datetime(data["time_key"]).dt.tz_localize(None)

                    if "open" not in data.columns or "close" not in data.columns:
                        last_error = f"Missing required price columns for {ticker_symbol}"
                        break

                    time.sleep(self.rate_limit_delay)
                    return data, None

                except Exception as e:
                    error_msg = str(e)
                    last_error = f"{ticker_symbol}: {error_msg}"

                    # 404/not-found for a symbol is not transient; try next symbol candidate.
                    lower = error_msg.lower()
                    if "404" in lower or "not found" in lower:
                        break

                    if attempt < self.max_retries:
                        print(f"  Exception: {error_msg} - Retrying ({attempt + 1}/{self.max_retries})...")
                        time.sleep(self.rate_limit_delay * 2)
                        continue

        return None, self.classify_unavailable_reason(stock_code, symbols, last_error)

    def calculate_percentage_change(self, current_price, past_price):
        """Calculate percentage change between two prices"""
        if past_price == 0 or pd.isna(past_price):
            return None
        return ((current_price - past_price) / past_price) * 100

    def generate_stock_report(self, stock_codes, location_map=None):
        """
        Generate comprehensive stock report for multiple stocks

        Args:
            stock_codes: List of stock codes (e.g., ['HK.00700', 'HK.00005'])
            location_map: Dictionary mapping stock codes to their locations

        Returns:
            Tuple of (DataFrame with all calculated metrics, dict of missing stocks with reasons)
        """
        results = []
        missing_stocks = {}

        for stock_code in stock_codes:
            print(f"Processing {stock_code}...")

            data, error_msg = self.get_stock_data(stock_code, days=FETCH_DAYS)

            if data is None or len(data) == 0:
                location = location_map.get(stock_code, "Unknown") if location_map else "Unknown"
                missing_stocks[stock_code] = {"location": location, "error": error_msg or "No data"}
                print(f"No data available for {stock_code}")
                continue

            stock_name = self.get_stock_name(stock_code)

            data = data.sort_values("time_key", ascending=False).reset_index(drop=True)

            latest = data.iloc[0]
            opening_price = latest["open"]
            closing_price = latest["close"]

            pct_change_1d = self.calculate_percentage_change(
                closing_price,
                data.iloc[1]["close"] if len(data) > 1 else None,
            )

            pct_change_3d = self.calculate_percentage_change(
                closing_price,
                data.iloc[3]["close"] if len(data) > 3 else None,
            )

            pct_change_5d = self.calculate_percentage_change(
                closing_price,
                data.iloc[5]["close"] if len(data) > 5 else None,
            )

            pct_change_30d = self.calculate_percentage_change(
                closing_price,
                data.iloc[30]["close"] if len(data) > 30 else None,
            )

            pct_change_60d = self.calculate_percentage_change(
                closing_price,
                data.iloc[60]["close"] if len(data) > 60 else None,
            )

            pct_change_120d = self.calculate_percentage_change(
                closing_price,
                data.iloc[120]["close"] if len(data) > 120 else None,
            )

            date_value = latest["time_key"]
            if isinstance(date_value, pd.Timestamp):
                date_str = date_value.strftime("%Y-%m-%d")
            elif isinstance(date_value, datetime):
                date_str = date_value.strftime("%Y-%m-%d")
            else:
                date_str = str(date_value).split()[0] if " " in str(date_value) else str(date_value)

            result = {
                "Stock Code": stock_code,
                "Stock Name": stock_name,
                "Location": location_map.get(stock_code, "Unknown") if location_map else "Unknown",
                "Date": date_str,
                "Opening Price": opening_price,
                "Closing Price": closing_price,
                "% Change vs Yesterday": round(pct_change_1d, 2) if pct_change_1d is not None else None,
                "% Change vs 3 Days": round(pct_change_3d, 2) if pct_change_3d is not None else None,
                "% Change vs 5 Days": round(pct_change_5d, 2) if pct_change_5d is not None else None,
                "% Change vs 30 Days": round(pct_change_30d, 2) if pct_change_30d is not None else None,
                "% Change vs 60 Days": round(pct_change_60d, 2) if pct_change_60d is not None else None,
                "% Change vs 120 Days": round(pct_change_120d, 2) if pct_change_120d is not None else None,
            }

            results.append(result)
            time.sleep(0.1)

        df = pd.DataFrame(results)
        if not df.empty and "% Change vs Yesterday" in df.columns:
            df = df.sort_values("% Change vs Yesterday", ascending=False, na_position="last")
        return df, missing_stocks

    def authenticate_google_sheets(self):
        """Authenticate and return Google Sheets client"""
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        return gspread.authorize(creds)

    def open_or_create_sheet(self):
        """Open existing worksheet or create new one with headers"""
        sheet = self.gc.open_by_url(GOOGLE_SHEET_URL)
        try:
            worksheet = sheet.worksheet(WORKSHEET_TITLE)
            print(f"Found existing worksheet: {WORKSHEET_TITLE}")
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=WORKSHEET_TITLE, rows="1000", cols="20")
            headers = [
                "Stock Code",
                "Stock Name",
                "Location",
                "Date",
                "Opening Price",
                "Closing Price",
                "% Change vs Yesterday",
                "% Change vs 3 Days",
                "% Change vs 5 Days",
                "% Change vs 30 Days",
                "% Change vs 60 Days",
                "% Change vs 120 Days",
            ]
            worksheet.append_row(headers)
            print(f"Created new worksheet: {WORKSHEET_TITLE}")
        return worksheet

    def update_google_sheet(self, df):
        """
        Update Google Sheets with stock data

        Args:
            df: DataFrame with stock data to upload
        """
        if df is None or len(df) == 0:
            print("No data to update in Google Sheets")
            return

        try:
            self.sheet.clear()

            headers = [
                "Stock Code",
                "Stock Name",
                "Location",
                "Date",
                "Opening Price",
                "Closing Price",
                "% Change vs Yesterday",
                "% Change vs 3 Days",
                "% Change vs 5 Days",
                "% Change vs 30 Days",
                "% Change vs 60 Days",
                "% Change vs 120 Days",
            ]

            data_rows = []
            for _, row in df.iterrows():
                data_row = [
                    str(row["Stock Code"]),
                    str(row["Stock Name"]),
                    str(row["Location"]),
                    str(row["Date"]),
                    row["Opening Price"],
                    row["Closing Price"],
                    row["% Change vs Yesterday"] if pd.notna(row["% Change vs Yesterday"]) else "",
                    row["% Change vs 3 Days"] if pd.notna(row["% Change vs 3 Days"]) else "",
                    row["% Change vs 5 Days"] if pd.notna(row["% Change vs 5 Days"]) else "",
                    row["% Change vs 30 Days"] if pd.notna(row["% Change vs 30 Days"]) else "",
                    row["% Change vs 60 Days"] if pd.notna(row["% Change vs 60 Days"]) else "",
                    row["% Change vs 120 Days"] if pd.notna(row["% Change vs 120 Days"]) else "",
                ]
                data_rows.append(data_row)

            all_rows = [headers] + data_rows
            self.sheet.append_rows(all_rows, value_input_option="USER_ENTERED")

            print("Applying color formatting...")
            if data_rows:
                sheet_id = self.sheet.id
                last_row_index = len(data_rows) + 1  # 1-based row number including header

                requests = [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,  # row 2 in sheet (0-based index)
                                "endRowIndex": last_row_index,
                                "startColumnIndex": 6,  # column G
                                "endColumnIndex": 7,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                                    "textFormat": {"bold": False},
                                }
                            },
                            "fields": "userEnteredFormat(backgroundColor,textFormat.bold)",
                        }
                    }
                ]

                for row_idx, (_, row) in enumerate(df.iterrows(), start=2):
                    pct_change = pd.to_numeric(row.get("% Change vs Yesterday"), errors="coerce")
                    if pd.isna(pct_change) or pct_change == 0:
                        continue

                    if pct_change > 0:
                        bg = {"red": 0.7, "green": 1.0, "blue": 0.7}
                    else:
                        bg = {"red": 1.0, "green": 0.7, "blue": 0.7}

                    requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": row_idx - 1,
                                    "endRowIndex": row_idx,
                                    "startColumnIndex": 6,
                                    "endColumnIndex": 7,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": bg,
                                        "textFormat": {"bold": True},
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,textFormat.bold)",
                            }
                        }
                    )

                self.sheet.spreadsheet.batch_update({"requests": requests})

            print(f"\nGoogle Sheets updated successfully with {len(data_rows)} stocks")
            print(f"View at: {GOOGLE_SHEET_URL}")

        except Exception as e:
            print(f"Error updating Google Sheets: {e}")

    def save_to_csv(self, df, filename=None):
        """
        Save DataFrame to CSV file (optional backup)

        Args:
            df: DataFrame to save
            filename: Output filename (default: hk_stocks_YYYYMMDD.csv)
        """
        if filename is None:
            filename = f"hk_stocks_{datetime.now().strftime('%Y%m%d')}.csv"

        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"Data also saved to CSV: {filename}")
        return filename

    def close(self):
        """No persistent data provider connection to close for Yahoo Finance."""
        return


def load_stock_codes_with_location(filename=BASE_DIR / "fullstocks.txt"):
    """
    Load stock codes with their location information from a text file

    Args:
        filename: Path to file with stock codes (one per line with section headers)

    Returns:
        Tuple of (list of stock codes, dictionary mapping codes to locations)
    """
    file_path = Path(filename)

    if not file_path.exists():
        print(f"ERROR: {file_path} not found!")
        print(f"Please create {file_path} with your stock codes (one per line)")
        return [], {}

    stock_codes = []
    location_map = {}
    current_location = "Unknown"

    try:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    current_location = line.lstrip("#").strip()
                    continue

                if not line:
                    continue

                code = line.split()[0] if line.split() else ""
                if code and (code.startswith("HK.") or code.startswith("US.")):
                    if code not in stock_codes:
                        stock_codes.append(code)
                    location_map[code] = current_location

        if not stock_codes:
            print(f"WARNING: No valid stock codes found in {file_path}")
            print("Stock codes should be in format: HK.XXXXX or US.XXXXX")

        return stock_codes, location_map

    except Exception as e:
        print(f"ERROR reading {file_path}: {e}")
        return [], {}


def load_stock_codes(filename="stocks.txt"):
    """
    Load stock codes from a text file

    Args:
        filename: Path to file with stock codes (one per line)

    Returns:
        List of stock codes
    """
    if not os.path.exists(filename):
        print(f"ERROR: {filename} not found!")
        print(f"Please create {filename} with your stock codes (one per line)")
        return []

    stock_codes = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    code = line.split()[0] if line.split() else ""
                    if code and (code.startswith("HK.") or code.startswith("US.")):
                        stock_codes.append(code)

        if not stock_codes:
            print(f"WARNING: No valid stock codes found in {filename}")
            print("Stock codes should be in format: HK.XXXXX or US.XXXXX")

        return stock_codes

    except Exception as e:
        print(f"ERROR reading {filename}: {e}")
        return []


def main():
    """Main function to run the stock tracker"""

    stock_codes, location_map = load_stock_codes_with_location()
    if not stock_codes:
        return

    print("Stock Price Tracker (Yahoo Finance)")
    print("=" * 60)
    print(f"\nRate Limit: {RATE_LIMIT_DELAY}s between requests")
    print(f"Max Retries: {MAX_RETRIES}")
    print(f"Historical Data: {FETCH_DAYS} days")
    print("=" * 60)

    try:
        tracker = StockTracker(
            rate_limit_delay=RATE_LIMIT_DELAY,
            max_retries=MAX_RETRIES,
        )

        print(f"\nFetching data for {len(stock_codes)} stocks...\n")
        df, missing_stocks = tracker.generate_stock_report(stock_codes, location_map)

        print("\n" + "=" * 60)
        print("RESULTS:")
        print("=" * 60)
        print(df.to_string(index=False))

        tracker.update_google_sheet(df)

        if missing_stocks:
            print("\n" + "=" * 60)
            print("MISSING STOCKS (No data from Yahoo Finance):")
            print("=" * 60)

            for stock, info in missing_stocks.items():
                print(f"{stock:12} | {info['location']:20} | {info['error']}")

            print(f"\nTotal missing: {len(missing_stocks)} out of {len(stock_codes)} stocks")
            print("\nNote: Some tickers may be delisted, suspended, or unavailable on Yahoo Finance.")

        tracker.close()

        print("\nDone!")
        print("JOSLIST_DONE")

    except Exception as e:
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Is your internet connection available?")
        print("2. Are your stock codes valid (HK.XXXXX or US.XXXX)?")
        print("3. Is your Google Sheets credential file configured correctly?")


if __name__ == "__main__":
    main()