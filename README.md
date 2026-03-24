# Top Gainers Analyzer with Smart Tracking - Futu API

This script fetches market data using the Futu API and displays top gainers with intelligent tracking of significant movers over time.

## Prerequisites

1. **Install FutuOpenD**: Download and install FutuOpenD from [Futu's official website](https://openapi.futunn.com/futu-api-doc/)
2. **Run FutuOpenD**: Make sure FutuOpenD is running on your system (default: localhost:11111)
3. **Futu Account**: You need a Futu account with API access enabled

## Installation

1. Install the required Python packages:
```bash
pip install -r requirements.txt
```

2. Create your local environment file from the template:
```bash
# Windows PowerShell
Copy-Item .env.example .env

# macOS/Linux
cp .env.example .env
```

3. Edit `.env` and set your real credentials (Telegram + Perplexity keys).
   The `.env` file is gitignored and should not be committed.

## Usage

1. Make sure FutuOpenD is running on your machine
2. Run the script:
```bash
python top_gainers.py
```

3. Main Menu Options:
   - **Market Scans (1-3)**: View top gainers and 5-minute movers
   - **Watchlist Management (4-6)**: Manage tracked stocks for each market
   - **System (7-8)**: Check API quota and exit

## Features

### 📊 Real-Time Market Analysis
- **Top Daily Gainers**: Shows stocks with highest daily percentage gains
- **Top 5-Minute Movers**: Displays stocks with biggest 5-minute movements
- **Continued Movement Tracking**: Highlights tracked stocks with sustained trends

### 🎯 Smart Auto-Tracking
- Automatically tracks stocks with **>7% movement in 5 minutes**
- Monitors continued price movement after initial detection
- Shows total percentage change since first detection
- **Quota-Friendly**: Uses only 1 extra API call per scan to check all tracked stocks

### 💾 Persistent Watchlist
- **Auto-Cleanup**: Removes tracked stocks older than 3 days automatically
- **Manual Pin**: Mark important stocks as permanent (won't auto-delete)
- **Quick Remove**: Instantly remove stocks you're no longer interested in
- **Manual Add**: Add any stock to your watchlist manually

### 🔧 Watchlist Management
Per market (US/HK/CN), you can:
1. **Pin a stock** - Make it permanent (survives 3-day cleanup)
2. **Unpin a stock** - Allow auto-cleanup after 3 days
3. **Remove a stock** - Delete immediately from watchlist
4. **Add a stock** - Manually add any stock code with permanent option

### 📈 Data Displayed
For each stock:
- Stock symbol and company name
- Current price and initial tracked price
- Daily change percentage
- 5-minute change percentage
- Total change since first detection
- Days tracked
- Pin status (📌 for permanent stocks)

## API Quota Management

The system is designed to be **quota-friendly**:
- Scans all 3,621 HK stocks using ~10 batched API calls (400 stocks per call)
- Tracked stocks checked with single batch call (not per-stock)
- Rate limit: ~30 requests per 30 seconds (well within limits)
- Option 7 displays your current API quota usage

## Configuration

If your FutuOpenD is running on a different host or port, modify the initialization in `top_gainers.py`:

```python
analyzer = TopGainersAnalyzer(host='your_host', port=your_port)
```

## Data Storage

Tracked stocks are stored in `tracked_movers.json` with:
- Stock code, name, and market
- Initial price and 5-minute change at detection
- First seen timestamp
- Permanent flag (for pinned stocks)

## Example Workflow

1. **Run scan**: `python top_gainers.py` → Select option 2 (HK Market)
2. **Auto-tracking**: System automatically tracks significant movers (>7% in 5min)
3. **View continued movers**: Next scan shows which tracked stocks continued moving
4. **Pin important ones**: Use option 5 to pin stocks you want to monitor permanently
5. **Auto-cleanup**: Non-pinned stocks auto-delete after 3 days

## Notes

- Ensure you have proper API permissions for the markets you want to access
- Data is fetched in real-time when FutuOpenD is connected
- Tracking file is stored locally and persists between sessions
- Cleanup runs automatically on script startup
