#!/usr/bin/env python3
"""
Unified market scanner — daily gainers, 5-min movers, gap-up scoring, and Telegram alerts.
"""

from futu import *
import pandas as pd
import json
import os
from datetime import datetime, timedelta

from scoring import (
    score_gap_up,
    score_intraday,
    save_scan_snapshot,
    save_prev_day_turnover,
    is_on_cooldown,
    mark_alerted,
    build_telegram_message,
    build_intraday_message,
    send_telegram,
    _format_turnover,
)


class TopGainersAnalyzer:
    def __init__(self, host='127.0.0.1', port=11111,
                 bot_token='', chat_id=''):
        """
        Initialize the Futu API connection
        
        Args:
            host: FutuOpenD host address
            port: FutuOpenD port number
            bot_token: Telegram bot token (from @BotFather)
            chat_id: Telegram chat/group ID to send alerts to
        """
        self.quote_ctx = OpenQuoteContext(host=host, port=port)
        self.host = host
        self.port = port
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.tracking_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'tracked_movers.json')
        self.cleanup_old_tracks()  # Clean up old entries on startup
        
    def test_connection(self):
        """Test if connection to FutuOpenD is working"""
        try:
            ret, data = self.quote_ctx.get_global_state()
            if ret == RET_OK:
                print(f"✓ Successfully connected to FutuOpenD at {self.host}:{self.port}")
                print(f"  Market State: {data}")
                return True
            else:
                print(f"✗ Connection failed: {data}")
                return False
        except Exception as e:
            print(f"✗ Cannot connect to FutuOpenD: {e}")
            print(f"\nPlease ensure:")
            print(f"  1. FutuOpenD is installed and running")
            print(f"  2. FutuOpenD is listening on {self.host}:{self.port}")
            print(f"  3. You are logged into your Futu account in FutuOpenD")
            return False
    
    def check_quota(self):
        """Check API usage quota"""
        try:
            ret, data = self.quote_ctx.query_subscription()
            if ret == RET_OK:
                print("\n" + "="*80)
                print("API QUOTA STATUS")
                print("="*80)
                if data is not None:
                    if isinstance(data, pd.DataFrame) and not data.empty:
                        print(f"\nSubscribed Markets: {len(data)} total")
                        for idx, row in data.iterrows():
                            print(f"  - {row.get('code', 'N/A')}: {row.get('used_quota', 0)}/{row.get('total_quota', 0)} used")
                    else:
                        print(f"\nSubscription Data: {data}")
                else:
                    print("No detailed quota information available")
                
                # Check historical K-line quota
                print("\n📈 Historical K-Line (Candlestick) Quota:")
                try:
                    ret_hist, hist_data = self.quote_ctx.get_history_kl_quota(get_detail=True)
                    if ret_hist == RET_OK and hist_data is not None:
                        if isinstance(hist_data, tuple) and len(hist_data) >= 2:
                            used = hist_data[0]
                            remaining = hist_data[1]
                            total = used + remaining if remaining > 0 else used
                            print(f"  • Used today: {used} requests")
                            print(f"  • Remaining today: {remaining} requests")
                            print(f"  • Total daily quota: {total} requests")
                            usage_pct = (used / total * 100) if total > 0 else 0
                            if usage_pct > 80:
                                print(f"  ⚠️  WARNING: {usage_pct:.1f}% of daily quota used!")
                        elif isinstance(hist_data, dict):
                            print(f"  • Used today: {hist_data.get('used_quota', 0)}")
                            print(f"  • Remaining today: {hist_data.get('remain_quota', 0)}")
                            print(f"  • Total quota: {hist_data.get('total_quota', 0)}")
                        elif isinstance(hist_data, pd.DataFrame) and not hist_data.empty:
                            for idx, row in hist_data.iterrows():
                                print(f"  • {row.get('code', 'N/A')}: {row.get('used_quota', 0)}/{row.get('remain_quota', 0)} remaining")
                        else:
                            print(f"  • Quota info available (see raw data)")
                    else:
                        print(f"  • Could not retrieve K-line quota")
                except Exception as e:
                    print(f"  • K-line quota check unavailable")
                
                print("\n📊 Futu API Limits:")
                print("  • Real-time quotes: ~400 stocks per request, ~30 requests/30sec")
                print("  • Historical K-line: Limited daily quota (typically 50-200 requests/day)")
                print("  • This script uses: ~10 API calls per market scan (real-time only)")
                print("  • Watchlist check: 1 additional API call")
                print("="*80 + "\n")
                return True
            else:
                print(f"\n⚠️  Could not retrieve quota info: {data}")
                print("\n📊 Futu API General Limits:")
                print("  • Real-time: 400 stocks per call, ~30 requests/30sec")
                print("  • Historical K-line: Limited daily quota (50-200 requests/day)")
                print("  • This script: ~10 batched API calls per scan + 1 for watchlist")
                return False
        except Exception as e:
            print(f"\n⚠️  Error checking quota: {e}")
            print("\n📊 Futu API General Limits:")
            print("  • Real-time: 400 stocks per call, ~30 requests/30sec")
            print("  • Historical K-line: Limited daily quota (varies by account)")
            print("  • This script: Uses only real-time data (no K-line quota used)")
            return False
    
    def load_tracked_stocks(self):
        """Load tracked stocks from JSON file"""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_tracked_stocks(self, tracked):
        """Save tracked stocks to JSON file"""
        with open(self.tracking_file, 'w') as f:
            json.dump(tracked, f, indent=2)
    
    def add_to_tracking(self, stock_code, name, initial_price, initial_5min_change, market, permanent=False):
        """Add a stock to tracking"""
        tracked = self.load_tracked_stocks()
        tracked[stock_code] = {
            'name': name,
            'initial_price': initial_price,
            'initial_5min_change': initial_5min_change,
            'first_seen': datetime.now().isoformat(),
            'market': market,
            'permanent': permanent
        }
        self.save_tracked_stocks(tracked)
    
    def remove_from_tracking(self, stock_code):
        """Remove a stock from tracking"""
        tracked = self.load_tracked_stocks()
        if stock_code in tracked:
            del tracked[stock_code]
            self.save_tracked_stocks(tracked)
            return True
        return False
    
    def cleanup_old_tracks(self):
        """Remove tracks older than 3 days (except permanent ones)"""
        tracked = self.load_tracked_stocks()
        cutoff_date = datetime.now() - timedelta(days=3)
        
        to_remove = []
        for code, data in tracked.items():
            if not data.get('permanent', False):
                first_seen = datetime.fromisoformat(data['first_seen'])
                if first_seen < cutoff_date:
                    to_remove.append(code)
        
        for code in to_remove:
            del tracked[code]
        
        if to_remove:
            self.save_tracked_stocks(tracked)
            print(f"🧹 Cleaned up {len(to_remove)} old tracked stocks (older than 3 days)")
    
    def check_continued_movement(self, market='HK'):
        """Check tracked stocks for continued movement"""
        tracked = self.load_tracked_stocks()
        
        if not tracked:
            return pd.DataFrame()
        
        # Filter by market
        market_stocks = {code: data for code, data in tracked.items() 
                        if data.get('market') == market}
        
        if not market_stocks:
            return pd.DataFrame()
        
        # Get current prices for tracked stocks (quota-friendly: single batch call)
        stock_codes = list(market_stocks.keys())
        
        try:
            ret, snapshot = self.quote_ctx.get_market_snapshot(stock_codes)
            
            if ret != RET_OK or snapshot is None or snapshot.empty:
                return pd.DataFrame()
            
            # Calculate continued movement
            results = []
            for idx, row in snapshot.iterrows():
                code = row['code']
                if code in market_stocks:
                    tracked_data = market_stocks[code]
                    current_price = row['last_price']
                    initial_price = tracked_data['initial_price']
                    
                    # Calculate total movement since first detection
                    total_change = ((current_price - initial_price) / initial_price * 100) if initial_price > 0 else 0
                    
                    # Only include if significant continued movement (>3%)
                    if abs(total_change) >= 3.0:
                        results.append({
                            'code': code,
                            'name': tracked_data['name'],
                            'current_price': current_price,
                            'initial_price': initial_price,
                            'initial_5min_change': tracked_data['initial_5min_change'],
                            'total_change': total_change,
                            'first_seen': tracked_data['first_seen'],
                            'permanent': tracked_data.get('permanent', False),
                            'days_tracked': (datetime.now() - datetime.fromisoformat(tracked_data['first_seen'])).days
                        })
            
            if results:
                df = pd.DataFrame(results)
                df = df.sort_values('total_change', key=abs, ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error checking continued movement: {e}")
            return pd.DataFrame()
    
    def display_continued_movers(self, market='HK'):
        """Display stocks with continued significant movement"""
        continued = self.check_continued_movement(market=market)
        
        if continued.empty:
            print(f"\n💤 No tracked stocks with significant continued movement (>3%)")
            return
        
        print(f"\n{'='*90}")
        print(f"🔥 TRACKED STOCKS WITH CONTINUED MOVEMENT - {market} MARKET")
        print(f"{'='*90}\n")
        print(f"{'Rank':<6} {'Symbol':<10} {'Name':<25} {'Current':<10} {'Initial':<10} {'Total %':<12} {'Days':<6} {'Pin':<5}")
        print(f"{'-'*90}")
        
        for idx, row in continued.iterrows():
            rank = idx + 1
            symbol = row['code']
            name = row['name'][:24]
            current = row['current_price']
            initial = row['initial_price']
            total_change = row['total_change']
            days = row['days_tracked']
            pin = '📌' if row['permanent'] else ''
            
            print(f"{rank:<6} {symbol:<10} {name:<25} ${current:<9.2f} ${initial:<9.2f} {total_change:>+11.2f}% {days:<6} {pin:<5}")
        
        print(f"{'-'*90}\n")
    
    def display_watchlist_activity(self, market='HK'):
        """Display all watchlist activity with current prices and changes"""
        tracked = self.load_tracked_stocks()
        
        # Filter by market
        market_stocks = {code: data for code, data in tracked.items() 
                        if data.get('market') == market}
        
        if not market_stocks:
            return  # Silently skip if no stocks tracked for this market
        
        # Get current prices for tracked stocks (quota-friendly: single batch call)
        stock_codes = list(market_stocks.keys())
        
        try:
            ret, snapshot = self.quote_ctx.get_market_snapshot(stock_codes)
            
            if ret != RET_OK or snapshot is None or snapshot.empty:
                return  # Silently skip if can't fetch data
            
            # Build activity data
            results = []
            for idx, row in snapshot.iterrows():
                code = row['code']
                if code in market_stocks:
                    tracked_data = market_stocks[code]
                    current_price = row['last_price']
                    initial_price = tracked_data['initial_price']
                    first_seen = datetime.fromisoformat(tracked_data['first_seen'])
                    time_elapsed = datetime.now() - first_seen
                    
                    # Calculate total change
                    total_change = ((current_price - initial_price) / initial_price * 100) if initial_price > 0 else 0
                    
                    # Format time elapsed
                    if time_elapsed.days > 0:
                        time_str = f"{time_elapsed.days}d {time_elapsed.seconds//3600}h"
                    else:
                        hours = time_elapsed.seconds // 3600
                        minutes = (time_elapsed.seconds % 3600) // 60
                        if hours > 0:
                            time_str = f"{hours}h {minutes}m"
                        else:
                            time_str = f"{minutes}m"
                    
                    results.append({
                        'code': code,
                        'name': tracked_data['name'],
                        'initial_price': initial_price,
                        'current_price': current_price,
                        'total_change': total_change,
                        'time_elapsed': time_str,
                        'first_seen': first_seen.strftime('%m/%d %H:%M'),
                        'permanent': tracked_data.get('permanent', False),
                        'abs_change': abs(total_change)
                    })
            
            if not results:
                return  # Silently skip if no tracked stocks
            
            # Filter for meaningful movement (>3% change)
            meaningful_movers = [r for r in results if r['abs_change'] >= 3.0]
            
            if not meaningful_movers:
                return  # Silently skip if no meaningful movement
            
            # Sort by absolute change (biggest movers first)
            results_sorted = sorted(meaningful_movers, key=lambda x: x['abs_change'], reverse=True)
            
            print(f"\n{'='*110}")
            print(f"🔥 WATCHLIST: MEANINGFUL MOVERS - {market} MARKET ({len(meaningful_movers)} of {len(results)} tracked stocks)")
            print(f"{'='*110}\n")
            print(f"{'#':<4} {'Symbol':<10} {'Name':<23} {'Initial':<10} {'Current':<10} {'Change %':<12} {'Elapsed':<11} {'Since':<13} {'Pin':<4}")
            print(f"{'-'*110}")
            
            for i, data in enumerate(results_sorted, 1):
                pin = '📌' if data['permanent'] else ''
                print(f"{i:<4} {data['code']:<10} {data['name'][:22]:<23} ${data['initial_price']:<9.3f} "
                      f"${data['current_price']:<9.3f} {data['total_change']:>+11.2f}% {data['time_elapsed']:<11} "
                      f"{data['first_seen']:<13} {pin:<4}")
            
            print(f"{'-'*110}\n")
            
        except Exception as e:
            pass  # Silently skip on error
    
    def manage_watchlist(self, market='HK'):
        """Interactive watchlist management"""
        while True:
            tracked = self.load_tracked_stocks()
            market_stocks = {code: data for code, data in tracked.items() 
                           if data.get('market') == market}
            
            print(f"\n{'='*80}")
            print(f"📋 WATCHLIST MANAGEMENT - {market} MARKET")
            print(f"{'='*80}")
            
            if not market_stocks:
                print("\n📭 No stocks currently tracked for this market")
            else:
                print(f"\n{len(market_stocks)} stocks tracked:\n")
                print(f"{'#':<4} {'Symbol':<10} {'Name':<30} {'Days':<6} {'Pinned':<8}")
                print(f"{'-'*80}")
                
                sorted_stocks = sorted(market_stocks.items(), 
                                     key=lambda x: datetime.fromisoformat(x[1]['first_seen']), 
                                     reverse=True)
                
                for i, (code, data) in enumerate(sorted_stocks, 1):
                    days = (datetime.now() - datetime.fromisoformat(data['first_seen'])).days
                    pinned = '📌' if data.get('permanent') else ''
                    print(f"{i:<4} {code:<10} {data['name'][:29]:<30} {days:<6} {pinned:<8}")
            
            print(f"\n{'='*80}")
            print("Options:")
            print("  1. Pin a stock (make permanent)")
            print("  2. Unpin a stock")
            print("  3. Remove a stock")
            print("  4. Add a stock manually")
            print("  5. Back to main menu")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                code = input("Enter stock code to pin (e.g., HK.00700): ").strip()
                tracked = self.load_tracked_stocks()
                if code in tracked:
                    tracked[code]['permanent'] = True
                    self.save_tracked_stocks(tracked)
                    print(f"✅ Pinned {code}")
                else:
                    print(f"❌ Stock {code} not found in watchlist")
            
            elif choice == '2':
                code = input("Enter stock code to unpin: ").strip()
                tracked = self.load_tracked_stocks()
                if code in tracked:
                    tracked[code]['permanent'] = False
                    self.save_tracked_stocks(tracked)
                    print(f"✅ Unpinned {code}")
                else:
                    print(f"❌ Stock {code} not found in watchlist")
            
            elif choice == '3':
                code = input("Enter stock code to remove: ").strip()
                if self.remove_from_tracking(code):
                    print(f"✅ Removed {code} from watchlist")
                else:
                    print(f"❌ Stock {code} not found in watchlist")
            
            elif choice == '4':
                code = input("Enter stock code (e.g., HK.00700): ").strip()
                try:
                    ret, snapshot = self.quote_ctx.get_market_snapshot([code])
                    if ret == RET_OK and not snapshot.empty:
                        row = snapshot.iloc[0]
                        permanent = input("Make permanent? (y/n): ").strip().lower() == 'y'
                        self.add_to_tracking(
                            code, 
                            row['name'], 
                            row['last_price'],
                            0.0,  # No initial 5min change for manual adds
                            market,
                            permanent
                        )
                        print(f"✅ Added {code} to watchlist")
                    else:
                        print(f"❌ Could not fetch data for {code}")
                except Exception as e:
                    print(f"❌ Error: {e}")
            
            elif choice == '5':
                break
            else:
                print("Invalid choice")
        
    def get_market_snapshot(self, market='US', stock_type='STOCK'):
        """
        Get market snapshot data for all stocks
        
        Args:
            market: Market type ('US', 'HK', 'CN')
            stock_type: Type of security ('STOCK', 'ETF', etc.)
            
        Returns:
            DataFrame with stock data including change rate
        """
        try:
            # Get stock list for the market
            if market == 'US':
                market_type = Market.US
            elif market == 'HK':
                market_type = Market.HK
            elif market == 'CN':
                market_type = Market.CN
            else:
                market_type = Market.US
                
            # Get plate stocks or use market snapshot
            ret, data = self.quote_ctx.get_market_snapshot(Market.US)
            
            if ret == RET_OK:
                return data
            else:
                print(f'Error getting market snapshot: {data}')
                return None
                
        except Exception as e:
            print(f'Exception occurred: {e}')
            return None
    
    def get_top_gainers(self, market='US', count=10):
        """
        Get top gainers by daily change rate
        
        Args:
            market: Market type ('US', 'HK', 'CN')
            count: Number of top gainers to return
            
        Returns:
            DataFrame with top gainers
        """
        try:
            # Set market type
            if market == 'US':
                market_type = Market.US
            elif market == 'HK':
                market_type = Market.HK
            elif market == 'CN':
                market_type = Market.SH  # Shanghai Stock Exchange
            else:
                market_type = Market.US
            
            # Get list of all stocks in the market
            ret, stock_list = self.quote_ctx.get_stock_basicinfo(market=market_type, stock_type=SecurityType.STOCK)
            
            if ret != RET_OK or stock_list is None or stock_list.empty:
                print(f'Error getting stock list: {stock_list}')
                if 'No right' in str(stock_list):
                    print(f'⚠️  You may not have quote access for {market} market.')
                    print(f'   Please enable this market in your Futu account settings.')
                return None
            
            # Scan all available stocks
            stock_codes = stock_list['code'].tolist()
            print(f"Scanning {len(stock_codes)} stocks...")
            
            # Get market snapshot in batches (API limit is 400 per call)
            all_snapshots = []
            batch_size = 400
            
            for i in range(0, len(stock_codes), batch_size):
                batch = stock_codes[i:i+batch_size]
                ret, batch_data = self.quote_ctx.get_market_snapshot(batch)
                
                if ret == RET_OK and batch_data is not None and not batch_data.empty:
                    all_snapshots.append(batch_data)
                    
            if not all_snapshots:
                print(f'Error getting market snapshot')
                return None
            
            snapshot_data = pd.concat(all_snapshots, ignore_index=True)
            
            # Calculate change rate from price data
            snapshot_data['change_rate'] = ((snapshot_data['last_price'] - snapshot_data['prev_close_price']) / 
                                            snapshot_data['prev_close_price'] * 100)
            
            # Filter for positive gains and sort by change rate
            gainers = snapshot_data[snapshot_data['change_rate'] > 0].copy()
            gainers = gainers.sort_values('change_rate', ascending=False)
            
            # Return top N
            return gainers.head(count)
                
        except Exception as e:
            print(f'Exception occurred: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def get_top_5min_movers(self, market='US', count=10):
        """
        Get top 5-minute movers
        
        Args:
            market: Market type ('US', 'HK', 'CN')
            count: Number of top movers to return
            
        Returns:
            DataFrame with top 5-minute movers
        """
        try:
            # Set market type
            if market == 'US':
                market_type = Market.US
            elif market == 'HK':
                market_type = Market.HK
            elif market == 'CN':
                market_type = Market.SH
            else:
                market_type = Market.US
            
            # Get list of all stocks in the market
            ret, stock_list = self.quote_ctx.get_stock_basicinfo(market=market_type, stock_type=SecurityType.STOCK)
            
            if ret != RET_OK or stock_list is None or stock_list.empty:
                print(f'Error getting stock list: {stock_list}')
                if 'No right' in str(stock_list):
                    print(f'⚠️  You may not have quote access for {market} market.')
                    print(f'   Please enable this market in your Futu account settings.')
                return None
            
            # Scan all available stocks
            stock_codes = stock_list['code'].tolist()
            print(f"Scanning {len(stock_codes)} stocks for 5min movers...")
            
            # Get market snapshot in batches (API limit is 400 per call)
            all_snapshots = []
            batch_size = 400
            
            for i in range(0, len(stock_codes), batch_size):
                batch = stock_codes[i:i+batch_size]
                ret, batch_data = self.quote_ctx.get_market_snapshot(batch)
                
                if ret == RET_OK and batch_data is not None and not batch_data.empty:
                    all_snapshots.append(batch_data)
                    
            if not all_snapshots:
                print(f'Error getting market snapshot')
                return None
            
            snapshot_data = pd.concat(all_snapshots, ignore_index=True)
            
            # Calculate 5-minute change rate if available
            if 'close_price_5min' not in snapshot_data.columns:
                print(f'⚠️  5-minute data not available for {market} market.')
                return None
            
            # Calculate 5-minute change
            snapshot_data['change_5min'] = snapshot_data.apply(
                lambda row: ((row['last_price'] - row['close_price_5min']) / row['close_price_5min'] * 100)
                if row['close_price_5min'] > 0 else 0, axis=1
            )
            
            # Filter for stocks with 5min price data and sort by absolute 5min change
            movers = snapshot_data[snapshot_data['close_price_5min'] > 0].copy()
            movers['abs_change_5min'] = movers['change_5min'].abs()
            movers = movers.sort_values('abs_change_5min', ascending=False)
            
            # Auto-track significant movers (>7% in 5 minutes) - quota-friendly: no extra API calls
            tracked = self.load_tracked_stocks()
            new_tracks = 0
            for idx, row in movers.head(20).iterrows():  # Check top 20 for tracking
                if abs(row['change_5min']) >= 7.0 and row['code'] not in tracked:
                    self.add_to_tracking(
                        row['code'],
                        row['name'],
                        row['last_price'],
                        row['change_5min'],
                        market,
                        permanent=False
                    )
                    new_tracks += 1
            
            if new_tracks > 0:
                print(f"✨ Auto-tracked {new_tracks} new significant mover(s) (>7% in 5min)")
            
            # Return top N
            return movers.head(count)
                
        except Exception as e:
            print(f'Exception occurred: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def display_top_gainers(self, market='US', count=10):
        """
        Display top gainers in a formatted way
        
        Args:
            market: Market type
            count: Number of top gainers to display
        """
        print(f"\n{'='*80}")
        print(f"TOP {count} DAILY GAINERS - {market} MARKET")
        print(f"{'='*80}\n")
        
        gainers = self.get_top_gainers(market=market, count=count)
        
        if gainers is not None and not gainers.empty:
            # Display the results
            print(f"{'Rank':<6} {'Symbol':<10} {'Name':<30} {'Price':<12} {'Change %':<12}")
            print(f"{'-'*80}")
            
            for idx, row in gainers.iterrows():
                rank = idx + 1
                symbol = row.get('code', 'N/A')
                name = row.get('name', 'N/A')
                price = row.get('last_price', 0)
                change_rate = row.get('change_rate', 0)
                
                print(f"{rank:<6} {symbol:<10} {name:<30} ${price:<11.2f} {change_rate:>+11.2f}%")
            
            print(f"{'-'*80}\n")
        else:
            print("No data available or error occurred.\n")
    
    def display_top_5min_movers(self, market='US', count=10):
        """
        Display top 5-minute movers in a formatted way
        
        Args:
            market: Market type
            count: Number of top movers to display
        """
        print(f"\n{'='*80}")
        print(f"TOP {count} 5-MINUTE MOVERS - {market} MARKET")
        print(f"{'='*80}\n")
        
        movers = self.get_top_5min_movers(market=market, count=count)
        
        if movers is not None and not movers.empty:
            # Display the results
            print(f"{'Rank':<6} {'Symbol':<10} {'Name':<30} {'Price':<12} {'5min %':<12}")
            print(f"{'-'*80}")
            
            for idx, row in movers.iterrows():
                rank = idx + 1
                symbol = row.get('code', 'N/A')
                name = row.get('name', 'N/A')
                price = row.get('last_price', 0)
                change_5min = row.get('change_5min', 0)
                
                print(f"{rank:<6} {symbol:<10} {name:<30} ${price:<11.2f} {change_5min:>+11.2f}%")
            
            print(f"{'-'*80}\n")
        else:
            print("No data available or error occurred.\n")
    
    def display_both(self, market='US', count=10):
        """
        Display both top daily gainers and top 5-minute movers
        
        Args:
            market: Market type
            count: Number of stocks to display in each table
        """
        # Show watchlist activity first (if any)
        self.display_watchlist_activity(market=market)
        
        # Then show current top gainers and 5min movers
        self.display_top_gainers(market=market, count=count)
        self.display_top_5min_movers(market=market, count=count)

    # ── Gap-up scoring & alerting ────────────────────────────────────────────

    def _fetch_full_snapshot(self, market='HK'):
        """Fetch snapshot for every stock in a market. Returns a DataFrame."""
        market_map = {'US': Market.US, 'HK': Market.HK, 'CN': Market.SH}
        market_type = market_map.get(market, Market.HK)

        ret, stock_list = self.quote_ctx.get_stock_basicinfo(
            market=market_type, stock_type=SecurityType.STOCK
        )
        if ret != RET_OK or stock_list is None or stock_list.empty:
            return pd.DataFrame()

        stock_codes = stock_list['code'].tolist()
        all_snapshots = []
        for i in range(0, len(stock_codes), 400):
            batch = stock_codes[i:i + 400]
            ret, batch_data = self.quote_ctx.get_market_snapshot(batch)
            if ret == RET_OK and batch_data is not None and not batch_data.empty:
                all_snapshots.append(batch_data)

        if not all_snapshots:
            return pd.DataFrame()

        snapshot = pd.concat(all_snapshots, ignore_index=True)
        if 'change_rate' not in snapshot.columns:
            snapshot['change_rate'] = (
                (snapshot['last_price'] - snapshot['prev_close_price'])
                / snapshot['prev_close_price'] * 100
            )

        # Compute 5-min change if Futu provides the reference price
        if 'close_price_5min' in snapshot.columns and 'change_5min' not in snapshot.columns:
            snapshot['change_5min'] = snapshot.apply(
                lambda row: (
                    (row['last_price'] - row['close_price_5min'])
                    / row['close_price_5min'] * 100
                ) if row.get('close_price_5min', 0) > 0 else 0.0,
                axis=1,
            )

        return snapshot

    def scan_gap_ups(self, market='HK'):
        """
        Score every gap-up stock in the market.

        Returns:
            alerts  — list of (row, result) with alert_tier == 'alert'
            watches — list of (row, result) with alert_tier == 'watch'
        """
        snapshot = self._fetch_full_snapshot(market)
        if snapshot.empty:
            return [], []

        # Only upward-gapping stocks
        candidates = snapshot[
            (snapshot['last_price'] > snapshot['prev_close_price']) &
            (snapshot['open_price'] > snapshot['prev_close_price'])
        ].copy()

        alerts, watches = [], []
        for _, row in candidates.iterrows():
            result = score_gap_up(row, market=market)
            if result['alert_tier'] == 'alert':
                alerts.append((row, result))
            elif result['alert_tier'] == 'watch':
                watches.append((row, result))

        # ── Second path: intraday breakouts (opened flat, spiked mid-session) ──
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
                breakouts = flat_open[flat_open['intraday_move'] >= 10.0]

            for _, row in breakouts.iterrows():
                if row['code'] in already_seen:
                    continue
                result = score_gap_up(row, market=market, trigger='intraday_breakout')
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

        # Persist turnover for delta calc on next scan
        save_scan_snapshot(market, snapshot)

        alerts.sort(key=lambda x: x[1]['score'], reverse=True)
        watches.sort(key=lambda x: x[1]['score'], reverse=True)
        return alerts, watches

    def scan_intraday_movers(self, market='HK', min_change_pct=5.0):
        """
        Scan for intraday movers — stocks up significantly today.
        Does NOT factor in overnight gap; scores purely on intraday activity.

        Returns:
            alerts  — list of (row, result) with alert_tier == 'alert'
            watches — list of (row, result) with alert_tier == 'watch'
        """
        snapshot = self._fetch_full_snapshot(market)
        if snapshot.empty:
            return [], []

        # Stocks with meaningful daily gains and real trading activity
        candidates = snapshot[
            (snapshot['last_price'] > 0) &
            (snapshot['prev_close_price'] > 0) &
            (snapshot['open_price'] > 0) &
            (snapshot['turnover'] > 0)
        ].copy()

        candidates['daily_change'] = (
            (candidates['last_price'] - candidates['prev_close_price'])
            / candidates['prev_close_price'] * 100
        )
        candidates = candidates[candidates['daily_change'] >= min_change_pct]

        alerts, watches = [], []
        for _, row in candidates.iterrows():
            result = score_intraday(row, market=market)
            result['trigger'] = 'intraday'
            if result['alert_tier'] == 'alert':
                alerts.append((row, result))
            elif result['alert_tier'] == 'watch':
                watches.append((row, result))

        # Persist turnover for delta calc on next scan
        save_scan_snapshot(market, snapshot)

        alerts.sort(key=lambda x: x[1]['score'], reverse=True)
        watches.sort(key=lambda x: x[1]['score'], reverse=True)
        return alerts, watches

    def scan_and_alert_intraday(self, market='HK', dry_run=False):
        """
        Intraday pipeline: scan movers → score → send Telegram alerts.
        Also re-checks tracked stocks for continued gains.

        Returns a list of message strings.
        """
        alerts, watches = self.scan_intraday_movers(market)
        sent_messages = []

        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"{market} intraday: {len(alerts)} alerts, {len(watches)} watches")

        for row, result in alerts:
            code = result['code']
            if is_on_cooldown(code):
                continue

            msg = build_intraday_message(row, result)

            if not dry_run and self.bot_token and self.chat_id:
                ok = send_telegram(self.bot_token, self.chat_id, msg)
                if ok:
                    mark_alerted(code, result['score'],
                                 str(row.get('name', '')))
            else:
                mark_alerted(code, result['score'],
                             str(row.get('name', '')))

            sent_messages.append(msg)

            self.add_to_tracking(
                code,
                str(row.get('name', '')),
                float(row.get('last_price', 0) or 0),
                float(row.get('change_5min', 0) or 0),
                market,
            )

        for row, result in watches:
            code = result['code']
            tracked = self.load_tracked_stocks()
            if code not in tracked:
                self.add_to_tracking(
                    code,
                    str(row.get('name', '')),
                    float(row.get('last_price', 0) or 0),
                    float(row.get('change_5min', 0) or 0),
                    market,
                )

        followup_msgs = self._check_tracked_for_followup(market, dry_run)
        sent_messages.extend(followup_msgs)

        return sent_messages

    def scan_and_alert(self, market='HK', dry_run=False):
        """
        Full pipeline: scan → score → send Telegram alerts.
        Also re-checks tracked stocks for continued gains.

        Returns a list of message strings that were (or would be) sent,
        so a Telegram bot or other caller can use them directly.
        """
        alerts, watches = self.scan_gap_ups(market)
        sent_messages = []

        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"{market}: {len(alerts)} alerts, {len(watches)} watches")

        for row, result in alerts:
            code = result['code']
            if is_on_cooldown(code):
                continue

            msg = build_telegram_message(row, result)

            if not dry_run and self.bot_token and self.chat_id:
                ok = send_telegram(self.bot_token, self.chat_id, msg)
                if ok:
                    mark_alerted(code, result['score'],
                                 str(row.get('name', '')))
            else:
                mark_alerted(code, result['score'],
                             str(row.get('name', '')))

            sent_messages.append(msg)

            # Auto-track alerted stocks
            self.add_to_tracking(
                code,
                str(row.get('name', '')),
                float(row.get('last_price', 0) or 0),
                float(row.get('change_5min', 0) or 0),
                market,
            )

        # Also track watch-tier stocks so we can follow up on them
        for row, result in watches:
            code = result['code']
            tracked = self.load_tracked_stocks()
            if code not in tracked:
                self.add_to_tracking(
                    code,
                    str(row.get('name', '')),
                    float(row.get('last_price', 0) or 0),
                    float(row.get('change_5min', 0) or 0),
                    market,
                )

        # ── Re-check tracked stocks for continued gains ──────────────────
        followup_msgs = self._check_tracked_for_followup(market, dry_run)
        sent_messages.extend(followup_msgs)

        return sent_messages

    def _check_tracked_for_followup(self, market='HK', dry_run=False):
        """
        Re-scan all tracked stocks. If a previously flagged stock has gained
        significantly since it was first detected, send a follow-up alert.

        Thresholds for follow-up:
          - 5%+ total gain since first detection  → follow-up alert
          - Must not be on cooldown (60 min between repeats)
        """
        tracked = self.load_tracked_stocks()
        market_stocks = {c: d for c, d in tracked.items()
                         if d.get('market') == market}
        if not market_stocks:
            return []

        codes = list(market_stocks.keys())
        try:
            all_snaps = []
            for i in range(0, len(codes), 400):
                batch = codes[i:i + 400]
                ret, snap = self.quote_ctx.get_market_snapshot(batch)
                if ret == RET_OK and snap is not None and not snap.empty:
                    all_snaps.append(snap)
            if not all_snaps:
                return []
            snapshot = pd.concat(all_snaps, ignore_index=True)
        except Exception as e:
            print(f"[followup] snapshot error: {e}")
            return []

        msgs = []
        updated_tracked = dict(tracked)  # copy for updates

        for _, row in snapshot.iterrows():
            code = row['code']
            if code not in market_stocks:
                continue
            td = market_stocks[code]
            if is_on_cooldown(code):
                continue

            cur_price = float(row.get('last_price', 0) or 0)
            ini_price = td['initial_price']
            if ini_price <= 0:
                continue

            total_gain = (cur_price - ini_price) / ini_price * 100
            prev_close = float(row.get('prev_close_price', 0) or 0)
            daily_chg = ((cur_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

            # Follow-up if stock gained 5%+ since first spotted AND is still
            # rising today (daily change > 0 — don't alert on stale gains)
            if total_gain >= 5.0 and daily_chg > 0:
                name = td['name'][:20]
                days = (datetime.now() - datetime.fromisoformat(td['first_seen'])).days
                turnover = float(row.get('turnover', 0) or 0)

                msg = (
                    f"📡 *{code}* — {name}  (Follow-up)\n"
                    f"\n"
                    f"💰 Price: HK${cur_price:.3f}\n"
                    f"📈 Today: {daily_chg:+.1f}%\n"
                    f"🚀 Since flagged: {total_gain:+.1f}% "
                    f"(${ini_price:.3f} → ${cur_price:.3f})\n"
                    f"📅 Tracked: {days}d ago\n"
                    f"💵 Turnover: {_format_turnover(turnover)}"
                )

                if not dry_run and self.bot_token and self.chat_id:
                    ok = send_telegram(self.bot_token, self.chat_id, msg)
                    if ok:
                        mark_alerted(code, 0, name)
                else:
                    mark_alerted(code, 0, name)

                msgs.append(msg)

                # Update the tracking baseline so we don't keep re-alerting
                # at the same level — next follow-up needs another 5% from here
                updated_tracked[code] = dict(td)
                updated_tracked[code]['initial_price'] = cur_price

        # Persist updated baselines
        if updated_tracked != tracked:
            self.save_tracked_stocks(updated_tracked)

        if msgs:
            print(f"[followup] {len(msgs)} follow-up alerts sent")

        return msgs

    def display_gap_scan(self, market='HK'):
        """Interactive display of gap-up scan results."""
        print(f"\nScanning {market} for gap-ups...")
        alerts, watches = self.scan_gap_ups(market)

        if alerts:
            print(f"\n{'='*90}")
            print(f"🔥 GAP-UP ALERTS — {market} MARKET")
            print(f"{'='*90}")
            for row, result in alerts:
                sig = result['signals']
                code = result['code']
                name = str(row.get('name', ''))[:20]
                price = float(row.get('last_price', 0) or 0)
                print(
                    f"  {code:<12} {name:<20} "
                    f"${price:<8.3f}  score={result['score']}  "
                    f"gap={sig['gap_pct']:+.1f}%  "
                    f"cont={sig['continuation_pct']:+.1f}%  "
                    f"flow={_format_turnover(sig['turnover_delta_hkd'])}"
                )
        else:
            print("\n  No gap-up alerts.")

        if watches:
            print(f"\n📊 WATCH LIST (top 10):")
            for row, result in watches[:10]:
                sig = result['signals']
                print(
                    f"    {result['code']:<12} score={result['score']}  "
                    f"gap={sig['gap_pct']:+.1f}%  "
                    f"cont={sig['continuation_pct']:+.1f}%  "
                    f"flow={_format_turnover(sig['turnover_delta_hkd'])}"
                )
        print()

    def get_eod_recap(self, market='HK', top_n=10):
        """
        End-of-day recap: top gainers, top losers, and most active by turnover.
        Returns a list of message strings suitable for Telegram.
        """
        snapshot = self._fetch_full_snapshot(market)
        if snapshot.empty:
            return []

        # Filter out stocks with no trading activity
        active = snapshot[
            (snapshot['last_price'] > 0) &
            (snapshot['prev_close_price'] > 0) &
            (snapshot['turnover'] > 0)
        ].copy()

        if active.empty:
            return []

        # ── Top Gainers ──
        gainers = active.nlargest(top_n, 'change_rate')
        gainer_lines = [f"🏁 *End of Market Recap — {market}*\n",
                        "📈 *Top {n} Gainers*".format(n=min(top_n, len(gainers)))]
        for i, (_, row) in enumerate(gainers.iterrows(), 1):
            name = str(row.get('name', ''))[:16]
            price = float(row['last_price'])
            chg = float(row['change_rate'])
            tvr = float(row.get('turnover', 0) or 0)
            gainer_lines.append(
                f"{i}. *{row['code']}* {name}  "
                f"${price:.3f}  {chg:+.1f}%  "
                f"T/O: {_format_turnover(tvr)}"
            )

        # ── Top Losers ──
        losers = active.nsmallest(top_n, 'change_rate')
        loser_lines = ["\n📉 *Top {n} Losers*".format(n=min(top_n, len(losers)))]
        for i, (_, row) in enumerate(losers.iterrows(), 1):
            name = str(row.get('name', ''))[:16]
            price = float(row['last_price'])
            chg = float(row['change_rate'])
            tvr = float(row.get('turnover', 0) or 0)
            loser_lines.append(
                f"{i}. *{row['code']}* {name}  "
                f"${price:.3f}  {chg:+.1f}%  "
                f"T/O: {_format_turnover(tvr)}"
            )

        # ── Most Active by Turnover ──
        most_active = active.nlargest(top_n, 'turnover')
        active_lines = ["\n🔥 *Most Active by Turnover*"]
        for i, (_, row) in enumerate(most_active.iterrows(), 1):
            name = str(row.get('name', ''))[:16]
            price = float(row['last_price'])
            chg = float(row['change_rate'])
            tvr = float(row.get('turnover', 0) or 0)
            active_lines.append(
                f"{i}. *{row['code']}* {name}  "
                f"${price:.3f}  {chg:+.1f}%  "
                f"T/O: {_format_turnover(tvr)}"
            )

        # Combine into one or two messages (depending on length)
        all_lines = gainer_lines + loser_lines + active_lines
        return ["\n".join(all_lines)]

    def get_watchlist_summary(self, market='HK'):
        """
        Return a formatted string summarising watchlist movers.
        Useful for sending periodic updates via Telegram.
        """
        tracked = self.load_tracked_stocks()
        market_stocks = {c: d for c, d in tracked.items()
                         if d.get('market') == market}
        if not market_stocks:
            return ''

        stock_codes = list(market_stocks.keys())
        ret, snapshot = self.quote_ctx.get_market_snapshot(stock_codes)
        if ret != RET_OK or snapshot is None or snapshot.empty:
            return ''

        lines = [f"📋 *Watchlist — {market}*  ({len(market_stocks)} stocks)"]
        for _, row in snapshot.iterrows():
            code = row['code']
            if code not in market_stocks:
                continue
            td = market_stocks[code]
            cur = float(row['last_price'])
            ini = td['initial_price']
            chg = ((cur - ini) / ini * 100) if ini > 0 else 0
            pin = '📌 ' if td.get('permanent') else ''
            lines.append(
                f"{pin}*{code}* {td['name'][:18]}  "
                f"${cur:.3f}  {chg:+.1f}%"
            )

        return '\n'.join(lines) if len(lines) > 1 else ''

    def run_scheduled_scan(self, market='HK', interval_s=300, dry_run=False):
        """
        Blocking loop — scans every interval_s during HK market hours.
        Sends Telegram alerts if bot_token/chat_id are configured.
        """
        import time
        print(f"[Scheduler] {market} market, every {interval_s}s")
        print(f"[Scheduler] Telegram: {'ON' if self.bot_token else 'OFF (dry run)'}")

        eod_saved_date = None

        while True:
            now_hkt = datetime.utcnow() + timedelta(hours=8)
            hm = now_hkt.hour * 100 + now_hkt.minute
            in_session = (930 <= hm < 1200) or (1300 <= hm < 1600)

            if in_session:
                try:
                    self.scan_and_alert(market=market, dry_run=dry_run)
                except Exception as e:
                    print(f"[Scheduler] Error: {e}")
            else:
                # Save yesterday's turnover once after market close
                today_date = now_hkt.date()
                if hm >= 1600 and eod_saved_date != today_date:
                    try:
                        snapshot = self._fetch_full_snapshot(market)
                        if not snapshot.empty:
                            save_prev_day_turnover(market, snapshot)
                            eod_saved_date = today_date
                    except Exception as e:
                        print(f"[EOD] Failed to save daily turnover: {e}")

                print(f"[{now_hkt.strftime('%H:%M')} HKT] Market closed")

            time.sleep(interval_s)

    def close(self):
        """Close the quote context connection"""
        self.quote_ctx.close()


def main():
    """Main function to run the top gainers analyzer"""
    print("Futu API Top Gainers Analyzer")
    print("=" * 80)
    
    # Initialize analyzer
    analyzer = TopGainersAnalyzer()
    
    # Test connection first
    print("\nTesting connection to FutuOpenD...")
    if not analyzer.test_connection():
        print("\n" + "=" * 80)
        print("Setup Instructions:")
        print("=" * 80)
        print("1. Download FutuOpenD from: https://www.futunn.com/download/OpenAPI")
        print("2. Install and launch FutuOpenD")
        print("3. Log in with your Futu account")
        print("4. Make sure FutuOpenD shows 'Connected' status")
        print("5. Run this script again")
        print("=" * 80)
        return
    
    # Show quota information
    analyzer.check_quota()
    
    try:
        # Display menu
        while True:
            tracked = analyzer.load_tracked_stocks()
            tracked_count = len(tracked)
            
            print("\n" + "="*80)
            print("MAIN MENU")
            print("="*80)
            print("\n📊 Market Scans:")
            print("  1. US Market - Daily Gainers & 5min Movers")
            print("  2. HK Market - Daily Gainers & 5min Movers")
            print("  3. CN Market - Daily Gainers & 5min Movers")
            print("\n🔥 Gap-Up Scanner:")
            print("  4. HK Gap-Up Scan (score + alerts)")
            print("  5. HK Gap-Up Scan → Send Telegram alerts")
            print("  6. Start scheduled scanner (every 5 min)")
            print(f"\n🔍 Watchlist ({tracked_count} stocks tracked):")
            print("  7. Manage US Watchlist")
            print("  8. Manage HK Watchlist")
            print("  9. Manage CN Watchlist")
            print("\n⚙️  System:")
            print("  10. Check API Quota")
            print("  0. Exit")
            
            choice = input("\nEnter your choice: ").strip()
            
            if choice == '1':
                analyzer.display_both(market='US', count=10)
            elif choice == '2':
                analyzer.display_both(market='HK', count=10)
            elif choice == '3':
                analyzer.display_both(market='CN', count=10)
            elif choice == '4':
                analyzer.display_gap_scan(market='HK')
            elif choice == '5':
                msgs = analyzer.scan_and_alert(market='HK')
                if msgs:
                    print(f"\n✅ {len(msgs)} alert(s) sent")
                else:
                    print("\n💤 No alerts to send")
            elif choice == '6':
                analyzer.run_scheduled_scan(market='HK', interval_s=300)
            elif choice == '7':
                analyzer.manage_watchlist(market='US')
            elif choice == '8':
                analyzer.manage_watchlist(market='HK')
            elif choice == '9':
                analyzer.manage_watchlist(market='CN')
            elif choice == '10':
                analyzer.check_quota()
            elif choice == '0':
                print("\nExiting... Goodbye!")
                break
            else:
                print("\n❌ Invalid choice. Please try again.")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
    
    finally:
        # Close connection
        analyzer.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
