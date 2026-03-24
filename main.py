#!/usr/bin/env python3
"""
main.py — Entry point / orchestrator.

Usage:
    python main.py              # Launch Telegram bot (default)
    python main.py --cli        # Launch interactive CLI scanner
    python main.py --help       # Show options

Project structure:
    main.py                     ← you are here (orchestrator)
    ├── bot/telegram_bot.py     ← Telegram bot interface
    ├── scanner/market_scanner.py  ← Market scanning engine (TopGainersAnalyzer)
    ├── scoring/gap_scorer.py   ← Scoring algorithm & alert formatting
    ├── ccass/hkex_tracker.py   ← CCASS shareholding tracker
    ├── data/                   ← Runtime JSON data files
    └── docs/                   ← Documentation & reference
"""

import sys


def main():
    mode = "bot"
    if "--cli" in sys.argv:
        mode = "cli"
    elif "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__.strip())
        print()
        print("Options:")
        print("  (no args)   Start Telegram bot (reads .env for tokens)")
        print("  --cli       Start interactive CLI scanner")
        print("  --help      Show this message")
        return

    if mode == "cli":
        from scanner.market_scanner import main as cli_main
        cli_main()
    else:
        from bot.telegram_bot import main as bot_main
        bot_main()


if __name__ == "__main__":
    main()
