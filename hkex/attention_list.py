#!/usr/bin/env python3
"""
hkex/attention_list.py — Manages the "Extra Attention" watchlist.

Stocks flagged by the HKEX announcement tracker (privatisation, takeover,
rights issue, etc.) are added here and monitored for 3 months.

Data stored in data/attention_stocks.json:
{
    "HK.06693": {
        "stock_code": "06693",
        "stock_name": "CHIFENG GOLD",
        "category": "Trading Halt",
        "title": "Trading Halt",
        "added": "2026-03-19T19:00:00",
        "expires": "2026-06-19T19:00:00",
        "link": "https://..."
    }
}
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ATTENTION_FILE = DATA_DIR / "attention_stocks.json"

RETENTION_DAYS = 90  # 3 months


def _load() -> dict:
    if ATTENTION_FILE.exists():
        try:
            return json.loads(ATTENTION_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save(data: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ATTENTION_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def add_stock(
    stock_code: str,
    stock_name: str,
    category: str,
    title: str,
    link: str = "",
    key_info: str = "",
):
    """Add a stock to the extra attention list (or update if already present)."""
    data = _load()
    now = datetime.now()

    # Use HK.XXXXX format as key for consistency with Futu
    futu_code = f"HK.{stock_code}" if not stock_code.startswith("HK.") else stock_code
    raw_code = stock_code.replace("HK.", "")

    existing = data.get(futu_code)

    data[futu_code] = {
        "stock_code": raw_code,
        "stock_name": stock_name,
        "category": category,
        "title": title[:200],
        "link": link,
        "key_info": key_info or "",
        "added": now.isoformat(),
        "expires": (now + timedelta(days=RETENTION_DAYS)).isoformat(),
    }

    # If already present with a different announcement, keep both categories
    if existing and existing.get("category") != category:
        prev = existing.get("all_categories", [existing.get("category")])
        if category not in prev:
            prev.append(category)
        data[futu_code]["all_categories"] = prev

    _save(data)
    logger.info("Attention list: added %s %s (%s)", futu_code, stock_name, category)


def remove_stock(stock_code: str) -> bool:
    """Remove a stock from the attention list."""
    data = _load()
    futu_code = f"HK.{stock_code}" if not stock_code.startswith("HK.") else stock_code
    if futu_code in data:
        del data[futu_code]
        _save(data)
        return True
    return False


def get_all() -> dict:
    """Return all active (non-expired) attention stocks."""
    data = _load()
    now = datetime.now()
    active = {}
    expired = []

    for code, info in data.items():
        expires = datetime.fromisoformat(info["expires"])
        if now < expires:
            active[code] = info
        else:
            expired.append(code)

    # Prune expired entries
    if expired:
        for code in expired:
            del data[code]
        _save(data)
        logger.info("Attention list: pruned %d expired entries", len(expired))

    return active


def get_codes() -> list[str]:
    """Return list of Futu-format codes (HK.XXXXX) for all active stocks."""
    return list(get_all().keys())


def get_annotation(stock_code: str) -> str | None:
    """
    Get the announcement annotation for a stock.
    Returns a short string like "⚠️ Takeover" or None if not in list.
    """
    data = get_all()
    futu_code = f"HK.{stock_code}" if not stock_code.startswith("HK.") else stock_code
    info = data.get(futu_code)
    if not info:
        return None

    cat = info.get("category", "")
    emojis = {
        "Trading Halt": "🔴",
        "Trading Resumption": "🟢",
        "Rights Issue": "💰",
        "Share Placement": "📊",
        "Privatisation": "🏛️",
        "Takeover": "⚠️",
        "M&A": "🔵",
    }
    emoji = emojis.get(cat, "📢")
    key_info = info.get("key_info", "")
    note = f"{emoji} {cat}"
    if key_info:
        note += f" — {key_info[:80]}"
    return note


def add_from_scan_results(results: list[dict]) -> list[str]:
    """
    Add all original/material announcements from a scan to the attention list.
    Returns list of stock codes that were added.
    """
    added = []
    for item in results:
        if not item.get("is_original", True):
            continue
        code = item.get("stock_code", "")
        if not code or code == "N/A":
            continue
        add_stock(
            stock_code=code,
            stock_name=item.get("stock_name", ""),
            category=item.get("category", ""),
            title=item.get("title", ""),
            link=item.get("link", ""),
            key_info=item.get("key_info", ""),
        )
        added.append(code)
    return added
