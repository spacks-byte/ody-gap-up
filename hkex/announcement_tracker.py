#!/usr/bin/env python3
"""
hkex/announcement_tracker.py — Monitors HKEX for material announcements.

Fetches latest filings from the HKEX Listed Company Information JSON feed
and flags announcements matching key corporate action categories:
  - Trading halt / suspension
  - Trading resumption
  - Rights issue
  - Share placement
  - Privatisation
  - Takeovers
  - M&A / major transactions
"""

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

from hkex.llm_classifier import classify_batch

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SEEN_FILE = DATA_DIR / "seen_announcements.json"

# ── HKEX JSON Feed ──────────────────────────────────────────────────────────

HKEX_BASE = "https://www1.hkexnews.hk"
# Latest Main Board announcements sorted by release time descending (English)
# Pattern: /ncms/json/eds/{tab}{board}{range}{sort}{dir}{lang}_{page}.json
LCI_URL_TEMPLATE = HKEX_BASE + "/ncms/json/eds/lcisehk1relsde_{page}.json"
LCI_URL_7DAY = HKEX_BASE + "/ncms/json/eds/lcisehk7relsde_{page}.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

# ── Classification Rules ────────────────────────────────────────────────────
# Ordered list of category names for display
CATEGORIES = [
    "Trading Halt",
    "Trading Resumption",
    "Rights Issue",
    "Share Placement",
    "Privatisation",
    "Takeover",
    "M&A",
]

# Each rule: (category_label, emoji, patterns_on_title, patterns_on_lTxt,
#             exclude_patterns)
# A match is triggered if ANY title pattern OR ANY lTxt pattern matches,
# provided NONE of the exclude patterns match.

RULES = [
    (
        "Trading Halt",
        "🔴",
        [r"trading halt", r"suspension of trading", r"halt in trading",
         r"suspended? trading", r"continued suspension"],
        [r"Trading Halt", r"Suspension"],
        # Exclude: scheme documents, composite documents, circulars
        [r"^Circulars", r"^Documents on Display"],
    ),
    (
        "Trading Resumption",
        "🟢",
        [r"resumption of trading", r"trading resumption",
         r"\bresumption\b.*trading"],
        [r"Resumption"],
        [r"^Circulars", r"^Documents on Display",
         r"additional resumption guidance"],
    ),
    (
        "Rights Issue",
        "💰",
        [r"rights issue", r"proposed rights issue"],
        [r"Rights Issue"],
        [r"^Circulars", r"^Documents on Display", r"^Proxy Forms"],
    ),
    (
        "Share Placement",
        "📊",
        [r"\bplacing\b", r"\bplacement\b", r"subscription of new shares"],
        [r"Placing"],
        [r"^Circulars", r"^Documents on Display", r"^Proxy Forms",
         r"Disclosure of Dealings"],
    ),
    (
        "Privatisation",
        "🏛️",
        [r"privati[sz]ation", r"withdrawal.*listing",
         r"cancellation of listing"],
        [r"Privatisation", r"Withdrawal or Cancellation of Listing"],
        [r"^Circulars", r"^Documents on Display", r"^Proxy Forms",
         r"Composite Document", r"Scheme Document"],
    ),
    (
        "Takeover",
        "⚠️",
        [r"voluntary.*offer", r"mandatory.*offer",
         r"general offer", r"takeover", r"take-over"],
        [r"Announcement by Offeror.*Takeovers",
         r"Announcement by Offeree.*Takeovers"],
        [r"Disclosure of Dealings", r"^Circulars",
         r"^Documents on Display", r"Composite Document",
         r"Scheme Document"],
    ),
    (
        "M&A",
        "🔵",
        [r"\bacquisition\b", r"\bmerger\b", r"major transaction",
         r"very substantial"],
        [r"Major Transaction", r"Very Substantial"],
        [r"^Circulars", r"^Documents on Display", r"^Proxy Forms",
         r"Disclosure of Dealings", r"Composite Document"],
    ),
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _hk_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=8)


def _load_seen() -> dict:
    """Return {newsId_str: timestamp_str} of previously alerted items."""
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_seen(seen: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Keep last 7 days only to avoid unbounded growth
    cutoff = (_hk_now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M")
    pruned = {k: v for k, v in seen.items() if v >= cutoff}
    SEEN_FILE.write_text(json.dumps(pruned, indent=2))


def _matches_any(text: str, patterns: list[str]) -> bool:
    for p in patterns:
        if re.search(p, text, re.IGNORECASE):
            return True
    return False


def classify(item: dict) -> Optional[tuple[str, str]]:
    """Classify an announcement item. Returns (label, emoji) or None."""
    title = item.get("title", "")
    ltxt = item.get("lTxt", "")

    for label, emoji, title_pats, ltxt_pats, exclude_pats in RULES:
        # Check exclusions first (on both title and lTxt)
        if _matches_any(title, exclude_pats) or _matches_any(ltxt, exclude_pats):
            continue
        # Check for match
        if _matches_any(title, title_pats) or _matches_any(ltxt, ltxt_pats):
            return label, emoji
    return None


# ── Core fetcher ─────────────────────────────────────────────────────────────

def fetch_announcements(max_pages: int = 2, use_7day: bool = False) -> list[dict]:
    """Fetch latest HKEX announcements (Main Board, English)."""
    all_items = []
    session = requests.Session()
    session.headers.update(HEADERS)
    template = LCI_URL_7DAY if use_7day else LCI_URL_TEMPLATE

    for page in range(1, max_pages + 1):
        url = template.format(page=page)
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("newsInfoLst", [])
            all_items.extend(items)
            logger.info("HKEX page %d: %d items fetched", page, len(items))
            if page >= data.get("maxNumOfFile", 1):
                break
        except Exception as e:
            logger.warning("HKEX fetch page %d failed: %s", page, e)
            break

    return all_items


def scan_announcements(
    since_hours: float = 18.0,
) -> list[dict]:
    """
    Fetch & classify announcements from the last `since_hours` hours.

    Returns list of dicts:
        {category, emoji, stock_code, stock_name, title, time, link, news_id}
    """
    items = fetch_announcements()
    if not items:
        return []

    now = _hk_now()
    cutoff = now - timedelta(hours=since_hours)
    results = []

    for item in items:
        news_id = str(item.get("newsId", ""))

        # Parse release time  "19/03/2026 18:57"
        rel_str = item.get("relTime", "")
        try:
            rel_dt = datetime.strptime(rel_str, "%d/%m/%Y %H:%M")
            rel_dt = rel_dt.replace(tzinfo=timezone(timedelta(hours=8)))
        except ValueError:
            continue

        if rel_dt < cutoff:
            continue

        result = classify(item)
        if result is None:
            continue

        label, emoji = result
        stocks = item.get("stock", [])
        stock_code = stocks[0]["sc"] if stocks else "N/A"
        stock_name = stocks[0]["sn"] if stocks else "N/A"
        web_path = item.get("webPath", "")
        link = f"{HKEX_BASE}{web_path}" if web_path else ""

        results.append({
            "category": label,
            "emoji": emoji,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "title": item.get("title", "").strip(),
            "time": rel_str,
            "link": link,
            "news_id": news_id,
        })

    logger.info("Announcement scan: %d keyword-matched items", len(results))

    # ── LLM enrichment: classify originality + extract key info ──
    enriched = classify_batch(results)
    original = [r for r in enriched if r.get("is_original", True)]
    follow_up = len(enriched) - len(original)
    if follow_up:
        logger.info("LLM filtered out %d follow-up announcements", follow_up)
    return enriched


def scan_by_date(target_date: str) -> list[dict]:
    """
    Fetch & classify announcements for a specific date.

    Args:
        target_date: Date string in DD/MM/YYYY, YYYY-MM-DD, or YYYYMMDD format.

    Returns same structure as scan_announcements but:
      - Uses the 7-day feed to reach older dates
      - Filters to the exact target date
      - Skips dedup (returns all matches, not just unseen)
    """
    # Normalise date input
    dt = None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            dt = datetime.strptime(target_date.strip(), fmt)
            break
        except ValueError:
            continue
    if dt is None:
        raise ValueError(
            f"Unrecognised date format: '{target_date}'. "
            "Use DD/MM/YYYY, YYYY-MM-DD, or YYYYMMDD."
        )

    target_str = dt.strftime("%d/%m/%Y")  # match relTime format

    items = fetch_announcements(max_pages=4, use_7day=True)
    if not items:
        return []

    results = []
    for item in items:
        rel_str = item.get("relTime", "")
        # relTime is "DD/MM/YYYY HH:MM" — compare date portion
        if not rel_str.startswith(target_str):
            continue

        result = classify(item)
        if result is None:
            continue

        label, emoji = result
        stocks = item.get("stock", [])
        stock_code = stocks[0]["sc"] if stocks else "N/A"
        stock_name = stocks[0]["sn"] if stocks else "N/A"
        web_path = item.get("webPath", "")
        link = f"{HKEX_BASE}{web_path}" if web_path else ""

        results.append({
            "category": label,
            "emoji": emoji,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "title": item.get("title", "").strip(),
            "time": rel_str,
            "link": link,
            "news_id": str(item.get("newsId", "")),
        })

    logger.info("Announcement scan for %s: %d keyword-matched items", target_str, len(results))

    # ── LLM enrichment ──
    enriched = classify_batch(results)
    return enriched


def format_alerts(results: list[dict], date_label: str = "") -> str:
    """Format flagged announcements for Telegram with LLM enrichment."""
    if not results:
        return ""

    # Split into original vs follow-up
    original = [r for r in results if r.get("is_original", True)]
    follow_ups = [r for r in results if not r.get("is_original", True)]

    if not original and not follow_ups:
        return ""

    header = f"📢 *HKEX Announcements — {date_label}*" if date_label else "📢 *HKEX Announcement Alert*"
    lines = [header, ""]

    # ── Original / material announcements ──
    if original:
        lines.append("🔥 *Material / Original Announcements:*")
        lines.append("")

        by_cat: dict[str, list[dict]] = {}
        for r in original:
            by_cat.setdefault(r["category"], []).append(r)

        for cat in CATEGORIES:
            items = by_cat.get(cat, [])
            if not items:
                continue

            emoji = items[0]["emoji"]
            lines.append(f"{emoji} *{cat}*")
            for item in items:
                code = item["stock_code"]
                name = item["stock_name"]
                title_short = item["title"].replace("\n", " ")
                if len(title_short) > 80:
                    title_short = title_short[:77] + "..."
                lines.append(f"  `{code}` {name}")
                lines.append(f"  _{title_short}_")
                key_info = item.get("key_info")
                if key_info:
                    lines.append(f"  💡 {key_info}")
                lines.append(f"  [{item['time']}]({item['link']})")
                lines.append("")

    # ── Follow-up / procedural (collapsed summary) ──
    if follow_ups:
        lines.append(f"📋 *Follow-up / Procedural ({len(follow_ups)} items):*")
        for item in follow_ups:
            code = item["stock_code"]
            reason = item.get("reason", "")
            title_short = item["title"].replace("\n", " ")
            if len(title_short) > 60:
                title_short = title_short[:57] + "..."
            lines.append(f"  `{code}` _{title_short}_")
        lines.append("")

    return "\n".join(lines)
