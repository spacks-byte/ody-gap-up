#!/usr/bin/env python3
"""
hkex/llm_classifier.py — LLM-powered announcement classification + PDF extraction.

Uses Perplexity Agent API (OpenAI-compatible) to:
  1. Determine if an HKEX announcement is an ORIGINAL material event
     vs a procedural follow-up.
  2. Extract key deal information from PDF filings.

Env var required:
    PERPLEXITY_API_KEY — your Perplexity API key
"""

import io
import logging
import os
import re
import json
import tempfile
from typing import Optional

import requests
import pdfplumber
from openai import OpenAI

logger = logging.getLogger(__name__)

# ── Perplexity client ────────────────────────────────────────────────────────

HKEX_BASE = "https://www1.hkexnews.hk"

CATEGORIES = [
    "Trading Halt",
    "Trading Resumption",
    "Rights Issue",
    "Share Placement",
    "Privatisation",
    "Takeover",
    "M&A",
]

SYSTEM_PROMPT = """\
You are an expert analyst of Hong Kong stock exchange (HKEX) corporate filings.

Your job: given an announcement title (and optionally text from the PDF filing), \
classify it into ONE of these categories OR reject it:

Categories: Trading Halt, Trading Resumption, Rights Issue, Share Placement, \
Privatisation, Takeover, M&A

CRITICAL DISTINCTION — you must determine whether the announcement is:
- "original": The FIRST announcement of a material corporate action \
(e.g. the company is PROPOSING a rights issue, ANNOUNCING a trading halt, \
LAUNCHING a placement, MAKING an offer). These are tradeable, market-moving events.
- "follow_up": A PROCEDURAL or ADMINISTRATIVE update on a previously announced \
action (e.g. results of acceptance, timetable updates, despatch of documents, \
supplemental notices, monthly returns, forms of proxy, listing approvals, \
dealings disclosures, composite documents, scheme documents). \
These are NOT the original announcement.

Follow-up examples (should be rejected):
- "Results of valid acceptances of the rights shares..."
- "Despatch of circular in relation to the rights issue..."
- "Supplemental notice of the extraordinary general meeting..."
- "Monthly return of equity issuer on movements in securities..."
- "Disclosure of dealings under Rule 22 of the Takeovers Code..."
- "Composite document relating to the mandatory conditional cash offer..."
- "Expected timetable for the proposed rights issue..."
- "Form of proxy for the extraordinary general meeting..."

Original examples (should be flagged):
- "Proposed rights issue on the basis of 1 for every 2 shares"
- "Trading halt pending release of inside information"
- "Voluntary conditional general offer by X for all shares"
- "Placing of new shares under general mandate"
- "Privatisation by way of scheme of arrangement"
- "Very substantial acquisition relating to..."
- "Suspension of trading"

Respond with ONLY valid JSON (no markdown, no code fences):
{
  "category": "<one of the categories or null>",
  "is_original": true/false,
  "confidence": 0.0-1.0,
  "key_info": "<one-line summary of the deal details if available, else null>",
  "reason": "<brief explanation of your classification>"
}

If the announcement does not fit any category, set category to null.
If it's a follow-up/procedural document, set is_original to false.
"""


def _get_client() -> Optional[OpenAI]:
    """Create Perplexity API client."""
    api_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None
    return OpenAI(
        api_key=api_key,
        base_url="https://api.perplexity.ai",
    )


def extract_pdf_text(url: str, max_chars: int = 3000) -> Optional[str]:
    """
    Download and extract text from a HKEX filing PDF.
    Returns the first `max_chars` characters of text, or None on failure.
    """
    if not url:
        return None

    try:
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
            ),
        })
        resp.raise_for_status()

        # Check it's actually a PDF
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not resp.content[:5] == b"%PDF-":
            logger.debug("Not a PDF: %s (%s)", url, content_type)
            return None

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            text_parts = []
            total = 0
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text_parts.append(page_text)
                total += len(page_text)
                if total >= max_chars:
                    break

        full_text = "\n".join(text_parts)
        # Clean up whitespace
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        return full_text[:max_chars] if full_text.strip() else None

    except Exception as e:
        logger.debug("PDF extraction failed for %s: %s", url, e)
        return None


def classify_with_llm(
    title: str,
    stock_code: str = "",
    stock_name: str = "",
    pdf_text: Optional[str] = None,
) -> Optional[dict]:
    """
    Classify an announcement using Perplexity Agent API.

    Returns dict with keys:
        category, is_original, confidence, key_info, reason
    Or None if the API is unavailable or the call fails.
    """
    client = _get_client()
    if client is None:
        return None

    user_msg = f"Stock: {stock_code} {stock_name}\nTitle: {title}"
    if pdf_text:
        user_msg += f"\n\nPDF excerpt (first ~3000 chars):\n{pdf_text}"

    try:
        response = client.chat.completions.create(
            model="sonar",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=300,
        )

        raw = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)

        result = json.loads(raw)

        # Validate expected fields
        if "category" not in result or "is_original" not in result:
            logger.warning("LLM response missing fields: %s", raw)
            return None

        return result

    except json.JSONDecodeError as e:
        logger.warning("LLM returned invalid JSON: %s — %s", raw, e)
        return None
    except Exception as e:
        logger.warning("LLM classification failed: %s", e)
        return None


def classify_batch(items: list[dict], fetch_pdf: bool = True) -> list[dict]:
    """
    Classify a batch of announcement items using LLM.

    Each item should have: title, stock_code, stock_name, link
    Returns list of items enriched with LLM classification fields:
        llm_category, is_original, confidence, key_info, reason

    Items classified as follow-ups (is_original=False) are still returned
    but marked so the caller can filter them.
    """
    results = []

    if not os.environ.get("PERPLEXITY_API_KEY", ""):
        logger.info("PERPLEXITY_API_KEY not set — skipping LLM, keyword match only")
        for item in items:
            enriched = dict(item)
            enriched["llm_category"] = item.get("category")
            enriched["is_original"] = True
            enriched["confidence"] = 0
            enriched["key_info"] = None
            enriched["reason"] = "LLM unavailable — keyword match only"
            results.append(enriched)
        return results

    for item in items:
        title = item.get("title", "")
        stock_code = item.get("stock_code", "")
        stock_name = item.get("stock_name", "")
        link = item.get("link", "")

        # Optionally extract PDF text for deeper context
        pdf_text = None
        if fetch_pdf and link:
            pdf_text = extract_pdf_text(link)

        llm_result = classify_with_llm(
            title=title,
            stock_code=stock_code,
            stock_name=stock_name,
            pdf_text=pdf_text,
        )

        enriched = dict(item)
        if llm_result:
            enriched["llm_category"] = llm_result.get("category")
            enriched["is_original"] = llm_result.get("is_original", False)
            enriched["confidence"] = llm_result.get("confidence", 0)
            enriched["key_info"] = llm_result.get("key_info")
            enriched["reason"] = llm_result.get("reason", "")
        else:
            # LLM unavailable — fall back to assuming original
            enriched["llm_category"] = item.get("category")
            enriched["is_original"] = True
            enriched["confidence"] = 0
            enriched["key_info"] = None
            enriched["reason"] = "LLM unavailable — keyword match only"

        results.append(enriched)

    return results


# ── Deep deal analysis for Privatisations / Takeovers ────────────────────────

DEAL_ANALYSIS_PROMPT = """\
You are an expert analyst of Hong Kong stock exchange (HKEX) corporate filings.

Given the announcement title, stock details, and the PDF filing text below, \
extract a detailed analysis of the deal. This is a privatisation or takeover \
announcement.

Provide the following information:

1. **Deal Size**: Total consideration (in HKD or the stated currency). \
If not stated, estimate from offer price × shares outstanding if available.

2. **Cash vs Equity split**: What percentage of the consideration is in cash \
vs equity/shares? Is it a pure cash offer, pure share swap, or mixed?

3. **Buyer Background**:
   - Who is the buyer/offeror? (Name, entity)
   - What industry is the buyer from?
   - Does the buyer control other listed or major companies? If so, which ones?
   - Is this likely an **asset injection** story (buyer injecting assets into \
the target to grow it) or a **Reverse Takeover (RTO)** story (buyer using the \
listed shell to list their own business)?
   - Any other notable background on the buyer.

4. **Seller Background**:
   - Who is the seller / target company controlling shareholder?
   - Why are they selling? (Retirement, restructuring, financial distress, etc.)
   - Any notable background.

If certain information is not available in the PDF, state "Not disclosed in filing".

Respond in clear, concise bullet points. Do NOT use JSON format — use readable \
text with markdown formatting suitable for Telegram (use * for bold).
"""


def analyze_deal(
    title: str,
    stock_code: str,
    stock_name: str,
    link: str,
) -> Optional[str]:
    """
    Deep analysis of a Privatisation/Takeover deal.
    Downloads the PDF, extracts more text, and asks LLM for structured analysis.

    Returns formatted text string for Telegram, or None on failure.
    """
    client = _get_client()
    if client is None:
        return "❌ PERPLEXITY_API_KEY not set — cannot perform deal analysis."

    # Extract more text from the PDF for deep analysis
    pdf_text = extract_pdf_text(link, max_chars=8000) if link else None
    if not pdf_text:
        pdf_text = "(PDF text not available)"

    user_msg = (
        f"Stock: {stock_code} {stock_name}\n"
        f"Title: {title}\n\n"
        f"PDF filing text:\n{pdf_text}"
    )

    try:
        response = client.chat.completions.create(
            model="sonar",
            messages=[
                {"role": "system", "content": DEAL_ANALYSIS_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=1000,
        )

        analysis = response.choices[0].message.content.strip()
        return (
            f"🔍 *Deal Analysis: {stock_code} {stock_name}*\n\n"
            f"{analysis}"
        )

    except Exception as e:
        logger.warning("Deal analysis failed: %s", e)
        return f"❌ Deal analysis failed: {e}"
