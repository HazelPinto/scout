import re
from typing import Dict, Any, Tuple, List

EVENT_TYPES = {"funding", "partnership", "product_launch", "expansion", "acquisition", "milestone", "other"}
ROUND_TYPES = {"pre_seed", "seed", "series_a", "series_b", "series_c", "series_d", "series_e", "series_f", "series_g",
               "growth", "venture_debt", "grant", "unknown"}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _quote_in_text(quote: str, text: str) -> bool:
    if not quote or not text:
        return False
    return quote in text


def validate_extraction_for_chunk(extraction: Dict[str, Any], chunk_text: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns (accepted, stats)
    accepted contains filtered people/events/funding_rounds.
    """
    accepted = {"extractor_version": extraction.get("extractor_version", "unknown"),
                "people": [], "events": [], "funding_rounds": []}

    stats = {
        "people_in": len(extraction.get("people", []) or []),
        "events_in": len(extraction.get("events", []) or []),
        "funding_in": len(extraction.get("funding_rounds", []) or []),
        "people_ok": 0, "events_ok": 0, "funding_ok": 0,
        "rejected": 0,
        "reject_reasons": []
    }

    # People
    for p in (extraction.get("people") or []):
        try:
            quote = (p.get("evidence_quote") or "").strip()
            if not _quote_in_text(quote, chunk_text):
                stats["rejected"] += 1
                stats["reject_reasons"].append("person_quote_not_in_text")
                continue

            # no invented linkedin
            li = p.get("linkedin_url")
            if li and li not in quote:
                stats["rejected"] += 1
                stats["reject_reasons"].append("person_linkedin_not_in_quote")
                continue

            accepted["people"].append(p)
            stats["people_ok"] += 1
        except Exception:
            stats["rejected"] += 1
            stats["reject_reasons"].append("person_exception")

    # Events
    for e in (extraction.get("events") or []):
        try:
            et = (e.get("type") or "").strip()
            quote = (e.get("evidence_quote") or "").strip()

            if et not in EVENT_TYPES:
                stats["rejected"] += 1
                stats["reject_reasons"].append("event_bad_type")
                continue

            if not _quote_in_text(quote, chunk_text):
                stats["rejected"] += 1
                stats["reject_reasons"].append("event_quote_not_in_text")
                continue

            accepted["events"].append(e)
            stats["events_ok"] += 1
        except Exception:
            stats["rejected"] += 1
            stats["reject_reasons"].append("event_exception")

    # Funding rounds
    for fr in (extraction.get("funding_rounds") or []):
        try:
            rt = (fr.get("round_type") or "").strip()
            quote = (fr.get("evidence_quote") or "").strip()

            if rt not in ROUND_TYPES:
                stats["rejected"] += 1
                stats["reject_reasons"].append("funding_bad_round_type")
                continue

            if not _quote_in_text(quote, chunk_text):
                stats["rejected"] += 1
                stats["reject_reasons"].append("funding_quote_not_in_text")
                continue

            # no invented amounts: if amount exists, it must appear in quote
            amt = fr.get("amount")
            if amt is not None and str(amt) not in quote:
                stats["rejected"] += 1
                stats["reject_reasons"].append("funding_amount_not_in_quote")
                continue

            accepted["funding_rounds"].append(fr)
            stats["funding_ok"] += 1
        except Exception:
            stats["rejected"] += 1
            stats["reject_reasons"].append("funding_exception")

    return accepted, stats