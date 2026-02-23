import re
from typing import Dict, List

LABELS = ["founders_team", "funding", "commercial_event", "irrelevant"]

# --- Keyword packs (cheap-first) ---
FOUNDERS_KW = [
    "founder", "co-founder", "cofounder",
    "leadership", "team", "management",
    "ceo", "cto", "cfo", "chief",
    "board", "executive", "head of",
]

FUNDING_KW = [
    "raised", "funding", "series a", "series b", "series c", "series d",
    "seed round", "seed", "pre-seed",
    "investment", "investor", "valuation", "venture", "vc", "capital",
    "financing", "round led by",
]

# Partnerships / deals / announcements
COMMERCIAL_KW = [
    "partnership", "partnered", "strategic partner",
    "customers", "client", "contract", "deal",
    "launched", "launch", "release", "released", "announced",
    "expands", "expansion", "opened",
    "acquisition", "acquired", "merger",
]

# Product / value prop / marketing pages (homepages often match these)
PRODUCT_KW = [
    "platform", "product", "solution", "features", "workflow", "dashboard",
    "ai-native", "ai trained", "use cases",
    "pricing", "request a demo", "book a demo", "demo",
    "credit teams", "debt markets", "data extraction", "insights",
]

NOISE_KW = [
    "cookie", "privacy", "terms", "all rights reserved", "subscribe",
    "newsletter", "sign up", "login",
]

# Role regex (cheap signal boost)
ROLE_RE = re.compile(r"\b(CEO|CTO|CFO|COO|Chief|Founder|Co-?Founder|VP|Vice President|Head of|Managing Director)\b", re.I)

def _score_keywords(text_l: str, kws: List[str]) -> int:
    return sum(1 for kw in kws if kw in text_l)

def triage_chunk(chunk_text: str) -> Dict:
    """
    Cheap triage: returns {labels:[...], confidence:0..1, reason:"..."}
    Multi-label allowed.
    """
    t = re.sub(r"\s+", " ", chunk_text).strip()
    tl = t.lower()

    # quick exits
    if len(t) < 250:
        return {"labels": ["irrelevant"], "confidence": 0.9, "reason": "too_short"}

    founders = _score_keywords(tl, FOUNDERS_KW)
    funding = _score_keywords(tl, FUNDING_KW)
    commercial_base = _score_keywords(tl, COMMERCIAL_KW)
    product = _score_keywords(tl, PRODUCT_KW)
    noise = _score_keywords(tl, NOISE_KW)

    role_hits = len(ROLE_RE.findall(t))

    labels = []
    reason_parts = []

    # --- Label rules ---
    # Founders/team: needs some signal; role hits boosts
    if founders >= 2 or role_hits >= 2:
        labels.append("founders_team")
        reason_parts.append(f"founders_kw={founders}")
        if role_hits:
            reason_parts.append(f"role_hits={role_hits}")

    # Funding: strong keywords
    if funding >= 2:
        labels.append("funding")
        reason_parts.append(f"funding_kw={funding}")

    # Commercial: partnerships OR product/value prop
    commercial = commercial_base + product
    if commercial >= 2:
        labels.append("commercial_event")
        reason_parts.append(f"commercial_kw={commercial_base}")
        reason_parts.append(f"product_kw={product}")

    # If nothing matched, decide irrelevant vs content-rich
    if not labels:
        # if mostly boilerplate/noise -> irrelevant
        if noise >= 2:
            return {"labels": ["irrelevant"], "confidence": 0.85, "reason": f"noise_kw={noise}"}

        # if content is long but no keywords, still mark irrelevant but lower confidence
        conf = 0.65 if len(t) > 1200 else 0.70
        return {"labels": ["irrelevant"], "confidence": conf, "reason": "no_signal"}

    # --- Confidence heuristic ---
    signal = founders + funding + commercial + min(role_hits, 3)
    conf = min(0.95, 0.55 + 0.07 * signal)
    conf = max(0.60, conf)

    return {"labels": labels, "confidence": conf, "reason": ";".join(reason_parts)}