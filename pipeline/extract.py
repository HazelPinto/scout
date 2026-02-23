import re
from typing import Dict, Any, Optional

try:
    from pipeline.llm import call_llm_json
except Exception:
    call_llm_json = None


EXTRACTOR_VERSION = "v0.1.0"

EVENT_TYPES = [
    "funding",
    "partnership",
    "product_launch",
    "expansion",
    "acquisition",
    "milestone",
    "other",
]

ROUND_TYPES = [
    "pre_seed", "seed", "series_a", "series_b", "series_c", "series_d",
    "series_e", "series_f", "series_g", "growth", "venture_debt", "grant", "unknown"
]


def _strip_code_fences(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^```(json)?", "", s, flags=re.I).strip()
    s = re.sub(r"```$", "", s).strip()
    return s


def extract_chunk(company_name: str, source_url: str, chunk_text: str) -> Dict[str, Any]:
    """
    Calls LLM to extract people, events, funding_rounds.
    Must include evidence_quote that is an exact substring of chunk_text.
    """
    if not call_llm_json:
        raise RuntimeError("LLM not configured. pipeline.llm.call_llm_json not importable.")

    prompt = f"""
You are an information extraction system. Extract ONLY what is explicitly supported by the text.
Company: {company_name}
Source URL: {source_url}

Return STRICT JSON ONLY (no markdown, no code fences) in this shape:
{{
  "extractor_version": "{EXTRACTOR_VERSION}",
  "people": [
    {{"name": "...", "role": "...", "linkedin_url": null, "confidence": 0.0, "evidence_quote": "..."}}
  ],
  "events": [
    {{"type": "funding|partnership|product_launch|expansion|acquisition|milestone|other",
      "date": null,
      "title": "...",
      "summary": "...",
      "confidence": 0.0,
      "evidence_quote": "..." }}
  ],
  "funding_rounds": [
    {{"round_type": "pre_seed|seed|series_a|series_b|series_c|series_d|series_e|series_f|series_g|growth|venture_debt|grant|unknown",
      "amount": null,
      "currency": null,
      "date": null,
      "investors": [],
      "confidence": 0.0,
      "evidence_quote": "..." }}
  ]
}}

Hard rules:
- evidence_quote MUST be an exact substring from the provided TEXT.
- Do NOT invent LinkedIn URLs. Only include if present verbatim.
- Do NOT invent amounts/dates/investors.
- If nothing is present, return empty arrays.

TEXT:
\"\"\"{chunk_text}\"\"\"
""".strip()

    payload = call_llm_json(prompt)

    # normalize minimal fields
    if not isinstance(payload, dict):
        raise RuntimeError(f"Bad extraction payload type: {type(payload)}")

    payload.setdefault("extractor_version", EXTRACTOR_VERSION)
    payload.setdefault("people", [])
    payload.setdefault("events", [])
    payload.setdefault("funding_rounds", [])

    return payload