import os
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from dotenv import load_dotenv

# Always load .env from repo root (.../500co/.env), NOT from current working dir
REPO_ROOT = Path(__file__).resolve().parents[1]   # pipeline/ -> repo root
load_dotenv(REPO_ROOT / ".env")

OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini-2024-07-18")  # cheap default

# timeouts/retries
TIMEOUT_S = float(os.getenv("OPENAI_TIMEOUT_S", "30"))
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
SLEEP_BASE = float(os.getenv("OPENAI_RETRY_SLEEP_BASE", "1.5"))


def _get_api_key() -> str:
    """
    Fetch API key at call-time (not only at import-time),
    so it works even if env is set later in the process.
    """
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        # include where we tried to load .env from
        raise RuntimeError(f"Missing OPENAI_API_KEY in environment. Checked .env at: {REPO_ROOT / '.env'}")
    return key


def _strip_code_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        # remove first fence line
        t = t.split("\n", 1)[-1]
        # remove trailing fence
        if t.rstrip().endswith("```"):
            t = t.rsplit("```", 1)[0]
    return t.strip()


def call_llm_json(prompt: str, *, model: Optional[str] = None, max_tokens: int = 600, temperature: float = 0.1) -> Dict[str, Any]:
    """
    Calls OpenAI Chat Completions endpoint and expects JSON in content.
    Returns parsed dict. Retries on 429/5xx.
    """
    key = _get_api_key()
    use_model = model or OPENAI_MODEL

    url = f"{OPENAI_API_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": use_model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": "Return STRICT JSON only. No markdown, no code fences."},
            {"role": "user", "content": prompt},
        ],
    }

    last_text = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=TIMEOUT_S) as client:
                r = client.post(url, headers=headers, json=payload)
                # retry on rate limit / transient
                if r.status_code in (429, 500, 502, 503, 504):
                    last_text = r.text
                    time.sleep(SLEEP_BASE * attempt)
                    continue
                r.raise_for_status()
                data = r.json()

            # Extract content
            content = data["choices"][0]["message"]["content"]
            content = _strip_code_fences(content)
            last_text = content

            # Parse JSON
            return json.loads(content)

        except json.JSONDecodeError:
            raise RuntimeError(f"LLM did not return valid JSON. Raw content:\n{last_text}")
        except httpx.HTTPStatusError as e:
            # non-retryable 4xx etc
            raise RuntimeError(f"OpenAI HTTP error: {e.response.status_code} {e.response.text}") from e
        except Exception as e:
            last_text = str(e)
            time.sleep(SLEEP_BASE * attempt)

    raise RuntimeError(f"OpenAI call failed after retries. Last response: {last_text}")