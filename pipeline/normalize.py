import re
import hashlib
import unicodedata

def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def normalize_person_name(name: str) -> str:
    if not name:
        return ""
    # remove accents
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    # remove titles / punctuation-ish
    s = re.sub(r"[\.\,\(\)\[\]\{\}\-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def title_hash(title: str) -> str:
    return sha256_hex((title or "").strip().lower())