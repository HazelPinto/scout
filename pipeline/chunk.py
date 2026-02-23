from typing import List, Dict

def chunk_text(clean_text: str, max_chars: int = 2400, max_chunks_per_source: int = 3) -> List[Dict]:
    """
    Robust chunking:
    - Splits by blank lines into paragraphs
    - Packs paragraphs into chunks up to max_chars
    - Always returns at least 1 chunk if clean_text has meaningful length
    """
    text = (clean_text or "").strip()
    if len(text) < 200:
        return []

    # paragraphs
    paras = []
    buf = []
    for line in text.splitlines():
        ln = line.strip()
        if not ln:
            if buf:
                paras.append(" ".join(buf).strip())
                buf = []
            continue
        buf.append(ln)
    if buf:
        paras.append(" ".join(buf).strip())

    if not paras:
        # fallback: whole text as one "paragraph"
        paras = [text]

    chunks = []
    cur = []
    cur_len = 0

    def flush():
        nonlocal cur, cur_len, chunks
        if not cur:
            return
        chunk = "\n\n".join(cur).strip()
        if chunk:
            chunks.append(chunk)
        cur = []
        cur_len = 0

    for p in paras:
        p = p.strip()
        if not p:
            continue

        if cur_len + len(p) + 2 > max_chars and cur:
            flush()

        # if single paragraph is huge, hard-split
        if len(p) > max_chars:
            start = 0
            while start < len(p):
                part = p[start:start + max_chars]
                if part.strip():
                    chunks.append(part.strip())
                start += max_chars
            continue

        cur.append(p)
        cur_len += len(p) + 2

        if len(chunks) >= max_chunks_per_source:
            break

    flush()

    # ensure at least 1 chunk
    if not chunks:
        chunks = [text[:max_chars]]

    # build heading+text objects
    out = []
    for i, c in enumerate(chunks[:max_chunks_per_source], 1):
        heading = "BODY"
        # heuristic: if first line is short, treat as heading-ish
        first_line = c.splitlines()[0].strip() if c.splitlines() else ""
        if 0 < len(first_line) <= 80:
            heading = first_line
        out.append({"idx": i, "heading": heading, "text": c})

    return out