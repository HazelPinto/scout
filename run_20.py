import sys
import json
import subprocess
from pathlib import Path

PY = sys.executable  # venv-safe


def run_step(label, cmd, fatal=False):
    print("\n>>>", label, "::", " ".join(cmd))
    try:
        subprocess.check_call(cmd)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[WARN] step_failed label={label} rc={e.returncode}")
        if fatal:
            raise
        return False


def main():
    if len(sys.argv) < 3:
        print("Usage: python run_20.py companies_seed.jsonl N")
        sys.exit(1)

    seed_file = Path(sys.argv[1])
    limit = int(sys.argv[2])

    companies = []
    with seed_file.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                companies.append(json.loads(line))
            if len(companies) >= limit:
                break

    print(f"[batch] seed={seed_file} n={len(companies)}")

    for i, row in enumerate(companies, 1):
        name = row["company_name"]
        website = row["company_website_url"]

        print("\n" + "=" * 100)
        print(f"[batch] {i}/{len(companies)} {name} {website}")

        # ensure company
        cid = subprocess.check_output([PY, "-m", "pipeline.ensure_company", name, website], text=True).strip()
        print("[company_id]", cid)

        # discovery (may fail due to TLS/blocked)
        run_step("discover", [PY, "-m", "pipeline.discover", cid], fatal=False)

        # web search (usually works even if homepage fails)
        run_step("web_search", [PY, "-m", "pipeline.web_search", cid], fatal=False)

        # fetch queued urls (should not crash on 403; your fetch_pending already skips)
        run_step("fetch_pending", [PY, "-m", "pipeline.fetch_pending", cid, "20"], fatal=False)

        # extraction
        run_step("extract_all", [PY, "-m", "pipeline.run_extract_all", cid], fatal=False)

        # changes (if you have it)
        run_step("diff", [PY, "-m", "pipeline.run_diff", cid], fatal=False)

    print("\n[BATCH DONE]")


if __name__ == "__main__":
    main()