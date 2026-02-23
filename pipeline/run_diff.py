import os
import sys
from dotenv import load_dotenv

load_dotenv()

from pipeline.diff import run_diff

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.run_diff <company_uuid>")
        return
    company_id = sys.argv[1].strip()
    stats = run_diff(company_id)
    print(f"[diff] company_id={company_id} stats={stats}")

if __name__ == "__main__":
    main()