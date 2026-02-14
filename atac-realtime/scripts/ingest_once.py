"""Single-shot ingestion — fetch all feeds once, then exit. Meant for cron."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.ingest import run_once

if __name__ == "__main__":
    run_once()
