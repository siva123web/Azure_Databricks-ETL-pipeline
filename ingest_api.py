"""
ingestion/ingest_api.py
=======================

Step 1 of the pipeline: pull raw data from a public REST API and land it
into the RAW zone of the (simulated) Azure Data Lake Gen2.

Source API : https://fakestoreapi.com/products
Why this API:
    - No auth required, perfect for portfolio demos.
    - Returns realistic e-commerce data (id, title, price, category, rating, ...).
    - Stable schema, ~20 records — small enough for a laptop, large enough to be useful.

What this script does
---------------------
1. Calls the API and gets the full list of products as JSON.
2. Adds an ingestion timestamp to every record (audit column).
3. Writes a partitioned, dated JSON file under ``data/raw/products/``.
   In real Azure this would be the path
   ``abfss://raw@<storage>.dfs.core.windows.net/products/ingest_date=YYYY-MM-DD/``.

Run locally
-----------
    python ingestion/ingest_api.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# In production these would come from environment variables / Key Vault.
API_URL: str = "https://fakestoreapi.com/products"
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
RAW_BASE_PATH: Path = PROJECT_ROOT / "data" / "raw" / "products"
REQUEST_TIMEOUT_SECONDS: int = 30


def fetch_products(api_url: str) -> list[dict]:
    """Hit the public API and return a list of product dicts.

    Raises:
        requests.HTTPError: if the API returns a non-2xx status.
        ValueError: if the payload is empty or not a list.
    """
    print(f"[ingest] GET {api_url}")
    response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()  # surface 4xx / 5xx as exceptions

    payload = response.json()
    if not isinstance(payload, list) or len(payload) == 0:
        raise ValueError(f"Unexpected payload from {api_url}: {payload!r}")

    print(f"[ingest] Received {len(payload)} records")
    return payload


def stamp_records(records: list[dict]) -> list[dict]:
    """Add a single audit column, ``_ingested_at``, to every record.

    Keeping the audit column at ingestion time means the Bronze layer
    can rely on it for incremental loads, late-arriving data analysis, etc.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    return [{**record, "_ingested_at": now_iso} for record in records]


def write_raw_json(records: list[dict], base_path: Path) -> Path:
    """Persist the records as a single JSON file partitioned by ingest date.

    Layout:
        data/raw/products/
            ingest_date=2026-05-08/
                products_2026-05-08T12-34-56Z.json
    """
    today_partition = datetime.now(timezone.utc).strftime("ingest_date=%Y-%m-%d")
    partition_dir = base_path / today_partition
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_file = partition_dir / f"products_{file_stamp}.json"

    with out_file.open("w", encoding="utf-8") as f:
        # ``indent=2`` keeps the file human-readable for portfolio reviewers.
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"[ingest] Wrote {len(records)} records to {out_file}")
    return out_file


def main() -> int:
    """Entry point. Returns a POSIX exit code."""
    try:
        records = fetch_products(API_URL)
        records = stamp_records(records)
        write_raw_json(records, RAW_BASE_PATH)
    except Exception as exc:  # noqa: BLE001 — top-level guard for CLI run
        # In ADF this would surface as a failed activity; we mimic that here.
        print(f"[ingest] FAILED: {exc}", file=sys.stderr)
        return 1

    print("[ingest] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
