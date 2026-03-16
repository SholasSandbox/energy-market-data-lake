#!/usr/bin/env python3
"""
Run the Lambda ingestion handler locally.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path


def _load_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("ingest_elexon", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description="Invoke ingest_elexon.py locally")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=2,
        help="Backfill days for this run (default: 2)",
    )
    parser.add_argument(
        "--base-url",
        default="https://data.elexon.co.uk/bmrs/api/v1",
        help="Elexon API base URL",
    )
    args = parser.parse_args()

    os.environ["S3_BUCKET"] = args.bucket
    os.environ["BACKFILL_DAYS"] = str(args.backfill_days)
    os.environ["ELEXON_BASE_URL"] = args.base_url

    module_path = Path(__file__).resolve().parents[1] / "lambda" / "ingest_elexon.py"
    module = _load_module(module_path)
    result = module.handler({}, None)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
