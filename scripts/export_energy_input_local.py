#!/usr/bin/env python3
"""Create an energy_input_v1 sample from the generated dashboard data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import build_energy_input, load_json, write_json  # noqa: E402


DEFAULT_INPUT = ROOT / "dashboard-ui" / "public" / "dashboard-data.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "energy_input_v1.sample.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export energy_input_v1 JSON from dashboard data")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dashboard_data = load_json(args.input)
    payload = build_energy_input(dashboard_data)

    write_json(args.output, payload)
    print(f"Wrote {len(payload['records'])} energy records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
