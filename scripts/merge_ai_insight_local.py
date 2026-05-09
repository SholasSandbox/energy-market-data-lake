#!/usr/bin/env python3
"""Create deterministic ai_insight_v1 output from a local AI input bundle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import build_ai_insight, load_json, write_json  # noqa: E402


DEFAULT_INPUT = ROOT / "docs" / "evidence" / "ai" / "ai_input_bundle_v1.sample.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "curated" / "ai_insight_v1.sample.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local ai_insight_v1 JSON")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = load_json(args.input)
    payload = build_ai_insight(bundle)

    write_json(args.output, payload)
    print(f"Wrote AI insight output to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
