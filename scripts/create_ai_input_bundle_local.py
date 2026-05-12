#!/usr/bin/env python3
"""Create a local AI input bundle from validated energy and news evidence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import build_bundle, load_json, write_json  # noqa: E402


DEFAULT_ENERGY_INPUT = ROOT / "docs" / "evidence" / "energy_input_v1.sample.json"
DEFAULT_NEWS_INPUT = ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "ai" / "ai_input_bundle_v1.sample.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local AI input bundle JSON")
    parser.add_argument("--energy-input", type=Path, default=DEFAULT_ENERGY_INPUT)
    parser.add_argument("--news-input", type=Path, default=DEFAULT_NEWS_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    energy_input = load_json(args.energy_input)
    news_summary = load_json(args.news_input)
    bundle = build_bundle(energy_input, news_summary)

    write_json(args.output, bundle)
    print(f"Wrote AI input bundle to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
