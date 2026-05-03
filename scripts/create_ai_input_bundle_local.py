#!/usr/bin/env python3
"""Create a local AI input bundle from validated energy and news evidence."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENERGY_INPUT = ROOT / "docs" / "evidence" / "energy_input_v1.sample.json"
DEFAULT_NEWS_INPUT = ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "ai" / "ai_input_bundle_v1.sample.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_bundle(energy_input: dict, news_summary: dict) -> dict:
    """Package validated inputs with explicit instructions for the AI merge step."""
    return {
        "bundle_version": "ai_input_bundle_v1",
        "generated_at": utc_now(),
        "purpose": "Merge validated energy facts with curated news summaries.",
        "expected_output_schema": "schemas/ai_insight_v1.schema.json",
        "instructions": [
            "Return only JSON that follows ai_insight_v1.schema.json.",
            "Use confidence between 0 and 1.",
            "Use risk_level as one of low, watch, or high.",
            "Cite at least one energy reference and one news reference.",
            "Do not include raw RSS payloads or private fields in the output.",
        ],
        "energy_input": energy_input,
        "news_summary": news_summary,
    }


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

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(bundle, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote AI input bundle to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
