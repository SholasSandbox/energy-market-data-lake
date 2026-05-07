#!/usr/bin/env python3
"""Create deterministic ai_insight_v1 output from a local AI input bundle."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "docs" / "evidence" / "ai" / "ai_input_bundle_v1.sample.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "curated" / "ai_insight_v1.sample.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def first_record(payload: dict) -> dict:
    records = payload.get("records", [])
    if not records:
        raise ValueError("energy input contains no records")
    return records[0]


def best_article(payload: dict, region: str) -> dict:
    """Prefer news for the same region, then topic-tagged news, then first article."""
    articles = payload.get("articles", [])
    if not articles:
        raise ValueError("news summary contains no articles")

    for article in articles:
        if region in article.get("regions", []):
            return article

    for article in articles:
        if article.get("topics"):
            return article

    return articles[0]


def risk_level(article: dict, price: float | None) -> str:
    topics = set(article.get("topics", []))
    if price is not None and price >= 150:
        return "high"
    if {"gas_supply", "oil_gas", "grid"} & topics:
        return "watch"
    return "low"


def confidence(article: dict, energy: dict) -> float:
    score = 0.55
    if article.get("topics"):
        score += 0.1
    if article.get("regions"):
        score += 0.1
    if energy.get("system_buy_price_gbp_mwh") is not None:
        score += 0.1
    return min(round(score, 2), 0.9)


def build_ai_insight(bundle: dict) -> dict:
    """Produce schema-shaped AI insight without calling a model yet."""
    energy_input = bundle.get("energy_input", {})
    news_summary = bundle.get("news_summary", {})
    energy = first_record(energy_input)
    region = str(energy.get("region", "gb")).lower()
    article = best_article(news_summary, region)
    price = energy.get("system_buy_price_gbp_mwh")
    demand = energy.get("demand_mw")
    insight_risk = risk_level(article, price)
    latest_date = energy.get("date", utc_now()[:10])

    return {
        "schema_version": "ai_insight_v1",
        "generated_at": utc_now(),
        "insights": [
            {
                "id": f"insight-{latest_date}-{region}-001",
                "title": f"{region.upper()} energy news context linked to: {article.get('title', 'curated news')}",
                "summary": (
                    f"Validated power evidence shows demand at {demand:,.0f} MW "
                    f"and power price at {price:,.2f} GBP/MWh. "
                    f"Relevant curated news: {article.get('summary', 'No summary available')}"
                ),
                "region": region,
                "risk_level": insight_risk,
                "confidence": confidence(article, energy),
                "time_window": {
                    "start": energy.get("timestamp_utc", energy_input.get("generated_at")),
                    "end": news_summary.get("generated_at", utc_now()),
                },
                "energy_references": [
                    {
                        "source": energy.get("source", "unknown"),
                        "metric": "system_buy_price_gbp_mwh",
                        "reference": energy.get("source_reference", "local://energy_input_v1.sample.json"),
                    }
                ],
                "news_references": [
                    {
                        "publisher": article.get("publisher", "unknown publisher"),
                        "title": article.get("title", "untitled article"),
                        "url": article.get("url", article.get("source_reference", "")),
                    }
                ],
                "validation_notes": [
                    "Deterministic local merge output for Week 3 validation.",
                    "Replace with local OpenClaw/manual-reviewed output after schema gate is stable.",
                ],
            }
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local ai_insight_v1 JSON")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = load_json(args.input)
    payload = build_ai_insight(bundle)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote AI insight output to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
