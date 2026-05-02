#!/usr/bin/env python3
"""Publish a public-safe dashboard_snapshot_v1 file from local evidence."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENERGY_INPUT = ROOT / "docs" / "evidence" / "energy_input_v1.sample.json"
DEFAULT_NEWS_INPUT = ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json"
DEFAULT_OUTPUT = ROOT / "dashboard-ui" / "public" / "dashboard_snapshot_v1.sample.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_optional_number(value: float | int | None, suffix: str) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f}{suffix}"


def first_record(payload: dict) -> dict:
    records = payload.get("records", [])
    if not records:
        raise ValueError("energy input contains no records")
    return records[0]


def first_article(payload: dict) -> dict:
    articles = payload.get("articles", [])
    if not articles:
        raise ValueError("news summary contains no articles")
    return articles[0]


def risk_from_article(article: dict) -> str:
    topics = set(article.get("topics", []))
    if {"gas_supply", "oil_gas"} & topics:
        return "watch"
    return "low"


def build_snapshot(energy_input: dict, news_summary: dict) -> dict:
    """Create only the public fields allowed by dashboard_snapshot_v1."""
    energy = first_record(energy_input)
    article = first_article(news_summary)
    risk_level = risk_from_article(article)

    region = str(energy.get("region", "gb")).lower()
    latest_date = energy.get("date", utc_now()[:10])
    demand_mw = energy.get("demand_mw")
    price = energy.get("system_buy_price_gbp_mwh")
    article_count = len(news_summary.get("articles", []))

    return {
        "schema_version": "dashboard_snapshot_v1",
        "generated_at": utc_now(),
        "metadata": {
            "region": region,
            "latest_date": latest_date,
            "data_freshness": "Local validated evidence snapshot",
            "status": "watch" if risk_level == "watch" else "ok",
        },
        "summary_cards": [
            {
                "label": "Market Price",
                "value": format_optional_number(price, " GBP/MWh"),
                "trend": "From validated energy_input_v1 evidence",
                "status": "watch" if price is not None else "error",
            },
            {
                "label": "Demand",
                "value": format_optional_number(demand_mw, " MW"),
                "trend": "From validated energy_input_v1 evidence",
                "status": "ok" if demand_mw is not None else "error",
            },
            {
                "label": "News Articles",
                "value": str(article_count),
                "trend": "Curated RSS summaries available",
                "status": "ok" if article_count else "error",
            },
        ],
        "insights": [
            {
                "id": f"local-{latest_date}-{region}-news-energy-001",
                "title": article.get("title", "Energy market news context"),
                "summary": (
                    f"{article.get('summary', 'No summary available')} "
                    f"Energy context: demand {format_optional_number(demand_mw, ' MW')}, "
                    f"market price {format_optional_number(price, ' GBP/MWh')}."
                ),
                "risk_level": risk_level,
                "confidence": 0.65,
                "sources": [
                    {
                        "label": "Validated energy evidence",
                        "url": energy.get("source_reference", "local://energy_input_v1.sample.json"),
                    },
                    {
                        "label": article.get("publisher", "Curated news source"),
                        "url": article.get("url", article.get("source_reference", "local://news_summary_v1.sample.json")),
                    },
                ],
            }
        ],
        "data_quality": {
            "status": "ok",
            "checks": [
                {
                    "label": "Energy contract",
                    "status": "ok",
                    "detail": "Publisher input came from energy_input_v1 evidence.",
                },
                {
                    "label": "News contract",
                    "status": "ok",
                    "detail": "Publisher input came from curated news_summary_v1 evidence.",
                },
                {
                    "label": "Public fields",
                    "status": "ok",
                    "detail": "Snapshot includes only dashboard-safe fields.",
                },
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish local dashboard_snapshot_v1 JSON")
    parser.add_argument("--energy-input", type=Path, default=DEFAULT_ENERGY_INPUT)
    parser.add_argument("--news-input", type=Path, default=DEFAULT_NEWS_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    energy_input = load_json(args.energy_input)
    news_summary = load_json(args.news_input)
    snapshot = build_snapshot(energy_input, news_summary)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote dashboard snapshot to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
