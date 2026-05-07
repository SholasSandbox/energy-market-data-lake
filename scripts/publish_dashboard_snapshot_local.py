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
DEFAULT_AI_INSIGHT_INPUT = ROOT / "docs" / "evidence" / "curated" / "ai_insight_v1.sample.json"
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


def first_insight(payload: dict) -> dict:
    insights = payload.get("insights", [])
    if not insights:
        raise ValueError("AI insight input contains no insights")
    return insights[0]


def public_news_articles(news_summary: dict, max_articles: int = 12) -> list[dict]:
    """Copy only dashboard-safe article fields into the public snapshot."""
    articles: list[dict] = []
    for article in news_summary.get("articles", [])[:max_articles]:
        articles.append(
            {
                "publisher": article.get("publisher", "Curated news source"),
                "title": article.get("title", "Untitled article"),
                "url": article.get("url", article.get("source_reference", "")),
                "published_at": article.get("published_at", news_summary.get("generated_at", utc_now())),
                "summary": article.get("summary", "No summary available"),
                "topics": article.get("topics", []),
                "regions": article.get("regions", []),
            }
        )
    return articles


def dashboard_sources(ai_insight: dict) -> list[dict]:
    """Flatten AI source references into dashboard-safe label/url objects."""
    sources: list[dict] = []

    for reference in ai_insight.get("energy_references", []):
        label = f"{reference.get('source', 'Energy')} - {reference.get('metric', 'metric')}"
        sources.append(
            {
                "label": label,
                "url": reference.get("reference", "local://energy_input_v1.sample.json"),
            }
        )

    for reference in ai_insight.get("news_references", []):
        sources.append(
            {
                "label": reference.get("publisher", "Curated news source"),
                "url": reference.get("url", "local://news_summary_v1.sample.json"),
            }
        )

    return sources


def build_snapshot(energy_input: dict, news_summary: dict, ai_insight_input: dict) -> dict:
    """Create only the public fields allowed by dashboard_snapshot_v1."""
    energy = first_record(energy_input)
    article = first_article(news_summary)
    ai_insight = first_insight(ai_insight_input)

    region = str(ai_insight.get("region") or energy.get("region", "gb")).lower()
    latest_date = energy.get("date", utc_now()[:10])
    demand_mw = energy.get("demand_mw")
    price = energy.get("system_buy_price_gbp_mwh")
    article_count = len(news_summary.get("articles", []))
    snapshot_status = "watch" if ai_insight.get("risk_level") in {"watch", "high"} else "ok"

    return {
        "schema_version": "dashboard_snapshot_v1",
        "generated_at": utc_now(),
        "metadata": {
            "region": region,
            "latest_date": latest_date,
            "data_freshness": "Local demo evidence snapshot; not live market freshness",
            "status": snapshot_status,
        },
        "summary_cards": [
            {
                "label": "Power Price",
                "value": format_optional_number(price, " GBP/MWh"),
                "trend": "From validated power evidence",
                "status": "watch" if price is not None else "error",
            },
            {
                "label": "Power Demand",
                "value": format_optional_number(demand_mw, " MW"),
                "trend": "From validated power evidence",
                "status": "ok" if demand_mw is not None else "error",
            },
            {
                "label": "Energy News",
                "value": str(article_count),
                "trend": "Wider energy-sector RSS context",
                "status": "ok" if article_count else "error",
            },
        ],
        "insights": [
            {
                "id": ai_insight.get("id", f"local-{latest_date}-{region}-ai-001"),
                "title": ai_insight.get("title", article.get("title", "Energy market insight")),
                "summary": ai_insight.get("summary", article.get("summary", "No summary available")),
                "risk_level": ai_insight.get("risk_level", "watch"),
                "confidence": ai_insight.get("confidence", 0.5),
                "sources": dashboard_sources(ai_insight),
            }
        ],
        "news_articles": public_news_articles(news_summary),
        "data_quality": {
            "status": "ok",
            "checks": [
                {
                    "label": "Power evidence contract",
                    "status": "ok",
                    "detail": "Publisher metric input came from local demo power-focused energy_input_v1 evidence.",
                },
                {
                    "label": "Energy news contract",
                    "status": "ok",
                    "detail": "Publisher input came from local demo curated news_summary_v1 evidence.",
                },
                {
                    "label": "AI insight contract",
                    "status": "ok",
                    "detail": "Publisher insight came from local demo validated ai_insight_v1 evidence.",
                },
                {
                    "label": "Public fields",
                    "status": "ok",
                    "detail": "Local demo snapshot includes only dashboard-safe fields.",
                },
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish local dashboard_snapshot_v1 JSON")
    parser.add_argument("--energy-input", type=Path, default=DEFAULT_ENERGY_INPUT)
    parser.add_argument("--news-input", type=Path, default=DEFAULT_NEWS_INPUT)
    parser.add_argument("--ai-insight", type=Path, default=DEFAULT_AI_INSIGHT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    energy_input = load_json(args.energy_input)
    news_summary = load_json(args.news_input)
    ai_insight = load_json(args.ai_insight)
    snapshot = build_snapshot(energy_input, news_summary, ai_insight)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote dashboard snapshot to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
