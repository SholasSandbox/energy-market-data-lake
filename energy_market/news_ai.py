"""Reusable news and AI insight pipeline logic.

Local CLI scripts and future Lambda handlers should import these functions
instead of duplicating transformation logic. File-system concerns stay in the
scripts; S3 concerns will stay in Lambda handlers.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import feedparser


DEFAULT_FEEDS = [
    "https://www.energyvoice.com/feed/",
    "https://www.energylivenews.com/feed/",
    "https://www.power-technology.com/feed/",
    "https://www.offshore-energy.biz/feed/",
    "https://oilprice.com/rss/main",
    "https://www.renewableenergyworld.com/feed/",
    "https://www.pv-magazine.com/feed/",
]

TOPIC_KEYWORDS = {
    "power_prices": ["power price", "electricity price", "wholesale price", "fuel price"],
    "power_supply": ["electricity", "power", "generation", "capacity", "load", "data center"],
    "gas_supply": ["natural gas", "gas supply", "lng", "pipeline", "storage", "fsru"],
    "oil_gas": ["oil", "gas", "north sea", "offshore worker", "offshore energy"],
    "renewables": ["wind", "solar", "renewable", "offshore wind", "pv"],
    "grid": ["grid", "transmission", "interconnector", "nerc"],
    "policy": ["policy", "regulator", "government", "ofgem"],
}

REGION_KEYWORDS = {
    "gb": ["uk", "britain", "gb", "great britain", "england", "scotland", "wales"],
    "eu": ["europe", "european", "eu"],
    "fr": ["france", "french"],
    "de": ["germany", "german"],
    "nl": ["netherlands", "dutch"],
}


def load_json(path: Path) -> dict:
    """Load a JSON object from disk."""
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: Path, payload: dict) -> None:
    """Write a JSON object to disk with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def utc_now() -> str:
    """Return a JSON-friendly UTC timestamp."""
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_feed_time(entry: dict) -> str:
    """Use the RSS published time when present; otherwise use current UTC time."""
    for field in ("published", "updated", "created"):
        value = entry.get(field)
        if not value:
            continue
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            continue
        return parsed.astimezone(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return utc_now()


def clean_text(value: str, max_length: int = 280) -> str:
    """Collapse whitespace and keep summaries short for the MVP contract."""
    text = " ".join((value or "").split())
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def detect_values(text: str, keyword_map: dict[str, list[str]]) -> list[str]:
    """Return labels whose keywords appear in the article text."""
    lower_text = text.lower()
    matches = [
        label
        for label, keywords in keyword_map.items()
        if any(keyword in lower_text for keyword in keywords)
    ]
    return sorted(matches)


def publisher_from_feed(feed: dict, feed_url: str) -> str:
    """Prefer RSS title, then fall back to the feed host."""
    title = feed.get("title")
    if title:
        return clean_text(title, max_length=80)
    host = urlparse(feed_url).netloc
    return host.removeprefix("www.") or "unknown publisher"


def article_from_entry(entry: dict, publisher: str) -> dict:
    """Map an RSS entry into the news_summary_v1 article shape."""
    title = clean_text(entry.get("title", "Untitled article"), max_length=160)
    url = entry.get("link", "")
    summary = clean_text(entry.get("summary") or entry.get("description") or title)
    search_text = f"{title} {summary}"

    return {
        "source": "rss",
        "publisher": publisher,
        "title": title,
        "url": url,
        "published_at": parse_feed_time(entry),
        "summary": summary,
        "topics": detect_values(search_text, TOPIC_KEYWORDS),
        "regions": detect_values(search_text, REGION_KEYWORDS),
        "entities": [],
        "source_reference": url,
    }


def topic_bucket(article: dict) -> str:
    """Group articles so the demo keeps both power and gas context visible."""
    topics = set(article.get("topics", []))
    if {"gas_supply", "oil_gas"} & topics:
        return "gas"
    if {"power_prices", "power_supply", "grid", "renewables"} & topics:
        return "power"
    return "other"


def dedupe_articles(articles: list[dict]) -> list[dict]:
    """Remove duplicate RSS entries across syndicated feeds."""
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    deduped: list[dict] = []

    for article in articles:
        url_key = article["url"].split("?")[0].rstrip("/")
        title_key = article["title"].casefold()
        if url_key in seen_urls or title_key in seen_titles:
            continue
        seen_urls.add(url_key)
        seen_titles.add(title_key)
        deduped.append(article)

    return deduped


def balance_articles(articles: list[dict], max_articles: int | None) -> list[dict]:
    """Interleave gas, power, and wider energy articles for a mixed-energy demo."""
    if max_articles is None or len(articles) <= max_articles:
        return articles

    buckets = {
        "gas": [article for article in articles if topic_bucket(article) == "gas"],
        "power": [article for article in articles if topic_bucket(article) == "power"],
        "other": [article for article in articles if topic_bucket(article) == "other"],
    }

    balanced: list[dict] = []
    while len(balanced) < max_articles and any(buckets.values()):
        for bucket_name in ("gas", "power", "other"):
            if buckets[bucket_name] and len(balanced) < max_articles:
                balanced.append(buckets[bucket_name].pop(0))

    return balanced


def fetch_articles(feed_urls: list[str], limit_per_feed: int, max_articles: int | None) -> list[dict]:
    """Fetch RSS feeds and return normalized article dictionaries."""
    articles: list[dict] = []

    for feed_url in feed_urls:
        parsed_feed = feedparser.parse(feed_url)
        publisher = publisher_from_feed(parsed_feed.feed, feed_url)

        for entry in parsed_feed.entries[:limit_per_feed]:
            article = article_from_entry(entry, publisher)
            if article["url"]:
                articles.append(article)

    return balance_articles(dedupe_articles(articles), max_articles)


def build_news_summary(articles: list[dict]) -> dict:
    """Build a news_summary_v1 payload from normalized articles."""
    return {
        "schema_version": "news_summary_v1",
        "generated_at": utc_now(),
        "articles": articles,
    }


def parse_number(value: str) -> float | None:
    """Extract the first number from strings like GBP127.05/MWh or 43,492 MW."""
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", value or "")
    if not match:
        return None
    return float(match.group(0).replace(",", ""))


def parse_as_of(value: str) -> str:
    """Convert dashboard timestamps into schema-friendly UTC date-times."""
    if not value:
        return utc_now()

    cleaned = value.replace(" UTC", "+00:00")
    parsed = dt.datetime.fromisoformat(cleaned)
    return parsed.astimezone(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_summary_value(summary_cards: list[dict], label: str) -> str:
    """Find a summary-card value by label."""
    for card in summary_cards:
        if card.get("label") == label:
            return card.get("value", "")
    return ""


def build_energy_input(dashboard_data: dict) -> dict:
    """Map the current dashboard JSON into the energy_input_v1 contract."""
    metadata = dashboard_data.get("metadata", {})
    summary_cards = dashboard_data.get("overview", {}).get("summaryCards", [])

    market_price = parse_number(find_summary_value(summary_cards, "Market Price"))
    peak_demand = parse_number(find_summary_value(summary_cards, "Peak Demand"))
    generated_at = parse_as_of(metadata.get("asOf", ""))
    latest_date = metadata.get("latestDate", generated_at[:10])
    region = str(metadata.get("region", "gb")).lower()
    table = metadata.get("table", "dashboard-data")
    bucket = metadata.get("bucket", "local")

    return {
        "schema_version": "energy_input_v1",
        "generated_at": generated_at,
        "records": [
            {
                "source": "elexon",
                "dataset": "electricity_dashboard_snapshot",
                "region": region,
                "timestamp_utc": generated_at,
                "date": latest_date,
                "settlement_period": None,
                "demand_mw": peak_demand,
                "system_sell_price_gbp_mwh": None,
                "system_buy_price_gbp_mwh": market_price,
                "day_ahead_price_eur_mwh": None,
                "source_reference": f"s3://{bucket}/curated/{table}/date={latest_date}/",
            }
        ],
    }


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
    """Classify deterministic risk from price and article topics."""
    topics = set(article.get("topics", []))
    if price is not None and price >= 150:
        return "high"
    if {"gas_supply", "oil_gas", "grid"} & topics:
        return "watch"
    return "low"


def confidence(article: dict, energy: dict) -> float:
    """Score deterministic insight confidence from source richness."""
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


def format_optional_number(value: float | int | None, suffix: str) -> str:
    """Format a number for public dashboard display."""
    if value is None:
        return "n/a"
    return f"{value:,.2f}{suffix}"


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
