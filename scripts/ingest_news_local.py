#!/usr/bin/env python3
"""Fetch RSS feeds and write a curated-style news_summary_v1 JSON file."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import feedparser


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json"

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


def utc_now() -> str:
    """Return a JSON-friendly UTC timestamp."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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


def write_news_summary(output_path: Path, articles: list[dict]) -> None:
    """Write a news_summary_v1 payload to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "news_summary_v1",
        "generated_at": utc_now(),
        "articles": articles,
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch RSS feeds into news_summary_v1 JSON")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit-per-feed", type=int, default=4)
    parser.add_argument("--max-articles", type=int, default=18)
    parser.add_argument("--feed", action="append", dest="feeds", help="RSS feed URL; can be repeated")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    feed_urls = args.feeds or DEFAULT_FEEDS
    articles = fetch_articles(feed_urls, args.limit_per_feed, args.max_articles)

    if not articles:
        raise SystemExit("No articles were fetched. Check RSS URLs or network access.")

    write_news_summary(args.output, articles)
    print(f"Wrote {len(articles)} articles to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
