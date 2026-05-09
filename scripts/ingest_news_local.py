#!/usr/bin/env python3
"""Fetch RSS feeds and write a curated-style news_summary_v1 JSON file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import (  # noqa: E402
    DEFAULT_FEEDS,
    build_news_summary,
    fetch_articles,
    write_json,
)


DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json"


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

    write_json(args.output, build_news_summary(articles))
    print(f"Wrote {len(articles)} articles to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
