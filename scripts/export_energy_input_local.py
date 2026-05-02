#!/usr/bin/env python3
"""Create an energy_input_v1 sample from the generated dashboard data."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "dashboard-ui" / "public" / "dashboard-data.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "energy_input_v1.sample.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def parse_number(value: str) -> float | None:
    """Extract the first number from strings like GBP127.05/MWh or 43,492 MW."""
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", value or "")
    if not match:
        return None
    return float(match.group(0).replace(",", ""))


def parse_as_of(value: str) -> str:
    """Convert dashboard timestamps into schema-friendly UTC date-times."""
    if not value:
        return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    cleaned = value.replace(" UTC", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export energy_input_v1 JSON from dashboard data")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dashboard_data = load_json(args.input)
    payload = build_energy_input(dashboard_data)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(payload['records'])} energy records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
