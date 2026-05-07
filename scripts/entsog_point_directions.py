#!/usr/bin/env python3
"""
Fetch ENTSOG pointDirection IDs and filter by country codes.
Usage:
  python scripts/entsog_point_directions.py --countries GB,FR,DE,NL
"""
import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def build_url(base_url: str, params: dict) -> str:
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_items(payload: Any) -> list[dict]:
    """Return the list of point-direction records from known ENTSOG shapes."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected ENTSOG response type: {type(payload).__name__}")

    for key in ("items", "operatorpointdirections", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    keys = ", ".join(sorted(payload.keys()))
    raise ValueError(f"Could not find point-direction list in ENTSOG response. Keys: {keys}")


def first_value(item: dict, *keys: str) -> str:
    """Read the first non-empty value from possible old/new API field names."""
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def query_point_direction(item: dict) -> str:
    """Build the pointDirection value accepted by operationaldatas."""
    explicit = first_value(item, "pointDirection")
    if explicit:
        return explicit

    operator_key = first_value(item, "operatorKey")
    point_key = first_value(item, "fromPointKey", "pointKey")
    direction_key = first_value(item, "directionKey")
    if operator_key and point_key and direction_key:
        return f"{operator_key}{point_key}{direction_key}"

    raw_id = first_value(item, "id")
    data_set = first_value(item, "dataSet")
    if raw_id and data_set and raw_id.startswith(data_set):
        return raw_id[len(data_set) :]
    return raw_id


def expand_country_codes(countries: set[str]) -> set[str]:
    """Add ENTSOG-specific country aliases used by gas data."""
    expanded = set(countries)
    if "GB" in expanded:
        expanded.add("UK")
    return expanded


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--countries",
        default="GB,FR,DE,NL",
        help="Comma-separated country codes to include (default: GB,FR,DE,NL)",
    )
    parser.add_argument(
        "--base-url",
        default="https://transparency.entsog.eu/api/v1",
        help="ENTSOG base URL",
    )
    parser.add_argument(
        "--limit",
        default="2000",
        help="Result limit (default: 2000)",
    )
    parser.add_argument(
        "--ids-only",
        action="store_true",
        help="Print only pointDirection IDs (comma-separated)",
    )
    parser.add_argument(
        "--has-data-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Filter to hasData=true point directions (default: true)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=0,
        help="Maximum matched records to output after filtering (default: all)",
    )
    parser.add_argument(
        "--save-env",
        action="store_true",
        help="Write ENTSOG_POINT_DIRECTIONS to config/sample.env",
    )
    args = parser.parse_args()

    countries = expand_country_codes(
        {c.strip().upper() for c in args.countries.split(",") if c.strip()}
    )
    base = f"{args.base_url}/operatorpointdirections"
    params = {"limit": args.limit}
    if args.has_data_only:
        params["hasData"] = "1"
    url = build_url(base, params)
    payload = fetch_json(url)

    items = extract_items(payload)
    matches = []
    for item in items:
        from_country = first_value(item, "fromCountry", "tSOCountry").upper()
        to_country = first_value(item, "toCountry", "adjacentCountry").upper()
        has_data = item.get("hasData")
        if args.has_data_only and has_data is not True:
            continue
        if from_country in countries or to_country in countries:
            matches.append(
                {
                    "pointDirection": query_point_direction(item),
                    "operatorPointDirectionId": first_value(item, "id"),
                    "fromCountry": from_country,
                    "toCountry": to_country,
                    "directionKey": item.get("directionKey"),
                    "fromPointKey": first_value(item, "fromPointKey", "pointKey"),
                    "toPointKey": item.get("toPointKey"),
                    "pointLabel": item.get("pointLabel"),
                    "operatorLabel": item.get("operatorLabel"),
                    "hasData": item.get("hasData"),
                }
            )
            if args.max_results and len(matches) >= args.max_results:
                break

    if args.ids_only:
        ids = [m["pointDirection"] for m in matches if m.get("pointDirection")]
        print(",".join(ids))
    else:
        print(json.dumps(matches, indent=2, sort_keys=True))

    if args.save_env:
        ids = [m["pointDirection"] for m in matches if m.get("pointDirection")]
        env_path = Path("config/sample.env")
        if not env_path.exists():
            raise FileNotFoundError("config/sample.env not found")
        contents = env_path.read_text(encoding="utf-8").splitlines()
        updated = []
        replaced = False
        for line in contents:
            if line.startswith("ENTSOG_POINT_DIRECTIONS="):
                updated.append(f"ENTSOG_POINT_DIRECTIONS={','.join(ids)}")
                replaced = True
            else:
                updated.append(line)
        if not replaced:
            updated.append(f"ENTSOG_POINT_DIRECTIONS={','.join(ids)}")
        env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
