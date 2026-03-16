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


def build_url(base_url: str, params: dict) -> str:
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


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
        "--save-env",
        action="store_true",
        help="Write ENTSOG_POINT_DIRECTIONS to config/sample.env",
    )
    args = parser.parse_args()

    countries = {c.strip().upper() for c in args.countries.split(",") if c.strip()}
    base = f"{args.base_url}/operatorpointdirections"
    url = build_url(base, {"limit": args.limit})
    payload = fetch_json(url)

    items = payload.get("items", payload)
    matches = []
    for item in items:
        from_country = str(item.get("fromCountry", "")).upper()
        to_country = str(item.get("toCountry", "")).upper()
        if from_country in countries or to_country in countries:
            matches.append(
                {
                    "pointDirection": item.get("pointDirection"),
                    "fromCountry": from_country,
                    "toCountry": to_country,
                    "fromPointKey": item.get("fromPointKey"),
                    "toPointKey": item.get("toPointKey"),
                }
            )

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
