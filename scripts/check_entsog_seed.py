#!/usr/bin/env python3
"""
Live-check ENTSOG pointDirection seeds for flow and allocation data.

Example:
  python3 scripts/check_entsog_seed.py \
    --point-directions BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit \
    --date 2026-05-03
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "https://transparency.entsog.eu/api/v1"


def build_url(base_url: str, endpoint: str, params: dict[str, str]) -> str:
    return f"{base_url.rstrip('/')}/{endpoint}?{urllib.parse.urlencode(params)}"


def fetch_json(url: str, timeout_seconds: int) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "energy-market-lake/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_point_directions(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def default_check_date() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()


def check_indicator(
    *,
    base_url: str,
    point_direction: str,
    check_date: str,
    indicator: str,
    period_type: str,
    time_zone: str,
    include_exemptions: str,
    limit: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    params = {
        "pointDirection": point_direction,
        "from": check_date,
        "to": check_date,
        "indicator": indicator,
        "periodType": period_type,
        "timeZone": time_zone,
        "includeExemptions": include_exemptions,
        "limit": limit,
    }
    url = build_url(base_url, "operationaldatas", params)
    payload = fetch_json(url, timeout_seconds)
    rows = payload.get("operationaldatas", [])
    values = [row.get("value") for row in rows if row.get("value") not in (None, "")]

    return {
        "indicator": indicator,
        "url": url,
        "row_count": len(rows),
        "non_empty_value_count": len(values),
        "sample_values": values[:3],
        "passed": bool(rows) and bool(values),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# ENTSOG Seed Check",
        "",
        f"- Timestamp (UTC): {report['timestamp_utc']}",
        f"- Date checked: {report['date']}",
        f"- Status: **{report['status'].upper()}**",
        "",
        "## Results",
        "",
    ]

    for result in report["results"]:
        marker = "PASS" if result["passed"] else "FAIL"
        lines.append(f"### `{result['point_direction']}`")
        lines.append("")
        lines.append(f"- Status: {marker}")
        for check in result["checks"]:
            check_marker = "PASS" if check["passed"] else "FAIL"
            lines.append(
                f"- {check_marker} `{check['indicator']}`: "
                f"{check['row_count']} rows, "
                f"{check['non_empty_value_count']} populated values, "
                f"samples `{check['sample_values']}`"
            )
        if result["errors"]:
            for error in result["errors"]:
                lines.append(f"- ERROR `{error['indicator']}`: {error['message']}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Live-check ENTSOG pointDirection seeds for flow and allocation data."
    )
    parser.add_argument(
        "--point-directions",
        default=os.environ.get("ENTSOG_POINT_DIRECTIONS", ""),
        help="Comma-separated pointDirection values. Defaults to ENTSOG_POINT_DIRECTIONS.",
    )
    parser.add_argument(
        "--date",
        default=default_check_date(),
        help="Gas day to check in YYYY-MM-DD format. Default: yesterday.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--flow-indicator", default="Physical Flow")
    parser.add_argument("--demand-indicator", default="Allocation")
    parser.add_argument("--period-type", default="day")
    parser.add_argument("--time-zone", default="WET")
    parser.add_argument("--include-exemptions", default="0")
    parser.add_argument("--limit", default="5")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional markdown report path to write.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of markdown.",
    )
    args = parser.parse_args()

    point_directions = parse_point_directions(args.point_directions)
    if not point_directions:
        raise SystemExit(
            "No point directions provided. Use --point-directions or set "
            "ENTSOG_POINT_DIRECTIONS."
        )

    report: dict[str, Any] = {
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "date": args.date,
        "status": "pass",
        "results": [],
    }

    indicators = [args.flow_indicator, args.demand_indicator]
    for point_direction in point_directions:
        result = {
            "point_direction": point_direction,
            "passed": True,
            "checks": [],
            "errors": [],
        }
        for indicator in indicators:
            try:
                check = check_indicator(
                    base_url=args.base_url,
                    point_direction=point_direction,
                    check_date=args.date,
                    indicator=indicator,
                    period_type=args.period_type,
                    time_zone=args.time_zone,
                    include_exemptions=args.include_exemptions,
                    limit=args.limit,
                    timeout_seconds=args.timeout_seconds,
                )
                result["checks"].append(check)
                if not check["passed"]:
                    result["passed"] = False
            except Exception as exc:  # noqa: BLE001
                result["passed"] = False
                result["errors"].append(
                    {
                        "indicator": indicator,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                )

        if not result["passed"]:
            report["status"] = "fail"
        report["results"].append(result)

    output = json.dumps(report, indent=2, sort_keys=True) if args.json else render_markdown(report)

    if args.output_file:
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output, encoding="utf-8")
    else:
        print(output, end="")

    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
