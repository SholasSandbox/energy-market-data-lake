#!/usr/bin/env python3
"""
Validate the curated Athena electricity table after the crawler runs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import time
from typing import Dict, List, Optional


REQUIRED_COLUMNS = {
    "source": "string",
    "region": "string",
    "date": "string",
    "settlement_period": "int",
    "start_time_utc": "timestamp",
    "demand_mw": "double",
    "day_ahead_price_eur_mwh": "double",
    "system_sell_price": "double",
    "system_buy_price": "double",
    "net_imbalance_volume": "double",
}


def _run_aws(args: List[str], expect_json: bool = True):
    proc = subprocess.run(
        ["aws"] + args,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"AWS CLI command failed: aws {' '.join(args)}\n{proc.stderr.strip()}"
        )
    out = proc.stdout.strip()
    if not expect_json:
        return out
    return json.loads(out) if out else {}


def _discover_bucket_name() -> str:
    data = _run_aws(
        [
            "s3api",
            "list-buckets",
            "--query",
            "Buckets[?starts_with(Name, `energy-market-lake-`)].Name",
            "--output",
            "json",
        ]
    )
    if not data:
        raise RuntimeError("No bucket matching energy-market-lake-* found")
    return sorted(data)[-1]


def _start_athena_query(
    query: str, database: str, output_location: str, region: str
) -> str:
    return _run_aws(
        [
            "athena",
            "start-query-execution",
            "--query-string",
            query,
            "--query-execution-context",
            f"Database={database}",
            "--result-configuration",
            f"OutputLocation={output_location}",
            "--region",
            region,
            "--query",
            "QueryExecutionId",
            "--output",
            "text",
        ],
        expect_json=False,
    )


def _wait_athena_query(execution_id: str, region: str):
    while True:
        state = _run_aws(
            [
                "athena",
                "get-query-execution",
                "--query-execution-id",
                execution_id,
                "--region",
                region,
                "--query",
                "QueryExecution.Status.State",
                "--output",
                "text",
            ],
            expect_json=False,
        )
        if state == "SUCCEEDED":
            return
        if state in {"FAILED", "CANCELLED"}:
            reason = _run_aws(
                [
                    "athena",
                    "get-query-execution",
                    "--query-execution-id",
                    execution_id,
                    "--region",
                    region,
                    "--query",
                    "QueryExecution.Status.StateChangeReason",
                    "--output",
                    "text",
                ],
                expect_json=False,
            )
            raise RuntimeError(f"Athena query failed ({state}): {reason}")
        time.sleep(1.5)


def _fetch_athena_rows(execution_id: str, region: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    headers: Optional[List[str]] = None
    next_token = None

    while True:
        args = [
            "athena",
            "get-query-results",
            "--query-execution-id",
            execution_id,
            "--region",
            region,
            "--output",
            "json",
        ]
        if next_token:
            args += ["--next-token", next_token]
        page = _run_aws(args)

        page_rows = page.get("ResultSet", {}).get("Rows", [])
        if headers is None:
            if not page_rows:
                return rows
            headers = [
                col.get("VarCharValue", "")
                for col in page_rows[0].get("Data", [])
            ]
            page_rows = page_rows[1:]

        for row in page_rows:
            vals = [c.get("VarCharValue", "") for c in row.get("Data", [])]
            if vals:
                rows.append(dict(zip(headers, vals)))

        next_token = page.get("NextToken")
        if not next_token:
            break

    return rows


def _run_athena_query(
    query: str, database: str, output_location: str, region: str
) -> List[Dict[str, str]]:
    qid = _start_athena_query(query, database, output_location, region)
    _wait_athena_query(qid, region)
    return _fetch_athena_rows(qid, region)


def _athena_type_matches(expected: str, actual: str) -> bool:
    actual_lower = (actual or "").lower()
    if expected == "string":
        return actual_lower in {"varchar", "string"}
    if expected == "int":
        return actual_lower in {"integer", "int", "bigint"}
    return actual_lower == expected


def main():
    parser = argparse.ArgumentParser(description="Validate curated Athena table schema")
    parser.add_argument("--region", default="eu-west-2")
    parser.add_argument("--database", default="energy_market_lake")
    parser.add_argument("--table", default="curated_dataset_electricity")
    parser.add_argument("--output-location", default="")
    parser.add_argument("--expected-sources", default="elexon")
    parser.add_argument("--output-file", default="")
    args = parser.parse_args()

    bucket = _discover_bucket_name()
    output_location = args.output_location or f"s3://{bucket}/athena-results/"
    expected_sources = [s.strip().lower() for s in args.expected_sources.split(",") if s.strip()]

    schema_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{args.database}'
      AND table_name = '{args.table}'
    ORDER BY ordinal_position
    """
    source_query = f"""
    SELECT source, COUNT(*) AS row_count
    FROM {args.table}
    GROUP BY source
    ORDER BY source
    """
    freshness_query = f"""
    SELECT source, region, MAX(date) AS latest_date
    FROM {args.table}
    GROUP BY source, region
    ORDER BY source, region
    """

    schema_rows = _run_athena_query(
        schema_query, args.database, output_location, args.region
    )
    source_rows = _run_athena_query(
        source_query, args.database, output_location, args.region
    )
    freshness_rows = _run_athena_query(
        freshness_query, args.database, output_location, args.region
    )

    actual_columns = {
        row["column_name"]: row["data_type"] for row in schema_rows if row.get("column_name")
    }
    missing_columns = [
        name for name in REQUIRED_COLUMNS if name not in actual_columns
    ]
    type_mismatches = [
        f"{name}: expected {expected}, got {actual_columns.get(name)}"
        for name, expected in REQUIRED_COLUMNS.items()
        if name in actual_columns and not _athena_type_matches(expected, actual_columns[name])
    ]

    actual_sources = {
        row["source"].lower(): int(float(row["row_count"]))
        for row in source_rows
        if row.get("source")
    }
    missing_sources = [
        source for source in expected_sources if actual_sources.get(source, 0) <= 0
    ]

    status = "pass"
    if missing_columns or type_mismatches or missing_sources:
        status = "fail"

    report = [
        "# Athena Schema Validation",
        "",
        f"- Timestamp (UTC): {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"- Region: {args.region}",
        f"- Database: {args.database}",
        f"- Table: {args.table}",
        f"- Output location: {output_location}",
        f"- Status: **{status.upper()}**",
        "",
        "## Required Columns",
        "",
    ]

    for name, expected in REQUIRED_COLUMNS.items():
        actual = actual_columns.get(name, "missing")
        marker = "OK" if name in actual_columns and _athena_type_matches(expected, actual) else "FAIL"
        report.append(f"- {marker} `{name}` -> expected `{expected}`, actual `{actual}`")

    report.extend(
        [
            "",
            "## Source Coverage",
            "",
        ]
    )
    for row in source_rows:
        report.append(f"- `{row['source']}`: {row['row_count']} rows")

    report.extend(
        [
            "",
            "## Latest Dates By Source and Region",
            "",
        ]
    )
    for row in freshness_rows:
        report.append(f"- `{row['source']}` / `{row['region']}`: {row['latest_date']}")

    if missing_columns or type_mismatches or missing_sources:
        report.extend(["", "## Validation Errors", ""])
        for name in missing_columns:
            report.append(f"- Missing required column `{name}`")
        for item in type_mismatches:
            report.append(f"- Type mismatch: {item}")
        for source in missing_sources:
            report.append(f"- Expected source `{source}` has no rows")

    output_text = "\n".join(report) + "\n"

    if args.output_file:
        output_path = pathlib.Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text, encoding="utf-8")
    else:
        print(output_text)

    if status != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
