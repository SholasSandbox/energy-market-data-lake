#!/usr/bin/env python3
"""
Generate an HTML dashboard from Athena curated data using AWS CLI.

Includes:
1) Market analytics tab (from ingested data)
2) Fictional utilities company positions/hedges + P&L tab
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import time
from typing import Dict, List, Optional


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


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _build_fictional_portfolio(latest_market_price: float):
    limits = {
        "min_margin_per_mwh": 4.0,
        "max_portfolio_unhedged_pct": 25.0,
    }

    # Fictional books for a conservative retailer posture:
    # high forward cover and tighter hedge bands to reduce spot exposure.
    books = [
        {
            "book": "Residential Fixed South",
            "volume_mwh": 135000,
            "customer_tariff": 106.0,
            "hedge_coverage_pct": 92.0,
            "hedge_price": 79.0,
            "target_min_hedge_pct": 85.0,
            "target_max_hedge_pct": 97.0,
        },
        {
            "book": "SME Indexed Midlands",
            "volume_mwh": 90000,
            "customer_tariff": 99.0,
            "hedge_coverage_pct": 84.0,
            "hedge_price": 86.0,
            "target_min_hedge_pct": 80.0,
            "target_max_hedge_pct": 92.0,
        },
        {
            "book": "Industrial Flex",
            "volume_mwh": 60000,
            "customer_tariff": 114.0,
            "hedge_coverage_pct": 90.0,
            "hedge_price": 84.0,
            "target_min_hedge_pct": 85.0,
            "target_max_hedge_pct": 98.0,
        },
        {
            "book": "Public Sector Framework",
            "volume_mwh": 70000,
            "customer_tariff": 103.0,
            "hedge_coverage_pct": 82.0,
            "hedge_price": 87.0,
            "target_min_hedge_pct": 80.0,
            "target_max_hedge_pct": 92.0,
        },
        {
            "book": "EV Flex Portfolio",
            "volume_mwh": 40000,
            "customer_tariff": 95.0,
            "hedge_coverage_pct": 76.0,
            "hedge_price": 88.0,
            "target_min_hedge_pct": 75.0,
            "target_max_hedge_pct": 90.0,
        },
    ]

    for row in books:
        total = float(row["volume_mwh"])
        hedged = total * (float(row["hedge_coverage_pct"]) / 100.0)
        unhedged = total - hedged
        unhedged_pct = (unhedged / total) * 100.0 if total else 0.0
        revenue = total * float(row["customer_tariff"])
        hedged_cost = hedged * float(row["hedge_price"])
        unhedged_cost = unhedged * latest_market_price
        gross_margin = revenue - (hedged_cost + unhedged_cost)
        gross_margin_per_mwh = gross_margin / total if total else 0.0
        hedge_mtm = (latest_market_price - float(row["hedge_price"])) * hedged
        target_min = float(row["target_min_hedge_pct"])
        target_max = float(row["target_max_hedge_pct"])
        hedge_gap_mwh = max(0.0, ((target_min - float(row["hedge_coverage_pct"])) / 100.0) * total)

        breaches = []
        if float(row["hedge_coverage_pct"]) < target_min:
            breaches.append("under-hedged")
        if float(row["hedge_coverage_pct"]) > target_max:
            breaches.append("over-hedged")
        if gross_margin_per_mwh < limits["min_margin_per_mwh"]:
            breaches.append("low-margin")
        if unhedged_pct > limits["max_portfolio_unhedged_pct"]:
            breaches.append("high-open-risk")

        row["hedged_volume_mwh"] = round(hedged, 0)
        row["unhedged_volume_mwh"] = round(unhedged, 0)
        row["unhedged_pct"] = round(unhedged_pct, 1)
        row["market_price"] = round(latest_market_price, 2)
        row["revenue"] = round(revenue, 0)
        row["hedged_cost"] = round(hedged_cost, 0)
        row["unhedged_cost"] = round(unhedged_cost, 0)
        row["gross_margin"] = round(gross_margin, 0)
        row["gross_margin_per_mwh"] = round(gross_margin_per_mwh, 2)
        row["hedge_mtm"] = round(hedge_mtm, 0)
        row["hedge_gap_mwh"] = round(hedge_gap_mwh, 0)
        row["status"] = "Profit" if gross_margin >= 0 else "Loss"
        row["risk_status"] = "Breach" if breaches else "Within Limits"
        row["breach_reason"] = ", ".join(breaches) if breaches else "none"

    return {"books": books, "limits": limits}


def _format_currency_millions(value: float) -> str:
    return f"£{value / 1_000_000:.2f}m"


def _format_pct(value: float) -> str:
    return f"{value:.1f}%"


def _format_signed_pct(value: float) -> str:
    return f"{value:+.1f}%"


def _book_segment(book: str) -> str:
    if "Residential" in book:
        return "Residential"
    if "SME" in book:
        return "SME"
    if "Industrial" in book:
        return "Industrial"
    if "Public Sector" in book:
        return "Public Sector"
    if "EV" in book:
        return "EV"
    return "Other"


def _build_dashboard_context(
    bucket: str,
    table: str,
    daily_rows: List[Dict[str, str]],
    intraday_rows: List[Dict[str, str]],
):
    if not daily_rows:
        raise RuntimeError("No daily rows returned; cannot build dashboard")

    dates = [r["date"] for r in daily_rows]
    total_demand = [_to_float(r["total_demand_mw"]) for r in daily_rows]
    avg_sell = [_to_float(r["avg_system_sell_price"]) for r in daily_rows]
    avg_buy = [_to_float(r["avg_system_buy_price"]) for r in daily_rows]
    peaks = [_to_float(r["peak_demand_mw"]) for r in daily_rows]
    settlements = [_to_int(r["settlement_rows"]) for r in daily_rows]

    latest = daily_rows[-1]
    latest_date = latest["date"]
    latest_total = _to_float(latest["total_demand_mw"])
    latest_peak = _to_float(latest["peak_demand_mw"])
    latest_avg_sell = _to_float(latest["avg_system_sell_price"])
    latest_settlements = _to_int(latest["settlement_rows"])

    intraday_period = [_to_int(r["settlement_period"]) for r in intraday_rows]
    intraday_demand = [_to_float(r["demand_mw"]) for r in intraday_rows]
    intraday_sell = [_to_float(r["system_sell_price"]) for r in intraday_rows]

    utility_model = _build_fictional_portfolio(latest_avg_sell)
    utility_books = utility_model["books"]
    utility_limits = utility_model["limits"]
    total_margin = sum(b["gross_margin"] for b in utility_books)
    loss_books = sum(1 for b in utility_books if b["status"] == "Loss")
    profit_books = len(utility_books) - loss_books
    breach_books = sum(1 for b in utility_books if b["risk_status"] == "Breach")
    total_shortfall = sum(b["hedge_gap_mwh"] for b in utility_books)
    total_volume = sum(b["volume_mwh"] for b in utility_books)
    total_unhedged = sum(b["unhedged_volume_mwh"] for b in utility_books)
    weighted_coverage = (1 - (total_unhedged / total_volume)) * 100 if total_volume else 0.0
    open_exposure_pct = (total_unhedged / total_volume) * 100 if total_volume else 0.0

    recent_sell = avg_sell[-7:] if len(avg_sell) >= 7 else avg_sell
    recent_peaks = peaks[-7:] if len(peaks) >= 7 else peaks
    recent_sell_avg = sum(recent_sell) / len(recent_sell) if recent_sell else 0.0
    recent_peak_avg = sum(recent_peaks) / len(recent_peaks) if recent_peaks else 0.0
    market_price_vs_7d = (
        ((latest_avg_sell - recent_sell_avg) / recent_sell_avg) * 100 if recent_sell_avg else 0.0
    )
    peak_vs_7d = (
        ((latest_peak - recent_peak_avg) / recent_peak_avg) * 100 if recent_peak_avg else 0.0
    )

    market_payload = {
        "dates": dates,
        "totalDemand": total_demand,
        "avgSell": avg_sell,
        "avgBuy": avg_buy,
        "peaks": peaks,
        "settlementRows": settlements,
        "intradayPeriod": intraday_period,
        "intradayDemand": intraday_demand,
        "intradaySell": intraday_sell,
    }
    utility_payload = {
        "books": utility_books,
        "labels": [b["book"] for b in utility_books],
        "margins": [b["gross_margin"] for b in utility_books],
        "marginPerMwh": [b["gross_margin_per_mwh"] for b in utility_books],
        "hedgedVolume": [b["hedged_volume_mwh"] for b in utility_books],
        "unhedgedVolume": [b["unhedged_volume_mwh"] for b in utility_books],
        "coveragePct": [b["hedge_coverage_pct"] for b in utility_books],
        "targetMin": [b["target_min_hedge_pct"] for b in utility_books],
        "targetMax": [b["target_max_hedge_pct"] for b in utility_books],
        "hedgeGapMwh": [b["hedge_gap_mwh"] for b in utility_books],
        "riskStatus": [b["risk_status"] for b in utility_books],
        "limits": utility_limits,
    }

    generated_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sorted_books = sorted(utility_books, key=lambda b: b["gross_margin"], reverse=True)
    sorted_exceptions = sorted(
        utility_books,
        key=lambda b: (
            0 if b["risk_status"] == "Breach" else 1,
            b["gross_margin_per_mwh"],
            -b["unhedged_pct"],
        ),
    )

    dashboard_model = {
        "metadata": {
            "asOf": generated_utc,
            "latestDate": latest_date,
            "region": "GB",
            "scenario": "Conservative Hedge Posture",
            "table": table,
            "bucket": bucket,
            "dataFreshness": f"Daily snapshot ({latest_settlements}/48 settlement rows)",
        },
        "navItems": ["Overview", "Portfolio Risk", "Market Context", "Data Quality"],
        "overview": {
            "alerts": [
                {
                    "label": "Books Breaching Limits",
                    "value": str(breach_books),
                    "detail": (
                        "One or more books are outside policy limits."
                        if breach_books
                        else "All books are currently within policy limits."
                    ),
                    "status": "investigate" if breach_books else "healthy",
                },
                {
                    "label": "Loss-Making Books",
                    "value": str(loss_books),
                    "detail": (
                        "Current portfolio view shows no loss-making books."
                        if loss_books == 0
                        else "At least one book is currently loss-making."
                    ),
                    "status": "healthy" if loss_books == 0 else "investigate",
                },
                {
                    "label": "Settlement Completeness",
                    "value": f"{latest_settlements}/48",
                    "detail": (
                        "All settlement periods were captured for the latest date."
                        if latest_settlements == 48
                        else "One or more settlement periods are missing in the latest snapshot."
                    ),
                    "status": "healthy" if latest_settlements == 48 else "watch",
                },
                {
                    "label": "Open Exposure Above Limit",
                    "value": "Yes" if open_exposure_pct > utility_limits["max_portfolio_unhedged_pct"] else "No",
                    "detail": (
                        "Portfolio open exposure is above the control threshold."
                        if open_exposure_pct > utility_limits["max_portfolio_unhedged_pct"]
                        else f"Portfolio open exposure remains below the {utility_limits['max_portfolio_unhedged_pct']:.0f}% control threshold."
                    ),
                    "status": (
                        "investigate"
                        if open_exposure_pct > utility_limits["max_portfolio_unhedged_pct"]
                        else "healthy"
                    ),
                },
            ],
            "summaryCards": [
                {
                    "label": "Portfolio Gross Margin",
                    "value": _format_currency_millions(total_margin),
                    "trend": f"{profit_books}/{len(utility_books)} books profitable",
                    "detail": "Driven by strong hedge carry on the fixed and industrial books.",
                },
                {
                    "label": "Open Exposure",
                    "value": _format_pct(open_exposure_pct),
                    "trend": f"Limit {utility_limits['max_portfolio_unhedged_pct']:.0f}%",
                    "detail": "Comfortable headroom against the portfolio open exposure ceiling.",
                },
                {
                    "label": "Weighted Hedge Cover",
                    "value": _format_pct(weighted_coverage),
                    "trend": "Conservative posture retained",
                    "detail": "High forward coverage across the major books.",
                },
                {
                    "label": "Market Price vs 7D Avg",
                    "value": _format_signed_pct(market_price_vs_7d),
                    "trend": f"Latest £{latest_avg_sell:.2f}/MWh",
                    "detail": "Spot market remains above the trailing 7-day average.",
                },
                {
                    "label": "Peak Demand vs 7D Avg",
                    "value": _format_signed_pct(peak_vs_7d),
                    "trend": f"Latest {latest_peak:,.0f} MW",
                    "detail": "Demand is firm but still within a plausible seasonal band.",
                },
                {
                    "label": "Data Freshness",
                    "value": latest_date,
                    "trend": "Athena daily snapshot",
                    "detail": "Dashboard values are generated from the latest curated dataset date.",
                },
            ],
            "pnlDrivers": [
                {
                    "label": b["book"],
                    "value": float(b["gross_margin"]),
                    "tone": "loss" if b["gross_margin"] < 0 else "default",
                }
                for b in sorted_books
            ],
            "coveragePoints": [
                {
                    "label": b["book"],
                    "value": float(b["hedge_coverage_pct"]),
                    "targetMin": float(b["target_min_hedge_pct"]),
                    "targetMax": float(b["target_max_hedge_pct"]),
                    "flagged": b["risk_status"] == "Breach",
                }
                for b in utility_books
            ],
            "exposurePoints": [
                {
                    "label": b["book"],
                    "hedged": float(b["hedge_coverage_pct"]),
                    "open": round(100.0 - float(b["hedge_coverage_pct"]), 1),
                }
                for b in utility_books
            ],
            "exceptionRows": [
                {
                    "book": b["book"],
                    "segment": _book_segment(b["book"]),
                    "grossMargin": f"£{b['gross_margin'] / 1_000_000:.2f}m",
                    "marginPerMwh": f"£{b['gross_margin_per_mwh']:.2f}",
                    "hedgeCover": f"{b['hedge_coverage_pct']:.0f}%",
                    "targetBand": f"{b['target_min_hedge_pct']:.0f}% - {b['target_max_hedge_pct']:.0f}%",
                    "openExposure": f"{b['unhedged_pct']:.0f}%",
                    "riskStatus": b["risk_status"].lower(),
                    "breachReason": b["breach_reason"],
                    "tone": (
                        "critical"
                        if b["risk_status"] == "Breach"
                        else (
                            "warning"
                            if (
                                b["gross_margin_per_mwh"] < utility_limits["min_margin_per_mwh"] + 6
                                or b["unhedged_pct"] > utility_limits["max_portfolio_unhedged_pct"] * 0.65
                            )
                            else None
                        )
                    ),
                }
                for b in sorted_exceptions
            ],
            "marketPanels": [
                {
                    "title": "Price Trend",
                    "legend": [
                        {"label": "Sell", "tone": "teal"},
                        {"label": "Buy", "tone": "amber"},
                    ],
                    "note": f"Latest sell price £{latest_avg_sell:.2f}/MWh against trailing market range.",
                    "series": [
                        {"label": "Sell", "tone": "teal", "values": avg_sell[-14:]},
                        {"label": "Buy", "tone": "amber", "values": avg_buy[-14:]},
                    ],
                },
                {
                    "title": "Demand Trend",
                    "legend": [{"label": "Demand", "tone": "blue"}],
                    "note": "Demand trend provides context for current market conditions without driving the main page.",
                    "series": [
                        {"label": "Demand", "tone": "blue", "values": total_demand[-14:]},
                    ],
                },
                {
                    "title": "Intraday Profile Preview",
                    "legend": [
                        {"label": "Demand", "tone": "blue"},
                        {"label": "Price", "tone": "amber"},
                    ],
                    "note": f"Latest-day intraday profile for {latest_date}.",
                    "series": [
                        {"label": "Demand", "tone": "blue", "values": intraday_demand},
                        {"label": "Price", "tone": "amber", "values": intraday_sell},
                    ],
                },
            ],
        },
    }

    return {
        "latest_date": latest_date,
        "latest_total": latest_total,
        "latest_peak": latest_peak,
        "latest_avg_sell": latest_avg_sell,
        "latest_settlements": latest_settlements,
        "utility_books": utility_books,
        "utility_limits": utility_limits,
        "total_margin": total_margin,
        "loss_books": loss_books,
        "profit_books": profit_books,
        "breach_books": breach_books,
        "total_shortfall": total_shortfall,
        "weighted_coverage": weighted_coverage,
        "market_payload": market_payload,
        "utility_payload": utility_payload,
        "generated_utc": generated_utc,
        "dashboard_model": dashboard_model,
    }


def _render_html(
    output_file: pathlib.Path,
    bucket: str,
    table: str,
    context: Dict[str, object],
):
    latest_date = str(context["latest_date"])
    latest_total = float(context["latest_total"])
    latest_peak = float(context["latest_peak"])
    latest_avg_sell = float(context["latest_avg_sell"])
    utility_books = list(context["utility_books"])
    utility_limits = dict(context["utility_limits"])
    total_margin = float(context["total_margin"])
    loss_books = int(context["loss_books"])
    profit_books = int(context["profit_books"])
    breach_books = int(context["breach_books"])
    total_shortfall = float(context["total_shortfall"])
    weighted_coverage = float(context["weighted_coverage"])
    market_payload = dict(context["market_payload"])
    utility_payload = dict(context["utility_payload"])
    generated_utc = str(context["generated_utc"])
    market_json = json.dumps(market_payload)
    utility_json = json.dumps(utility_payload)

    table_rows_html = "\n".join(
        f"""
        <tr>
          <td>{b['book']}</td>
          <td>{b['volume_mwh']:,.0f}</td>
          <td>{b['hedge_coverage_pct']:.0f}%</td>
          <td>{b['target_min_hedge_pct']:.0f}% - {b['target_max_hedge_pct']:.0f}%</td>
          <td>{b['hedge_gap_mwh']:,.0f}</td>
          <td>£{b['hedge_price']:.2f}</td>
          <td>£{b['market_price']:.2f}</td>
          <td>£{b['revenue']:,.0f}</td>
          <td>£{b['gross_margin_per_mwh']:.2f}</td>
          <td>£{b['gross_margin']:,.0f}</td>
          <td><span class="pill {'profit' if b['status'] == 'Profit' else 'loss'}">{b['status']}</span></td>
          <td><span class="pill {'loss' if b['risk_status'] == 'Breach' else 'profit'}">{b['risk_status']}</span></td>
          <td>{b['breach_reason']}</td>
        </tr>
        """
        for b in utility_books
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Energy Market Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #eef3f7;
      --card: #ffffff;
      --ink: #12212f;
      --muted: #5f7285;
      --primary: #0f6ab8;
      --accent: #0aa57f;
      --warn: #d97a00;
      --danger: #c23b38;
      --border: #d9e4ee;
      --shadow: 0 10px 28px rgba(12, 34, 56, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 500px at 110% -10%, #c6ddf1 0%, transparent 60%),
        radial-gradient(800px 500px at -10% -20%, #e0f2ee 0%, transparent 60%),
        var(--bg);
    }}
    .container {{
      width: min(97vw, 1820px);
      margin: 0 auto;
      padding: clamp(16px, 1.4vw, 28px) clamp(12px, 1.6vw, 28px) 44px;
    }}
    .hero {{
      background: linear-gradient(125deg, #0d4f88 0%, #0f6ab8 55%, #1f86d5 100%);
      color: #fff;
      border-radius: 18px;
      padding: clamp(18px, 1.8vw, 30px) clamp(16px, 2vw, 30px);
      box-shadow: var(--shadow);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: clamp(24px, 2.1vw, 34px);
      letter-spacing: 0.2px;
    }}
    .hero p {{
      margin: 0;
      color: #d6e8f8;
      font-size: clamp(13px, 1vw, 15px);
    }}
    .meta {{
      margin-top: 14px;
      font-size: clamp(12px, 0.95vw, 13px);
      color: #e3f0fb;
      display: flex;
      gap: clamp(10px, 1vw, 16px);
      flex-wrap: wrap;
    }}
    .tabs {{
      margin-top: 16px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .tab-btn {{
      border: 1px solid #0e5d9f;
      background: rgba(255,255,255,0.15);
      color: #fff;
      padding: 9px 14px;
      border-radius: 10px;
      cursor: pointer;
      font-weight: 600;
    }}
    .tab-btn.active {{
      background: #fff;
      color: #0d4f88;
    }}
    .tab-content {{ display: none; margin-top: 16px; }}
    .tab-content.active {{ display: block; }}
    .grid-kpi {{
      margin-top: 4px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: clamp(10px, 1vw, 16px);
    }}
    .kpi {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: clamp(12px, 1vw, 18px) clamp(12px, 1.1vw, 16px);
      box-shadow: var(--shadow);
      min-height: 88px;
    }}
    .kpi .label {{
      font-size: clamp(11px, 0.8vw, 12px);
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}
    .kpi .value {{
      margin-top: 6px;
      font-weight: 700;
      font-size: clamp(22px, 1.45vw, 30px);
      color: var(--ink);
    }}
    .charts {{
      margin-top: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));
      gap: clamp(10px, 1vw, 16px);
    }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 14px 14px 8px;
      min-height: clamp(310px, 34vh, 500px);
    }}
    .panel h3 {{
      margin: 0 0 10px;
      font-size: clamp(14px, 1vw, 16px);
      color: #1a3045;
    }}
    .full {{
      grid-column: 1 / -1;
      min-height: clamp(360px, 42vh, 620px);
    }}
    canvas {{ width: 100% !important; height: clamp(250px, 31vh, 430px) !important; }}
    .full canvas {{ height: clamp(310px, 39vh, 570px) !important; }}
    .table-wrap {{
      margin-top: 14px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      overflow: auto;
      max-height: clamp(260px, 35vh, 560px);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: clamp(12px, 0.8vw, 13px);
      min-width: 980px;
    }}
    th, td {{
      padding: 10px 11px;
      border-bottom: 1px solid #e7eef5;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{
      background: #f6f9fc;
      color: #2a4156;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .pill {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 11px;
      letter-spacing: 0.3px;
      text-transform: uppercase;
    }}
    .pill.profit {{ background: #dff4ea; color: #0f7f55; }}
    .pill.loss {{ background: #fde8e8; color: #b2322f; }}
    @media (max-width: 1180px) {{
      .charts {{ grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); }}
      .full {{ min-height: clamp(320px, 45vh, 520px); }}
    }}
    @media (max-width: 900px) {{
      .container {{ width: min(99vw, 1600px); padding-inline: 10px; }}
      .grid-kpi {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
      .charts {{ grid-template-columns: 1fr; }}
      .full {{ grid-column: span 1; }}
    }}
    @media (max-width: 640px) {{
      .hero h1 {{ font-size: 22px; }}
      .meta {{ flex-direction: column; gap: 6px; }}
      .tabs {{ gap: 6px; }}
      .tab-btn {{ width: 100%; text-align: center; }}
      canvas {{ height: 240px !important; }}
      .full canvas {{ height: 280px !important; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>UK Electricity Analytics & Portfolio Dashboard</h1>
      <p>Ingested from Elexon -> S3 raw -> Glue curated parquet -> Athena analytics</p>
      <div class="meta">
        <span><strong>Data table:</strong> {table}</span>
        <span><strong>S3 bucket:</strong> {bucket}</span>
        <span><strong>Generated:</strong> {generated_utc}</span>
      </div>
      <div class="tabs">
        <button class="tab-btn active" data-tab="market-tab">Market Analytics</button>
        <button class="tab-btn" data-tab="utility-tab">NorthGrid Utilities (Fictional)</button>
      </div>
    </section>

    <section id="market-tab" class="tab-content active">
      <section class="grid-kpi">
        <div class="kpi"><div class="label">Latest Date</div><div class="value">{latest_date}</div></div>
        <div class="kpi"><div class="label">Total Demand (MWh)</div><div class="value">{latest_total:,.0f}</div></div>
        <div class="kpi"><div class="label">Peak Demand (MW)</div><div class="value">{latest_peak:,.0f}</div></div>
        <div class="kpi"><div class="label">Avg Sell Price</div><div class="value">£{latest_avg_sell:,.2f}</div></div>
      </section>

      <section class="charts">
        <article class="panel">
          <h3>Daily Total Demand</h3>
          <canvas id="demandChart"></canvas>
        </article>
        <article class="panel">
          <h3>Daily Average System Prices</h3>
          <canvas id="priceChart"></canvas>
        </article>
        <article class="panel">
          <h3>Settlement Rows by Day (target 48)</h3>
          <canvas id="qualityChart"></canvas>
        </article>
        <article class="panel">
          <h3>Daily Peak Demand</h3>
          <canvas id="peakChart"></canvas>
        </article>
        <article class="panel full">
          <h3>Intraday Profile ({latest_date})</h3>
          <canvas id="intradayChart"></canvas>
        </article>
      </section>
    </section>

    <section id="utility-tab" class="tab-content">
      <section class="grid-kpi">
        <div class="kpi"><div class="label">Portfolio Margin</div><div class="value">£{total_margin:,.0f}</div></div>
        <div class="kpi"><div class="label">Books in Profit</div><div class="value">{profit_books}</div></div>
        <div class="kpi"><div class="label">Books in Loss</div><div class="value">{loss_books}</div></div>
        <div class="kpi"><div class="label">Weighted Hedge Cover</div><div class="value">{weighted_coverage:.1f}%</div></div>
        <div class="kpi"><div class="label">Books Breaching Limits</div><div class="value">{breach_books}</div></div>
        <div class="kpi"><div class="label">Hedge Shortfall to Min Target</div><div class="value">{total_shortfall:,.0f} MWh</div></div>
        <div class="kpi"><div class="label">Open Exposure Limit</div><div class="value">{utility_limits["max_portfolio_unhedged_pct"]:.0f}%</div></div>
        <div class="kpi"><div class="label">Min Margin Target</div><div class="value">£{utility_limits["min_margin_per_mwh"]:.2f}/MWh</div></div>
      </section>

      <section class="charts">
        <article class="panel">
          <h3>Gross Margin by Book</h3>
          <canvas id="marginChart"></canvas>
        </article>
        <article class="panel">
          <h3>Hedged vs Unhedged Volume</h3>
          <canvas id="hedgeChart"></canvas>
        </article>
        <article class="panel full">
          <h3>Hedge Coverage vs Target Band</h3>
          <canvas id="coverageChart"></canvas>
        </article>
      </section>

      <section class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Book</th>
              <th>Volume (MWh)</th>
              <th>Hedge Cover</th>
              <th>Target Hedge Band</th>
              <th>Shortfall to Min (MWh)</th>
              <th>Hedge Price</th>
              <th>Market Price</th>
              <th>Revenue</th>
              <th>Margin / MWh</th>
              <th>Gross Margin</th>
              <th>Status</th>
              <th>Risk Status</th>
              <th>Breach Reason</th>
            </tr>
          </thead>
          <tbody>
            {table_rows_html}
          </tbody>
        </table>
      </section>
    </section>
  </div>

  <script>
    const market = {market_json};
    const utility = {utility_json};
    const getCharts = () => {{
      if (Chart.instances instanceof Map) {{
        return Array.from(Chart.instances.values());
      }}
      return Object.values(Chart.instances || {{}});
    }};
    const reflowCharts = () => {{
      getCharts().forEach((chart) => chart.resize());
    }};

    document.querySelectorAll('.tab-btn').forEach((btn) => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach((c) => c.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab).classList.add('active');
        requestAnimationFrame(() => requestAnimationFrame(reflowCharts));
      }});
    }});
    window.addEventListener('resize', reflowCharts);

    const common = {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ labels: {{ boxWidth: 12, usePointStyle: true }} }}
      }},
      scales: {{
        x: {{ ticks: {{ maxRotation: 45, minRotation: 45 }} }},
        y: {{ grid: {{ color: 'rgba(12,34,56,0.08)' }} }}
      }}
    }};

    new Chart(document.getElementById('demandChart'), {{
      type: 'line',
      data: {{
        labels: market.dates,
        datasets: [{{
          label: 'Total Demand (MWh)',
          data: market.totalDemand,
          borderColor: '#0f6ab8',
          backgroundColor: 'rgba(15,106,184,0.15)',
          fill: true,
          tension: 0.25,
          pointRadius: 1.8
        }}]
      }},
      options: common
    }});

    new Chart(document.getElementById('priceChart'), {{
      type: 'line',
      data: {{
        labels: market.dates,
        datasets: [
          {{
            label: 'Avg System Sell Price',
            data: market.avgSell,
            borderColor: '#0aa57f',
            backgroundColor: 'rgba(10,165,127,0.10)',
            tension: 0.25,
            pointRadius: 1.5
          }},
          {{
            label: 'Avg System Buy Price',
            data: market.avgBuy,
            borderColor: '#d97a00',
            backgroundColor: 'rgba(217,122,0,0.10)',
            tension: 0.25,
            pointRadius: 1.5
          }}
        ]
      }},
      options: common
    }});

    new Chart(document.getElementById('qualityChart'), {{
      type: 'bar',
      data: {{
        labels: market.dates,
        datasets: [
          {{
            label: 'Rows Captured',
            data: market.settlementRows,
            backgroundColor: 'rgba(15,106,184,0.75)',
            borderRadius: 3
          }},
          {{
            label: 'Expected 48',
            data: market.settlementRows.map(_ => 48),
            type: 'line',
            borderColor: '#a22b2b',
            pointRadius: 0,
            borderWidth: 1.5
          }}
        ]
      }},
      options: common
    }});

    new Chart(document.getElementById('peakChart'), {{
      type: 'bar',
      data: {{
        labels: market.dates,
        datasets: [{{
          label: 'Peak Demand (MW)',
          data: market.peaks,
          backgroundColor: 'rgba(10,165,127,0.75)',
          borderRadius: 3
        }}]
      }},
      options: common
    }});

    new Chart(document.getElementById('intradayChart'), {{
      type: 'line',
      data: {{
        labels: market.intradayPeriod,
        datasets: [
          {{
            label: 'Demand (MW)',
            data: market.intradayDemand,
            borderColor: '#0f6ab8',
            backgroundColor: 'rgba(15,106,184,0.15)',
            fill: true,
            tension: 0.2,
            yAxisID: 'y'
          }},
          {{
            label: 'System Sell Price',
            data: market.intradaySell,
            borderColor: '#d97a00',
            backgroundColor: 'rgba(217,122,0,0.1)',
            fill: false,
            tension: 0.2,
            yAxisID: 'y1'
          }}
        ]
      }},
      options: {{
        ...common,
        scales: {{
          x: {{ title: {{ display: true, text: 'Settlement Period' }} }},
          y: {{ position: 'left', title: {{ display: true, text: 'Demand (MW)' }} }},
          y1: {{
            position: 'right',
            title: {{ display: true, text: 'Price (£/MWh)' }},
            grid: {{ drawOnChartArea: false }}
          }}
        }}
      }}
    }});

    new Chart(document.getElementById('marginChart'), {{
      type: 'bar',
      data: {{
        labels: utility.labels,
        datasets: [{{
          label: 'Gross Margin (£)',
          data: utility.margins,
          borderWidth: 1,
          borderColor: utility.margins.map(v => v >= 0 ? '#0f7f55' : '#b2322f'),
          backgroundColor: utility.margins.map(v => v >= 0 ? 'rgba(15,127,85,0.70)' : 'rgba(178,50,47,0.70)'),
          borderRadius: 4
        }}]
      }},
      options: {{
        ...common,
        scales: {{
          x: {{ ticks: {{ maxRotation: 0, minRotation: 0 }} }},
          y: {{ grid: {{ color: 'rgba(12,34,56,0.08)' }} }}
        }}
      }}
    }});

    new Chart(document.getElementById('hedgeChart'), {{
      type: 'bar',
      data: {{
        labels: utility.labels,
        datasets: [
          {{
            label: 'Hedged Volume (MWh)',
            data: utility.hedgedVolume,
            backgroundColor: 'rgba(10,165,127,0.75)',
            borderRadius: 4
          }},
          {{
            label: 'Unhedged Volume (MWh)',
            data: utility.unhedgedVolume,
            backgroundColor: 'rgba(217,122,0,0.75)',
            borderRadius: 4
          }}
        ]
      }},
      options: {{
        ...common,
        scales: {{
          x: {{ stacked: true, ticks: {{ maxRotation: 0, minRotation: 0 }} }},
          y: {{ stacked: true, grid: {{ color: 'rgba(12,34,56,0.08)' }} }}
        }}
      }}
    }});

    new Chart(document.getElementById('coverageChart'), {{
      type: 'bar',
      data: {{
        labels: utility.labels,
        datasets: [
          {{
            label: 'Actual Hedge Cover (%)',
            data: utility.coveragePct,
            backgroundColor: utility.riskStatus.map(v => v === 'Breach' ? 'rgba(178,50,47,0.70)' : 'rgba(15,127,85,0.70)'),
            borderColor: utility.riskStatus.map(v => v === 'Breach' ? '#b2322f' : '#0f7f55'),
            borderWidth: 1,
            borderRadius: 4
          }},
          {{
            label: 'Target Min (%)',
            data: utility.targetMin,
            type: 'line',
            borderColor: '#0f6ab8',
            borderDash: [6, 5],
            pointRadius: 0,
            borderWidth: 2
          }},
          {{
            label: 'Target Max (%)',
            data: utility.targetMax,
            type: 'line',
            borderColor: '#d97a00',
            borderDash: [6, 5],
            pointRadius: 0,
            borderWidth: 2
          }}
        ]
      }},
      options: {{
        ...common,
        scales: {{
          x: {{ ticks: {{ maxRotation: 0, minRotation: 0 }} }},
          y: {{
            suggestedMin: 0,
            suggestedMax: 100,
            title: {{ display: true, text: 'Hedge Coverage (%)' }},
            grid: {{ color: 'rgba(12,34,56,0.08)' }}
          }}
        }}
      }}
    }});
    requestAnimationFrame(reflowCharts);
  </script>
</body>
</html>
"""

    output_file.write_text(html, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Generate dashboard HTML from Athena data")
    parser.add_argument("--region", default="eu-west-2")
    parser.add_argument("--database", default="energy_market_lake")
    parser.add_argument("--table", default="curated_dataset_electricity")
    parser.add_argument("--output-location", default="")
    parser.add_argument("--output-file", default="")
    parser.add_argument("--output-json", default="")
    args = parser.parse_args()

    bucket = _discover_bucket_name()
    output_location = args.output_location or f"s3://{bucket}/athena-results/"

    daily_query = f"""
    SELECT
      date,
      SUM(demand_mw) AS total_demand_mw,
      AVG(system_sell_price) AS avg_system_sell_price,
      AVG(system_buy_price) AS avg_system_buy_price,
      MAX(demand_mw) AS peak_demand_mw,
      COUNT(*) AS settlement_rows
    FROM {args.table}
    WHERE region = 'gb'
    GROUP BY date
    ORDER BY date
    """

    intraday_query = f"""
    WITH latest AS (
      SELECT MAX(date) AS d
      FROM {args.table}
      WHERE region = 'gb'
    )
    SELECT
      settlement_period,
      demand_mw,
      system_sell_price,
      system_buy_price
    FROM {args.table}
    WHERE region = 'gb'
      AND date = (SELECT d FROM latest)
    ORDER BY settlement_period
    """

    daily_rows = _run_athena_query(
        daily_query, args.database, output_location, args.region
    )
    intraday_rows = _run_athena_query(
        intraday_query, args.database, output_location, args.region
    )

    if args.output_file:
        output_file = pathlib.Path(args.output_file)
    else:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
        output_file = (
            pathlib.Path(__file__).resolve().parents[1]
            / "docs"
            / "evidence"
            / f"dashboard-{stamp}.html"
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    context = _build_dashboard_context(bucket, args.table, daily_rows, intraday_rows)
    _render_html(output_file, bucket, args.table, context)

    if args.output_json:
        output_json = pathlib.Path(args.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(context["dashboard_model"], indent=2),
            encoding="utf-8",
        )

    print(str(output_file))


if __name__ == "__main__":
    main()
