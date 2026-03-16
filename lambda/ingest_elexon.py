"""
Lambda ingestion for Elexon Insights Solution (UK electricity).
Writes raw JSON to S3 in partition-friendly paths.
"""
import os
from datetime import datetime, timedelta, timezone

import urllib.parse
import urllib.request

S3_BUCKET = os.environ.get("S3_BUCKET")
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30"))
# Insights Solution base URL
ELEXON_BASE_URL = os.environ.get(
    "ELEXON_BASE_URL", "https://data.elexon.co.uk/bmrs/api/v1"
)

# Endpoints wired for this project (UK Elexon):
# - Demand by bidding zone (GSP group proxy): /datasets/ATL
# - System prices (includes SBP/SSP): /balancing/settlement/system-prices/{settlementDate}

ATL_ENDPOINT = f"{ELEXON_BASE_URL}/datasets/ATL"
SYSTEM_PRICES_ENDPOINT = f"{ELEXON_BASE_URL}/balancing/settlement/system-prices"

# ENTSO-E (EU electricity) - requires security token
ENTSOE_BASE_URL = os.environ.get("ENTSOE_BASE_URL", "https://web-api.tp.entsoe.eu/api")
ENTSOE_TOKEN = os.environ.get("ENTSOE_TOKEN")

# ENTSOG (EU gas) - public API
ENTSOG_BASE_URL = os.environ.get("ENTSOG_BASE_URL", "https://transparency.entsog.eu/api/v1")

# Default EIC codes for requested zones (GB, FR, DE-LU, NL)
ENTSOE_ZONE_MAP = {
    "GB": "10YGB----------A",
    "FR": "10YFR-RTE------C",
    "DE": "10Y1001A1001A82H",  # DE-LU bidding zone
    "NL": "10YNL----------L",
}


def fetch_url(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "energy-market-lake/1.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
        return resp.read()


def build_query_url(base_url: str, params: dict) -> str:
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def entsoe_query(params: dict) -> bytes:
    if not ENTSOE_TOKEN:
        raise ValueError("ENTSOE_TOKEN is required for ENTSO-E requests")
    params_with_token = {"securityToken": ENTSOE_TOKEN, **params}
    url = build_query_url(ENTSOE_BASE_URL, params_with_token)
    return fetch_url(url)


def entsog_query(endpoint: str, params: dict) -> bytes:
    base = f"{ENTSOG_BASE_URL}/{endpoint}"
    url = build_query_url(base, params)
    return fetch_url(url)


def handler(event, context):
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is required")

    now = datetime.now(timezone.utc)
    backfill_days = int(os.environ.get("BACKFILL_DAYS", "30"))
    if backfill_days < 1:
        raise ValueError("BACKFILL_DAYS must be >= 1")

    # Lazy import to keep local testing simple
    import boto3  # noqa: WPS433

    s3 = boto3.client("s3")
    s3_keys = []
    warnings = []
    # Backfill window (default 30 days) for UK demand by bidding zone + system prices.
    for day_offset in range(backfill_days):
        day = (now - timedelta(days=day_offset)).date()
        date_str = day.strftime("%Y-%m-%d")
        start = f"{date_str}T00:00:00.000Z"
        end = f"{date_str}T23:59:59.000Z"

        # Demand by bidding zone (GSP group proxy)
        atl_url = (
            f"{ATL_ENDPOINT}?format=json&PublishDateTimeFrom={start}"
            f"&PublishDateTimeTo={end}"
        )

        # System prices (includes SBP/SSP imbalance prices)
        sys_prices_url = f"{SYSTEM_PRICES_ENDPOINT}/{date_str}"

        try:
            atl_payload = fetch_url(atl_url)
            sys_prices_payload = fetch_url(sys_prices_url)

            atl_key = f"raw/source=elexon/dataset=atl/date={date_str}/payload.json"
            sys_key = f"raw/source=elexon/dataset=system_prices/date={date_str}/payload.json"

            s3.put_object(Bucket=S3_BUCKET, Key=atl_key, Body=atl_payload)
            s3.put_object(Bucket=S3_BUCKET, Key=sys_key, Body=sys_prices_payload)
            s3_keys.extend([atl_key, sys_key])
        except Exception as exc:
            warnings.append(
                f"Elexon ingestion failed for {date_str}: {type(exc).__name__}: {exc}"
            )

    # ENTSO-E ingestion (EU electricity) - actual load + day-ahead prices
    entsoe_zones = os.environ.get("ENTSOE_ZONES", "GB,FR,DE,NL").split(",")
    entsoe_zones = [z.strip().upper() for z in entsoe_zones if z.strip()]
    if ENTSOE_TOKEN and entsoe_zones:
        try:
            for day_offset in range(backfill_days):
                day = (now - timedelta(days=day_offset)).date()
                date_str = day.strftime("%Y-%m-%d")
                period_start = day.strftime("%Y%m%d0000")
                period_end = (day + timedelta(days=1)).strftime("%Y%m%d0000")

                for zone in entsoe_zones:
                    eic = ENTSOE_ZONE_MAP.get(zone)
                    if not eic:
                        continue

                    # Actual load (A65) - realised (A16)
                    load_payload = entsoe_query(
                        {
                            "documentType": "A65",
                            "processType": "A16",
                            "outBiddingZone_Domain": eic,
                            "periodStart": period_start,
                            "periodEnd": period_end,
                        }
                    )
                    load_key = (
                        f"raw/source=entsoe/dataset=actual_load/zone={zone.lower()}/"
                        f"date={date_str}/payload.xml"
                    )
                    s3.put_object(Bucket=S3_BUCKET, Key=load_key, Body=load_payload)
                    s3_keys.append(load_key)

                    # Day-ahead prices (A44) - same in/out domain
                    price_payload = entsoe_query(
                        {
                            "documentType": "A44",
                            "in_Domain": eic,
                            "out_Domain": eic,
                            "periodStart": period_start,
                            "periodEnd": period_end,
                        }
                    )
                    price_key = (
                        f"raw/source=entsoe/dataset=day_ahead_prices/zone={zone.lower()}/"
                        f"date={date_str}/payload.xml"
                    )
                    s3.put_object(Bucket=S3_BUCKET, Key=price_key, Body=price_payload)
                    s3_keys.append(price_key)
        except Exception as exc:
            warnings.append(f"ENTSO-E ingestion skipped due to error: {exc}")

    # ENTSOG ingestion (EU gas) - flows + demand proxies
    entsog_point_dirs = os.environ.get("ENTSOG_POINT_DIRECTIONS", "")
    entsog_point_dirs = [p.strip() for p in entsog_point_dirs.split(",") if p.strip()]
    entsog_flow_indicator = os.environ.get("ENTSOG_FLOW_INDICATOR", "Physical Flow")
    entsog_demand_indicator = os.environ.get("ENTSOG_DEMAND_INDICATOR", "Allocation")
    entsog_period_type = os.environ.get("ENTSOG_PERIOD_TYPE", "day")
    entsog_timezone = os.environ.get("ENTSOG_TIMEZONE", "WET")
    entsog_limit = os.environ.get("ENTSOG_LIMIT", "1000")

    if entsog_point_dirs:
        try:
            for day_offset in range(backfill_days):
                day = (now - timedelta(days=day_offset)).date()
                date_str = day.strftime("%Y-%m-%d")

                for pd in entsog_point_dirs:
                    # Flows (operational datas)
                    flow_payload = entsog_query(
                        "operationaldatas",
                        {
                            "pointDirection": pd,
                            "from": date_str,
                            "to": date_str,
                            "indicator": entsog_flow_indicator,
                            "periodType": entsog_period_type,
                            "timeZone": entsog_timezone,
                            "limit": entsog_limit,
                        },
                    )
                    flow_key = (
                        "raw/source=entsog/dataset=gas_flow/"
                        f"point_direction={urllib.parse.quote(pd, safe='')}/"
                        f"date={date_str}/payload.json"
                    )
                    s3.put_object(Bucket=S3_BUCKET, Key=flow_key, Body=flow_payload)
                    s3_keys.append(flow_key)

                    # Demand proxy (aggregated data, balancing zones)
                    demand_payload = entsog_query(
                        "aggregatedData",
                        {
                            "pointDirection": pd,
                            "from": date_str,
                            "to": date_str,
                            "indicator": entsog_demand_indicator,
                            "periodType": entsog_period_type,
                            "timeZone": entsog_timezone,
                            "limit": entsog_limit,
                        },
                    )
                    demand_key = (
                        "raw/source=entsog/dataset=gas_demand/"
                        f"point_direction={urllib.parse.quote(pd, safe='')}/"
                        f"date={date_str}/payload.json"
                    )
                    s3.put_object(Bucket=S3_BUCKET, Key=demand_key, Body=demand_payload)
                    s3_keys.append(demand_key)
        except Exception as exc:
            warnings.append(f"ENTSOG ingestion skipped due to error: {exc}")

    if not s3_keys:
        raise RuntimeError("No datasets were ingested successfully")

    return {
        "status": "ok" if not warnings else "partial",
        "s3_keys": s3_keys,
        "warnings": warnings,
    }


def lambda_handler(event, context):
    return handler(event, context)
