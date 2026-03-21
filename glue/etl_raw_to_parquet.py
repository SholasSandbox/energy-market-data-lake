"""
Glue ETL job for raw electricity payloads -> curated partitioned Parquet.

Input layout:
  s3://<bucket>/raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json
  s3://<bucket>/raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json
  s3://<bucket>/raw/source=entsoe/dataset=actual_load/zone=<zone>/date=YYYY-MM-DD/payload.xml
  s3://<bucket>/raw/source=entsoe/dataset=day_ahead_prices/zone=<zone>/date=YYYY-MM-DD/payload.xml

Output layout:
  s3://<bucket>/curated/dataset=electricity/source=<source>/region=<region>/date=YYYY-MM-DD/part-*.parquet
"""
import datetime as dt
import re
import sys
import xml.etree.ElementTree as ET

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T


ELECTRICITY_SCHEMA = T.StructType(
    [
        T.StructField("source", T.StringType(), False),
        T.StructField("region", T.StringType(), False),
        T.StructField("date", T.StringType(), True),
        T.StructField("settlement_period", T.IntegerType(), True),
        T.StructField("start_time_utc", T.TimestampType(), True),
        T.StructField("demand_mw", T.DoubleType(), True),
        T.StructField("day_ahead_price_eur_mwh", T.DoubleType(), True),
        T.StructField("system_sell_price", T.DoubleType(), True),
        T.StructField("system_buy_price", T.DoubleType(), True),
        T.StructField("net_imbalance_volume", T.DoubleType(), True),
        T.StructField("atl_publish_time_utc", T.StringType(), True),
        T.StructField("price_created_at_utc", T.StringType(), True),
    ]
)

ELEXON_LOAD_SCHEMA = T.StructType(
    [
        T.StructField("region", T.StringType(), False),
        T.StructField("date", T.StringType(), True),
        T.StructField("settlement_period", T.IntegerType(), True),
        T.StructField("start_time_utc", T.TimestampType(), True),
        T.StructField("atl_publish_time_utc", T.StringType(), True),
        T.StructField("demand_mw", T.DoubleType(), True),
    ]
)

ELEXON_PRICE_SCHEMA = T.StructType(
    [
        T.StructField("date", T.StringType(), True),
        T.StructField("settlement_period", T.IntegerType(), True),
        T.StructField("system_sell_price", T.DoubleType(), True),
        T.StructField("system_buy_price", T.DoubleType(), True),
        T.StructField("net_imbalance_volume", T.DoubleType(), True),
        T.StructField("price_created_at_utc", T.StringType(), True),
    ]
)

ENTSOE_LOAD_SCHEMA = T.StructType(
    [
        T.StructField("region", T.StringType(), False),
        T.StructField("date", T.StringType(), True),
        T.StructField("settlement_period", T.IntegerType(), True),
        T.StructField("start_time_utc", T.TimestampType(), True),
        T.StructField("demand_mw", T.DoubleType(), True),
        T.StructField("entsoe_load_created_at_utc", T.StringType(), True),
    ]
)

ENTSOE_PRICE_SCHEMA = T.StructType(
    [
        T.StructField("region", T.StringType(), False),
        T.StructField("date", T.StringType(), True),
        T.StructField("settlement_period", T.IntegerType(), True),
        T.StructField("start_time_utc", T.TimestampType(), True),
        T.StructField("day_ahead_price_eur_mwh", T.DoubleType(), True),
        T.StructField("entsoe_price_created_at_utc", T.StringType(), True),
    ]
)

DURATION_PATTERN = re.compile(r"^PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?$")
ZONE_PATTERN = re.compile(r"zone=([^/]+)")


def _extract_date_from_path(path_col):
    return F.regexp_extract(path_col, r"date=(\d{4}-\d{2}-\d{2})", 1)


def _empty_df(spark_session, schema):
    return spark_session.createDataFrame([], schema)


def _path_exists(spark_session, path: str) -> bool:
    jvm = spark_session._jvm
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    return fs.exists(jpath)


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _child_text(element, local_name: str):
    for child in list(element):
        if _local_name(child.tag) == local_name and child.text:
            return child.text.strip()
    return None


def _find_descendant_text(element, names):
    current = element
    for name in names:
        match = None
        for child in list(current):
            if _local_name(child.tag) == name:
                match = child
                break
        if match is None:
            return None
        current = match
    return current.text.strip() if current.text else None


def _parse_iso_datetime(value: str):
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _parse_duration_minutes(value: str):
    match = DURATION_PATTERN.match(value or "")
    if not match:
        raise ValueError(f"Unsupported ENTSO-E resolution: {value}")
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    total = (hours * 60) + minutes
    if total <= 0:
        raise ValueError(f"Unsupported ENTSO-E resolution: {value}")
    return total


def _safe_int(value: str):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: str):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_region_from_path(path: str):
    match = ZONE_PATTERN.search(path)
    return match.group(1).lower() if match else None


def _read_atl(raw_root, spark_session):
    atl_root = f"{raw_root.rstrip('/')}/source=elexon/dataset=atl"
    if not _path_exists(spark_session, atl_root):
        return _empty_df(spark_session, ELEXON_LOAD_SCHEMA)

    atl_path = f"{atl_root}/date=*/payload.json"
    atl_raw = spark_session.read.option("multiLine", True).json(atl_path)

    if "data" not in atl_raw.columns:
        return _empty_df(spark_session, ELEXON_LOAD_SCHEMA)

    return (
        atl_raw.select(
            F.explode_outer(F.col("data")).alias("row"),
            F.input_file_name().alias("_src"),
        )
        .select(
            F.lit("gb").alias("region"),
            F.coalesce(
                F.col("row.settlementDate").cast("string"),
                _extract_date_from_path(F.col("_src")),
            ).alias("date"),
            F.col("row.settlementPeriod").cast("int").alias("settlement_period"),
            F.to_timestamp(F.col("row.startTime")).alias("start_time_utc"),
            F.col("row.publishTime").cast("string").alias("atl_publish_time_utc"),
            F.col("row.quantity").cast("double").alias("demand_mw"),
        )
        .filter(F.col("date").isNotNull() & F.col("settlement_period").isNotNull())
    )


def _read_system_prices(raw_root, spark_session):
    prices_root = f"{raw_root.rstrip('/')}/source=elexon/dataset=system_prices"
    if not _path_exists(spark_session, prices_root):
        return _empty_df(spark_session, ELEXON_PRICE_SCHEMA)

    prices_path = f"{prices_root}/date=*/payload.json"
    prices_raw = spark_session.read.option("multiLine", True).json(prices_path)

    if "data" not in prices_raw.columns:
        return _empty_df(spark_session, ELEXON_PRICE_SCHEMA)

    return (
        prices_raw.select(
            F.explode_outer(F.col("data")).alias("row"),
            F.input_file_name().alias("_src"),
        )
        .select(
            F.coalesce(
                F.col("row.settlementDate").cast("string"),
                _extract_date_from_path(F.col("_src")),
            ).alias("date"),
            F.col("row.settlementPeriod").cast("int").alias("settlement_period"),
            F.col("row.systemSellPrice").cast("double").alias("system_sell_price"),
            F.col("row.systemBuyPrice").cast("double").alias("system_buy_price"),
            F.col("row.netImbalanceVolume").cast("double").alias("net_imbalance_volume"),
            F.col("row.createdDateTime").cast("string").alias("price_created_at_utc"),
        )
        .filter(F.col("date").isNotNull() & F.col("settlement_period").isNotNull())
    )


def _parse_entsoe_points(xml_text: str, region: str, value_name: str, created_field: str):
    root = ET.fromstring(xml_text)
    created_at = _child_text(root, "createdDateTime")
    rows = []

    for series in root.findall(".//{*}TimeSeries"):
        for period in series.findall("./{*}Period"):
            start_text = _find_descendant_text(period, ["timeInterval", "start"])
            resolution_text = _child_text(period, "resolution")
            if not start_text or not resolution_text:
                continue

            try:
                period_start = _parse_iso_datetime(start_text)
                resolution_minutes = _parse_duration_minutes(resolution_text)
            except ValueError:
                continue

            for point in period.findall("./{*}Point"):
                position = _safe_int(_child_text(point, "position"))
                value = _safe_float(_child_text(point, value_name))
                if position is None or value is None:
                    continue

                start_time = period_start + dt.timedelta(
                    minutes=(position - 1) * resolution_minutes
                )
                rows.append(
                    {
                        "region": region,
                        "date": start_time.date().isoformat(),
                        "settlement_period": position,
                        "start_time_utc": start_time,
                        created_field: created_at,
                        "value": value,
                    }
                )

    return rows


def _read_entsoe_metric(raw_root, dataset_name, value_name, schema, value_column, created_column, spark_session):
    dataset_root = f"{raw_root.rstrip('/')}/source=entsoe/dataset={dataset_name}"
    if not _path_exists(spark_session, dataset_root):
        return _empty_df(spark_session, schema)

    files = spark_session.sparkContext.wholeTextFiles(
        f"{dataset_root}/zone=*/date=*/payload.xml"
    ).collect()

    rows = []
    for path, xml_text in files:
        region = _extract_region_from_path(path)
        if not region:
            continue

        for row in _parse_entsoe_points(xml_text, region, value_name, created_column):
            row[value_column] = row.pop("value")
            rows.append(row)

    if not rows:
        return _empty_df(spark_session, schema)
    return spark_session.createDataFrame(rows, schema=schema)


def _read_elexon_electricity(raw_root, spark_session):
    atl_df = _read_atl(raw_root, spark_session)
    prices_df = _read_system_prices(raw_root, spark_session)

    if atl_df.rdd.isEmpty() and prices_df.rdd.isEmpty():
        return _empty_df(spark_session, ELECTRICITY_SCHEMA)

    return (
        atl_df.alias("atl")
        .join(
            prices_df.alias("prices"),
            on=["date", "settlement_period"],
            how="left",
        )
        .select(
            F.lit("elexon").alias("source"),
            F.col("atl.region").alias("region"),
            F.col("date"),
            F.col("settlement_period"),
            F.col("atl.start_time_utc").alias("start_time_utc"),
            F.col("atl.demand_mw").alias("demand_mw"),
            F.lit(None).cast("double").alias("day_ahead_price_eur_mwh"),
            F.col("prices.system_sell_price").alias("system_sell_price"),
            F.col("prices.system_buy_price").alias("system_buy_price"),
            F.col("prices.net_imbalance_volume").alias("net_imbalance_volume"),
            F.col("atl.atl_publish_time_utc").alias("atl_publish_time_utc"),
            F.col("prices.price_created_at_utc").alias("price_created_at_utc"),
        )
    )


def _read_entsoe_electricity(raw_root, spark_session):
    load_df = _read_entsoe_metric(
        raw_root,
        "actual_load",
        "quantity",
        ENTSOE_LOAD_SCHEMA,
        "demand_mw",
        "entsoe_load_created_at_utc",
        spark_session,
    )
    price_df = _read_entsoe_metric(
        raw_root,
        "day_ahead_prices",
        "price.amount",
        ENTSOE_PRICE_SCHEMA,
        "day_ahead_price_eur_mwh",
        "entsoe_price_created_at_utc",
        spark_session,
    )

    if load_df.rdd.isEmpty() and price_df.rdd.isEmpty():
        return _empty_df(spark_session, ELECTRICITY_SCHEMA)

    return (
        load_df.alias("load")
        .join(
            price_df.alias("price"),
            on=["region", "date", "settlement_period", "start_time_utc"],
            how="full",
        )
        .select(
            F.lit("entsoe").alias("source"),
            F.coalesce(F.col("load.region"), F.col("price.region")).alias("region"),
            F.coalesce(F.col("load.date"), F.col("price.date")).alias("date"),
            F.coalesce(
                F.col("load.settlement_period"),
                F.col("price.settlement_period"),
            ).alias("settlement_period"),
            F.coalesce(
                F.col("load.start_time_utc"),
                F.col("price.start_time_utc"),
            ).alias("start_time_utc"),
            F.col("load.demand_mw").alias("demand_mw"),
            F.col("price.day_ahead_price_eur_mwh").alias("day_ahead_price_eur_mwh"),
            F.lit(None).cast("double").alias("system_sell_price"),
            F.lit(None).cast("double").alias("system_buy_price"),
            F.lit(None).cast("double").alias("net_imbalance_volume"),
            F.col("load.entsoe_load_created_at_utc").alias("atl_publish_time_utc"),
            F.col("price.entsoe_price_created_at_utc").alias("price_created_at_utc"),
        )
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_path = args["RAW_PATH"]
curated_path = args["CURATED_PATH"].rstrip("/")

elexon_df = _read_elexon_electricity(raw_path, spark)
entsoe_df = _read_entsoe_electricity(raw_path, spark)
electricity_df = elexon_df.unionByName(entsoe_df)

output_path = f"{curated_path}/dataset=electricity"
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    electricity_df.repartition("source", "region", "date")
    .write.mode("overwrite")
    .partitionBy("source", "region", "date")
    .parquet(output_path)
)

print(
    f"Wrote curated electricity parquet to {output_path} "
    f"(rows={electricity_df.count()})"
)
job.commit()
