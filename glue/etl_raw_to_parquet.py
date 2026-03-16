"""
Glue ETL job for Elexon raw payloads -> curated partitioned Parquet.
Input layout:
  s3://<bucket>/raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json
  s3://<bucket>/raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json
Output layout:
  s3://<bucket>/curated/dataset=electricity/region=gb/date=YYYY-MM-DD/part-*.parquet
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _extract_date_from_path(path_col):
    return F.regexp_extract(path_col, r"date=(\d{4}-\d{2}-\d{2})", 1)


def _empty_atl_df(spark_session):
    schema = T.StructType(
        [
            T.StructField("region", T.StringType(), False),
            T.StructField("date", T.StringType(), True),
            T.StructField("settlement_period", T.IntegerType(), True),
            T.StructField("start_time_utc", T.TimestampType(), True),
            T.StructField("publish_time_utc", T.StringType(), True),
            T.StructField("demand_mw", T.DoubleType(), True),
        ]
    )
    return spark_session.createDataFrame([], schema)


def _empty_system_prices_df(spark_session):
    schema = T.StructType(
        [
            T.StructField("date", T.StringType(), True),
            T.StructField("settlement_period", T.IntegerType(), True),
            T.StructField("system_sell_price", T.DoubleType(), True),
            T.StructField("system_buy_price", T.DoubleType(), True),
            T.StructField("net_imbalance_volume", T.DoubleType(), True),
            T.StructField("price_created_at_utc", T.StringType(), True),
        ]
    )
    return spark_session.createDataFrame([], schema)


def _read_atl(raw_root, spark_session):
    atl_path = f"{raw_root.rstrip('/')}/source=elexon/dataset=atl/date=*/payload.json"
    atl_raw = spark_session.read.option("multiLine", True).json(atl_path)

    if "data" not in atl_raw.columns:
        return _empty_atl_df(spark_session)

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
            F.col("row.publishTime").cast("string").alias("publish_time_utc"),
            F.col("row.quantity").cast("double").alias("demand_mw"),
        )
        .filter(F.col("date").isNotNull() & F.col("settlement_period").isNotNull())
    )


def _read_system_prices(raw_root, spark_session):
    prices_path = (
        f"{raw_root.rstrip('/')}/source=elexon/dataset=system_prices/date=*/payload.json"
    )
    prices_raw = spark_session.read.option("multiLine", True).json(prices_path)

    if "data" not in prices_raw.columns:
        return _empty_system_prices_df(spark_session)

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


args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_path = args["RAW_PATH"]
curated_path = args["CURATED_PATH"].rstrip("/")

atl_df = _read_atl(raw_path, spark)
prices_df = _read_system_prices(raw_path, spark)

electricity_df = (
    atl_df.alias("atl")
    .join(
        prices_df.alias("prices"),
        on=["date", "settlement_period"],
        how="left",
    )
    .select(
        F.col("atl.region").alias("region"),
        F.col("date"),
        F.col("settlement_period"),
        F.col("atl.start_time_utc").alias("start_time_utc"),
        F.col("atl.publish_time_utc").alias("atl_publish_time_utc"),
        F.col("atl.demand_mw").alias("demand_mw"),
        F.col("prices.system_sell_price").alias("system_sell_price"),
        F.col("prices.system_buy_price").alias("system_buy_price"),
        F.col("prices.net_imbalance_volume").alias("net_imbalance_volume"),
        F.col("prices.price_created_at_utc").alias("price_created_at_utc"),
    )
)

output_path = f"{curated_path}/dataset=electricity"
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    electricity_df.repartition("region", "date")
    .write.mode("overwrite")
    .partitionBy("region", "date")
    .parquet(output_path)
)

print(
    f"Wrote curated electricity parquet to {output_path} "
    f"(rows={electricity_df.count()})"
)
job.commit()
