"""Initial load: CSVs on HDFS (/landing) -> Avro on HDFS, for orders and weather.

Every dataset goes through the same pipeline:
    read CSV -> (optional) transform -> write Avro (1 file, snappy) -> read back and compare row counts

A dataset is a row in the registry below: where the CSV is, where the Avro goes,
and optionally an explicit schema and/or a transform. To add a dataset, add a row.

Input (uploaded by upload_to_hdfs.sh):
  /landing/orders/<restaurant>-orders.csv
  /landing/weather/<WEATHER_FILE>

Config (from the spark-integration ConfigMap):
  HDFS_URL, ORDERS_PATH (e.g. /historic/orders/restaurant-*), WEATHER_HISTORICAL_PATH (e.g. /historic/weather)
Optional env:
  RESTAURANTS  default "restaurant-1,restaurant-2"
  LANDING      default /landing
  WEATHER_FILE default london_79_20_weather_data.csv (file name inside /landing/weather)
  ONLY         comma separated name prefixes to run a subset, e.g. "weather" or "restaurant-1"
"""

import os
import re
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

HDFS = os.environ["HDFS_URL"]
LANDING = HDFS + os.environ.get("LANDING", "/landing")
RESTAURANTS = os.environ.get("RESTAURANTS", "restaurant-1,restaurant-2").split(",")
ONLY = [s for s in os.environ.get("ONLY", "").split(",") if s]


def config_path(key):
    value = os.environ.get(key, "").strip()
    if not value:
        raise SystemExit(f"{key} is not set in the spark-integration ConfigMap")
    return value


# ---------------------------------------------------------------- dataset registry
@dataclass
class Dataset:
    name: str  # label in the logs, also used by ONLY
    src: str  # CSV path inside LANDING
    dst: str  # HDFS output dir (without hdfs://host)
    schema: StructType | None = None  # None = infer types + sanitise column names
    transform: Callable[[DataFrame], DataFrame] | None = None


# --- orders: explicit schema. The CSV header is skipped and replaced by these names
#     (Avro field names cannot contain spaces). Verify the column ORDER against the CSVs.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_number", IntegerType(), False),
        StructField("order_date_raw", StringType(), False),  # "03/08/2019 20:25"
        StructField("item_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("total_products", IntegerType(), True),
    ]
)
ORDER_DATE_FORMAT = "dd/MM/yyyy HH:mm"


def parse_order_date(df):
    df = df.withColumn(
        "order_date", F.to_timestamp("order_date_raw", ORDER_DATE_FORMAT)
    ).drop("order_date_raw")
    bad = df.filter(F.col("order_date").isNull()).count()
    if bad:
        raise ValueError(
            f"{bad} rows with unparseable order dates, check ORDER_DATE_FORMAT"
        )
    return df


# --- weather: one dataset, daily values. Header is skipped, names applied by position.
WEATHER_FILE = os.environ.get("WEATHER_FILE", "london_weather.csv")
WEATHER_SCHEMA = StructType(
    [
        StructField("date_raw", StringType(), False),  # "19790101"
        StructField("cloud_cover", DoubleType(), True),
        StructField("sunshine", DoubleType(), True),
        StructField("global_radiation", DoubleType(), True),
        StructField("max_temp", DoubleType(), True),
        StructField("mean_temp", DoubleType(), True),
        StructField("min_temp", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("snow_depth", DoubleType(), True),
    ]
)
WEATHER_DATE_FORMAT = "yyyyMMdd"


def parse_weather_date(df):
    df = df.withColumn("date", F.to_date("date_raw", WEATHER_DATE_FORMAT)).drop(
        "date_raw"
    )
    bad = df.filter(F.col("date").isNull()).count()
    if bad:
        raise ValueError(
            f"{bad} rows with unparseable weather dates, check WEATHER_DATE_FORMAT"
        )
    return df.select("date", *[c for c in df.columns if c != "date"])


orders_base = os.path.dirname(config_path("ORDERS_PATH"))  # /historic/orders

DATASETS = [
    Dataset(
        f"{r}/orders",
        f"orders/{r}-orders.csv",
        f"{orders_base}/{r}",
        ORDERS_SCHEMA,
        parse_order_date,
    )
    for r in RESTAURANTS
]
DATASETS.append(
    Dataset(
        "weather",
        f"weather/{WEATHER_FILE}",
        config_path("WEATHER_HISTORICAL_PATH"),
        WEATHER_SCHEMA,
        parse_weather_date,
    )
)


# ---------------------------------------------------------------- shared pipeline
spark = SparkSession.builder.appName("initial-load").getOrCreate()


def avro_safe(name):
    """Avro field names must match [A-Za-z_][A-Za-z0-9_]*"""
    n = re.sub(r"[^0-9a-zA-Z_]+", "_", name.strip()).strip("_").lower()
    return n if n and not n[0].isdigit() else "_" + n


def read_csv(ds):
    reader = spark.read.option("header", True)
    path = f"{LANDING}/{ds.src}"
    if ds.schema is not None:
        return reader.option("mode", "FAILFAST").schema(ds.schema).csv(path)
    df = reader.option("inferSchema", True).csv(path)
    return df.toDF(*[avro_safe(c) for c in df.columns])


def load(ds):
    df = read_csv(ds)
    if ds.transform:
        df = ds.transform(df)
    expected = df.count()
    out = HDFS + ds.dst
    print(f"--- {ds.name}: {LANDING}/{ds.src} -> {out}")
    df.printSchema()
    (
        df.coalesce(1)
        .write.format("avro")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(out)
    )
    got = spark.read.format("avro").load(out).count()
    if got != expected:
        raise ValueError(f"row count mismatch: csv={expected} avro={got}")
    return expected


results = {}
for ds in DATASETS:
    if ONLY and not any(ds.name.startswith(p) for p in ONLY):
        continue
    try:
        results[ds.name] = f"OK ({load(ds)} rows)"
    except Exception as e:
        results[ds.name] = f"FAILED: {str(e).splitlines()[0][:200]}"

print("\n===== SUMMARY =====")
for name, status in results.items():
    print(f"{name:45s} {status}")

spark.stop()
raise SystemExit(
    0 if results and all(s.startswith("OK") for s in results.values()) else 1
)
