import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

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
    name: str                                   # label in the logs, also used by ONLY
    src: str                                    # CSV file name inside LANDING
    dst: str                                    # HDFS output dir (without hdfs://host)
    schema: Optional[StructType] = None         # None = infer types + sanitise column names
    transform: Optional[Callable[[DataFrame], DataFrame]] = None


# --- restaurants: explicit schemas. CSV headers are ignored and replaced by these names
#     (Avro field names cannot contain spaces). Verify the column ORDER against the CSVs.
ORDERS_SCHEMA = StructType([
    StructField("order_number",   IntegerType(), False),
    StructField("order_date_raw", StringType(),  False),
    StructField("item_name",      StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("product_price",  DoubleType(),  True),
    StructField("total_products", IntegerType(), True),
])
ORDER_DATE_FORMAT = "dd/MM/yyyy HH:mm"


def parse_order_date(df):
    df = (df.withColumn("order_date", F.to_timestamp("order_date_raw", ORDER_DATE_FORMAT))
            .drop("order_date_raw"))
    bad = df.filter(F.col("order_date").isNull()).count()
    if bad:
        raise ValueError(f"{bad} rows with unparseable order dates, check ORDER_DATE_FORMAT")
    return df


# --- weather: schemas not decided yet, so types are inferred and columns left as they are.
WEATHER_FILES = {
    "WEATHER_PATH_1979_2020": "london_79_20_weather_data.csv",   # emmanuelfwerr
    "WEATHER_PATH_1979_2023": "london_79_23_weather_data.csv",   # zongaobian (ECA&D)
    "WEATHER_PATH_2000_2023": "london_00_23_weather_data.csv",   # noahx1 (Meteostat)
}

orders_base = os.path.dirname(config_path("ORDERS_PATH"))        # /historic/orders/restaurant-* -> /historic/orders

DATASETS = []
for r in RESTAURANTS:
    DATASETS.append(Dataset(f"{r}/orders", f"{r}-orders.csv", f"{orders_base}/{r}",
                            ORDERS_SCHEMA, parse_order_date))
for key, fname in WEATHER_FILES.items():
    DATASETS.append(Dataset(f"weather/{key}", fname, config_path(key)))


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
    (df.coalesce(1).write.format("avro").mode("overwrite")
       .option("compression", "snappy").save(out))
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
raise SystemExit(0 if results and all(s.startswith("OK") for s in results.values()) else 1)
