from pyspark.sql import functions as F
from weather_sources import snake, expect


def load_daily_orders(spark, fmt, path):
    """Order lines (Avro in HDFS, historic + live) -> one row per restaurant and day: orders, revenue."""
    raw = snake(spark.read.format(fmt).load(path))
    expect(raw, ["order_number", "order_date", "quantity", "product_price"], "orders")
    rid = (F.col("restaurant_id").cast("string") if "restaurant_id" in raw.columns
           else F.regexp_extract(F.input_file_name(), r"restaurant-([^/]+)", 1))
    s = F.col("order_date").cast("string")
    ts = F.when(s.rlike(r"^\d{2}/\d{2}/\d{4}"), F.to_timestamp(s, "dd/MM/yyyy HH:mm")).otherwise(F.to_timestamp(s))
    orders = raw.select(rid.alias("restaurant_id"), F.col("order_number").cast("string").alias("order_id"),
                        ts.alias("ts"), F.col("quantity").cast("double").alias("quantity"),
                        F.col("product_price").cast("double").alias("price")).withColumn("date", F.to_date("ts"))
    print("ORDER LINES:", orders.count(), "| unparsable dates:", orders.where(F.col("date").isNull()).count())
    return (orders.where(F.col("date").isNotNull()).groupBy("restaurant_id", "date")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.sum(F.col("quantity") * F.col("price")).alias("revenue")))
