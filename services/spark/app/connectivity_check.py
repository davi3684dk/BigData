"""Integration check. Usage:
  connectivity_check.py hdfs  <hdfs path or glob>   e.g. /topics/restaurant-1
  connectivity_check.py kafka <topic>
"""
import os, sys
from pyspark.sql import SparkSession, functions as F

mode, target = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName(f"check-{mode}").getOrCreate()

if mode == "hdfs":
    df = spark.read.format("avro").load(os.environ["HDFS_URL"].rstrip("/") + target)
    df.printSchema()
    df.show(5, truncate=False)
    print("ROWS:", df.count())
elif mode == "kafka":
    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
          .option("subscribe", target).option("startingOffsets", "earliest").load())
    print("MESSAGES:", df.count())
    # first byte 0 + 4 byte schema id => Confluent framing (set KAFKA_REGISTRY_FRAMING=true)
    df.select("partition", "offset", F.length("value").alias("len"),
              F.hex(F.substring("value", 1, 5)).alias("first5bytes_hex")).show(5, truncate=False)
else:
    sys.exit("mode must be hdfs or kafka")
spark.stop()
