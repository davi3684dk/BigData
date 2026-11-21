"""Proves driver -> master -> workers works: runs a tiny job and prints which worker pods did the work."""
import socket
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
sc = spark.sparkContext
total = sc.parallelize(range(1_000_000), 8).map(lambda x: x * x % 7).sum()
hosts = sorted(set(sc.parallelize(range(16), 16).map(lambda _: socket.gethostname()).collect()))
print("RESULT:", total, "| executed on:", hosts)
spark.stop()
