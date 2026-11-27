"""Kafka test helper, run as a Job (k8s/kafka-test.yaml). Uses the same Avro schemas and framing settings as host_model.py.
  send <topic> [start_offset_days=0] [n_days=3]
        publish ONE forecast batch (issued_at + forecasts[], schema app/schemas/weather_forecast.avsc) for n_days days,
        starting today + start_offset_days (day 1 rainy/cold, day 2 dry/warm, day 3 mixed)
  show <topic> [n=20]
        decode and print the newest messages of a predictions topic (schema app/schemas/prediction.avsc)"""
import os, sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

BOOT = os.environ["KAFKA_BOOTSTRAP"]
SCHEMAS = os.environ.get("SCHEMAS_DIR", "/opt/schemas")
FRAMED_IN = os.environ.get("KAFKA_REGISTRY_FRAMING", "true").lower() == "true"          # weather topic (what host_model strips)
FRAMED_OUT = bool(os.environ.get("PREDICTIONS_SCHEMA_ID", "").strip())                  # predictions topic (what host_model adds)
mode, topic = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName(f"kafka-test-{mode}").getOrCreate()

if mode == "send":
    start = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    n = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    now = datetime.now(timezone.utc)
    # (mean, max, min, precipitation, sunshine, cloud_cover)
    pattern = [(6.0, 8.5, 3.0, 7.2, 0.5, 8.0), (17.0, 21.0, 12.0, 0.0, 8.0, 1.0), (11.0, 14.0, 8.0, 1.5, 3.0, 5.0)]
    fc = []
    for i in range(n):
        mean, mx, mn, rain, sun, cloud = pattern[i % 3]
        d = (now + timedelta(days=start + i)).strftime("%Y-%m-%d")
        fc.append((d, cloud, sun, 100.0, mx, mean, mn, rain, 1015.0, 0.0))  # date, cloud, sunshine, radiation, max, mean, min, precip, pressure, snow
    ddl = ("issued_at string, forecasts array<struct<date:string,cloud_cover:double,sunshine:double,global_radiation:double,"
           "max_temp:double,mean_temp:double,min_temp:double,precipitation:double,pressure:double,snow_depth:double>>")
    df = spark.createDataFrame([(now.strftime("%Y-%m-%dT%H:%M:%SZ"), fc)], ddl)
    avro = to_avro(F.struct("issued_at", "forecasts"), open(f"{SCHEMAS}/weather_forecast.avsc").read())
    if FRAMED_IN:  # fake Confluent framing: magic byte 0 + 4 byte schema id (host_model ignores the id)
        avro = F.concat(F.lit(bytearray(b"\x00\x00\x00\x00\x01")), avro)
    (df.select(avro.alias("value")).write.format("kafka")
       .option("kafka.bootstrap.servers", BOOT).option("topic", topic).save())
    print(f"SENT 1 forecast batch with {n} days ({fc[0][0]} .. {fc[-1][0]}) to topic {topic}")
elif mode == "show":
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", BOOT).option("subscribe", topic)
           .option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
    payload = F.expr("substring(value, 6, length(value) - 5)") if FRAMED_OUT else F.col("value")
    dec = (raw.select("partition", "offset", "timestamp", F.col("key").cast("string").alias("key"),
                      from_avro(payload, open(f"{SCHEMAS}/prediction.avsc").read()).alias("p"))
           .select("partition", "offset", "timestamp", "key", "p.*"))
    print("MESSAGES IN TOPIC:", raw.count())
    dec.orderBy(F.desc("timestamp")).show(n, truncate=False)
else:
    sys.exit("mode must be send or show")
spark.stop()
