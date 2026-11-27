"""The 'hosted ML model': one long-running Spark app that
  - keeps <MODEL_DIR>/current in memory (re-checked every 60 s, hot-swapped when training promotes a new model)
  - serves it over HTTP for the prediction tool / anyone:  POST /predict, GET /model, GET /health
  - is the trigger: Structured Streaming on the weather topics; every new forecast gets scored and (PUBLISH_FROM_SPARK=true)
     published as Avro to PREDICTIONS_TOPIC and restaurant-<id>.
Config via the spark-integration env vars."""
import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel
from features import WEATHER
from scoring import predict_df

HDFS = os.environ["HDFS_URL"].rstrip("/")
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
IN_PATTERN = os.environ.get("WEATHER_TOPIC_PATTERN", "weather-forecast-.*")
OUT_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "predictions")
FRAMED = os.environ.get("KAFKA_REGISTRY_FRAMING", "true").lower() == "true"
OUT_ID = os.environ.get("PREDICTIONS_SCHEMA_ID", "")
PUBLISH = os.environ.get("PUBLISH_FROM_SPARK", "true").lower() == "true"
MODEL_DIR = HDFS + os.environ.get("MODEL_DIR", "/models")
PRED_LOG = HDFS + os.environ.get("PREDICTIONS_LOG", "/predictions")  # every prediction, kept for accuracy checks
CURRENT = f"{MODEL_DIR}/current"
CHECKPOINTS = HDFS + os.environ.get("CHECKPOINT_DIR", "/checkpoints")
SCHEMAS = os.environ.get("SCHEMAS_DIR", "/opt/schemas") # mounted from the spark-schemas ConfigMap
PORT = int(os.environ.get("MODEL_API_PORT", "8000"))

spark = SparkSession.builder.appName("model-host").getOrCreate()


class Holder:
    def __init__(self):
        self.lock = threading.Lock()
        self.model = self.aov = self.version = None
        self.info = {}

    def refresh(self):
        try:
            m = spark.read.parquet(f"{CURRENT}/metrics").first()
            if m is None or m["run_id"] == self.version:
                return
            model = PipelineModel.load(f"{CURRENT}/pipeline")
            rows = spark.read.parquet(f"{CURRENT}/avg_order_value").collect()  # tiny: keep in memory, no file dependency
            aov = spark.createDataFrame([(r["restaurant_id"], float(r["avg_order_value"])) for r in rows],
                                        "restaurant_id string, avg_order_value double")
            with self.lock:
                self.model, self.aov, self.version, self.info = model, aov, m["run_id"], m.asDict()
            print("MODEL LOADED", self.version, self.info, flush=True)
        except Exception as e: # no model yet, or training is swapping the folder: retry next cycle
            print("model refresh failed (will retry):", str(e)[:200], flush=True)

    def get(self):
        with self.lock:
            return self.model, self.aov, self.version


holder = Holder()
FORECAST_SCHEMA = StructType([StructField("area", StringType()), StructField("date", StringType())] +
                             [StructField(c, DoubleType()) for c in WEATHER])


def predict(payload):
    """payload: one forecast object, a list of them, or {"records": [...], "restaurant_id": "1" | ["1","2"]}"""
    model, aov, version = holder.get()
    if model is None:
        raise RuntimeError("no model loaded yet")
    ids = payload.get("restaurant_id") if isinstance(payload, dict) else None
    recs = payload["records"] if isinstance(payload, dict) and "records" in payload else \
        ([payload] if isinstance(payload, dict) else payload)
    rows = [(r.get("area"), r["date"], *[None if r.get(c) is None else float(r[c]) for c in WEATHER]) for r in recs]
    df = spark.createDataFrame(rows, FORECAST_SCHEMA).withColumn("date", F.to_date("date"))
    if ids:
        ids = ids if isinstance(ids, list) else [ids]
        aov = aov.filter(F.col("restaurant_id").isin([str(i) for i in ids]))
    return [r.asDict() for r in predict_df(model, aov, df, version).collect()]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            ready = holder.model is not None
            self._send(200 if ready else 503, {"ready": ready})
        elif self.path == "/model":
            self._send(200, {"version": holder.version, **holder.info})
        else:
            self._send(404, {"error": "use POST /predict, GET /model, GET /health"})

    def do_POST(self):
        if self.path != "/predict":
            return self._send(404, {"error": "use POST /predict"})
        try:
            payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            preds = predict(payload)
            self._send(200, {"model_version": holder.version, "predictions": preds})
        except (KeyError, ValueError, TypeError) as e:
            self._send(400, {"error": f"bad request: {e}"})
        except Exception as e:
            self._send(500, {"error": str(e)[:300]})

    def log_message(self, *a):
        pass


def reload_loop():
    while True:
        time.sleep(60)
        holder.refresh()


holder.refresh()
threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever, daemon=True).start()
threading.Thread(target=reload_loop, daemon=True).start()
while holder.model is None: # don't consume weather until there is a model to score it with
    print("waiting for a trained model in", CURRENT, flush=True)
    time.sleep(15)
    holder.refresh()

# Trigger: new weather forecast on Kafka -> predictions
in_schema = open(f"{SCHEMAS}/weather_forecast.avsc").read()
out_schema = open(f"{SCHEMAS}/prediction.avsc").read()
raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribePattern", IN_PATTERN).option("startingOffsets", "latest").load())
payload = F.expr("substring(value, 6, length(value) - 5)") if FRAMED else F.col("value")  # strip Confluent framing
forecast = (
    raw.select(from_avro(payload, in_schema).alias("j"))
    .select(
        F.col("j.issued_at").alias("issued_at"),
        F.explode("j.forecasts").alias("obs"),
    )
    .select(
        F.to_date("obs.date").alias("date"),
        F.lit("london").alias("area"), # For now hardcoded London, later we need to talk about adding area to forecast schema TODO
        F.col("issued_at"),
        F.col("obs.mean_temp").cast("double").alias("mean_temp"),
        F.col("obs.mean_temp").cast("double").alias("temperature"),
        F.col("obs.min_temp").cast("double").alias("min_temp"),
        F.col("obs.max_temp").cast("double").alias("max_temp"),
        F.col("obs.precipitation").cast("double").alias("precipitation"),
        F.col("obs.sunshine").cast("double").alias("sunshine"),
        F.col("obs.cloud_cover").cast("double").alias("cloud_cover"),
        F.col("obs.global_radiation").cast("double").alias("global_radiation"),
        F.col("obs.pressure").cast("double").alias("pressure"),
        F.col("obs.snow_depth").cast("double").alias("snow_depth"),
    )
    .filter(F.col("date").isNotNull())
)



def on_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    model, aov, version = holder.get()
    out = predict_df(model, aov, batch_df, version).cache()
    out.write.mode("append").parquet(PRED_LOG) # history: what we predicted, when, with which model
    if PUBLISH:
        avro = to_avro(F.struct(*out.columns), out_schema)
        if OUT_ID: # prepend Confluent framing so Schema Registry consumers can decode
            avro = F.concat(F.lit(bytearray(b"\x00" + int(OUT_ID).to_bytes(4, "big"))), avro)
        msgs = out.select(F.col("restaurant_id").alias("key"), avro.alias("value"))
        w = lambda d: d.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
        w(msgs).option("topic", OUT_TOPIC).save()
        w(msgs.withColumn("topic", F.concat(F.lit("restaurant-"), F.col("key")))).save() # feedback loop
    print(f"batch {batch_id}: {out.count()} predictions with model {version}", flush=True)
    out.unpersist()


(forecast.writeStream.foreachBatch(on_batch)
 .option("checkpointLocation", f"{CHECKPOINTS}/model-host").trigger(processingTime="5 seconds")
 .start().awaitTermination())
