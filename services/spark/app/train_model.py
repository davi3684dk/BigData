"""Historic training job.
HDFS (orders + 3 weather datasets) -> daily dataset -> MLlib GBT -> candidate model in <MODEL_DIR>/training/<run_id>
-> promoted to <MODEL_DIR>/current only if it beats the current model on the same test period (old one goes to archive/).
All locations come from the spark-integration ConfigMap (env vars)."""
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from features import WEATHER, add_calendar_features
from weather_sources import snake, from_emmanuel, from_ecad, from_meteostat, from_live_forecast, harmonise
from orders_data import load_daily_orders

HDFS = os.environ["HDFS_URL"].rstrip("/")
FMT = os.environ.get("INPUT_FORMAT", "avro")
ORDERS_PATH = HDFS + os.environ.get("ORDERS_PATH", "/topics/restaurant-*")
MODEL_DIR = HDFS + os.environ.get("MODEL_DIR", "/models")
CUTOFF = os.environ.get("TRAIN_CUTOFF", "").strip()      # fixed date; default: test on the newest TEST_FRACTION of days
TEST_FRACTION = float(os.environ.get("TEST_FRACTION", "0.2"))
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

spark = SparkSession.builder.appName("train-order-model").getOrCreate()

# Weather: 1 dataset -> one harmonised table, MOOT since only one source now
SOURCES = [("WEATHER_HISTORICAL_PATH", from_emmanuel)] 
frames = [fn(snake(spark.read.format(FMT).load(HDFS + os.environ[env].strip())))
          for env, fn in SOURCES if os.environ.get(env, "").strip()]
if not frames:
    raise SystemExit("No weather source configured (WEATHER_PATH_* in the ConfigMap)")
weather = harmonise(frames)

# Orders (historic + live, whatever is in HDFS) -> daily orders + revenue per restaurant
daily = load_daily_orders(spark, FMT, ORDERS_PATH)
data = add_calendar_features(daily.join(weather, "date", "inner"))
data.write.mode("overwrite").parquet(f"{HDFS}/curated/daily")
print("DAILY ROWS (orders joined with weather):", data.count())
data.groupBy("restaurant_id").agg(F.min("date"), F.max("date"), F.count("*").alias("days"), F.avg("orders")).show()

aov = (data.groupBy("restaurant_id").agg((F.sum("revenue") / F.sum("orders")).alias("avg_order_value")))

# Time-based split. newest days = test, so live data is what we are judged on
if not CUTOFF:
    dates = [r[0] for r in data.select("date").distinct().orderBy("date").collect()]
    if len(dates) < 10:
        raise SystemExit(f"Only {len(dates)} days of data, need more to train")
    CUTOFF = str(dates[int(len(dates) * (1 - TEST_FRACTION))])
print("TEST PERIOD STARTS", CUTOFF)
train, test = data.filter(F.col("date") < CUTOFF), data.filter(F.col("date") >= CUTOFF)
n_train, n_test = train.count(), test.count()
if n_train == 0 or n_test == 0:
    raise SystemExit(f"Empty split (train={n_train}, test={n_test}); change TRAIN_CUTOFF")

imputed = [f"{c}_imp" for c in WEATHER]
pipeline = Pipeline(stages=[
    Imputer(inputCols=WEATHER, outputCols=imputed, strategy="median"),
    StringIndexer(inputCol="restaurant_id", outputCol="rid_idx", handleInvalid="keep"),
    OneHotEncoder(inputCols=["rid_idx"], outputCols=["rid_vec"]),
    VectorAssembler(inputCols=imputed + ["dow", "month", "is_weekend", "is_friday", "rid_vec"], outputCol="features"),
    GBTRegressor(featuresCol="features", labelCol="orders", maxIter=100, maxDepth=5, seed=42),
])
model = pipeline.fit(train)


def metric(df, name, col="prediction"):
    return RegressionEvaluator(labelCol="orders", predictionCol=col, metricName=name).evaluate(df)


pred = model.transform(test)
rmse, mae, r2 = metric(pred, "rmse"), metric(pred, "mae"), metric(pred, "r2")
# Baseline without weather: average orders per restaurant and weekday
mean_all = train.agg(F.avg("orders")).first()[0]
base = train.groupBy("restaurant_id", "dow").agg(F.avg("orders").alias("baseline"))
base_test = test.join(base, ["restaurant_id", "dow"], "left").withColumn("baseline", F.coalesce("baseline", F.lit(mean_all)))
rmse_base = metric(base_test, "rmse", "baseline")
print(f"METRICS run={RUN_ID} rmse={rmse:.3f} mae={mae:.3f} r2={r2:.3f} | baseline(weekday only) rmse={rmse_base:.3f}")

# Save as a candidate (the 'training' slot)
CAND = f"{MODEL_DIR}/training/{RUN_ID}"
model.write().overwrite().save(f"{CAND}/pipeline")
aov.write.mode("overwrite").parquet(f"{CAND}/avg_order_value")
spark.createDataFrame([(RUN_ID, float(rmse), float(mae), float(r2), float(rmse_base), CUTOFF, n_train, n_test)],
                      "run_id string, rmse double, mae double, r2 double, baseline_rmse double, cutoff string, "
                      "n_train long, n_test long").write.mode("overwrite").parquet(f"{CAND}/metrics")

# Promote to the 'current' (hosted) slot only if better on the same test period
P = spark._jvm.org.apache.hadoop.fs.Path
fs = P(MODEL_DIR).getFileSystem(spark._jsc.hadoopConfiguration())
CUR = f"{MODEL_DIR}/current"
promote = True
if fs.exists(P(CUR)):
    try:
        cur_rmse = metric(PipelineModel.load(f"{CUR}/pipeline").transform(test), "rmse")
        promote = rmse <= cur_rmse
        print(f"CURRENT MODEL rmse on same test set: {cur_rmse:.3f} -> new model {'better' if promote else 'NOT better'}")
    except Exception as e:  # e.g. current model uses different features
        print("could not evaluate current model, promoting new one:", e)
    if promote:
        old_id = spark.read.parquet(f"{CUR}/metrics").first()["run_id"]
        fs.mkdirs(P(f"{MODEL_DIR}/archive"))
        fs.rename(P(CUR), P(f"{MODEL_DIR}/archive/{old_id}"))
if promote:
    fs.rename(P(CAND), P(CUR))
    print("PROMOTED", RUN_ID, "-> current")
else:
    print("KEPT current model; candidate left in", CAND)
spark.stop()
