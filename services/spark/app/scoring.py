from pyspark.sql import functions as F
from features import add_calendar_features


def predict_df(model, aov, forecast, version):
    """forecast: DataFrame(area, date[DateType], <WEATHER columns>) -> one row per forecast day x restaurant."""
    f = forecast.withColumn("k", F.lit(1))
    scored = model.transform(add_calendar_features(f.join(aov.withColumn("k", F.lit(1)), "k").drop("k")))
    orders = F.greatest(F.round("prediction"), F.lit(0.0))
    return scored.select(
        "restaurant_id", "area", F.date_format("date", "yyyy-MM-dd").alias("date"),
        orders.cast("long").alias("predicted_orders"),
        (orders * F.col("avg_order_value")).alias("predicted_revenue"),
        "mean_temp", "precipitation",
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss").alias("predicted_at"),
        F.lit(version).alias("model_version"))
