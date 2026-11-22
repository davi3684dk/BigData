from pyspark.sql import functions as F

WEATHER = ["mean_temp", "max_temp", "min_temp", "precipitation", "sunshine", "cloud_cover"]


def add_calendar_features(df, date_col="date"):
    """Shared by training and streaming so features are identical in both."""
    return (
        df.withColumn("dow", F.dayofweek(date_col))  # 1=Sun ... 7=Sat
        .withColumn("month", F.month(date_col))
        .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
        .withColumn("is_friday", (F.col("dow") == 6).cast("int"))
    )
