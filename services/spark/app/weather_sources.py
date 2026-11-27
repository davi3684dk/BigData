"""Turns the three historic London weather datasets into ONE schema (WEATHER columns, same units as the live forecast).

 1979-2020 (emmanuelfwerr, london_weather.csv): date, cloud_cover, sunshine[h], max/mean/min_temp[C], precipitation[mm], pressure[Pa]...
 1979-2023 (zongaobian, ECA&D style): DATE, TX/TN/TG[0.1 C], RR[0.1 mm], SS[0.1 h], CC[oktas] + Q_xx quality flags (9 = missing)
 2000-2023 (noahx1, Meteostat style): date, tavg/tmin/tmax[C], prcp[mm], tsun[min], pres[hPa] (many empty values)
Target units: temperatures C, precipitation mm, sunshine hours, cloud_cover oktas.
"""
import re
from functools import reduce
from pyspark.sql import functions as F, Window
from features import WEATHER


def _snake(c):
    c = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", c.strip())  # OrderNumber -> Order_Number (Avro names cannot contain spaces)
    return re.sub(r"[^0-9a-z]+", "_", c.lower()).strip("_")


def snake(df):
    """'Order Number' / 'OrderNumber' / 'order_number' -> order_number, 'Q_TX' -> q_tx: tolerant to how the Avro field names were sanitised."""
    return df.toDF(*[_snake(c) for c in df.columns])


def expect(df, cols, what):
    """Fail loudly (listing what IS there) if the Avro schema does not look like we assume."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{what}: expected columns {missing} not found. Columns present: {df.columns}. "
                         f"Fix the field names in the sink/producer or the mapping in app/weather_sources.py / app/orders_data.py")


def _num(df, c, scale=1.0, quality=None):
    if c not in df.columns:
        return F.lit(None).cast("double")
    x = F.col(c).cast("double")
    bad = x == -9999
    if quality and quality in df.columns:
        bad = bad | (F.col(quality).cast("int") == 9)
    return F.when(bad, F.lit(None)).otherwise(x / scale)


def _date(df):
    for cand in ("date", "time", "datetime", "day"):
        if cand in df.columns:
            s = F.col(cand).cast("string")
            return F.when(s.rlike(r"^\d{8}$"), F.to_date(s, "yyyyMMdd")).otherwise(F.to_date(s))
    raise ValueError(f"no date column among {df.columns}")


def from_emmanuel(df):
    expect(df, ["mean_temp", "precipitation"], "weather 1979-2020 (london_weather)")
    return df.select(_date(df).alias("date"),
                     _num(df, "mean_temp").alias("mean_temp"), _num(df, "max_temp").alias("max_temp"),
                     _num(df, "min_temp").alias("min_temp"), _num(df, "precipitation").alias("precipitation"),
                     _num(df, "sunshine").alias("sunshine"), _num(df, "cloud_cover").alias("cloud_cover"))


def from_ecad(df):
    expect(df, ["tg", "rr"], "weather 1979-2023 (ECA&D: DATE, TX, TN, TG, SS, RR, CC ...)")
    return df.select(_date(df).alias("date"),
                     _num(df, "tg", 10, "q_tg").alias("mean_temp"), _num(df, "tx", 10, "q_tx").alias("max_temp"),
                     _num(df, "tn", 10, "q_tn").alias("min_temp"), _num(df, "rr", 10, "q_rr").alias("precipitation"),
                     _num(df, "ss", 10, "q_ss").alias("sunshine"), _num(df, "cc", 1, "q_cc").alias("cloud_cover"))


def from_meteostat(df):
    expect(df, ["tavg", "prcp"], "weather 2000-2023 (Meteostat: tavg, tmin, tmax, prcp, tsun ...)")
    return df.select(_date(df).alias("date"),
                     _num(df, "tavg").alias("mean_temp"), _num(df, "tmax").alias("max_temp"),
                     _num(df, "tmin").alias("min_temp"), _num(df, "prcp").alias("precipitation"),
                     _num(df, "tsun", 60).alias("sunshine"),  # minutes -> hours
                     F.lit(None).cast("double").alias("cloud_cover"))


def harmonise(frames):
    """frames in priority order: for each day, the first source that has a value wins, later ones fill the gaps."""
    clean = [f.where(F.col("date").isNotNull()).dropDuplicates(["date"]) for f in frames]

    def merge(a, b):
        j = a.alias("a").join(b.alias("b"), "date", "full")
        return j.select("date", *[F.coalesce(F.col(f"a.{c}"), F.col(f"b.{c}")).alias(c) for c in WEATHER])
    return reduce(merge, clean)


def from_live_forecast(df):
    """Forecast batches that were streamed through Kafka and landed in HDFS (schema: issued_at, forecasts[] - see
    app/schemas/weather_forecast.avsc). Used for days that have live orders but no historic observation.
    One row per date = the most recently issued forecast for that date."""
    expect(df, ["issued_at", "forecasts"], "live weather forecasts")
    x = df.select("issued_at", F.explode("forecasts").alias("f"))
    x = x.select("issued_at", F.col("f.date").alias("date"), *[F.col(f"f.{c}").alias(c) for c in WEATHER])
    x = x.select("issued_at", _date(x).alias("date"), *[_num(x, c).alias(c) for c in WEATHER]).where(F.col("date").isNotNull())
    w = Window.partitionBy("date").orderBy(F.col("issued_at").desc())
    return x.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn", "issued_at")