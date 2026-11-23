"""
Split the full CSVs into a historical and a streaming portion.
"""

import glob
import os
import sys

import pandas as pd

HISTORICAL_FRAC = 0.25
ORDER_DATE_FORMAT = "%d/%m/%Y %H:%M"
WEATHER_DATE_FORMAT = "%Y%m%d"  # e.g. 19790101
WEATHER_FILE = "./full/london_weather.csv"

os.makedirs("./historical", exist_ok=True)
os.makedirs("./streaming", exist_ok=True)

hist_ends, stream_ends = [], []
order_files = sorted(glob.glob("./full/restaurant-*-orders.csv"))
if not order_files:
    sys.exit("No ./full/restaurant-*-orders.csv files found")

for path in order_files:
    name = os.path.basename(path)

    df = pd.read_csv(path)
    order_col, date_col = df.columns[0], df.columns[1]
    parsed = pd.to_datetime(df[date_col], format=ORDER_DATE_FORMAT)

    first_seen = parsed.groupby(df[order_col]).min().sort_values(kind="stable")
    cut = int(len(first_seen) * HISTORICAL_FRAC)
    historical_ids = set(first_seen.index[:cut])

    is_hist = df[order_col].isin(historical_ids)
    df[is_hist].to_csv(f"./historical/{name}", index=False)
    df[~is_hist].to_csv(f"./streaming/{name}", index=False)

    hist_ends.append(parsed[is_hist].max())
    stream_ends.append(parsed[~is_hist].max())
    print(
        f"{name}: {is_hist.sum()} rows historical ({parsed[is_hist].min():%Y-%m-%d} to "
        f"{parsed[is_hist].max():%Y-%m-%d}), {(~is_hist).sum()} rows streaming "
        f"({parsed[~is_hist].min():%Y-%m-%d} to {parsed[~is_hist].max():%Y-%m-%d})"
    )

# ---------------------------------------------------------------- 2. weather
hist_end = max(hist_ends).normalize()  # last historical order day (any restaurant)
stream_end = max(stream_ends).normalize()  # last order day overall

weather = pd.read_csv(WEATHER_FILE)
wdate = pd.to_datetime(weather["date"].astype(str), format=WEATHER_DATE_FORMAT)

is_hist = wdate <= hist_end
is_stream = (wdate > hist_end) & (wdate <= stream_end)

wname = os.path.basename(WEATHER_FILE)
weather[is_hist].to_csv(f"./historical/{wname}", index=False)
weather[is_stream].to_csv(f"./streaming/{wname}", index=False)

print(
    f"\n{wname}: {is_hist.sum()} days historical (up to {hist_end:%Y-%m-%d}), "
    f"{is_stream.sum()} days streaming ({hist_end + pd.Timedelta(days=1):%Y-%m-%d} to {stream_end:%Y-%m-%d}), "
    f"{(~is_hist & ~is_stream).sum()} days after the last order dropped"
)

if wdate.max() < stream_end:
    print(
        f"WARNING: weather ends {wdate.max():%Y-%m-%d}, before the last order day {stream_end:%Y-%m-%d}"
    )
