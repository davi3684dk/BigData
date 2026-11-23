"""Split the full Kaggle CSVs into historical (earliest ~25%) and streaming (latest ~75%).

Splits by ORDER, not by row, so an order's items are never divided.
"""
import glob
import os
import shutil

import pandas as pd

FRAC = 0.75
DATE_FORMAT = "%d/%m/%Y %H:%M"

os.makedirs("./historical", exist_ok=True)
os.makedirs("./streaming", exist_ok=True)

for path in sorted(glob.glob("./full/*.csv")):
    name = os.path.basename(path)

    df = pd.read_csv(path)
    order_col, date_col = df.columns[0], df.columns[1]
    parsed = pd.to_datetime(df[date_col], format=DATE_FORMAT)

    first_seen = parsed.groupby(df[order_col]).min().sort_values(kind="stable")
    cut = int(len(first_seen) * (1 - FRAC))
    historical_ids = set(first_seen.index[:cut])

    is_hist = df[order_col].isin(historical_ids)
    df[is_hist].to_csv(f"./historical/{name}", index=False)
    df[~is_hist].to_csv(f"./streaming/{name}", index=False)

    print(f"{name}: {is_hist.sum()} rows historical, {(~is_hist).sum()} rows streaming "
          f"({len(historical_ids)} / {len(first_seen) - len(historical_ids)} orders)")
