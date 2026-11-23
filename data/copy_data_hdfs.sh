#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

NS="${NS:-bd-gr-06}"
POD=hdfs-cli
CONTAINER=hadoop
shopt -s nullglob

hdfs_exec() { kubectl exec -n "$NS" -c "$CONTAINER" "$POD" -- "$@"; }

put_file() {   # put_file <local file> <hdfs dir>
    local f="$1" dir="$2"
    echo "Uploading $f -> $dir/$(basename "$f")"
    kubectl exec -i -n "$NS" -c "$CONTAINER" "$POD" -- \
        hdfs dfs -put -f - "$dir/$(basename "$f")" < "$f"
}

orders=(historical/restaurant-*-orders.csv)
weather=()
for f in historical/*.csv; do
    [[ "$f" == historical/restaurant-*-orders.csv ]] || weather+=("$f")
done

if [[ ${#orders[@]} -eq 0 || ${#weather[@]} -eq 0 ]]; then
    echo "Expected order files and a weather file in historical/ (found ${#orders[@]} orders, ${#weather[@]} weather)" >&2
    exit 1
fi

echo "Creating HDFS folders"
hdfs_exec hdfs dfs -mkdir -p /landing/orders /landing/weather

echo "Starting orders upload"
for f in "${orders[@]}"; do put_file "$f" /landing/orders; done

echo "Starting weather upload"
for f in "${weather[@]}"; do put_file "$f" /landing/weather; done

echo "Done. Contents of /landing:"
hdfs_exec hdfs dfs -ls -R -h /landing
