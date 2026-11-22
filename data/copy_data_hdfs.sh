#!/bin/bash

export NS=bd-gr-06

echo "Creating /landing folder"
kubectl exec -n $NS hdfs-cli -- hdfs dfs -mkdir -p /landing


kubectl exec -n $NS hdfs-cli -- mkdir -p /tmp/orders
echo "Starting orders copy"
for f in historical/*.csv; do
    echo "Copying file: $f"
    kubectl cp "$f" $NS/hdfs-cli:/tmp/orders/$(basename "$f")
done

kubectl exec -n $NS hdfs-cli -- mkdir -p /tmp/weather
echo "Starting weather copy"
for f in weather/*.csv; do
    echo "Copying file: $f"
    kubectl cp "$f" $NS/hdfs-cli:/tmp/weather/$(basename "$f")
done
