## Run manually in an interactive Kubernetes pod

Run these PowerShell commands from the repository root, in the same namespace
as Kafka and Schema Registry:

```powershell
kubectl run restaurant-orders-manual `
    --image=python:3.12-slim `
    --restart=Never `
    --command -- sleep infinity

kubectl wait --for=condition=Ready pod/restaurant-orders-manual --timeout=120s
kubectl exec restaurant-orders-manual -- apt-get update
kubectl exec restaurant-orders-manual -- apt-get install -y tar
kubectl exec restaurant-orders-manual -- mkdir -p /app

$archive = "services\restaurant-orders-stream\streaming-inputs.zip"
Compress-Archive -Path `
    "data\streaming\restaurant-1-orders.csv", `
    "data\streaming\restaurant-2-orders.csv", `
    "data\streaming\london_weather.csv" `
    -DestinationPath $archive -Force
try {
    kubectl cp "services\restaurant-orders-stream\stream-restaurant-orders.py" `
        restaurant-orders-manual:/app/
    kubectl cp "services\restaurant-orders-stream\requirements.txt" `
        restaurant-orders-manual:/app/
    kubectl cp $archive restaurant-orders-manual:/app/streaming-inputs.zip
} finally {
    Remove-Item "services\restaurant-orders-stream\streaming-inputs.zip" -ErrorAction SilentlyContinue
}

kubectl exec -it restaurant-orders-manual -- sh
```

Inside the pod, install the requirements and run the script:

```sh
python -m pip install --target /tmp/python-packages -r /app/requirements.txt
PYTHONPATH=/tmp/python-packages python /app/stream-restaurant-orders.py
```

The script defaults to `kafka:9092` and
`http://kafka-schema-registry:8081`; override them with
`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_SCHEMA_REGISTRY_URL` if needed. The
simulation uses one minute per event-day, so replaying the full input date
range takes many hours.

After the run, exit the shell and remove the pod:

```powershell
kubectl delete pod restaurant-orders-manual
```

The script streams daily observations to `weather-london` and publishes one
message containing seven weather records in a `forecasts` list to
`weather-predictions-london` at midnight, including the issue date and the
following six days. Actual observations go to `weather-london` at `23:59` on
their date. Timestamps use the same `DD/MM/YYYY HH:MM` format as order
timestamps. The final six dates have no forecast batch because the CSV has no
complete seven-day horizon for them.

All three topics use Confluent-framed Avro values registered in Schema
Registry: order records use a `RestaurantOrder` schema, observations use a
`WeatherRecord` schema, and forecast batches use a `WeatherForecastBatch`
schema containing weather records. The producer connects to
`http://kafka-schema-registry:8081` by default; override it with
`KAFKA_SCHEMA_REGISTRY_URL` if Schema Registry is hosted elsewhere.