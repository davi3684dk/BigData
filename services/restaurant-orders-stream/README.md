## Run as a Kubernetes Job

From main directory, compress the CSV inputs before creating a ConfigMap. This
keeps the ConfigMap below Kubernetes' size limit:

```powershell
Compress-Archive -Path "data\streaming\restaurant-1-orders.csv", "data\streaming\restaurant-2-orders.csv", "data\streaming\london_weather.csv" `
    -DestinationPath "services\restaurant-orders-stream\streaming-inputs.zip" -Force
try {
    kubectl create configmap restaurant-orders-stream-files `
        --from-file="services\restaurant-orders-stream\stream-restaurant-orders.py" `
        --from-file="services\restaurant-orders-stream\streaming-inputs.zip" `
        --from-file="services\restaurant-orders-stream\requirements.txt" `
        --dry-run=client -o yaml |
        kubectl apply --server-side -f -
} finally {
    Remove-Item "services\restaurant-orders-stream\streaming-inputs.zip" -ErrorAction SilentlyContinue
}
```

Create the Job after
the ConfigMap, Kafka, and Kafka Schema Registry are available in the same
namespace:

```sh
kubectl apply -f restaurant-orders-job.yaml
kubectl logs -f job/restaurant-orders-stream
```

To run the Job again, delete the existing Job and
apply the manifest again. Update the ConfigMap first if you have changed the
script, CSV files, or dependency list.

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