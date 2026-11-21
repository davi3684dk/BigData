## Run as a Kubernetes Job

From this directory, compress the CSV inputs before creating a ConfigMap. This
keeps the ConfigMap below Kubernetes' size limit:

```powershell
Compress-Archive -Path restaurant-1-orders-streaming.csv, restaurant-2-orders-streaming.csv, london_weather.csv `
    -DestinationPath streaming-inputs.zip -Force
try {
    kubectl create configmap restaurant-orders-stream-files `
        --from-file=stream-restaurant-orders.py `
        --from-file=streaming-inputs.zip `
        --from-file=requirements.txt `
        --dry-run=client -o yaml |
        kubectl apply --server-side -f -
} finally {
    Remove-Item streaming-inputs.zip -ErrorAction SilentlyContinue
}
```

Create the Job after
the ConfigMap and Kafka are available in the same namespace:

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