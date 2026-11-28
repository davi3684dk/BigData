# Flask portal backend

One Flask process serves the dashboard and its JSON API on port 8080. A background Kafka consumer keeps the last received current-weather record and order prediction in memory. PostgreSQL supplies history on request.

## Run with Kafka in Kubernetes

Build from this directory and make the image available to the cluster nodes.

```powershell
docker build -t portal-backend:local .
kubectl apply -f portal-backend.yaml
kubectl rollout status deployment/portal-backend
kubectl port-forward svc/portal-backend 8080:8080
```

Deploy in the namespace containing the PostgreSQL and Kafka services. The manifest uses the Sqoop Helm database settings and reads the password from Secret `postgresql`. For a different database, psycopg accepts `DATABASE_URL` or the standard `PG*` variables.

## Endpoints

| Request | Response |
| --- | --- |
| `GET /health` | HTTP process status |
| `GET /api/live` | Latest weather observation, last received prediction, and a live outlook containing the forecast issue, target days and matching restaurant predictions. |
| `GET /api/history/YYYY-MM-DD` | Weather observations and one comparison per restaurant with orders or predictions on that date |

The comparison counts distinct `order_number` values in `"restaurant-1"` and `"restaurant-2"` by order date. It matches those counts to restaurant IDs `1` and `2` in `prediction`. For multiple predictions on one target day, the query selects the greatest target timestamp, breaking ties by descending model version, predicted orders and predicted revenue. The response includes the selected model version and predicted revenue. The compatibility field `issued_at` is null because the table supplies only the target timestamp.

Daily measurements come from `weather`, keyed by an integer date in YYYYMMDD format.

## Kafka settings

The consumer reads `weather-london` (observations), `weather-predictions-london` (forecast batches), and `predictions` (Spark sales estimates) at `kafka:9092`. Override with `WEATHER_TOPIC`, `WEATHER_FORECAST_TOPIC`, `PREDICTIONS_TOPIC`, and `KAFKA_BOOTSTRAP_SERVERS`.

Weather and predictions use the writer schema ID in the Confluent Avro header. The portal fetches each schema from `KAFKA_SCHEMA_REGISTRY_URL` (default `http://kafka-schema-registry:8081`). If Spark is configured to write raw predictions, set `PREDICTIONS_SCHEMA_ID` to an empty string in both services. The manifest reads it from Spark's ConfigMap and mounts the shared schema. For local raw decoding, point `PREDICTIONS_SCHEMA_FILE` at [the prediction schema](../spark/app/schemas/prediction.avsc).

Spark predictions identify the restaurant, target date and scoring time. The outlook shows the most recently scored estimate for each restaurant and target date in the current weather outlook. Overlapping forecasts can therefore show an estimate from a previous issue until a newer score arrives.

## Editing the dashboard

The dashboard uses Vue 3 with browser ES modules.

Run Flask as described above and refresh the browser after editing.
