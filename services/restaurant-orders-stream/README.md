## Run as a Kubernetes Job

From this directory,
create a ConfigMap containing the script, its CSV inputs, and the dependency
list:

```powershell
kubectl create configmap restaurant-orders-stream-files --from-file=stream-restaurant-orders.py --from-file=restaurant-1-orders-streaming.csv --from-file=restaurant-2-orders-streaming.csv --from-file=requirements.txt --dry-run=client -o yaml | kubectl apply --server-side -f -
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