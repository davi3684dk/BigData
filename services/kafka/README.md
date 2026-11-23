## Deploy the Kafka cluster

Will be using a helm chart from [Bitnami](https://artifacthub.io/packages/helm/bitnami/kafka#bitnami-package-for-apache-kafka) to deploy a Kafka cluster.

```bash
helm install --values kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
```

## Additional deployments of Kafka Connect, Kafka Schema Registry, and Kafka KSQL

We will also use Kafka Connect, Kafka Schema Registry, and Kafka KSQL to facilitate a distributed transport and streaming of records in Kafka. These services are included in the given in the manifest files:

- [kafka-schema-registry.yaml](./kafka-schema-registry.yaml)
- [kafka-connect.yaml](./kafka-connect.yaml)
- [kafka-ksqldb.yaml](./kafka-ksqldb.yaml)

1. Apply the Kafka Schema Registry manifest file to your namespace. `kubectl apply -f kafka-schema-registry.yaml`
2. Apply the Kafka Connect module to your namespace. `kubectl apply -f kafka-connect.yaml`

## Save the streaming topics to HDFS

The `streaming-topics-hdfs-sink.json` connector consumes `restaurant-orders`,
`weather-london`, and `weather-predictions-london`, and writes Avro files under
`/topics/<topic>` in HDFS. It starts from the earliest available Kafka offsets.
Kafka, Schema Registry, Kafka Connect, and HDFS must be deployed in the same
namespace. The Connect worker image must include the HDFS sink plugin.

Forward the Kafka Connect REST API in one PowerShell window:

```powershell
kubectl port-forward svc/kafka-connect 8083:8083
```

In another window, check for the HDFS sink plugin and register the connector:

```powershell
Invoke-RestMethod http://localhost:8083/connector-plugins |
    Select-Object -ExpandProperty class

Invoke-RestMethod `
    -Method Post `
    -Uri http://localhost:8083/connectors `
    -ContentType "application/json" `
    -InFile "services\kafka\streaming-topics-hdfs-sink.json"
```

Check that the connector and its task are running:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/streaming-topics-hdfs-sink/status
```

The connector uses a string converter for Kafka keys and Schema Registry's
Avro converter for values. HDFS files are finalized after 1,000 records or
about one minute. To inspect the output, run `hdfs dfs -ls -R /topics` from
the `hdfs-cli` pod. Connector write-ahead logs are stored under
`/logs/streaming-topics-hdfs-sink`.
