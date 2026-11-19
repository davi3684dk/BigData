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
