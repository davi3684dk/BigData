## Deploy Spark
```bash
helm install --values spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.10
```