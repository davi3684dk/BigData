## Deploy Hive
Requires HDFS to be deployed firsts

### Hive metastore service

The Hive metastore service will be deployed with PostgreSQL.

Deploy postgres database.

```bash
helm install postgresql \
  --version=12.1.5 \
  --set image.repository=bitnamilegacy/postgresql \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=hive \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql
```