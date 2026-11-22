# Sqoop
## Deploy the Sqoop manifest using
```kubectl apply -f sqoop.yaml```

Sqoop will know about the HDFS and is connected to the namenode. 

## Deploy a PostgreSQL database using Helm Chart:
```
helm install postgresql oci://registry-1.docker.io/bitnamicharts/postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=employees \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --set image.repository=bitnamilegacy/postgresql
  ``` 

 The PostgreSQL database is exposed on port 5432, and Sqoop is connected by using the ```--connect <endpoint>``` argument in commands. Endpoint is ```"jdbc:postgresql://postgresql:5432/<db_name>```.
