The full datasets can be found in `full/`

`split_data.py` splits the dataset into a historical and streaming portion which
is stored in their respective folders (`historical/` and `streaming/`).

`copy_data_hdfs.sh` will copy the historical files into the `/tmp/` folder on the hdfs-cli.
Form here they may manually be put into hdfs by exec into the pod.

```sh
kubectl exec -it hdfs-cli -- /bin/bash
```

once inside the pod

```sh
hdfs dfs -put -f /tmp/orders/*.csv /landing/
hdfs dfs -put -f /tmp/weather/*.csv /landing/
```
