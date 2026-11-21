
## Setup (once)
    ./setup.sh
Applies the `spark-integration` ConfigMap and installs/updates the chart. Needs `kubectl` and `helm`, nothing else.
 
Verify:
    ./deploy.sh k8s/smoke-test.yaml
    kubectl logs -f -n bd-gr-06 job/spark-smoke-test      # prints RESULT and the worker pods that ran the work
Spark UI: `kubectl port-forward -n bd-gr-06 svc/spark-master-svc 8080:80`


## To deploy jobs

    ./deploy.sh <.yaml file>
starts a job from a given .yaml job description, 