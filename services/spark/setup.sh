#!/usr/bin/env bash
# Installs Spark (Bitnami chart, teacher's values) into YOUR namespace only. No images to build, no registry.
# Usage: ./setup.sh      (optional: NAMESPACE, CHART_VERSION)
set -euo pipefail
NAMESPACE="${NAMESPACE:-bd-gr-06}"
CHART="oci://registry-1.docker.io/bitnamicharts/spark"
CHART_VERSION="${CHART_VERSION:-9.2.10}"
cd "$(dirname "$0")"

kubectl apply -n "$NAMESPACE" -f k8s/config.yaml
helm upgrade --install spark "$CHART" --version "$CHART_VERSION" -n "$NAMESPACE" -f spark-values.yaml --wait --timeout 10m
echo ">> done. Test: ./deploy.sh k8s/smoke-test.yaml && kubectl logs -f -n $NAMESPACE job/spark-smoke-test"
