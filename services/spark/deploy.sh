#!/usr/bin/env bash
# Publishes app/ code as ConfigMaps, then applies a manifest (Jobs are recreated on every run).
# Usage: ./deploy.sh k8s/smoke-test.yaml      (optional: NAMESPACE, IMAGE, CHECK_MODE, CHECK_TARGET)
set -euo pipefail
cd "$(dirname "$0")"
NAMESPACE="${NAMESPACE:-bd-gr-06}"
IMAGE="${IMAGE:-docker.io/bitnamilegacy/spark:3.5.2-debian-12-r1}"   # must be the same image as the cluster (spark-values.yaml)
CHECK_MODE="${CHECK_MODE:-hdfs}"; CHECK_TARGET="${CHECK_TARGET:-/topics/restaurant-1}"

args=(); for f in app/*.py; do args+=(--from-file="$f"); done
kubectl create configmap spark-app "${args[@]}" -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -n "$NAMESPACE" -f -
if ls app/schemas/* >/dev/null 2>&1; then
  kubectl create configmap spark-schemas --from-file=app/schemas -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -n "$NAMESPACE" -f -
fi

out=$(sed -e "s#\${IMAGE}#$IMAGE#g" -e "s#\${CHECK_MODE}#$CHECK_MODE#g" -e "s#\${CHECK_TARGET}#$CHECK_TARGET#g" "$1")
if grep -q '^kind: Job' <<<"$out"; then kubectl delete -n "$NAMESPACE" --ignore-not-found -f - <<<"$out"; fi
kubectl apply -n "$NAMESPACE" -f - <<<"$out"
