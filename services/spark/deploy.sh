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
#!/usr/bin/env bash
# Publishes app/ code as ConfigMaps, then applies a manifest (Jobs are recreated on every run).
# Usage: ./deploy.sh jobs/host-model.yaml        (optional: NAMESPACE, IMAGE, CHECK_MODE, CHECK_TARGET)
#        FOLLOW=1 ./deploy.sh jobs/host-model.yaml   -> afterwards wait for the pod and stream its logs (Ctrl+C stops following only)
set -euo pipefail
cd "$(dirname "$0")"
NAMESPACE="${NAMESPACE:-bd-gr-06}"
IMAGE="${IMAGE:-docker.io/bitnamilegacy/spark:3.5.2-debian-12-r1}"   # must be the same image as the cluster (spark-values.yaml)
CHECK_MODE="${CHECK_MODE:-hdfs}"; CHECK_TARGET="${CHECK_TARGET:-/topics/restaurant-1}"
[ -f "${1:-}" ] || { echo "usage: ./deploy.sh <manifest.yaml>"; exit 1; }

args=(); for f in app/*.py; do args+=(--from-file="$f"); done
kubectl create configmap spark-app "${args[@]}" -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -n "$NAMESPACE" -f -
if ls app/schemas/*.avsc >/dev/null 2>&1; then
  kubectl create configmap spark-schemas --from-file=app/schemas -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -n "$NAMESPACE" -f -
elif grep -q spark-schemas "$1"; then
  echo "ERROR: $1 needs the ConfigMap spark-schemas but app/schemas/*.avsc is missing in this checkout." >&2
  echo "       Add app/schemas/weather_forecast.avsc and app/schemas/prediction.avsc and run again." >&2
  exit 1
fi

out=$(sed -e "s#\${IMAGE}#$IMAGE#g" -e "s#\${CHECK_MODE}#$CHECK_MODE#g" -e "s#\${CHECK_TARGET}#$CHECK_TARGET#g" "$1")
if grep -q '^kind: Job' <<<"$out"; then kubectl delete -n "$NAMESPACE" --ignore-not-found -f - <<<"$out"; fi
kubectl apply -n "$NAMESPACE" -f - <<<"$out"

if [ "${FOLLOW:-0}" = "1" ]; then
  kind=$(grep -m1 '^kind:' <<<"$out" | awk '{print $2}')
  name=$(sed -n 's/^metadata: {name: \([^,}]*\).*/\1/p' <<<"$out" | head -1)
  case "$kind" in
    Job) sel="job-name=$name"; target="job/$name" ;;
    Deployment) sel="app=$name"; target="deploy/$name" ;;
    *) echo "FOLLOW only supports Job and Deployment manifests"; exit 0 ;;
  esac
  echo ">> waiting for the pod to start ($sel)..."
  if ! kubectl wait -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Running pod -l "$sel" --timeout=180s; then
    echo ">> pod did not start within 3 minutes. Why (see the Events at the bottom):"
    kubectl describe pod -n "$NAMESPACE" -l "$sel" | sed -n '/^Events:/,$p'
    exit 1
  fi
  kubectl logs -n "$NAMESPACE" -f "$target"
fi