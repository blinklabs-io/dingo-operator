#!/usr/bin/env bash
# Best-effort diagnostics collector for the k3d e2e suite. Runs as a
# `if: failure()` step in CI, so it must never fail the job itself: no `-e`,
# and every command that talks to the cluster is followed by `|| true`.
#
# Deliberately does NOT collect the keys Secret: it's throwaway devnet
# material, but dumping Secrets into CI artifacts is a habit worth not
# forming.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# This script runs as its own step, outside the `make e2e` invocation that
# exports KUBECONFIG for the Go test process. Point it at the same dedicated
# e2e kubeconfig `hack/e2e/k3d-up.sh` writes, so kubectl doesn't fall back to
# the user's/runner's default kubeconfig (or none at all) and silently
# produce empty artifacts.
export KUBECONFIG="${KUBECONFIG:-${ROOT_DIR}/.e2e/kubeconfig}"

OUT="${OUT:-/tmp/e2e-diagnostics}"
mkdir -p "${OUT}"

kubectl get all --all-namespaces > "${OUT}/all-resources.txt" 2>&1 || true
kubectl get dingonodes -A -o yaml > "${OUT}/dingonodes.yaml" 2>&1 || true
kubectl -n dingo-operator-system logs deployment/dingo-operator \
  > "${OUT}/operator.log" 2>&1 || true

for ns in $(kubectl get ns -o name 2>/dev/null | grep -o 'e2e-.*'); do
  kubectl -n "${ns}" get events --sort-by=.lastTimestamp \
    > "${OUT}/${ns}-events.txt" 2>&1 || true
  kubectl -n "${ns}" describe pod > "${OUT}/${ns}-pods.txt" 2>&1 || true
  kubectl -n "${ns}" describe statefulset \
    > "${OUT}/${ns}-statefulsets.txt" 2>&1 || true
  kubectl -n "${ns}" get configmap devnet-config -o yaml \
    > "${OUT}/${ns}-devnet-config.yaml" 2>&1 || true
  for pod in $(kubectl -n "${ns}" get pods -o name 2>/dev/null); do
    pod="${pod#pod/}"
    kubectl -n "${ns}" logs "${pod}" > "${OUT}/${ns}-${pod}.log" 2>&1 || true
    # Matters after a rotation roll (Task 9): the interesting failure is in
    # the container that just died, not the one that replaced it.
    kubectl -n "${ns}" logs "${pod}" --previous \
      > "${OUT}/${ns}-${pod}-previous.log" 2>&1 || true
  done
done

exit 0
