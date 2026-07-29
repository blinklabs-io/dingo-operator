#!/usr/bin/env bash
set -euo pipefail

# Tears down the k3d e2e cluster and its dedicated kubeconfig.
#
# Two env vars gate teardown, for two different purposes — do not conflate
# them:
#
#   E2E_KEEP_UP=1        Leaves the cluster AND the per-test namespaces the
#                         Go harness creates (test/e2e/harness_test.go's
#                         createNamespace, which independently checks this
#                         same var in its t.Cleanup) in place. The knob a
#                         developer sets to debug a failed suite by hand.
#
#   E2E_SKIP_TEARDOWN=1  Leaves only the cluster (and kubeconfig) running;
#                         it is NOT read by the Go harness, so `go test`
#                         still deletes its own e2e-* namespaces normally.
#                         This is what CI sets so a later diagnostics-
#                         collection step (hack/e2e/collect-diagnostics.sh)
#                         has a live cluster to query. A *failing* test's
#                         namespace survives too — the harness only deletes
#                         a namespace when its test passed — so the pod
#                         logs and events that matter are still there.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-dingo-operator-e2e}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${ROOT_DIR}/.e2e/kubeconfig}"

if [[ "${E2E_KEEP_UP:-}" == "1" ]]; then
  echo "[e2e] E2E_KEEP_UP=1 — leaving cluster ${CLUSTER_NAME} running"
  exit 0
fi

if [[ "${E2E_SKIP_TEARDOWN:-}" == "1" ]]; then
  echo "[e2e] E2E_SKIP_TEARDOWN=1 — leaving cluster ${CLUSTER_NAME} running" \
    "(namespaces are still cleaned up by the test harness)"
  exit 0
fi

k3d cluster delete "${CLUSTER_NAME}" 2>/dev/null || true
rm -f "${KUBECONFIG_PATH}"
