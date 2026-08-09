#!/usr/bin/env bash
set -euo pipefail

# Brings up (or reuses) a k3d cluster for the operator's e2e suite, side-loads
# the operator image built by `make image` plus the Dingo node image the suite
# runs, and installs the CRDs, RBAC and in-cluster manager the Go suite expects
# to already be running.
#
# Deliberately does NOT touch the user's default kubeconfig or
# current-context: this machine may already have another cluster selected
# (e.g. a local k3s cluster at ~/.kube/config), and a test suite that creates
# and deletes namespaces must never be one env var away from hitting it.
# Instead we write a dedicated kubeconfig for this cluster; `make e2e` and the
# Go e2e suite both read KUBECONFIG_PATH (default: .e2e/kubeconfig at the repo
# root).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-dingo-operator-e2e}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.31.5-k3s1}"
IMAGE="${IMAGE:-dingo-operator:latest}"
# The Dingo build the suite's block producer runs. Side-loaded so a fresh
# cluster does not re-pull ~600 MB from ghcr.io on every run. Pinned to a
# concrete release, not a floating tag: combined with the pull-only-if-absent
# check below, ":main" would leave a machine that pulled once testing a stale
# build forever, while CI silently tracked upstream. Keep in sync with
# defaultDingoImage in test/e2e/harness_test.go.
DINGO_IMAGE="${DINGO_IMAGE:-ghcr.io/blinklabs-io/dingo:0.69.0}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dingo-operator-system}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${ROOT_DIR}/.e2e/kubeconfig}"

mkdir -p "$(dirname "${KUBECONFIG_PATH}")"

if k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
  echo "[e2e] cluster ${CLUSTER_NAME} already exists"
else
  echo "[e2e] creating cluster ${CLUSTER_NAME}"
  k3d cluster create "${CLUSTER_NAME}" \
    --image "${K3S_IMAGE}" \
    --servers 1 --agents 0 \
    --k3s-arg '--disable=traefik@server:0' \
    --kubeconfig-update-default=false \
    --wait
fi

if ! docker image inspect "${DINGO_IMAGE}" >/dev/null 2>&1; then
  echo "[e2e] pulling ${DINGO_IMAGE}"
  docker pull "${DINGO_IMAGE}"
fi

echo "[e2e] importing ${IMAGE} and ${DINGO_IMAGE}"
k3d image import "${IMAGE}" "${DINGO_IMAGE}" --cluster "${CLUSTER_NAME}"

echo "[e2e] writing kubeconfig to ${KUBECONFIG_PATH}"
k3d kubeconfig write "${CLUSTER_NAME}" \
  --output "${KUBECONFIG_PATH}" \
  --overwrite \
  --kubeconfig-switch-context=false

export KUBECONFIG="${KUBECONFIG_PATH}"

kubectl wait --for=condition=Ready nodes --all --timeout=120s

# Install the operator itself. Done here rather than in the Go suite so a
# cluster left up with E2E_KEEP_UP=1 is always fully provisioned, and so the
# manager is already reconciling before any test generates a devnet genesis
# (whose systemStart is only seconds in the future).
echo "[e2e] installing CRDs, RBAC and the manager"
manager_existed=0
if kubectl -n "${OPERATOR_NAMESPACE}" get deployment dingo-operator \
  >/dev/null 2>&1; then
  manager_existed=1
fi

kubectl create namespace "${OPERATOR_NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "${ROOT_DIR}/config/crd/bases"
kubectl apply -f "${ROOT_DIR}/config/rbac/role.yaml"
kubectl apply -f "${ROOT_DIR}/test/e2e/manifests/manager.yaml"

# `k3d image import` replaces the image in containerd but never restarts a
# running pod, so on a reused cluster the manager would keep serving the
# previous build. Force it to pick up what was just imported.
if [[ "${manager_existed}" == "1" ]]; then
  kubectl -n "${OPERATOR_NAMESPACE}" rollout restart deployment/dingo-operator
fi
kubectl -n "${OPERATOR_NAMESPACE}" rollout status deployment/dingo-operator \
  --timeout=180s

echo "[e2e] cluster ${CLUSTER_NAME} is ready; kubeconfig: ${KUBECONFIG_PATH}"
