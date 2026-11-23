# Dingo Operator — Claude Code Guide

Guidance for working in this repository. See [AGENTS.md](AGENTS.md) for coding
and review standards shared by all agents. The design summary, rotation model
and HA model are in [docs/design.md](docs/design.md); the roadmap is below.

## What this is

A Kubernetes operator (Kubebuilder / controller-runtime) that manages
[Dingo](https://github.com/blinklabs-io/dingo) Cardano nodes — relays and block
producers — including KES/opcert rotation, Mithril bootstrap, and topology.
Module: `github.com/blinklabs-io/dingo-operator`.

## Architecture map

| Path | Purpose |
|------|---------|
| `api/v1alpha1/` | `DingoNode` CRD types, `+kubebuilder` markers, generated deepcopy |
| `cmd/dingo-operator/` | Manager entrypoint (leader election, metrics, health) |
| `internal/controller/` | Reconciler: `dingonode_controller.go` (reconcile loop, status), `apply.go` (CreateOrUpdate upserts), `validate.go` (cross-field validation) |
| `internal/resources/` | Pure builders for StatefulSet, Services, ConfigMap, PDB, NetworkPolicy, PodMonitor, ServiceAccount |
| `internal/topology/` | Renders `topology.json` (in-cluster peering + external relays) |
| `internal/forgestatus/` | Scrapes the node's Prometheus metrics for KES/opcert state |
| `internal/onchain/` | Reads the authoritative on-chain opcert counter from the node over node-to-client local-state-query |
| `internal/version/` | ldflags-injected `Version`/`CommitHash` |
| `config/` | Generated CRD (`crd/bases`), RBAC (`rbac`), samples |

The operator Helm chart lives in the separate `helm-charts` repo under
`charts/dingo-operator/`.

## Toolchain (important)

- The module targets `go 1.26.0`; the default `go` on this machine may be
  older. Use `go1.26.1` explicitly, or prepend its GOROOT to `PATH`:
  `export PATH="$(go1.26.1 env GOROOT)/bin:$PATH"` so `controller-gen`'s
  `go list` and `golangci-lint` use the right toolchain.
- `make tools` installs `controller-gen` and `setup-envtest` into `./bin`.

## Common commands

```sh
make manifests generate   # regenerate CRDs, RBAC, deepcopy after api/ changes
make build                # CGO_ENABLED=0 build -> ./dingo-operator
make test                 # go test -race ./... (envtest for controller tests)
make test-envtest         # controller tests, including envtest (CI's envtest job)
make lint                 # golangci-lint run + nilaway + modernize
make run                  # run against the current kubecontext
make e2e                  # k3d + single-BP devnet end-to-end suite (~25 min)
```

After editing anything under `api/`, run `make manifests generate` and commit
the regenerated files (`config/crd/bases/*.yaml`, `config/rbac/role.yaml`,
`api/v1alpha1/zz_generated.deepcopy.go`).

## Testing on the local cluster

- A single-node k3s cluster is available (`kubectl` context `default`).
  It has `local-path` storage only and no cert-manager, prometheus-operator,
  or external-secrets — so PodMonitor creation is gated on CRD presence and
  there are no admission webhooks in v1.
- For end-to-end block production, use the Dingo two-pool devnet in the
  Dingo repo: `internal/test/devnet/` (`run-tests.sh`). It generates fresh
  genesis + pool keys and forges blocks; point a `DingoNode` at it to validate
  rotation/forging.
- `make e2e` is this repo's own end-to-end suite, and the only test that runs a
  real node: envtest has no kubelet, so it never starts a pod. It needs `k3d`
  and Docker, builds and side-loads the operator image, brings up a throwaway
  cluster (`hack/e2e/k3d-up.sh`) and runs `test/e2e/` — a generated single-pool
  devnet forging real blocks, plus the Assisted-rotation roll and rejection
  paths. Use it for any change to key handling, rotation, or workload
  rendering.
  - It cannot touch the k3s cluster above: `k3d-up.sh` passes
    `--kubeconfig-update-default=false` and writes a dedicated kubeconfig to
    `.e2e/` (git-ignored), and the harness refuses to run against any context
    but `k3d-<cluster>`.
  - Two env vars skip teardown and are not interchangeable.
    `E2E_KEEP_UP=1` leaves the cluster *and* every test's namespace up, for
    debugging by hand. `E2E_SKIP_TEARDOWN=1` leaves only the cluster up — this
    is what CI sets so `hack/e2e/collect-diagnostics.sh` has something to
    query, while passing tests still delete their own namespaces so a long run
    does not accumulate them. A failing test always retains its namespace
    regardless of either.
  - Override the node image with `DINGO_IMAGE`; it moves both the side-load in
    `k3d-up.sh` and the pod spec, because the harness falls back to it.
    `E2E_DINGO_IMAGE` still wins if set, for overriding the pod alone.
  - Budget ~25 minutes (measured: `go test` 1362s, whole `make e2e` 1463s,
    against a 55m timeout). The three-layer deadline budget is documented in
    `test/e2e/harness_test.go`.
  - Linting the suite needs two flags, not one. `test/e2e` is almost all
    `_test.go` behind the `e2e` build tag, and `.golangci.yml` sets
    `run.tests=false`, which skips `_test.go` regardless of tags — so the build
    tag alone lints nothing. `make lint` and the lint job in `.github/workflows/ci.yml`
    both pass `--build-tags e2e --tests`. `nilaway` and `modernize` accept
    `-tags` but ignore it; `GOFLAGS=-tags=e2e` is what works for those.

## Conventions & gotchas

- License header: every `.go` file starts with the 13-line Apache 2.0 block
  (`// Copyright 2026 Blink Labs Software`, no SPDX). Generated files use
  `hack/boilerplate.go.txt`.
- Formatting: `golangci-lint fmt` (gci, gofmt, gofumpt, goimports) then
  `golines --max-len=80`. Import order: stdlib, then everything else
  alphabetically (blinklabs before k8s.io before sigs.k8s.io).
- Lint is strict: `golangci-lint` config schema v2; staticcheck SA1019
  (deprecations) fails the build — do not use deprecated APIs. This is why
  resource application uses `controllerutil.CreateOrUpdate` (with explicit
  immutable-field handling) rather than the deprecated `client.Apply` patch
  type. `nilaway` and `modernize` run separately.
- Tests: table-driven with `testify`; controller tests use `envtest`
  (self-skip when `KUBEBUILDER_ASSETS` is unset; CI's `envtest` workflow
  fails on that skip).
- Commits: Conventional Commits, GPG-signed (required for all Blink repos
  except `skunkworks`). Only commit when the user asks.
- securityContext defaults: managed Dingo containers run non-root with
  numeric IDs because `runAsNonRoot` cannot verify the image's named `dingo`
  user. The one exception is the `data-volume-sticky-bit` init container, which
  runs as root with only `CAP_FOWNER` to set the sticky bit on the fsGroup
  data volume root (see README "Data volume permissions"). The canonical
  `ghcr.io/blinklabs-io/dingo` image uses `100:101` for `runAsUser:runAsGroup`
  and `fsGroup` through v0.69.x, including `/ipc` ownership; v0.70.0 and later
  use `1000:1000` for both the process and `/ipc`.
  The operator selects the matching default from the effective image version,
  with an omitted tag resolving to `DefaultDingoTag` (currently v0.85.0).
  Digest-pinned images use `spec.image.capabilityVersion`; existing digest and
  tag pairs retain tag inference only while their digest and tag stay
  unchanged and no capability version was previously declared. A declared
  capability version must remain set while the image is digest-pinned. Custom
  repositories and unrecognized tags without a declared capability version
  retain the conservative `100:101` non-root compatibility default. Set
  `spec.podSecurityContext` when
  a custom or unrecognized image uses different IDs; an explicit context is
  preserved unchanged.
- Cold keys never enter the cluster. The operator issues opcerts by sending
  the signable to a pluggable cold-signer (Bursa); it generates KES/VRF keys
  itself but never holds the cold key.

## Design & roadmap

The design summary, Dingo constraints, rotation state machine, cold signers,
in-place reload, pool lists and HA model are in
[docs/design.md](docs/design.md). Update it in the same change as the behaviour
it describes.

### Roadmap

- Shipped: relays, block producers with `MonitorOnly`, `Assisted` and `Auto`
  rotation (Bursa or Secret cold signer, counter-safe issuance), in-place KES
  reload (`rotation.reloadInPlace`), several pools per node, and `SingleActive`
  HA.
- Not implemented: `ActiveStandby` HA. `validateSpec` rejects it; the intended
  design is in docs/design.md under "HA".
- Upstream still open: dingoctl
  [#14](https://github.com/blinklabs-io/dingoctl/issues/14) (`reload` and
  readiness/leader state).
