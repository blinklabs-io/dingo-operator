# Dingo Operator — Claude Code Guide

Guidance for working in this repository. See [AGENTS.md](AGENTS.md) for coding
and review standards shared by all agents. The design summary, rotation model,
HA model, and roadmap live in the "Design & roadmap" section below.

## What this is

A Kubernetes operator (Kubebuilder / controller-runtime) that manages
[Dingo](https://github.com/blinklabs-io/dingo) Cardano nodes — relays and block
producers — including KES/opcert rotation, Mithril bootstrap, topology, and
active/standby failover. Module: `github.com/blinklabs-io/dingo-operator`.

## Architecture map

| Path | Purpose |
|------|---------|
| `api/v1alpha1/` | `DingoNode` CRD types, `+kubebuilder` markers, generated deepcopy |
| `cmd/dingo-operator/` | Manager entrypoint (leader election, metrics, health) |
| `internal/controller/` | Reconciler: `dingonode_controller.go` (reconcile loop, status), `apply.go` (CreateOrUpdate upserts), `validate.go` (cross-field validation) |
| `internal/resources/` | Pure builders for StatefulSet, Services, ConfigMap, PDB, NetworkPolicy, PodMonitor, ServiceAccount |
| `internal/topology/` | Renders `topology.json` (in-cluster peering + external relays) |
| `internal/forgestatus/` | Scrapes the node's Prometheus metrics for KES/opcert state |
| `internal/version/` | ldflags-injected `Version`/`CommitHash` |
| `config/` | Generated CRD (`crd/bases`), RBAC (`rbac`), samples |

The operator Helm chart lives in the separate `helm-charts` repo under
`charts/dingo-operator/`.

## Toolchain (important)

- The module targets **`go 1.26.0`**; the default `go` on this machine may be
  older. Use **`go1.26.1`** explicitly, or prepend its GOROOT to `PATH`:
  `export PATH="$(go1.26.1 env GOROOT)/bin:$PATH"` so `controller-gen`'s
  `go list` and `golangci-lint` use the right toolchain.
- `make tools` installs `controller-gen` and `setup-envtest` into `./bin`.

## Common commands

```sh
make manifests generate   # regenerate CRDs, RBAC, deepcopy after api/ changes
make build                # CGO_ENABLED=0 build -> ./dingo-operator
make test                 # go test -race ./... (envtest for controller tests)
make lint                 # golangci-lint run + nilaway + modernize
make run                  # run against the current kubecontext
```

After editing anything under `api/`, run `make manifests generate` and commit
the regenerated files (`config/crd/bases/*.yaml`, `config/rbac/role.yaml`,
`api/v1alpha1/zz_generated.deepcopy.go`).

## Testing on the local cluster

- A single-node **k3s** cluster is available (`kubectl` context `default`).
  It has `local-path` storage only and **no cert-manager, prometheus-operator,
  or external-secrets** — so PodMonitor creation is gated on CRD presence and
  there are no admission webhooks in v1.
- For end-to-end block production, use the Dingo **two-pool devnet** in the
  Dingo repo: `internal/test/devnet/` (`run-tests.sh`). It generates fresh
  genesis + pool keys and forges blocks; point a `DingoNode` at it to validate
  rotation/forging.

## Conventions & gotchas

- **License header**: every `.go` file starts with the 13-line Apache 2.0 block
  (`// Copyright 2026 Blink Labs Software`, no SPDX). Generated files use
  `hack/boilerplate.go.txt`.
- **Formatting**: `golangci-lint fmt` (gci, gofmt, gofumpt, goimports) then
  `golines --max-len=80`. Import order: stdlib, then everything else
  alphabetically (blinklabs before k8s.io before sigs.k8s.io).
- **Lint is strict**: `golangci-lint` config schema v2; staticcheck **SA1019
  (deprecations) fails the build** — do not use deprecated APIs. This is why
  resource application uses `controllerutil.CreateOrUpdate` (with explicit
  immutable-field handling) rather than the deprecated `client.Apply` patch
  type. `nilaway` and `modernize` run separately.
- **Tests**: table-driven with `testify`; controller tests use `envtest`
  (self-skip when `KUBEBUILDER_ASSETS` is unset).
- **Commits**: Conventional Commits, GPG-signed (required for all Blink repos
  except `skunkworks`). **Only commit when the user asks.**
- **securityContext defaults**: managed Dingo pods run non-root as the Dingo
  image's baked-in `dingo` user — numeric `runAsUser: 100`, `runAsGroup: 101`,
  `fsGroup: 101` (constants `dingoUID`/`dingoGID` in `internal/resources`). The
  numeric UID is required because the image declares `USER dingo` by name, which
  `runAsNonRoot` alone cannot verify; `/ipc` and the data volume are owned by
  `100:101`. Verified on k3s (relay syncs preview). Override via
  `spec.podSecurityContext` if a future image changes these IDs.
- **Cold keys never enter the cluster.** The operator issues opcerts by sending
  the signable to a pluggable cold-signer (Bursa); it generates KES/VRF keys
  itself but never holds the cold key.

## Design & roadmap

One CRD, `DingoNode` (`dingo.blinklabs.io/v1alpha1`), with `role: relay |
blockProducer`. A single reconciler branches on role; block-producer-only
concerns (keys, rotation, HA) run behind dedicated internal paths. Each node is
a StatefulSet (+ PVC) with a `dingo mithril sync` init container, headless +
client Services, a topology ConfigMap, and — for block producers — a PDB and a
default-deny NetworkPolicy. Validation is CRD OpenAPI schema + controller-side
checks + status conditions; **no admission webhooks in v1** (k3s has no
cert-manager).

### Dingo constraints that shape the design

- KES auto-evolves in memory while forging — no action within an opcert's
  ~93-day window.
- OpCert rotation today = new KES key + new opcert (counter+1, cold-signed) →
  replace mounted files → **restart the pod**. Dingo has no SIGHUP/hot-reload.
- Startup validation is strict: opcert not expired/future, VRF hash matches the
  pool's on-chain registration, and opcert counter ≥ the on-chain observed
  counter (guards `CounterOverIncrementedOCERT`).
- The node already tracks the authoritative on-chain opcert counter internally;
  exposing it over an API is upstream work (see below).
- **Genesis creation was not restartable before Dingo 0.68.0.** Killing a node
  while it is still writing genesis into its data directory left orphaned
  `pool_registration` / `pool_registration_owner` rows in the SQLite metadata
  DB; every subsequent start died on `create genesis pool registration:
  constraint failed: FOREIGN KEY constraint failed` and CrashLooped forever,
  with the PVC unrecoverable without wiping it. Filed as dingo
  [#2959](https://github.com/blinklabs-io/dingo/issues/2959) and **fixed in
  0.68.0** by [#2975](https://github.com/blinklabs-io/dingo/pull/2975), which
  made `SetGenesisStaking` idempotent.
  - This matters to the operator because *every* rotation, config-bundle change
    and reschedule rolls the pod, so a block producer rolled during its first
    boot on a pre-0.68.0 image is permanently bricked. `DefaultDingoTag`
    (`internal/resources/resources.go`) is therefore held at 0.68.0 or later —
    a `DingoNode` that omits `spec.image.tag` gets that value. A spec that
    *pins* an older tag is still exposed.

### OpCert rotation state machine (block producers)

`rotation.mode`: `MonitorOnly` (status/events only), `Assisted` (validate + roll
an externally-delivered opcert), or `Auto`:

1. Detect: `remainingPeriods <= renewBeforePeriods` → `RotationDue`.
2. Resolve the authoritative counter from the node (interim: operator-tracked
   status + the node's startup stale-counter rejection). Target = on-chain + 1.
3. Generate a new KES keypair; compute `OpCertSignableBytes`.
4. Cold-sign via the pluggable signer (Bursa over mTLS). Note Bursa's watermark
   is an equivocation guard, not a monotonic counter guard — the operator
   enforces counter anti-regression against the on-chain value itself.
5. Assemble + verify the opcert (`VerifyOpCertSignature`); abort on mismatch.
6. Write the new `kes.skey` + `opcert.cert` into the keys Secret (0600).
7. Roll in: v1 restarts the pod (fenced for BPs); future hot-reload avoids the
   restart.

### HA

- `SingleActive` (default): `replicas: 1` + pod anti-affinity + PDB +
  NetworkPolicy → exactly one forger; rely on reschedule.
- `ActiveStandby`: keyless hot standby; the operator holds a Lease and only the
  holder is keyed. Promotion is **fenced** (old primary confirmed down/keyless)
  so two nodes can never forge. `failover: Automatic | Manual` (default
  Automatic).

### Delivery phases (all on one review branch)

- **P0 (done)** — scaffold + relays: CRD, manager, workloads, topology (incl.
  external relays), Mithril, full CI + Helm chart.
- **P1 (done)** — block producers: key mounting, KES/opcert status from metrics,
  `MonitorOnly`/`Assisted` rotation, `SingleActive` HA.
- **P2** — full auto rotation: Bursa cold-signer, counter-safe issuance, `Auto`
  mode (pod-restart rollout).
- **P3** — `ActiveStandby` + credential hot-reload.

P2/P3 depend on the upstream work below; the cold-signer interface and
rotation-due monitoring are already in place.

### Upstream dependencies (filed)

Refs to pin as they land: dingo
[#2870](https://github.com/blinklabs-io/dingo/issues/2870) (credential
hot-reload), [#2871](https://github.com/blinklabs-io/dingo/issues/2871) (expose
on-chain opcert counter), [#2872](https://github.com/blinklabs-io/dingo/issues/2872)
(`/healthz`+`/readyz`), [#2873](https://github.com/blinklabs-io/dingo/issues/2873)
(SPO forging metrics); bursa
[#592](https://github.com/blinklabs-io/bursa/issues/592) (opcert CBOR envelope +
external cold-signer sign type); gouroboros
[#1890](https://github.com/blinklabs-io/gouroboros/issues/1890) (LSQ opcert
counter); dingoctl [#14](https://github.com/blinklabs-io/dingoctl/issues/14)
(`reload` + readiness/leader-state); bark
[#17](https://github.com/blinklabs-io/bark/issues/17) (LifecycleService).
