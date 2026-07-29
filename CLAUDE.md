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
make e2e                  # k3d + single-BP devnet end-to-end suite (~15 min)
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
- `make e2e` is this repo's own end-to-end suite, and the only test that runs a
  real node: envtest has no kubelet, so it never starts a pod. It needs **`k3d`
  and Docker**, builds and side-loads the operator image, brings up a throwaway
  cluster (`hack/e2e/k3d-up.sh`) and runs `test/e2e/` — a generated single-pool
  devnet forging real blocks, plus the Assisted-rotation roll and rejection
  paths. Use it for any change to key handling, rotation, or workload
  rendering.
  - It **cannot touch the k3s cluster above**: `k3d-up.sh` passes
    `--kubeconfig-update-default=false` and writes a dedicated kubeconfig to
    `.e2e/` (git-ignored), and the harness refuses to run against any context
    but `k3d-<cluster>`.
  - Two env vars skip teardown and are **not** interchangeable.
    `E2E_KEEP_UP=1` leaves the cluster *and* every test's namespace up, for
    debugging by hand. `E2E_SKIP_TEARDOWN=1` leaves only the cluster up — this
    is what CI sets so `hack/e2e/collect-diagnostics.sh` has something to
    query, while passing tests still delete their own namespaces so a long run
    does not accumulate them. A failing test always retains its namespace
    regardless of either.
  - Override the node image with `DINGO_IMAGE`; it moves both the side-load in
    `k3d-up.sh` and the pod spec, because the harness falls back to it.
    `E2E_DINGO_IMAGE` still wins if set, for overriding the pod alone.
  - Budget ~15 minutes (measured: `go test` ~890s against a 30m timeout). The
    three-layer deadline budget is documented in `test/e2e/harness_test.go`.
  - **Linting the suite needs two flags, not one.** `test/e2e` is almost all
    `_test.go` behind the `e2e` build tag, and `.golangci.yml` sets
    `run.tests=false`, which skips `_test.go` regardless of tags — so the build
    tag alone lints nothing. `make lint` and `.github/workflows/golangci-lint.yml`
    both pass `--build-tags e2e --tests`. `nilaway` and `modernize` accept
    `-tags` but ignore it; `GOFLAGS=-tags=e2e` is what works for those.

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

`rotation.mode`: `MonitorOnly` (status/events only), `Assisted` (see below), or
`Auto`.

**Key-material validation runs for every block producer, in all three modes**
(`internal/controller/keys.go`) — `rotation.mode` only governs who *issues* the
certificate, never whether a delivered one is checked before it can reach the
node. `Assisted` is therefore "validate an externally-delivered opcert and roll
the pod onto it":

- **Validated**: all three files present; `kes.skey`/`vrf.skey` are well-formed
  text envelopes of the right kind; the opcert envelope parses and its
  cold signature verifies (`VerifyOpCertSignature`); blake2b-224 of the signing
  cold vkey equals `spec.blockProducer.poolId` (hex or bech32), so another
  pool's self-consistent certificate cannot pass — **this leg is skipped when
  `poolId` is empty**, which the CRD permits; the delivered `kes.skey` is
  exactly 608 bytes and its derived public key equals the certificate's KES
  vkey, so a new opcert shipped without its matching KES key is refused rather
  than CrashLooping the pod on Dingo's own "KES verification key mismatch";
  `vrf.skey` is a 32-byte seed or the 64-byte cardano-cli seed||pubkey form
  (matching `dingo/keystore/keyfile.go`); the counter does not regress below
  `status.opcert.onDiskCounter`; and the KES window covers the node's current
  period.
  - The KES length check must precede the derivation: gouroboros
    `kes.PublicKey` slices the key at fixed depth-6 offsets with no bounds
    check and panics on a short key.
- **The KES period checks — both of them — only run once
  `status.kes.currentPeriod > 0`.** Zero means "not scraped yet", not "period
  0", and failing on it would refuse a healthy node's own valid keys on the
  first reconcile. The consequence is real and worth knowing: before the
  operator's first successful metrics scrape, an *already-expired* opcert is
  accepted and rolled out, and Dingo's own startup check is what catches it —
  as a CrashLoop. Once the guard is satisfied, the expiry (upper) bound is
  unconditional, because mounting a dead opcert can only CrashLoop the one
  forging pod. The "start period is in the future" lower bound is additionally
  **skipped when the delivered counter advances past `onDiskCounter`**:
  `status.kes.currentPeriod` is only as fresh as the last successful metrics
  scrape and freezes if scraping breaks, and refusing a correct bundle against a
  stale period would strand the node on keys it can never renew. A forward
  counter is unambiguous evidence of a deliberate rotation, and Dingo still
  rejects a future-dated opcert at startup.
- **On acceptance**: `KeysValid=True`/`OpCertAccepted`, the certificate's issue
  number is published as `status.opcert.onDiskCounter`, and the keys-checksum
  pod annotation changes, which rolls the pod.
- **On rejection**: `KeysValid=False`/`OpCertRejected`, `Degraded=True` (the
  only signal in `kubectl get dingonode`, since the node keeps forging and
  readiness stays green), plus a Warning Event. The reconcile carries the
  checksum already on the live pod template forward so the rendered template
  stays byte-identical and **the operator does not roll the pod**. (An empty
  checksum would *remove* the annotation — itself a template change — and roll
  the pod onto the rejected keys.) The reconcile does not fail: last-known-good
  is the safe state.
  - **What this does and does not protect.** Only the running *process* stays
    on its loaded keys. The keys volume is a plain whole-Secret mount with no
    `subPath` (`internal/resources/resources.go`), so kubelet refreshes `/keys`
    in the live pod within about a minute of the Secret changing: the rejected
    material is physically on disk. Refusing a bundle declines to *initiate* a
    roll — it does not fence one. Any other restart (eviction, drain,
    reschedule, OOM, image bump, a `configRef` change, `kubectl delete pod`)
    starts the node on the rejected bundle, and Dingo's startup validation
    turns that into a CrashLoop. Fix the Secret; do not leave a refused bundle
    sitting in it.
- **Not yet checked**: the authoritative on-chain counter, so over-incrementing
  past the chain is still only caught by Dingo at startup. This is now
  implementation work rather than an upstream wait — gouroboros
  `GetOpCertCounters()` shipped in v0.189.0 (see "Upstream dependencies" below);
  wiring it into validation is P2.

Both legs are covered by the envtest controller suite
(`internal/controller/dingonode_controller_test.go`): a valid bundle rolls the
pod and publishes its counter; a bundle signed by another pool's cold key leaves
the pod template byte-identical and marks the node Degraded. They are also
proven end to end against a real forging node in `test/e2e/rotation_test.go`
(`make e2e`) — which is what establishes that the roll actually resumes forging
on the new key, something envtest cannot show.

Full `Auto` rotation:

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
external cold-signer sign type); dingoctl
[#14](https://github.com/blinklabs-io/dingoctl/issues/14)
(`reload` + readiness/leader-state); bark
[#17](https://github.com/blinklabs-io/bark/issues/17) (LifecycleService).

gouroboros [#1890](https://github.com/blinklabs-io/gouroboros/issues/1890) (LSQ
opcert counter) is **closed and shipped in v0.189.0** — `GetOpCertCounters()` /
`DebugChainDepState` are available, so P2's counter-safe issuance no longer waits
on dingo #2871.
