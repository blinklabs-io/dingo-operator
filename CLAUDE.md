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
| `internal/onchain/` | Reads the authoritative on-chain opcert counter from the node over node-to-client local-state-query |
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
- The authoritative on-chain opcert counter is readable **now**, over
  node-to-client local-state-query: gouroboros
  `localstatequery.Client.GetOpCertCounters()` returns a map keyed by pool
  cold-key hash. Dingo serves NtC over TCP on its private port (3002) as well as
  its UNIX socket, so the operator dials the pod and needs no access to `/ipc`.
  Dingo's own `/ipc`-free HTTP surface for this (dingo #2871) is no longer a
  blocker.
  - Two things gate it, both off by default and both documented under "OpCert
    rotation state machine": `spec.blockProducer.nodeToClient.enabled` (Dingo
    binds NtC to `127.0.0.1` unless told otherwise) and the NetworkPolicy label
    `dingo.blinklabs.io/node-to-client=allowed` on the client pod (and its
    namespace, when it differs).
- **Dingo's graceful shutdown is budgeted 30s by default, and so is
  Kubernetes'.** Dingo traps SIGTERM and flushes its database with a
  `shutdownTimeout` that defaults to 30s (`internal/config/config.go`), which is
  exactly Kubernetes' default `terminationGracePeriodSeconds` — so on the
  defaults kubelet SIGKILLs at the moment Dingo's own deadline expires, and a
  flush that uses its budget is cut off. The operator therefore sets the pod's
  grace period (default 60s, `spec.terminationGracePeriodSeconds`) and derives
  Dingo's budget from it as `CARDANO_SHUTDOWN_TIMEOUT`, so the node's deadline is
  always strictly inside the pod's. This matters more here than for most
  workloads: on a block producer, restarts are routine rather than exceptional,
  since every key rotation and config-bundle change rolls the pod.
  - The variable is `CARDANO_`-prefixed, not `DINGO_`. Dingo runs a single
    `envconfig.Process("cardano", ...)`; the `DINGO_*` names the operator sets
    work only because those fields carry an explicit `envconfig:"DINGO_..."`
    tag that envconfig falls back to. `ShutdownTimeout` has no such tag, so
    `DINGO_SHUTDOWN_TIMEOUT` would be silently ignored.
- **A Mithril bootstrap peaks at roughly twice the node's steady-state disk.**
  `dingo mithril sync` keeps the compressed immutable archives and the extracted
  chain on the data volume simultaneously, reclaiming the archives only once the
  load completes (`CleanupAfterLoad` / `DINGO_MITHRIL_CLEANUP` defaults to
  true, so the operator need not set it). Measured on preview with Dingo 0.69.0:
  **34Gi peak, 19Gi steady, ratio 1.8** — 15Gi of that peak was cache, reclaimed
  to 13Mi afterwards. So `spec.persistence.size` must be sized for the peak: at
  the 60Gi default the usable steady-state chain is about 34Gi, not 60Gi.
  - Figures are preview only. preprod and mainnet hold far more chain and need
    their own measurement; do not assume the default scales.
  - Wall clock was ~50 minutes for preview on a warm local cluster
    (~11.3k immutable chunks, then a block copy over ~4.6M blocks at
    ~1470 blocks/sec). That is why Mithril cannot gate a pull request and needs
    a scheduled job instead.
  - An enforcing storage class fails the bootstrap with `ENOSPC` mid-extraction;
    a hostpath class such as k3s `local-path` does not enforce the request and
    consumes the node's disk instead, so undersizing shows up as a full node
    rather than a failed pod.
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
  `status.opcert.onDiskCounter` **or below the on-chain counter** (see below);
  and the KES window covers the node's current period.
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
- **The on-chain counter floor.** `internal/onchain` dials the node's
  node-to-client TCP listener and runs `GetOpCertCounters()`, publishing the
  pool's counter as `status.opcert.onChainCounter` (with
  `status.opcert.onChainCounterAt`). Validation then refuses a delivered opcert
  whose counter is **below** that value: the chain has already accepted a higher
  certificate, so nothing forged with this one can be adopted. A counter above
  `onchain.MaxPlausibleCounter` is discarded rather than enforced — used as a
  floor, a wrong-but-huge value would invert fail-open into refusing every
  rotation.
  - **Fail open, always.** Unknown counter → today's behaviour (the on-disk
    floor only). Every way the counter can be missing — node absent, starting,
    syncing, unreachable, `nodeToClient.enabled` false, pool has never minted —
    is a normal state, not evidence against the bundle. Refusing rotations
    whenever the node is unreachable would strand an operator mid-incident, and
    Dingo re-checks the bound at startup regardless. Same reasoning as the
    narrowed KES lower bound above.
  - **Stale values are not enforced.** Only an observation newer than
    `onChainCounterMaxAge` (15 min, ~7 requeue intervals) is a floor. The bound
    is not about monotonicity — the counter only rises, so an old reading is
    still a valid lower bound — but about provenance: a repoint at another
    network or a rollback breaks the link between the stored number and the node,
    and neither updates it. (A `poolId` edit is handled exactly rather than by
    the age bound — see below.) A failed read never *clears* the value, for the
    same reason `status.kes.currentPeriod` is not cleared.
  - **Provenance is recorded, not assumed.** A successful read also stores the
    pool it was read for in `status.opcert.onChainCounterPoolId`, and
    `onChainFloor` enforces a counter only for the pool it names. Editing
    `spec.blockProducer.poolId` therefore discards the observation on the next
    reconcile instead of leaving the previous pool's counter enforced until it
    ages out.
  - **Ordering: the fetch runs *before* `reconcileResources`,** in `Reconcile`
    itself — not in `reconcileStatus` with the metrics scrape, because
    `reconcileResources` is what validates the bundle and acts on the verdict.
    Be exact about what that buys, because it differs by case:
    - **StatefulSet already exists** — a genuine roll-prevention: the live
      keys-checksum is carried forward, the pod template stays byte-identical,
      and the process keeps forging on its loaded keys.
    - **Fresh cluster, no StatefulSet yet** — nothing is prevented.
      `reconcileResources` applies the StatefulSet unconditionally, the keys
      Secret is mounted, and Dingo CrashLoops on the below-chain opcert exactly
      as it would have. What the ordering buys here is **correct signalling** —
      `KeysValid=False`, `Degraded`, a Warning Event, no published
      `onDiskCounter` — rather than the operator reporting a healthy rotation it
      had not checked. Same distinction as "refusing declines to initiate a roll,
      it does not fence one" above.
  - **One dial per node per `onChainCounterRefreshInterval`**
    (`onChainCounterMaxAge/3` = 5 min), keyed on the last **attempt**, held in
    memory (`onChainAttempts`), *not* on `status.opcert.onChainCounterAt`. The
    difference is the whole point: only the success path writes that timestamp,
    so gating on it leaves the failing case ungated — an operator whose pod is
    not labelled for the NetworkPolicy never succeeds, so it would re-dial every
    pass forever, which is the state this feature ships in until the Helm chart
    sets the label. Per-call timeouts bound one query but not the aggregate:
    reconciles run one at a time (`MaxConcurrentReconciles` is the default 1) and
    a default-deny CNI *drops* rather than refuses, so each attempt costs the
    full 5s dial timeout and enough block producers starve every other node's
    rollout. The remembered attempt also carries its outcome, so a reconcile that
    skips the dial still restores the observation into status and re-asserts the
    condition — which is what keeps a successful read from being lost when
    `reconcileResources` fails after it (the reconcile returns before
    `reconcileStatus` persists anything) and keeps a controller-runtime backoff
    retry from re-dialling. The map is in memory on purpose: it resets on
    operator restart (one fresh read per node, and the persisted status still
    carries the floor), and entries are dropped when a node is deleted. The cost
    of the gate: after a transient node outage the floor stays unavailable for up
    to the refresh interval instead of until the next reconcile, since the
    recovered node is not re-dialled at once. That is well inside
    `onChainCounterMaxAge` and fails open the same way the check already does,
    which is what makes five minutes the accepted trade.
  - **Observability.** The `OnChainCounterAvailable` condition reports
    `Observed` / `QueryFailed` / `NodeNotReady` / `PoolNotOnChain` /
    `PoolIDUnset` / `InvalidPoolID` / `UnknownNetworkMagic` / `Disabled`, so the
    check cannot be silently inert; `QueryFailed` names the NetworkPolicy label a
    client needs. A failed read whose node has no Service yet reports
    `NodeNotReady` instead — every healthy new block producer passes through that
    state (the first dial happens before the reconcile creates the Service), and
    pointing at a NetworkPolicy label there would be misleading. It is *not*
    Degraded — an unsynced node or a pool that has never minted is normal.
    `nodeToClient.enabled: false` reports `Disabled` rather than removing the
    condition: an absent condition is indistinguishable from a feature that was
    never wired up.
  - **Reaching the port.** `spec.blockProducer.nodeToClient.enabled` sets
    `CARDANO_PRIVATE_BIND_ADDR=0.0.0.0` (Dingo defaults to loopback, so without
    it no NetworkPolicy change can help), and *while it is true* the block
    producer's NetworkPolicy admits port 3002 from pods labelled
    `dingo.blinklabs.io/node-to-client=allowed` — plus a matching label on the
    *namespace* when the client is in another one. That label is the **only** way
    in: declared `relayRefs` peers get the node-to-node port (3001) and nothing
    else. Peering is not consent to drive the node as a client, and the rule is
    gated on the spec field so the policy cannot grant a port the operator does
    not query. Both gates are off by default, so the default posture is
    unchanged. **The operator's own Deployment must carry that label**; it lives
    in the `helm-charts` repo (`charts/dingo-operator/`), not here, so until the
    chart sets it (and labels the operator's namespace) the query fails closed to
    `QueryFailed` on any CNI that enforces NetworkPolicy.
- **Still not checked**: over-incrementing *past* the chain
  (`CounterOverIncrementedOCERT`). The operator knows the on-chain value but does
  not cap the delivered counter at `on-chain + 1`, because the value it holds can
  be stale by design and a legitimate multi-step rotation would be refused.
  Dingo's startup validation remains the check for that.

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
opcert counter) is **closed, shipped in v0.189.0, and now in use** —
`internal/onchain` calls `GetOpCertCounters()` over node-to-client, so neither
the validation floor nor P2's counter-safe issuance waits on dingo #2871. That
issue is now a convenience (an `/ipc`-free HTTP counter would let the operator
drop the NtC dial and the NetworkPolicy grant it needs), not a blocker.
