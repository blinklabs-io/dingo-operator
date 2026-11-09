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
    tag alone lints nothing. `make lint` and `.github/workflows/golangci-lint.yml`
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
- securityContext defaults: managed Dingo pods always run non-root with
  numeric IDs because `runAsNonRoot` cannot verify the image's named `dingo`
  user. The canonical `ghcr.io/blinklabs-io/dingo` image uses `100:101` for
  `runAsUser:runAsGroup` and `fsGroup` through v0.69.x, including `/ipc`
  ownership; v0.70.0 and later use `1000:1000` for both the process and `/ipc`.
  The operator selects the matching default from the effective image tag, with
  the omitted tag resolving to `DefaultDingoTag` (currently v0.85.0). Custom
  repositories and unrecognized tags retain the conservative `100:101`
  non-root compatibility default. Set `spec.podSecurityContext` when a custom
  or unrecognized image uses different IDs; an explicit context is preserved
  unchanged.
- Cold keys never enter the cluster. The operator issues opcerts by sending
  the signable to a pluggable cold-signer (Bursa); it generates KES/VRF keys
  itself but never holds the cold key.

## Design & roadmap

One CRD, `DingoNode` (`dingo.blinklabs.io/v1alpha1`), with `role: relay |
blockProducer`. A single reconciler branches on role; block-producer-only
concerns (keys, rotation, HA) run behind dedicated internal paths. Each node is
a StatefulSet (+ PVC) with a `dingo mithril sync` init container, headless +
client Services, a topology ConfigMap, and — for block producers — a PDB and a
default-deny NetworkPolicy. Validation is CRD OpenAPI schema + controller-side
checks + status conditions; no admission webhooks in v1 (k3s has no
cert-manager).

### Dingo constraints that shape the design

- KES auto-evolves in memory while forging — no action within an opcert's
  ~93-day window.
- OpCert rotation today = new KES key + new opcert (counter+1, cold-signed) →
  replace mounted files → restart the pod, unless the node reloads keys in
  place (see "In-place reload" below). Dingo reloads its credential files on
  SIGHUP and, with `CARDANO_SHELLEY_KEY_WATCH_INTERVAL`, when they change.
- Startup validation is strict: opcert not expired/future, VRF hash matches the
  pool's on-chain registration, and opcert counter ≥ the on-chain observed
  counter (guards `CounterOverIncrementedOCERT`).
- The authoritative on-chain opcert counter is readable now, over
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
- Dingo's graceful shutdown is budgeted 30s by default, and so is
  Kubernetes'. Dingo traps SIGTERM and flushes its database with a
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
- Dingo v0.70.13+ health probes distinguish process health from chain
  readiness. The operator uses `/healthz` for startup and liveness, so a
  node remains alive while it bootstraps or catches up. `/readyz` drives pod
  readiness and returns ready only when the chain tip is within Dingo's
  `healthReadyGapSlots` tolerance (default 1000 slots). This removes stale
  nodes from client Services without restarting them. The dedicated listener
  is on port 12799 and is not added to either Service.
- Dingo v0.75.0 adds useful sync diagnostics to Prometheus. The existing
  PodMonitor already scrapes `/metrics`, so blockfetch recent delay and fetch
  duration, plus ledger block-apply batch latency, are available without an
  operator API change. These complement the KES/opcert metrics used for status
  and rotation checks.
- Probes cannot see a wedged node, so the controller watches Dingo's own
  wedge gauges. A ledger pipeline that stops following the chain keeps the
  process live and the pod Ready; a block producer in that state goes on
  forging at its leader slots and nobody adopts the result.
  `dingo_ledger_pipeline_stuck` and `dingo_ledger_pipeline_halted` (halted is
  terminal) are scraped for every node, relays included, and drive the
  `LedgerPipelineHealthy` condition plus `Degraded`. `Ready` deliberately still
  reports StatefulSet truth (the desired ready pods *and* a finished rollout:
  `observedGeneration` caught up and `currentRevision` equal to
  `updateRevision`, otherwise `Ready=False`/`RolloutInProgress`): the pod really is ready and the chain really is not
  advancing, and collapsing the two would hide which one failed. An unreadable
  scrape sets the condition `Unknown`, never True — reading a failed fetch as
  "no wedge reported" is how this detection goes blind exactly when it matters.
- The gauges miss a node whose tip stops moving without the pipeline noticing,
  so the controller also tracks `cardano_node_metrics_slotNum_int` (the tip
  block's slot; the lowest across replicas) in `status.tip`. When it stays
  unchanged beyond `--tip-stall-threshold` (default 10m, 0 disables) the
  condition turns False with reason `TipNotAdvancing` and `Degraded` is set;
  Halted and Stuck keep precedence. Status is written every reconcile and each
  write triggers another reconcile, so `status.tip` changes only with the slot
  and the message carries no elapsed time. Any pass that cannot observe the tip
  (scrape failure, missing gauge, slot 0 while Dingo loads its tip) clears
  `status.tip`, restarting the clock. Unlike Halted and Stuck, a stall is not
  kept through an unreadable scrape: the condition turns `Unknown`. On a
  network whose blocks can be more than the threshold apart, raise the
  threshold or disable the check.
- The metrics listener must be bound explicitly. Dingo defaults
  `metricsBindAddr` to loopback as of v0.81.0, so the operator sets
  `DINGO_METRICS_BIND_ADDR=0.0.0.0` on every node. Without it the PodMonitor and
  the operator's own KES and wedge scrapes all fail across the pod boundary and
  the node reports `RotationDue=Unknown` indefinitely. The block-producer and
  relay policies allow this listener only to clients carrying the metrics
  access label. The scrape goes through the *headless* Service: the client
  Service gates endpoints on readiness, which would hide KES and wedge state
  precisely while a node is behind, stalled or catching up.
- Mithril completion is tracked by markers, not by the database file.
  `.mithril-bootstrap-started` and `.mithril-bootstrap-complete` live in the data
  dir. A restore interrupted after `metadata.sqlite` appears but before the chain
  is complete is indistinguishable from success by file existence, and the init
  container used to take the skip branch and hand the node a partial database.
  Resuming is safe: `dingo mithril sync` pins its artifact and records
  per-immutable-file progress, so it continues rather than restarting. Re-running
  unconditionally is not an option, because catch-up against a complete database
  is core-storage-mode only. A volume holding a database and no markers predates
  them and is adopted as complete. `dingo serve` independently refuses to start
  on an incomplete sync, so a marker bug fails loudly rather than forging on
  partial state.
- A Mithril bootstrap peaks at roughly twice the node's steady-state disk.
  `dingo mithril sync` keeps the compressed immutable archives and the extracted
  chain on the data volume simultaneously, reclaiming the archives only once the
  load completes (`CleanupAfterLoad` / `DINGO_MITHRIL_CLEANUP` defaults to
  true, so the operator need not set it). Measured on preview with Dingo 0.69.0:
  34Gi peak, 19Gi steady, ratio 1.8 — 15Gi of that peak was cache, reclaimed
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
- Genesis creation was not restartable before Dingo 0.68.0. Killing a node
  while it is still writing genesis into its data directory left orphaned
  `pool_registration` / `pool_registration_owner` rows in the SQLite metadata
  DB; every subsequent start died on `create genesis pool registration:
  constraint failed: FOREIGN KEY constraint failed` and CrashLooped forever,
  with the PVC unrecoverable without wiping it. Filed as dingo
  [#2959](https://github.com/blinklabs-io/dingo/issues/2959) and fixed in
  0.68.0 by [#2975](https://github.com/blinklabs-io/dingo/pull/2975), which
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

Key-material validation runs for every block producer, in all three modes
(`internal/controller/keys.go`) — `rotation.mode` only governs who *issues* the
certificate, never whether a delivered one is checked before it can reach the
node. `Assisted` is therefore "validate an externally-delivered opcert and roll
the pod onto it":

- Validated: all three files present (the certificate as `opcert.cert` or the
  dingo chart's `node.cert`, identical if both); `kes.skey`/`vrf.skey` are
  well-formed text envelopes of the right kind; the opcert envelope parses and
  its cold signature verifies (`VerifyOpCertSignature`); blake2b-224 of the
  signing cold vkey equals `spec.blockProducer.poolId` (hex or bech32), so
  another pool's self-consistent certificate cannot pass (`validateSpec`
  rejects a block producer without `poolId`); the delivered `kes.skey` is
  exactly 608 bytes and its derived public key equals the certificate's KES
  vkey, so a new opcert shipped without its matching KES key is refused rather
  than CrashLooping the pod on Dingo's own "KES verification key mismatch";
  `vrf.skey` is a 32-byte seed or the 64-byte cardano-cli seed||pubkey form
  (matching `dingo/keystore/keyfile.go`); the counter does not regress below
  `status.opcert.onDiskCounter` or below the on-chain counter (see below); and
  the KES window covers the node's current period.
  - The KES length check must precede the derivation: gouroboros
    `kes.PublicKey` slices the key at fixed depth-6 offsets with no bounds
    check and panics on a short key.
- The KES period checks — both of them — only run once
  `status.kes.currentPeriod > 0`. Zero means "not scraped yet", not "period
  0", and failing on it would refuse a healthy node's own valid keys on the
  first reconcile. The consequence is real and worth knowing: before the
  operator's first successful metrics scrape, an *already-expired* opcert is
  accepted and rolled out, and Dingo's own startup check is what catches it —
  as a CrashLoop. Once the guard is satisfied, the expiry (upper) bound is
  unconditional, because mounting a dead opcert can only CrashLoop the one
  forging pod. The "start period is in the future" lower bound is additionally
  skipped when the delivered counter advances past `onDiskCounter`:
  `status.kes.currentPeriod` is only as fresh as the last successful metrics
  scrape and freezes if scraping breaks, and refusing a correct bundle against a
  stale period would strand the node on keys it can never renew. A forward
  counter is unambiguous evidence of a deliberate rotation, and Dingo still
  rejects a future-dated opcert at startup.
- On acceptance: `KeysValid=True`/`OpCertAccepted`, the certificate's issue
  number is published as `status.opcert.onDiskCounter`, and the keys-checksum
  pod annotation changes, which rolls the pod.
- On rejection: `KeysValid=False`/`OpCertRejected`, `Degraded=True` and phase
  `Degraded` rather than `Forging` (the node keeps running on its loaded keys,
  so `Ready` stays True), plus a Warning Event. The reconcile keeps the live
  pod template of an existing StatefulSet unchanged until the Secret
  validates, so the operator does not roll the pod — not for the keys, and not
  for any other template change, including one introduced by an operator
  upgrade or a spec edit. The checksum already on the live template is also
  carried forward. (An empty checksum would *remove* the annotation — itself a
  template change — and roll the pod onto the rejected keys.) The reconcile
  does not fail: last-known-good is the safe state.
  - What this does and does not protect. An init container copies the Secret
    into a memory-backed `/keys` volume with owner-only permissions. The running
    pod therefore keeps its staged files when kubelet refreshes the source
    Secret, not just the keys already loaded by the process. Refusing a bundle
    still declines only to *initiate* a roll — it does not fence pod recreation.
    An eviction, drain, reschedule, or `kubectl delete pod` stages the
    Secret's current contents, and Dingo's startup validation turns a rejected
    bundle into a CrashLoop. Spec edits and operator upgrades are held back
    until the Secret validates; while one is held, the `KeysValid` and
    `Degraded` messages end with "pod template update held until the keys
    Secret validates". Fix the Secret; do not leave a refused bundle sitting
    in it.
- On a missing keys Secret: `KeysValid=False`/`KeysSecretMissing`,
  `Degraded=True` and a Warning Event. On an existing StatefulSet the live pod
  template and its checksum are kept, as on rejection, so the operator does not
  roll the forging pod onto one stuck in `ContainerCreating` (the Secret volume
  is not optional). Pod recreation is not fenced: a recreated pod cannot start
  until the Secret returns. With no StatefulSet yet, it is still created as
  before, without a keys checksum.
- The on-chain counter floor. `internal/onchain` dials the node's
  node-to-client TCP listener and runs `GetOpCertCounters()`, publishing the
  pool's counter as `status.opcert.onChainCounter` (with
  `status.opcert.onChainCounterAt`). Validation then refuses a delivered opcert
  whose counter is below that value: the chain has already accepted a higher
  certificate, so nothing forged with this one can be adopted. A counter above
  `onchain.MaxPlausibleCounter` is discarded rather than enforced — used as a
  floor, a wrong-but-huge value would invert fail-open into refusing every
  rotation.
  - Fail open, always. Unknown counter → today's behaviour (the on-disk
    floor only). Every way the counter can be missing — node absent, starting,
    syncing, unreachable, `nodeToClient.enabled` false, pool has never minted —
    is a normal state, not evidence against the bundle. Refusing rotations
    whenever the node is unreachable would strand an operator mid-incident, and
    Dingo re-checks the bound at startup regardless. Same reasoning as the
    narrowed KES lower bound above.
  - Stale values are not enforced. Only an observation newer than
    `onChainCounterMaxAge` (15 min, ~7 requeue intervals) is a floor. The bound
    is not about monotonicity — the counter only rises, so an old reading is
    still a valid lower bound — but about provenance: a repoint at another
    network or a rollback breaks the link between the stored number and the node,
    and neither updates it. (A `poolId` edit is handled exactly rather than by
    the age bound — see below.) A failed read never *clears* the value, for the
    same reason `status.kes.currentPeriod` is not cleared.
  - Provenance is recorded, not assumed. A successful read also stores the
    pool it was read for in `status.opcert.onChainCounterPoolId`, and
    `onChainFloor` enforces a counter only for the pool it names. Editing
    `spec.blockProducer.poolId` therefore discards the observation on the next
    reconcile instead of leaving the previous pool's counter enforced until it
    ages out.
  - Ordering: the fetch runs *before* `reconcileResources`, in `Reconcile`
    itself — not in `reconcileStatus` with the metrics scrape, because
    `reconcileResources` is what validates the bundle and acts on the verdict.
    Be exact about what that buys, because it differs by case:
    - StatefulSet already exists — a genuine roll-prevention: the live
      pod template is kept as it is, and the process keeps forging on its loaded keys.
    - Fresh cluster, no StatefulSet yet — nothing is prevented.
      `reconcileResources` applies the StatefulSet unconditionally, the keys
      Secret is mounted, and Dingo CrashLoops on the below-chain opcert exactly
      as it would have. What the ordering buys here is correct signalling —
      `KeysValid=False`, `Degraded`, a Warning Event, no published
      `onDiskCounter` — rather than the operator reporting a healthy rotation it
      had not checked. Same distinction as "refusing declines to initiate a roll,
      it does not fence one" above.
  - One dial per node per `onChainCounterRefreshInterval`
    (`onChainCounterMaxAge/3` = 5 min), keyed on the last attempt, held in
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
  - Observability. The `OnChainCounterAvailable` condition reports
    `Observed` / `QueryFailed` / `NodeNotReady` / `PoolNotOnChain` /
    `InvalidPoolID` / `UnknownNetworkMagic` / `Disabled`, so the
    check cannot be silently inert; `QueryFailed` names the NetworkPolicy label a
    client needs. A failed read whose node has no Service yet reports
    `NodeNotReady` instead — every healthy new block producer passes through that
    state (the first dial happens before the reconcile creates the Service), and
    pointing at a NetworkPolicy label there would be misleading. It is *not*
    Degraded — an unsynced node or a pool that has never minted is normal.
    `nodeToClient.enabled: false` reports `Disabled` rather than removing the
    condition: an absent condition is indistinguishable from a feature that was
    never wired up.
  - Reaching the port. `spec.blockProducer.nodeToClient.enabled` sets
    `CARDANO_PRIVATE_BIND_ADDR=0.0.0.0` (Dingo defaults to loopback, so without
    it no NetworkPolicy change can help), and *while it is true* the block
    producer's NetworkPolicy admits port 3002 from pods labelled
    `dingo.blinklabs.io/node-to-client=allowed` — plus a matching label on the
    *namespace* when the client is in another one. That label is the only way
    in: declared `relayRefs` peers get the node-to-node port
    (`relayService.port`, default 3001) and nothing else. Peering is not
    consent to drive the node as a client, and the rule is gated on the spec
    field so the policy cannot grant a port the operator does
    not query. Both gates are off by default, so the default posture is
    unchanged. The operator's own Deployment must carry that label; it lives
    in the `helm-charts` repo (`charts/dingo-operator/`), not here, so until the
    chart sets it (and labels the operator's namespace) the query fails closed to
    `QueryFailed` on any CNI that enforces NetworkPolicy.
- Still not checked: over-incrementing *past* the chain
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

Only one block producer may forge per pool on a network. When two DingoNodes on
the same network share a pool ID (any namespace, hex or bech32), or share a keys
Secret (same namespace, whatever network each names), the older one
by creation time, then namespace/name, keeps the keys. The newer is set
`Degraded=True` and `Ready=False` with reason `DuplicateForger`, its
StatefulSet is scaled to zero if it exists (across namespaces only when the
older node's keys Secret holds the pool's keys, as described below), and it is
re-checked every requeue interval. One cold key may register the same pool ID on
several networks, so a pool shared across networks is not a conflict. The network
is the one Dingo gets from the node's effective environment, including
`spec.environment` overrides, and for a workload from the environment of its
`dingo` container: `CARDANO_NETWORK_MAGIC` when it is a non-zero number, the
magic of the named `CARDANO_NETWORK` when the magic is unset or zero. A network
that cannot be resolved (an unparseable or out-of-range magic, an unknown name,
a value from a Secret or ConfigMap, `envFrom`, a container command or
arguments, which can override it, or no `dingo` container) matches any
network. Copies of one key bundle under different Secret names count as the
same keys: the pool is also read from the cold key in the opcert. The newer
node resumes only after the older DingoNode is gone and its StatefulSet is
scaled to zero or removed with all its pods finished, since deleting a
DingoNode does not stop its pod at once. Likewise the older node is held until
a newer one already running with the same keys has stopped, and an older node
whose spec fails validation holds the pool only while its workload runs. Any
other block-producer workload still running the same keys, whatever DingoNode
now carries its name, also holds them. A pod holds the pool it staged until it
stops, even after its node's poolId and keys Secret move to another pool: the
pod template records it in `dingo.blinklabs.io/keys-pool` when it is first
rendered or its keys change. A template rendered before that annotation existed
gains it only at its next keys change, normally the next KES rotation, so
adding it does not restart running producers; until then every pod it starts is
judged by its keys Secret and its `dingo.blinklabs.io/keys-checksum`: a pod
whose checksum equals another producer's keys staged the same bundle. Its
keys Secret names its pool only while that checksum equals the Secret's. Once
its keys are replaced, the pod may still forge a pool recorded nowhere, so
until it stops every other block producer is refused, without halting. A
producer already running still rolls when its own pod staged the pool its keys
now name, as for a KES rotation: the pod records that pool, or records no pool
but the keys checksum its keys Secret has now. A producer whose own pod is of
unknown pool too waits, since the other pod may have staged the pool its keys
now name. A refused producer keeps its template, which mounts its keys Secret
by name, so any recreation of its pod, including a `kubectl delete` or the
loss of its Kubernetes node, stages the Secret's current contents. A refused
producer is therefore also scaled to zero when its keys Secret no longer holds
the keys its template was rendered with, unless the new keys are for the pool
the template records, as after a KES rotation; a template that records no pool
halts on any new keys. The same rule applies to a node whose spec fails
validation, such as a block producer without `spec.topology`: its template is
held too, so it is scaled to zero on such a key change while the spec stays
invalid. Two legacy producers whose keys are replaced in the same
window hold each other and are both scaled to zero; the first whose rival pod
stops rolls onto its new keys, and the other resumes, with its pool recorded,
once the first one's old pod has stopped. A pod stays unknown while its own
node's keys are refused or missing, since its template is held. Anyone who may
create a pod can make one unknown, and so keep every block producer that is not
running from starting. A pod that records no keys checksum is judged by its
keys Secret. Besides that held template, only two kinds of
evidence scale a running producer to zero: an older valid DingoNode
naming the same pool or keys, and the workload of a live DingoNode that shares
them, meaning the StatefulSet that DingoNode controls or a pod that StatefulSet
controls, matched by UID. Across namespaces either counts only when that
DingoNode's keys Secret holds the KES signing key its opcert certifies for the
shared pool: a pool ID, or an opcert copied from a block header, is public, so
a DingoNode in another namespace that only names the pool refuses without
halting. Owner references are set by whoever creates the object, so this
trusts anyone who may create pods or StatefulSets in the producer's namespace,
or in the namespace of a DingoNode whose keys Secret holds the pool's keys. A
producer's own pods are only those controlled, by UID, by the StatefulSet it
controls; a pod that only carries its name label is another workload. A
workload no live DingoNode
accounts for, including one whose DingoNode is gone, refuses without halting:
it keeps a producer that is not running from starting, and withholds template
rolls, including KES and opcert rotation, from a running one until it stops or
is removed. Anyone who may create a pod can label and annotate one, so such a
workload stops a running producer only through its held template: when the
producer's keys move to another pool, or change at all under a template that
records no pool, it is scaled to zero as described above. These checks read StatefulSets and
pods from the API server, so the operator needs `list` on pods. A failed lookup
is not a refusal: the reconcile returns the error and leaves the workload as it
is. The exception is another block producer's keys Secret the operator may not
read with its current permissions: only that node's pool ID and Secret name
count, and if its workload is running the reconcile fails closed.

Full `Auto` rotation (`internal/controller/rotate.go`), run after the
metrics scrape on every reconcile:

1. Detect: `RotationDue=True` (`remainingPeriods <= renewBeforePeriods`).
2. Refuse (`OpCertIssued=False`, Warning Event) without calling the signer when
   the on-chain counter is not fresh for this pool
   (`observedOnChainCounter`; an observed 0 counts, unlike `onChainFloor`),
   the KES period has not been scraped, or `renewBeforePeriods` is not below
   the KES window (every new opcert would be due at once). A pool that has
   never minted has no on-chain counter, so it cannot auto-rotate; deliver its
   first opcert with `Assisted`.
3. Skip if the keys Secret already holds a bundle that validates and is not
   due. The running pod keeps reporting its old opcert until it rolls and is
   scraped again, so this check is what keeps one rotation from issuing on
   every reconcile. A staged bundle that fails validation is replaced. A
   dry-run update of the Secret then checks the write is allowed before a
   signature is spent on it.
4. Generate a KES key and ask the cold signer for an opcert at on-chain
   counter + 1, starting at the current KES period
   (`coldsigner.Issuer.SignOpCertFromChain`). Reissuing the same number is
   valid while the pool has not forged with it. Bursa chooses the number from
   the counter and keeps a per-key floor; the operator verifies the issue
   number, the cold key against `coldKeyHash`, and the signature.
5. Run `validateKeysSecret` on the candidate Secret and abort on any failure,
   so self-issued material passes the same checks as a delivered bundle.
6. Update the keys Secret (`kes.skey` and the opcert under the name it already
   uses), carrying the read resourceVersion. `vrf.skey` is untouched.
7. The next reconcile validates the new bundle and rolls the pod.

A failure from step 4 on (signer unreachable or refusing, bundle rejected,
Secret write denied) is remembered per node in memory, and the signer is not
contacted again for `issueRetryInterval` (10 minutes). Without it a signer
timeout would block every reconcile while `RotationDue` holds. A refusal sets
`OpCertIssued=False` and a Warning Event but not `Degraded`: the node is still
forging, and several refusals (counter not yet read, KES not yet scraped) are
normal transient states.

`validateSpec` requires `poolId` for every block producer and, for `Auto`,
that it parses, `nodeToClient.enabled`, and a cold signer that can issue.
Bursa needs an https `endpoint`, `mtlsSecretRef` (a Secret with `tls.crt`, `tls.key` and optionally `ca.crt`) and
`coldKeyHash` equal to the pool ID, and optionally `profile` (the Bursa
signer profile holding the cold key, sent as `profile` on the request; needed
when the signer holds the key in several profiles); `secret` needs `secretRef` holding
`cold.skey` and is refused on mainnet (by name or network magic), since it
keeps the cold key in the cluster. Auto writes the keys Secret, so the deployment must grant the
operator `update` on Secrets in the keys namespaces, on top of `get`.
In every rotation mode, `validateSpec` refuses `coldSigner.profile` with a
signer type other than bursa.

### In-place reload (`rotation.reloadInPlace`)

Off by default. When on, a validated bundle that changes only `kes.skey` and
the opcert is loaded into the running node instead of rolling the pod
(`internal/controller/keyreload.go`, `internal/resources/keyreload.go`).

- Why a container is needed. Dingo reads the owner-only copies in `/keys`, not
  the Secret volume, which the pod's `fsGroup` makes group-readable. The
  kubelet refreshes the Secret volume in place, but nothing refreshes `/keys`.
  `sync-block-producer-keys` runs beside Dingo and does.
- The operator's validation still gates every bundle. The container stages the
  Secret's `kes.skey` and certificate only when SHA-256 of the two equals the
  digest in the `<name>-keys-approval` ConfigMap, which the operator writes
  only for a bundle that validated. It checks the copies against the digest
  again before they replace the live files, since the Secret volume can be
  swapped mid-read. A rejected bundle is never approved, so it is never staged
  into the running pod.
- Which changes qualify. The pod template records
  `dingo.blinklabs.io/keys-static-checksum`, the checksum of every Secret
  entry except `kes.skey` and the opcert (the VRF key, the vote key and
  anything else). A bundle loads in place only when that and the recorded
  pool (`keys-pool`) still match, and the node has reported a loaded opcert
  counter that differs from the bundle's: confirmation is by counter, so a
  bundle reusing the loaded counter, or one approved while the counter is
  unobserved, cannot be confirmed and rolls. Anything else rolls, as does the first
  render after enabling the setting, which adds the container, the approval
  volume and Dingo's watch interval.
- After a roll. The scrape goes through the pod's stable DNS name, so until the
  new pod is scraped, `loadedCounter` may still be the outgoing pod's. Every
  render that changes the live `keys-checksum` (or creates the StatefulSet)
  records the bundle's counter in `status.opcert.rolledCounter`, and so does a
  render whose live template already staged the bundle while the node reports
  another counter (restoring it if the status write after the roll was lost);
  while it is
  set, a bundle loads in place only with a counter above it and the loaded
  one, which neither pod can report before that bundle loads. The scrape
  clears it when the node reports exactly that counter (any other value may be
  the outgoing pod's), or when an in-place reload is confirmed.
- While a reload is pending, the live `keys-checksum` is carried forward, so
  the template does not change, and `status.opcert.inPlaceReload` records the
  approved checksum, counter and time. The scrape reads
  `dingo_forge_opcert_counter` into `status.opcert.loadedCounter`; when it
  equals the approved counter the reload is `applied` and a
  `KeysReloadedInPlace` Event is recorded. If it is not confirmed within
  `keysReloadTimeout` (10 minutes: kubelet refresh, the container's 5s poll,
  Dingo's 10s watch, the 2 minute requeue), the new checksum is rendered and
  the pod rolls, with a `KeysReloadTimedOut` Event. A Dingo without in-place
  reload therefore still rotates, ten minutes later.
- Fencing is unchanged. The pool recorded on the template stays true, since a
  reload never changes the pool, so the forger checks that read `keys-pool`
  hold. A template that keeps an older `keys-checksum` than its Secret is
  judged by its recorded pool, as after any KES rotation.

### HA

- `SingleActive` (default): `replicas: 1` + pod anti-affinity + PDB +
  NetworkPolicy → exactly one forger; rely on reschedule. With one replica the
  PDB blocks every voluntary eviction, so a drain never completes;
  `spec.blockProducer.podDisruptionBudget.enabled: false` removes it.
- `ActiveStandby`: keyless hot standby; the operator holds a Lease and only the
  holder is keyed. Promotion is fenced (old primary confirmed down/keyless)
  so two nodes can never forge. `failover: Automatic | Manual` (default
  Automatic).

### Delivery phases (all on one review branch)

- P0 (done) — scaffold + relays: CRD, manager, workloads, topology (incl.
  external relays), Mithril, full CI + Helm chart.
- P1 (done) — block producers: key mounting, KES/opcert status from metrics,
  `MonitorOnly`/`Assisted` rotation, `SingleActive` HA.
- P2 — full auto rotation: Bursa cold-signer, counter-safe issuance, `Auto`
  mode (pod-restart rollout). Implemented; see above.
- P3 — `ActiveStandby` + credential hot-reload. Hot-reload of a KES rotation
  is implemented as `rotation.reloadInPlace` (see above).

P3 depends on the upstream work below; P2's Bursa backend depends on bursa
#1081.

### Upstream dependencies (filed)

Upstream support now shipped: Dingo
[#2872](https://github.com/blinklabs-io/dingo/issues/2872) (`/healthz` and
`/readyz`), [#2870](https://github.com/blinklabs-io/dingo/issues/2870)
(credential hot-reload, used by `rotation.reloadInPlace`), and Dingo v0.75.0's
blockfetch and ledger latency metrics are wired through the existing
PodMonitor. Remaining Dingo dependencies: dingo
[#2871](https://github.com/blinklabs-io/dingo/issues/2871) (expose
on-chain opcert counter), [#2873](https://github.com/blinklabs-io/dingo/issues/2873)
(SPO forging metrics); bursa
[#1081](https://github.com/blinklabs-io/bursa/issues/1081) (opcert issued from
the on-chain counter via `chain_counter`, which `BursaSigner` sends; bursa
[#592](https://github.com/blinklabs-io/bursa/issues/592), the opcert sign type,
is closed); dingoctl
[#14](https://github.com/blinklabs-io/dingoctl/issues/14)
(`reload` + readiness/leader-state); bark
[#17](https://github.com/blinklabs-io/bark/issues/17) (LifecycleService).

gouroboros [#1890](https://github.com/blinklabs-io/gouroboros/issues/1890) (LSQ
opcert counter) is closed, shipped in v0.189.0, and now in use —
`internal/onchain` calls `GetOpCertCounters()` over node-to-client, so neither
the validation floor nor P2's counter-safe issuance waits on dingo #2871. That
issue is now a convenience (an `/ipc`-free HTTP counter would let the operator
drop the NtC dial and the NetworkPolicy grant it needs), not a blocker.
