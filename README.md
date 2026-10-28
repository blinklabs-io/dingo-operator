# dingo-operator

A Kubernetes operator for [Dingo](https://github.com/blinklabs-io/dingo), the Go
Cardano node. It manages the full lifecycle of Cardano relays and block
producers — including KES/operational-certificate monitoring and rotation,
Mithril bootstrap, topology (with external relays), and active/standby failover
— aimed at secure, production mainnet block production for enterprise stake pool
operators.

> Status: early development (`v1alpha1`). The API may change.

## Features

- **One CRD, two roles.** `DingoNode` runs as a `relay` or a `blockProducer`.
- **Env-var-driven Dingo workloads** as StatefulSets with a persistent DB
  volume, headless + client Services, and an optional PodMonitor.
- **Node health probes** use Dingo's dedicated listener on canonical
  `ghcr.io/blinklabs-io/dingo` images v0.70.13 and later: liveness stays
  healthy during sync, while readiness waits until the chain tip is within
  Dingo's configured slot-gap tolerance. Catching-up or stalled nodes are
  removed from client Services without being restarted. Older and custom
  images keep TCP probes.
- **Mithril bootstrap** via an init container running `dingo mithril sync`
  (native Go client, no external binary).
- **Topology management** — auto-wire in-cluster block-producer/relay peering,
  plus static/external relays merged into local roots.
- **Block-producer key handling** — stages KES/VRF/opcert from a Secret into a
  memory-backed `/keys` volume with mode `0600`, sets the `CARDANO_SHELLEY_*`
  env, and surfaces KES period / opcert state from the node's metrics. A
  delivered bundle is validated (opcert signature, pool binding, KES-key
  binding, counter, KES period) before the operator rolls the pod onto it; a
  refused bundle sets `KeysValid=False` and `Degraded=True` and is not rolled
  out. The certificate may be named `opcert.cert` or `node.cert` (the dingo
  Helm chart's name), so a chart key Secret works unchanged; if both are present
  they must be identical, and it is staged at `/keys/opcert.cert` either way.
  The live pod keeps its staged keys when the Secret changes. Any later pod
  recreation stages the Secret's current contents, so fix a refused bundle
  rather than leaving it in place.
- **Operational metrics** — the optional PodMonitor scrapes Dingo's complete
  Prometheus endpoint, including blockfetch delay and fetch duration, ledger
  block-apply batch latency, and the KES/opcert metrics used for status and
  rotation monitoring.
- **Tracing and logging** — `spec.tracing` exports OpenTelemetry spans over
  OTLP/HTTP (endpoint, service name defaulting to the node name, sample ratio)
  and needs a Dingo image of v0.82.0 or later, which the operator checks before
  rendering. `spec.logging` sets the node's log level and format; `json` suits
  Loki or ELK.
- **On-chain counter floor** — with `blockProducer.nodeToClient.enabled`, the
  operator reads the pool's authoritative opcert counter from the node over
  node-to-client local-state-query, publishes it as
  `status.opcert.onChainCounter`, and refuses a delivered certificate numbered
  below it (the chain would reject blocks forged with it). It fails open: an
  unreachable or unsynced node falls back to the operator's own last accepted
  counter, and the `OnChainCounterAvailable` condition says which applies.
  Reaching the port also needs the label
  `dingo.blinklabs.io/node-to-client=allowed` on the client pod — see the
  NetworkPolicy note below.
- **OpCert rotation** — `MonitorOnly`, `Assisted`, or full `Auto` issuance via a
  pluggable cold-signer (Bursa) that keeps cold keys out of the cluster.
- **Safe HA** — `SingleActive` (default) or `ActiveStandby` with fenced,
  lease-based promotion so exactly one node ever forges.
- **Secure by default** — non-root, read-only root filesystem with writable
  `/tmp` and `/ipc` volumes, dropped capabilities, no privilege escalation,
  RuntimeDefault seccomp, no node service-account token, least-privilege RBAC,
  and ingress NetworkPolicies for all nodes (default-deny for block producers,
  with relay peers limited to node-to-node traffic). Node-to-client (port
  3002) is closed unless a client opts in: label the client pod
  `dingo.blinklabs.io/node-to-client=allowed`, and its namespace too when it is
  not the node's own. That label is the only way in — peers named in
  `topology.relayRefs` get the node-to-node port (`relayService.port`, default
  3001) and nothing else. Dingo also binds node-to-client to loopback until
  `blockProducer.nodeToClient.enabled` is set, so both the listener and the
  policy have to be opened deliberately.
  Metrics also require an explicit grant: label the operator and Prometheus
  client pods `dingo.blinklabs.io/metrics=allowed`, and their namespaces too
  when they differ from the node's. Apply these labels before upgrading;
  otherwise status and KES monitoring cannot scrape nodes. Metrics access does
  not grant node-to-client access.
  Set `spec.image.digest` to pin immutable image content; it overrides the
  tag in container references. Keep `spec.image.tag` set to the pinned image's
  version because the operator uses it to select the UID and health probes.

## Installation

Install the operator via its Helm chart (published to GHCR as an OCI artifact):

```sh
helm install dingo-operator \
  oci://ghcr.io/blinklabs-io/helm-charts/charts/dingo-operator \
  --namespace dingo-operator-system --create-namespace
```

The chart installs the `DingoNode` CRD, the operator Deployment, RBAC, and
(optionally) a PodMonitor. See the chart in
[blinklabs-io/helm-charts](https://github.com/blinklabs-io/helm-charts).

## Quick start

Create a namespace and a relay:

```sh
kubectl create namespace cardano
kubectl apply -f config/samples/dingo_v1alpha1_dingonode_relay.yaml
kubectl get dn -n cardano
```

For a block producer, first create a Secret with your pool credentials in
cardano-cli text-envelope form, then apply the sample:

```sh
kubectl create secret generic producer-pool-keys -n cardano \
  --from-file=vrf.skey --from-file=kes.skey --from-file=opcert.cert
kubectl apply -f config/samples/dingo_v1alpha1_dingonode_blockproducer.yaml
```

## `DingoNode` at a glance

```yaml
apiVersion: dingo.blinklabs.io/v1alpha1
kind: DingoNode
spec:
  role: blockProducer          # relay | blockProducer
  network: mainnet
  storageMode: core
  mithril: { enabled: true }
  topology:
    relayRefs: [relay]
    externalRelays:
      - { address: relay.example.com, port: 3001, valency: 1, trustable: true }
  blockProducer:
    keys: { secretRef: producer-pool-keys }
    rotation: { mode: MonitorOnly, renewBeforePeriods: 8 }
    ha: { strategy: SingleActive, failover: Automatic }
```

See [CLAUDE.md](CLAUDE.md) for the design summary, rotation/HA models, delivery
roadmap, and the upstream issues that unlock full automation, and
[AGENTS.md](AGENTS.md) for coding and review standards.

## Development

```sh
make manifests generate   # regenerate CRDs, RBAC, deepcopy
make build                # build the operator binary
make test                 # run tests (uses envtest)
make lint                 # golangci-lint + nilaway + modernize
make run                  # run the operator against the current kubecontext
```

Requires Go 1.26.10+. A local Cardano devnet for end-to-end testing lives in the
[Dingo repo](https://github.com/blinklabs-io/dingo) under
`internal/test/devnet/`.

## License

Apache-2.0. See [LICENSE](LICENSE).

## Storage operations

Increase `spec.persistence.size` to grow existing node data volumes:

```yaml
spec:
  persistence:
    size: 120Gi
```

The operator patches data PVC requests without replacing the StatefulSet, pods,
PVCs, or bound volumes. The StorageClass and provisioner must support expansion.
Kubernetes and the CSI driver control block-device and filesystem resizing; some
drivers require a pod restart. The operator does not restart a producer to finish
resizing.

`StorageReady` reports capacity independently of pod readiness:

| Reason | Meaning |
| --- | --- |
| `VolumesReady` | Existing data claims have their requested capacity. |
| `ClaimsPending` | An active replica's data claim has not appeared yet. |
| `ExpansionPending` | Capacity or filesystem resizing is pending. |
| `VolumeResizeFailed` | A claim could not be read or expanded; the condition carries the API error. |

Inspect the condition and PVC events when expansion is refused. Unsupported
StorageClasses are not replaced. Decreasing the spec never shrinks existing
volumes; reducing capacity requires a separate data migration.

New StatefulSets name their data claim template `dingo-data`, so claims are
`dingo-data-<name>-<ordinal>`, matching the dingo Helm chart. A DingoNode named
after the chart's StatefulSet adopts its existing claims once that StatefulSet
is removed; the chart names it after the release when the release name contains
`dingo`, and `<release>-dingo` otherwise. Claim templates are immutable, so a
StatefulSet created by an earlier operator version keeps its `data` template and
`data-<name>-<ordinal>` claims. When the operator creates a StatefulSet for a
node whose retained claims exist, it uses their template name, preferring
`dingo-data` when both are present.

Claims are retained when replicas scale down or a DingoNode is deleted. Retained
ordinals are included in later size reconciliations. PVCs do not acquire a
DingoNode owner reference, so deleting a node does not garbage-collect its chain
database. Provisioner reclaim policies still apply to explicit PVC deletion.

StatefulSet claim templates remain immutable at their original size. A new
ordinal on an existing StatefulSet initially requests that template size; the
operator expands its claim on a subsequent reconciliation. Reconciliation polls
every two minutes. Plan replica increases with this asynchronous delay in mind.

### Sizing and health

Size for growth and bootstrap peak usage. Mithril bootstrap can temporarily hold
both compressed archives and extracted chain data. API storage mode builds more
indexes and needs its own capacity and resource budget. Set CPU and memory
requests for scheduling, and leave memory headroom for replay and epoch changes.

Check `Ready`, `StorageReady`, and `LedgerPipelineHealthy` together. A healthy
process or open API listener does not prove the node has caught up to its peers.

## API and Service exposure

Configure API ports once; the operator uses them for Dingo's listener environment,
container ports, Services, and NetworkPolicy. Zero disables a listener. Use an
image supporting the selected APIs (the six-listener configuration below targets
Dingo 0.83.0 or later). API mode needs more storage and memory than core mode.
Midnight also enables its indexer; custom networks need their Midnight parameters
in the environment, while supported named networks use Dingo's defaults.

```yaml
spec:
  role: relay
  network: preview
  image:
    tag: "0.83.0"
  storageMode: api
  mithril:
    enabled: false
  apis:
    utxorpc: 9090
    blockfrost: 3000
    mesh: 8080
    kupo: 1442
    bark: 3003
    midnight: 50051
    ingressFrom:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: api-gateway
        podSelector:
          matchLabels:
            app: gateway
```

The operator creates these stable Services:

| Service | Ports | Exposure |
| --- | --- | --- |
| `<name>-relay` | P2P only | ClusterIP by default; configurable NodePort or LoadBalancer. |
| `<name>-private` | NtC and enabled APIs | ClusterIP only. |
| `<name>-metrics` | Prometheus metrics only | ClusterIP only. |
| `<name>` | Existing relay, NtC, metrics, and enabled API ports | ClusterIP compatibility endpoint. |
| `<name>-headless` | Internal per-pod discovery | Headless; includes unready pods for peering. |

API clients should use `<name>-private`; the headless Service publishes unready
pods and is intended for peer discovery. `spec.relayService.type` can publish P2P
without also publishing APIs or metrics. It also accepts annotations,
externalTrafficPolicy, loadBalancerSourceRanges, nodePort, and hostPort. HostPort
requires CNI support and sufficient hosts for replicas. Keep the original
ClusterIP endpoint while migrating existing clients to the tiered Services.

`spec.podLabels` adds labels to the pod template only (not to selectors,
Services or other objects), for example the alias, azs and group labels a
PodMonitor reads from a node moved from the dingo Helm chart. Keys under
`app.kubernetes.io/` and `dingo.blinklabs.io/`, and `cardano_network` and
`cardano_service`, belong to the operator and are rejected.

The managed NetworkPolicy admits only enabled API ports. `apis.ingressFrom` uses
standard NetworkPolicy peers; each peer's namespace and pod selectors are ANDed.
With no explicit peers, API clients must carry `dingo.blinklabs.io/api=allowed` on
their pod and, for cross-namespace access, their namespace too. This grants no
NtC or metrics access. Existing NtC and metrics opt-ins remain separate. Network
isolation requires a CNI that enforces NetworkPolicy; APIs still need application
authentication or an authenticated gateway where appropriate.

Legacy `DINGO_UTXORPC_PORT`, `DINGO_BLOCKFROST_PORT`, and `DINGO_MESH_PORT`
environment settings, canonical `DINGO_PLUGINS_API_*_CONFIG_PORT` settings, and
Bark/Midnight port variables also drive Service and policy ports when `spec.apis`
is omitted. When `spec.apis` is present, contradictory environment overrides,
port collisions and loopback binding are rejected before resources change.
`CARDANO_PORT`, `CARDANO_PRIVATE_PORT`, `CARDANO_METRICS_PORT` and
`DINGO_HEALTH_PORT` in `spec.environment` are rejected unless they equal the
operator's ports (relay `relayService.port`, default 3001; 3002, 12798, 12799);
container ports, Services, NetworkPolicy and topology dialing use those ports.
Set `relayService.port` to run the relay on another port, for example 3010 to
match a node previously deployed with the dingo Helm chart: it becomes the
container port, the port of every Service that publishes P2P, the NetworkPolicy
port, the readiness-probe port of images without the health listener, and
`CARDANO_PORT` (which is left unset at the default). A node that lists this one in `topology.relayRefs` dials the port
configured on this `DingoNode`; a peer's port change is picked up on its next
periodic reconcile. Publish the relay elsewhere with `relayService.hostPort`
(the container's host port) or `relayService.nodePort` on a NodePort or
LoadBalancer Service.
Core storage mode is rejected only when an API listener is enabled. Kupo uses
`DINGO_PLUGINS_API_KUPO_CONFIG_PORT`.

Set `apis.utxorpcTLSSecretRef` to mount `tls.crt` and `tls.key` from an existing
TLS Secret for UTxORPC. The operator projects only those two files read-only and
sets their paths; certificate/key bytes never enter environment variables.

Storage expansion is asynchronous as described above. API pod readiness still
reports ledger health, rather than merely testing whether a TCP listener opened.

Mithril bootstrap uses the same effective storage mode as the serving container,
including a `DINGO_STORAGE_MODE` environment override. For API nodes, this lets
Dingo backfill historical API metadata before the serving container starts.
Downloading a snapshot alone does not mean an API node is ready: allow the
history backfill to finish, then verify ledger freshness and queries for both
historical and recent data. An interrupted operator-managed bootstrap resumes
using its persisted completion markers; preserve the PVC when restarting it.
