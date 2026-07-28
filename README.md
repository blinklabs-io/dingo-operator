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
- **Mithril bootstrap** via an init container running `dingo mithril sync`
  (native Go client, no external binary).
- **Topology management** — auto-wire in-cluster block-producer/relay peering,
  plus static/external relays merged into local roots.
- **Block-producer key handling** — mounts KES/VRF/opcert from a Secret at
  `/keys` (mode `0600`), sets the `CARDANO_SHELLEY_*` env, and surfaces KES
  period / opcert state from the node's metrics. A delivered bundle is validated
  (opcert signature, pool binding, KES-key binding, counter, KES period) before
  the operator rolls the pod onto it; a refused bundle sets `KeysValid=False`
  and `Degraded=True` and is not rolled out. Note that the running *process*
  keeps its loaded keys, but `/keys` is a whole-Secret mount, so kubelet does
  refresh the rejected files onto the pod's disk — any subsequent restart from
  any cause starts the node on them.
- **OpCert rotation** — `MonitorOnly`, `Assisted`, or full `Auto` issuance via a
  pluggable cold-signer (Bursa) that keeps cold keys out of the cluster.
- **Safe HA** — `SingleActive` (default) or `ActiveStandby` with fenced,
  lease-based promotion so exactly one node ever forges.
- **Secure by default** — non-root, dropped capabilities, least-privilege RBAC,
  and a default-deny NetworkPolicy for block producers.

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

Requires Go 1.26+. A local Cardano devnet for end-to-end testing lives in the
[Dingo repo](https://github.com/blinklabs-io/dingo) under
`internal/test/devnet/`.

## License

Apache-2.0. See [LICENSE](LICENSE).
