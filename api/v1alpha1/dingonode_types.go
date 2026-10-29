// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Role determines whether a DingoNode runs as a relay or a block producer.
// +kubebuilder:validation:Enum=relay;blockProducer
type Role string

const (
	// RoleRelay is a relay node: it does not forge blocks and needs no keys.
	RoleRelay Role = "relay"
	// RoleBlockProducer is a block-producing (forging) node.
	RoleBlockProducer Role = "blockProducer"
)

// StorageMode selects how much data Dingo persists. "core" is sufficient for
// relays and block producers; "api" is only needed to serve Blockfrost/UTxORPC.
// +kubebuilder:validation:Enum=core;api
type StorageMode string

const (
	StorageModeCore StorageMode = "core"
	StorageModeAPI  StorageMode = "api"
)

// RotationMode controls how aggressively the operator rotates KES keys and
// operational certificates.
// +kubebuilder:validation:Enum=Auto;Assisted;MonitorOnly
type RotationMode string

const (
	// RotationModeAuto performs the full generate -> cold-sign -> roll pipeline.
	RotationModeAuto RotationMode = "Auto"
	// RotationModeAssisted validates and rolls an externally-delivered opcert.
	RotationModeAssisted RotationMode = "Assisted"
	// RotationModeMonitorOnly only surfaces rotation state; it never mutates keys.
	RotationModeMonitorOnly RotationMode = "MonitorOnly"
)

// ColdSignerType selects the cold-signing backend used for opcert issuance.
// +kubebuilder:validation:Enum=bursa;secret;none
type ColdSignerType string

const (
	// ColdSignerBursa delegates cold signing to a Bursa signer service (mTLS).
	ColdSignerBursa ColdSignerType = "bursa"
	// ColdSignerSecret reads a cold key from a Secret. DEV/TESTNET ONLY.
	ColdSignerSecret ColdSignerType = "secret"
	// ColdSignerNone disables issuance (used with Assisted/MonitorOnly).
	ColdSignerNone ColdSignerType = "none"
)

// HAStrategy selects the high-availability model for a block producer.
// +kubebuilder:validation:Enum=SingleActive;ActiveStandby
type HAStrategy string

const (
	// HASingleActive runs exactly one replica and relies on reschedule.
	HASingleActive HAStrategy = "SingleActive"
	// HAActiveStandby runs a keyless hot standby promoted via a Lease.
	HAActiveStandby HAStrategy = "ActiveStandby"
)

// FailoverMode controls whether standby promotion is automatic or manual.
// +kubebuilder:validation:Enum=Automatic;Manual
type FailoverMode string

const (
	FailoverAutomatic FailoverMode = "Automatic"
	FailoverManual    FailoverMode = "Manual"
)

// KeySourceType selects where signing key material is sourced from. Only Secret
// is implemented in v1; the others are reserved so the schema does not break
// when external backends are added.
// +kubebuilder:validation:Enum=Secret;ExternalSecret;CSI
type KeySourceType string

const (
	KeySourceSecret         KeySourceType = "Secret"
	KeySourceExternalSecret KeySourceType = "ExternalSecret"
	KeySourceCSI            KeySourceType = "CSI"
)

// ImageSpec configures the Dingo container image.
// +kubebuilder:validation:XValidation:rule="!has(self.digest) || (has(self.tag) && size(self.tag) > 0)",message="tag must be set when digest is set"
type ImageSpec struct {
	// +kubebuilder:default="ghcr.io/blinklabs-io/dingo"
	// +optional
	Repository string `json:"repository,omitempty"`
	// Tag defaults to the operator-tested version when empty.
	// +optional
	Tag string `json:"tag,omitempty"`
	// Digest pins immutable image content and takes precedence over Tag.
	// Tag is required with Digest for UID and probe selection.
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	// +optional
	Digest string `json:"digest,omitempty"`
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// PersistenceSpec configures the blockchain database volume.
type PersistenceSpec struct {
	// Size of the node's data volume.
	//
	// Size for the Mithril bootstrap *peak*, not the steady-state footprint.
	// "dingo mithril sync" holds the compressed immutable archives and the
	// extracted chain on the volume at the same time, and only reclaims the
	// archives once the load finishes — so the high-water mark is roughly
	// twice what the node settles at. Measured on preview (Dingo 0.69.0,
	// August 2026): 34Gi peak against a 19Gi steady state, a ratio of 1.8.
	// At 60Gi that peak is the binding constraint, so the usable steady-state
	// chain is about 34Gi rather than 60Gi.
	//
	// The default suits preview, which is the only network these figures were
	// taken from. preprod and mainnet carry far more chain and need their own
	// sizing — measure the target network rather than assuming this default
	// scales, and remember a re-bootstrap on an existing volume pays the peak
	// again.
	//
	// A storage class that enforces its request fails the bootstrap with
	// ENOSPC part-way through extraction, which leaves a half-written volume.
	// Hostpath-backed classes such as k3s local-path do not enforce it and
	// will instead consume the node's disk, so an undersized request there
	// shows up as a full node rather than a failed pod.
	// +kubebuilder:default="60Gi"
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
	// StorageClass uses the cluster default when nil.
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`
	// +kubebuilder:default="ReadWriteOnce"
	// +optional
	AccessMode corev1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`
}

// MithrilSpec configures fast bootstrap via "dingo mithril sync".
type MithrilSpec struct {
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// AggregatorURL is auto-detected from the network when empty.
	// +optional
	AggregatorURL string `json:"aggregatorUrl,omitempty"`
	// +kubebuilder:default=true
	// +optional
	VerifyCertificates *bool `json:"verifyCertificates,omitempty"`
	// ForceResync deletes existing DB data before bootstrapping. Dangerous.
	// +optional
	ForceResync bool `json:"forceResync,omitempty"`
}

// ExternalRelay is a static, non-cluster peer to add to the node topology.
type ExternalRelay struct {
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Valency int32 `json:"valency,omitempty"`
	// +optional
	Trustable bool `json:"trustable,omitempty"`
	// +optional
	Advertise bool `json:"advertise,omitempty"`
}

// AccessPoint is a bootstrap/public root peer passthrough entry.
type AccessPoint struct {
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// TopologySpec configures P2P peering.
type TopologySpec struct {
	// AutoPeerRelays auto-wires in-cluster block producer <-> relay peering
	// using RelayRefs (for a relay, using the referenced block producer).
	// +kubebuilder:default=true
	// +optional
	AutoPeerRelays *bool `json:"autoPeerRelays,omitempty"`
	// RelayRefs are names of sibling DingoNode relays (same namespace) to peer.
	// +optional
	RelayRefs []string `json:"relayRefs,omitempty"`
	// ExternalRelays are static/external peers merged into local roots.
	// +optional
	ExternalRelays []ExternalRelay `json:"externalRelays,omitempty"`
	// +optional
	BootstrapPeers []AccessPoint `json:"bootstrapPeers,omitempty"`
	// +optional
	PublicRoots []AccessPoint `json:"publicRoots,omitempty"`
	// +optional
	UseLedgerAfterSlot *int64 `json:"useLedgerAfterSlot,omitempty"`
}

// KeysSpec references the block-producer signing key material.
type KeysSpec struct {
	// +kubebuilder:default=Secret
	// +optional
	SourceType KeySourceType `json:"sourceType,omitempty"`
	// SecretRef names a Secret holding vrf.skey, kes.skey and the operational
	// certificate in cardano-cli text-envelope JSON form. The certificate is
	// opcert.cert or node.cert (the name the dingo Helm chart uses); a Secret
	// may hold both only if they are identical. An optional vote.skey (or the
	// chart's bls.skey) holds the Leios BLS12-381 vote signing key (text
	// envelope or raw hex scalar); when present it is staged with the other keys and wired to
	// DINGO_LEIOS_VOTE_SIGNING_KEY_FILE.
	// +kubebuilder:validation:MinLength=1
	SecretRef string `json:"secretRef"`
}

// ColdSignerSpec configures the pluggable cold-signer used for opcert issuance.
type ColdSignerSpec struct {
	// +kubebuilder:default=bursa
	// +optional
	Type ColdSignerType `json:"type,omitempty"`
	// Endpoint is the Bursa signer URL (required when Type=bursa).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// MTLSSecretRef names a Secret with the signer client certificate and key
	// (tls.crt, tls.key) and optionally the signer CA (ca.crt). Required when
	// Type=bursa and Mode=Auto.
	// +optional
	MTLSSecretRef string `json:"mtlsSecretRef,omitempty"`
	// ColdKeyHash is the blake2b224 hash of the cold verification key; the
	// signer selects the matching key. With Mode=Auto it must equal the pool
	// ID.
	// +optional
	ColdKeyHash string `json:"coldKeyHash,omitempty"`
	// SecretRef names a Secret holding the cold key as a cardano-cli text
	// envelope in cold.skey (only when Type=secret). DEV/TESTNET ONLY.
	// +optional
	SecretRef string `json:"secretRef,omitempty"`
}

// RotationSpec configures KES/OpCert rotation behaviour.
type RotationSpec struct {
	// Mode selects how the operator rotates keys. Note that key-material
	// validation is not mode-dependent: the operator validates the keys Secret
	// (opcert signature, pool binding, counter, KES period) for every block
	// producer, including MonitorOnly, and refuses to roll the pod onto a
	// bundle that fails.
	// +kubebuilder:default=MonitorOnly
	// +optional
	Mode RotationMode `json:"mode,omitempty"`
	// RenewBeforePeriods triggers rotation when remaining KES periods <= N.
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	// +optional
	RenewBeforePeriods int32 `json:"renewBeforePeriods,omitempty"`
	// +optional
	ColdSigner ColdSignerSpec `json:"coldSigner,omitempty"`
}

// HASpec configures block-producer high availability.
type HASpec struct {
	// +kubebuilder:default=SingleActive
	// +optional
	Strategy HAStrategy `json:"strategy,omitempty"`
	// +kubebuilder:default=Automatic
	// +optional
	Failover FailoverMode `json:"failover,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	StandbyReplicas int32 `json:"standbyReplicas,omitempty"`
}

// NodeToClientSpec controls the node's node-to-client (NtC) TCP listener.
//
// Dingo binds NtC to 127.0.0.1:3002 by default, so nothing outside the pod can
// reach it however permissive the NetworkPolicy is. Enabling this binds it to
// all pod interfaces, which is what lets the operator read the authoritative
// on-chain operational-certificate counter over local-state-query — and what
// makes the interface reachable by anything the NetworkPolicy admits.
//
// It is off by default on purpose. NtC carries arbitrary ledger-state queries
// and transaction submission, and the managed NetworkPolicy is only as good as
// the cluster's CNI: on a CNI that does not enforce NetworkPolicy, binding this
// to the pod network exposes it to the whole cluster. Turning it on is
// therefore an explicit, auditable decision recorded in the spec rather than
// an operator default.
type NodeToClientSpec struct {
	// Enabled binds Dingo's node-to-client listener to all pod interfaces
	// (CARDANO_PRIVATE_BIND_ADDR=0.0.0.0) rather than loopback. Access is still
	// governed by the block producer's NetworkPolicy, which admits the port only
	// from clients labelled dingo.blinklabs.io/node-to-client=allowed — declared
	// relay peers get the node-to-node port and nothing more. The policy rule is
	// emitted only while this is true, so the listener and the policy cannot
	// disagree.
	//
	// While this is false the operator cannot query the on-chain opcert counter,
	// so counter validation falls back to the operator's own last accepted
	// counter (status.opcert.onDiskCounter).
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// BlockProducerSpec holds block-producer-only configuration. Required when
// spec.role is blockProducer.
type BlockProducerSpec struct {
	// PoolID (bech32 or hex) is optional and cross-checked against the keys.
	// +optional
	PoolID string `json:"poolId,omitempty"`
	// +kubebuilder:default=129600
	// +kubebuilder:validation:Minimum=1
	// +optional
	SlotsPerKESPeriod int64 `json:"slotsPerKESPeriod,omitempty"`
	// +kubebuilder:default=62
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxKESEvolutions int64 `json:"maxKESEvolutions,omitempty"`
	// +kubebuilder:default=100
	// +optional
	ForgeSyncToleranceSlots int64 `json:"forgeSyncToleranceSlots,omitempty"`
	// +kubebuilder:default=1000
	// +optional
	ForgeStaleGapThresholdSlots int64 `json:"forgeStaleGapThresholdSlots,omitempty"`
	// +optional
	Keys KeysSpec `json:"keys"`
	// +optional
	Rotation RotationSpec `json:"rotation,omitempty"`
	// +optional
	HA HASpec `json:"ha,omitempty"`
	// NodeToClient exposes the node-to-client interface to the cluster so the
	// operator can read the authoritative on-chain opcert counter.
	// +optional
	NodeToClient NodeToClientSpec `json:"nodeToClient,omitempty"`
}

// PodMonitorSpec configures Prometheus Operator scraping.
type PodMonitorSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:default="30s"
	// +optional
	Interval string `json:"interval,omitempty"`
}

// MetricsSpec configures metrics exposure for the managed node.
type MetricsSpec struct {
	// +optional
	PodMonitor PodMonitorSpec `json:"podMonitor,omitempty"`
}

// RelayServiceSpec configures public P2P exposure without publishing APIs or metrics.
type RelayServiceSpec struct {
	// Port is the node-to-node (relay) listener port. It is the container
	// port, the port of every Service that publishes P2P, the NetworkPolicy
	// port and CARDANO_PORT, and the port in-cluster peers dial when they list
	// this node in topology.relayRefs. Images without the health listener are
	// probed for readiness on it; others use the health port. It exists so a
	// node deployed with the dingo Helm chart on a non-default port keeps that
	// port after moving to a DingoNode. Defaults to 3001.
	// +kubebuilder:default=3001
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// +kubebuilder:validation:Enum=Cluster;Local
	// +optional
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicy `json:"externalTrafficPolicy,omitempty"`
	// +optional
	LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty"`
	// NodePort is used only for NodePort or LoadBalancer Services; zero is allocated by Kubernetes.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	NodePort int32 `json:"nodePort,omitempty"`
	// HostPort preserves peer source addresses when supported by the CNI. Zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	HostPort int32 `json:"hostPort,omitempty"`
}

// APISpec configures API listeners and their cluster access. API listeners
// require storageMode=api. Ports also configure the container and Services.
type APISpec struct {
	// UTxORPC is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	UTxORPC int32 `json:"utxorpc,omitempty"`
	// Blockfrost is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Blockfrost int32 `json:"blockfrost,omitempty"`
	// Mesh is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Mesh int32 `json:"mesh,omitempty"`
	// Kupo is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Kupo int32 `json:"kupo,omitempty"`
	// Bark is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Bark int32 `json:"bark,omitempty"`
	// Midnight is the listener port; zero disables it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Midnight int32 `json:"midnight,omitempty"`
	// IngressFrom grants access to enabled API ports using standard NetworkPolicy
	// peers. When empty, only clients labelled dingo.blinklabs.io/api=allowed
	// are admitted; cross-namespace clients must label their namespace too.
	// This does not grant node-to-client, metrics, or relay access.
	// +optional
	IngressFrom []networkingv1.NetworkPolicyPeer `json:"ingressFrom,omitempty"`
	// UTxORPCTLSSecretRef mounts tls.crt and tls.key from a TLS Secret for UTxORPC.
	// The Secret is mounted read-only and its contents never enter environment variables.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +optional
	UTxORPCTLSSecretRef string `json:"utxorpcTLSSecretRef,omitempty"`
}

// TracingSpec configures OpenTelemetry trace export from the managed node.
//
// Spans are sent over OTLP/HTTP. The endpoint, service name and sample ratio
// need a Dingo image that supports them (v0.82.0 or later); the operator
// rejects a spec that enables tracing on an older canonical image rather than
// leaving the node to ignore the settings.
// +kubebuilder:validation:XValidation:rule="(!has(self.endpoint) && !has(self.serviceName) && !has(self.sampleRatio)) || (has(self.enabled) && self.enabled)",message="endpoint, serviceName and sampleRatio require enabled=true"
type TracingSpec struct {
	// Enabled turns on trace export. It is off by default because an exporter
	// with no collector listening logs connection errors continuously.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Endpoint is the OTLP/HTTP collector base URL, for example
	// http://otel-collector.observability:4318. The standard /v1/traces path is
	// appended when the URL has none. When empty, Dingo falls back to the
	// standard OTEL_EXPORTER_OTLP_* variables, which spec.environment can set.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://[^\s]+$`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// ServiceName is the service.name attribute on exported spans. It defaults
	// to the DingoNode name so nodes are distinguishable in the backend.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ServiceName string `json:"serviceName,omitempty"`
	// SampleRatio is the fraction of new traces sampled, from "0" to "1". It is
	// a string because CRD schemas do not carry floating-point fields portably.
	// Dingo's own default applies when empty.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	SampleRatio string `json:"sampleRatio,omitempty"`
}

// LogFormat selects Dingo's log output handler.
// +kubebuilder:validation:Enum=text;json
type LogFormat string

// LogLevel is Dingo's minimum log level.
// +kubebuilder:validation:Enum=debug;info;warn;error
type LogLevel string

// LoggingSpec configures the managed node's log output. Unset fields leave
// Dingo's defaults (info, text) in place.
type LoggingSpec struct {
	// +optional
	Level LogLevel `json:"level,omitempty"`
	// Format json is intended for machine ingestion such as Loki or ELK.
	// +optional
	Format LogFormat `json:"format,omitempty"`
}

// DingoNodeSpec defines the desired state of a DingoNode.
type DingoNodeSpec struct {
	// Role selects relay or blockProducer behaviour.
	//
	// Immutable: a node's identity does not change in place, and the reconciler
	// only ever creates role-specific resources — it has no delete path. Flipping
	// blockProducer -> relay therefore leaves the PodDisruptionBudget and the
	// default-deny NetworkPolicy behind, still restricting a node that is no
	// longer a forger, and freezes status.kes / status.opcert at their last
	// block-producer values because refreshing them is gated on the role. The
	// transition rule applies only on update; creation is unaffected.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="role is immutable"
	Role Role `json:"role"`
	// Network is a named Cardano network (mainnet, preprod, preview, devnet) or
	// "custom" (with networkMagic).
	// +kubebuilder:validation:MinLength=1
	Network string `json:"network"`
	// NetworkMagic is required when network is "custom". It is a 32-bit unsigned
	// value; it is modeled as int64 with an explicit 0..4294967295 range so the
	// CRD accepts the full uint32 space (a uint32 renders as int32 and would
	// reject magics above 2147483647).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	// +optional
	NetworkMagic *int64 `json:"networkMagic,omitempty"`
	// ConfigRef names a ConfigMap holding a Cardano node config.json plus its
	// referenced genesis files (byron/shelley/alonzo/conway-genesis.json), all as
	// sibling keys. When set, the operator mounts it read-only and points Dingo at
	// it via CARDANO_CONFIG. Required for custom networks whose genesis is not
	// built into Dingo; leave empty for named networks (mainnet/preprod/preview).
	// Must be a valid ConfigMap name (RFC 1123 DNS subdomain) so an invalid
	// reference is rejected at admission rather than failing pod creation later.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +optional
	ConfigRef string `json:"configRef,omitempty"`
	// +optional
	Image ImageSpec `json:"image,omitempty"`
	// Replicas applies to relays. Block-producer active count is HA-managed.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// +kubebuilder:default=core
	// +optional
	StorageMode StorageMode `json:"storageMode,omitempty"`
	// APIs enables optional API listeners. Zero ports remain disabled.
	// +optional
	APIs *APISpec `json:"apis,omitempty"`
	// RelayService publishes only P2P on the <name>-relay Service.
	// +optional
	RelayService RelayServiceSpec `json:"relayService,omitempty"`
	// PodLabels are extra labels for the pod template only; they are not added
	// to selectors, Services or other objects. Use them to carry labels that
	// PodMonitors or other tooling read from the pod (for example the alias,
	// azs and group labels of a node moved from the dingo Helm chart). Keys in
	// the operator's own namespaces (app.kubernetes.io/*, dingo.blinklabs.io/*)
	// and cardano_network and cardano_service are rejected.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`
	// +optional
	Persistence PersistenceSpec `json:"persistence,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// TerminationGracePeriodSeconds is how long kubelet waits after SIGTERM
	// before it SIGKILLs the node.
	//
	// This must exceed the time Dingo needs to flush and close its database, or
	// the node is killed mid-write and its next start pays for it with a replay
	// — and on a block producer, restarts are routine rather than exceptional,
	// since every key rotation and config-bundle change rolls the pod. The
	// operator derives Dingo's own shutdown budget from this value (see
	// CARDANO_SHUTDOWN_TIMEOUT in BuildEnv) so the two cannot drift into the
	// default state where Kubernetes and Dingo are given the same 30s and
	// kubelet fires exactly as Dingo's deadline expires.
	//
	// Raise it for a node with a large ledger, whose flush takes longer.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=15
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// PodSecurityContext overrides the operator's secure defaults.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`
	// Environment passes extra CARDANO_*/DINGO_* variables to the container.
	// The operator owns the relay (relayService.port, default 3001),
	// node-to-client (3002), metrics (12798) and health (12799) listener ports:
	// CARDANO_PORT, CARDANO_PRIVATE_PORT, CARDANO_METRICS_PORT and
	// DINGO_HEALTH_PORT are rejected unless they equal those values. Change the
	// relay port with relayService.port; publish it elsewhere with
	// relayService.hostPort (the container's host port) or relayService.nodePort
	// on a NodePort or LoadBalancer Service.
	// +optional
	Environment map[string]string `json:"environment,omitempty"`
	// +optional
	Mithril MithrilSpec `json:"mithril,omitempty"`
	// +optional
	Topology TopologySpec `json:"topology,omitempty"`
	// BlockProducer is required when role is blockProducer.
	// +optional
	BlockProducer *BlockProducerSpec `json:"blockProducer,omitempty"`
	// +optional
	Metrics MetricsSpec `json:"metrics,omitempty"`
	// Tracing configures OpenTelemetry trace export.
	// +optional
	Tracing TracingSpec `json:"tracing,omitempty"`
	// Logging configures the node's log level and format.
	// +optional
	Logging LoggingSpec `json:"logging,omitempty"`
}

// KESStatus reports the KES key lifecycle state observed from the node.
type KESStatus struct {
	CurrentPeriod    int64 `json:"currentPeriod,omitempty"`
	OpCertKESPeriod  int64 `json:"opcertKesPeriod,omitempty"`
	ExpiryPeriod     int64 `json:"expiryPeriod,omitempty"`
	RemainingPeriods int64 `json:"remainingPeriods,omitempty"`
}

// OpCertStatus reports operational certificate counter state.
type OpCertStatus struct {
	OnDiskCounter int64 `json:"onDiskCounter,omitempty"`
	// OnChainCounter is the highest opcert issue number the chain has accepted
	// for this pool, read from the node over node-to-client local-state-query.
	// Zero means "not observed": either the node could not be reached or the
	// pool has no counter on chain yet (it has never minted a block under an
	// operational certificate). Use OnChainCounterAt to tell how fresh
	// the value is — it is never cleared on a failed read, because a stale
	// counter is still a valid lower bound and clearing it would throw away the
	// only floor the operator has.
	OnChainCounter int64 `json:"onChainCounter,omitempty"`
	// OnChainCounterAt is when onChainCounter was last read successfully.
	// +optional
	OnChainCounterAt *metav1.Time `json:"onChainCounterAt,omitempty"`
	// OnChainCounterPoolID is the pool (hex cold-key hash) onChainCounter was
	// read for. It records the observation's provenance: a counter is only used
	// as a validation floor for the pool it was actually read for, so editing
	// spec.blockProducer.poolId discards it immediately instead of leaving it
	// enforced until it ages out.
	// +optional
	OnChainCounterPoolID string `json:"onChainCounterPoolId,omitempty"`
	// +optional
	LastRotated *metav1.Time `json:"lastRotated,omitempty"`
}

// HAStatus reports the high-availability state of a block producer.
type HAStatus struct {
	ActivePod   string   `json:"activePod,omitempty"`
	StandbyPods []string `json:"standbyPods,omitempty"`
	// +optional
	LastPromotion *metav1.Time `json:"lastPromotion,omitempty"`
}

// DingoNodeStatus defines the observed state of a DingoNode.
type DingoNodeStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Phase is a coarse lifecycle summary.
	// +optional
	Phase string `json:"phase,omitempty"`
	// Conditions follows the standard Kubernetes condition conventions.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	KES KESStatus `json:"kes,omitempty"`
	// +optional
	OpCert OpCertStatus `json:"opcert,omitempty"`
	// +optional
	HA HAStatus `json:"ha,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dn,categories=cardano
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Network",type=string,JSONPath=`.spec.network`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Remaining-KES",type=integer,JSONPath=`.status.kes.remainingPeriods`
// OpCert reports onDiskCounter: the counter of the certificate the operator
// has accepted. status.opcert.onChainCounter is populated separately, from the
// node's own node-to-client local-state-query, and is deliberately not a
// column: it is empty until the node is reachable and the pool has minted, so a
// column would read as broken on every healthy new pool.
// Keys surfaces a refused key bundle, which is otherwise invisible in the
// default table: the node keeps forging on what its process already loaded, so
// Phase and readiness both stay green while rotation has stopped.
// +kubebuilder:printcolumn:name="OpCert",type=integer,JSONPath=`.status.opcert.onDiskCounter`
// +kubebuilder:printcolumn:name="Keys",type=string,JSONPath=`.status.conditions[?(@.type=="KeysValid")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DingoNode is the Schema for the dingonodes API.
type DingoNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DingoNodeSpec   `json:"spec,omitempty"`
	Status DingoNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DingoNodeList contains a list of DingoNode.
type DingoNodeList struct {
	metav1.TypeMeta `            json:",inline"`
	metav1.ListMeta `            json:"metadata,omitempty"`
	Items           []DingoNode `json:"items"`
}
