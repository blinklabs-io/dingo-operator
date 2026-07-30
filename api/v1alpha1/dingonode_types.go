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
type ImageSpec struct {
	// +kubebuilder:default="ghcr.io/blinklabs-io/dingo"
	// +optional
	Repository string `json:"repository,omitempty"`
	// Tag defaults to the operator-tested version when empty.
	// +optional
	Tag string `json:"tag,omitempty"`
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// PersistenceSpec configures the blockchain database volume.
type PersistenceSpec struct {
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
	// SecretRef names a Secret holding vrf.skey, kes.skey and opcert.cert in
	// cardano-cli text-envelope JSON form.
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
	// MTLSSecretRef names a Secret with client cert/key/ca for the signer.
	// +optional
	MTLSSecretRef string `json:"mtlsSecretRef,omitempty"`
	// ColdKeyHash is the blake2b224 hash of the cold verification key; the
	// signer selects the matching key.
	// +optional
	ColdKeyHash string `json:"coldKeyHash,omitempty"`
	// SecretRef names a Secret holding the cold key (only when Type=secret).
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
	OnDiskCounter  int64 `json:"onDiskCounter,omitempty"`
	OnChainCounter int64 `json:"onChainCounter,omitempty"`
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
// has accepted. onChainCounter has no producer yet (it needs the node's LSQ
// opcert counter, P2) and a column pointed at it is always empty.
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
