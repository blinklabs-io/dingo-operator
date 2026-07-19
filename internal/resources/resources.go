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

// Package resources builds the Kubernetes objects that make up a managed Dingo
// node (StatefulSet, Services, ConfigMap, PodDisruptionBudget, NetworkPolicy,
// PodMonitor). All builders are pure functions of the DingoNode spec so they
// can be unit tested without a cluster.
package resources

import (
	"fmt"
	"strconv"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

const (
	// AppName is the app.kubernetes.io/name of managed workloads.
	AppName = "dingo"
	// ManagedByName is the app.kubernetes.io/managed-by value.
	ManagedByName = "dingo-operator"
	// ComponentRelay / ComponentBlockProducer are the component labels.
	ComponentRelay         = "relay"
	ComponentBlockProducer = "block-producer"

	// NodeLabel is a stable, operator-owned selector label.
	NodeLabel = "dingo.blinklabs.io/node"
	// TopologyChecksumAnnotation forces a rollout when topology changes.
	TopologyChecksumAnnotation = "dingo.blinklabs.io/topology-checksum"
	// KeysChecksumAnnotation forces a rollout when key material changes.
	KeysChecksumAnnotation = "dingo.blinklabs.io/keys-checksum"

	containerName   = "dingo"
	mithrilInitName = "mithril-sync"

	dataVolumeName = "data"
	dataMountPath  = "/data"

	keysVolumeName = "block-producer-keys"
	keysMountPath  = "/keys"

	topologyVolumeName = "topology"
	topologyMountPath  = "/config"
	topologyFileName   = "topology.json"

	portRelay   = 3001
	portPrivate = 3002
	portMetrics = 12798
	metricsPath = "/metrics"

	// dingoUID / dingoGID are the numeric uid/gid of the "dingo" user baked into
	// the upstream Dingo image (which declares USER dingo by name). Kubernetes
	// needs a numeric runAsUser to satisfy runAsNonRoot, and the node writes its
	// NtC socket under /ipc (owned by this uid/gid in the image), so managed
	// pods default to these values. Override via spec.podSecurityContext.
	dingoUID = 100
	dingoGID = 101
)

// IsBlockProducer reports whether the node forges blocks.
func IsBlockProducer(dn *dingov1alpha1.DingoNode) bool {
	return dn.Spec.Role == dingov1alpha1.RoleBlockProducer
}

// Component returns the component label for the node's role.
func Component(dn *dingov1alpha1.DingoNode) string {
	if IsBlockProducer(dn) {
		return ComponentBlockProducer
	}
	return ComponentRelay
}

// Labels returns the full label set applied to managed objects, following the
// Blink node-chart convention (standard app.kubernetes.io labels plus the
// Cardano-specific cardano_network / cardano_service labels).
func Labels(dn *dingov1alpha1.DingoNode) map[string]string {
	l := SelectorLabels(dn)
	l["app.kubernetes.io/managed-by"] = ManagedByName
	l["app.kubernetes.io/component"] = Component(dn)
	l["cardano_network"] = dn.Spec.Network
	l["cardano_service"] = AppName
	return l
}

// SelectorLabels returns the stable subset used for pod selectors. These must
// never change for a given DingoNode or the StatefulSet becomes unmanageable.
func SelectorLabels(dn *dingov1alpha1.DingoNode) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     AppName,
		"app.kubernetes.io/instance": dn.Name,
		NodeLabel:                    dn.Name,
	}
}

// HeadlessServiceName returns the name of the headless Service backing the
// StatefulSet.
func HeadlessServiceName(dn *dingov1alpha1.DingoNode) string {
	return dn.Name + "-headless"
}

// TopologyConfigMapName returns the name of the topology ConfigMap.
func TopologyConfigMapName(dn *dingov1alpha1.DingoNode) string {
	return dn.Name + "-topology"
}

// imageRef returns the fully-qualified image reference.
func imageRef(dn *dingov1alpha1.DingoNode) string {
	repo := dn.Spec.Image.Repository
	if repo == "" {
		repo = "ghcr.io/blinklabs-io/dingo"
	}
	tag := dn.Spec.Image.Tag
	if tag == "" {
		tag = DefaultDingoTag
	}
	return fmt.Sprintf("%s:%s", repo, tag)
}

// DefaultDingoTag is the Dingo image tag used when the spec omits one. It should
// track a version the operator has been tested against.
const DefaultDingoTag = "0.51.0"

// storageMode returns the effective storage mode string.
func storageMode(dn *dingov1alpha1.DingoNode) string {
	if dn.Spec.StorageMode == "" {
		return string(dingov1alpha1.StorageModeCore)
	}
	return string(dn.Spec.StorageMode)
}

// RenderOptions carry reconcile-time inputs into the workload builders.
type RenderOptions struct {
	// HasTopology indicates a topology ConfigMap should be mounted and
	// CARDANO_TOPOLOGY set.
	HasTopology bool
	// TopologyChecksum triggers a rollout when the topology changes.
	TopologyChecksum string
	// KeysChecksum triggers a rollout when key material changes.
	KeysChecksum string
	// Replicas is the desired StatefulSet replica count.
	Replicas int32
	// MountKeys mounts the block-producer key Secret. Keyless standby pods set
	// this false.
	MountKeys bool
}

// BuildEnv assembles the container environment. Spec.Environment values take
// precedence over operator-derived defaults.
func BuildEnv(dn *dingov1alpha1.DingoNode, opts RenderOptions) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "CARDANO_NETWORK", Value: dn.Spec.Network},
		{Name: "CARDANO_DATABASE_PATH", Value: dataMountPath},
		{Name: "DINGO_STORAGE_MODE", Value: storageMode(dn)},
	}
	if dn.Spec.NetworkMagic != nil {
		env = append(env, corev1.EnvVar{
			Name:  "CARDANO_NETWORK_MAGIC",
			Value: strconv.FormatUint(uint64(*dn.Spec.NetworkMagic), 10),
		})
	}
	if opts.HasTopology {
		env = append(env, corev1.EnvVar{
			Name:  "CARDANO_TOPOLOGY",
			Value: topologyMountPath + "/" + topologyFileName,
		})
	}
	if mountsBlockProducerKeys(dn, opts) {
		bp := dn.Spec.BlockProducer
		env = append(
			env,
			corev1.EnvVar{Name: "CARDANO_BLOCK_PRODUCER", Value: "true"},
			corev1.EnvVar{
				Name:  "CARDANO_SHELLEY_VRF_KEY",
				Value: keysMountPath + "/vrf.skey",
			},
			corev1.EnvVar{
				Name:  "CARDANO_SHELLEY_KES_KEY",
				Value: keysMountPath + "/kes.skey",
			},
			corev1.EnvVar{
				Name:  "CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE",
				Value: keysMountPath + "/opcert.cert",
			},
			corev1.EnvVar{
				Name:  "DINGO_SLOTS_PER_KES_PERIOD",
				Value: strconv.FormatInt(bp.SlotsPerKESPeriod, 10),
			},
			corev1.EnvVar{
				Name:  "DINGO_MAX_KES_EVOLUTIONS",
				Value: strconv.FormatInt(bp.MaxKESEvolutions, 10),
			},
			corev1.EnvVar{
				Name:  "DINGO_FORGE_SYNC_TOLERANCE_SLOTS",
				Value: strconv.FormatInt(bp.ForgeSyncToleranceSlots, 10),
			},
			corev1.EnvVar{
				Name:  "DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS",
				Value: strconv.FormatInt(bp.ForgeStaleGapThresholdSlots, 10),
			},
		)
	}
	return mergeEnv(env, dn.Spec.Environment)
}

// mergeEnv overlays user-provided environment onto defaults, overriding by name
// and appending any new keys in a deterministic order.
func mergeEnv(base []corev1.EnvVar, extra map[string]string) []corev1.EnvVar {
	if len(extra) == 0 {
		return base
	}
	index := make(map[string]int, len(base))
	for i, e := range base {
		index[e.Name] = i
	}
	for _, k := range sortedKeys(extra) {
		if i, ok := index[k]; ok {
			base[i].Value = extra[k]
			continue
		}
		base = append(base, corev1.EnvVar{Name: k, Value: extra[k]})
	}
	return base
}

// podSecurityContext returns the effective pod security context (secure default
// unless overridden by the spec).
func podSecurityContext(
	dn *dingov1alpha1.DingoNode,
) *corev1.PodSecurityContext {
	if dn.Spec.PodSecurityContext != nil {
		return dn.Spec.PodSecurityContext
	}
	return &corev1.PodSecurityContext{
		RunAsNonRoot:        ptr.To(true),
		RunAsUser:           ptr.To(int64(dingoUID)),
		RunAsGroup:          ptr.To(int64(dingoGID)),
		FSGroup:             ptr.To(int64(dingoGID)),
		FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// containerSecurityContext returns the hardened container security context.
func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		// Dingo writes its NtC socket and scratch files to the container
		// filesystem, so the root FS cannot be fully read-only yet.
		ReadOnlyRootFilesystem: ptr.To(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

// BuildStatefulSet constructs the StatefulSet for a DingoNode.
func BuildStatefulSet(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) *appsv1.StatefulSet {
	labels := Labels(dn)
	podAnnotations := map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
		"kubectl.kubernetes.io/default-container":        containerName,
	}
	if opts.TopologyChecksum != "" {
		podAnnotations[TopologyChecksumAnnotation] = opts.TopologyChecksum
	}
	if opts.KeysChecksum != "" {
		podAnnotations[KeysChecksumAnnotation] = opts.KeysChecksum
	}

	container := corev1.Container{
		Name:            containerName,
		Image:           imageRef(dn),
		ImagePullPolicy: pullPolicy(dn),
		Env:             BuildEnv(dn, opts),
		Ports:           containerPorts(),
		VolumeMounts:    volumeMounts(dn, opts),
		Resources:       dn.Spec.Resources,
		SecurityContext: containerSecurityContext(),
		StartupProbe:    tcpProbe(portMetrics, 30, 10),
		LivenessProbe:   tcpProbe(portMetrics, 3, 30),
		ReadinessProbe:  tcpProbe(portRelay, 3, 15),
	}

	podSpec := corev1.PodSpec{
		ServiceAccountName: dn.Name,
		SecurityContext:    podSecurityContext(dn),
		InitContainers:     initContainers(dn, opts),
		Containers:         []corev1.Container{container},
		Volumes:            volumes(dn, opts),
		NodeSelector:       dn.Spec.NodeSelector,
		Tolerations:        dn.Spec.Tolerations,
		Affinity:           affinity(dn),
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name,
			Namespace: dn.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         HeadlessServiceName(dn),
			Replicas:            ptr.To(opts.Replicas),
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Selector: &metav1.LabelSelector{MatchLabels: SelectorLabels(dn)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: podAnnotations,
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{dataPVC(dn)},
		},
	}
}

func pullPolicy(dn *dingov1alpha1.DingoNode) corev1.PullPolicy {
	if dn.Spec.Image.PullPolicy != "" {
		return dn.Spec.Image.PullPolicy
	}
	return corev1.PullIfNotPresent
}

func containerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{Name: "relay", ContainerPort: portRelay, Protocol: corev1.ProtocolTCP},
		{
			Name:          "private",
			ContainerPort: portPrivate,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "metrics",
			ContainerPort: portMetrics,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

func tcpProbe(port int32, failureThreshold, periodSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)},
		},
		PeriodSeconds:    periodSeconds,
		FailureThreshold: failureThreshold,
		TimeoutSeconds:   5,
	}
}

func volumeMounts(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: dataVolumeName, MountPath: dataMountPath},
	}
	if opts.HasTopology {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      topologyVolumeName,
			MountPath: topologyMountPath,
			ReadOnly:  true,
		})
	}
	if mountsBlockProducerKeys(dn, opts) {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      keysVolumeName,
			MountPath: keysMountPath,
			ReadOnly:  true,
		})
	}
	return mounts
}

func volumes(dn *dingov1alpha1.DingoNode, opts RenderOptions) []corev1.Volume {
	var vols []corev1.Volume
	if opts.HasTopology {
		vols = append(vols, corev1.Volume{
			Name: topologyVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: TopologyConfigMapName(dn),
					},
				},
			},
		})
	}
	if mountsBlockProducerKeys(dn, opts) {
		vols = append(vols, corev1.Volume{
			Name: keysVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  dn.Spec.BlockProducer.Keys.SecretRef,
					DefaultMode: ptr.To(int32(0o600)),
				},
			},
		})
	}
	return vols
}

func mountsBlockProducerKeys(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) bool {
	return IsBlockProducer(dn) && opts.MountKeys && dn.Spec.BlockProducer != nil
}

func dataPVC(dn *dingov1alpha1.DingoNode) corev1.PersistentVolumeClaim {
	accessMode := dn.Spec.Persistence.AccessMode
	if accessMode == "" {
		accessMode = corev1.ReadWriteOnce
	}
	size := dn.Spec.Persistence.Size
	if size.IsZero() {
		size = defaultStorageSize()
	}
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   dataVolumeName,
			Labels: map[string]string{"cardano_network": dn.Spec.Network},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{accessMode},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if dn.Spec.Persistence.StorageClass != nil {
		pvc.Spec.StorageClassName = dn.Spec.Persistence.StorageClass
	}
	return pvc
}

func affinity(dn *dingov1alpha1.DingoNode) *corev1.Affinity {
	if dn.Spec.Affinity != nil {
		return dn.Spec.Affinity
	}
	// Default: spread replicas of this node across hosts.
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey: "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: SelectorLabels(dn),
						},
					},
				},
			},
		},
	}
}
