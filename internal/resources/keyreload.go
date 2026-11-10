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

package resources

import (
	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// KeysStaticChecksumAnnotation records, on a pod template that reloads
	// keys in place, the checksum of the key material a reload cannot change:
	// every keys Secret entry except the KES key and opcert. The operator
	// loads a new KES key and opcert into the running pod only while this
	// still matches the keys Secret.
	KeysStaticChecksumAnnotation = "dingo.blinklabs.io/keys-static-checksum"
	// KeysApprovalKey is the key of the approval ConfigMap that holds the
	// digest of the KES key and opcert the operator approved.
	KeysApprovalKey = "approved"
	// KeyWatchIntervalEnv is Dingo's credential file watch interval.
	KeyWatchIntervalEnv = "CARDANO_SHELLEY_KEY_WATCH_INTERVAL"
	// keyWatchInterval is how often Dingo checks its key files for a change.
	keyWatchInterval = "10s"

	keysSyncName           = "sync-block-producer-keys"
	keysApprovalVolumeName = "block-producer-keys-approval"
	keysApprovalPath       = "/var/run/dingo-keys-approval"
)

// syncBlockProducerKeysScript stages a new KES key and opcert from the keys
// Secret into the node's key directory, once the operator has approved them.
// The Secret volume refreshes in place, but its files can be read by the pod's
// fsGroup, so Dingo reads owner-only copies; this keeps those copies current
// without restarting the pod.
//
// It copies only when the SHA-256 of the Secret's kes.skey followed by its
// certificate equals the digest in the approval ConfigMap, so a bundle the
// operator has not validated, or has rejected, is never staged. The copies are
// checked against the digest again before they replace the live files, since
// the Secret volume can be swapped while they are read. The certificate is
// chosen as the init container chooses it.
//
// The container runs this as PID 1, which ignores SIGTERM unless it installs a
// handler, so the script traps it and sleeps in the background where the trap
// can interrupt the wait; otherwise every pod stop would wait out the grace
// period.
const syncBlockProducerKeysScript = `set -u
trap 'exit 0' TERM INT
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
approval="${DINGO_KEYS_APPROVAL:-/var/run/dingo-keys-approval/approved}"
interval="${DINGO_KEYS_SYNC_INTERVAL:-5}"
umask 077
digest() {
  cat "$@" 2>/dev/null | sha256sum | cut -d' ' -f1
}
while :; do
  sleep "${interval}" &
  wait "$!"
  [ -s "${approval}" ] || continue
  want="$(cat "${approval}")"
  cert=node.cert
  if [ -s "${source_dir}/opcert.cert" ]; then
    cert=opcert.cert
  fi
  [ "$(digest "${source_dir}/kes.skey" "${source_dir}/${cert}")" = "${want}" ] ||
    continue
  [ "$(digest "${destination_dir}/kes.skey" "${destination_dir}/opcert.cert")" = "${want}" ] &&
    continue
  rm -f "${destination_dir}/.kes.skey.new" "${destination_dir}/.opcert.cert.new"
  cp "${source_dir}/kes.skey" "${destination_dir}/.kes.skey.new" &&
    cp "${source_dir}/${cert}" "${destination_dir}/.opcert.cert.new" &&
    chmod 0600 "${destination_dir}/.kes.skey.new" \
      "${destination_dir}/.opcert.cert.new" || continue
  [ "$(digest "${destination_dir}/.kes.skey.new" "${destination_dir}/.opcert.cert.new")" = "${want}" ] ||
    continue
  mv -f "${destination_dir}/.kes.skey.new" "${destination_dir}/kes.skey" &&
    mv -f "${destination_dir}/.opcert.cert.new" "${destination_dir}/opcert.cert" &&
    echo "staged the approved KES key and opcert"
done
`

// syncSigningAgentKeysScript is syncBlockProducerKeysScript for a block
// producer that signs through a remote agent. The agent holds the KES key, so
// only the opcert is staged, and the approval digest is the SHA-256 of the
// opcert alone; a kes.skey left in the Secret is never read. It is a separate
// script so the staging container of a node with a local KES key stays
// byte-identical.
const syncSigningAgentKeysScript = `set -u
trap 'exit 0' TERM INT
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
approval="${DINGO_KEYS_APPROVAL:-/var/run/dingo-keys-approval/approved}"
interval="${DINGO_KEYS_SYNC_INTERVAL:-5}"
umask 077
digest() {
  cat "$@" 2>/dev/null | sha256sum | cut -d' ' -f1
}
while :; do
  sleep "${interval}" &
  wait "$!"
  [ -s "${approval}" ] || continue
  want="$(cat "${approval}")"
  cert=node.cert
  if [ -s "${source_dir}/opcert.cert" ]; then
    cert=opcert.cert
  fi
  [ "$(digest "${source_dir}/${cert}")" = "${want}" ] || continue
  [ "$(digest "${destination_dir}/opcert.cert")" = "${want}" ] && continue
  rm -f "${destination_dir}/.opcert.cert.new"
  cp "${source_dir}/${cert}" "${destination_dir}/.opcert.cert.new" &&
    chmod 0600 "${destination_dir}/.opcert.cert.new" || continue
  [ "$(digest "${destination_dir}/.opcert.cert.new")" = "${want}" ] ||
    continue
  mv -f "${destination_dir}/.opcert.cert.new" "${destination_dir}/opcert.cert" &&
    echo "staged the approved opcert"
done
`

// syncKeysScript returns the staging script for dn, chosen by the spec as
// keysScript chooses the init script: a node that signs through an agent never
// stages a kes.skey.
func syncKeysScript(dn *dingov1alpha1.DingoNode) string {
	if UsesSigningAgent(dn) {
		return syncSigningAgentKeysScript
	}
	return syncBlockProducerKeysScript
}

// ReloadsKeysInPlace reports whether a block producer loads a new KES key and
// opcert into the running pod instead of rolling it.
func ReloadsKeysInPlace(dn *dingov1alpha1.DingoNode) bool {
	bp := dn.Spec.BlockProducer
	return IsBlockProducer(dn) && bp != nil && bp.Rotation.ReloadInPlace
}

// reloadsKeysInPlace reports whether this render stages keys and reloads them
// in place.
func reloadsKeysInPlace(dn *dingov1alpha1.DingoNode, opts RenderOptions) bool {
	return mountsBlockProducerKeys(dn, opts) && ReloadsKeysInPlace(dn)
}

// KeysApprovalConfigMapName returns the ConfigMap holding the approved key
// digest of a node that reloads keys in place.
func KeysApprovalConfigMapName(dn *dingov1alpha1.DingoNode) string {
	return dn.Name + "-keys-approval"
}

// BuildKeysApprovalConfigMap constructs the approval ConfigMap. digest is the
// hex SHA-256 of the approved kes.skey followed by its opcert, or of the opcert
// alone on a node that signs through a remote agent.
func BuildKeysApprovalConfigMap(
	dn *dingov1alpha1.DingoNode,
	digest string,
) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KeysApprovalConfigMapName(dn),
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Data: map[string]string{KeysApprovalKey: digest},
	}
}

// keysSyncResources are the staging container's requests, set equal to its
// limits. It copies two small files at most every few seconds. Requests equal
// to limits keep a pod whose node container is Guaranteed in that QoS class,
// and copying the node's own resources would double the pod's requests.
var keysSyncResources = corev1.ResourceRequirements{
	Requests: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("50m"),
		corev1.ResourceMemory: resource.MustParse("32Mi"),
	},
	Limits: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("50m"),
		corev1.ResourceMemory: resource.MustParse("32Mi"),
	},
}

// keysSyncContainer stages approved key changes into the running pod.
func keysSyncContainer(dn *dingov1alpha1.DingoNode) corev1.Container {
	return corev1.Container{
		Name:            keysSyncName,
		Image:           imageRef(dn),
		ImagePullPolicy: pullPolicy(dn),
		Command:         []string{"/bin/sh", "-c", syncKeysScript(dn)},
		VolumeMounts: []corev1.VolumeMount{
			{Name: keysSourceName, MountPath: keysSourcePath, ReadOnly: true},
			{Name: keysVolumeName, MountPath: keysMountPath},
			{
				Name:      keysApprovalVolumeName,
				MountPath: keysApprovalPath,
				ReadOnly:  true,
			},
		},
		Resources:       *keysSyncResources.DeepCopy(),
		SecurityContext: containerSecurityContext(),
	}
}

// keysApprovalVolume mounts the approval ConfigMap. It is optional, so a pod
// starts before the operator has approved anything.
func keysApprovalVolume(dn *dingov1alpha1.DingoNode) corev1.Volume {
	return corev1.Volume{
		Name: keysApprovalVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: KeysApprovalConfigMapName(dn),
				},
				Optional: new(true),
			},
		},
	}
}

// podContainers returns the node container and, on a node that reloads keys in
// place, the container that stages approved key changes beside it.
func podContainers(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
	node corev1.Container,
) []corev1.Container {
	if !reloadsKeysInPlace(dn, opts) {
		return []corev1.Container{node}
	}
	return []corev1.Container{node, keysSyncContainer(dn)}
}
