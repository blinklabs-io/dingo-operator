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
	"sort"
	"strconv"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// defaultStorageSize is used when persistence.size is unset.
func defaultStorageSize() resource.Quantity {
	return resource.MustParse("60Gi")
}

// sortedKeys returns the keys of m in deterministic order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// mithrilEnabled reports whether Mithril bootstrap is enabled (default true).
func mithrilEnabled(dn *dingov1alpha1.DingoNode) bool {
	return dn.Spec.Mithril.Enabled == nil || *dn.Spec.Mithril.Enabled
}

const prepareBlockProducerKeysScript = `set -eu
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
umask 077
for name in vrf.skey kes.skey opcert.cert; do
  cp "${source_dir}/${name}" "${destination_dir}/${name}"
done
chmod 0600 \
  "${destination_dir}/vrf.skey" \
  "${destination_dir}/kes.skey" \
  "${destination_dir}/opcert.cert"
`

// initContainers prepares private key files when mounted and builds the Mithril
// bootstrap init container when enabled. The Secret source can be widened by a
// pod fsGroup, so Dingo reads an owner-only copy from a memory-backed volume.
func initContainers(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) []corev1.Container {
	var containers []corev1.Container
	if mountsBlockProducerKeys(dn, opts) {
		containers = append(containers, corev1.Container{
			Name:            keysInitName,
			Image:           imageRef(dn),
			ImagePullPolicy: pullPolicy(dn),
			Command: []string{
				"/bin/sh",
				"-c",
				prepareBlockProducerKeysScript,
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      keysSourceName,
					MountPath: keysSourcePath,
					ReadOnly:  true,
				},
				{
					Name:      keysVolumeName,
					MountPath: keysMountPath,
				},
			},
			SecurityContext: containerSecurityContext(),
			Resources:       dn.Spec.Resources,
		})
	}
	if !mithrilEnabled(dn) {
		return containers
	}
	const script = `set -eu
DB="${CARDANO_DATABASE_PATH}/metadata.sqlite"
if [ -f "$DB" ] && [ "${FORCE_RESYNC:-false}" != "true" ]; then
  echo "dingo database present; skipping mithril bootstrap"
  exit 0
fi
if [ "${FORCE_RESYNC:-false}" = "true" ]; then
  echo "force resync requested; removing existing database"
  rm -rf "${CARDANO_DATABASE_PATH:?}/"* || true
fi
echo "bootstrapping via dingo mithril sync"
exec dingo mithril sync
`
	verify := dn.Spec.Mithril.VerifyCertificates == nil ||
		*dn.Spec.Mithril.VerifyCertificates
	env := []corev1.EnvVar{
		{Name: "CARDANO_NETWORK", Value: dn.Spec.Network},
		{Name: "CARDANO_DATABASE_PATH", Value: dataMountPath},
		{
			Name:  "FORCE_RESYNC",
			Value: strconv.FormatBool(dn.Spec.Mithril.ForceResync),
		},
		{Name: "DINGO_MITHRIL_VERIFY_CERTS", Value: strconv.FormatBool(verify)},
	}
	if dn.Spec.NetworkMagic != nil {
		env = append(
			env,
			corev1.EnvVar{
				Name:  "CARDANO_NETWORK_MAGIC",
				Value: strconv.FormatInt(*dn.Spec.NetworkMagic, 10),
			},
		)
	}
	if dn.Spec.Mithril.AggregatorURL != "" {
		env = append(
			env,
			corev1.EnvVar{
				Name:  "DINGO_MITHRIL_AGGREGATOR_URL",
				Value: dn.Spec.Mithril.AggregatorURL,
			},
		)
	}
	// A custom network's genesis is not built into Dingo, so "dingo mithril
	// sync" must be pointed at the mounted config bundle just like the main
	// container; otherwise it fails to load config.json before bootstrap.
	mounts := []corev1.VolumeMount{
		{Name: dataVolumeName, MountPath: dataMountPath},
	}
	if dn.Spec.ConfigRef != "" {
		env = append(env, corev1.EnvVar{
			Name:  "CARDANO_CONFIG",
			Value: configBundleMountPath + "/" + configBundleFileName,
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      configBundleVolumeName,
			MountPath: configBundleMountPath,
			ReadOnly:  true,
		})
	}
	return append(
		containers,
		corev1.Container{
			Name:            mithrilInitName,
			Image:           imageRef(dn),
			ImagePullPolicy: pullPolicy(dn),
			Command:         []string{"/bin/sh", "-c", script},
			Env:             env,
			VolumeMounts:    mounts,
			SecurityContext: containerSecurityContext(),
			Resources:       dn.Spec.Resources,
		},
	)
}
