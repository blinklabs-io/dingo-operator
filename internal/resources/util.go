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

// initContainers builds the Mithril bootstrap init container when enabled. It
// mirrors the Dingo Helm chart: skip when a database already exists (unless a
// force resync is requested), then bootstrap via the native "dingo mithril
// sync" client.
func initContainers(
	dn *dingov1alpha1.DingoNode,
	_ RenderOptions,
) []corev1.Container {
	if !mithrilEnabled(dn) {
		return nil
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
	return []corev1.Container{
		{
			Name:            mithrilInitName,
			Image:           imageRef(dn),
			ImagePullPolicy: pullPolicy(dn),
			Command:         []string{"/bin/sh", "-c", script},
			Env:             env,
			VolumeMounts:    mounts,
			SecurityContext: containerSecurityContext(),
			Resources:       dn.Spec.Resources,
		},
	}
}
