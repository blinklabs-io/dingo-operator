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
for name in vrf.skey kes.skey; do
  cp "${source_dir}/${name}" "${destination_dir}/${name}"
done
# The operational certificate is opcert.cert or, as in the dingo Helm chart,
# node.cert. Dingo is always pointed at /keys/opcert.cert.
cert=node.cert
if [ -s "${source_dir}/opcert.cert" ]; then
  cert=opcert.cert
fi
cp "${source_dir}/${cert}" "${destination_dir}/opcert.cert"
chmod 0600 \
  "${destination_dir}/vrf.skey" \
  "${destination_dir}/kes.skey" \
  "${destination_dir}/opcert.cert"
# vote.skey (or the dingo Helm chart's bls.skey) is the optional Leios vote
# signing key; Dingo requires it to be owner-only.
vote=
if [ -s "${source_dir}/vote.skey" ]; then
  vote=vote.skey
elif [ -s "${source_dir}/bls.skey" ]; then
  vote=bls.skey
fi
if [ -n "${vote}" ]; then
  cp "${source_dir}/${vote}" "${destination_dir}/vote.skey"
  chmod 0600 "${destination_dir}/vote.skey"
fi
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
	// Completion is tracked by an explicit marker pair, not by the presence of
	// the database file. A restore interrupted after metadata.sqlite appears
	// but before the chain is complete is indistinguishable from success by
	// file existence alone, and the old check took that branch and handed the
	// node a partial database.
	//
	// The started marker separates the two cases the completion marker cannot:
	// a volume written by an operator version that predates these markers
	// (adopted as complete, preserving its behaviour) versus a bootstrap this
	// container began and did not finish (resumed). Resuming is safe and cheap
	// -- dingo mithril sync pins its artifact and records per-immutable-file
	// progress, so it continues rather than restarting, and no-ops once the
	// marker is at the latest artifact. Re-running unconditionally is not an
	// option: catch-up against a complete database is core-storage-mode only.
	//
	// dingo serve independently refuses to start on an incomplete sync, so a
	// marker bug fails loudly at startup rather than forging on partial state.
	const script = `set -eu
DB="${CARDANO_DATABASE_PATH}/metadata.sqlite"
DONE="${CARDANO_DATABASE_PATH}/.mithril-bootstrap-complete"
STARTED="${CARDANO_DATABASE_PATH}/.mithril-bootstrap-started"
if [ "${FORCE_RESYNC:-false}" = "true" ]; then
  echo "force resync requested; removing existing database"
  rm -f "$DONE" "$STARTED"
  rm -rf "${CARDANO_DATABASE_PATH:?}/"* || true
fi
if [ -f "$DONE" ]; then
  echo "mithril bootstrap already complete; skipping"
  exit 0
fi
if [ -f "$DB" ] && [ ! -f "$STARTED" ]; then
  echo "pre-existing database with no bootstrap marker; adopting as complete"
  touch "$DONE"
  exit 0
fi
if [ -f "$STARTED" ]; then
  echo "previous mithril bootstrap did not complete; resuming"
else
  echo "bootstrapping via dingo mithril sync"
fi
touch "$STARTED"
dingo mithril sync
touch "$DONE"
echo "mithril bootstrap complete"
`
	verify := dn.Spec.Mithril.VerifyCertificates == nil ||
		*dn.Spec.Mithril.VerifyCertificates
	mode := storageMode(dn)
	if override, ok := dn.Spec.Environment["DINGO_STORAGE_MODE"]; ok {
		mode = override
	}
	env := []corev1.EnvVar{
		{Name: "CARDANO_NETWORK", Value: dn.Spec.Network},
		{Name: "CARDANO_DATABASE_PATH", Value: dataMountPath},
		{Name: "DINGO_STORAGE_MODE", Value: mode},
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
		{Name: tmpVolumeName, MountPath: "/tmp"},
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

// tracingEnv returns Dingo's tracing settings. Nothing is emitted while
// tracing is disabled: Dingo enables tracing whenever an endpoint is set, so a
// stray DINGO_TRACING_ENDPOINT alone would switch the exporter on.
func tracingEnv(dn *dingov1alpha1.DingoNode) []corev1.EnvVar {
	t := dn.Spec.Tracing
	if !t.Enabled {
		return nil
	}
	serviceName := t.ServiceName
	if serviceName == "" {
		serviceName = dn.Name
	}
	env := []corev1.EnvVar{
		{Name: "DINGO_TRACING_ENABLED", Value: "true"},
		{Name: "DINGO_TRACING_SERVICE_NAME", Value: serviceName},
	}
	if t.Endpoint != "" {
		env = append(env, corev1.EnvVar{
			Name:  "DINGO_TRACING_ENDPOINT",
			Value: t.Endpoint,
		})
	}
	if t.SampleRatio != "" {
		env = append(env, corev1.EnvVar{
			Name:  "DINGO_TRACING_SAMPLE_RATIO",
			Value: t.SampleRatio,
		})
	}
	return env
}

// loggingEnv returns Dingo's log settings, leaving its defaults for any field
// the spec does not set.
func loggingEnv(dn *dingov1alpha1.DingoNode) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, 2)
	if l := dn.Spec.Logging.Level; l != "" {
		env = append(env, corev1.EnvVar{
			Name:  "DINGO_LOGGING_LEVEL",
			Value: string(l),
		})
	}
	if f := dn.Spec.Logging.Format; f != "" {
		env = append(env, corev1.EnvVar{
			Name:  "DINGO_LOGGING_FORMAT",
			Value: string(f),
		})
	}
	return env
}
