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
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PoolsConfigChecksumAnnotation forces a rollout when the rendered pool
	// list changes. The config file is mounted with subPath, which the kubelet
	// never refreshes, so only a new pod reads a changed one.
	PoolsConfigChecksumAnnotation = "dingo.blinklabs.io/pools-config-checksum"
	// PoolsConfigKey is the key of the pool list ConfigMap, the file Dingo
	// reads.
	PoolsConfigKey = "dingo.yaml"

	// MinPoolListMinor is the first 0.x release that reads blockProducerPools.
	// An earlier release refuses the unknown YAML field at startup.
	MinPoolListMinor = 94

	poolsConfigVolumeName = "dingo-pools-config"
	// poolsConfigMountPath is the file Dingo reads when it is given no
	// --config: the image's entrypoint passes none.
	poolsConfigMountPath = "/etc/dingo/dingo.yaml"
)

// RenderPool is one pool of a pool-list node that is rendered into the pod:
// its bundle validated.
type RenderPool struct {
	// PoolID is the pool's hex cold-key hash. It names the pool's directory
	// under /keys.
	PoolID string
	// SecretName is the keys Secret holding the pool's vrf.skey and opcert.
	SecretName string
	// AgentProfile is the signing agent profile that holds the pool's KES key.
	AgentProfile string
	// HasVote is set when the Secret holds a Leios vote key, which is staged
	// with the other keys.
	HasVote bool
}

// UsesPoolList reports whether dn is a block producer that forges for the
// pools of spec.blockProducer.pools.
func UsesPoolList(dn *dingov1alpha1.DingoNode) bool {
	return IsBlockProducer(dn) && dn.Spec.BlockProducer != nil &&
		len(dn.Spec.BlockProducer.Pools) > 0
}

// SupportsPoolList reports whether the selected image reads blockProducerPools.
// As for SupportsTracing, an image whose version cannot be determined is
// trusted.
func SupportsPoolList(dn *dingov1alpha1.DingoNode) bool {
	major, minor, _, ok := canonicalImageVersion(dn)
	return !ok || major > 0 || minor >= MinPoolListMinor
}

// rendersPools reports whether this render stages the pools in opts.Pools.
func rendersPools(dn *dingov1alpha1.DingoNode, opts RenderOptions) bool {
	return mountsBlockProducerKeys(dn, opts) && UsesPoolList(dn)
}

// PoolsConfigMapName returns the ConfigMap holding the rendered pool list.
func PoolsConfigMapName(dn *dingov1alpha1.DingoNode) string {
	return dn.Name + "-pools"
}

// SortedPools returns pools in rendering order: by pool ID, so the order of
// spec.blockProducer.pools never changes the pod template.
func SortedPools(pools []RenderPool) []RenderPool {
	out := slices.Clone(pools)
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].PoolID < out[j].PoolID },
	)
	return out
}

// PoolKeysDir is the directory under /keys that holds a pool's staged keys.
func PoolKeysDir(poolID string) string {
	return keysMountPath + "/" + poolID
}

// PoolsConfig renders the Dingo YAML configuration that lists the pools, in
// pool ID order. Every pool shares the node's signing agent connection and has
// its own profile; no pool names a KES key, since the agent holds them.
func PoolsConfig(
	dn *dingov1alpha1.DingoNode,
	pools []RenderPool,
) string {
	var sa dingov1alpha1.SigningAgentSpec
	if dn.Spec.BlockProducer != nil &&
		dn.Spec.BlockProducer.SigningAgent != nil {
		sa = *dn.Spec.BlockProducer.SigningAgent
	}
	var b strings.Builder
	b.WriteString("blockProducerPools:\n")
	for _, p := range SortedPools(pools) {
		dir := PoolKeysDir(p.PoolID)
		line := func(first bool, key, value string) {
			prefix := "    "
			if first {
				prefix = "  - "
			}
			fmt.Fprintf(&b, "%s%s: %s\n", prefix, key, value)
		}
		line(true, "shelleyVrfKey", strconv.Quote(dir+"/vrf.skey"))
		line(false, "shelleyOperationalCertificate",
			strconv.Quote(dir+"/opcert.cert"))
		line(false, "shelleyKesAgentAddress", strconv.Quote(sa.Address))
		if pin := strings.TrimSpace(sa.ServerSPKISHA256); pin != "" {
			line(false, "shelleyKesAgentServerSpkiSha256", strconv.Quote(pin))
		}
		if sa.Plaintext {
			line(false, "shelleyKesAgentPlaintext", "true")
		}
		if sa.ClientCertSecretRef != "" {
			line(false, "shelleyKesAgentClientCertFile",
				strconv.Quote(signingAgentTLSPath+"/tls.crt"))
			line(false, "shelleyKesAgentClientKeyFile",
				strconv.Quote(signingAgentTLSPath+"/tls.key"))
		}
		line(false, "shelleyKesAgentProfile", strconv.Quote(p.AgentProfile))
	}
	return b.String()
}

// BuildPoolsConfigMap constructs the ConfigMap holding the pool list.
func BuildPoolsConfigMap(
	dn *dingov1alpha1.DingoNode,
	pools []RenderPool,
) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PoolsConfigMapName(dn),
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Data: map[string]string{PoolsConfigKey: PoolsConfig(dn, pools)},
	}
}

// BuildPoolKeysApprovalConfigMap constructs the approval ConfigMap of a
// pool-list node: for each approved pool, keyed by pool ID, the hex SHA-256 of
// the opcert the staging container may stage.
func BuildPoolKeysApprovalConfigMap(
	dn *dingov1alpha1.DingoNode,
	digests map[string]string,
) *corev1.ConfigMap {
	data := make(map[string]string, len(digests))
	for pool, digest := range digests {
		data[pool] = digest
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KeysApprovalConfigMapName(dn),
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Data: data,
	}
}

// poolSourceVolume names the Secret volume of the i-th pool in rendering order.
func poolSourceVolume(i int) string {
	return keysSourceName + "-" + strconv.Itoa(i)
}

// poolSourcePath is where a pool's keys Secret is mounted in the staging
// containers.
func poolSourcePath(poolID string) string {
	return keysSourcePath + "/" + poolID
}

// poolSourceVolumes returns the Secret volumes of the rendered pools.
func poolSourceVolumes(opts RenderOptions) []corev1.Volume {
	var vols []corev1.Volume
	for i, p := range SortedPools(opts.Pools) {
		vols = append(vols, corev1.Volume{
			Name: poolSourceVolume(i),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  p.SecretName,
					DefaultMode: new(int32(0o400)),
				},
			},
		})
	}
	return vols
}

// poolSourceMounts returns the read-only mounts of the rendered pools' Secrets
// for a staging container.
func poolSourceMounts(opts RenderOptions) []corev1.VolumeMount {
	var mounts []corev1.VolumeMount
	for i, p := range SortedPools(opts.Pools) {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      poolSourceVolume(i),
			MountPath: poolSourcePath(p.PoolID),
			ReadOnly:  true,
		})
	}
	return mounts
}

// KeysSecretNames returns every Secret a pod spec mounts its block-producer
// keys from: the one Secret of a single-pool node, or one per pool.
func KeysSecretNames(spec corev1.PodSpec) []string {
	var names []string
	for _, v := range spec.Volumes {
		if v.Secret == nil {
			continue
		}
		if v.Name == keysSourceName ||
			strings.HasPrefix(v.Name, keysSourceName+"-") {
			names = append(names, v.Secret.SecretName)
		}
	}
	return names
}

// SplitKeysPools returns the pools recorded in a keys-pool annotation: one for
// a single-pool pod, a comma-separated list for a pool-list pod.
func SplitKeysPools(annotation string) []string {
	var pools []string
	for p := range strings.SplitSeq(annotation, ",") {
		if p = strings.TrimSpace(p); p != "" {
			pools = append(pools, p)
		}
	}
	return pools
}

// JoinKeysPools renders pool IDs as a keys-pool annotation, sorted.
func JoinKeysPools(pools []string) string {
	sorted := slices.Clone(pools)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

// poolsManagedEnv lists the settings a pool list replaces: Dingo refuses
// blockProducerPools while any of them is set, with or without the CARDANO_
// prefix envconfig also reads.
var poolsManagedEnv = func() []string {
	names := []string{
		"SHELLEY_VRF_KEY",
		"SHELLEY_KES_KEY",
		"SHELLEY_OPERATIONAL_CERTIFICATE",
		"SHELLEY_KES_AGENT_SOCKET",
		"SHELLEY_KES_AGENT_MODE",
		"SHELLEY_KES_AGENT_ADDRESS",
		"SHELLEY_KES_AGENT_ADDRESSES",
		"SHELLEY_KES_AGENT_CLIENT_CERT_FILE",
		"SHELLEY_KES_AGENT_CLIENT_KEY_FILE",
		"SHELLEY_KES_AGENT_SERVER_SPKI_SHA256",
		"SHELLEY_KES_AGENT_PLAINTEXT",
		"SHELLEY_KES_AGENT_PROFILE",
	}
	var out []string
	for _, n := range names {
		out = append(out, "CARDANO_"+n, n)
	}
	return out
}()

// validatePoolsEnv rejects spec.environment overrides that Dingo refuses
// beside blockProducerPools: a single-pool key or agent setting, and the vote
// key path, which the operator points at a pool's directory.
func validatePoolsEnv(dn *dingov1alpha1.DingoNode) error {
	if !UsesPoolList(dn) {
		return nil
	}
	for _, name := range poolsManagedEnv {
		if _, ok := dn.Spec.Environment[name]; ok {
			return fmt.Errorf(
				"%s is refused by Dingo beside "+
					"spec.blockProducer.pools, which sets each pool's keys "+
					"in its own configuration; remove it from spec.environment",
				name,
			)
		}
	}
	if _, ok := dn.Spec.Environment[VoteKeyEnv]; ok {
		return fmt.Errorf(
			"%s is managed by the operator when spec.blockProducer.pools "+
				"is set; remove it from spec.environment",
			VoteKeyEnv,
		)
	}
	return nil
}

// poolsConfigVolume mounts the rendered pool list.
func poolsConfigVolume(dn *dingov1alpha1.DingoNode) corev1.Volume {
	return corev1.Volume{
		Name: poolsConfigVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: PoolsConfigMapName(dn),
				},
			},
		},
	}
}

// poolsConfigMount mounts the pool list at the path Dingo reads by default.
func poolsConfigMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      poolsConfigVolumeName,
		MountPath: poolsConfigMountPath,
		SubPath:   PoolsConfigKey,
		ReadOnly:  true,
	}
}

// preparePoolListKeysScript stages the keys of each pool of a pool-list node
// into its own directory under /keys: the VRF key, the opcert and the
// optional vote key. The agent holds the KES keys, so none is copied.
const preparePoolListKeysScript = `set -eu
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
umask 077
for source in "${source_dir}"/*/; do
  pool="$(basename "${source}")"
  destination="${destination_dir}/${pool}"
  mkdir -p "${destination}"
  cp "${source}vrf.skey" "${destination}/vrf.skey"
  # The operational certificate is opcert.cert or, as in the dingo Helm chart,
  # node.cert. Dingo is always pointed at opcert.cert.
  cert=node.cert
  if [ -s "${source}opcert.cert" ]; then
    cert=opcert.cert
  fi
  cp "${source}${cert}" "${destination}/opcert.cert"
  chmod 0600 "${destination}/vrf.skey" "${destination}/opcert.cert"
  vote=
  if [ -s "${source}vote.skey" ]; then
    vote=vote.skey
  elif [ -s "${source}bls.skey" ]; then
    vote=bls.skey
  fi
  if [ -n "${vote}" ]; then
    cp "${source}${vote}" "${destination}/vote.skey"
    chmod 0600 "${destination}/vote.skey"
  fi
done
`

// syncPoolListKeysScript is syncSigningAgentKeysScript for each pool of a
// pool-list node: it stages a pool's opcert into the pool's directory once
// the operator has approved the digest of the Secret's opcert, so a new
// opcert reaches Dingo without a roll. A pool with no approval is left alone.
const syncPoolListKeysScript = `set -u
trap 'exit 0' TERM INT
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
approval_dir="${DINGO_KEYS_APPROVAL_DIR:-/var/run/dingo-keys-approval}"
interval="${DINGO_KEYS_SYNC_INTERVAL:-5}"
umask 077
digest() {
  cat "$@" 2>/dev/null | sha256sum | cut -d' ' -f1
}
while :; do
  sleep "${interval}" &
  wait "$!"
  for source in "${source_dir}"/*/; do
    pool="$(basename "${source}")"
    approval="${approval_dir}/${pool}"
    destination="${destination_dir}/${pool}"
    [ -s "${approval}" ] || continue
    want="$(cat "${approval}")"
    cert=node.cert
    if [ -s "${source}opcert.cert" ]; then
      cert=opcert.cert
    fi
    [ "$(digest "${source}${cert}")" = "${want}" ] || continue
    [ "$(digest "${destination}/opcert.cert")" = "${want}" ] && continue
    rm -f "${destination}/.opcert.cert.new"
    cp "${source}${cert}" "${destination}/.opcert.cert.new" &&
      chmod 0600 "${destination}/.opcert.cert.new" || continue
    [ "$(digest "${destination}/.opcert.cert.new")" = "${want}" ] ||
      continue
    mv -f "${destination}/.opcert.cert.new" "${destination}/opcert.cert" &&
      echo "staged the approved opcert of pool ${pool}"
  done
done
`
