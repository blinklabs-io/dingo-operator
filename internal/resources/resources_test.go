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
	"strconv"
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func relayNode() *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "cardano"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "mainnet",
		},
	}
}

func bpNode() *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "cardano"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "mainnet",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				SlotsPerKESPeriod: 129600,
				MaxKESEvolutions:  62,
				Keys: dingov1alpha1.KeysSpec{
					SecretRef: "pool-keys",
				},
			},
		},
	}
}

func envMap(dn *dingov1alpha1.DingoNode, opts RenderOptions) map[string]string {
	m := map[string]string{}
	for _, e := range BuildEnv(dn, opts) {
		m[e.Name] = e.Value
	}
	return m
}

func TestLabelsAndSelector(t *testing.T) {
	dn := bpNode()
	sel := SelectorLabels(dn)
	assert.Equal(t, "dingo", sel["app.kubernetes.io/name"])
	assert.Equal(t, "bp", sel["app.kubernetes.io/instance"])
	assert.Equal(t, "bp", sel[NodeLabel])

	labels := Labels(dn)
	assert.Equal(t, "block-producer", labels["app.kubernetes.io/component"])
	assert.Equal(t, "mainnet", labels["cardano_network"])
	assert.Equal(t, "dingo", labels["cardano_service"])
	// selector labels must be a subset of the full label set
	for k, v := range sel {
		assert.Equal(t, v, labels[k])
	}
}

func TestBuildEnv(t *testing.T) {
	t.Run("relay has no block producer env", func(t *testing.T) {
		env := envMap(relayNode(), RenderOptions{})
		assert.Equal(t, "mainnet", env["CARDANO_NETWORK"])
		assert.Equal(t, "/data", env["CARDANO_DATABASE_PATH"])
		assert.NotContains(t, env, "CARDANO_BLOCK_PRODUCER")
	})

	t.Run("block producer sets key paths and kes params", func(t *testing.T) {
		env := envMap(bpNode(), RenderOptions{MountKeys: true})
		assert.Equal(t, "true", env["CARDANO_BLOCK_PRODUCER"])
		assert.Equal(t, "/keys/kes.skey", env["CARDANO_SHELLEY_KES_KEY"])
		assert.Equal(t, "/keys/vrf.skey", env["CARDANO_SHELLEY_VRF_KEY"])
		assert.Equal(
			t,
			"/keys/opcert.cert",
			env["CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE"],
		)
		assert.Equal(t, "129600", env["DINGO_SLOTS_PER_KES_PERIOD"])
		assert.Equal(t, "62", env["DINGO_MAX_KES_EVOLUTIONS"])
	})

	t.Run("block producer without config has no key env", func(t *testing.T) {
		dn := bpNode()
		dn.Spec.BlockProducer = nil
		env := envMap(dn, RenderOptions{MountKeys: true})
		// All block-producer env vars are gated behind the same bp != nil
		// condition in BuildEnv, so none of them should be present.
		for _, key := range []string{
			"CARDANO_BLOCK_PRODUCER",
			"CARDANO_SHELLEY_VRF_KEY",
			"CARDANO_SHELLEY_KES_KEY",
			"CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE",
			"DINGO_SLOTS_PER_KES_PERIOD",
			"DINGO_MAX_KES_EVOLUTIONS",
			"DINGO_FORGE_SYNC_TOLERANCE_SLOTS",
			"DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS",
		} {
			assert.NotContains(t, env, key)
		}
	})

	t.Run("spec environment overrides defaults", func(t *testing.T) {
		dn := relayNode()
		dn.Spec.Environment = map[string]string{
			"CARDANO_NETWORK":     "preprod",
			"DINGO_LOGGING_LEVEL": "debug",
		}
		env := envMap(dn, RenderOptions{})
		assert.Equal(t, "preprod", env["CARDANO_NETWORK"])
		assert.Equal(t, "debug", env["DINGO_LOGGING_LEVEL"])
	})

	t.Run("topology enables CARDANO_TOPOLOGY", func(t *testing.T) {
		env := envMap(relayNode(), RenderOptions{HasTopology: true})
		assert.Equal(t, "/config/topology.json", env["CARDANO_TOPOLOGY"])
	})

	t.Run("config ref sets CARDANO_CONFIG", func(t *testing.T) {
		dn := relayNode()
		dn.Spec.ConfigRef = "devnet-config"
		env := envMap(dn, RenderOptions{})
		assert.Equal(
			t,
			"/cardano-config/config.json",
			env["CARDANO_CONFIG"],
		)
	})

	t.Run("no config ref omits CARDANO_CONFIG", func(t *testing.T) {
		env := envMap(relayNode(), RenderOptions{})
		assert.NotContains(t, env, "CARDANO_CONFIG")
	})

	t.Run("spec environment overrides CARDANO_CONFIG", func(t *testing.T) {
		dn := relayNode()
		dn.Spec.ConfigRef = "devnet-config"
		dn.Spec.Environment = map[string]string{
			"CARDANO_CONFIG": "/custom/config.json",
		}
		env := envMap(dn, RenderOptions{})
		assert.Equal(t, "/custom/config.json", env["CARDANO_CONFIG"])
	})
}

func TestBuildStatefulSet(t *testing.T) {
	t.Run("relay statefulset basics", func(t *testing.T) {
		dn := relayNode()
		sts := BuildStatefulSet(dn, RenderOptions{Replicas: 2})
		require.NotNil(t, sts.Spec.Replicas)
		assert.Equal(t, int32(2), *sts.Spec.Replicas)
		assert.Equal(t, HeadlessServiceName(dn), sts.Spec.ServiceName)
		require.Len(t, sts.Spec.Template.Spec.Containers, 1)
		assert.Equal(t, "dingo", sts.Spec.Template.Spec.Containers[0].Name)
		require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
		assert.Equal(t, dataVolumeName, sts.Spec.VolumeClaimTemplates[0].Name)
	})

	t.Run("block producer mounts keys secret", func(t *testing.T) {
		dn := bpNode()
		sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1, MountKeys: true})
		var found bool
		for _, v := range sts.Spec.Template.Spec.Volumes {
			if v.Name == keysVolumeName {
				require.NotNil(t, v.Secret)
				assert.Equal(t, "pool-keys", v.Secret.SecretName)
				require.NotNil(t, v.Secret.DefaultMode)
				assert.Equal(t, int32(0o600), *v.Secret.DefaultMode)
				found = true
			}
		}
		assert.True(t, found, "expected block-producer keys volume")
	})

	t.Run("keys checksum stamps a rollout annotation", func(t *testing.T) {
		dn := bpNode()
		anns := func(sum string) map[string]string {
			return BuildStatefulSet(dn, RenderOptions{
				Replicas: 1, MountKeys: true, KeysChecksum: sum,
			}).Spec.Template.Annotations
		}
		// No checksum -> no annotation (no spurious rollout).
		_, present := anns("")[KeysChecksumAnnotation]
		assert.False(t, present, "empty checksum must not set the annotation")
		// A checksum is propagated to the pod template so a key change rolls.
		assert.Equal(t, "abc123", anns("abc123")[KeysChecksumAnnotation])
		// A different checksum changes the template (triggers a new revision).
		assert.NotEqual(
			t,
			anns("abc123")[KeysChecksumAnnotation],
			anns("def456")[KeysChecksumAnnotation],
		)
	})

	t.Run("nil block producer config omits key material", func(t *testing.T) {
		dn := bpNode()
		dn.Spec.BlockProducer = nil
		sts := BuildStatefulSet(
			dn,
			RenderOptions{Replicas: 1, MountKeys: true},
		)
		for _, volume := range sts.Spec.Template.Spec.Volumes {
			assert.NotEqual(t, keysVolumeName, volume.Name)
		}
		env := envMap(dn, RenderOptions{MountKeys: true})
		assert.NotContains(t, env, "CARDANO_BLOCK_PRODUCER")
	})

	t.Run("mithril init container present by default", func(t *testing.T) {
		sts := BuildStatefulSet(relayNode(), RenderOptions{Replicas: 1})
		require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
		assert.Equal(
			t,
			mithrilInitName,
			sts.Spec.Template.Spec.InitContainers[0].Name,
		)
	})

	t.Run("config ref mounts config bundle read-only", func(t *testing.T) {
		dn := relayNode()
		dn.Spec.ConfigRef = "devnet-config"
		sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})

		var vol *corev1.Volume
		for i := range sts.Spec.Template.Spec.Volumes {
			v := &sts.Spec.Template.Spec.Volumes[i]
			if v.Name == configBundleVolumeName {
				vol = v
			}
		}
		require.NotNil(t, vol, "expected cardano-config volume")
		require.NotNil(t, vol.ConfigMap)
		assert.Equal(t, "devnet-config", vol.ConfigMap.Name)

		container := sts.Spec.Template.Spec.Containers[0]
		var mount *corev1.VolumeMount
		for i := range container.VolumeMounts {
			m := &container.VolumeMounts[i]
			if m.Name == configBundleVolumeName {
				mount = m
			}
		}
		require.NotNil(t, mount, "expected cardano-config volume mount")
		assert.Equal(t, configBundleMountPath, mount.MountPath)
		assert.True(t, mount.ReadOnly)
	})

	t.Run("no config ref omits config bundle", func(t *testing.T) {
		sts := BuildStatefulSet(relayNode(), RenderOptions{Replicas: 1})
		for _, v := range sts.Spec.Template.Spec.Volumes {
			assert.NotEqual(t, configBundleVolumeName, v.Name)
		}
		container := sts.Spec.Template.Spec.Containers[0]
		for _, m := range container.VolumeMounts {
			assert.NotEqual(t, configBundleVolumeName, m.Name)
		}
	})

	t.Run("mithril init container gets the config bundle", func(t *testing.T) {
		dn := relayNode()
		dn.Spec.ConfigRef = "devnet-config"
		sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})
		require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
		ic := sts.Spec.Template.Spec.InitContainers[0]

		var cfg string
		for _, e := range ic.Env {
			if e.Name == "CARDANO_CONFIG" {
				cfg = e.Value
			}
		}
		assert.Equal(
			t,
			configBundleMountPath+"/"+configBundleFileName,
			cfg,
			"init container must point at the mounted config.json",
		)

		var mounted bool
		for _, m := range ic.VolumeMounts {
			if m.Name == configBundleVolumeName {
				mounted = true
				assert.Equal(t, configBundleMountPath, m.MountPath)
				assert.True(t, m.ReadOnly)
			}
		}
		assert.True(t, mounted, "init container must mount the config bundle")
	})

	t.Run("init container omits config bundle without a ref", func(t *testing.T) {
		sts := BuildStatefulSet(relayNode(), RenderOptions{Replicas: 1})
		ic := sts.Spec.Template.Spec.InitContainers[0]
		for _, e := range ic.Env {
			assert.NotEqual(t, "CARDANO_CONFIG", e.Name)
		}
		for _, m := range ic.VolumeMounts {
			assert.NotEqual(t, configBundleVolumeName, m.Name)
		}
	})

	t.Run("config checksum stamps a rollout annotation", func(t *testing.T) {
		dn := relayNode()
		anns := func(sum string) map[string]string {
			return BuildStatefulSet(dn, RenderOptions{
				Replicas: 1, ConfigChecksum: sum,
			}).Spec.Template.Annotations
		}
		// No checksum -> no annotation (no spurious rollout).
		_, present := anns("")[ConfigChecksumAnnotation]
		assert.False(t, present, "empty checksum must not set the annotation")
		// A checksum is propagated so a config-bundle change rolls the pod.
		assert.Equal(t, "cfg123", anns("cfg123")[ConfigChecksumAnnotation])
		assert.NotEqual(
			t,
			anns("cfg123")[ConfigChecksumAnnotation],
			anns("cfg456")[ConfigChecksumAnnotation],
		)
	})
}

func TestBuildNetworkPolicy(t *testing.T) {
	t.Run(
		"without relay refs only metrics ingress is allowed",
		func(t *testing.T) {
			policy := BuildNetworkPolicy(bpNode())
			require.Len(t, policy.Spec.Ingress, 1)
			require.Len(t, policy.Spec.Ingress[0].Ports, 1)
			require.NotNil(t, policy.Spec.Ingress[0].Ports[0].Port)
			assert.Equal(
				t,
				portMetrics,
				policy.Spec.Ingress[0].Ports[0].Port.IntValue(),
			)
		},
	)

	t.Run(
		"with relay refs allows node ports from selected relays",
		func(t *testing.T) {
			dn := bpNode()
			dn.Spec.Topology.RelayRefs = []string{"relay-a", "relay-b"}
			policy := BuildNetworkPolicy(dn)
			require.Len(t, policy.Spec.Ingress, 2)
			require.Len(t, policy.Spec.Ingress[0].From, 1)
			require.NotNil(t, policy.Spec.Ingress[0].From[0].PodSelector)
			require.Len(
				t,
				policy.Spec.Ingress[0].From[0].PodSelector.MatchExpressions,
				1,
			)
			assert.ElementsMatch(
				t,
				[]string{"relay-a", "relay-b"},
				policy.Spec.Ingress[0].From[0].PodSelector.
					MatchExpressions[0].Values,
			)
		},
	)
}

func TestImageRef(t *testing.T) {
	dn := relayNode()
	assert.Equal(t, "ghcr.io/blinklabs-io/dingo:"+DefaultDingoTag, imageRef(dn))
	dn.Spec.Image.Repository = "example/dingo"
	dn.Spec.Image.Tag = "1.2.3"
	assert.Equal(t, "example/dingo:1.2.3", imageRef(dn))
}

// TestDefaultDingoTagFloor guards the one property of DefaultDingoTag that is a
// correctness constraint rather than a preference: Dingo releases before 0.68.0
// permanently brick their data volume if the pod is rolled mid-genesis-write
// (dingo #2959, fixed in 0.68.0 by #2975). A DingoNode that omits
// spec.image.tag gets this value, and every rotation, config-bundle change and
// reschedule rolls the pod — so drifting back below the fix would hand block
// producers a default that can be bricked on first boot.
//
// TestImageRef above interpolates the const symbolically and so passes for any
// value, and the e2e suite always sets spec.image explicitly, so without this
// test nothing would catch such a drift.
func TestDefaultDingoTagFloor(t *testing.T) {
	const (
		floorMinor = 68
		floorText  = "0.68.0"
	)

	parts := strings.Split(DefaultDingoTag, ".")
	require.Len(t, parts, 3, "DefaultDingoTag %q is not major.minor.patch",
		DefaultDingoTag)

	major, err := strconv.Atoi(parts[0])
	require.NoError(t, err, "major version in %q", DefaultDingoTag)
	minor, err := strconv.Atoi(parts[1])
	require.NoError(t, err, "minor version in %q", DefaultDingoTag)

	// Dingo is pre-1.0; a future 1.x is unambiguously past the fix.
	if major > 0 {
		return
	}
	assert.GreaterOrEqual(t, minor, floorMinor,
		"DefaultDingoTag %q predates the genesis-restart fix in %s; see dingo "+
			"#2959 before lowering it",
		DefaultDingoTag, floorText)
}
