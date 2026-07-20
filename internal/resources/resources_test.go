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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
