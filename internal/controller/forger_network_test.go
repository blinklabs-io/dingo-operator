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

package controller

import (
	"context"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// onNetwork moves n to a custom network with the given magic.
func onNetwork(
	n *dingov1alpha1.DingoNode,
	magic int64,
) *dingov1alpha1.DingoNode {
	n.Spec.Network = "custom"
	n.Spec.NetworkMagic = &magic
	n.Spec.ConfigRef = "genesis"
	n.Spec.Mithril.Enabled = new(false)
	return n
}

func TestNetworkKey(t *testing.T) {
	t.Parallel()
	preview := forgerNode("a", "ns", conflictPoolID, "k", 0)
	assert.Equal(t, "magic:2", networkKey(preview))
	assert.Equal(t, networkKey(preview), networkKey(onNetwork(
		forgerNode("b", "ns", conflictPoolID, "k", 0), 2,
	)), "a custom network with a named network's magic is that network")
	assert.NotEqual(t, networkKey(preview), networkKey(onNetwork(
		forgerNode("b", "ns", conflictPoolID, "k", 0), 3,
	)))
	assert.NotEqual(t,
		networkKey(onNetwork(forgerNode("a", "ns", conflictPoolID, "k", 0), 7)),
		networkKey(onNetwork(forgerNode("b", "ns", conflictPoolID, "k", 0), 8)),
		"two custom magics are two networks")

	// Dingo takes its network from the rendered env, so an override counts.
	moved := forgerNode("c", "ns", conflictPoolID, "k", 0)
	moved.Spec.Network = "preprod"
	moved.Spec.Environment = map[string]string{"CARDANO_NETWORK": "preview"}
	assert.Equal(t, networkKey(preview), networkKey(moved))
	moved.Spec.Environment = map[string]string{"CARDANO_NETWORK_MAGIC": "2"}
	assert.Equal(t, networkKey(preview), networkKey(moved))

	unknown := forgerNode("d", "ns", conflictPoolID, "k", 0)
	unknown.Spec.Network = "no-such-network"
	assert.Empty(
		t,
		networkKey(unknown),
		"an unknown name is not compared by name",
	)
	unknown.Spec.Network = ""
	assert.Empty(t, networkKey(unknown), "no network at all is unknown")

	// Other nodes are unvalidated, and their env is rendered all the same.
	assert.Empty(t, networkKey(&dingov1alpha1.DingoNode{}))
	bare := &dingov1alpha1.DingoNode{}
	bare.Spec.Role = dingov1alpha1.RoleBlockProducer
	bare.Spec.Network = "preview"
	assert.Equal(t, "magic:2", networkKey(bare))
}

func TestEnvNetwork(t *testing.T) {
	t.Parallel()
	v := func(name, value string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, Value: value}
	}
	from := func(name string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
				Key:                  "k",
			},
		}}
	}
	for _, tt := range []struct {
		name    string
		env     []corev1.EnvVar
		envFrom bool
		want    string
	}{
		{"known name", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preview"),
		}, false, "magic:2"},
		{"magic wins over name", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preview"), v("CARDANO_NETWORK_MAGIC", "42"),
		}, false, "magic:42"},
		{"zero magic falls back to the name", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preview"), v("CARDANO_NETWORK_MAGIC", "0"),
		}, false, "magic:2"},
		{"last duplicate wins", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preprod"), v("CARDANO_NETWORK", "preview"),
		}, false, "magic:2"},
		{"magic valueFrom", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preview"), from("CARDANO_NETWORK_MAGIC"),
		}, false, ""},
		{"name valueFrom", []corev1.EnvVar{from("CARDANO_NETWORK")}, false, ""},
		{"unparseable magic", []corev1.EnvVar{
			v("CARDANO_NETWORK", "preview"), v("CARDANO_NETWORK_MAGIC", "two"),
		}, false, ""},
		// Dingo parses the magic as a Go integer literal (base 0).
		{"leading zeros", []corev1.EnvVar{
			v("CARDANO_NETWORK_MAGIC", "002"),
		}, false, "magic:2"},
		{"octal magic", []corev1.EnvVar{
			v("CARDANO_NETWORK_MAGIC", "010"),
		}, false, "magic:8"},
		{"hex magic", []corev1.EnvVar{
			v("CARDANO_NETWORK_MAGIC", "0x2"),
		}, false, "magic:2"},
		{"magic out of range", []corev1.EnvVar{
			v("CARDANO_NETWORK_MAGIC", "99999999999"),
		}, false, ""},
		{"envFrom", []corev1.EnvVar{v("CARDANO_NETWORK", "preview")}, true, ""},
		{"unknown name", []corev1.EnvVar{
			v("CARDANO_NETWORK", "nope"),
		}, false, ""},
		{"no network", nil, false, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, envNetwork(tt.env, tt.envFrom))
		})
	}
}

func TestPodNetwork(t *testing.T) {
	t.Parallel()
	dingo := func(c corev1.Container) corev1.PodSpec {
		c.Name = "dingo"
		return corev1.PodSpec{Containers: []corev1.Container{c}}
	}
	env := []corev1.EnvVar{{Name: "CARDANO_NETWORK", Value: "preview"}}
	assert.Equal(t, "magic:2", podNetwork(dingo(corev1.Container{Env: env})))
	assert.Empty(t, podNetwork(dingo(corev1.Container{
		Env: env,
		EnvFrom: []corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
			}},
		},
	})), "envFrom can set the network where it cannot be read")
	assert.Empty(t, podNetwork(dingo(corev1.Container{
		Env: env, Args: []string{"--network-magic", "1"},
	})), "arguments can set the network where it cannot be read")
	assert.Empty(t, podNetwork(dingo(corev1.Container{
		Env: env, Command: []string{"/bin/dingo"},
	})), "a command can set the network where it cannot be read")
	assert.Empty(t, podNetwork(corev1.PodSpec{Containers: []corev1.Container{
		{Name: "sidecar", Env: env},
	}}), "no dingo container")
}

func TestForgerConflictIsPerNetwork(t *testing.T) {
	t.Parallel()
	type node = dingov1alpha1.DingoNode
	for _, tt := range []struct {
		name     string
		other    func(*node) *node
		conflict bool
	}{
		{"different named network", func(n *node) *node {
			n.Spec.Network = "preprod"
			return n
		}, false},
		{"custom network with another magic", func(n *node) *node {
			return onNetwork(n, 424242)
		}, false},
		{"custom network with the same magic", func(n *node) *node {
			return onNetwork(n, 2)
		}, true},
		{"same network", func(n *node) *node {
			return n
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			older := forgerNode("bp", "ns", conflictPoolID, "keys-a", time.Hour)
			newer := tt.other(
				forgerNode("bp-2", "ns", conflictPoolID, "keys-b", 0),
			)
			err := forgerConflict(
				newer,
				[]dingov1alpha1.DingoNode{*older, *newer},
			)
			if tt.conflict {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	// The keys Secret is the keys, whatever network each node names.
	older := forgerNode("bp", "ns", conflictPoolID, "keys", time.Hour)
	newer := onNetwork(forgerNode("bp-2", "ns", otherPoolID, "keys", 0), 424242)
	assert.Error(
		t,
		forgerConflict(newer, []dingov1alpha1.DingoNode{*older, *newer}),
	)
}

// One cold key registers the same pool on several networks: a producer on each,
// in different namespaces, must both run, whichever keys Secret holds a bound
// KES key.
func TestReconcileSamePoolOnDifferentNetworksBothForge(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		newer func(*dingov1alpha1.DingoNode) *dingov1alpha1.DingoNode
	}{
		{"named networks", func(
			n *dingov1alpha1.DingoNode,
		) *dingov1alpha1.DingoNode {
			n.Spec.Network = "preprod"
			return n
		}},
		{"custom network", func(
			n *dingov1alpha1.DingoNode,
		) *dingov1alpha1.DingoNode {
			return onNetwork(n, 424242)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			older := forgerNode("bp", "ns-a", pool, "keys-a", time.Hour)
			older.UID = "older-uid"
			newer := tt.newer(forgerNode("bp", "ns-b", pool, "keys-b", 0))
			newer.UID = "newer-uid"
			c := conflictClient(t, older, newer,
				keysSecret("keys-a", "ns-a", data),
				keysSecret("keys-b", "ns-b", data),
			)
			r := reconcilerFor(c)
			for range 2 {
				reconcileNode(t, r, older)
				reconcileNode(t, r, newer)
			}
			for _, n := range []*dingov1alpha1.DingoNode{older, newer} {
				assert.True(t, forging(t, c, n), n.Namespace)
				assert.False(t, refusedAsDuplicate(t, c, n), n.Namespace)
			}
		})
	}
}

func TestReconcileSamePoolOnDifferentNetworksSameNamespace(t *testing.T) {
	t.Parallel()
	pool, data := poolKeys(t)
	older := forgerNode("bp-1", "ns", pool, "keys-a", time.Hour)
	older.UID = "older-uid"
	newer := forgerNode("bp-2", "ns", pool, "keys-b", 0)
	newer.Spec.Network = "preprod"
	newer.UID = "newer-uid"
	c := conflictClient(t, older, newer,
		keysSecret("keys-a", "ns", data), keysSecret("keys-b", "ns", data),
	)
	r := reconcilerFor(c)
	for range 2 {
		reconcileNode(t, r, older)
		reconcileNode(t, r, newer)
	}
	for _, n := range []*dingov1alpha1.DingoNode{older, newer} {
		assert.True(t, forging(t, c, n), n.Name)
		assert.False(
			t,
			refusedAsDuplicate(t, c, n),
			"%s: %s",
			n.Name,
			degradedMessage(t, c, n),
		)
	}
}

func TestReconcileSamePoolOnSameNetworkStillRefused(t *testing.T) {
	t.Parallel()
	for _, ns := range []string{"ns-a", "ns-b"} {
		t.Run(ns, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			older := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
			older.UID = "older-uid"
			// Same magic as preview, spelled as a custom network.
			newer := onNetwork(forgerNode("bp-2", ns, pool, "keys-b", 0), 2)
			newer.UID = "newer-uid"
			c := conflictClient(
				t,
				older,
				newer,
				keysSecret(
					"keys-a",
					"ns-a",
					data,
				),
				keysSecret("keys-b", ns, data),
			)
			r := reconcilerFor(c)
			reconcileNode(t, r, older)
			reconcileNode(t, r, newer)
			assert.True(t, refusedAsDuplicate(t, c, newer))
			assert.False(t, forging(t, c, newer))
			reconcileNode(t, r, older)
			assert.True(t, forging(t, c, older))
		})
	}
}

// An orphan pod holds a pool only on the network its dingo container's env
// names; one whose network cannot be read may be on any.
func TestReconcileOrphanPodHoldsPoolPerNetwork(t *testing.T) {
	t.Parallel()
	plain := func(name, value string) []corev1.EnvVar {
		return []corev1.EnvVar{{Name: name, Value: value}}
	}
	for _, tt := range []struct {
		name    string
		env     []corev1.EnvVar
		envFrom bool
		holds   bool
	}{
		{"same network", plain("CARDANO_NETWORK", "preview"), false, true},
		{"different network", plain("CARDANO_NETWORK", "preprod"),
			false, false},
		{"different magic", plain("CARDANO_NETWORK_MAGIC", "424242"),
			false, false},
		{"same magic", plain("CARDANO_NETWORK_MAGIC", "2"), false, true},
		{"no network env", nil, false, true},
		{"unknown name", plain("CARDANO_NETWORK", "nope"), false, true},
		{"envFrom", plain("CARDANO_NETWORK", "preprod"), true, true},
		{"magic from a Secret", []corev1.EnvVar{
			{Name: "CARDANO_NETWORK", Value: "preprod"},
			{Name: "CARDANO_NETWORK_MAGIC", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "s",
					},
					Key: "k",
				},
			}},
		}, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			pool, data := poolKeys(t)
			victim := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
			victim.UID = "victim-uid"
			c := conflictClient(t, victim, keysSecret("keys-a", "ns-a", data))
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.True(t, forging(t, c, victim))

			pod, _ := forgedWorkload(pool)
			container := corev1.Container{Name: "dingo", Env: tt.env}
			if tt.envFrom {
				container.EnvFrom = []corev1.EnvFromSource{{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cm",
						},
					},
				}}
			}
			pod.Spec.Containers = []corev1.Container{container}
			require.NoError(t, c.Create(ctx, pod))
			reconcileNode(t, r, victim)
			assert.Equal(t, tt.holds, refusedAsDuplicate(t, c, victim))
			assert.True(t, forging(t, c, victim), "an orphan never halts")
		})
	}
}

// Producers already running have templates from before the operator told them
// apart by network: their env names the network, and nothing else is recorded.
func TestEnvtestSamePoolOnDifferentNetworksBothForge(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "net-a")
	createNamespace(t, ctx, c, "net-b")
	pool, data := poolKeys(t)
	for _, ns := range []string{"net-a", "net-b"} {
		require.NoError(t, c.Create(ctx, keysSecret("keys", ns, data)))
	}
	older := blockProducerNode("bp", "net-a", "keys", pool)
	require.NoError(t, c.Create(ctx, older))
	r := reconcilerFor(c)
	reconcileNode(t, r, older)
	require.True(t, forging(t, c, older))
	tmpl := podTemplate(t, ctx, c, "bp", "net-a")
	require.Contains(t, tmpl.Annotations, resources.KeysPoolAnnotation)

	newer := blockProducerNode("bp", "net-b", "keys", pool)
	newer.Spec.Network = "preprod"
	require.NoError(t, c.Create(ctx, newer))
	reconcileNode(t, r, newer)
	reconcileNode(t, r, older)
	for _, n := range []*dingov1alpha1.DingoNode{older, newer} {
		assert.True(t, forging(t, c, n), n.Namespace)
		assert.False(t, refusedAsDuplicate(t, c, n), "%s: %s",
			n.Namespace, degradedMessage(t, c, n))
	}
}

// Dingo takes its network from env, which spec.environment overrides, so a
// node that names another network but runs this one is the same forger.
func TestEnvtestEnvironmentOverrideKeepsPoolFenced(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "ovr-a")
	createNamespace(t, ctx, c, "ovr-b")
	pool, data := poolKeys(t)
	for _, ns := range []string{"ovr-a", "ovr-b"} {
		require.NoError(t, c.Create(ctx, keysSecret("keys", ns, data)))
	}
	older := blockProducerNode("bp", "ovr-a", "keys", pool)
	require.NoError(t, c.Create(ctx, older))
	r := reconcilerFor(c)
	reconcileNode(t, r, older)
	require.True(t, forging(t, c, older))

	newer := blockProducerNode("bp", "ovr-b", "keys", pool)
	newer.Spec.Network = "preprod"
	newer.Spec.Environment = map[string]string{"CARDANO_NETWORK": "preview"}
	require.NoError(t, c.Create(ctx, newer))
	reconcileNode(t, r, newer)
	assert.True(t, refusedAsDuplicate(t, c, newer))
	assert.False(t, forging(t, c, newer))
	reconcileNode(t, r, older)
	assert.True(t, forging(t, c, older))
}

func degradedMessage(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) string {
	t.Helper()
	var got dingov1alpha1.DingoNode
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &got,
	))
	d := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
	if d == nil {
		return ""
	}
	return d.Message
}
