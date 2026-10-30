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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func relayServicePort(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) int32 {
	t.Helper()
	svc := &corev1.Service{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: name, Namespace: ns}, svc,
	))
	for _, p := range svc.Spec.Ports {
		if p.Name == "relay" {
			return p.Port
		}
	}
	t.Fatalf("service %s has no relay port", name)
	return 0
}

// peerIngressPorts returns the TCP ports the block producer's NetworkPolicy
// admits from its declared relay peers.
func peerIngressPorts(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) []int32 {
	t.Helper()
	np := &networkingv1.NetworkPolicy{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: name, Namespace: ns}, np,
	))
	var ports []int32
	for _, rule := range np.Spec.Ingress {
		if len(rule.From) != 1 || rule.From[0].PodSelector == nil ||
			len(rule.From[0].PodSelector.MatchExpressions) == 0 {
			continue
		}
		for _, p := range rule.Ports {
			ports = append(ports, p.Port.IntVal)
		}
	}
	return ports
}

func relayContainerPort(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) int32 {
	t.Helper()
	tmpl := podTemplate(t, ctx, c, name, ns)
	for _, ct := range tmpl.Spec.Containers {
		for _, p := range ct.Ports {
			if p.Name == "relay" {
				return p.ContainerPort
			}
		}
	}
	t.Fatal("no relay container port")
	return 0
}

// topologyPortFor returns the port the node's topology dials for address.
func topologyPortFor(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns, address string,
) int32 {
	t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: name + "-topology", Namespace: ns}, cm,
	))
	var doc struct {
		LocalRoots []struct {
			AccessPoints []struct {
				Address string `json:"address"`
				Port    int32  `json:"port"`
			} `json:"accessPoints"`
		} `json:"localRoots"`
	}
	require.NoError(t, json.Unmarshal([]byte(cm.Data["topology.json"]), &doc))
	for _, root := range doc.LocalRoots {
		for _, ap := range root.AccessPoints {
			if ap.Address == address {
				return ap.Port
			}
		}
	}
	t.Fatalf("topology of %s does not dial %s", name, address)
	return 0
}

// While a block producer's template is held (keys missing), the pod keeps
// listening on its live port. Services, the NetworkPolicy and relay peers'
// topology must keep targeting that port, and move to the spec port once the
// keys validate and the template rolls.
func TestHeldTemplateKeepsLiveRelayPort(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	const ns = "held-port-ns"
	createNamespace(t, ctx, c, ns)

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	good, err := keys.SecretData(2, 0)
	require.NoError(t, err)
	secret := &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", ns),
		Data:       good,
	}
	require.NoError(t, c.Create(ctx, secret))

	bp := blockProducerNode(
		"bp", ns, "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	bp.Spec.Topology.RelayRefs = []string{"rl"}
	bp.Spec.RelayService.Port = 3001
	require.NoError(t, c.Create(ctx, bp))
	rl := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "rl", Namespace: ns},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:     dingov1alpha1.RoleRelay,
			Network:  "preview",
			Topology: dingov1alpha1.TopologySpec{RelayRefs: []string{"bp"}},
		},
	}
	require.NoError(t, c.Create(ctx, rl))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	reconcile(t, ctx, reconcilerFor(c), "rl", ns)
	require.Equal(t, int32(3001), relayContainerPort(t, ctx, c, "bp", ns))

	// The keys go missing and the port changes while the template is held.
	require.NoError(t, c.Delete(ctx, secret))
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: "bp", Namespace: ns}, bp,
	))
	bp.Spec.RelayService.Port = 4001
	require.NoError(t, c.Update(ctx, bp))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	reconcile(t, ctx, reconcilerFor(c), "rl", ns)

	assert.Equal(t, int32(3001), relayContainerPort(t, ctx, c, "bp", ns),
		"the held template keeps the old container port")
	for _, svc := range []string{"bp", "bp-relay", "bp-headless"} {
		assert.Equal(t, int32(3001), relayServicePort(t, ctx, c, svc, ns),
			"service %s must target the port the live pod listens on", svc)
	}
	assert.Equal(t, []int32{3001}, peerIngressPorts(t, ctx, c, "bp", ns),
		"the policy must admit the live port to declared relays")
	assert.Equal(t, int32(3001), topologyPortFor(
		t, ctx, c, "rl", ns, "bp-headless.held-port-ns.svc.cluster.local",
	), "relay peers must dial the live port")

	// Valid keys return: the template rolls and everything moves.
	require.NoError(t, keys.RotateKES(rand.Reader))
	fixed, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", ns),
		Data:       fixed,
	}))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	reconcile(t, ctx, reconcilerFor(c), "rl", ns)

	assert.Equal(t, int32(4001), relayContainerPort(t, ctx, c, "bp", ns))
	for _, svc := range []string{"bp", "bp-relay", "bp-headless"} {
		assert.Equal(t, int32(4001), relayServicePort(t, ctx, c, svc, ns))
	}
	assert.Equal(t, []int32{4001}, peerIngressPorts(t, ctx, c, "bp", ns))
	assert.Equal(t, int32(4001), topologyPortFor(
		t, ctx, c, "rl", ns, "bp-headless.held-port-ns.svc.cluster.local",
	))
}
