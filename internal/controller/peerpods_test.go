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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// topologyAddresses returns the access points of the node's rendered topology.
func topologyAddresses(
	t *testing.T,
	c client.Client,
	name string,
) []string {
	t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(
		t.Context(),
		types.NamespacedName{Name: name + "-topology", Namespace: "ns"},
		cm,
	))
	var doc struct {
		LocalRoots []struct {
			AccessPoints []struct {
				Address string `json:"address"`
			} `json:"accessPoints"`
			Valency     int `json:"valency"`
			WarmValency int `json:"warmValency"`
		} `json:"localRoots"`
	}
	require.NoError(t, json.Unmarshal([]byte(cm.Data["topology.json"]), &doc))
	require.Len(t, doc.LocalRoots, 1)
	root := doc.LocalRoots[0]
	addrs := make([]string, 0, len(root.AccessPoints))
	for _, ap := range root.AccessPoints {
		addrs = append(addrs, ap.Address)
	}
	assert.Equal(t, len(addrs), root.Valency)
	assert.Equal(t, len(addrs), root.WarmValency)
	return addrs
}

// Producer and relay each list every pod of the other. The relay's
// relayRefs name for an agent-signed producer with two pods yields two
// peers, and a change of the producer's replicas re-renders the relay's
// topology on its next reconcile.
func TestTopologyListsEveryPodOfAReferencedNode(t *testing.T) {
	t.Parallel()
	bp := agentProducer(2)
	bp.Spec.Topology.RelayRefs = []string{"relay"}
	relay := relayNodeForStatus("relay", "ns")
	relay.Spec.Replicas = new(int32(2))
	relay.Spec.Topology.RelayRefs = []string{"bp"}
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
		WithObjects(bp, relay).Build()
	r := reconcilerFor(c)

	require.NoError(t, r.reconcileResources(t.Context(), relay))
	require.NoError(t, r.reconcileResources(t.Context(), bp))
	assert.Equal(t, []string{
		"bp-0.bp-headless.ns.svc.cluster.local",
		"bp-1.bp-headless.ns.svc.cluster.local",
	}, topologyAddresses(t, c, "relay"))
	assert.Equal(t, []string{
		"relay-0.relay-headless.ns.svc.cluster.local",
		"relay-1.relay-headless.ns.svc.cluster.local",
	}, topologyAddresses(t, c, "bp"))

	require.NoError(t, c.Get(
		t.Context(), types.NamespacedName{Name: "bp", Namespace: "ns"}, bp,
	))
	bp.Spec.Replicas = new(int32(3))
	require.NoError(t, c.Update(t.Context(), bp))
	require.NoError(t, r.reconcileResources(t.Context(), relay))
	assert.Equal(t, []string{
		"bp-0.bp-headless.ns.svc.cluster.local",
		"bp-1.bp-headless.ns.svc.cluster.local",
		"bp-2.bp-headless.ns.svc.cluster.local",
	}, topologyAddresses(t, c, "relay"))
}

// A producer holding its KES key in a Secret runs one pod whatever
// spec.replicas says, and is listed as one.
func TestTopologyListsOnePodOfAKeyFileProducer(t *testing.T) {
	t.Parallel()
	bp := forgerNode("bp", "ns", conflictPoolID, "keys-a", 0)
	bp.Spec.Replicas = new(int32(3))
	relay := relayNodeForStatus("relay", "ns")
	relay.Spec.Topology.RelayRefs = []string{"bp"}
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
		WithObjects(bp, relay).Build()

	require.NoError(t, reconcilerFor(c).reconcileResources(t.Context(), relay))
	assert.Equal(t, []string{"bp-0.bp-headless.ns.svc.cluster.local"},
		topologyAddresses(t, c, "relay"))
}
