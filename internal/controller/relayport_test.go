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
	"encoding/json"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A relayRefs peer is dialed on the port its own DingoNode configures, not the
// referencing node's.
func TestTopologyDialsReferencedNodesRelayPort(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))

	relay := func(name string, port int32) *dingov1alpha1.DingoNode {
		return &dingov1alpha1.DingoNode{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: dingov1alpha1.DingoNodeSpec{
				Role:         dingov1alpha1.RoleRelay,
				Network:      "mainnet",
				RelayService: dingov1alpha1.RelayServiceSpec{Port: port},
			},
		}
	}
	bp := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "mainnet",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				SlotsPerKESPeriod: 129600,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: "keys"},
			},
			Topology: dingov1alpha1.TopologySpec{
				RelayRefs: []string{"musashi", "plain", "not-created"},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(relay("musashi", 3010), relay("plain", 0), bp).Build()

	require.NoError(t, reconcilerFor(c).reconcileResources(context.Background(), bp))

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(
		context.Background(),
		types.NamespacedName{Name: "bp-topology", Namespace: "ns"},
		cm,
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
	require.Len(t, doc.LocalRoots, 1)
	ports := map[string]int32{}
	for _, ap := range doc.LocalRoots[0].AccessPoints {
		ports[ap.Address] = ap.Port
	}
	assert.Equal(t, map[string]int32{
		"musashi-headless.ns.svc.cluster.local":     3010,
		"plain-headless.ns.svc.cluster.local":       3001,
		"not-created-headless.ns.svc.cluster.local": 3001,
	}, ports)
}
