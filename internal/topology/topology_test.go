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

package topology

import (
	"encoding/json"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNode(topo dingov1alpha1.TopologySpec) *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:     dingov1alpha1.RoleBlockProducer,
			Network:  "mainnet",
			Topology: topo,
		},
	}
}

func TestRender(t *testing.T) {
	t.Parallel()

	t.Run("empty topology has no content", func(t *testing.T) {
		_, hasContent, err := Render(newNode(dingov1alpha1.TopologySpec{}), "ns")
		require.NoError(t, err)
		assert.False(t, hasContent)
	})

	t.Run("external relays become local roots", func(t *testing.T) {
		dn := newNode(dingov1alpha1.TopologySpec{
			ExternalRelays: []dingov1alpha1.ExternalRelay{
				{Address: "relay.example.com", Port: 3001, Valency: 2, Trustable: true},
			},
		})
		out, hasContent, err := Render(dn, "ns")
		require.NoError(t, err)
		assert.True(t, hasContent)

		var doc document
		require.NoError(t, json.Unmarshal([]byte(out), &doc))
		require.Len(t, doc.LocalRoots, 1)
		assert.Equal(t, "relay.example.com", doc.LocalRoots[0].AccessPoints[0].Address)
		assert.Equal(t, 2, doc.LocalRoots[0].Valency)
		assert.True(t, doc.LocalRoots[0].Trustable)
	})

	t.Run("each relay ref is its own local root group", func(t *testing.T) {
		dn := newNode(dingov1alpha1.TopologySpec{
			RelayRefs: []string{"relay-0", "relay-1"},
		})
		out, hasContent, err := Render(dn, "cardano")
		require.NoError(t, err)
		assert.True(t, hasContent)

		var doc document
		require.NoError(t, json.Unmarshal([]byte(out), &doc))
		require.Len(t, doc.LocalRoots, 2)
		for i, want := range []string{
			"relay-0-headless.cardano.svc.cluster.local",
			"relay-1-headless.cardano.svc.cluster.local",
		} {
			g := doc.LocalRoots[i]
			require.Len(t, g.AccessPoints, 1)
			assert.Equal(t, want, g.AccessPoints[0].Address)
			assert.Equal(t, int32(relayNtNPort), g.AccessPoints[0].Port)
			assert.Equal(t, 1, g.Valency)
			assert.Equal(t, 1, g.WarmValency)
			assert.True(t, g.Trustable)
			assert.False(t, g.Advertise)
		}
	})

	t.Run("relay ref groups precede external relay groups", func(t *testing.T) {
		dn := newNode(dingov1alpha1.TopologySpec{
			RelayRefs: []string{"relay-0", "relay-1"},
			ExternalRelays: []dingov1alpha1.ExternalRelay{
				{Address: "relay.example.com", Port: 3001, Valency: 1, Trustable: true},
			},
		})
		out, _, err := Render(dn, "ns")
		require.NoError(t, err)

		var doc document
		require.NoError(t, json.Unmarshal([]byte(out), &doc))
		require.Len(t, doc.LocalRoots, 3)
		assert.Equal(t, "relay-0-headless.ns.svc.cluster.local", doc.LocalRoots[0].AccessPoints[0].Address)
		assert.Equal(t, "relay-1-headless.ns.svc.cluster.local", doc.LocalRoots[1].AccessPoints[0].Address)
		assert.Equal(t, "relay.example.com", doc.LocalRoots[2].AccessPoints[0].Address)
	})

	t.Run("auto-peer disabled omits relay refs", func(t *testing.T) {
		off := false
		dn := newNode(dingov1alpha1.TopologySpec{
			AutoPeerRelays: &off,
			RelayRefs:      []string{"relay-0"},
		})
		_, hasContent, err := Render(dn, "ns")
		require.NoError(t, err)
		assert.False(t, hasContent)
	})
}
