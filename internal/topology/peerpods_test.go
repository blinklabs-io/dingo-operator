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

func renderedPoints(
	t *testing.T,
	refs []string,
	peers map[string]Peer,
) (document, []accessPoint) {
	t.Helper()
	dn := newNode(dingov1alpha1.TopologySpec{RelayRefs: refs})
	out, hasContent, err := Render(dn, "cardano", peers)
	require.NoError(t, err)
	require.True(t, hasContent)
	var doc document
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	require.Len(t, doc.LocalRoots, 1)
	return doc, doc.LocalRoots[0].AccessPoints
}

// Every pod of a referenced node is a peer: its headless name resolves to all
// of them, but a node dials only one resolved address per access point.
func TestRenderListsEveryPodOfAPeer(t *testing.T) {
	t.Parallel()
	doc, points := renderedPoints(t,
		[]string{"bp", "relay"},
		map[string]Peer{
			"bp":    {Port: 3001, Replicas: 2},
			"relay": {Port: 3010, Replicas: 3},
		})

	got := make(map[string]int32, len(points))
	for _, p := range points {
		got[p.Address] = p.Port
	}
	assert.Equal(t, map[string]int32{
		"bp-0.bp-headless.cardano.svc.cluster.local":       3001,
		"bp-1.bp-headless.cardano.svc.cluster.local":       3001,
		"relay-0.relay-headless.cardano.svc.cluster.local": 3010,
		"relay-1.relay-headless.cardano.svc.cluster.local": 3010,
		"relay-2.relay-headless.cardano.svc.cluster.local": 3010,
	}, got)
	assert.Len(t, points, 5, "no pod is listed twice")
	assert.Equal(t, 5, doc.LocalRoots[0].Valency)
	assert.Equal(t, 5, doc.LocalRoots[0].WarmValency)
}

// A peer scaled to zero keeps its headless name. Dropping it would empty the
// topology, which is then omitted and leaves the node dialing Dingo's built-in
// bootstrap peers.
func TestRenderKeepsHeadlessNameForPeerWithoutPods(t *testing.T) {
	t.Parallel()
	doc, points := renderedPoints(t,
		[]string{"idle", "up"},
		map[string]Peer{
			"idle": {Port: 3001, Replicas: 0},
			"up":   {Port: 3001, Replicas: 2},
		})
	var addrs []string
	for _, p := range points {
		addrs = append(addrs, p.Address)
	}
	assert.Equal(t, []string{
		"idle-headless.cardano.svc.cluster.local",
		"up-0.up-headless.cardano.svc.cluster.local",
		"up-1.up-headless.cardano.svc.cluster.local",
	}, addrs)
	assert.Equal(t, 3, doc.LocalRoots[0].Valency)

	dn := newNode(dingov1alpha1.TopologySpec{RelayRefs: []string{"idle"}})
	_, hasContent, err := Render(dn, "cardano", map[string]Peer{
		"idle": {Port: 3001},
	})
	require.NoError(t, err)
	assert.True(t, hasContent,
		"a producer whose only peer is scaled to zero must keep its topology")
}
