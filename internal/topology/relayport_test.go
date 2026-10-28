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

func TestRenderRelayRefPorts(t *testing.T) {
	t.Parallel()
	dn := newNode(dingov1alpha1.TopologySpec{
		RelayRefs: []string{"musashi", "default-port", "unresolved"},
	})
	out, hasContent, err := Render(dn, "cardano", map[string]int32{
		"musashi":      3010,
		"default-port": 3001,
	})
	require.NoError(t, err)
	require.True(t, hasContent)

	var doc document
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	require.Len(t, doc.LocalRoots, 1)
	points := doc.LocalRoots[0].AccessPoints
	require.Len(t, points, 3)
	assert.Equal(
		t,
		"musashi-headless.cardano.svc.cluster.local",
		points[0].Address,
	)
	assert.Equal(t, int32(3010), points[0].Port)
	assert.Equal(t, int32(3001), points[1].Port)
	// A ref whose DingoNode is unknown keeps the default relay port.
	assert.Equal(t, int32(3001), points[2].Port)
}
