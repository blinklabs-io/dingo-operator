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

	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The KES period is the network's, not a pool's: a pool the node has not
// reported on yet is judged by the period its other pools report, so an
// expired bundle is not accepted for it.
func TestReconcilePoolKeysChecksExpiryOfAnUnobservedPool(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	poolStatus(dn, pools[0].id).KES.CurrentPeriod = 900
	c := conflictClient(t, dn,
		poolSecret(t, "ns", pools[0], 0),
		poolSecret(t, "ns", pools[1], 0),
	)
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	_, _, err := poolReconciler(c).reconcilePoolKeys(
		context.Background(), dn, &opts,
	)
	require.NoError(t, err)
	b := poolStatus(dn, pools[1].id)
	assert.False(t, b.KeysValid)
	assert.Contains(t, b.KeysMessage, "opcert expired")
}
