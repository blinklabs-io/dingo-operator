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
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
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

// NaN for the loaded counter means the node has no opcert loaded for the
// pool; a series that is absent says nothing and keeps the last value.
func TestRefreshForgeStatusClearsLoadedCounterOnNaN(t *testing.T) {
	t.Parallel()
	t.Run("labelled", func(t *testing.T) {
		t.Parallel()
		pools := newTestPools(t, "a", "b")
		dn := poolListNode("bp", "ns", 0, pools...)
		for _, p := range pools {
			ps := poolStatus(dn, p.id)
			ps.KeysValid = true
			ps.LoadedCounter = new(int64(6))
		}
		a, b := poolLabelOf(pools[0].id), poolLabelOf(pools[1].id)
		st, err := forgestatus.Parse(strings.NewReader(fmt.Sprintf(`
cardano_node_metrics_currentKESPeriod_int{pool_id="%[1]s"} 900
cardano_node_metrics_remainingKESPeriods_int{pool_id="%[1]s"} 20
dingo_forge_opcert_counter_loaded{pool_id="%[1]s"} NaN
cardano_node_metrics_currentKESPeriod_int{pool_id="%[2]s"} 900
cardano_node_metrics_remainingKESPeriods_int{pool_id="%[2]s"} 20
`, a, b)))
		require.NoError(t, err)
		r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
		r.refreshForgeStatus(context.Background(), dn)
		assert.Nil(t, poolStatus(dn, pools[0].id).LoadedCounter)
		got := poolStatus(dn, pools[1].id).LoadedCounter
		require.NotNil(t, got, "an absent series keeps the last value")
		assert.Equal(t, int64(6), *got)
	})
	t.Run("one pool unlabelled", func(t *testing.T) {
		t.Parallel()
		pools := newTestPools(t, "a")
		dn := poolListNode("bp", "ns", 0, pools...)
		ps := poolStatus(dn, pools[0].id)
		ps.KeysValid = true
		ps.LoadedCounter = new(int64(6))
		st, err := forgestatus.Parse(strings.NewReader(`
cardano_node_metrics_currentKESPeriod_int 900
cardano_node_metrics_remainingKESPeriods_int 20
dingo_forge_opcert_counter_loaded NaN
`))
		require.NoError(t, err)
		r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
		r.refreshForgeStatus(context.Background(), dn)
		assert.Nil(t, poolStatus(dn, pools[0].id).LoadedCounter)
	})
}
