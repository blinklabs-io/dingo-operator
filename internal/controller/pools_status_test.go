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
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A node that forges for one pool exposes its forge series unlabelled, and
// Dingo names the loaded counter dingo_forge_opcert_counter_loaded there too.
func TestRefreshForgeStatusOnePoolReadsUnlabelledLoadedCounter(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	poolStatus(dn, pools[0].id).KeysValid = true // pool b was dropped
	st, err := forgestatus.Parse(strings.NewReader(`
cardano_node_metrics_currentKESPeriod_int 900
cardano_node_metrics_remainingKESPeriods_int 20
cardano_node_metrics_operationalCertificateStartKESPeriod_int 858
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int 920
dingo_forge_opcert_counter_loaded 6
dingo_forge_opcert_counter_onchain 5
dingo_ledger_pipeline_stuck 0
dingo_ledger_pipeline_halted 0
`))
	require.NoError(t, err)
	r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
	r.refreshForgeStatus(context.Background(), dn)

	ps := poolStatus(dn, pools[0].id)
	assert.Equal(t, int64(20), ps.KES.RemainingPeriods)
	require.NotNil(t, ps.LoadedCounter)
	assert.Equal(t, int64(6), *ps.LoadedCounter)
}

// status.pools describes a pool list; it does not outlive one.
func TestReconcileResourcesClearsPoolStatusWithoutPoolList(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		node func() *dingov1alpha1.DingoNode
	}{
		{"single pool", func() *dingov1alpha1.DingoNode {
			return forgerNode("bp", "ns", conflictPoolID, "keys", 0)
		}},
		{"relay", func() *dingov1alpha1.DingoNode {
			dn := forgerNode("relay", "ns", "", "", 0)
			dn.Spec.Role = dingov1alpha1.RoleRelay
			dn.Spec.BlockProducer = nil
			return dn
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := tc.node()
			dn.Status.Pools = []dingov1alpha1.PoolStatus{{
				PoolID: otherPoolID, KeysValid: true, OnDiskCounter: 3,
			}}
			c := conflictClient(t, dn)
			require.NoError(
				t, reconcilerFor(c).reconcileResources(context.Background(), dn),
			)
			assert.Empty(t, dn.Status.Pools)
		})
	}
}

// onChainBoth answers a single-pool query and a pool-list query alike.
type onChainBoth struct {
	value  int64
	single int
	lists  int
}

func (s *onChainBoth) Fetch(
	context.Context,
	onchain.Query,
) (onchain.Counter, error) {
	s.single++
	return onchain.Counter{Value: s.value, Found: true}, nil
}

func (s *onChainBoth) FetchPools(
	_ context.Context,
	_ onchain.Query,
	pools []common.Blake2b224,
) (map[common.Blake2b224]onchain.Counter, error) {
	s.lists++
	out := make(map[common.Blake2b224]onchain.Counter, len(pools))
	for _, p := range pools {
		out[p] = onchain.Counter{Value: s.value, Found: true}
	}
	return out, nil
}

// A node that moves between poolId and a one-pool list names the same pool
// either way. What the other mode remembered is not an answer for this one.
func TestRefreshOnChainCounterAcrossPoolListSwitch(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a")
	single := func() *dingov1alpha1.DingoNode {
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		dn.Spec.BlockProducer.NodeToClient.Enabled = true
		return dn
	}
	list := func() *dingov1alpha1.DingoNode {
		dn := poolListNode("bp", "ns", 0, pools...)
		dn.Spec.BlockProducer.NodeToClient.Enabled = true
		return dn
	}
	observed := func(t *testing.T, dn *dingov1alpha1.DingoNode) {
		t.Helper()
		cond := meta.FindStatusCondition(
			dn.Status.Conditions, condOnChainCounter,
		)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
		assert.Equal(t, "Observed", cond.Reason)
	}

	t.Run("poolId to pool list", func(t *testing.T) {
		t.Parallel()
		stub := &onChainBoth{value: 7}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), single())
		require.Equal(t, 1, stub.single)

		dn := list()
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 1, stub.lists, "the list is read, not answered "+
			"from the single-pool attempt")
		observed(t, dn)
		assert.Equal(t, int64(7), poolStatus(dn, pools[0].id).OnChainCounter)
	})
	t.Run("pool list to poolId", func(t *testing.T) {
		t.Parallel()
		stub := &onChainBoth{value: 9}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), list())
		require.Equal(t, 1, stub.lists)

		dn := single()
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 1, stub.single, "poolId is read, not answered "+
			"from the pool list attempt")
		observed(t, dn)
		assert.Equal(t, int64(9), dn.Status.OpCert.OnChainCounter)
	})
}
