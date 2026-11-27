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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
)

func divergenceStatus(gap, hash, ancestor float64) *forgestatus.Status {
	st := tipStatus(1000)
	st.HasChainDivergence = true
	st.ChainDivergence = map[string]float64{
		forgestatus.ReasonPeerHeightGap:                gap,
		forgestatus.ReasonPrimaryTipHashDiverged:       hash,
		forgestatus.ReasonAppliedTipNotPrimaryAncestor: ancestor,
	}
	return st
}

// advancingTip moves the tip so the stall check stays out of the way.
func advancingTip(st *forgestatus.Status, slot int64) *forgestatus.Status {
	st.TipSlot = slot
	return st
}

func newDivergenceFixture(
	st *forgestatus.Status,
) (*DingoNodeReconciler, *statusFetcher, *tipClock, *dingov1alpha1.DingoNode) {
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	fetcher := &statusFetcher{st: st}
	r, _ := newTipReconciler(nil, clock)
	r.ForgeStatus = fetcher
	return r, fetcher, clock, relayNodeForStatus("bp", "ns")
}

func TestChainDivergedFirstObservationOnlySetsBaseline(t *testing.T) {
	t.Parallel()
	r, _, _, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))

	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Following", cond.Reason)
}

func TestChainDivergedOnCounterIncrease(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 1))
	r.refreshForgeStatus(context.Background(), dn)

	clock.now = clock.now.Add(time.Minute)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 1), 1001)
	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ChainDiverged", cond.Reason)
	assert.Contains(t, cond.Message, forgestatus.ReasonPeerHeightGap)
	assert.Contains(t, cond.Message, "refused to forge")
	assert.NotContains(
		t,
		cond.Message,
		forgestatus.ReasonAppliedTipNotPrimaryAncestor,
	)
	assert.NotContains(t, cond.Message, "2026")
	assert.True(
		t,
		meta.IsStatusConditionTrue(dn.Status.Conditions, condDegraded),
	)
}

func TestChainDivergedNamesEveryIncreasedReason(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dn := newDivergenceFixture(divergenceStatus(0, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)

	fetcher.st = advancingTip(divergenceStatus(0, 1, 3), 1001)
	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	require.Equal(t, "ChainDiverged", cond.Reason)
	assert.Contains(t, cond.Message, forgestatus.ReasonPrimaryTipHashDiverged)
	assert.Contains(
		t,
		cond.Message,
		forgestatus.ReasonAppliedTipNotPrimaryAncestor,
	)
	assert.NotContains(t, cond.Message, forgestatus.ReasonPeerHeightGap)
}

// Status is written every reconcile and each write triggers another one, so
// the report must not change while the counters do not.
func TestChainDivergedMessageIsStableWhileFlat(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	first := *pipelineCond(t, dn)
	require.Equal(t, "ChainDiverged", first.Reason)

	for i := range 3 {
		clock.now = clock.now.Add(10 * time.Minute)
		fetcher.st = advancingTip(divergenceStatus(6, 0, 0), int64(1002+i))
		r.refreshForgeStatus(context.Background(), dn)
		assert.Equal(t, first.Reason, pipelineCond(t, dn).Reason)
		assert.Equal(t, first.Message, pipelineCond(t, dn).Message)
	}
}

func TestChainDivergedClearsAfterHoldWithoutIncrease(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	// Exactly at the hold is still reported; beyond it the report clears.
	clock.now = clock.now.Add(defaultChainDivergedHold)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	clock.now = clock.now.Add(time.Second)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1003)
	r.refreshForgeStatus(context.Background(), dn)
	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Following", cond.Reason)
}

func TestChainDivergedIncreaseExtendsTheHold(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)

	clock.now = clock.now.Add(defaultChainDivergedHold - time.Minute)
	fetcher.st = advancingTip(divergenceStatus(7, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)

	clock.now = clock.now.Add(defaultChainDivergedHold - time.Minute)
	fetcher.st = advancingTip(divergenceStatus(7, 0, 0), 1003)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

func TestChainDivergedCounterDecreaseRebaselines(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dn := newDivergenceFixture(divergenceStatus(50, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)

	// The pod restarted: the counter starts again from zero.
	fetcher.st = advancingTip(divergenceStatus(1, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)

	fetcher.st = advancingTip(divergenceStatus(1, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)

	fetcher.st = advancingTip(divergenceStatus(2, 0, 0), 1003)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

// A fresh operator process only learns the baseline; refusals counted before
// it started are not reported.
func TestChainDivergedOperatorRestartRebaselines(t *testing.T) {
	t.Parallel()
	r1, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r1.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r1.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	r2, _ := newTipReconciler(nil, clock)
	r2.ForgeStatus = &statusFetcher{
		st: advancingTip(divergenceStatus(6, 0, 0), 1002),
	}
	r2.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
}

func TestChainDivergedIgnoresOtherStaleTipSkipReasons(t *testing.T) {
	t.Parallel()
	parse := func(gap, other int) *forgestatus.Status {
		body := strings.Join([]string{
			"cardano_node_metrics_slotNum_int " + strconv.Itoa(1000+gap+other),
			"dingo_ledger_pipeline_stuck 0",
			"dingo_ledger_pipeline_halted 0",
			`dingo_forge_stale_tip_skip_total{reason="peer_height_gap"} ` + strconv.Itoa(
				gap,
			),
			`dingo_forge_stale_tip_skip_total{reason="primary_tip_hash_diverged"} 0`,
			`dingo_forge_stale_tip_skip_total{reason="applied_tip_not_primary_ancestor"} 0`,
			`dingo_forge_stale_tip_skip_total{reason="unapplied_block_gap"} ` + strconv.Itoa(
				other,
			),
			`dingo_forge_stale_tip_skip_total{reason="applied_tip_stale"} ` + strconv.Itoa(
				other,
			),
			`dingo_forge_stale_tip_skip_total{reason="primary_tip_behind_applied"} ` + strconv.Itoa(
				other,
			),
		}, "\n") + "\n"
		st, err := forgestatus.Parse(strings.NewReader(body))
		require.NoError(t, err)
		return st
	}
	r, fetcher, _, dn := newDivergenceFixture(parse(0, 0))
	r.refreshForgeStatus(context.Background(), dn)

	fetcher.st = parse(0, 9)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)

	fetcher.st = parse(1, 9)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

func TestChainDivergedAbsentSeriesChangesNothing(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(tipStatus(1000))
	r.refreshForgeStatus(context.Background(), dn)
	for i := range 3 {
		clock.now = clock.now.Add(time.Minute)
		fetcher.st = tipStatus(int64(1001 + i))
		r.refreshForgeStatus(context.Background(), dn)
		cond := pipelineCond(t, dn)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, "Following", cond.Reason)
	}
	assert.False(
		t,
		meta.IsStatusConditionTrue(dn.Status.Conditions, condDegraded),
	)
}

// An image downgrade that drops the series must not keep reporting a diverged
// chain from state read while it was present.
func TestChainDivergedSeriesDisappearingClearsTheReport(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	fetcher.st = tipStatus(1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
}

func TestChainDivergedRanking(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mut    func(*forgestatus.Status)
		reason string
	}{
		"halted": {
			func(s *forgestatus.Status) { s.PipelineHalted = true },
			"LedgerPipelineHalted",
		},
		"stuck": {
			func(s *forgestatus.Status) { s.PipelineStuck = true },
			"LedgerPipelineStuck",
		},
		"diverged": {func(*forgestatus.Status) {}, "ChainDiverged"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, fetcher, clock, dn := newDivergenceFixture(
				divergenceStatus(5, 0, 0),
			)
			r.refreshForgeStatus(context.Background(), dn)
			// The tip also stays frozen beyond the stall threshold.
			clock.now = clock.now.Add(tipThreshold + time.Minute)
			st := divergenceStatus(6, 0, 0)
			tc.mut(st)
			fetcher.st = st
			r.refreshForgeStatus(context.Background(), dn)

			assert.Equal(t, tc.reason, pipelineCond(t, dn).Reason)
		})
	}
}

func TestChainDivergedOutranksTipStall(t *testing.T) {
	t.Parallel()
	r, fetcher, clock, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	clock.now = clock.now.Add(tipThreshold + time.Minute)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "TipNotAdvancing", pipelineCond(t, dn).Reason)

	fetcher.st = divergenceStatus(6, 0, 0)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

func TestChainDivergedKeepsPipelineUnknownWithoutGauges(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)

	st := advancingTip(divergenceStatus(6, 0, 0), 1001)
	st.HasPipelineStuckMetric = false
	fetcher.st = st
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, metav1.ConditionUnknown, pipelineCond(t, dn).Status)
}

func TestChainDivergedEventFiresOnce(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	fetcher := &statusFetcher{st: divergenceStatus(5, 0, 0)}
	r, rec := newTipReconciler(nil, clock)
	r.ForgeStatus = fetcher
	dn := relayNodeForStatus("bp", "ns")
	r.refreshForgeStatus(context.Background(), dn)
	require.Empty(t, rec.Events)

	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Len(t, rec.Events, 1)
	assert.Contains(t, <-rec.Events, "ChainDiverged")

	fetcher.st = advancingTip(divergenceStatus(7, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Empty(t, rec.Events)
}

func TestChainDivergedBaselinesAreKeptPerNode(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dnA := newDivergenceFixture(divergenceStatus(5, 0, 0))
	dnB := relayNodeForStatus("other", "ns")
	r.refreshForgeStatus(context.Background(), dnA)

	// The other node's first observation reads 9; it is not judged against
	// the first node's 5.
	fetcher.st = advancingTip(divergenceStatus(9, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dnB)
	assert.Equal(t, "Following", pipelineCond(t, dnB).Reason)
}

// divergencePodFetcher answers each pod's scrape from its own status; a pod without an
// entry fails the scrape.
type divergencePodFetcher struct {
	pods map[string]*forgestatus.Status
}

func (f *divergencePodFetcher) Fetch(
	_ context.Context,
	url string,
) (*forgestatus.Status, error) {
	for pod, st := range f.pods {
		if strings.Contains(url, "//"+pod+".") {
			return st, nil
		}
	}
	return nil, assert.AnError
}

func newDivergencePodFixture(
	pods map[string]*forgestatus.Status,
) (*DingoNodeReconciler, *divergencePodFetcher, *dingov1alpha1.DingoNode) {
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	fetcher := &divergencePodFetcher{pods: pods}
	r, _ := newTipReconciler(nil, clock)
	r.ForgeStatus = fetcher
	dn := relayNodeForStatus("bp", "ns")
	dn.Spec.Replicas = new(int32(len(pods)))
	return r, fetcher, dn
}

// One pod restarting must not hide a rise on another in the same pass.
func TestChainDivergedPodRestartDoesNotMaskAnotherPodsRise(t *testing.T) {
	t.Parallel()
	r, fetcher, dn := newDivergencePodFixture(map[string]*forgestatus.Status{
		"bp-0": divergenceStatus(5, 0, 0),
		"bp-1": divergenceStatus(5, 0, 0),
	})
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "Following", pipelineCond(t, dn).Reason)

	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(0, 0, 0), 1001)
	fetcher.pods["bp-1"] = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

func TestChainDivergedNewPodOnlySetsItsBaseline(t *testing.T) {
	t.Parallel()
	r, fetcher, dn := newDivergencePodFixture(map[string]*forgestatus.Status{
		"bp-0": divergenceStatus(5, 0, 0),
	})
	r.refreshForgeStatus(context.Background(), dn)

	dn.Spec.Replicas = new(int32(2))
	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(5, 0, 0), 1001)
	fetcher.pods["bp-1"] = advancingTip(divergenceStatus(9, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)

	fetcher.pods["bp-1"] = advancingTip(divergenceStatus(10, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

func TestChainDivergedRemovedPodBaselineIsForgotten(t *testing.T) {
	t.Parallel()
	r, fetcher, dn := newDivergencePodFixture(map[string]*forgestatus.Status{
		"bp-0": divergenceStatus(5, 0, 0),
		"bp-1": divergenceStatus(5, 0, 0),
	})
	r.refreshForgeStatus(context.Background(), dn)

	dn.Spec.Replicas = new(int32(1))
	delete(fetcher.pods, "bp-1")
	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(5, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "Following", pipelineCond(t, dn).Reason)

	// A pod of the same name returns with a counter above the old baseline.
	dn.Spec.Replicas = new(int32(2))
	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(5, 0, 0), 1002)
	fetcher.pods["bp-1"] = advancingTip(divergenceStatus(9, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
}

func TestChainDivergedPodWithoutSeriesDoesNotHideAnother(t *testing.T) {
	t.Parallel()
	r, fetcher, dn := newDivergencePodFixture(map[string]*forgestatus.Status{
		"bp-0": divergenceStatus(5, 0, 0),
		"bp-1": tipStatus(1000),
	})
	r.refreshForgeStatus(context.Background(), dn)

	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(6, 0, 0), 1001)
	fetcher.pods["bp-1"] = tipStatus(1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	// The pod without the series must not clear the other pod's report.
	fetcher.pods["bp-0"] = advancingTip(divergenceStatus(6, 0, 0), 1002)
	fetcher.pods["bp-1"] = tipStatus(1002)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
}

// seedHealthyPipeline gives the node the conditions a reconcile that found it
// healthy leaves behind. Degraded is present too, so setting it later does not
// grow the slice: the condition pointer read before the update is then the
// element the update writes.
func seedHealthyPipeline(dn *dingov1alpha1.DingoNode) {
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condPipelineHealthy, Status: metav1.ConditionTrue,
		Reason: "Following", Message: "ledger pipeline is following the chain",
	})
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condDegraded, Status: metav1.ConditionFalse,
		Reason: "Healthy", Message: "healthy",
	})
}

func TestPipelineEventFiresOnEntryFromExistingCondition(t *testing.T) {
	t.Parallel()
	t.Run("ChainDiverged", func(t *testing.T) {
		t.Parallel()
		clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
		fetcher := &statusFetcher{st: divergenceStatus(5, 0, 0)}
		r, rec := newTipReconciler(nil, clock)
		r.ForgeStatus = fetcher
		dn := relayNodeForStatus("bp", "ns")
		r.refreshForgeStatus(context.Background(), dn)
		seedHealthyPipeline(dn)

		fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
		r.refreshForgeStatus(context.Background(), dn)
		require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)
		require.Len(t, rec.Events, 1)
		assert.Contains(t, <-rec.Events, "ChainDiverged")

		fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1002)
		r.refreshForgeStatus(context.Background(), dn)
		assert.Empty(t, rec.Events)
	})
	t.Run("TipNotAdvancing", func(t *testing.T) {
		t.Parallel()
		clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
		r, rec := newTipReconciler(tipStatus(1000), clock)
		dn := relayNodeForStatus("relay", "ns")
		r.refreshForgeStatus(context.Background(), dn)
		seedHealthyPipeline(dn)

		clock.now = clock.now.Add(tipThreshold + time.Minute)
		r.refreshForgeStatus(context.Background(), dn)
		require.Equal(t, "TipNotAdvancing", pipelineCond(t, dn).Reason)
		require.Len(t, rec.Events, 1)
		assert.Contains(t, <-rec.Events, "TipNotAdvancing")

		clock.now = clock.now.Add(time.Minute)
		r.refreshForgeStatus(context.Background(), dn)
		assert.Empty(t, rec.Events)
	})
}

func TestChainDivergedForgottenWhenScaledToZero(t *testing.T) {
	t.Parallel()
	r, fetcher, _, dn := newDivergenceFixture(divergenceStatus(5, 0, 0))
	r.refreshForgeStatus(context.Background(), dn)
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1001)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "ChainDiverged", pipelineCond(t, dn).Reason)

	dn.Spec.Replicas = new(int32(0))
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, "NoReplicas", pipelineCond(t, dn).Reason)

	// Well inside the hold, with the counter flat on the new replica.
	dn.Spec.Replicas = new(int32(1))
	fetcher.st = advancingTip(divergenceStatus(6, 0, 0), 1002)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
}
