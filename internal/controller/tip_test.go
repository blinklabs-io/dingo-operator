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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
)

const tipThreshold = 10 * time.Minute

type tipClock struct{ now time.Time }

func (c *tipClock) Now() time.Time { return c.now }

func tipStatus(slot int64) *forgestatus.Status {
	return &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		TipSlot: slot, HasTipSlot: true,
	}
}

func newTipReconciler(
	st *forgestatus.Status,
	clock *tipClock,
) (*DingoNodeReconciler, *events.FakeRecorder) {
	rec := events.NewFakeRecorder(10)
	return &DingoNodeReconciler{
		ForgeStatus:       &statusFetcher{st: st},
		TipStallThreshold: tipThreshold,
		Now:               clock.Now,
		Recorder:          rec,
	}, rec
}

func pipelineCond(t *testing.T, dn *dingov1alpha1.DingoNode) *metav1.Condition {
	t.Helper()
	cond := meta.FindStatusCondition(dn.Status.Conditions, condPipelineHealthy)
	require.NotNil(t, cond)
	return cond
}

func TestTipNotAdvancingBeyondThreshold(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, rec := newTipReconciler(tipStatus(1000), clock)

	r.refreshForgeStatus(context.Background(), dn)
	assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)

	clock.now = clock.now.Add(tipThreshold + time.Minute)
	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "TipNotAdvancing", cond.Reason)
	assert.Contains(t, cond.Message, "1000")
	assert.Contains(t, cond.Message, "2026-10-30T12:00:00Z")
	assert.True(t, meta.IsStatusConditionTrue(dn.Status.Conditions, condDegraded))
	assert.Equal(t, int64(1000), dn.Status.Tip.Slot)

	require.Len(t, rec.Events, 1)
	assert.Contains(t, <-rec.Events, "TipNotAdvancing")

	// Still frozen on the next scrape: no second event.
	clock.now = clock.now.Add(2 * time.Minute)
	r.refreshForgeStatus(context.Background(), dn)
	assert.Empty(t, rec.Events)
}

func TestTipFrozenBelowThresholdStaysHealthy(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, _ := newTipReconciler(tipStatus(1000), clock)

	r.refreshForgeStatus(context.Background(), dn)
	// Exactly at the threshold is still healthy; only beyond it is a stall.
	clock.now = clock.now.Add(tipThreshold)
	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Following", cond.Reason)
}

func TestTipAdvancingStaysHealthyAndRecovers(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	fetcher := &statusFetcher{st: tipStatus(1000)}
	r, _ := newTipReconciler(nil, clock)
	r.ForgeStatus = fetcher

	r.refreshForgeStatus(context.Background(), dn)
	for i := range 5 {
		clock.now = clock.now.Add(5 * time.Minute)
		fetcher.st = tipStatus(int64(2000 + i))
		r.refreshForgeStatus(context.Background(), dn)
		assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)
	}

	fetcher.st = tipStatus(2004)
	clock.now = clock.now.Add(tipThreshold + time.Minute)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, metav1.ConditionFalse, pipelineCond(t, dn).Status)

	fetcher.st = tipStatus(2500)
	clock.now = clock.now.Add(2 * time.Minute)
	r.refreshForgeStatus(context.Background(), dn)
	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Following", cond.Reason)
}

func TestTipStallDoesNotOutrankHaltedOrStuck(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mut    func(*forgestatus.Status)
		reason string
	}{
		"halted": {func(s *forgestatus.Status) { s.PipelineHalted = true }, "LedgerPipelineHalted"},
		"stuck":  {func(s *forgestatus.Status) { s.PipelineStuck = true }, "LedgerPipelineStuck"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			dn := relayNodeForStatus("relay", "ns")
			st := tipStatus(1000)
			tc.mut(st)
			r, rec := newTipReconciler(st, clock)

			r.refreshForgeStatus(context.Background(), dn)
			clock.now = clock.now.Add(time.Hour)
			r.refreshForgeStatus(context.Background(), dn)

			assert.Equal(t, tc.reason, pipelineCond(t, dn).Reason)
			assert.Empty(t, rec.Events)
		})
	}
}

func TestTipMetricsUnavailableStaysUnknown(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, _ := newTipReconciler(nil, clock)
	r.ForgeStatus = fakeForgeStatusFetcher{err: assert.AnError}

	r.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionUnknown, cond.Status)
	assert.Equal(t, "MetricsUnavailable", cond.Reason)
	assert.Nil(t, dn.Status.Tip.LastProgressAt)
}

func TestTipSlotGaugeMissingIsNotAStall(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	st := tipStatus(0)
	st.HasTipSlot = false
	r, _ := newTipReconciler(st, clock)

	r.refreshForgeStatus(context.Background(), dn)
	clock.now = clock.now.Add(time.Hour)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)
}

func TestTipDisabledWhenThresholdZero(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, _ := newTipReconciler(tipStatus(1000), clock)
	r.TipStallThreshold = 0

	r.refreshForgeStatus(context.Background(), dn)
	clock.now = clock.now.Add(time.Hour)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)
}

// A fresh operator process has no memory; the frozen tip must be judged from
// the persisted status.
func TestTipStallSurvivesOperatorRestart(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r1, _ := newTipReconciler(tipStatus(1000), clock)
	r1.refreshForgeStatus(context.Background(), dn)

	clock.now = clock.now.Add(tipThreshold - time.Minute)
	r1.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)

	// New reconciler, same persisted status; two minutes later.
	clock.now = clock.now.Add(2 * time.Minute)
	r2, _ := newTipReconciler(tipStatus(1000), clock)
	r2.refreshForgeStatus(context.Background(), dn)

	cond := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "TipNotAdvancing", cond.Reason)
}

// After a long gap with no observation (node down, metrics unreachable) the
// persisted last-progress time says nothing about the freshly started node.
func TestTipObservationGapRestartsTheClock(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, _ := newTipReconciler(tipStatus(1000), clock)
	r.refreshForgeStatus(context.Background(), dn)

	healthy := r.ForgeStatus
	r.ForgeStatus = fakeForgeStatusFetcher{err: assert.AnError}
	clock.now = clock.now.Add(3 * time.Hour)
	r.refreshForgeStatus(context.Background(), dn)
	require.Equal(t, metav1.ConditionUnknown, pipelineCond(t, dn).Status)

	r.ForgeStatus = healthy
	clock.now = clock.now.Add(time.Minute)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)
}

func TestTipAcrossReplicasUsesLowestSlot(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	dn.Spec.Replicas = new(int32(2))
	// Replica 1 advances while replica 0 stays frozen.
	var n int64
	fetcher := &seqTipFetcher{next: func(url string) *forgestatus.Status {
		if len(url) > 8 && url[7:15] == "relay-0." {
			return tipStatus(1000)
		}
		n++
		return tipStatus(5000 + n)
	}}
	r, _ := newTipReconciler(nil, clock)
	r.ForgeStatus = fetcher

	r.refreshForgeStatus(context.Background(), dn)
	clock.now = clock.now.Add(tipThreshold + time.Minute)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, "TipNotAdvancing", pipelineCond(t, dn).Reason)
	assert.Equal(t, int64(1000), dn.Status.Tip.Slot)
}

type seqTipFetcher struct {
	next func(url string) *forgestatus.Status
}

func (f *seqTipFetcher) Fetch(
	_ context.Context,
	url string,
) (*forgestatus.Status, error) {
	return f.next(url), nil
}

// Status is written on every reconcile and a DingoNode status change triggers
// another reconcile, so a pass that observes nothing new must leave status
// byte-identical, both while following and while stalled.
func TestTipStatusStableWhenSlotUnchanged(t *testing.T) {
	t.Parallel()
	for name, start := range map[string]time.Duration{
		"following": 0,
		"stalled":   tipThreshold + time.Minute,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			dn := relayNodeForStatus("relay", "ns")
			r, _ := newTipReconciler(tipStatus(1000), clock)
			r.refreshForgeStatus(context.Background(), dn)
			clock.now = clock.now.Add(start)
			r.refreshForgeStatus(context.Background(), dn)
			before := dn.Status.DeepCopy()

			clock.now = clock.now.Add(2 * time.Minute)
			r.refreshForgeStatus(context.Background(), dn)

			assert.Equal(t, before, &dn.Status)
		})
	}
}

// Every pass that does not observe the tip forgets the last progress time, so
// a node is never judged by progress recorded before a gap in observation.
func TestTipUnobservedPassRestartsTheClock(t *testing.T) {
	t.Parallel()
	for name, unobserve := range map[string]func(*DingoNodeReconciler){
		"no fetcher": func(r *DingoNodeReconciler) { r.ForgeStatus = nil },
		"gauge missing": func(r *DingoNodeReconciler) {
			st := tipStatus(1000)
			st.HasTipSlot = false
			r.ForgeStatus = &statusFetcher{st: st}
		},
		"check disabled": func(r *DingoNodeReconciler) { r.TipStallThreshold = 0 },
		"slot zero": func(r *DingoNodeReconciler) {
			r.ForgeStatus = &statusFetcher{st: tipStatus(0)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			dn := relayNodeForStatus("relay", "ns")
			r, _ := newTipReconciler(tipStatus(1000), clock)
			r.refreshForgeStatus(context.Background(), dn)

			u, _ := newTipReconciler(tipStatus(1000), clock)
			unobserve(u)
			clock.now = clock.now.Add(time.Hour)
			u.refreshForgeStatus(context.Background(), dn)
			assert.Nil(t, dn.Status.Tip.LastProgressAt)

			clock.now = clock.now.Add(time.Minute)
			r.refreshForgeStatus(context.Background(), dn)
			assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
		})
	}
}

// Dingo serves metrics before it loads its tip, so the gauge reads 0 while a
// restarted node opens its database. Slot 0 is not an observation.
func TestTipSlotZeroIsNotAStall(t *testing.T) {
	t.Parallel()
	clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dn := relayNodeForStatus("relay", "ns")
	r, _ := newTipReconciler(tipStatus(0), clock)

	r.refreshForgeStatus(context.Background(), dn)
	clock.now = clock.now.Add(time.Hour)
	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, "Following", pipelineCond(t, dn).Reason)
}

// A tip stall is inferred over an observation window. Once the window is
// broken (scrape failure, no fetcher, gauges missing) the clock restarts, so the
// stall must not outlive it as a False condition. Gauge-confirmed wedges are
// kept; see TestTipStallDoesNotOutrankHaltedOrStuck.
func TestTipStallBecomesUnknownWhenUnobserved(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		unobserve func(*DingoNodeReconciler)
		reason    string
	}{
		"scrape failure": {func(r *DingoNodeReconciler) {
			r.ForgeStatus = fakeForgeStatusFetcher{err: assert.AnError}
		}, "MetricsUnavailable"},
		"no fetcher": {func(r *DingoNodeReconciler) {
			r.ForgeStatus = nil
		}, "MetricsUnavailable"},
		"pipeline gauges missing": {func(r *DingoNodeReconciler) {
			r.ForgeStatus = &statusFetcher{st: &forgestatus.Status{
				TipSlot: 1000, HasTipSlot: true,
			}}
		}, "PipelineMetricsUnavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := &tipClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			dn := relayNodeForStatus("relay", "ns")
			r, _ := newTipReconciler(tipStatus(1000), clock)
			r.refreshForgeStatus(context.Background(), dn)
			clock.now = clock.now.Add(tipThreshold + time.Minute)
			r.refreshForgeStatus(context.Background(), dn)
			require.Equal(t, reasonTipNotAdvancing, pipelineCond(t, dn).Reason)

			tc.unobserve(r)
			clock.now = clock.now.Add(2 * time.Minute)
			r.refreshForgeStatus(context.Background(), dn)

			cond := pipelineCond(t, dn)
			assert.Equal(t, metav1.ConditionUnknown, cond.Status)
			assert.Equal(t, tc.reason, cond.Reason)
		})
	}
}
