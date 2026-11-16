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

	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runningPoolsStatefulSet is the live StatefulSet of dn, whose template
// stages the given pools.
func runningPoolsStatefulSet(name, ns string, pools ...string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					resources.KeysPoolAnnotation: resources.JoinKeysPools(pools),
				}},
			},
		},
	}
}

// When every bundle is refused the template is held and its pod keeps
// forging with the keys it loaded, so the pools it runs are still observed:
// a pool near expiry makes rotation due.
func TestRefreshForgeStatusObservesPoolsOfAHeldTemplate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
	// No keys Secret exists, so every bundle is refused.
	c := conflictClient(t, dn,
		runningPoolsStatefulSet("bp", "ns", pools[0].id, pools[1].id),
	)
	r := poolReconciler(c)
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &opts)
	require.NoError(t, err)
	require.True(t, keep, "every bundle refused holds the template")
	for _, p := range pools {
		require.False(t, poolStatus(dn, p.id).KeysValid)
	}

	r.ForgeStatus = poolMetricsFetcher{st: &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		Pools: map[string]*forgestatus.PoolMetrics{
			poolLabelOf(pools[0].id): {
				CurrentKESPeriod: 900, RemainingKESPeriods: 5,
				OpCertExpiryKESPeriod: 905, HasKESData: true,
				OpCertCounter: 3, HasOpCertCounter: true,
			},
			poolLabelOf(pools[1].id): {
				CurrentKESPeriod: 900, RemainingKESPeriods: 40,
				OpCertExpiryKESPeriod: 940, HasKESData: true,
			},
		},
	}}
	r.refreshForgeStatus(ctx, dn)

	a := poolStatus(dn, pools[0].id)
	assert.Equal(t, int64(5), a.KES.RemainingPeriods)
	require.NotNil(t, a.LoadedCounter)
	assert.Equal(t, int64(3), *a.LoadedCounter)
	assert.Equal(t, int64(40), poolStatus(dn, pools[1].id).KES.RemainingPeriods)
	assert.Equal(t, int64(5), dn.Status.KES.RemainingPeriods)
	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionTrue, due.Status)
	assert.Contains(t, due.Message, "pool "+pools[0].id)
}

// A held template running one pool exposes its series unlabelled; they are
// that pool's even though its bundle is refused.
func TestRefreshForgeStatusObservesTheOnePoolOfAHeldTemplate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
	c := conflictClient(t, dn,
		runningPoolsStatefulSet("bp", "ns", pools[0].id),
	)
	r := poolReconciler(c)
	r.ForgeStatus = poolMetricsFetcher{st: &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		HasKESData: true, CurrentKESPeriod: 900, RemainingKESPeriods: 3,
	}}
	r.refreshForgeStatus(ctx, dn)

	assert.Equal(t, int64(3), poolStatus(dn, pools[0].id).KES.RemainingPeriods)
	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionTrue, due.Status)
}

// A pool the running template does not stage is not forging, whatever its
// bundle's verdict: its KES and loaded counter are cleared, and it does not
// hold RotationDue at Unknown.
func TestRefreshForgeStatusClearsPoolsOutOfTheWorkload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
	// b is accepted but not yet in the running template; c was refused and
	// left out of it.
	poolStatus(dn, pools[0].id).KeysValid = true
	for _, p := range pools[1:] {
		ps := poolStatus(dn, p.id)
		ps.KeysValid = p.id == pools[1].id
		ps.KES.RemainingPeriods = 2
		ps.LoadedCounter = new(int64(4))
	}
	c := conflictClient(t, dn,
		runningPoolsStatefulSet("bp", "ns", pools[0].id),
	)
	r := poolReconciler(c)
	r.ForgeStatus = poolMetricsFetcher{st: &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		HasKESData: true, CurrentKESPeriod: 900, RemainingKESPeriods: 30,
	}}
	r.refreshForgeStatus(ctx, dn)

	assert.Equal(t, int64(30), poolStatus(dn, pools[0].id).KES.RemainingPeriods)
	for _, p := range pools[1:] {
		ps := poolStatus(dn, p.id)
		assert.Zero(t, ps.KES, p.id)
		assert.Nil(t, ps.LoadedCounter, p.id)
	}
	assert.Equal(t, int64(30), dn.Status.KES.RemainingPeriods)
	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionFalse, due.Status)
}
