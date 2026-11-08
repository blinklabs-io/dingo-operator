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
	"crypto/rand"
	"encoding/hex"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestStatefulSetReady checks the rule that decides Ready. A StatefulSet keeps
// reporting its old ReadyReplicas while it replaces pods for a new spec, so a
// count of ready pods alone says "ready" in the middle of a rollout. The rule
// must also see the controller catch up to the latest generation and the
// current revision reach the update revision.
func TestStatefulSetReady(t *testing.T) {
	tests := []struct {
		name       string
		generation int64
		status     appsv1.StatefulSetStatus
		desired    int32
		want       bool
	}{
		{
			name:       "rolled out",
			generation: 2,
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 1,
				CurrentRevision: "r1", UpdateRevision: "r1",
			},
			desired: 1,
			want:    true,
		},
		{
			name:       "revisions not yet reported",
			generation: 1,
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 1, ReadyReplicas: 1,
			},
			desired: 1,
			want:    true,
		},
		{
			name:       "rollout in progress",
			generation: 2,
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 1,
				CurrentRevision: "r1", UpdateRevision: "r2",
			},
			desired: 1,
		},
		{
			name:       "new generation not observed",
			generation: 3,
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 1,
				CurrentRevision: "r1", UpdateRevision: "r1",
			},
			desired: 1,
		},
		{
			name:       "fewer ready than desired",
			generation: 1,
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 1, ReadyReplicas: 1,
				CurrentRevision: "r1", UpdateRevision: "r1",
			},
			desired: 2,
		},
		{
			name:       "no ready replicas",
			generation: 1,
			status:     appsv1.StatefulSetStatus{ObservedGeneration: 1},
			desired:    0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Generation: tt.generation},
				Status:     tt.status,
			}
			assert.Equal(t, tt.want, statefulSetReady(sts, tt.desired))
		})
	}
}

// markStatefulSetReady stands in for the StatefulSet controller, which envtest
// does not run.
func markStatefulSetReady(
	t *testing.T,
	c client.Client,
	key types.NamespacedName,
	currentRevision, updateRevision string,
) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(t.Context(), key, sts))
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.Replicas = 1
	sts.Status.ReadyReplicas = 1
	sts.Status.CurrentRevision = currentRevision
	sts.Status.UpdateRevision = updateRevision
	require.NoError(t, c.Status().Update(t.Context(), sts))
}

// TestReconcileStatusReportsRolloutAndKeyRefusal walks one block producer
// through the phases a user sees:
//
//  1. No pod ready: Pending.
//  2. One pod ready, but the StatefulSet is still moving to a new revision:
//     not Ready, reason RolloutInProgress, still Pending.
//  3. Rollout finished: Ready and Forging.
//  4. The chain reports a higher opcert counter than the delivered keys, so
//     the bundle is refused. The pod still runs on its old keys, so Ready
//     stays True, but the phase must be Degraded, not Forging.
func TestReconcileStatusReportsRolloutAndKeyRefusal(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "phase-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-keys", Namespace: "phase-ns"},
		Data:       data,
	}))
	dn := blockProducerNode(
		"bp", "phase-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	key := types.NamespacedName{Name: "bp", Namespace: "phase-ns"}
	r := reconcilerFor(c)
	r.ForgeStatus = &statusFetcher{st: &forgestatus.Status{
		HasKESData:          true,
		CurrentKESPeriod:    10,
		RemainingKESPeriods: 50,
	}}
	get := func() *dingov1alpha1.DingoNode {
		got := &dingov1alpha1.DingoNode{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}

	reconcile(t, ctx, r, "bp", "phase-ns")
	got := get()
	assert.Equal(t, "Pending", got.Status.Phase, "no pod is ready yet")

	// ReadyReplicas matches, but the StatefulSet is still replacing the pod.
	markStatefulSetReady(t, c, key, "rev-1", "rev-2")
	reconcile(t, ctx, r, "bp", "phase-ns")
	got = get()
	assert.Equal(t, "Pending", got.Status.Phase)
	ready := meta.FindStatusCondition(got.Status.Conditions, condReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, "RolloutInProgress", ready.Reason)

	markStatefulSetReady(t, c, key, "rev-2", "rev-2")
	reconcile(t, ctx, r, "bp", "phase-ns")
	got = get()
	assert.Equal(t, "Forging", got.Status.Phase)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, condReady))

	// The chain has moved past the delivered certificate counter, so the bundle
	// is refused. The pod stays Ready on its last keys but is not Forging.
	r.OnChain = &stubOnChain{counter: onchain.Counter{Value: 5, Found: true}}
	reconcile(t, ctx, r, "bp", "phase-ns")
	got = get()
	assert.True(
		t, meta.IsStatusConditionFalse(got.Status.Conditions, condKeysValid),
	)
	assert.True(
		t, meta.IsStatusConditionTrue(got.Status.Conditions, condDegraded),
	)
	assert.True(
		t, meta.IsStatusConditionTrue(got.Status.Conditions, condReady),
		"Ready still reports StatefulSet truth",
	)
	assert.Equal(t, "Degraded", got.Status.Phase)
}

// TestReconcileStatusWedgedProducerIsNotForging covers the other way a ready
// producer stops being healthy: its ledger pipeline wedges. The pod stays
// Ready, but the node no longer follows the chain, so the phase must move from
// Forging to Degraded.
func TestReconcileStatusWedgedProducerIsNotForging(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "wedge-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-keys", Namespace: "wedge-ns"},
		Data:       data,
	}))
	require.NoError(t, c.Create(ctx, blockProducerNode(
		"bp", "wedge-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)))

	key := types.NamespacedName{Name: "bp", Namespace: "wedge-ns"}
	r := reconcilerFor(c)
	fetcher := &statusFetcher{st: &forgestatus.Status{
		HasKESData:              true,
		CurrentKESPeriod:        10,
		RemainingKESPeriods:     50,
		HasPipelineStuckMetric:  true,
		HasPipelineHaltedMetric: true,
	}}
	r.ForgeStatus = fetcher
	reconcile(t, ctx, r, "bp", "wedge-ns")
	markStatefulSetReady(t, c, key, "rev-1", "rev-1")

	reconcile(t, ctx, r, "bp", "wedge-ns")
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, "Forging", got.Status.Phase)

	fetcher.st.PipelineStuck = true
	reconcile(t, ctx, r, "bp", "wedge-ns")
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, "Degraded", got.Status.Phase)
	assert.True(
		t, meta.IsStatusConditionTrue(got.Status.Conditions, condDegraded),
	)
}
