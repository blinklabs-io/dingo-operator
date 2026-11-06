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

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const conflictPoolID = "" +
	"00112233445566778899aabbccddeeff" +
	"00112233445566778899aabb"

func forgerNode(
	name, ns, poolID, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			CreationTimestamp: metav1.NewTime(
				time.Unix(1_700_000_000, 0).Add(-age),
			),
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "preview",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				PoolID: poolID,
				Keys:   dingov1alpha1.KeysSpec{SecretRef: secret},
			},
		},
	}
}

func TestReconcileRejectsSecondForgerForPool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		first    *dingov1alpha1.DingoNode
		second   *dingov1alpha1.DingoNode
		conflict bool
	}{
		{
			name:     "same pool in another namespace",
			first:    forgerNode("bp", "ns-a", conflictPoolID, "keys-a", time.Hour),
			second:   forgerNode("bp", "ns-b", conflictPoolID, "keys-b", 0),
			conflict: true,
		},
		{
			name:  "same pool in different hex case",
			first: forgerNode("bp-1", "ns-a", conflictPoolID, "keys-a", time.Hour),
			second: forgerNode(
				"bp-2", "ns-a", "00112233445566778899AABBCCDDEEFF00112233445566778899AABB",
				"keys-b", 0,
			),
			conflict: true,
		},
		{
			name:     "same keys secret in one namespace without pool id",
			first:    forgerNode("bp-1", "ns-a", "", "keys", time.Hour),
			second:   forgerNode("bp-2", "ns-a", "", "keys", 0),
			conflict: true,
		},
		{
			name:     "same secret name in another namespace is a different secret",
			first:    forgerNode("bp-1", "ns-a", "", "keys", time.Hour),
			second:   forgerNode("bp-2", "ns-b", "", "keys", 0),
			conflict: false,
		},
		{
			name:  "different pools",
			first: forgerNode("bp-1", "ns-a", conflictPoolID, "keys-a", time.Hour),
			second: forgerNode(
				"bp-2", "ns-a", "ff112233445566778899aabbccddeeff00112233445566778899aabb",
				"keys-b", 0,
			),
			conflict: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, dingov1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.first, tt.second).
				WithStatusSubresource(&dingov1alpha1.DingoNode{}).
				Build()
			r := reconcilerFor(c)

			// The newer node must be refused; Reconcile stops at validation,
			// so no workload is created for it.
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: tt.second.Name, Namespace: tt.second.Namespace,
				},
			})
			require.NoError(t, err)
			var got dingov1alpha1.DingoNode
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tt.second), &got))
			degraded := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
			var sts appsv1.StatefulSet
			stsErr := c.Get(ctx, client.ObjectKeyFromObject(tt.second), &sts)
			if tt.conflict {
				require.NotNil(t, degraded, "newer forger must be Degraded")
				assert.Equal(t, "DuplicateForger", degraded.Reason)
				assert.True(t, apierrors.IsNotFound(stsErr), "no StatefulSet for the loser")
			} else if degraded != nil {
				assert.NotEqual(t, "DuplicateForger", degraded.Reason)
			}

			// The older node keeps forging: it is never flagged.
			_, err = r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: tt.first.Name, Namespace: tt.first.Namespace,
				},
			})
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tt.first), &got))
			d := meta.FindStatusCondition(
				got.Status.Conditions, condDegraded,
			)
			if d != nil {
				assert.NotEqual(t, "DuplicateForger", d.Reason)
			}
		})
	}
}

func TestForgerConflict(t *testing.T) {
	t.Parallel()
	a := forgerNode("a", "ns", conflictPoolID, "k1", 0)
	b := forgerNode("b", "ns", conflictPoolID, "k2", 0)
	all := []dingov1alpha1.DingoNode{*a, *b}

	// Same creation instant: exactly one of the pair is refused.
	assert.NoError(t, forgerConflict(a, all))
	assert.Error(t, forgerConflict(b, all))

	// A peer with no blockProducer block, or one being deleted, holds no keys.
	bare := forgerNode("0", "ns", conflictPoolID, "k1", time.Hour)
	bare.Spec.BlockProducer = nil
	assert.NoError(t, forgerConflict(b, []dingov1alpha1.DingoNode{*bare}))
	gone := forgerNode("0", "ns", conflictPoolID, "k1", time.Hour)
	gone.DeletionTimestamp = &metav1.Time{Time: time.Unix(1_700_000_000, 0)}
	assert.NoError(t, forgerConflict(b, []dingov1alpha1.DingoNode{*gone}))
}

func conflictClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&dingov1alpha1.DingoNode{}).
		Build()
}

func reconcileNode(
	t *testing.T, r *DingoNodeReconciler, n *dingov1alpha1.DingoNode,
) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(n),
	})
	require.NoError(t, err)
	return res
}

// A node that already forges must stop when it turns out to share keys with an
// older one, and resume when the older node goes away.
func TestReconcileScalesRunningDuplicateForgerDown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	holder := forgerNode("bp-1", "ns", conflictPoolID, "keys-a", time.Hour)
	loser := forgerNode(
		"bp-2", "ns",
		"ff112233445566778899aabbccddeeff00112233445566778899aabb",
		"keys-b", 0,
	)
	c := conflictClient(t, holder, loser)
	r := reconcilerFor(c)

	reconcileNode(t, r, loser)
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(loser), &sts))
	require.NotNil(t, sts.Spec.Replicas)
	require.Equal(t, int32(1), *sts.Spec.Replicas)

	var live dingov1alpha1.DingoNode
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(loser), &live))
	live.Spec.BlockProducer.PoolID = conflictPoolID
	require.NoError(t, c.Update(ctx, &live))

	res := reconcileNode(t, r, &live)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(loser), &sts))
	require.NotNil(t, sts.Spec.Replicas)
	assert.Equal(t, int32(0), *sts.Spec.Replicas,
		"a running duplicate forger must be scaled to zero")
	assert.Equal(t, requeueInterval, res.RequeueAfter,
		"a refused node must be re-evaluated, nothing watches its holder")

	// The holder goes away: the next reconcile restores the forger.
	require.NoError(t, c.Delete(ctx, holder))
	reconcileNode(t, r, &live)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(loser), &sts))
	require.NotNil(t, sts.Spec.Replicas)
	assert.Equal(t, int32(1), *sts.Spec.Replicas)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(loser), &live))
	d := meta.FindStatusCondition(live.Status.Conditions, condDegraded)
	if d != nil {
		assert.NotEqual(t, "DuplicateForger", d.Reason)
	}
}

func TestReconcileRefusedForgerRequeues(t *testing.T) {
	t.Parallel()
	holder := forgerNode("bp-1", "ns", "", "keys", time.Hour)
	loser := forgerNode("bp-2", "ns", "", "keys", 0)
	r := reconcilerFor(conflictClient(t, holder, loser))
	res := reconcileNode(t, r, loser)
	assert.Equal(t, requeueInterval, res.RequeueAfter)
}

func TestNormalizedPoolIDBech32EqualsHex(t *testing.T) {
	t.Parallel()
	pool, err := parsePoolID(conflictPoolID)
	require.NoError(t, err)
	bech := pool.Bech32("pool")
	require.NotEqual(t, conflictPoolID, bech)
	assert.Equal(t, normalizedPoolID(conflictPoolID), normalizedPoolID(bech))
}
