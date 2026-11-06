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
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func replicasOf(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) int32 {
	t.Helper()
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &sts,
	))
	require.NotNil(t, sts.Spec.Replicas)
	return *sts.Spec.Replicas
}

func forging(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) bool {
	t.Helper()
	var sts appsv1.StatefulSet
	err := c.Get(context.Background(), client.ObjectKeyFromObject(n), &sts)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return sts.Spec.Replicas == nil || *sts.Spec.Replicas > 0
}

func refusedAsDuplicate(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) bool {
	t.Helper()
	var got dingov1alpha1.DingoNode
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &got,
	))
	d := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
	return d != nil && d.Reason == reasonDuplicateForger
}

func keysSecret(name, ns string, d map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       d,
	}
}

func poolKeys(t *testing.T) (string, map[string][]byte) {
	t.Helper()
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 5)
	require.NoError(t, err)
	return hex.EncodeToString(keys.PoolID), data
}

// holderPod is the pod a block-producer StatefulSet leaves behind: it carries
// the node's labels and mounts its keys Secret.
func holderPod(dn *dingov1alpha1.DingoNode) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name + "-0",
			Namespace: dn.Namespace,
			Labels:    resources.Labels(dn),
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "block-producer-keys-source",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: dn.Spec.BlockProducer.Keys.SecretRef,
				},
			},
		}}},
	}
}

// The refused node resumes only once the holder's workload is down. Deleting
// the holder DingoNode does not stop its pod: the StatefulSet and pod outlive
// it through garbage collection and the termination grace period.
func TestReconcileRefusedForgerWaitsForHolderWorkload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	poolA, dataA := poolKeys(t)
	poolB, dataB := poolKeys(t)

	setup := func(t *testing.T) (
		client.Client, *DingoNodeReconciler,
		*dingov1alpha1.DingoNode, *dingov1alpha1.DingoNode,
	) {
		holder := forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
		holder.Finalizers = []string{metav1.FinalizerDeleteDependents}
		loser := forgerNode("bp-2", "ns", poolA, "keys-b", 0)
		c := conflictClient(t, holder, loser,
			keysSecret("keys-a", "ns", dataA),
			keysSecret("keys-b", "ns", dataA),
		)
		r := reconcilerFor(c)
		// Reconcile the holder first so its StatefulSet exists at replicas 1,
		// then make the loser's pool match so it is refused.
		reconcileNode(t, r, holder)
		require.Equal(t, int32(1), replicasOf(t, c, holder))
		return c, r, holder, loser
	}

	t.Run("foreground deletion keeps the holder StatefulSet at one", func(t *testing.T) {
		t.Parallel()
		c, r, holder, loser := setup(t)
		require.NoError(t, c.Delete(ctx, holder))
		var deleting dingov1alpha1.DingoNode
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(holder), &deleting))
		require.False(t, deleting.DeletionTimestamp.IsZero())

		reconcileNode(t, r, loser)
		assert.True(t, refusedAsDuplicate(t, c, loser),
			"holder StatefulSet still at replicas=1")
		assert.False(t, forging(t, c, loser))

		// Holder workload goes to zero: the loser may resume.
		var sts appsv1.StatefulSet
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(holder), &sts))
		sts.Spec.Replicas = new(int32)
		require.NoError(t, c.Update(ctx, &sts))
		reconcileNode(t, r, loser)
		assert.False(t, refusedAsDuplicate(t, c, loser))
		assert.True(t, forging(t, c, loser))
	})

	t.Run("plain deletion keeps the holder StatefulSet then pod", func(t *testing.T) {
		t.Parallel()
		c, r, holder, loser := setup(t)
		pod := holderPod(holder)
		require.NoError(t, c.Create(ctx, pod))
		var live dingov1alpha1.DingoNode
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(holder), &live))
		live.Finalizers = nil
		require.NoError(t, c.Update(ctx, &live))
		require.NoError(t, c.Delete(ctx, &live))

		// StatefulSet still at replicas=1, DingoNode gone.
		reconcileNode(t, r, loser)
		assert.True(t, refusedAsDuplicate(t, c, loser), "StatefulSet alive")

		// Garbage collection removes the StatefulSet; the pod is still
		// terminating.
		var sts appsv1.StatefulSet
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(holder), &sts))
		require.NoError(t, c.Delete(ctx, &sts))
		reconcileNode(t, r, loser)
		assert.True(t, refusedAsDuplicate(t, c, loser), "pod alive")
		assert.False(t, forging(t, c, loser))

		require.NoError(t, c.Delete(ctx, pod))
		reconcileNode(t, r, loser)
		assert.False(t, refusedAsDuplicate(t, c, loser))
		assert.True(t, forging(t, c, loser))
	})

	t.Run("an unrelated pool's workload does not block", func(t *testing.T) {
		t.Parallel()
		other := forgerNode("bp-9", "ns", poolB, "keys-z", time.Hour)
		loser := forgerNode("bp-2", "ns", poolA, "keys-b", 0)
		c := conflictClient(t, loser, holderPod(other),
			keysSecret("keys-z", "ns", dataB),
			keysSecret("keys-b", "ns", dataA),
		)
		reconcileNode(t, reconcilerFor(c), loser)
		assert.False(t, refusedAsDuplicate(t, c, loser))
	})
}

// Two DingoNodes naming copies of one key bundle in different Secrets still
// share a pool, whether or not poolId is set: the cold key in the delivered
// opcert says so.
func TestReconcileRefusesCopiedKeysInDifferentSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, data := poolKeys(t)
	_, otherData := poolKeys(t)
	tests := []struct {
		name     string
		loserNS  string
		loserKey map[string][]byte
		conflict bool
	}{
		{"copy in another namespace", "ns-b", data, true},
		{"copy in the same namespace", "ns-a", data, true},
		{"different pool's keys", "ns-b", otherData, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			holder := forgerNode("bp-1", "ns-a", "", "keys-a", time.Hour)
			loser := forgerNode("bp-2", tt.loserNS, "", "keys-b", 0)
			c := conflictClient(t, holder, loser,
				keysSecret("keys-a", "ns-a", data),
				keysSecret("keys-b", tt.loserNS, tt.loserKey),
			)
			reconcileNode(t, reconcilerFor(c), loser)
			assert.Equal(t, tt.conflict, refusedAsDuplicate(t, c, loser))
			if tt.conflict {
				var sts appsv1.StatefulSet
				err := c.Get(ctx, client.ObjectKeyFromObject(loser), &sts)
				assert.True(t, apierrors.IsNotFound(err))
			}
		})
	}
}

// A failed lookup is not a duplicate: the forger keeps its replicas and the
// reconcile returns the error, rather than halting a node nobody duplicates.
func TestReconcileLookupFailureDoesNotHaltForger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, data := poolKeys(t)
	unavailable := apierrors.NewServiceUnavailable("apiserver unavailable")
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "pods"}, "", nil,
	)
	tests := []struct {
		name string
		fail func(obj any, name string) bool
		err  error
		// holder adds an older node staging a copy of the keys.
		holder bool
	}{
		{"pod list forbidden", func(obj any, _ string) bool {
			_, ok := obj.(*corev1.PodList)
			return ok
		}, forbidden, false},
		{"StatefulSet list unavailable", func(obj any, _ string) bool {
			_, ok := obj.(*appsv1.StatefulSetList)
			return ok
		}, unavailable, false},
		{"holder keys Secret unavailable", func(obj any, name string) bool {
			_, ok := obj.(*corev1.Secret)
			return ok && name == "keys-a"
		}, unavailable, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			holder := forgerNode("bp-1", "ns-a", "", "keys-a", time.Hour)
			node := forgerNode("bp-2", "ns-b", "", "keys-b", 0)
			failing := false
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, dingov1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(
					node,
					keysSecret("keys-a", "ns-a", data),
					keysSecret("keys-b", "ns-b", data),
				).
				WithStatusSubresource(&dingov1alpha1.DingoNode{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(
						ctx context.Context, cl client.WithWatch,
						key client.ObjectKey, obj client.Object,
						opts ...client.GetOption,
					) error {
						if failing && tt.fail(obj, key.Name) {
							return tt.err
						}
						return cl.Get(ctx, key, obj, opts...)
					},
					List: func(
						ctx context.Context, cl client.WithWatch,
						list client.ObjectList, opts ...client.ListOption,
					) error {
						if failing && tt.fail(list, "") {
							return tt.err
						}
						return cl.List(ctx, list, opts...)
					},
				}).
				Build()
			r := reconcilerFor(c)
			reconcileNode(t, r, node)
			require.Equal(t, int32(1), replicasOf(t, c, node))

			// Only an unread Secret can hide a holder; a list failure
			// must not invent one.
			if tt.holder {
				require.NoError(t, c.Create(ctx, holder))
			}
			failing = true
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(node),
			})
			failing = false
			assert.Equal(t, int32(1), replicasOf(t, c, node),
				"a failed lookup must not halt the forger")
			assert.False(t, refusedAsDuplicate(t, c, node))
			assert.Error(t, err, "a failed lookup must not admit")
		})
	}
}

// The older node takes the keys only once a newer node already running with
// them is down: the newer node is refused and halts, but its pod outlives the
// scale-down. Here the older node's Secret becomes a copy of the newer one's
// keys with no spec change.
func TestReconcileHolderWaitsForOutrankedWorkload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, dataA := poolKeys(t)
	_, dataB := poolKeys(t)
	older := forgerNode("bp-1", "ns-a", "", "keys-a", time.Hour)
	newer := forgerNode("bp-2", "ns-b", "", "keys-b", 0)
	secretA := keysSecret("keys-a", "ns-a", dataA)
	c := conflictClient(t, older, newer, secretA,
		keysSecret("keys-b", "ns-b", dataB),
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, newer)
	reconcileNode(t, r, older)
	require.True(t, forging(t, c, newer))
	require.True(t, forging(t, c, older))
	pod := holderPod(newer)
	require.NoError(t, c.Create(ctx, pod))

	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secretA), secretA))
	secretA.Data = dataB
	require.NoError(t, c.Update(ctx, secretA))

	reconcileNode(t, r, older)
	assert.True(t, refusedAsDuplicate(t, c, older),
		"newer StatefulSet still at replicas=1")
	assert.False(t, forging(t, c, older))

	reconcileNode(t, r, newer)
	assert.True(t, refusedAsDuplicate(t, c, newer))
	assert.False(t, forging(t, c, newer))

	reconcileNode(t, r, older)
	assert.True(t, refusedAsDuplicate(t, c, older), "newer pod alive")
	assert.False(t, forging(t, c, older))

	require.NoError(t, c.Delete(ctx, pod))
	reconcileNode(t, r, older)
	assert.False(t, refusedAsDuplicate(t, c, older))
	assert.True(t, forging(t, c, older))
	reconcileNode(t, r, newer)
	assert.False(t, forging(t, c, newer))
}

// A holder recreated under its old name is newer than the refused node, but
// the old incarnation's pod carries the same name label and still forges.
func TestReconcileRecreatedHolderPodStillBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, data := poolKeys(t)
	holder := forgerNode("bp-1", "ns", "", "keys-a", time.Hour)
	loser := forgerNode("bp-2", "ns", "", "keys-b", 30*time.Minute)
	c := conflictClient(t, holder, loser,
		keysSecret("keys-a", "ns", data),
		keysSecret("keys-b", "ns", data),
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, holder)
	pod := holderPod(holder)
	require.NoError(t, c.Create(ctx, pod))
	reconcileNode(t, r, loser)
	require.True(t, refusedAsDuplicate(t, c, loser))

	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(holder), &sts))
	require.NoError(t, c.Delete(ctx, &sts))
	require.NoError(t, c.Delete(ctx, holder))
	recreated := forgerNode("bp-1", "ns", "", "keys-a", 0)
	require.NoError(t, c.Create(ctx, recreated))

	reconcileNode(t, r, loser)
	assert.True(t, refusedAsDuplicate(t, c, loser), "old pod alive")
	assert.False(t, forging(t, c, loser))

	require.NoError(t, c.Delete(ctx, pod))
	reconcileNode(t, r, loser)
	assert.False(t, refusedAsDuplicate(t, c, loser))
	assert.True(t, forging(t, c, loser))
}
