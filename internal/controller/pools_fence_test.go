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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// singleNode is a single-pool block producer for the fence tests.
func singleNode(
	name, ns, pool, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	return forgerNode(name, ns, pool, secret, age)
}

// withSecret is p kept in another keys Secret.
func withSecret(p testPool, secret string) testPool {
	p.secret = secret
	return p
}

func TestPoolListFence(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b", "c")
	list := func(name, ns string, age time.Duration, p ...testPool) *dingov1alpha1.DingoNode {
		return poolListNode(name, ns, age, p...)
	}
	tests := []struct {
		name string
		// first is older and keeps the pool; second is refused when
		// conflict is set, and must name wantPool.
		first, second *dingov1alpha1.DingoNode
		conflict      bool
		wantPool      string
	}{
		{
			name: "a listed pool against another node's poolId",
			first: singleNode(
				"one",
				"ns",
				pools[1].id,
				"keys-one",
				time.Hour,
			),
			second:   list("many", "ns", 0, pools[0], pools[1]),
			conflict: true,
			wantPool: pools[1].id,
		},
		{
			name:     "a poolId against an older node's listed pool",
			first:    list("many", "ns", time.Hour, pools[0], pools[1]),
			second:   singleNode("one", "ns", pools[1].id, "keys-one", 0),
			conflict: true,
			wantPool: pools[1].id,
		},
		{
			// Signing every pool through an agent exempts no pair.
			name:     "a listed pool against another node's list",
			first:    list("first", "ns", time.Hour, pools[0], pools[1]),
			second:   list("second", "ns", 0, pools[1], pools[2]),
			conflict: true,
			wantPool: pools[1].id,
		},
		{
			name:   "disjoint lists",
			first:  list("first", "ns", time.Hour, pools[0]),
			second: list("second", "ns", 0, pools[1], pools[2]),
		},
		{
			name:  "the same pool on different networks",
			first: list("first", "ns", time.Hour, pools[0], pools[1]),
			// The pool's copy has a Secret of its own: a Secret shared by two
			// nodes is one forger on any network.
			second: onNetwork(
				list(
					"second",
					"ns",
					0,
					withSecret(pools[1], "keys-b2"),
					pools[2],
				),
				7,
			),
		},
		{
			name:  "a listed pool against a poolId on a different network",
			first: singleNode("one", "ns", pools[1].id, "keys-one", time.Hour),
			second: onNetwork(
				list("many", "ns", 0, pools[0], pools[1]), 7,
			),
		},
		{
			name: "one keys Secret listed by two nodes",
			first: singleNode(
				"one", "ns", pools[2].id, pools[1].secret, time.Hour,
			),
			second:   list("many", "ns", 0, pools[0], pools[1]),
			conflict: true,
		},
		{
			name:     "a listed pool in another namespace",
			first:    list("first", "ns-a", time.Hour, pools[0], pools[1]),
			second:   singleNode("one", "ns-b", pools[1].id, "keys-one", 0),
			conflict: true,
			wantPool: pools[1].id,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c := conflictClient(t, tt.first, tt.second)
			r := reconcilerFor(c)

			reconcileNode(t, r, tt.second)
			var got dingov1alpha1.DingoNode
			require.NoError(
				t, c.Get(ctx, client.ObjectKeyFromObject(tt.second), &got),
			)
			degraded := meta.FindStatusCondition(
				got.Status.Conditions, condDegraded,
			)
			var sts appsv1.StatefulSet
			stsErr := c.Get(ctx, client.ObjectKeyFromObject(tt.second), &sts)
			if tt.conflict {
				require.NotNil(t, degraded, "the newer forger must be Degraded")
				assert.Equal(t, reasonDuplicateForger, degraded.Reason)
				if tt.wantPool != "" {
					assert.Contains(t, degraded.Message, "pool "+tt.wantPool)
				}
				assert.True(
					t,
					apierrors.IsNotFound(stsErr),
					"no StatefulSet for the loser",
				)
			} else if degraded != nil {
				assert.NotEqual(t, reasonDuplicateForger, degraded.Reason)
			}

			// The older node keeps forging: it is never flagged.
			reconcileNode(t, r, tt.first)
			require.NoError(
				t, c.Get(ctx, client.ObjectKeyFromObject(tt.first), &got),
			)
			if d := meta.FindStatusCondition(
				got.Status.Conditions, condDegraded,
			); d != nil {
				assert.NotEqual(t, reasonDuplicateForger, d.Reason)
			}
		})
	}
}

// A pool left out of a node's pod for its keys is still the node's pool: the
// spec and the Secret still name it, so dropping it must not free it for
// another node.
func TestPoolListFenceKeepsADroppedPoolClaimed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	first := poolListNode("many", "ns-a", time.Hour, pools...)
	second := singleNode("one", "ns-b", pools[1].id, "keys-one", 0)
	// Pool b's bundle is invalid, so only pool a is rendered.
	broken := poolSecret(t, "ns-a", pools[1], 0)
	broken.Data[keyFileVRF] = []byte("junk")
	c := conflictClient(
		t, first, second, poolSecret(t, "ns-a", pools[0], 0), broken,
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, first)
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(first), &sts))
	assert.NotContains(
		t, sts.Spec.Template.Annotations[resources.KeysPoolAnnotation],
		pools[1].id, "the pool is not staged",
	)

	reconcileNode(t, r, second)
	var got dingov1alpha1.DingoNode
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(second), &got))
	degraded := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
	require.NotNil(t, degraded)
	assert.Equal(t, reasonDuplicateForger, degraded.Reason)
	assert.Contains(t, degraded.Message, "pool "+pools[1].id)
}

// The workload of a pool-list node holds every pool it staged after the
// DingoNode that owned it is gone.
func TestPoolListWorkloadHoldsEveryStagedPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	gone := poolListNode("many", "ns", 0, pools...)
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	for _, p := range pools {
		opts.Pools = append(opts.Pools, resources.RenderPool{
			PoolID: p.id, SecretName: p.secret, AgentProfile: p.profile,
		})
	}
	opts.KeysPool = resources.JoinKeysPools([]string{pools[0].id, pools[1].id})
	orphan := resources.BuildStatefulSet(gone, opts)

	for _, p := range pools {
		t.Run(p.id, func(t *testing.T) {
			t.Parallel()
			next := singleNode("one", "ns", p.id, "keys-one", 0)
			c := conflictClient(t, next, orphan.DeepCopy())
			reconcileNode(t, reconcilerFor(c), next)
			var got dingov1alpha1.DingoNode
			require.NoError(
				t,
				c.Get(ctx, client.ObjectKeyFromObject(next), &got),
			)
			degraded := meta.FindStatusCondition(
				got.Status.Conditions, condDegraded,
			)
			require.NotNil(t, degraded)
			assert.Equal(t, reasonDuplicateForger, degraded.Reason)
			assert.Contains(t, degraded.Message, "pool "+p.id)
		})
	}

	t.Run("another pool is free", func(t *testing.T) {
		t.Parallel()
		other := newTestPools(t, "z")[0]
		next := singleNode("one", "ns", other.id, "keys-one", 0)
		c := conflictClient(t, next, orphan.DeepCopy())
		reconcileNode(t, reconcilerFor(c), next)
		var got dingov1alpha1.DingoNode
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(next), &got))
		if d := meta.FindStatusCondition(
			got.Status.Conditions, condDegraded,
		); d != nil {
			assert.NotEqual(t, reasonDuplicateForger, d.Reason)
		}
	})
}

// A held template mounts each pool's Secret by name, so a recreated pod stages
// whatever the Secret holds then: another pool's keys would forge for that
// pool.
func TestRestagesOtherPoolOfList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	dn := poolListNode("many", "ns", 0, pools[0], pools[1])
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	for _, p := range pools[:2] {
		opts.Pools = append(opts.Pools, resources.RenderPool{
			PoolID: p.id, SecretName: p.secret, AgentProfile: p.profile,
		})
	}
	opts.KeysPool = resources.JoinKeysPools([]string{pools[0].id, pools[1].id})
	sts := resources.BuildStatefulSet(dn, opts)
	dn.UID = "many-uid"
	setController(
		sts,
		"dingo.blinklabs.io/v1alpha1",
		"DingoNode",
		"many",
		dn.UID,
	)

	secretOf := func(holder testPool, name string) *corev1.Secret {
		s := poolSecret(t, "ns", holder, 0)
		s.Name = name
		return s
	}
	tests := []struct {
		name string
		a, b *corev1.Secret
		want bool
	}{
		{"unchanged keys", secretOf(pools[0], pools[0].secret),
			secretOf(pools[1], pools[1].secret), false},
		{"a new opcert of the same pool", secretOf(pools[0], pools[0].secret),
			secretOf(pools[1], pools[1].secret), false},
		{"another pool's keys in a pool's Secret",
			secretOf(pools[2], pools[0].secret),
			secretOf(pools[1], pools[1].secret), true},
		{"the pools' Secrets swapped",
			secretOf(pools[1], pools[0].secret),
			secretOf(pools[0], pools[1].secret), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := conflictClient(t, dn.DeepCopy(), sts.DeepCopy(), tt.a, tt.b)
			got, err := reconcilerFor(c).restagesOtherPool(ctx, dn)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a record that does not match the Secrets", func(t *testing.T) {
		t.Parallel()
		unrecorded := sts.DeepCopy()
		unrecorded.Spec.Template.Annotations[resources.KeysPoolAnnotation] =
			pools[0].id
		c := conflictClient(t, dn.DeepCopy(), unrecorded,
			secretOf(pools[0], pools[0].secret),
			secretOf(pools[1], pools[1].secret))
		got, err := reconcilerFor(c).restagesOtherPool(ctx, dn)
		require.NoError(t, err)
		assert.True(t, got)
	})
}

func TestForgerIdentitySharedWithLists(t *testing.T) {
	t.Parallel()
	a := forgerIdentity{
		namespace: "ns", network: "magic:2",
		pools:   []string{"pool-1", "pool-2"},
		secrets: []string{"keys-1", "keys-2"},
	}
	assert.Equal(t, "pool pool-2", a.sharedWith(forgerIdentity{
		namespace: "other", network: "magic:2", pools: []string{"pool-2"},
	}))
	assert.Empty(t, a.sharedWith(forgerIdentity{
		namespace: "other", network: "magic:3", pools: []string{"pool-2"},
	}), "another network")
	assert.Equal(t, `keys Secret "keys-2"`, a.sharedWith(forgerIdentity{
		namespace: "ns", network: "magic:3", secrets: []string{"x", "keys-2"},
	}))
	assert.Empty(t, a.sharedWith(forgerIdentity{
		namespace: "other", secrets: []string{"keys-2"},
	}), "a Secret name in another namespace is another Secret")
	assert.Empty(t, a.sharedWith(forgerIdentity{
		namespace: "ns", secrets: []string{""},
	}), "an empty name is no Secret")
}
