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
	"errors"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A pool list's identity names every listed pool and keys Secret from the
// spec, even when reading one pool's Secret fails: a caller that tolerates a
// denied read compares the identity it got.
func TestNodeIdentityOfListKeepsSpecClaimsWhenAReadFails(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	c := fake.NewClientBuilder().
		WithScheme(conflictClient(t).Scheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context, cl client.WithWatch,
				key client.ObjectKey, obj client.Object,
				opts ...client.GetOption,
			) error {
				if _, ok := obj.(*corev1.Secret); ok &&
					key.Name == pools[0].secret {
					return apierrors.NewForbidden(
						schema.GroupResource{Resource: "secrets"},
						key.Name, errors.New("denied"),
					)
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	id, err := reconcilerFor(c).nodeIdentity(context.Background(), dn)
	require.True(t, apierrors.IsForbidden(err), "%v", err)
	assert.ElementsMatch(t, []string{pools[0].id, pools[1].id}, id.pools)
	assert.ElementsMatch(
		t, []string{pools[0].secret, pools[1].secret}, id.secrets,
	)
}

// A pool-list node rolls past another producer's pod of unknown pool only
// when its own running pod already staged every pool it lists: a pool new to
// the node may be the one that pod still forges.
func TestRollsPastUnknownPoolNeedsEveryListedPoolStaged(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	for _, tc := range []struct {
		name     string
		recorded []string
		want     bool
	}{
		{"every pool staged", []string{pools[0].id, pools[1].id}, true},
		{"a pool is new", []string{pools[0].id}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dn := poolListNode("bp", "ns", 0, pools...)
			dn.UID = "bp-uid"
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
				Name: "bp", Namespace: "ns", UID: "sts-uid",
			}}
			setController(
				sts, dingov1alpha1.GroupVersion.String(), "DingoNode",
				dn.Name, dn.UID,
			)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bp-0",
					Namespace: "ns",
					Labels:    map[string]string{resources.NodeLabel: "bp"},
					Annotations: map[string]string{
						resources.KeysPoolAnnotation: resources.JoinKeysPools(
							tc.recorded,
						),
					},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
			c := conflictClient(t, dn, sts, pod,
				poolSecret(t, "ns", pools[0], 0),
				poolSecret(t, "ns", pools[1], 0),
			)
			r := reconcilerFor(c)
			mine, err := r.nodeIdentity(ctx, dn)
			require.NoError(t, err)
			rolls, err := r.rollsPastUnknownPool(ctx, dn, mine)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rolls)
		})
	}
}
