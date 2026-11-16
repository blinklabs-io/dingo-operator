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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
