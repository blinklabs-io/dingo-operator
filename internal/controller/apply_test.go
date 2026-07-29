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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// conflictErr is the error the API server returns when the submitted
// resourceVersion is stale, which is what the manager's cache can hand a
// reconcile that just wrote the same object.
func conflictErr(name string) error {
	return apierrors.NewConflict(
		schema.GroupResource{Resource: "configmaps"},
		name,
		errors.New("the object has been modified"),
	)
}

func TestCreateOrUpdateWithRetryRecoversFromConflict(t *testing.T) {
	var updates int
	c := fake.NewClientBuilder().
		WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "ns"},
		}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.UpdateOption,
			) error {
				updates++
				// Fail only the first attempt, exactly as a stale cached read
				// would.
				if updates == 1 {
					return conflictErr(obj.GetName())
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()

	cm := &corev1.ConfigMap{
		ObjectMeta: objectMeta("cm", "ns"),
	}
	err := createOrUpdateWithRetry(context.Background(), c, cm, func() error {
		cm.Data = map[string]string{"k": "v"}
		return nil
	})

	require.NoError(t, err, "a single conflict must not fail the reconcile")
	assert.Equal(t, 2, updates, "expected one retry after the conflict")

	// The mutation must actually have landed — a retry that swallows the error
	// without applying the change would be worse than failing.
	got := &corev1.ConfigMap{}
	require.NoError(
		t,
		c.Get(context.Background(), client.ObjectKeyFromObject(cm), got),
	)
	assert.Equal(t, "v", got.Data["k"])
}

func TestCreateOrUpdateWithRetryPropagatesOtherErrors(t *testing.T) {
	var updates int
	sentinel := errors.New("not a conflict")
	c := fake.NewClientBuilder().
		WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "ns"},
		}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.UpdateOption,
			) error {
				updates++
				return sentinel
			},
		}).
		Build()

	cm := &corev1.ConfigMap{ObjectMeta: objectMeta("cm", "ns")}
	err := createOrUpdateWithRetry(context.Background(), c, cm, func() error {
		cm.Data = map[string]string{"k": "v"}
		return nil
	})

	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, updates, "non-conflict errors must not be retried")
}
