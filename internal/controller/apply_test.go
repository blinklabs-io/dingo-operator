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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReconcileResourcesReplacesStaleUnreadyPod(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))

	dn := &dingov1alpha1.DingoNode{
		TypeMeta: metav1.TypeMeta{
			APIVersion: dingov1alpha1.GroupVersion.String(),
			Kind:       "DingoNode",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "relay",
			Namespace: "relay-ns",
			UID:       types.UID("node-uid"),
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
	sts := resources.BuildStatefulSet(
		dn,
		resources.RenderOptions{Replicas: 1},
	)
	sts.CreationTimestamp = metav1.Now()
	sts.UID = types.UID("statefulset-uid")
	sts.Status.CurrentRevision = "old-revision"
	sts.Status.UpdateRevision = "new-revision"
	require.NoError(t, ctrl.SetControllerReference(dn, sts, scheme))

	pod := func(
		name, revision string,
		ready bool,
		ownerUID types.UID,
	) *corev1.Pod {
		readyStatus := corev1.ConditionFalse
		if ready {
			readyStatus = corev1.ConditionTrue
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: dn.Namespace,
				UID:       types.UID(name + "-uid"),
				Labels: map[string]string{
					resources.NodeLabel:             dn.Name,
					appsv1.StatefulSetRevisionLabel: revision,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       "StatefulSet",
					Name:       sts.Name,
					UID:        ownerUID,
					Controller: new(true),
				}},
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: readyStatus,
			}}},
		}
	}
	stale := pod("relay-0", "old-revision", false, sts.UID)
	ready := pod("relay-1", "old-revision", true, sts.UID)
	current := pod("relay-2", "new-revision", false, sts.UID)
	foreign := pod("relay-3", "old-revision", false, types.UID("other-uid"))
	unversioned := pod("relay-4", "", false, sts.UID)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&appsv1.StatefulSet{}).
		WithObjects(sts, stale, ready, current, foreign, unversioned).
		Build()

	require.NoError(
		t,
		reconcilerFor(c).reconcileResources(context.Background(), dn),
	)
	assert.True(t, apierrors.IsNotFound(c.Get(
		context.Background(), client.ObjectKeyFromObject(stale), &corev1.Pod{},
	)))
	for _, kept := range []*corev1.Pod{ready, current, foreign, unversioned} {
		require.NoError(t, c.Get(
			context.Background(),
			client.ObjectKeyFromObject(kept),
			&corev1.Pod{},
		))
	}
}

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
