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
	networkingv1 "k8s.io/api/networking/v1"
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

// staleRolloutFixture returns a reconciler over a fake client holding a
// node's StatefulSet mid-rollout with one stale unready pod. A block producer
// whose keys Secret does not exist has its template held.
func staleRolloutFixture(
	t *testing.T,
	dn *dingov1alpha1.DingoNode,
	delErr error,
	observe ...func(*client.DeleteOptions),
) (*DingoNodeReconciler, *dingov1alpha1.DingoNode, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	dn.UID = types.UID("node-uid")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&appsv1.StatefulSet{}).
		WithObjects(dn).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.DeleteOption,
			) error {
				if _, ok := obj.(*corev1.Pod); ok {
					var o client.DeleteOptions
					o.ApplyOptions(opts)
					for _, f := range observe {
						f(&o)
					}
					if delErr != nil {
						return delErr
					}
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := reconcilerFor(c)
	ctx := context.Background()
	// The first pass creates the StatefulSet. Later passes render the same
	// template, so a held one equals the live one.
	require.NoError(t, r.reconcileResources(ctx, dn))

	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), sts))
	sts.Status.UpdateRevision = "new-revision"
	require.NoError(t, c.Status().Update(ctx, sts))
	require.NoError(t, c.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name + "-0",
			Namespace: dn.Namespace,
			UID:       types.UID(dn.Name + "-0-uid"),
			Labels: map[string]string{
				resources.NodeLabel:             dn.Name,
				appsv1.StatefulSetRevisionLabel: "old-revision",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "StatefulSet",
				Name:       sts.Name,
				UID:        sts.UID,
				Controller: new(true),
			}},
		},
	}))
	return r, dn, c
}

func TestReconcileResourcesKeepsStalePodWhileTemplateHeld(t *testing.T) {
	r, dn, c := staleRolloutFixture(
		t,
		blockProducerNode("bp", "bp-ns", "missing-keys", "pool"),
		nil,
	)
	ctx := context.Background()

	require.NoError(t, r.reconcileResources(ctx, dn))

	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp-0", Namespace: dn.Namespace},
		&corev1.Pod{},
	), "a held template must not replace the pod onto the refused Secret")
}

// A pod recreated between the list and the delete fails the UID precondition
// with a Conflict. That must not stop the rest of the resources applying.
func TestReconcileResourcesToleratesStalePodUIDConflict(t *testing.T) {
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "relay-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
	r, dn, c := staleRolloutFixture(t, dn, conflictErr("relay-0"))
	ctx := context.Background()
	key := client.ObjectKeyFromObject(dn)
	require.NoError(t, c.Delete(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: dn.Name, Namespace: dn.Namespace},
	}))

	require.NoError(t, r.reconcileResources(ctx, dn))

	require.NoError(t, c.Get(ctx, key, &networkingv1.NetworkPolicy{}),
		"resources after the pod nudge must still be applied")
}

// A pod can become Ready between the list and the delete. The delete must
// carry the listed resourceVersion so the API server rejects it then.
func TestReconcileResourcesDeletesStalePodAtListedResourceVersion(
	t *testing.T,
) {
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "relay-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
	var got *client.DeleteOptions
	r, dn, c := staleRolloutFixture(t, dn, nil, func(o *client.DeleteOptions) {
		got = o
	})
	ctx := context.Background()
	pod := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: dn.Name + "-0", Namespace: dn.Namespace,
	}, pod))

	require.NoError(t, r.reconcileResources(ctx, dn))

	require.NotNil(t, got, "the stale pod must be deleted")
	require.NotNil(t, got.Preconditions)
	require.NotNil(t, got.Preconditions.ResourceVersion)
	assert.Equal(t, pod.ResourceVersion, *got.Preconditions.ResourceVersion)
	require.NotNil(t, got.Preconditions.UID)
	assert.Equal(t, pod.UID, *got.Preconditions.UID)
}

// While the StatefulSet controller has not observed the latest generation,
// UpdateRevision can predate a template change, so no pod is stale yet.
func TestReconcileResourcesKeepsPodsUntilGenerationObserved(t *testing.T) {
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "relay-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
	r, dn, c := staleRolloutFixture(t, dn, nil)
	ctx := context.Background()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), sts))
	sts.Generation = 2
	require.NoError(t, c.Update(ctx, sts))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), sts))
	require.Equal(t, int64(2), sts.Generation)
	sts.Status.ObservedGeneration = 1
	require.NoError(t, c.Status().Update(ctx, sts))
	podKey := types.NamespacedName{
		Name: dn.Name + "-0", Namespace: dn.Namespace,
	}

	require.NoError(t, r.reconcileResources(ctx, dn))
	require.NoError(t, c.Get(ctx, podKey, &corev1.Pod{}),
		"an unobserved generation must not classify pods as stale")

	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), sts))
	sts.Status.ObservedGeneration = sts.Generation
	require.NoError(t, c.Status().Update(ctx, sts))
	require.NoError(t, r.reconcileResources(ctx, dn))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, podKey, &corev1.Pod{})))
}
