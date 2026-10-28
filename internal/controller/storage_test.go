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
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func storageNode() *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "relay",
			Namespace: "nodes",
			UID:       "node-uid",
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Network: "preview", Role: dingov1alpha1.RoleRelay,
			Persistence: dingov1alpha1.PersistenceSpec{
				Size: resource.MustParse("60Gi"),
			},
		},
	}
}

func storageClaim(
	name, request, capacity string,
) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "nodes",
			UID:       types.UID(name),
			Labels:    map[string]string{"cardano_network": "preview"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "retained-volume", AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(request),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(capacity),
			},
		},
	}
}

func TestReconcileStorage(t *testing.T) {
	for _, tc := range []struct {
		name, request, capacity, expected, reason string
		replicas                                  int32
		resizePending                             bool
	}{
		{name: "expansion", request: "30Gi", capacity: "30Gi", expected: "60Gi", reason: "ExpansionPending", replicas: 1},
		{name: "retained while scaled down", request: "30Gi", capacity: "30Gi", expected: "60Gi", reason: "ExpansionPending"},
		{name: "already expanded", request: "60Gi", capacity: "60Gi", expected: "60Gi", reason: "VolumesReady", replicas: 1},
		{name: "never shrink", request: "100Gi", capacity: "100Gi", expected: "100Gi", reason: "VolumesReady", replicas: 1},
		{name: "filesystem expansion pending", request: "60Gi", capacity: "60Gi", expected: "60Gi", reason: "ExpansionPending", replicas: 1, resizePending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dn := storageNode()
			dn.Spec.Replicas = &tc.replicas
			pvc := storageClaim("dingo-data-relay-0", tc.request, tc.capacity)
			if tc.replicas == 0 {
				pvc.Name = "dingo-data-relay-3"
			}
			if tc.resizePending {
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
						Status: corev1.ConditionTrue,
					},
				}
			}
			foreign := storageClaim("dingo-data-relay-other-0", "10Gi", "10Gi")
			malformed := storageClaim("dingo-data-relay-01", "10Gi", "10Gi")
			c := fake.NewClientBuilder().
				WithObjects(pvc, foreign, malformed).
				WithStatusSubresource(pvc).
				Build()
			r := &DingoNodeReconciler{Client: c}
			require.NoError(t, r.reconcileStorage(ctx, dn))
			got := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), got))
			assert.Equal(
				t,
				resource.MustParse(tc.expected),
				*got.Spec.Resources.Requests.Storage(),
			)
			assert.Equal(t, pvc.UID, got.UID)
			assert.Equal(t, pvc.Spec.VolumeName, got.Spec.VolumeName)
			assert.Empty(t, got.OwnerReferences)
			for _, untouched := range []*corev1.PersistentVolumeClaim{foreign, malformed} {
				require.NoError(
					t,
					c.Get(ctx, client.ObjectKeyFromObject(untouched), got),
				)
				assert.Equal(
					t,
					resource.MustParse("10Gi"),
					*got.Spec.Resources.Requests.Storage(),
				)
			}
			condition := meta.FindStatusCondition(
				dn.Status.Conditions,
				condStorageReady,
			)
			require.NotNil(t, condition)
			assert.Equal(t, tc.reason, condition.Reason)
		})
	}
}

func TestReconcileStorageWaitsForPendingClaimCapacity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dn := storageNode()
	replicas := int32(1)
	dn.Spec.Replicas = &replicas
	pvc := storageClaim("dingo-data-relay-0", "60Gi", "60Gi")
	pvc.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}
	c := fake.NewClientBuilder().WithObjects(pvc).Build()
	r := &DingoNodeReconciler{Client: c}

	require.NoError(t, r.reconcileStorage(ctx, dn))
	condition := meta.FindStatusCondition(
		dn.Status.Conditions,
		condStorageReady,
	)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, "ClaimsPending", condition.Reason)
}

func TestStorageExpansionConflictNeverShrinks(t *testing.T) {
	ctx := context.Background()
	pvc := storageClaim("dingo-data-relay-0", "30Gi", "30Gi")
	var patches int
	c := fake.NewClientBuilder().
		WithObjects(pvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				patches++
				if patches == 1 {
					concurrent := &corev1.PersistentVolumeClaim{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), concurrent); err != nil {
						return err
					}
					concurrent.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse(
						"100Gi",
					)
					if err := cl.Update(ctx, concurrent); err != nil {
						return err
					}
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &DingoNodeReconciler{Client: c}
	require.NoError(t, r.reconcileStorage(ctx, storageNode()))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	assert.Equal(
		t,
		resource.MustParse("100Gi"),
		*pvc.Spec.Resources.Requests.Storage(),
	)
	assert.Equal(t, 1, patches)
}

func TestStorageFailureConditionIsPersisted(t *testing.T) {
	ctx := context.Background()
	dn := storageNode()
	pvc := storageClaim("dingo-data-relay-0", "30Gi", "30Gi")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	failure := errors.New("storage class does not allow expansion")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dn, pvc).
		WithStatusSubresource(dn, pvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return failure
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &DingoNodeReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(
		ctx,
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dn)},
	)
	require.ErrorIs(t, err, failure)
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), got))
	condition := meta.FindStatusCondition(
		got.Status.Conditions,
		condStorageReady,
	)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, "VolumeResizeFailed", condition.Reason)
	assert.Contains(t, condition.Message, failure.Error())
}

func TestStatefulSetStorageRetention(t *testing.T) {
	sts := resources.BuildStatefulSet(
		storageNode(),
		resources.RenderOptions{Replicas: 1},
	)
	require.NotNil(t, sts.Spec.PersistentVolumeClaimRetentionPolicy)
	assert.Equal(
		t,
		appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted,
	)
	assert.Equal(
		t,
		appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled,
	)
}

func TestStorageWaitsForClaims(t *testing.T) {
	dn := storageNode()
	r := &DingoNodeReconciler{Client: fake.NewClientBuilder().Build()}
	require.NoError(t, r.reconcileStorage(context.Background(), dn))
	condition := meta.FindStatusCondition(
		dn.Status.Conditions,
		condStorageReady,
	)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, "ClaimsPending", condition.Reason)
}

func TestStorageExpansionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pvc := storageClaim("dingo-data-relay-0", "60Gi", "60Gi")
	c := fake.NewClientBuilder().
		WithObjects(pvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return errors.New("an already-sized claim must not be patched")
			},
		}).
		Build()
	r := &DingoNodeReconciler{Client: c}
	require.NoError(t, r.reconcileStorage(ctx, storageNode()))
}

func TestStorageExpansionAdmission(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "nodes")
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "resizeable"},
		Provisioner: "example.com/test-csi", AllowVolumeExpansion: new(true),
	}
	require.NoError(t, c.Create(ctx, sc))
	dn := storageNode()
	dn.UID = ""
	dn.Spec.Persistence.Size = resource.MustParse("30Gi")
	dn.Spec.Persistence.StorageClass = new(sc.Name)
	require.NoError(t, c.Create(ctx, dn))
	r := reconcilerFor(c)
	reconcile(t, ctx, r, dn.Name, dn.Namespace)
	original := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), original))
	pvc := storageClaim("dingo-data-relay-0", "30Gi", "30Gi")
	pvc.UID = ""
	pvc.Spec.StorageClassName = new(sc.Name)
	desiredStatus := pvc.Status
	desiredStatus.Phase = corev1.ClaimBound
	pvc.Status = corev1.PersistentVolumeClaimStatus{}
	require.NoError(t, c.Create(ctx, pvc))
	pvc.Status = desiredStatus
	require.NoError(t, c.Status().Update(ctx, pvc))
	uid := pvc.UID
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), dn))
	dn.Spec.Persistence.Size = resource.MustParse("60Gi")
	require.NoError(t, c.Update(ctx, dn))
	reconcile(t, ctx, r, dn.Name, dn.Namespace)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	assert.Equal(
		t,
		resource.MustParse("60Gi"),
		*pvc.Spec.Resources.Requests.Storage(),
	)
	assert.Equal(t, uid, pvc.UID)
	assert.Equal(t, "retained-volume", pvc.Spec.VolumeName)
	current := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), current))
	assert.Equal(t, original.UID, current.UID)
	assert.Equal(
		t,
		original.Spec.VolumeClaimTemplates,
		current.Spec.VolumeClaimTemplates,
	)
	assert.Equal(t, original.Spec.Template, current.Spec.Template)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), dn))
	condition := meta.FindStatusCondition(
		dn.Status.Conditions,
		condStorageReady,
	)
	require.NotNil(t, condition)
	assert.Equal(t, "ExpansionPending", condition.Reason)
	sc.AllowVolumeExpansion = new(false)
	require.NoError(t, c.Update(ctx, sc))
	dn.Spec.Persistence.Size = resource.MustParse("90Gi")
	require.NoError(t, c.Update(ctx, dn))
	_, err := r.Reconcile(
		ctx,
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dn)},
	)
	require.Error(t, err)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), dn))
	condition = meta.FindStatusCondition(dn.Status.Conditions, condStorageReady)
	require.NotNil(t, condition)
	assert.Equal(t, "VolumeResizeFailed", condition.Reason)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	assert.Equal(
		t,
		resource.MustParse("60Gi"),
		*pvc.Spec.Resources.Requests.Storage(),
	)
}

func legacyDataStatefulSet(t *testing.T) *appsv1.StatefulSet {
	t.Helper()
	sts := resources.BuildStatefulSet(
		storageNode(),
		resources.RenderOptions{Replicas: 1, DataVolumeName: "data"},
	)
	require.Equal(t, "data", sts.Spec.VolumeClaimTemplates[0].Name)
	return sts
}

func TestReconcileStorageLegacyClaimName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dn := storageNode()
	legacy := storageClaim("data-relay-0", "30Gi", "30Gi")
	current := storageClaim("dingo-data-relay-0", "30Gi", "30Gi")
	c := fake.NewClientBuilder().
		WithObjects(legacyDataStatefulSet(t), legacy).
		Build()
	r := &DingoNodeReconciler{Client: c}
	require.NoError(t, r.reconcileStorage(ctx, dn))
	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(legacy), got))
	assert.Equal(t, "60Gi", got.Spec.Resources.Requests.Storage().String())

	// A node without a StatefulSet resizes dingo-data claims only.
	c = fake.NewClientBuilder().WithObjects(current).Build()
	r = &DingoNodeReconciler{Client: c}
	require.NoError(t, r.reconcileStorage(ctx, storageNode()))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(current), got))
	assert.Equal(t, "60Gi", got.Spec.Resources.Requests.Storage().String())
}
