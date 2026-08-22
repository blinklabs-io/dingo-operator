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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// childrenExistTimeout bounds the operator creating the node's full set of
	// child objects. They all go in during a single reconcile pass, well before
	// the pod this test has already waited to become Ready, so this is generous
	// by a wide margin — it only has to absorb a reconcile that errored once
	// and came back on its requeue.
	childrenExistTimeout = 2 * time.Minute

	// childGCTimeout bounds the garbage collector reaping the children of a
	// deleted DingoNode. This is the deadline that makes the test an e2e test
	// at all: cascade deletion is a kube-controller-manager behaviour, so it
	// does not happen under envtest (apiserver + etcd only), and it is
	// asynchronous — the delete call returns long before the children go.
	// Measured on k3d at a few seconds; 2m is margin for a busy GC queue.
	childGCTimeout = 2 * time.Minute

	// pvcRetainWindow is how long the data PVC must demonstrably keep existing
	// after the children are gone. A single Get right after the cascade would
	// pass even if the claim were queued for deletion a moment later, which is
	// exactly what a whenDeleted=Delete retention policy would look like: the
	// API server puts the ownerReference on at StatefulSet creation, but the GC
	// only acts once the owner is actually gone.
	pvcRetainWindow = 30 * time.Second
	pvcRetainPoll   = 5 * time.Second
)

// ownedChild is one object the reconciler creates with the DingoNode as its
// controller owner, and therefore one object the API server's garbage collector
// must reap when the DingoNode is deleted.
//
// obj is a constructor rather than a value so each Get gets a fresh, empty
// object: reusing one would leave the previous read's fields in place and a
// "still present" check could read stale data.
type ownedChild struct {
	what string
	key  types.NamespacedName
	obj  func() client.Object
}

// ownedChildren returns every object internal/controller's reconcileResources
// applies for this DingoNode with a controller reference back to it.
//
// The names are taken from the resource builders themselves rather than spelled
// out here, so a rename in internal/resources moves this list with it rather
// than silently leaving the test asserting on an object nobody creates.
//
// One deliberate omission: the PodMonitor. reconcileResources applies it only
// when the prometheus-operator CRD is installed (the reconciler's
// PodMonitorCRD field gates it, and the apply is best-effort even then), and
// the k3d cluster hack/e2e/k3d-up.sh brings up has no prometheus-operator — so
// none is ever created here and asserting on one would fail for the wrong
// reason. The keys Secret and the config-bundle ConfigMap are absent too, but
// for a different reason: they are inputs the test creates, not children the
// operator owns, and nothing should reap them.
func ownedChildren(dn *dingov1alpha1.DingoNode) []ownedChild {
	key := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: dn.Namespace}
	}
	// Mirrors what reconcileResources passes for a block producer with a
	// topology. Only the rendered object's name is used.
	opts := resources.RenderOptions{
		HasTopology: true,
		Replicas:    1,
		MountKeys:   resources.IsBlockProducer(dn),
	}
	return []ownedChild{
		{
			what: "ServiceAccount",
			key:  key(resources.BuildServiceAccount(dn).Name),
			obj:  func() client.Object { return &corev1.ServiceAccount{} },
		},
		{
			what: "headless Service",
			key:  key(resources.BuildHeadlessService(dn).Name),
			obj:  func() client.Object { return &corev1.Service{} },
		},
		{
			what: "client Service",
			key:  key(resources.BuildClientService(dn).Name),
			obj:  func() client.Object { return &corev1.Service{} },
		},
		{
			what: "topology ConfigMap",
			key:  key(resources.BuildTopologyConfigMap(dn, "").Name),
			obj:  func() client.Object { return &corev1.ConfigMap{} },
		},
		{
			what: "StatefulSet",
			key:  key(resources.BuildStatefulSet(dn, opts).Name),
			obj:  func() client.Object { return &appsv1.StatefulSet{} },
		},
		{
			what: "PodDisruptionBudget",
			key:  key(resources.BuildPodDisruptionBudget(dn).Name),
			obj: func() client.Object {
				return &policyv1.PodDisruptionBudget{}
			},
		},
		{
			what: "NetworkPolicy",
			key:  key(resources.BuildNetworkPolicy(dn).Name),
			obj: func() client.Object {
				return &networkingv1.NetworkPolicy{}
			},
		},
	}
}

// getChild reads one child, returning (nil, nil) when it does not exist.
// Non-fataling: safe inside any polling condition.
func (h *harness) getChild(
	ctx context.Context,
	c ownedChild,
) (client.Object, error) {
	obj := c.obj()
	if err := h.client.Get(ctx, c.key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get %s %s: %w", c.what, c.key.Name, err)
	}
	return obj, nil
}

// assertControlledBy checks that obj names dn as its controller owner. This is
// the mechanism behind every removal the deletion test observes: without a
// controller reference the garbage collector has no reason to touch the object,
// so asserting the references says *why* the cascade happens rather than just
// that things vanished.
func assertControlledBy(
	t *testing.T,
	obj client.Object,
	dn *dingov1alpha1.DingoNode,
	what string,
) {
	t.Helper()
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID != dn.UID {
			continue
		}
		assert.Equal(t, "DingoNode", ref.Kind,
			"%s %s is owned by the right UID under the wrong kind",
			what, obj.GetName())
		if ref.Controller == nil {
			t.Errorf("%s %s has a plain owner reference to the DingoNode, "+
				"not a controller reference", what, obj.GetName())
			return
		}
		assert.True(t, *ref.Controller,
			"%s %s names the DingoNode as a non-controller owner",
			what, obj.GetName())
		return
	}
	t.Errorf("%s %s has no owner reference to DingoNode %s (UID %s), so "+
		"nothing would ever garbage-collect it; refs: %+v",
		what, obj.GetName(), dn.Name, dn.UID, obj.GetOwnerReferences())
}

// dataPVCName returns the name the StatefulSet controller gives the node's data
// claim: the volumeClaimTemplate name, the StatefulSet name and the ordinal.
// Derived from the live object rather than hardcoded, so renaming the template
// cannot leave this test asserting on a claim that no longer exists.
func dataPVCName(
	t *testing.T,
	sts *appsv1.StatefulSet,
	podName string,
) string {
	t.Helper()
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1,
		"expected exactly one volumeClaimTemplate on StatefulSet %s",
		sts.Name)
	return sts.Spec.VolumeClaimTemplates[0].Name + "-" + podName
}

// TestDingoNodeDeletionReapsChildrenAndRetainsPVC covers the teardown half of
// the node lifecycle, which nothing else in the suite touches.
//
// It has to be an e2e test. Cascade deletion is driven by the garbage collector
// in kube-controller-manager, and envtest runs only an apiserver and etcd — so
// under envtest an ownerReference is an inert annotation and deleting a
// DingoNode leaves every child in place. k3d has a real controller-manager,
// which is why this is the first place the cascade can be observed at all.
//
// Two things are asserted, and the second is the one likely to regress:
//
//  1. Every object the reconciler owns is reaped.
//  2. The data PVC is *not*. The StatefulSet sets no
//     persistentVolumeClaimRetentionPolicy, so Kubernetes defaults both fields
//     to Retain. For a block producer that is right — the volume holds the
//     chain database, and a deleted-and-recreated DingoNode should resync from
//     what is there rather than from Mithril or genesis — but it is currently
//     implicit. Flipping whenDeleted to Delete would start silently destroying
//     chain data. This pins it so that change has to be deliberate.
func TestDingoNodeDeletionReapsChildrenAndRetainsPVC(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "reaps owned children and retains chain data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			h := newHarness(t)
			t.Cleanup(func() {
				if t.Failed() {
					t.Log(h.diagnostics(ctx))
				}
			})

			dn := h.applyDevNet(ctx)
			h.applyDingoNode(ctx, dn)

			// A Ready pod is not what this test asserts, but it is what gives the PVC
			// assertions something to be about: the local-path provisioner binds the
			// claim only once a consumer is scheduled, and "the volume survives" is a
			// hollow guarantee for a volume that was never provisioned or written to.
			// This test deliberately does not wait for a forged block — nothing here
			// disrupts a running node, so the genesis-write discipline the disruption
			// and rotation tests observe does not apply.
			pod := h.waitPodReady(ctx)
			t.Logf("pod %s ready", pod.Name)

			node := h.getNode(ctx)
			children := ownedChildren(node)
			require.NotEmpty(t, children, "the owned-object list is empty")

			// Non-vacuity, first half: the children must be observed *present* before
			// the delete. A test that only checked "absent afterwards" would pass just
			// as happily against an operator that never created them.
			h.waitFor(ctx, childrenExistTimeout,
				"every object owned by the DingoNode to exist",
				func(ctx context.Context) (bool, error) {
					var missing []string
					for _, c := range children {
						obj, err := h.getChild(ctx, c)
						if err != nil {
							return false, err
						}
						if obj == nil {
							missing = append(missing, c.what+" "+c.key.Name)
						}
					}
					if len(missing) > 0 {
						return false, fmt.Errorf("not created yet: %s",
							strings.Join(missing, ", "))
					}
					return true, nil
				})

			for _, c := range children {
				obj, err := h.getChild(ctx, c)
				require.NoError(t, err, "read %s %s", c.what, c.key.Name)
				require.NotNil(t, obj, "%s %s vanished between checks",
					c.what, c.key.Name)
				assertControlledBy(t, obj, node, c.what)
				t.Logf("present before delete: %s %s", c.what, c.key.Name)
			}

			sts := &appsv1.StatefulSet{}
			require.NoError(t, h.client.Get(ctx, h.nodeKey(), sts),
				"read the StatefulSet for its claim template")

			// The retention policy, pinned. The builder sets the field to nil, and a
			// cluster whose StatefulSetAutoDeletePVC feature is on defaults it to
			// {Retain, Retain} server-side — either shape is the behaviour this test
			// wants, and anything else is a deliberate change to how chain data is
			// treated. whenScaled matters as much as whenDeleted here: an
			// ActiveStandby node scaled back to one replica must not lose the standby's
			// database.
			retain := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
			if p := sts.Spec.PersistentVolumeClaimRetentionPolicy; p != nil {
				assert.Equal(t, retain, p.WhenDeleted,
					"the data PVC must outlive the StatefulSet; it holds the chain "+
						"database")
				assert.Equal(t, retain, p.WhenScaled,
					"the data PVC must outlive a scale-down; it holds the chain "+
						"database")
			}

			pvcKey := types.NamespacedName{
				Name:      dataPVCName(t, sts, h.podKey().Name),
				Namespace: h.namespace,
			}
			pvc := &corev1.PersistentVolumeClaim{}
			require.NoError(t, h.client.Get(ctx, pvcKey, pvc),
				"read the data PVC %s", pvcKey.Name)
			pvcUID := pvc.UID
			require.NotEmpty(t, pvcUID, "the data PVC has no UID")
			// The other half of the retention mechanism, and the part a policy flip
			// would change: with whenDeleted=Retain the StatefulSet controller puts no
			// ownerReference on the claim, so the garbage collector never considers it.
			assert.Empty(t, pvc.OwnerReferences,
				"the data PVC carries owner references, so the garbage collector "+
					"will reap it once its owner goes: %+v", pvc.OwnerReferences)
			t.Logf("data PVC %s UID %s phase %s",
				pvcKey.Name, pvcUID, pvc.Status.Phase)

			// Delete the DingoNode, not the namespace. The harness's own cleanup
			// deletes the namespace, which reaps everything regardless and would prove
			// nothing about ownership; the cascade has to be observed inside a
			// still-live namespace.
			require.NoError(t, h.client.Delete(ctx, node),
				"delete DingoNode %s/%s", h.namespace, nodeName)

			h.waitFor(ctx, childGCTimeout, "the DingoNode itself to disappear",
				func(ctx context.Context) (bool, error) {
					_, err := h.getNodeErr(ctx)
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				})

			// Non-vacuity, second half: the same list, now required to be gone. GC is
			// asynchronous, so this polls to an explicit deadline rather than asserting
			// straight after the delete call.
			h.waitFor(ctx, childGCTimeout,
				"every object owned by the DingoNode to be garbage-collected",
				func(ctx context.Context) (bool, error) {
					var remaining []string
					for _, c := range children {
						obj, err := h.getChild(ctx, c)
						if err != nil {
							return false, err
						}
						if obj != nil {
							remaining = append(remaining, c.what+" "+c.key.Name)
						}
					}
					if len(remaining) > 0 {
						return false, fmt.Errorf("still present: %s",
							strings.Join(remaining, ", "))
					}
					return true, nil
				})
			for _, c := range children {
				t.Logf("reaped after delete: %s %s", c.what, c.key.Name)
			}

			// The pod goes with its StatefulSet; check it explicitly, because it is the
			// only child that is owned transitively rather than by the DingoNode.
			h.waitFor(ctx, childGCTimeout,
				fmt.Sprintf("pod %s to be reaped with its StatefulSet",
					h.podKey().Name),
				func(ctx context.Context) (bool, error) {
					p, err := h.podOrNil(ctx)
					return p == nil, err
				})

			// And the volume that must not go.
			survived := &corev1.PersistentVolumeClaim{}
			require.NoError(t, h.client.Get(ctx, pvcKey, survived),
				"the data PVC must survive deleting the DingoNode; it holds the "+
					"chain database")
			assert.Equal(t, pvcUID, survived.UID,
				"the surviving claim is a different object than the one the node ran "+
					"on")
			assert.True(t, survived.DeletionTimestamp.IsZero(),
				"the data PVC is marked for deletion: %v", survived.DeletionTimestamp)

			// Keep watching for a window: a claim that acquired an ownerReference
			// would be reaped shortly after its owner, not instantly, so one Get is
			// not enough to distinguish Retain from Delete.
			assert.Never(t, func() bool {
				got := &corev1.PersistentVolumeClaim{}
				if err := h.client.Get(ctx, pvcKey, got); err != nil {
					return true
				}
				return got.UID != pvcUID || !got.DeletionTimestamp.IsZero()
			}, pvcRetainWindow, pvcRetainPoll,
				"the data PVC %s was deleted or replaced after the DingoNode went "+
					"away", pvcKey.Name)
			t.Logf("data PVC %s retained (UID %s unchanged)", pvcKey.Name, pvcUID)
		})
	}
}
