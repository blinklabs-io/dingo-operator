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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func setController(
	obj metav1.Object, apiVersion, kind, name string, uid types.UID,
) {
	isController := true
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		UID:        uid,
		Controller: &isController,
	}})
}

// forgedWorkload returns a pod and a StatefulSet that carry the labels,
// keys volume, and keys-pool annotation of a block producer for pool, in a
// namespace with no DingoNode of that name.
func forgedWorkload(pool string) (*corev1.Pod, *appsv1.StatefulSet) {
	fake := forgerNode("evil", "ns-evil", pool, "keys-evil", 0)
	labels := resources.Labels(fake)
	annotations := map[string]string{resources.KeysPoolAnnotation: pool}
	spec := corev1.PodSpec{Volumes: []corev1.Volume{{
		Name: "block-producer-keys-source",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "keys-evil"},
		},
	}}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evil-0", Namespace: "ns-evil", UID: "evil-pod-uid",
			Labels: labels, Annotations: annotations,
		},
		Spec: spec,
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evil", Namespace: "ns-evil", UID: "evil-sts-uid",
			Labels: labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(1)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels, Annotations: annotations,
				},
				Spec: spec,
			},
		},
	}
	return pod, sts
}

// A workload whose controller chain does not resolve to a live StatefulSet
// controlled by a live DingoNode is not evidence that the operator started it.
// It may hold back a producer that is not running, but must not scale a running
// one to zero.
func TestReconcileForgedOrphanDoesNotHaltForger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name  string
		build func(pool string) []client.Object
	}{
		{"pod with no controller", func(pool string) []client.Object {
			pod, _ := forgedWorkload(pool)
			return []client.Object{pod}
		}},
		{"pod controlled by a missing StatefulSet", func(pool string) []client.Object {
			pod, _ := forgedWorkload(pool)
			setController(pod, "apps/v1", "StatefulSet", "evil", "gone-uid")
			return []client.Object{pod}
		}},
		{"pod controlled by a StatefulSet no DingoNode controls", func(pool string) []client.Object {
			pod, sts := forgedWorkload(pool)
			sts.Spec.Replicas = new(int32)
			setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
			return []client.Object{pod, sts}
		}},
		{"StatefulSet controlled by a missing DingoNode", func(pool string) []client.Object {
			_, sts := forgedWorkload(pool)
			setController(sts, dingov1alpha1.GroupVersion.String(),
				"DingoNode", "evil", "gone-uid")
			return []client.Object{sts}
		}},
		{"StatefulSet with no controller", func(pool string) []client.Object {
			_, sts := forgedWorkload(pool)
			return []client.Object{sts}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			victim := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
			victim.UID = "victim-uid"
			c := conflictClient(t, victim, keysSecret("keys-a", "ns-a", data))
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.Equal(t, int32(1), replicasOf(t, c, victim))

			for _, obj := range tt.build(pool) {
				require.NoError(t, c.Create(ctx, obj))
			}
			reconcileNode(t, r, victim)
			assert.Equal(t, int32(1), replicasOf(t, c, victim),
				"unverified workload must not halt a running forger")
			assert.True(t, refusedAsDuplicate(t, c, victim),
				"the forger still does not roll beside it")
		})
	}
}

// A pod can name any StatefulSet in its namespace as its controller, so even a
// pod whose controller chain resolves does not halt a running forger: here
// the StatefulSet is a live relay's own.
func TestReconcilePodOfOtherStatefulSetDoesNotHalt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, data := poolKeys(t)
	victim := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
	victim.UID = "victim-uid"
	relay := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evil", Namespace: "ns-evil", UID: "relay-uid",
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role: dingov1alpha1.RoleRelay, Network: "preview",
		},
	}
	c := conflictClient(t, victim, keysSecret("keys-a", "ns-a", data))
	r := reconcilerFor(c)
	reconcileNode(t, r, victim)
	require.Equal(t, int32(1), replicasOf(t, c, victim))

	pod, sts := forgedWorkload(pool)
	sts.Labels = nil
	sts.Spec.Template = corev1.PodTemplateSpec{}
	setController(sts, dingov1alpha1.GroupVersion.String(),
		"DingoNode", relay.Name, relay.UID)
	setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
	for _, obj := range []client.Object{relay, sts, pod} {
		require.NoError(t, c.Create(ctx, obj))
	}
	reconcileNode(t, r, victim)
	assert.Equal(t, int32(1), replicasOf(t, c, victim))
	assert.True(t, refusedAsDuplicate(t, c, victim))
}

// A StatefulSet holds a running forger's pool, and halts it, only when it is
// the one a live DingoNode of the same name and UID controls: the operator
// names each node's StatefulSet after it.
func TestReconcileStatefulSetHaltsOnlyUnderItsNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tt := range []struct {
		name    string
		stsName string
		wanted  int32
	}{
		{"StatefulSet named after its DingoNode", "evil", 0},
		{"StatefulSet under another name", "x", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			otherPool, otherData := poolKeys(t)
			victim := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
			victim.UID = "victim-uid"
			moved := forgerNode("evil", "ns-evil", otherPool, "keys-evil", 0)
			moved.UID = "moved-uid"
			c := conflictClient(t, victim, keysSecret("keys-a", "ns-a", data))
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.Equal(t, int32(1), replicasOf(t, c, victim))

			// The template still records the pool it staged, as a template
			// held on a refused keys bundle does after poolId moved.
			_, sts := forgedWorkload(pool)
			sts.Name = tt.stsName
			setController(sts, dingov1alpha1.GroupVersion.String(),
				"DingoNode", moved.Name, moved.UID)
			for _, obj := range []client.Object{
				moved, keysSecret("keys-evil", "ns-evil", otherData), sts,
			} {
				require.NoError(t, c.Create(ctx, obj))
			}
			reconcileNode(t, r, victim)
			assert.Equal(t, tt.wanted, replicasOf(t, c, victim))
			assert.True(t, refusedAsDuplicate(t, c, victim))
		})
	}
}

// A newer DingoNode naming an older forger's pool holds the older one only
// through a workload it controls: a pod that merely carries its name label, or
// a StatefulSet that merely has its name, does not halt the older forger.
func TestReconcileNewerNodeUnownedWorkloadDoesNotHalt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name     string
		owned    bool
		labelPod bool
		wanted   int32
	}{
		{"pod with the name label", false, true, 1},
		{"StatefulSet with the name", false, false, 1},
		{"StatefulSet the newer node controls", true, false, 0},
		// The StatefulSet is at zero; only its pod, which could name any
		// StatefulSet as its controller, still runs.
		{"pod of the StatefulSet the newer node controls", true, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			victim := forgerNode("bp-1", "ns-a", pool, "keys-a", time.Hour)
			victim.UID = "victim-uid"
			newer := forgerNode("bp-x", "ns-evil", pool, "keys-x", 0)
			newer.UID = "newer-uid"
			c := conflictClient(t, victim, keysSecret("keys-a", "ns-a", data))
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.Equal(t, int32(1), replicasOf(t, c, victim))

			objs := []client.Object{newer}
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: newer.Name, Namespace: newer.Namespace,
					UID: "sts-uid",
				},
				Spec: appsv1.StatefulSetSpec{Replicas: new(int32(1))},
			}
			if tt.owned {
				setController(sts, dingov1alpha1.GroupVersion.String(),
					"DingoNode", newer.Name, newer.UID)
			}
			if tt.labelPod {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: "bp-x-0", Namespace: "ns-evil", UID: "pod-uid",
					Labels: resources.Labels(newer),
				}}
				if tt.owned {
					sts.Spec.Replicas = new(int32)
					setController(pod, "apps/v1", "StatefulSet",
						sts.Name, sts.UID)
					objs = append(objs, sts)
				}
				objs = append(objs, pod)
			} else {
				objs = append(objs, sts)
			}
			for _, obj := range objs {
				require.NoError(t, c.Create(ctx, obj))
			}
			reconcileNode(t, r, victim)
			assert.Equal(t, tt.wanted, replicasOf(t, c, victim))
			assert.True(t, refusedAsDuplicate(t, c, victim))
		})
	}
}

// A pod started from a template rendered before the keys-pool annotation
// existed records only the keys checksum. When that checksum equals the
// admitted node's keys, the pod staged the same bundle and holds its pool;
// the checksum counts only on a pod whose controller chain verifies.
func TestReconcileLegacyPodHoldsSameKeysBundle(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		verified bool
	}{
		{"verified pod holds", true},
		{"unverified pod does not", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			poolA, dataA := poolKeys(t)
			poolB, dataB := poolKeys(t)
			moved := forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
			moved.UID = "moved-uid"
			loser := forgerNode("bp-2", "ns", poolA, "keys-c", 0)
			loser.UID = "loser-uid"
			secretA := keysSecret("keys-a", "ns", dataA)
			c := conflictClient(t, moved, loser, secretA,
				keysSecret("keys-c", "ns", dataA),
			)
			r := reconcilerFor(c)
			reconcileNode(t, r, moved)
			require.True(t, forging(t, c, moved))
			pod := stagedPod(t, c, moved)
			delete(pod.Annotations, resources.KeysPoolAnnotation)
			require.NotEmpty(t, pod.Annotations[resources.KeysChecksumAnnotation])
			if tt.verified {
				var sts appsv1.StatefulSet
				require.NoError(t, c.Get(
					ctx, client.ObjectKeyFromObject(moved), &sts,
				))
				require.True(t, metav1.IsControlledBy(&sts, moved))
				setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
			}
			require.NoError(t, c.Create(ctx, pod))

			require.NoError(t, c.Get(
				ctx, client.ObjectKeyFromObject(moved), moved,
			))
			moved.Spec.BlockProducer.PoolID = poolB
			require.NoError(t, c.Get(
				ctx, client.ObjectKeyFromObject(secretA), secretA,
			))
			secretA.Data = dataB
			require.NoError(t, c.Update(ctx, secretA))
			require.NoError(t, c.Update(ctx, moved))
			reconcileNode(t, r, moved)
			require.True(t, forging(t, c, moved))

			reconcileNode(t, r, loser)
			assert.Equal(t, tt.verified, refusedAsDuplicate(t, c, loser))
			assert.Equal(t, !tt.verified, forging(t, c, loser))
		})
	}
}
