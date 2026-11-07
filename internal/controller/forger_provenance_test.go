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

// A StatefulSet no live DingoNode accounts for by its spec does not halt a
// running forger, even when its controller reference names a live DingoNode
// of the same name and UID: whoever creates a StatefulSet sets that reference.
func TestReconcileOrphanStatefulSetDoesNotHalt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tt := range []struct {
		name    string
		stsName string
		wanted  int32
	}{
		{"StatefulSet named after its DingoNode", "evil", 1},
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
		// podUID overrides the UID the pod's controller reference names.
		podUID types.UID
		wanted int32
	}{
		{"pod with the name label", false, true, "", 1},
		{"StatefulSet with the name", false, false, "", 1},
		{"StatefulSet the newer node controls", true, false, "", 0},
		// The StatefulSet is at zero; its pod still runs.
		{"pod of the StatefulSet the newer node controls", true, true, "", 0},
		{"pod naming that StatefulSet with another UID", true, true,
			"other-uid", 1},
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
					uid := sts.UID
					if tt.podUID != "" {
						uid = tt.podUID
					}
					setController(pod, "apps/v1", "StatefulSet",
						sts.Name, uid)
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
// admitted node's keys, the pod staged the same bundle and holds its pool,
// whether or not the StatefulSet that started it is still there.
func TestReconcileLegacyPodHoldsSameKeysBundle(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		verified bool
	}{
		{"pod of its StatefulSet", true},
		{"pod whose StatefulSet is gone", false},
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
			var sts appsv1.StatefulSet
			require.NoError(t, c.Get(
				ctx, client.ObjectKeyFromObject(moved), &sts,
			))
			require.True(t, metav1.IsControlledBy(&sts, moved))
			uid := sts.UID
			if !tt.verified {
				uid = "gone-uid"
			}
			setController(pod, "apps/v1", "StatefulSet", sts.Name, uid)
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
			assert.True(t, refusedAsDuplicate(t, c, loser))
			assert.False(t, forging(t, c, loser))
		})
	}
}

// Two producers already running turn out to share a pool, and the newer one
// reconciles first: it halts itself, but its pod still runs until it stops,
// so the older one must halt too rather than forge beside it.
func TestReconcileOlderHaltsForNewerPodAfterNewerHalts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	poolA, dataA := poolKeys(t)
	poolB, dataB := poolKeys(t)
	older := forgerNode("bp-1", "ns-a", poolA, "keys-a", time.Hour)
	older.UID = "older-uid"
	newer := forgerNode("bp-2", "ns-b", poolB, "keys-b", 0)
	newer.UID = "newer-uid"
	secretB := keysSecret("keys-b", "ns-b", dataB)
	c := conflictClient(t, older, newer,
		keysSecret("keys-a", "ns-a", dataA), secretB,
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, older)
	reconcileNode(t, r, newer)
	require.True(t, forging(t, c, older))
	require.True(t, forging(t, c, newer))
	pod := stagedPod(t, c, newer)
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(newer), &sts))
	setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
	require.NoError(t, c.Create(ctx, pod))

	// The newer node now names the older one's pool and keys.
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secretB), secretB))
	secretB.Data = dataA
	require.NoError(t, c.Update(ctx, secretB))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(newer), newer))
	newer.Spec.BlockProducer.PoolID = poolA
	require.NoError(t, c.Update(ctx, newer))

	reconcileNode(t, r, newer)
	require.True(t, refusedAsDuplicate(t, c, newer))
	require.False(t, forging(t, c, newer))

	reconcileNode(t, r, older)
	assert.True(t, refusedAsDuplicate(t, c, older), "newer pod alive")
	assert.False(t, forging(t, c, older))

	require.NoError(t, c.Delete(ctx, pod))
	reconcileNode(t, r, older)
	assert.False(t, refusedAsDuplicate(t, c, older))
	assert.True(t, forging(t, c, older))
}

// poolKeyBundles returns a pool and two different valid key bundles for it,
// such as an opcert re-issued with the next counter.
func poolKeyBundles(t *testing.T) (string, map[string][]byte, map[string][]byte) {
	t.Helper()
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	first, err := keys.SecretData(3, 5)
	require.NoError(t, err)
	second, err := keys.SecretData(4, 5)
	require.NoError(t, err)
	return hex.EncodeToString(keys.PoolID), first, second
}

// runningPod creates the pod n's StatefulSet would start from its current
// template, controlled by that StatefulSet. A legacy pod has no keys-pool
// annotation, as one started from a template rendered before it existed.
func runningPod(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode, legacy bool,
) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	pod := stagedPod(t, c, n)
	if legacy {
		delete(pod.Annotations, resources.KeysPoolAnnotation)
	}
	require.NotEmpty(t, pod.Annotations[resources.KeysChecksumAnnotation])
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(n), &sts))
	require.True(t, metav1.IsControlledBy(&sts, n))
	setController(pod, "apps/v1", "StatefulSet", sts.Name, sts.UID)
	require.NoError(t, c.Create(ctx, pod))
	return pod
}

// replaceKeys points n at data, naming pool, by replacing its keys Secret's
// contents.
func replaceKeys(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
	pool string, data map[string][]byte,
) {
	t.Helper()
	ctx := context.Background()
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Namespace: n.Namespace, Name: n.Spec.BlockProducer.Keys.SecretRef,
	}, secret))
	secret.Data = data
	require.NoError(t, c.Update(ctx, secret))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(n), n))
	n.Spec.BlockProducer.PoolID = pool
	require.NoError(t, c.Update(ctx, n))
}

func templateAnnotation(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode, key string,
) string {
	t.Helper()
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &sts,
	))
	return sts.Spec.Template.Annotations[key]
}

// A legacy pod whose keys checksum no longer matches its keys Secret staged a
// pool recorded nowhere: here pool A, while its Secret now holds pool B.
// Another producer with a different bundle for pool A must wait until that
// pod stops. Its own node still rolls onto the new keys.
func TestReconcileLegacyPodUnknownPoolHolds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	poolA, dataA, dataA2 := poolKeyBundles(t)
	poolB, dataB := poolKeys(t)
	moved := forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
	moved.UID = "moved-uid"
	loser := forgerNode("bp-2", "ns-2", poolA, "keys-c", 0)
	loser.UID = "loser-uid"
	c := conflictClient(t, moved, loser,
		keysSecret("keys-a", "ns", dataA),
		keysSecret("keys-c", "ns-2", dataA2),
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, moved)
	require.True(t, forging(t, c, moved))
	pod := runningPod(t, c, moved, true)

	replaceKeys(t, c, moved, poolB, dataB)
	reconcileNode(t, r, moved)
	assert.False(t, refusedAsDuplicate(t, c, moved))
	assert.True(t, forging(t, c, moved))
	assert.Equal(t, poolB, templateAnnotation(
		t, c, moved, resources.KeysPoolAnnotation,
	), "its own unknown pod does not hold the node's roll")

	reconcileNode(t, r, loser)
	assert.True(t, refusedAsDuplicate(t, c, loser),
		"the legacy pod may still forge pool A")
	assert.False(t, forging(t, c, loser))

	require.NoError(t, c.Delete(ctx, pod))
	reconcileNode(t, r, loser)
	assert.False(t, refusedAsDuplicate(t, c, loser))
	assert.True(t, forging(t, c, loser))
}

// A pod of unknown staged pool refuses without halting. A running producer
// whose keys still name the pool its own pod staged keeps rolling, as for a
// KES rotation; one whose keys move to another pool waits, still running,
// until that pod stops.
func TestReconcileUnknownPoolPodSparesRunningProducer(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		repoint bool
	}{
		{"same pool rolls", false},
		{"other pool waits", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			poolA, dataA := poolKeys(t)
			poolB, dataB := poolKeys(t)
			poolC, dataC, dataC2 := poolKeyBundles(t)
			poolD, dataD := poolKeys(t)
			moved := forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
			moved.UID = "moved-uid"
			other := forgerNode("bp-2", "ns-2", poolC, "keys-c", 0)
			other.UID = "other-uid"
			c := conflictClient(t, moved, other,
				keysSecret("keys-a", "ns", dataA),
				keysSecret("keys-c", "ns-2", dataC),
			)
			r := reconcilerFor(c)
			reconcileNode(t, r, moved)
			reconcileNode(t, r, other)
			require.True(t, forging(t, c, moved))
			require.True(t, forging(t, c, other))
			pod := runningPod(t, c, moved, true)
			runningPod(t, c, other, false)
			replaceKeys(t, c, moved, poolB, dataB)

			pool, data := poolC, dataC2
			if tt.repoint {
				pool, data = poolD, dataD
			}
			replaceKeys(t, c, other, pool, data)
			reconcileNode(t, r, other)
			assert.Equal(t, int32(1), replicasOf(t, c, other))
			assert.Equal(t, tt.repoint, refusedAsDuplicate(t, c, other))
			want := keysChecksum(keysSecret("keys-c", "ns-2", data))
			if tt.repoint {
				want = keysChecksum(keysSecret("keys-c", "ns-2", dataC))
			}
			assert.Equal(t, want, templateAnnotation(
				t, c, other, resources.KeysChecksumAnnotation,
			))

			require.NoError(t, c.Delete(ctx, pod))
			reconcileNode(t, r, other)
			assert.False(t, refusedAsDuplicate(t, c, other))
			assert.Equal(t, int32(1), replicasOf(t, c, other))
			assert.Equal(t, pool, templateAnnotation(
				t, c, other, resources.KeysPoolAnnotation,
			))
		})
	}
}

// A running producer whose own pod records no pool, but whose recorded keys
// checksum equals its keys Secret's, staged the pool its keys name, and so
// still proceeds past another producer's pod of unknown staged pool.
func TestReconcileUnknownPoolPodSparesLegacyCurrentPod(t *testing.T) {
	t.Parallel()
	poolA, dataA := poolKeys(t)
	poolB, dataB := poolKeys(t)
	poolC, dataC := poolKeys(t)
	moved := forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
	moved.UID = "moved-uid"
	other := forgerNode("bp-2", "ns-2", poolC, "keys-c", 0)
	other.UID = "other-uid"
	c := conflictClient(t, moved, other,
		keysSecret("keys-a", "ns", dataA),
		keysSecret("keys-c", "ns-2", dataC),
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, moved)
	reconcileNode(t, r, other)
	runningPod(t, c, moved, true)
	runningPod(t, c, other, true)
	replaceKeys(t, c, moved, poolB, dataB)

	reconcileNode(t, r, other)
	assert.False(t, refusedAsDuplicate(t, c, other))
	assert.Equal(t, int32(1), replicasOf(t, c, other))
}

// legacyPair starts two block producers on pools A and C, each with a
// template and pod rendered before the keys-pool annotation existed, then
// replaces their keys: x's with pool B, y's with a different bundle for pool
// A. x's pod still stages A and y's still stages C, both recorded nowhere.
func legacyPair(t *testing.T) (
	c client.Client, r *DingoNodeReconciler,
	x, y *dingov1alpha1.DingoNode, xPod, yPod *corev1.Pod,
	poolA, poolB string, yKeys *corev1.Secret,
) {
	t.Helper()
	poolA, dataA, dataA2 := poolKeyBundles(t)
	poolB, dataB := poolKeys(t)
	poolC, dataC := poolKeys(t)
	x = forgerNode("bp-1", "ns", poolA, "keys-a", time.Hour)
	x.UID = "x-uid"
	y = forgerNode("bp-2", "ns-2", poolC, "keys-c", 0)
	y.UID = "y-uid"
	c = conflictClient(t, x, y,
		keysSecret("keys-a", "ns", dataA),
		keysSecret("keys-c", "ns-2", dataC),
	)
	r = reconcilerFor(c)
	reconcileNode(t, r, x)
	reconcileNode(t, r, y)
	require.True(t, forging(t, c, x))
	require.True(t, forging(t, c, y))
	for _, n := range []*dingov1alpha1.DingoNode{x, y} {
		var sts appsv1.StatefulSet
		require.NoError(t, c.Get(
			context.Background(), client.ObjectKeyFromObject(n), &sts,
		))
		delete(sts.Spec.Template.Annotations, resources.KeysPoolAnnotation)
		require.NoError(t, c.Update(context.Background(), &sts))
	}
	xPod = runningPod(t, c, x, true)
	yPod = runningPod(t, c, y, true)
	replaceKeys(t, c, x, poolB, dataB)
	replaceKeys(t, c, y, poolA, dataA2)
	return c, r, x, y, xPod, yPod, poolA, poolB,
		keysSecret("keys-c", "ns-2", dataA2)
}

// Two legacy producers whose keys change in the same window each hold a pod
// of unknown staged pool. y's keys now name pool A, which x's pod may still
// forge, so neither rolls past the other's pod; both keep running on the keys
// they staged.
func TestReconcileLegacyKeyChangesOntoUnknownPoolHold(t *testing.T) {
	t.Parallel()
	c, r, x, y, xPod, yPod, _, _, _ := legacyPair(t)
	xSum := xPod.Annotations[resources.KeysChecksumAnnotation]
	ySum := yPod.Annotations[resources.KeysChecksumAnnotation]

	reconcileNode(t, r, x)
	reconcileNode(t, r, y)
	for n, sum := range map[*dingov1alpha1.DingoNode]string{
		x: xSum, y: ySum,
	} {
		assert.True(t, refusedAsDuplicate(t, c, n), n.Name)
		assert.Equal(t, int32(1), replicasOf(t, c, n),
			"%s: an unknown pod refuses without halting", n.Name)
		assert.Equal(t, sum, templateAnnotation(
			t, c, n, resources.KeysChecksumAnnotation,
		), "%s: the template does not roll", n.Name)
	}
}

// Scaling one of two producers that hold each other to zero lets the other
// roll. The scaled one stays at zero while refused, and resumes on its new
// keys, with its pool recorded, once the other's old pod has stopped.
func TestReconcileLegacyHoldRecoversByScalingDown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c, r, x, y, xPod, yPod, poolA, poolB, yKeys := legacyPair(t)
	reconcileNode(t, r, x)
	reconcileNode(t, r, y)
	require.True(t, refusedAsDuplicate(t, c, x))
	require.True(t, refusedAsDuplicate(t, c, y))

	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(y), &sts))
	sts.Spec.Replicas = new(int32)
	require.NoError(t, c.Update(ctx, &sts))
	reconcileNode(t, r, y)
	assert.True(t, refusedAsDuplicate(t, c, y))
	assert.Equal(t, int32(0), replicasOf(t, c, y),
		"a refused producer is not scaled back up")
	require.NoError(t, c.Delete(ctx, yPod))

	reconcileNode(t, r, x)
	assert.False(t, refusedAsDuplicate(t, c, x))
	assert.Equal(t, poolB, templateAnnotation(
		t, c, x, resources.KeysPoolAnnotation,
	))
	reconcileNode(t, r, y)
	assert.True(t, refusedAsDuplicate(t, c, y),
		"x's old pod may still forge pool A")
	assert.Equal(t, int32(0), replicasOf(t, c, y))

	require.NoError(t, c.Delete(ctx, xPod))
	runningPod(t, c, x, false)
	reconcileNode(t, r, y)
	assert.False(t, refusedAsDuplicate(t, c, y))
	assert.Equal(t, int32(1), replicasOf(t, c, y))
	assert.Equal(t, poolA, templateAnnotation(
		t, c, y, resources.KeysPoolAnnotation,
	))
	assert.Equal(t, keysChecksum(yKeys), templateAnnotation(
		t, c, y, resources.KeysChecksumAnnotation,
	))
}
