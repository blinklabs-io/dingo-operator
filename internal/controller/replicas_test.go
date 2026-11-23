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
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// podFetcher serves a scrape per pod name; a pod it has none for fails.
type podFetcher map[string]*forgestatus.Status

func (f podFetcher) Fetch(
	_ context.Context,
	url string,
) (*forgestatus.Status, error) {
	pod, _, _ := strings.Cut(strings.TrimPrefix(url, "http://"), ".")
	st, ok := f[pod]
	if !ok {
		return nil, errors.New("connection refused")
	}
	return st, nil
}

// agentProducer is a block producer signing through an agent with n pods.
func agentProducer(n int32) *dingov1alpha1.DingoNode {
	dn := agentForger("bp", "ns", conflictPoolID, "keys-a", 0)
	dn.Spec.Replicas = new(n)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 5
	return dn
}

func kesScrape(remaining, opcertStart, counter int64) *forgestatus.Status {
	return &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		HasTipSlot: true, TipSlot: 1000 + remaining,
		HasKESData: true, CurrentKESPeriod: 900,
		RemainingKESPeriods:  remaining,
		OpCertStartKESPeriod: opcertStart, OpCertExpiryKESPeriod: 962,
		OpCertCounter: counter, HasOpCertCounter: true,
	}
}

func TestDesiredReplicasBlockProducer(t *testing.T) {
	t.Parallel()
	keyFile := forgerNode("bp", "ns", conflictPoolID, "keys-a", 0)
	keyFile.Spec.Replicas = new(int32(2))
	relay := relayNodeForStatus("relay", "ns")
	relay.Spec.Replicas = new(int32(3))
	tests := []struct {
		name string
		dn   *dingov1alpha1.DingoNode
		want int32
	}{
		{"key file producer ignores replicas", keyFile, 1},
		{"agent producer honours replicas", agentProducer(2), 2},
		{"agent producer scaled to zero", agentProducer(0), 0},
		{"relay honours replicas", relay, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, desiredReplicas(tt.dn))
		})
	}
	unset := agentProducer(2)
	unset.Spec.Replicas = nil
	assert.Equal(t, int32(1), desiredReplicas(unset))
}

func TestValidateSpecProducerReplicas(t *testing.T) {
	t.Parallel()
	keyFile := func(n int32) *dingov1alpha1.DingoNode {
		dn := forgerNode("bp", "ns", conflictPoolID, "keys-a", 0)
		dn.Spec.Replicas = new(n)
		return dn
	}
	tests := []struct {
		name    string
		dn      *dingov1alpha1.DingoNode
		wantErr bool
	}{
		{"key file producer, one pod", keyFile(1), false},
		{"key file producer, two pods", keyFile(2), true},
		{"agent producer, two pods", agentProducer(2), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateSpec(tt.dn)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "signingAgent")
		})
	}
}

func replicaStatusNamed(
	t *testing.T,
	dn *dingov1alpha1.DingoNode,
	name string,
) dingov1alpha1.ReplicaStatus {
	t.Helper()
	for _, rs := range dn.Status.Replicas {
		if rs.Name == name {
			return rs
		}
	}
	require.Failf(t, "no replica status", "%s in %v", name, dn.Status.Replicas)
	return dingov1alpha1.ReplicaStatus{}
}

// One pod that cannot be scraped leaves the aggregate as it is for any
// multi-replica node, and the per-pod status says which pod it was.
func TestRefreshForgeStatusNamesTheUnscrapedReplica(t *testing.T) {
	t.Parallel()
	dn := agentProducer(2)
	dn.Status.Tip = dingov1alpha1.TipStatus{Slot: 5}
	r := &DingoNodeReconciler{ForgeStatus: podFetcher{
		"bp-0": kesScrape(40, 900, 7),
	}}

	r.refreshForgeStatus(context.Background(), dn)

	require.Len(t, dn.Status.Replicas, 2)
	ok := replicaStatusNamed(t, dn, "bp-0")
	assert.True(t, ok.Scraped)
	require.NotNil(t, ok.TipSlot)
	assert.Equal(t, int64(1040), *ok.TipSlot)
	require.NotNil(t, ok.RemainingKESPeriods)
	assert.Equal(t, int64(40), *ok.RemainingKESPeriods)
	require.NotNil(t, ok.OpCertKESPeriod)
	assert.Equal(t, int64(900), *ok.OpCertKESPeriod)
	failed := replicaStatusNamed(t, dn, "bp-1")
	assert.Equal(t, dingov1alpha1.ReplicaStatus{Name: "bp-1"}, failed)

	assert.Equal(t, dingov1alpha1.TipStatus{}, dn.Status.Tip)
	pipeline := pipelineCond(t, dn)
	assert.Equal(t, metav1.ConditionUnknown, pipeline.Status)
	assert.Equal(t, "MetricsUnavailable", pipeline.Reason)
	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionUnknown, due.Status)
}

func TestRefreshForgeStatusReportsPipelineFlagsPerReplica(t *testing.T) {
	t.Parallel()
	dn := agentProducer(2)
	halted := kesScrape(40, 900, 7)
	halted.PipelineHalted = true
	r := &DingoNodeReconciler{ForgeStatus: podFetcher{
		"bp-0": kesScrape(40, 900, 7), "bp-1": halted,
	}}

	r.refreshForgeStatus(context.Background(), dn)

	assert.False(t, replicaStatusNamed(t, dn, "bp-0").PipelineHalted)
	assert.True(t, replicaStatusNamed(t, dn, "bp-1").PipelineHalted)
	assert.Equal(t, metav1.ConditionFalse, pipelineCond(t, dn).Status)
}

// Pods differ while a rotation rolls through them. status.kes reports the one
// with the fewest periods left, and the loaded counter the lowest, so neither
// confirms a rotation a pod has not taken.
func TestRefreshForgeStatusReportsTheWorstReplicaKES(t *testing.T) {
	t.Parallel()
	for _, order := range [][2]string{{"bp-0", "bp-1"}, {"bp-1", "bp-0"}} {
		t.Run(order[0]+" is the old one", func(t *testing.T) {
			t.Parallel()
			dn := agentProducer(2)
			r := &DingoNodeReconciler{ForgeStatus: podFetcher{
				order[0]: kesScrape(3, 900, 7),
				order[1]: kesScrape(40, 936, 8),
			}}

			r.refreshForgeStatus(context.Background(), dn)

			assert.Equal(t, int64(3), dn.Status.KES.RemainingPeriods)
			assert.Equal(t, int64(900), dn.Status.KES.OpCertKESPeriod)
			require.NotNil(t, dn.Status.OpCert.LoadedCounter)
			assert.Equal(t, int64(7), *dn.Status.OpCert.LoadedCounter)
			due := meta.FindStatusCondition(
				dn.Status.Conditions, condRotationDue,
			)
			require.NotNil(t, due)
			assert.Equal(t, metav1.ConditionTrue, due.Status)
			assert.Equal(t, int64(40),
				*replicaStatusNamed(t, dn, order[1]).RemainingKESPeriods)
		})
	}
}

// A pod that reports no KES state, as a new one does while it starts, leaves
// rotation Unknown rather than judged from the pod that does.
func TestRefreshForgeStatusReplicaWithoutKESKeepsRotationUnknown(
	t *testing.T,
) {
	t.Parallel()
	dn := agentProducer(2)
	young := kesScrape(0, 0, 0)
	young.HasKESData, young.HasOpCertCounter = false, false
	r := &DingoNodeReconciler{ForgeStatus: podFetcher{
		"bp-0": kesScrape(40, 900, 7), "bp-1": young,
	}}

	r.refreshForgeStatus(context.Background(), dn)

	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionUnknown, due.Status)
	assert.Equal(t, "KESMetricsUnavailable", due.Reason)
	assert.Nil(t, replicaStatusNamed(t, dn, "bp-1").RemainingKESPeriods)
}

func TestRefreshForgeStatusDropsReplicasOnScaleToZero(t *testing.T) {
	t.Parallel()
	dn := agentProducer(0)
	dn.Status.Replicas = []dingov1alpha1.ReplicaStatus{{Name: "bp-1"}}
	r := &DingoNodeReconciler{ForgeStatus: podFetcher{}}

	r.refreshForgeStatus(context.Background(), dn)

	assert.Empty(t, dn.Status.Replicas)
}

func TestRefreshForgeStatusRelayKeepsNoReplicaStatus(t *testing.T) {
	t.Parallel()
	dn := relayNodeForStatus("relay", "ns")
	dn.Spec.Replicas = new(int32(2))
	r := &DingoNodeReconciler{ForgeStatus: podFetcher{
		"relay-0": tipStatus(1000), "relay-1": tipStatus(1001),
	}}

	r.refreshForgeStatus(context.Background(), dn)

	assert.Empty(t, dn.Status.Replicas)
	assert.Equal(t, metav1.ConditionTrue, pipelineCond(t, dn).Status)
}

// A pool list producer's pods each stage their own pools, and the unlabelled
// series of each belong to the pool that pod stages.
func TestRefreshForgeStatusAttributesPoolsPerReplica(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	a, b := pools[0], pools[1]
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.Replicas = new(int32(2))
	for _, id := range []string{a.id, b.id} {
		poolStatus(dn, id).KeysValid = true
	}
	podFor := func(name, uid, pool string) *corev1.Pod {
		p := scrapedPoolPod(uid, pool)
		p.Name = name
		return p
	}
	c := conflictClient(t, dn,
		runningPoolsStatefulSet("bp", "ns", a.id, b.id),
		podFor("bp-0", "uid-0", a.id), podFor("bp-1", "uid-1", b.id),
	)
	r := poolReconciler(c)
	r.ForgeStatus = podFetcher{
		"bp-0": kesScrape(7, 900, 5),
		"bp-1": kesScrape(3, 900, 6),
	}

	r.refreshForgeStatus(context.Background(), dn)

	pa, pb := poolStatus(dn, a.id), poolStatus(dn, b.id)
	assert.Equal(t, int64(7), pa.KES.RemainingPeriods)
	require.NotNil(t, pa.LoadedCounter)
	assert.Equal(t, int64(5), *pa.LoadedCounter)
	assert.Equal(t, int64(3), pb.KES.RemainingPeriods)
	require.NotNil(t, pb.LoadedCounter)
	assert.Equal(t, int64(6), *pb.LoadedCounter)
	assert.Equal(t, int64(3), dn.Status.KES.RemainingPeriods)
}

// Pods that stage the same pools report it labelled; each pool keeps the
// metrics of the pod furthest behind on it.
func TestRefreshForgeStatusMergesLabelledPoolsAcrossReplicas(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	a, b := pools[0], pools[1]
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.Replicas = new(int32(2))
	for _, id := range []string{a.id, b.id} {
		poolStatus(dn, id).KeysValid = true
	}
	scrape := func(ra, rb int64) *forgestatus.Status {
		st := kesScrape(0, 0, 0)
		st.HasKESData, st.HasOpCertCounter = false, false
		st.Pools = map[string]*forgestatus.PoolMetrics{
			poolLabelOf(a.id): {
				HasKESData: true, RemainingKESPeriods: ra,
				HasOpCertCounter: true, OpCertCounter: 4,
			},
			poolLabelOf(b.id): {
				HasKESData: true, RemainingKESPeriods: rb,
				HasOpCertCounter: true, OpCertCounter: 9,
			},
		}
		return st
	}
	c := conflictClient(t, dn,
		runningPoolsStatefulSet("bp", "ns", a.id, b.id),
		scrapedPoolPodNamed("bp-0", "uid-0", a.id, b.id),
		scrapedPoolPodNamed("bp-1", "uid-1", a.id, b.id),
	)
	r := poolReconciler(c)
	r.ForgeStatus = podFetcher{"bp-0": scrape(10, 20), "bp-1": scrape(5, 25)}

	r.refreshForgeStatus(context.Background(), dn)

	assert.Equal(t, int64(5), poolStatus(dn, a.id).KES.RemainingPeriods)
	assert.Equal(t, int64(20), poolStatus(dn, b.id).KES.RemainingPeriods)
	assert.Equal(t, int64(5), dn.Status.KES.RemainingPeriods)
}

func scrapedPoolPodNamed(name, uid string, pools ...string) *corev1.Pod {
	p := scrapedPoolPod(uid, strings.Join(pools, ","))
	p.Name = name
	return p
}

// Pods of one DingoNode share keys by design: neither is another forger of the
// node's pool, so the fence neither refuses nor stops the node.
func TestReconcileSiblingReplicasAreNotRivalForgers(t *testing.T) {
	t.Parallel()
	dn := agentProducer(2)
	dn.UID = types.UID("uid-bp")
	sts := ownedWorkload(dn, dn, conflictPoolID, 2)
	pods := make([]client.Object, 0, 2)
	for _, name := range []string{"bp-0", "bp-1"} {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "ns", UID: types.UID("uid-" + name),
				Labels:      sts.Spec.Template.Labels,
				Annotations: sts.Spec.Template.Annotations,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "StatefulSet",
					Name: sts.Name, UID: sts.UID, Controller: new(true),
				}},
			},
			Spec:   sts.Spec.Template.Spec,
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}
	c := conflictClient(t, append(pods, dn, sts)...)
	r := reconcilerFor(c)

	mine := forgerIdentity{pools: []string{conflictPoolID}}
	require.NoError(t, r.orphanForger(context.Background(), dn, mine))
	rolls, err := r.rollsPastUnknownPool(context.Background(), dn, mine)
	require.NoError(t, err)
	assert.True(t, rolls)

	for range 2 {
		reconcileNode(t, r, dn)
	}
	assert.False(t, refusedAsDuplicate(t, c, dn))
	var got appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(dn), &got,
	))
	require.NotNil(t, got.Spec.Replicas)
	assert.Equal(t, int32(2), *got.Spec.Replicas)
}

// A producer scales above one pod only once no pod of its StatefulSet can sign
// with a local KES key. Scaling up and switching to an agent in one edit would
// otherwise start the new ordinal on the agent while the old pod still forges
// with its kes.skey, and a template held at a local-KES render would start a
// second pod on the same key.
func TestUpsertStatefulSetHoldsScaleUpBehindLocalSigner(t *testing.T) {
	t.Parallel()
	keyFile := forgerNode("bp", "ns", conflictPoolID, "keys-a", 0)
	tests := []struct {
		name         string
		live         *dingov1alpha1.DingoNode
		liveReplicas int32
		rolling      bool
		keepTemplate bool
		want         int32
	}{
		{"local signer switches to the agent", keyFile, 1, false, false, 1},
		{"template held at a local signer", keyFile, 1, false, true, 1},
		{"agent pods still rolling", agentProducer(1), 1, true, false, 1},
		{"agent pods rolling keep their count", agentProducer(2), 2, true, false, 2},
		{"agent pods settled", agentProducer(1), 1, false, false, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dn := agentProducer(2)
			dn.UID = types.UID("uid-bp")
			sts := ownedWorkload(dn, tt.live, conflictPoolID, tt.liveReplicas)
			sts.Generation = 3
			sts.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: 3,
				Replicas:           tt.liveReplicas,
				CurrentRevision:    "bp-a",
				UpdateRevision:     "bp-a",
			}
			if tt.rolling {
				sts.Status.UpdateRevision = "bp-b"
			}
			c := conflictClient(t, dn, sts)
			r := reconcilerFor(c)

			_, err := r.upsertStatefulSet(
				context.Background(), dn,
				resources.RenderOptions{
					Replicas: desiredReplicas(dn), MountKeys: true,
				},
				tt.keepTemplate,
			)
			require.NoError(t, err)

			var got appsv1.StatefulSet
			require.NoError(t, c.Get(
				context.Background(), client.ObjectKeyFromObject(dn), &got,
			))
			require.NotNil(t, got.Spec.Replicas)
			assert.Equal(t, tt.want, *got.Spec.Replicas)
		})
	}
}

// A relay holds no key, so a roll does not delay its scaling up.
func TestUpsertStatefulSetScalesRollingRelay(t *testing.T) {
	t.Parallel()
	dn := relayNodeForStatus("relay", "ns")
	dn.UID = types.UID("uid-relay")
	dn.Spec.Replicas = new(int32(3))
	sts := resources.BuildStatefulSet(dn, resources.RenderOptions{Replicas: 1})
	sts.CreationTimestamp = metav1.Now()
	sts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dingov1alpha1.GroupVersion.String(),
		Kind:       "DingoNode", Name: dn.Name, UID: dn.UID,
		Controller: new(true),
	}}
	sts.Generation = 3
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: 3, Replicas: 1,
		CurrentRevision: "relay-a", UpdateRevision: "relay-b",
	}
	c := conflictClient(t, dn, sts)
	r := reconcilerFor(c)

	_, err := r.upsertStatefulSet(
		context.Background(), dn,
		resources.RenderOptions{Replicas: desiredReplicas(dn)}, false,
	)
	require.NoError(t, err)

	var got appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(dn), &got,
	))
	require.NotNil(t, got.Spec.Replicas)
	assert.Equal(t, int32(3), *got.Spec.Replicas)
}
