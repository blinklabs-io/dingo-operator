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
	"fmt"
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

// agentForger is forgerNode signing through a plaintext remote agent, with a
// UID so the StatefulSet it creates is controlled by it.
func agentForger(
	name, ns, poolID, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	dn := forgerNode(name, ns, poolID, secret, age)
	dn.UID = types.UID("uid-" + ns + "-" + name)
	dn.Spec.Image.Tag = agentImageTag
	dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:   "kes-agent.signing.svc:7000",
		Plaintext: true,
	}
	return dn
}

// Producers that sign through an agent are fenced like any other pair: the
// operator cannot prove they share one signing authority, profile and slot
// watermark, so the newer is refused, and a running newer is scaled to zero.
func TestReconcileAgentSignedForgersAreFenced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		older  *dingov1alpha1.DingoNode
		newer  *dingov1alpha1.DingoNode
		sameNS bool
	}{
		{
			name:  "same pool in another namespace",
			older: agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour),
			newer: agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0),
		},
		{
			name: "same pool in one namespace",
			older: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys-a", time.Hour,
			),
			newer:  agentForger("bp-2", "ns-a", conflictPoolID, "keys-b", 0),
			sameNS: true,
		},
		{
			name: "same keys Secret name in one namespace",
			older: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys", time.Hour,
			),
			newer:  agentForger("bp-2", "ns-a", otherPoolID, "keys", 0),
			sameNS: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			// The newer runs first, then the older appears.
			c := conflictClient(t, tt.newer)
			r := reconcilerFor(c)
			reconcileNode(t, r, tt.newer)
			require.True(t, forging(t, c, tt.newer))
			require.NoError(t, c.Create(ctx, tt.older))
			reconcileNode(t, r, tt.older)
			reconcileNode(t, r, tt.newer)
			assert.True(t, refusedAsDuplicate(t, c, tt.newer),
				degradedMessage(t, c, tt.newer))
			if tt.sameNS {
				assert.True(t, stoppedAtZero(t, c, tt.newer),
					"a running newer producer is scaled to zero")
				reconcileNode(t, r, tt.older)
				assert.True(t, forging(t, c, tt.older))
				assert.False(t, refusedAsDuplicate(t, c, tt.older),
					degradedMessage(t, c, tt.older))
			}
		})
	}
}

func localForger(
	name, ns, poolID, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	dn := forgerNode(name, ns, poolID, secret, age)
	dn.UID = types.UID("uid-" + ns + "-" + name)
	return dn
}

// ownedWorkload is the StatefulSet the operator renders for render, controlled
// by owner, with the given replicas.
func ownedWorkload(
	owner, render *dingov1alpha1.DingoNode,
	pool string,
	replicas int32,
) *appsv1.StatefulSet {
	sts := resources.BuildStatefulSet(render, resources.RenderOptions{
		Replicas: replicas, MountKeys: true, KeysPool: pool,
	})
	sts.UID = types.UID("sts-" + owner.Namespace + "-" + owner.Name)
	// The operator updates a StatefulSet that has been created.
	sts.CreationTimestamp = metav1.Now()
	sts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dingov1alpha1.GroupVersion.String(),
		Kind:       "DingoNode",
		Name:       owner.Name,
		UID:        owner.UID,
		Controller: new(true),
	}}
	return sts
}

// Two agent-signed producers of one pool in different namespaces, both already
// running, as an upgrade from a release that did not fence them leaves them.
// A pool ID is public and an agent bundle holds no KES key, so nothing shows
// which producer holds custody: both are refused as duplicates, neither
// StatefulSet is stopped, and the message names the other producer and the
// manual step.
func TestReconcileRunningAgentSignedForgersAcrossNamespacesAreRefusedNotStopped(
	t *testing.T,
) {
	t.Parallel()
	older := agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour)
	newer := agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0)
	c := conflictClient(t, older, newer,
		ownedWorkload(older, older, conflictPoolID, 1),
		ownedWorkload(newer, newer, conflictPoolID, 1),
	)
	r := reconcilerFor(c)
	for range 2 {
		reconcileNode(t, r, newer)
		reconcileNode(t, r, older)
	}
	for _, n := range []*dingov1alpha1.DingoNode{older, newer} {
		assert.True(t, refusedAsDuplicate(t, c, n), degradedMessage(t, c, n))
		assert.True(t, forging(t, c, n),
			"%s/%s is not stopped: the operator cannot verify custody",
			n.Namespace, n.Name)
	}
	assert.Contains(t, degradedMessage(t, c, newer), "ns-a/bp")
	assert.Contains(t, degradedMessage(t, c, newer), "stop one of the producers")
	assert.Contains(t, degradedMessage(t, c, older), "ns-b/bp")
	assert.Contains(t, degradedMessage(t, c, older), "stop one of the producers")
}

// runningPair is an upgrade-time duplicate: both agent-signed producers of one
// pool already run, each with the StatefulSet it controls.
func runningPair(
	olderNS, newerNS string,
) (older, newer *dingov1alpha1.DingoNode, objs []client.Object) {
	older = agentForger("bp", olderNS, conflictPoolID, "keys-a", time.Hour)
	newer = agentForger("bp2", newerNS, conflictPoolID, "keys-b", 0)
	return older, newer, []client.Object{
		older, newer,
		ownedWorkload(older, older, conflictPoolID, 1),
		ownedWorkload(newer, newer, conflictPoolID, 1),
	}
}

// Where one party controls both producers (one namespace) or the cluster
// admin lists both namespaces, the newer of a running pair is scaled to zero
// and refused; the older keeps running, whatever the reconcile order.
func TestReconcileRunningAgentSignedPairIsFencedWhereTrusted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		olderNS, newNS  string
		fenceNamespaces []string
	}{
		{"same namespace", "ns-a", "ns-a", nil},
		{"both namespaces listed", "ns-a", "ns-b", []string{"ns-a", "ns-b"}},
	}
	for _, tt := range tests {
		for _, newerFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s newerFirst=%t", tt.name, newerFirst),
				func(t *testing.T) {
					t.Parallel()
					older, newer, objs := runningPair(tt.olderNS, tt.newNS)
					c := conflictClient(t, objs...)
					r := reconcilerFor(c)
					r.FenceNamespaces = tt.fenceNamespaces
					order := []*dingov1alpha1.DingoNode{older, newer}
					if newerFirst {
						order = []*dingov1alpha1.DingoNode{newer, older}
					}
					// The older is never halted by the newer it outranks, even
					// before the newer has been fenced.
					reconcileNode(t, r, older)
					require.True(t, forging(t, c, older),
						"the older producer keeps running beside the newer")
					for range 3 {
						for _, n := range order {
							reconcileNode(t, r, n)
						}
					}
					assert.True(t, stoppedAtZero(t, c, newer),
						"the newer producer is scaled to zero")
					assert.True(t, refusedAsDuplicate(t, c, newer),
						degradedMessage(t, c, newer))
					assert.True(t, forging(t, c, older),
						"the older producer keeps running")
					assert.False(t, refusedAsDuplicate(t, c, older),
						degradedMessage(t, c, older))
				})
		}
	}
}

// A third producer of the pool in an unlisted namespace refuses without
// halting. Whatever order the DingoNodes are listed in, and whatever its age,
// that refusal must not stand in for the halting one the older of a trusted
// pair gives the newer: the older keeps running beside a newer it outranks.
func TestForgerHeldTrustedPairHaltsBesideUntrustedClaim(t *testing.T) {
	t.Parallel()
	for _, age := range []time.Duration{2 * time.Hour, 0} {
		t.Run(age.String(), func(t *testing.T) {
			t.Parallel()
			older, newer, objs := runningPair("ns-a", "ns-b")
			squat := agentForger(
				"squat", "ns-c", conflictPoolID, "squat-keys", age,
			)
			objs = append(objs, squat,
				ownedWorkload(squat, squat, conflictPoolID, 1))
			r := reconcilerFor(conflictClient(t, objs...))
			r.FenceNamespaces = []string{"ns-a", "ns-b"}
			all := []dingov1alpha1.DingoNode{*squat, *older, *newer}
			err := r.forgerHeld(context.Background(), newer, all)
			held, ok := errors.AsType[*forgerHeldError](err)
			require.True(t, ok, "%v", err)
			assert.True(t, held.halt, held.msg)
			assert.Contains(t, held.msg, "ns-a/bp")
		})
	}
}

// Only the workload a DingoNode controls shows it already runs. A stopped
// older producer that a pod merely labelled with its name makes look up must
// wait for the newer of a trusted pair to stop, not start beside it.
func TestForgerHeldStoppedOlderWaitsDespiteLabelledPod(t *testing.T) {
	t.Parallel()
	older := agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour)
	newer := agentForger("bp2", "ns-b", conflictPoolID, "keys-b", 0)
	// The newer's workload mounts no keys, so only heldBy can see it.
	running := resources.BuildStatefulSet(newer, resources.RenderOptions{
		Replicas: 1,
	})
	running.UID = types.UID("sts-ns-b-bp2")
	running.OwnerReferences = ownedWorkload(
		newer, newer, conflictPoolID, 1,
	).OwnerReferences
	stray := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "bp-0", Namespace: "ns-a",
		Labels: map[string]string{resources.NodeLabel: older.Name},
	}}
	r := reconcilerFor(conflictClient(t, older, newer, stray, running,
		ownedWorkload(older, older, conflictPoolID, 0),
	))
	r.FenceNamespaces = []string{"ns-a", "ns-b"}
	all := []dingov1alpha1.DingoNode{*older, *newer}
	err := r.forgerHeld(context.Background(), older, all)
	_, ok := errors.AsType[*forgerHeldError](err)
	require.True(t, ok, "the stopped older is refused while the newer runs: %v",
		err)
	assert.Contains(t, err.Error(), "ns-b/bp2")
}

// One namespace listed is no authority over the other: neither is stopped.
func TestReconcileRunningAgentSignedPairWithOneNamespaceListedIsNotStopped(
	t *testing.T,
) {
	t.Parallel()
	for _, listed := range []string{"ns-a", "ns-b"} {
		t.Run(listed, func(t *testing.T) {
			t.Parallel()
			older, newer, objs := runningPair("ns-a", "ns-b")
			c := conflictClient(t, objs...)
			r := reconcilerFor(c)
			r.FenceNamespaces = []string{listed}
			for range 2 {
				reconcileNode(t, r, newer)
				reconcileNode(t, r, older)
			}
			for _, n := range []*dingov1alpha1.DingoNode{older, newer} {
				assert.True(t, refusedAsDuplicate(t, c, n),
					degradedMessage(t, c, n))
				assert.True(t, forging(t, c, n),
					"%s/%s is not stopped", n.Namespace, n.Name)
			}
		})
	}
}

// An older agent-signed node in another namespace that names the same public
// pool ID, with a running StatefulSet it controls, is no authority over the
// newer producer: the newer keeps its replica and is only refused.
func TestReconcileUntrustedOlderAgentClaimDoesNotStopNewer(t *testing.T) {
	t.Parallel()
	older := agentForger("squat", "ns-a", conflictPoolID, "squat-keys", time.Hour)
	newer := agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0)
	c := conflictClient(t, older, newer,
		ownedWorkload(older, older, conflictPoolID, 1),
		ownedWorkload(newer, newer, conflictPoolID, 1),
	)
	r := reconcilerFor(c)
	for range 2 {
		reconcileNode(t, r, newer)
	}
	assert.True(t, refusedAsDuplicate(t, c, newer),
		degradedMessage(t, c, newer))
	assert.True(t, forging(t, c, newer),
		"a claim to a public pool ID does not stop a running producer")
}

// A producer that never ran cannot hold a pool: an older agent-signed
// producer with no workload does not stop a newer running one in another
// namespace.
func TestReconcileAgentSignedForgerKeepsRunningBesideIdleOlder(t *testing.T) {
	t.Parallel()
	older := agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour)
	newer := agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0)
	c := conflictClient(t, older, newer,
		ownedWorkload(newer, newer, conflictPoolID, 1),
	)
	r := reconcilerFor(c)
	reconcileNode(t, r, newer)
	assert.True(t, forging(t, c, newer))
}

// A stopped node whose template is held at a local-KES render of pool P keeps
// that template while its spec, now agent-signed for pool Q, finds its new
// keys Secret missing. The fence admits Q, but the held template would start
// a local signer on P's keys beside the producer that runs P, so the node
// stays at zero replicas.
func TestReconcileStoppedLocalTemplateForOtherPoolStaysStopped(t *testing.T) {
	t.Parallel()
	runner := localForger("runner", "ns-a", conflictPoolID, "k-run", time.Hour)
	switching := agentForger("sw", "ns-b", otherPoolID, "k-new", 0)
	prior := localForger("sw", "ns-b", conflictPoolID, "k-old", 0)
	c := conflictClient(t, runner, switching,
		ownedWorkload(runner, runner, conflictPoolID, 1),
		ownedWorkload(switching, prior, conflictPoolID, 0),
	)
	r := reconcilerFor(c)
	for range 2 {
		reconcileNode(t, r, switching)
		var sts appsv1.StatefulSet
		require.NoError(t, c.Get(
			context.Background(), client.ObjectKeyFromObject(switching), &sts,
		))
		require.True(t, forgesWithLocalKES(sts.Spec.Template.Spec),
			"the template is held at the local-KES render")
		assert.True(t, stoppedAtZero(t, c, switching),
			"a held local-KES template must not restart")
	}
	assert.True(t, forging(t, c, runner))
}

// stoppedAtZero reports whether n's StatefulSet still exists with zero
// replicas. Unlike !forging, it fails when the StatefulSet was deleted
// instead of stopped.
func stoppedAtZero(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) bool {
	t.Helper()
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &sts,
	), "the StatefulSet must remain")
	return sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0
}
