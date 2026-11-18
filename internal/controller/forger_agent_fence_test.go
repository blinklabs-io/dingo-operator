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
				assert.False(t, forging(t, c, tt.newer),
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
// Neither may stop the other: a pool ID is public, so it is no authority over
// another namespace's workload. The newer stops its own workload instead, so
// exactly one keeps running, and the older is not halted.
func TestReconcileRunningAgentSignedForgersAcrossNamespacesSelfFence(
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
	assert.False(t, forging(t, c, newer), "the newer stops its own workload")
	assert.True(t, refusedAsDuplicate(t, c, newer),
		degradedMessage(t, c, newer))
	assert.True(t, forging(t, c, older), "the older keeps forging")
}

// A producer that never ran cannot hold a pool: an older agent-signed
// producer with no workload does not stop a newer running one in another
// namespace, which only an older workload that runs may do.
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
		assert.False(t, forging(t, c, switching),
			"a held local-KES template must not restart")
	}
	assert.True(t, forging(t, c, runner))
}
