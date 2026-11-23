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
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func agentLinkOf(t *testing.T, sts *appsv1.StatefulSet) string {
	t.Helper()
	address, pin, ok := resources.SigningAgentLink(sts.Spec.Template.Spec)
	require.True(t, ok, "the template has no agent address")
	return address + "|" + pin
}

// A producer never has pods on two agents at once: two agents can hold the
// pool's key with independent guards, and each would sign a different header
// for one slot. Moving to another agent shrinks the producer to one pod on the
// old template, moves that pod, and only then scales back up.
func TestUpsertStatefulSetMovesProducersToAnotherAgentThroughOnePod(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*dingov1alpha1.SigningAgentSpec)
	}{
		{"address", func(sa *dingov1alpha1.SigningAgentSpec) {
			sa.Address = "kes-agent-b.signing.svc:7000"
		}},
		{"server key pin", func(sa *dingov1alpha1.SigningAgentSpec) {
			sa.Plaintext = false
			sa.ServerSPKISHA256 = strings.Repeat("ab", 32)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			live := agentProducer(2)
			dn := agentProducer(2)
			dn.UID = "uid-bp"
			tt.change(dn.Spec.BlockProducer.SigningAgent)
			sts := ownedWorkload(dn, live, conflictPoolID, 2)
			sts.Generation = 3
			sts.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: 3, Replicas: 2,
				CurrentRevision: "bp-a", UpdateRevision: "bp-a",
			}
			oldLink := agentLinkOf(t, sts)
			c := conflictClient(t, dn, sts)
			r := reconcilerFor(c)
			key := client.ObjectKeyFromObject(dn)
			apply := func() appsv1.StatefulSet {
				t.Helper()
				_, err := r.upsertStatefulSet(ctx, dn, resources.RenderOptions{
					Replicas: desiredReplicas(dn), MountKeys: true,
				}, false)
				require.NoError(t, err)
				var got appsv1.StatefulSet
				require.NoError(t, c.Get(ctx, key, &got))
				require.NotNil(t, got.Spec.Replicas)
				return got
			}
			setStatus := func(st appsv1.StatefulSetStatus) {
				t.Helper()
				var got appsv1.StatefulSet
				require.NoError(t, c.Get(ctx, key, &got))
				got.Status = st
				require.NoError(t, c.Status().Update(ctx, &got))
			}

			// Two pods run the old agent: shrink first, template unchanged.
			got := apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)
			assert.Equal(t, oldLink, agentLinkOf(t, &got),
				"the template must not move while two pods run the old agent")

			// The second pod is still terminating.
			setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: 3, Replicas: 2,
				CurrentRevision: "bp-a", UpdateRevision: "bp-a",
			})
			got = apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)
			assert.Equal(t, oldLink, agentLinkOf(t, &got),
				"the template must wait until the second pod is gone")

			// One pod left: it moves, and nothing is added beside it.
			setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: 3, Replicas: 1,
				CurrentRevision: "bp-a", UpdateRevision: "bp-a",
			})
			got = apply()
			assert.NotEqual(t, oldLink, agentLinkOf(t, &got))
			assert.Equal(t, int32(1), *got.Spec.Replicas,
				"the new template and a second pod must not arrive together")

			// The roll is not finished: still one pod.
			setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 1,
				CurrentRevision: "bp-a", UpdateRevision: "bp-b",
			})
			got = apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)

			// Every pod runs the new template: back to two.
			setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 1,
				CurrentRevision: "bp-b", UpdateRevision: "bp-b",
			})
			got = apply()
			assert.Equal(t, int32(2), *got.Spec.Replicas)
		})
	}
}

// A template change that keeps the agent rolls several pods as before.
func TestUpsertStatefulSetKeepsPodsForChangeThatKeepsTheAgent(t *testing.T) {
	t.Parallel()
	live := agentProducer(2)
	dn := agentProducer(2)
	dn.UID = "uid-bp"
	dn.Spec.Image.Tag = "0.94.9"
	sts := ownedWorkload(dn, live, conflictPoolID, 2)
	sts.Generation = 3
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: 3, Replicas: 2,
		CurrentRevision: "bp-a", UpdateRevision: "bp-a",
	}
	c := conflictClient(t, dn, sts)

	_, err := reconcilerFor(c).upsertStatefulSet(
		t.Context(), dn,
		resources.RenderOptions{Replicas: desiredReplicas(dn), MountKeys: true},
		false,
	)
	require.NoError(t, err)

	var got appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(dn), &got))
	assert.Equal(t, int32(2), *got.Spec.Replicas)
	assert.Contains(t, got.Spec.Template.Spec.Containers[0].Image, "0.94.9")
}
