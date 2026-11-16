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

// agentPoolList is poolListNode with a UID, so the StatefulSet it creates is
// controlled by it.
func agentPoolList(
	name, ns string,
	age time.Duration,
	pools ...testPool,
) *dingov1alpha1.DingoNode {
	dn := poolListNode(name, ns, age, pools...)
	dn.UID = types.UID("uid-" + ns + "-" + name)
	return dn
}

// poolSecretsIn returns the keys Secrets of pools in each namespace.
func poolSecretsIn(
	t *testing.T,
	pools []testPool,
	namespaces ...string,
) []client.Object {
	t.Helper()
	var objs []client.Object
	for _, ns := range namespaces {
		for _, p := range pools {
			objs = append(objs, poolSecret(t, ns, p, 0))
		}
	}
	return objs
}

// stagesKeys fails unless n's StatefulSet mounts keys, so a test that expects
// it to forge cannot pass on a template that stages nothing.
func stagesKeys(t *testing.T, c client.Client, n *dingov1alpha1.DingoNode) {
	t.Helper()
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &sts,
	))
	assert.NotEmpty(t, resources.KeysSecretNames(sts.Spec.Template.Spec),
		"%s/%s mounts no keys", n.Namespace, n.Name)
}

// Pool-list producers sign every pool through the agent, so two of them that
// share a pool are not fenced against each other.
func TestReconcilePoolListsSharingAnAgentDoNotFence(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b", "c")
	for _, tt := range []struct {
		name          string
		first, second *dingov1alpha1.DingoNode
	}{
		{
			name: "same namespace",
			first: agentPoolList(
				"bp-1", "ns-a", time.Hour, pools[0], pools[1],
			),
			second: agentPoolList(
				"bp-2", "ns-a", 0, pools[1], pools[2],
			),
		},
		{
			name: "another namespace",
			first: agentPoolList(
				"bp", "ns-a", time.Hour, pools[0], pools[1],
			),
			second: agentPoolList("bp", "ns-b", 0, pools[1], pools[2]),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			objs := append(
				[]client.Object{tt.first, tt.second},
				poolSecretsIn(t, pools, "ns-a", "ns-b")...,
			)
			c := conflictClient(t, objs...)
			r := reconcilerFor(c)
			reconcileNode(t, r, tt.first)
			reconcileNode(t, r, tt.second)
			reconcileNode(t, r, tt.first)
			reconcileNode(t, r, tt.second)
			for _, n := range []*dingov1alpha1.DingoNode{tt.first, tt.second} {
				assert.True(t, forging(t, c, n), "%s/%s", n.Namespace, n.Name)
				assert.False(t, refusedAsDuplicate(t, c, n), "%s/%s: %s",
					n.Namespace, n.Name, degradedMessage(t, c, n))
				notInvalid(t, c, n)
				stagesKeys(t, c, n)
			}
		})
	}
}

// A pool-list producer and a producer with a local kes.skey for one of its
// pools keep the fence: the older keeps the pool, and a newer one already
// running is scaled to zero.
func TestReconcilePoolListAndLocalForgerStayFenced(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	tests := []struct {
		name  string
		older func(ns string) *dingov1alpha1.DingoNode
		newer func(ns string) *dingov1alpha1.DingoNode
	}{
		{
			"older pool list, newer local",
			func(ns string) *dingov1alpha1.DingoNode {
				return agentPoolList("old", ns, time.Hour, pools...)
			},
			func(ns string) *dingov1alpha1.DingoNode {
				return localForger("new", ns, pools[1].id, "k-new", 0)
			},
		},
		{
			"older local, newer pool list",
			func(ns string) *dingov1alpha1.DingoNode {
				return localForger("old", ns, pools[1].id, "k-old", time.Hour)
			},
			func(ns string) *dingov1alpha1.DingoNode {
				return agentPoolList("new", ns, 0, pools...)
			},
		},
	}
	for _, tt := range tests {
		for _, ns := range []string{"ns-a", "ns-b"} {
			t.Run(tt.name+" in "+ns, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				older := tt.older("ns-a")
				newer := tt.newer(ns)
				objs := append(
					[]client.Object{newer},
					poolSecretsIn(t, pools, "ns-a", "ns-b")...,
				)
				// The newer runs first, then the older appears.
				c := conflictClient(t, objs...)
				r := reconcilerFor(c)
				reconcileNode(t, r, newer)
				require.True(t, forging(t, c, newer))
				require.NoError(t, c.Create(ctx, older))
				reconcileNode(t, r, older)
				reconcileNode(t, r, newer)
				if ns == "ns-a" {
					assert.False(t, forging(t, c, newer),
						"a running newer producer is scaled to zero")
				}
				assert.True(t, refusedAsDuplicate(t, c, newer),
					degradedMessage(t, c, newer))
			})
		}
	}
}

// renderedPoolList returns the StatefulSet the operator renders for a
// pool-list node, controlled by it.
func renderedPoolList(
	dn *dingov1alpha1.DingoNode,
	pools []testPool,
	replicas int32,
) *appsv1.StatefulSet {
	opts := resources.RenderOptions{MountKeys: true, Replicas: replicas}
	var ids []string
	for _, p := range pools {
		opts.Pools = append(opts.Pools, resources.RenderPool{
			PoolID: p.id, SecretName: p.secret, AgentProfile: p.profile,
		})
		ids = append(ids, p.id)
	}
	opts.KeysPool = resources.JoinKeysPools(ids)
	sts := resources.BuildStatefulSet(dn, opts)
	sts.Spec.Replicas = new(replicas)
	sts.UID = types.UID("sts-" + dn.Namespace + "-" + dn.Name)
	sts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dingov1alpha1.GroupVersion.String(),
		Kind:       "DingoNode",
		Name:       dn.Name,
		UID:        dn.UID,
		Controller: new(true),
	}}
	return sts
}

// withLocalKESKey sets a local KES key on the dingo container of spec.
func withLocalKESKey(spec *corev1.PodSpec) {
	for i := range spec.Containers {
		if spec.Containers[i].Name == "dingo" {
			spec.Containers[i].Env = append(spec.Containers[i].Env,
				corev1.EnvVar{Name: resources.KESKeyEnv, Value: "/keys/kes.skey"},
			)
		}
	}
}

// A pool-list node's own template and pod sign through the agent: neither is
// read as a local signer. The same template with a local KES key is one.
func TestPoolListOwnWorkloadIsNotALocalSigner(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	for _, local := range []bool{false, true} {
		name := "rendered"
		if local {
			name = "with a local KES key"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dn := agentPoolList("bp", "ns", 0, pools...)

			stopped := renderedPoolList(dn, pools, 0)
			if local {
				withLocalKESKey(&stopped.Spec.Template.Spec)
			}
			require.NotEmpty(t,
				resources.KeysSecretNames(stopped.Spec.Template.Spec))
			assert.Equal(t, local,
				holdsStoppedLocalSigner(dn, stopped, true),
				"holdsStoppedLocalSigner")

			running := renderedPoolList(dn, pools, 1)
			if local {
				withLocalKESKey(&running.Spec.Template.Spec)
			}
			pod := &corev1.Pod{
				ObjectMeta: *running.Spec.Template.ObjectMeta.DeepCopy(),
				Spec:       *running.Spec.Template.Spec.DeepCopy(),
			}
			pod.Name, pod.Namespace = "bp-0", "ns"
			pod.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       running.Name,
				UID:        running.UID,
				Controller: new(true),
			}}
			pod.Status.Phase = corev1.PodRunning
			c := conflictClient(t, dn, running, pod)
			got, err := reconcilerFor(c).holdsLocalKES(ctx, dn)
			require.NoError(t, err)
			assert.Equal(t, local, got, "holdsLocalKES")

			// Its pod alone, with the StatefulSet scaled down.
			running.Spec.Replicas = new(int32(0))
			c = conflictClient(t, dn, running, pod)
			got, err = reconcilerFor(c).holdsLocalKES(ctx, dn)
			require.NoError(t, err)
			assert.Equal(t, local, got, "holdsLocalKES from the pod")
		})
	}
}
