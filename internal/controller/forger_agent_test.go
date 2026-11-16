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
	"k8s.io/apimachinery/pkg/api/meta"
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

func localForger(
	name, ns, poolID, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	dn := forgerNode(name, ns, poolID, secret, age)
	dn.UID = types.UID("uid-" + ns + "-" + name)
	return dn
}

// notInvalid fails when a node was refused for a reason other than the fence,
// so a test that expects it to forge cannot pass on a spec the operator
// rejected.
func notInvalid(t *testing.T, c client.Client, n *dingov1alpha1.DingoNode) {
	t.Helper()
	var got dingov1alpha1.DingoNode
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &got,
	))
	if d := meta.FindStatusCondition(
		got.Status.Conditions, condDegraded,
	); d != nil {
		assert.NotEqual(t, "InvalidSpec", d.Reason, d.Message)
		assert.NotEqual(t, reasonDuplicateForger, d.Reason, d.Message)
	}
}

// Producers that both sign through an agent are not fenced against each other:
// the agent signs at most one header per pool and slot, and neither holds a KES
// key.
func TestReconcileAgentSignedForgersDoNotFenceEachOther(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		first  *dingov1alpha1.DingoNode
		second *dingov1alpha1.DingoNode
	}{
		{
			name:   "same pool in another namespace",
			first:  agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour),
			second: agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0),
		},
		{
			name: "same pool in one namespace",
			first: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys-a", time.Hour,
			),
			second: agentForger("bp-2", "ns-a", conflictPoolID, "keys-b", 0),
		},
		{
			name: "same keys Secret name in one namespace",
			first: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys", time.Hour,
			),
			second: agentForger("bp-2", "ns-a", otherPoolID, "keys", 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := conflictClient(t, tt.first, tt.second)
			r := reconcilerFor(c)
			// Reconcile the older first, then the newer, then the older
			// again: each sees the other running.
			reconcileNode(t, r, tt.first)
			reconcileNode(t, r, tt.second)
			reconcileNode(t, r, tt.first)
			reconcileNode(t, r, tt.second)
			for _, n := range []*dingov1alpha1.DingoNode{tt.first, tt.second} {
				assert.True(t, forging(t, c, n), "%s/%s", n.Namespace, n.Name)
				assert.False(t, refusedAsDuplicate(t, c, n), "%s/%s: %s",
					n.Namespace, n.Name, degradedMessage(t, c, n))
				notInvalid(t, c, n)
			}
		})
	}
}

// Agent-signed copies of one key bundle share a cold key, a checksum and a
// pool: still no conflict.
func TestReconcileAgentSignedForgersSharingKeyBundle(t *testing.T) {
	t.Parallel()
	pool, data := poolKeys(t)
	data = withoutKey(data, keyFileKES)
	first := agentForger("bp", "ns-a", pool, "keys", time.Hour)
	second := agentForger("bp", "ns-b", pool, "keys-copy", 0)
	third := agentForger("bp-3", "ns-a", otherPoolID, "keys-copy-2", 0)
	c := conflictClient(t, first, second, third,
		keysSecret("keys", "ns-a", data),
		keysSecret("keys-copy", "ns-b", data),
		keysSecret("keys-copy-2", "ns-a", data),
	)
	r := reconcilerFor(c)
	for range 2 {
		for _, n := range []*dingov1alpha1.DingoNode{first, second, third} {
			reconcileNode(t, r, n)
		}
	}
	for _, n := range []*dingov1alpha1.DingoNode{first, second, third} {
		assert.True(t, forging(t, c, n), "%s/%s", n.Namespace, n.Name)
		assert.False(t, refusedAsDuplicate(t, c, n), "%s/%s: %s",
			n.Namespace, n.Name, degradedMessage(t, c, n))
		notInvalid(t, c, n)
	}
}

// A pair with a local kes.skey on either side keeps today's fence: the older
// keeps the pool, and a newer one already running is scaled to zero.
func TestReconcileAgentAndLocalForgersStayFenced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		older func(ns string) *dingov1alpha1.DingoNode
		newer func(ns string) *dingov1alpha1.DingoNode
	}{
		{
			"older agent, newer local",
			func(ns string) *dingov1alpha1.DingoNode {
				return agentForger("old", ns, conflictPoolID, "k-old", time.Hour)
			},
			func(ns string) *dingov1alpha1.DingoNode {
				return localForger("new", ns, conflictPoolID, "k-new", 0)
			},
		},
		{
			"older local, newer agent",
			func(ns string) *dingov1alpha1.DingoNode {
				return localForger("old", ns, conflictPoolID, "k-old", time.Hour)
			},
			func(ns string) *dingov1alpha1.DingoNode {
				return agentForger("new", ns, conflictPoolID, "k-new", 0)
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
				// The newer runs first, then the older appears.
				c := conflictClient(t, newer)
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
				if ns == "ns-a" {
					// With the newer stopped, the older starts.
					reconcileNode(t, r, older)
					assert.True(t, forging(t, c, older))
					assert.False(t, refusedAsDuplicate(t, c, older),
						degradedMessage(t, c, older))
				}
			})
		}
	}
}

// renderedProducer returns the StatefulSet the operator renders for dn, as a
// workload running under it.
func renderedProducer(dn *dingov1alpha1.DingoNode) *appsv1.StatefulSet {
	sts := resources.BuildStatefulSet(dn, resources.RenderOptions{
		Replicas: 1, MountKeys: true, KeysPool: conflictPoolID,
	})
	sts.UID = types.UID("sts-" + dn.Namespace + "-" + dn.Name)
	return sts
}

// A DingoNode whose spec now names a signing agent but whose workload still
// carries a local KES key (the template is held at the old render) is a local
// forger until the workload signs through the agent.
func TestReconcileAgentSpecWithLocalKESWorkloadStaysFenced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	switching := agentForger("old", "ns", conflictPoolID, "k-old", time.Hour)
	// The workload was rendered before the switch: local KES key.
	pre := localForger("old", "ns", conflictPoolID, "k-old", time.Hour)
	preSts := renderedProducer(pre)
	preSts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dingov1alpha1.GroupVersion.String(),
		Kind:       "DingoNode",
		Name:       switching.Name,
		UID:        switching.UID,
		Controller: new(true),
	}}
	newer := agentForger("new", "ns", conflictPoolID, "k-new", 0)

	c := conflictClient(t, switching, preSts, newer)
	r := reconcilerFor(c)
	reconcileNode(t, r, newer)
	assert.False(t, forging(t, c, newer), "held by the local-KES workload")
	assert.True(t, refusedAsDuplicate(t, c, newer), degradedMessage(t, c, newer))

	// Once the old workload is gone, the spec decides and both forge.
	require.NoError(t, c.Delete(ctx, preSts))
	reconcileNode(t, r, switching)
	reconcileNode(t, r, newer)
	assert.True(t, forging(t, c, switching))
	assert.True(t, forging(t, c, newer))
	assert.False(t, refusedAsDuplicate(t, c, newer), degradedMessage(t, c, newer))
}

// An agent-signed producer is held by another block producer's workload that
// still holds a local KES key, and is not held by one that signs through an
// agent.
func TestReconcileAgentForgerAndOrphanWorkloads(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		other *dingov1alpha1.DingoNode
		held  bool
	}{
		{"local KES workload", localForger("o", "ns-o", conflictPoolID, "ko", 0), true},
		{"agent workload", agentForger("o", "ns-o", conflictPoolID, "ko", 0), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			victim := agentForger("bp", "ns-a", conflictPoolID, "keys", time.Hour)
			sts := renderedProducer(tt.other)
			// No DingoNode controls it: an orphan.
			pod := &corev1.Pod{
				ObjectMeta: sts.Spec.Template.ObjectMeta,
				Spec:       sts.Spec.Template.Spec,
			}
			pod.Name, pod.Namespace = "o-0", "ns-o"
			pod.Labels = resources.Labels(tt.other)
			c := conflictClient(t, victim)
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.True(t, forging(t, c, victim))
			require.NoError(t, c.Create(context.Background(), pod))
			reconcileNode(t, r, victim)
			assert.Equal(t, tt.held, refusedAsDuplicate(t, c, victim),
				degradedMessage(t, c, victim))
			assert.True(t, forging(t, c, victim), "an orphan never halts")
		})
	}
}

func envtestAgentProducer(
	name, ns, secret, pool string,
) *dingov1alpha1.DingoNode {
	dn := blockProducerNode(name, ns, secret, pool)
	dn.Spec.Image.Tag = agentImageTag
	dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:   "kes-agent.signing.svc:7000",
		Plaintext: true,
	}
	return dn
}

// Against a real API server, agent-signed producers of one pool forge side by
// side across namespaces and within one, whether they name one keys Secret or
// copies of it, and one with a local kes.skey is still refused beside them.
func TestEnvtestAgentSignedForgersForgeTogether(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "agent-fa")
	createNamespace(t, ctx, c, "agent-fb")
	pool, data := poolKeys(t)
	agentData := withoutKey(data, keyFileKES)
	for _, ns := range []string{"agent-fa", "agent-fb"} {
		require.NoError(t, c.Create(ctx, keysSecret("keys", ns, agentData)))
	}
	require.NoError(t, c.Create(ctx, keysSecret("keys-copy", "agent-fa", agentData)))
	r := reconcilerFor(c)

	nodes := []*dingov1alpha1.DingoNode{
		envtestAgentProducer("bp-1", "agent-fa", "keys", pool),
		envtestAgentProducer("bp-2", "agent-fb", "keys", pool),
		// Same namespace and same Secret name as bp-1.
		envtestAgentProducer("bp-3", "agent-fa", "keys", pool),
		// Same namespace, a copy of the same bundle.
		envtestAgentProducer("bp-4", "agent-fa", "keys-copy", pool),
	}
	for _, n := range nodes {
		require.NoError(t, c.Create(ctx, n))
	}
	for range 2 {
		for _, n := range nodes {
			reconcileNode(t, r, n)
		}
	}
	for _, n := range nodes {
		assert.True(t, forging(t, c, n), "%s/%s", n.Namespace, n.Name)
		assert.False(t, refusedAsDuplicate(t, c, n), "%s/%s: %s",
			n.Namespace, n.Name, degradedMessage(t, c, n))
		notInvalid(t, c, n)
	}

	// A producer with a local KES key for the same pool is still fenced.
	require.NoError(t, c.Create(ctx, keysSecret("keys-local", "agent-fa", data)))
	local := blockProducerNode("bp-local", "agent-fa", "keys-local", pool)
	require.NoError(t, c.Create(ctx, local))
	reconcileNode(t, r, local)
	assert.True(t, refusedAsDuplicate(t, c, local), degradedMessage(t, c, local))
	assert.False(t, forging(t, c, local))
}

// ownedLocalWorkload is the StatefulSet of a node whose spec now names an agent
// but whose workload was rendered before the switch: a local KES key.
func ownedLocalWorkload(dn *dingov1alpha1.DingoNode) *appsv1.StatefulSet {
	pre := localForger(dn.Name, dn.Namespace, conflictPoolID,
		dn.Spec.BlockProducer.Keys.SecretRef, 0)
	sts := renderedProducer(pre)
	sts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dingov1alpha1.GroupVersion.String(),
		Kind:       "DingoNode",
		Name:       dn.Name,
		UID:        dn.UID,
		Controller: new(true),
	}}
	return sts
}

// A running agent-signed producer is halted when an older node of the same
// namespace names an agent but still runs a local KES key, as for any older
// producer holding the pool.
func TestReconcileAgentRunningForgerHaltedByLocalKESWorkload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	running := agentForger("new", "ns", conflictPoolID, "k-new", 0)
	switching := agentForger("old", "ns", conflictPoolID, "k-old", time.Hour)
	c := conflictClient(t, running)
	r := reconcilerFor(c)
	reconcileNode(t, r, running)
	require.True(t, forging(t, c, running))

	require.NoError(t, c.Create(ctx, switching))
	require.NoError(t, c.Create(ctx, ownedLocalWorkload(switching)))
	reconcileNode(t, r, running)
	assert.False(t, forging(t, c, running),
		"the older producer still signs with a local KES key")
	assert.True(t, refusedAsDuplicate(t, c, running),
		degradedMessage(t, c, running))
}

// A node that names an agent while its own workload still holds a local KES
// key is fenced against an older agent-signed producer until that workload
// stops, then rolls to the agent and forges beside it.
func TestReconcileAgentSpecWithOwnLocalKESWorkloadIsFenced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	older := agentForger("old", "ns", conflictPoolID, "k-old", time.Hour)
	switching := agentForger("new", "ns", conflictPoolID, "k-new", 0)
	c := conflictClient(t, older)
	r := reconcilerFor(c)
	reconcileNode(t, r, older)
	require.True(t, forging(t, c, older))

	local := ownedLocalWorkload(switching)
	require.NoError(t, c.Create(ctx, switching))
	require.NoError(t, c.Create(ctx, local))
	reconcileNode(t, r, switching)
	assert.True(t, refusedAsDuplicate(t, c, switching),
		degradedMessage(t, c, switching))
	assert.False(t, forging(t, c, switching),
		"its local KES key must not sign beside the older producer")

	// Halted, the workload holds no key: the spec decides and it rolls.
	reconcileNode(t, r, switching)
	assert.True(t, forging(t, c, switching))
	assert.False(t, refusedAsDuplicate(t, c, switching),
		degradedMessage(t, c, switching))
	assert.True(t, forging(t, c, older))
}

// A pod another producer staged from a copy of the node's keys holds them
// while it carries a local KES key, and not once it signs through an agent.
func TestReconcileAgentForgerAndOrphanPodWithKeysBundle(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		other *dingov1alpha1.DingoNode
		held  bool
	}{
		{"local KES pod", localForger("o", "ns-o", otherPoolID, "ko", 0), true},
		{"agent pod", agentForger("o", "ns-o", otherPoolID, "ko", 0), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, data := poolKeys(t)
			data = withoutKey(data, keyFileKES)
			secret := keysSecret("keys", "ns-a", data)
			victim := agentForger("bp", "ns-a", pool, "keys", time.Hour)
			sts := renderedProducer(tt.other)
			sts.Spec.Template.Annotations = map[string]string{
				resources.KeysChecksumAnnotation: keysChecksum(secret),
			}
			pod := &corev1.Pod{
				ObjectMeta: sts.Spec.Template.ObjectMeta,
				Spec:       sts.Spec.Template.Spec,
			}
			pod.Name, pod.Namespace = "o-0", "ns-o"
			pod.Labels = resources.Labels(tt.other)
			c := conflictClient(t, victim, secret)
			r := reconcilerFor(c)
			reconcileNode(t, r, victim)
			require.True(t, forging(t, c, victim))
			require.NoError(t, c.Create(context.Background(), pod))
			reconcileNode(t, r, victim)
			assert.Equal(t, tt.held, refusedAsDuplicate(t, c, victim),
				degradedMessage(t, c, victim))
			assert.True(t, forging(t, c, victim), "an orphan never halts")
		})
	}
}

// A producer switching to an agent is halted by the fence while its workload
// still holds a local KES key. Halted, it counts as agent-signed and is
// admitted beside the older agent-signed producer, but while its pod template
// is held at the local-KES render it stays at zero replicas: the held template
// would start a local signer outside the agent's slot guard. Once the agent
// template can be applied, the template and replicas land together.
func TestReconcileSwitchingProducerStaysHaltedWhileTemplateHeld(t *testing.T) {
	t.Parallel()
	crt, key := testClientCertPair(t)
	for _, tt := range []struct {
		name   string
		mtls   bool
		reason string
		// release makes the agent template applicable.
		release func(ctx context.Context, c client.Client, keys *corev1.Secret) error
	}{
		{
			name:   "incomplete agent client certificate",
			mtls:   true,
			reason: reasonSigningAgentCertInvalid,
			release: func(ctx context.Context, c client.Client, _ *corev1.Secret) error {
				var cert corev1.Secret
				if err := c.Get(ctx, types.NamespacedName{
					Name: "agent-client", Namespace: "ns",
				}, &cert); err != nil {
					return err
				}
				cert.Data[corev1.TLSPrivateKeyKey] = key
				return c.Update(ctx, &cert)
			},
		},
		{
			name:   "missing keys Secret",
			reason: "KeysSecretMissing",
			release: func(ctx context.Context, c client.Client, keys *corev1.Secret) error {
				return c.Create(ctx, keys)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			pool, full := poolKeys(t)
			data := withoutKey(full, keyFileKES)
			older := agentForger("old", "ns", pool, "k-old", time.Hour)
			switching := agentForger("new", "ns", pool, "k-new", 0)
			objs := []client.Object{older, keysSecret("k-old", "ns", data)}
			newKeys := keysSecret("k-new", "ns", data)
			if tt.mtls {
				switching.Spec.BlockProducer.SigningAgent =
					&dingov1alpha1.SigningAgentSpec{
						Address:             "kes-agent.signing.svc:7000",
						ClientCertSecretRef: "agent-client",
					}
				objs = append(objs, newKeys, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name: "agent-client", Namespace: "ns",
					},
					Data: map[string][]byte{corev1.TLSCertKey: crt},
				})
			}
			c := conflictClient(t, objs...)
			r := reconcilerFor(c)
			reconcileNode(t, r, older)
			require.True(t, forging(t, c, older))

			// The workload was rendered before the switch: a local KES key.
			pre := localForger("new", "ns", pool, "k-new", 0)
			local := resources.BuildStatefulSet(pre, resources.RenderOptions{
				Replicas: 1, MountKeys: true, KeysPool: pool,
				KeysChecksum: keysChecksum(keysSecret("k-new", "ns", full)),
			})
			local.CreationTimestamp = metav1.Now()
			local.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: dingov1alpha1.GroupVersion.String(),
				Kind:       "DingoNode",
				Name:       switching.Name,
				UID:        switching.UID,
				Controller: new(true),
			}}
			require.NoError(t, c.Create(ctx, switching))
			require.NoError(t, c.Create(ctx, local))
			reconcileNode(t, r, switching)
			require.True(t, refusedAsDuplicate(t, c, switching),
				degradedMessage(t, c, switching))
			require.False(t, forging(t, c, switching))

			// Halted, it is admitted, but its template is held at the
			// local-KES render: it must not restart.
			for range 2 {
				reconcileNode(t, r, switching)
				var sts appsv1.StatefulSet
				require.NoError(t, c.Get(
					ctx, client.ObjectKeyFromObject(switching), &sts,
				))
				require.True(t, forgesWithLocalKES(sts.Spec.Template.Spec),
					"the template is held at the local-KES render")
				assert.False(t, forging(t, c, switching),
					"a held local-KES template must not restart")
				assert.Equal(t, tt.reason, degradedReason(t, c, switching),
					degradedMessage(t, c, switching))
				assert.True(t, forging(t, c, older))
			}

			// Once the agent template applies, it rolls and forges.
			require.NoError(t, tt.release(ctx, c, newKeys))
			reconcileNode(t, r, switching)
			var sts appsv1.StatefulSet
			require.NoError(t, c.Get(
				ctx, client.ObjectKeyFromObject(switching), &sts,
			))
			assert.True(t, resources.PodSignsThroughAgent(
				sts.Spec.Template.Spec,
			))
			assert.True(t, forging(t, c, switching))
			assert.False(t, refusedAsDuplicate(t, c, switching),
				degradedMessage(t, c, switching))
			assert.True(t, forging(t, c, older))
		})
	}
}

func degradedReason(
	t *testing.T, c client.Client, n *dingov1alpha1.DingoNode,
) string {
	t.Helper()
	var got dingov1alpha1.DingoNode
	require.NoError(t, c.Get(
		context.Background(), client.ObjectKeyFromObject(n), &got,
	))
	d := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
	if d == nil {
		return ""
	}
	return d.Reason
}
