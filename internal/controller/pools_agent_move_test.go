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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// poolListMove drives the pool keys and the StatefulSet of a pool-list
// producer the way a reconcile does: the pool list ConfigMap is written before
// the StatefulSet.
type poolListMove struct {
	t   *testing.T
	c   client.Client
	r   *DingoNodeReconciler
	dn  *dingov1alpha1.DingoNode
	key client.ObjectKey
}

func newPoolListMove(
	t *testing.T,
	pools []testPool,
	objs ...client.Object,
) *poolListMove {
	t.Helper()
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.UID = "uid-bp"
	dn.Spec.Replicas = new(int32(2))
	all := []client.Object{dn}
	for i, p := range pools {
		all = append(all, poolSecret(t, "ns", p, uint64(i+3)))
	}
	c := conflictClient(t, append(all, objs...)...)
	return &poolListMove{
		t: t, c: c, r: poolReconciler(c), dn: dn,
		key: client.ObjectKeyFromObject(dn),
	}
}

func (m *poolListMove) apply() appsv1.StatefulSet {
	m.t.Helper()
	opts := resources.RenderOptions{
		MountKeys: true, Replicas: desiredReplicas(m.dn),
	}
	keep, _, err := m.r.reconcilePoolKeys(m.t.Context(), m.dn, &opts)
	require.NoError(m.t, err)
	require.False(m.t, keep)
	_, err = m.r.upsertStatefulSet(m.t.Context(), m.dn, opts, false)
	require.NoError(m.t, err)
	var got appsv1.StatefulSet
	require.NoError(m.t, m.c.Get(m.t.Context(), m.key, &got))
	require.NotNil(m.t, got.Spec.Replicas)
	return got
}

func (m *poolListMove) setStatus(st appsv1.StatefulSetStatus) {
	m.t.Helper()
	var got appsv1.StatefulSet
	require.NoError(m.t, m.c.Get(m.t.Context(), m.key, &got))
	got.Status = st
	require.NoError(m.t, m.c.Status().Update(m.t.Context(), &got))
}

// poolsConfig is the pool list the pods read at startup.
func (m *poolListMove) poolsConfig() string {
	m.t.Helper()
	var cm corev1.ConfigMap
	require.NoError(m.t, m.c.Get(m.t.Context(), client.ObjectKey{
		Name: resources.PoolsConfigMapName(m.dn), Namespace: "ns",
	}, &cm))
	return cm.Data[resources.PoolsConfigKey]
}

// running settles two pods on the live template.
func (m *poolListMove) running() appsv1.StatefulSet {
	m.t.Helper()
	got := m.apply()
	require.Equal(m.t, int32(2), *got.Spec.Replicas)
	// The fake client sets no creation time, which upsertStatefulSet reads
	// as a StatefulSet still to be created.
	got.CreationTimestamp = metav1.Now()
	require.NoError(m.t, m.c.Update(m.t.Context(), &got))
	m.setStatus(appsv1.StatefulSetStatus{
		ObservedGeneration: got.Generation, Replicas: 2,
		CurrentRevision: "bp-a", UpdateRevision: "bp-a",
	})
	return got
}

func poolsChecksumOf(sts appsv1.StatefulSet) string {
	return sts.Spec.Template.Annotations[resources.PoolsConfigChecksumAnnotation]
}

// A pool-list pod reads its agent from the pool list file, not from its
// environment, so the file must not move while several pods run either: a
// restarted pod would read the new agent beside one still on the old.
func TestPoolListMovesToAnotherAgentThroughOnePod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*dingov1alpha1.SigningAgentSpec)
		oldVal string
		newVal string
	}{
		{"address", func(sa *dingov1alpha1.SigningAgentSpec) {
			sa.Address = "kes-agent-b.signing.svc:7000"
		}, `"kes-agent.signing.svc:7000"`, `"kes-agent-b.signing.svc:7000"`},
		{"server key pin", func(sa *dingov1alpha1.SigningAgentSpec) {
			sa.ServerSPKISHA256 = strings.Repeat("cd", 32)
		}, testSPKIPin, strings.Repeat("cd", 32)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newPoolListMove(t, newTestPools(t, "a"))
			old := poolsChecksumOf(m.running())
			require.Contains(t, m.poolsConfig(), tt.oldVal)
			tt.change(m.dn.Spec.BlockProducer.SigningAgent)

			// Two pods run the old agent: shrink first, and neither the
			// template nor the pool list moves.
			got := m.apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)
			assert.Equal(t, old, poolsChecksumOf(got),
				"the template must not move while two pods run the old agent")
			assert.Contains(t, m.poolsConfig(), tt.oldVal)
			assert.NotContains(t, m.poolsConfig(), tt.newVal,
				"the pool list must not move while two pods run the old agent")

			// The second pod is still terminating.
			m.setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 2,
				CurrentRevision: "bp-a", UpdateRevision: "bp-a",
			})
			got = m.apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)
			assert.Equal(t, old, poolsChecksumOf(got))
			assert.NotContains(t, m.poolsConfig(), tt.newVal)

			// One pod left: it moves, and nothing is added beside it.
			m.setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 1,
				CurrentRevision: "bp-a", UpdateRevision: "bp-a",
			})
			got = m.apply()
			assert.NotEqual(t, old, poolsChecksumOf(got))
			assert.Contains(t, m.poolsConfig(), tt.newVal)
			assert.Equal(t, int32(1), *got.Spec.Replicas,
				"the new template and a second pod must not arrive together")

			// The roll is not finished: still one pod.
			m.setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 1,
				CurrentRevision: "bp-a", UpdateRevision: "bp-b",
			})
			got = m.apply()
			assert.Equal(t, int32(1), *got.Spec.Replicas)

			// Every pod runs the new template: back to two.
			m.setStatus(appsv1.StatefulSetStatus{
				ObservedGeneration: got.Generation, Replicas: 1,
				CurrentRevision: "bp-b", UpdateRevision: "bp-b",
			})
			got = m.apply()
			assert.Equal(t, int32(2), *got.Spec.Replicas)
		})
	}
}

// A pool-list change that keeps the agent and its pin rolls several pods as
// before. A client certificate is presented to the same agent, whose guard
// still admits one header per slot.
func TestPoolListKeepsPodsForChangeThatKeepsTheAgent(t *testing.T) {
	t.Parallel()
	certPEM, keyPEM := testClientCertPair(t)
	cert := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-client", Namespace: "ns"},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	pools := newTestPools(t, "a", "b")
	tests := []struct {
		name   string
		change func(*dingov1alpha1.DingoNode)
	}{
		{"another pool", func(dn *dingov1alpha1.DingoNode) {
			p := pools[1]
			dn.Spec.BlockProducer.Pools = append(
				dn.Spec.BlockProducer.Pools,
				dingov1alpha1.BlockProducerPool{
					PoolID:       p.id,
					AgentProfile: p.profile,
					Keys:         dingov1alpha1.KeysSpec{SecretRef: p.secret},
				},
			)
		}},
		{"client certificate", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.BlockProducer.SigningAgent.ClientCertSecretRef =
				"agent-client"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newPoolListMove(t, pools[:1],
				cert.DeepCopy(), poolSecret(t, "ns", pools[1], 4))
			old := poolsChecksumOf(m.running())
			tt.change(m.dn)

			got := m.apply()
			assert.Equal(t, int32(2), *got.Spec.Replicas)
			assert.NotEqual(t, old, poolsChecksumOf(got))
			assert.Equal(t, poolsChecksumOf(got),
				checksum(m.poolsConfig()),
				"the pool list must move with the template")
		})
	}
}
