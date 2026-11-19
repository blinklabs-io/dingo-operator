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
	"crypto/rand"
	"strconv"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// A pool-list node is accepted by the CRD without poolId or keys, renders a
// pool list and one staged Secret per pool, and takes a new opcert of one pool
// without changing the pod template; only a change to static key material, or
// to the set of pools, rolls it.
func TestPoolListEnvtest(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	const ns = "pool-list"
	createNamespace(t, ctx, c, ns)

	pools := newTestPools(t, "a", "b", "c")
	for _, p := range pools {
		require.NoError(t, c.Create(ctx, poolSecret(t, ns, p, 1)))
	}
	dn := poolListNode("bp", ns, 0, pools...)
	dn.CreationTimestamp = metav1.Time{}
	require.NoError(t, c.Create(ctx, dn))
	r := reconcilerFor(c)
	reconcile(t, ctx, r, "bp", ns)

	key := types.NamespacedName{Name: "bp", Namespace: ns}
	getNode := func() *dingov1alpha1.DingoNode {
		got := &dingov1alpha1.DingoNode{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}
	getCM := func(name string) *corev1.ConfigMap {
		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{
			Name: name, Namespace: ns,
		}, cm))
		return cm
	}
	stored := getNode()
	assert.Empty(t, stored.Spec.BlockProducer.Keys.SecretRef)
	require.Len(t, stored.Spec.BlockProducer.Pools, 3)

	// Rendering.
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, key, sts))
	tpl := sts.Spec.Template
	for _, name := range []string{
		"CARDANO_SHELLEY_VRF_KEY", "CARDANO_SHELLEY_KES_KEY",
		"CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE",
		"CARDANO_SHELLEY_KES_AGENT_ADDRESS",
	} {
		_, ok := templateEnv(tpl, name)
		assert.False(t, ok, name)
	}
	producer, _ := templateEnv(tpl, "CARDANO_BLOCK_PRODUCER")
	assert.Equal(t, "true", producer)
	secrets := resources.KeysSecretNames(tpl.Spec)
	assert.ElementsMatch(t,
		[]string{pools[0].secret, pools[1].secret, pools[2].secret}, secrets)
	cfg := getCM("bp-pools").Data["dingo.yaml"]
	for _, p := range pools {
		assert.Contains(t, cfg, "/keys/"+p.id+"/vrf.skey")
		assert.Contains(t, cfg, `shelleyKesAgentProfile: "`+p.profile+`"`)
	}
	st := getNode().Status
	require.Len(t, st.Pools, 3)
	for _, ps := range st.Pools {
		assert.True(t, ps.KeysValid, ps.PoolID)
		assert.Equal(t, int64(1), ps.OnDiskCounter, ps.PoolID)
	}

	approval := func(p testPool) string {
		return getCM(resources.KeysApprovalConfigMapName(dn)).Data[p.id]
	}
	beforeA, beforeB := approval(pools[0]), approval(pools[1])
	require.NotEmpty(t, beforeA)
	require.NotEmpty(t, beforeB)

	// A new opcert for pool b: the template stays, the approval moves.
	before := podTemplate(t, ctx, c, "bp", ns)
	generation := sts.Generation
	update := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: pools[1].secret, Namespace: ns,
	}, update))
	update.Data = poolSecret(t, ns, pools[1], 2).Data
	require.NoError(t, c.Update(ctx, update))
	reconcile(t, ctx, r, "bp", ns)

	assert.Equal(t, before, podTemplate(t, ctx, c, "bp", ns),
		"a new opcert of one pool must not change the pod template")
	require.NoError(t, c.Get(ctx, key, sts))
	assert.Equal(t, generation, sts.Generation)
	assert.NotEqual(t, beforeB, approval(pools[1]))
	assert.Equal(t, beforeA, approval(pools[0]), "other pools are untouched")
	counters := map[string]int64{}
	for _, ps := range getNode().Status.Pools {
		counters[ps.PoolID] = ps.OnDiskCounter
	}
	assert.Equal(t, int64(2), counters[pools[1].id])
	assert.Equal(t, int64(1), counters[pools[0].id])

	// Static key material (a VRF key) rolls the pod.
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	vrf, err := other.VRFSKeyEnvelope()
	require.NoError(t, err)
	vrfUpdate := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: pools[2].secret, Namespace: ns,
	}, vrfUpdate))
	vrfUpdate.Data[keyFileVRF] = vrf
	require.NoError(t, c.Update(ctx, vrfUpdate))
	reconcile(t, ctx, r, "bp", ns)
	rolled := podTemplate(t, ctx, c, "bp", ns)
	assert.NotEqual(t,
		before.Annotations[resources.KeysChecksumAnnotation],
		rolled.Annotations[resources.KeysChecksumAnnotation])
	assert.Equal(t,
		before.Annotations[resources.PoolsConfigChecksumAnnotation],
		rolled.Annotations[resources.PoolsConfigChecksumAnnotation],
		"the pool list did not change")

	// A bad bundle drops its pool: one roll, the others stay.
	broken := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: pools[0].secret, Namespace: ns,
	}, broken))
	broken.Data[keyFileVRF] = []byte("junk")
	require.NoError(t, c.Update(ctx, broken))
	reconcile(t, ctx, r, "bp", ns)
	require.NoError(t, c.Get(ctx, key, sts))
	assert.NotContains(
		t,
		sts.Spec.Template.Annotations[resources.KeysPoolAnnotation],
		pools[0].id,
	)
	assert.Contains(
		t,
		sts.Spec.Template.Annotations[resources.KeysPoolAnnotation],
		pools[1].id,
	)
	assert.NotContains(t, getCM("bp-pools").Data["dingo.yaml"], pools[0].id)
	assert.Contains(t, getCM("bp-pools").Data["dingo.yaml"], pools[1].id)
	final := getNode()
	for _, ps := range final.Status.Pools {
		assert.Equal(t, ps.PoolID != pools[0].id, ps.KeysValid, ps.PoolID)
	}
	cond := meta.FindStatusCondition(final.Status.Conditions, condKeysValid)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "pool "+pools[0].id)
}

// The CRD refuses a pool entry without its poolId, secretRef or profile.
func TestPoolListCRDSchema(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	const ns = "pool-schema"
	createNamespace(t, ctx, c, ns)
	pools := newTestPools(t, "a")

	valid := poolListNode("schema-valid", ns, 0, pools...)
	valid.CreationTimestamp = metav1.Time{}
	require.NoError(t, c.Create(ctx, valid), "a complete entry is accepted")

	for i, tc := range []struct {
		name   string
		mutate func(*dingov1alpha1.BlockProducerPool)
	}{
		{"no poolId", func(p *dingov1alpha1.BlockProducerPool) { p.PoolID = "" }},
		{"no profile", func(p *dingov1alpha1.BlockProducerPool) { p.AgentProfile = "" }},
		{"no secretRef", func(p *dingov1alpha1.BlockProducerPool) { p.Keys.SecretRef = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dn := poolListNode("schema-"+strconv.Itoa(i), ns, 0, pools...)
			dn.CreationTimestamp = metav1.Time{}
			tc.mutate(&dn.Spec.BlockProducer.Pools[0])
			err := c.Create(ctx, dn)
			require.Error(t, err)
			assert.True(t, apierrors.IsInvalid(err), err.Error())
		})
	}
}
