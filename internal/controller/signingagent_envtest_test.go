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
	"encoding/hex"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// A block producer that signs through a remote agent forges from a keys
// Secret without kes.skey, gets the agent settings instead of a KES key path,
// rolls on a keys change as before, and holds its template while the agent
// client certificate Secret is incomplete.
func TestBlockProducerSigningAgent(t *testing.T) {
	c, ctx := startEnv(t)
	const ns = "agent-ns"
	createNamespace(t, ctx, c, ns)

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(0, 0)
	require.NoError(t, err)
	strayKES := data[keyFileKES]
	delete(data, keyFileKES)

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-keys", Namespace: ns},
		Data:       data,
	}))
	clientCrt, clientKey := testClientCertPair(t)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-client", Namespace: ns},
		Data: map[string][]byte{
			"tls.crt": clientCrt,
			"tls.key": clientKey,
		},
	}))

	dn := blockProducerNode(
		"bp", ns, "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:             "kes-agent.signing.svc:7000",
		ServerSPKISHA256:    testSPKIPin,
		ClientCertSecretRef: "agent-client",
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)

	getNode := func() *dingov1alpha1.DingoNode {
		got := &dingov1alpha1.DingoNode{}
		require.NoError(t, c.Get(
			ctx, types.NamespacedName{Name: "bp", Namespace: ns}, got,
		))
		return got
	}
	keysValid := func() *metav1.Condition {
		cond := meta.FindStatusCondition(getNode().Status.Conditions, condKeysValid)
		require.NotNil(t, cond)
		return cond
	}

	assert.Equal(t, metav1.ConditionTrue, keysValid().Status)
	first := keysAnnotation(t, ctx, c, "bp", ns)
	require.NotEmpty(t, first)
	tpl := podTemplate(t, ctx, c, "bp", ns)
	_, hasKES := templateEnv(tpl, "CARDANO_SHELLEY_KES_KEY")
	assert.False(t, hasKES, "agent mode must not pass a local KES key")
	addr, _ := templateEnv(tpl, "CARDANO_SHELLEY_KES_AGENT_ADDRESS")
	assert.Equal(t, "kes-agent.signing.svc:7000", addr)
	pin, _ := templateEnv(tpl, "CARDANO_SHELLEY_KES_AGENT_SERVER_SPKI_SHA256")
	assert.Equal(t, testSPKIPin, pin)
	_, hasCert := templateEnv(tpl, "CARDANO_SHELLEY_KES_AGENT_CLIENT_CERT_FILE")
	assert.True(t, hasCert)

	// Half a client certificate pair, together with a keys change that
	// would otherwise roll the pod: the template is held.
	certSecret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: "agent-client", Namespace: ns},
		certSecret,
	))
	delete(certSecret.Data, "tls.key")
	require.NoError(t, c.Update(ctx, certSecret))

	rotated, err := keys.SecretData(1, 0)
	require.NoError(t, err)
	delete(rotated, keyFileKES)
	keysSecret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: "pool-keys", Namespace: ns},
		keysSecret,
	))
	keysSecret.Data = rotated
	require.NoError(t, c.Update(ctx, keysSecret))
	// A spec change too, so the held render differs from the live template
	// and the hold is reported.
	moved := getNode()
	moved.Spec.BlockProducer.SigningAgent.Address = "kes-agent.signing.svc:7001"
	require.NoError(t, c.Update(ctx, moved))

	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	assert.Equal(t, tpl, podTemplate(t, ctx, c, "bp", ns),
		"an incomplete client certificate must hold the pod template")
	cond := keysValid()
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, reasonSigningAgentCertInvalid, cond.Reason)
	assert.Contains(t, cond.Message, "missing tls.key")
	assert.Contains(t, cond.Message, signingAgentCertHeldMessage)
	assert.NotContains(t, cond.Message, templateHeldMessage)
	assert.True(t, meta.IsStatusConditionTrue(
		getNode().Status.Conditions, condDegraded,
	))

	// The pair restored: the rotated bundle rolls the pod.
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: "agent-client", Namespace: ns},
		certSecret,
	))
	certSecret.Data["tls.key"] = clientKey
	require.NoError(t, c.Update(ctx, certSecret))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	second := keysAnnotation(t, ctx, c, "bp", ns)
	assert.NotEqual(t, first, second, "a keys change must still roll the pod")
	assert.Equal(t, metav1.ConditionTrue, keysValid().Status)
	assert.Equal(t, int64(1), getNode().Status.OpCert.OnDiskCounter)

	// A kes.skey left in the Secret is accepted, reported and never wired.
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: "pool-keys", Namespace: ns},
		keysSecret,
	))
	keysSecret.Data[keyFileKES] = strayKES
	require.NoError(t, c.Update(ctx, keysSecret))
	reconcile(t, ctx, reconcilerFor(c), "bp", ns)
	cond = keysValid()
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, unusedKESKeyMessage)
	_, hasKES = templateEnv(
		podTemplate(t, ctx, c, "bp", ns), "CARDANO_SHELLEY_KES_KEY",
	)
	assert.False(t, hasKES)
}
