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
	"crypto/sha256"
	"encoding/hex"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/dingo-operator/internal/test/fakeagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// agentBundleEnv is an Auto block producer signing through a fake agent whose
// control role the operator holds, reconciled once on a counter-3 bundle while
// the agent already serves a counter-4 opcert.
type agentBundleEnv struct {
	c      client.Client
	ctx    context.Context
	r      *DingoNodeReconciler
	ns     string
	keys   *devnet.PoolKeys
	agent  *fakeagent.Agent
	served []byte // the counter-4 opcert the agent serves
	tpl    corev1.PodTemplateSpec
}

func newAgentBundleEnv(t *testing.T, ns string) *agentBundleEnv {
	t.Helper()
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, ns)

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	delete(data, keyFileKES)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-keys", Namespace: ns},
		Data:       data,
	}))
	servedEnv, err := keys.IssueOpCert(4, 0)
	require.NoError(t, err)
	served, err := opcert.Payload(servedEnv)
	require.NoError(t, err)

	certPEM, keyPEM, spki := fakeagent.ClientCert(t)
	agent := fakeagent.New(t, fakeagent.Profile{
		Name:         "pool-a",
		ColdVKey:     keys.ColdVKey,
		ControlSPKI:  [][sha256.Size]byte{spki},
		ActiveVKey:   keys.KESVKey,
		ActiveIssue:  4,
		ActiveOpCert: served,
	})
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "control-tls", Namespace: ns},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}))
	clientCrt, clientKey := testClientCertPair(t)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-client", Namespace: ns},
		Data: map[string][]byte{
			"tls.crt": clientCrt,
			"tls.key": clientKey,
		},
	}))

	poolID := hex.EncodeToString(keys.PoolID)
	dn := blockProducerNode("bp", ns, "pool-keys", poolID)
	dn.Spec.Image.Tag = agentImageTag
	bp := dn.Spec.BlockProducer
	bp.NodeToClient.Enabled = true
	bp.Rotation = dingov1alpha1.RotationSpec{
		Mode:               dingov1alpha1.RotationModeAuto,
		RenewBeforePeriods: 8,
		ReloadInPlace:      true,
		ColdSigner: dingov1alpha1.ColdSignerSpec{
			Type:          dingov1alpha1.ColdSignerBursa,
			Endpoint:      "https://bursa-signer.example:8090",
			MTLSSecretRef: "signer-mtls",
			ColdKeyHash:   poolID,
		},
	}
	bp.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:                    agent.Addr,
		ServerSPKISHA256:           agent.ServerSPKI,
		ClientCertSecretRef:        "agent-client",
		ControlClientCertSecretRef: "control-tls",
		ControlProfile:             "pool-a",
	}
	require.NoError(t, c.Create(ctx, dn))
	r := reconcilerFor(c)
	reconcile(t, ctx, r, "bp", ns)

	e := &agentBundleEnv{
		c: c, ctx: ctx, r: r, ns: ns, keys: keys, agent: agent,
		served: served, tpl: podTemplate(t, ctx, c, "bp", ns),
	}
	// The agent serving a newer opcert for the same cold key does not stop
	// the counter-3 bundle: the node takes the agent's.
	keysValid := e.condition(t, condKeysValid)
	require.NotNil(t, keysValid)
	require.Equal(t, metav1.ConditionTrue, keysValid.Status, keysValid.Message)
	return e
}

func (e *agentBundleEnv) node(t *testing.T) *dingov1alpha1.DingoNode {
	t.Helper()
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, e.c.Get(
		e.ctx, types.NamespacedName{Name: "bp", Namespace: e.ns}, got,
	))
	return got
}

func (e *agentBundleEnv) condition(t *testing.T, kind string) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(e.node(t).Status.Conditions, kind)
}

// deliver reports the node forging with counter 4 and writes opCertEnv to the
// keys Secret, as an outside write of the bundle would.
func (e *agentBundleEnv) deliver(t *testing.T, opCertEnv []byte) {
	t.Helper()
	got := e.node(t)
	got.Status.OpCert.LoadedCounter = new(int64(4))
	require.NoError(t, e.c.Status().Update(e.ctx, got))
	s := &corev1.Secret{}
	require.NoError(t, e.c.Get(
		e.ctx, types.NamespacedName{Name: "pool-keys", Namespace: e.ns}, s,
	))
	s.Data[keyFileOpCert] = opCertEnv
	require.NoError(t, e.c.Update(e.ctx, s))
}

// A bundle whose opcert has the counter the node loaded but is not the one
// the agent serves is refused, and the pod is not rolled onto it: Dingo would
// refuse the agent on restart.
func TestBlockProducerSigningAgentRefusesUnservedOpCert(t *testing.T) {
	e := newAgentBundleEnv(t, "agent-unserved-ns")
	other, err := e.keys.IssueOpCert(4, 1)
	require.NoError(t, err)
	e.deliver(t, other)

	reconcile(t, e.ctx, e.r, "bp", e.ns)

	assert.Equal(t, e.tpl, podTemplate(t, e.ctx, e.c, "bp", e.ns),
		"the pod template does not move")
	got := e.node(t)
	if ir := got.Status.OpCert.InPlaceReload; ir != nil {
		assert.False(t, ir.Applied, "the bundle is not approved")
	}
	keysValid := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, keysValid)
	assert.Equal(t, metav1.ConditionFalse, keysValid.Status)
	assert.Equal(t, "OpCertRejected", keysValid.Reason)
	assert.Contains(t, keysValid.Message, "signing agent serves")
}

// A bundle carrying exactly the opcert the agent serves is approved without a
// roll, and is not checked against the agent again once approved.
func TestBlockProducerSigningAgentAcceptsServedOpCert(t *testing.T) {
	e := newAgentBundleEnv(t, "agent-served-ns")
	env, err := opcert.EncodeEnvelope(e.served)
	require.NoError(t, err)
	e.deliver(t, env)

	reconcile(t, e.ctx, e.r, "bp", e.ns)

	assert.Equal(t, e.tpl, podTemplate(t, e.ctx, e.c, "bp", e.ns))
	got := e.node(t)
	require.NotNil(t, got.Status.OpCert.InPlaceReload)
	assert.True(t, got.Status.OpCert.InPlaceReload.Applied)
	keysValid := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, keysValid)
	assert.Equal(t, metav1.ConditionTrue, keysValid.Status, keysValid.Message)

	before := len(e.agent.Commands())
	reconcile(t, e.ctx, e.r, "bp", e.ns)
	assert.Len(t, e.agent.Commands(), before,
		"an approved bundle does not ask the agent again")
}

// When the agent does not answer, a new bundle is held, not rolled out and
// not rejected, and the node is not marked Degraded.
func TestBlockProducerSigningAgentHoldsBundleWhileAgentSilent(t *testing.T) {
	e := newAgentBundleEnv(t, "agent-silent-ns")
	e.agent.SetInfoError("pool-a", "connection reset")
	env, err := opcert.EncodeEnvelope(e.served)
	require.NoError(t, err)
	e.deliver(t, env)

	reconcile(t, e.ctx, e.r, "bp", e.ns)

	assert.Equal(t, e.tpl, podTemplate(t, e.ctx, e.c, "bp", e.ns))
	got := e.node(t)
	if ir := got.Status.OpCert.InPlaceReload; ir != nil {
		assert.False(t, ir.Applied, "the bundle is not approved")
	}
	keysValid := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, keysValid)
	assert.Equal(t, metav1.ConditionFalse, keysValid.Status)
	assert.Equal(t, reasonOpCertUnverified, keysValid.Reason)
	assert.Contains(t, keysValid.Message, "connection reset")
	assert.False(t, meta.IsStatusConditionTrue(got.Status.Conditions, condDegraded))
}
