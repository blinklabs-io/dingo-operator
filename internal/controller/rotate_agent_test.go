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
	"errors"
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/dingo-operator/internal/test/fakeagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// agentRotateFixture is an Auto-rotation block producer whose KES key is held
// by a fake agent, with its keys Secret carrying no kes.skey.
type agentRotateFixture struct {
	*rotateFixture
	agent *fakeagent.Agent
}

// newAgentRotateFixture serves the pool's running key (counter 3) from a fake
// agent that lets the operator's client certificate control profile "pool-a".
func newAgentRotateFixture(
	t *testing.T,
	mutate func(*fakeagent.Profile),
) *agentRotateFixture {
	t.Helper()
	f := newRotateFixture(t)
	ctx := context.Background()

	s := f.secret(t)
	delete(s.Data, keyFileKES)
	require.NoError(t, f.c.Update(ctx, s))

	certPEM, keyPEM, spki := fakeagent.ClientCert(t)
	p := fakeagent.Profile{
		Name:        "pool-a",
		ColdVKey:    f.keys.ColdVKey,
		ControlSPKI: [][sha256.Size]byte{spki},
		ActiveVKey:  f.keys.KESVKey,
		ActiveIssue: 3,
	}
	if mutate != nil {
		mutate(&p)
	}
	agent := fakeagent.New(t, p)
	require.NoError(t, f.c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "control-tls", Namespace: "pools"},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}))
	f.dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:                    agent.Addr,
		ServerSPKISHA256:           agent.ServerSPKI,
		ControlClientCertSecretRef: "control-tls",
		ControlProfile:             "pool-a",
	}
	return &agentRotateFixture{rotateFixture: f, agent: agent}
}

func TestAutoRotateThroughAgent(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	before := f.secret(t)
	oldKey, _ := f.agent.Active("pool-a")

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls)

	all := f.agent.Commands()
	require.Len(t, all, 3)
	assert.Equal(t, "info", all[0].Command)
	cmds := all[1:]
	assert.Equal(t, "gen-staged-key", cmds[0].Command)
	assert.Equal(t, "install-key", cmds[1].Command)
	for _, c := range all {
		assert.Equal(t, "pool-a", c.Profile)
	}

	// The opcert the agent installed is the one in the Secret, issued at the
	// on-chain counter plus one for the key the agent generated.
	installed, _, err := opcert.ParseCBOR(cmds[1].OpCert)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), installed.IssueNumber)
	assert.Equal(t, uint64(160), installed.KesPeriod)
	newKey, issue := f.agent.Active("pool-a")
	assert.Equal(t, installed.KesVkey, newKey)
	assert.NotEqual(t, oldKey, newKey)
	assert.Equal(t, uint64(4), issue)

	after := f.secret(t)
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
	inSecret, _, err := opcert.Parse(after.Data[keyFileOpCert])
	require.NoError(t, err)
	assert.Equal(t, installed.KesVkey, inSecret.KesVkey)
	assert.Equal(t, installed.ColdSignature, inSecret.ColdSignature)

	// No KES private key reaches the Secret, and nothing else changes.
	assert.NotContains(t, after.Data, keyFileKES)
	assert.Equal(t, before.Data[keyFileVRF], after.Data[keyFileVRF])
	assert.NotEqual(t, before.Data[keyFileOpCert], after.Data[keyFileOpCert])
	assert.Len(t, after.Data, len(before.Data))
}

// Rotation stays due across reconciles until the node reports the new opcert;
// the staged bundle in the Secret must stop a second rotation.
func TestAutoRotateThroughAgentIssuesOnce(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)

	f.r.autoRotate(context.Background(), f.dn)
	staged := f.secret(t)
	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, 1, f.issuer.calls)
	assert.Len(t, f.agent.Commands(), 3)
	assert.Equal(t, staged.Data, f.secret(t).Data)
}

// A rotation that fails at any step leaves the previous key serving and the
// Secret as it was, and is not retried before issueRetryInterval.
func TestAutoRotateThroughAgentFailureKeepsOldKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		agent      func(*fakeagent.Profile)
		mutate     func(*agentRotateFixture)
		wantReason string
		wantMsg    string
		// wantCmds are the commands the agent may have received.
		wantCmds  []string
		wantCalls int
	}{
		{
			name: "profile outside the permitted set",
			mutate: func(f *agentRotateFixture) {
				f.dn.Spec.BlockProducer.SigningAgent.ControlProfile = "pool-b"
			},
			wantReason: "AgentInfoFailed",
			wantMsg:    fakeagent.ErrRoleNotPermitted,
		},
		{
			name: "agent server key does not match the pin",
			mutate: func(f *agentRotateFixture) {
				f.dn.Spec.BlockProducer.SigningAgent.ServerSPKISHA256 =
					strings.Repeat("ab", sha256.Size)
			},
			wantReason: "AgentInfoFailed",
			wantMsg:    "does not match the pin",
		},
		{
			name:       "agent refuses the opcert",
			agent:      func(p *fakeagent.Profile) { p.InstallError = "kesagent: opcert expired" },
			wantReason: "AgentInstallFailed",
			wantMsg:    "opcert expired",
			wantCmds:   []string{"info", "gen-staged-key", "install-key"},
			wantCalls:  1,
		},
		{
			name: "cold signer fails after staging",
			mutate: func(f *agentRotateFixture) {
				f.issuer.err = errors.New("signer unreachable")
			},
			wantReason: "IssueFailed",
			wantMsg:    "signer unreachable",
			wantCmds:   []string{"info", "gen-staged-key"},
			wantCalls:  1,
		},
		{
			name: "control certificate Secret missing",
			mutate: func(f *agentRotateFixture) {
				f.dn.Spec.BlockProducer.SigningAgent.ControlClientCertSecretRef =
					"absent"
			},
			wantReason: "ControlCertInvalid",
			wantMsg:    "not found",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newAgentRotateFixture(t, tc.agent)
			if tc.mutate != nil {
				tc.mutate(f)
			}
			before := f.secret(t)
			oldKey, oldIssue := f.agent.Active("pool-a")

			f.r.autoRotate(context.Background(), f.dn)

			cond := f.issued(t)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Equal(t, tc.wantReason, cond.Reason)
			assert.Contains(t, cond.Message, tc.wantMsg)
			assert.Equal(t, tc.wantCalls, f.issuer.calls)

			var got []string
			for _, c := range f.agent.Commands() {
				got = append(got, c.Command)
			}
			assert.Equal(t, tc.wantCmds, got)

			assert.Equal(t, before.Data, f.secret(t).Data)
			key, issue := f.agent.Active("pool-a")
			assert.Equal(t, oldKey, key)
			assert.Equal(t, oldIssue, issue)

			// Within the retry interval the agent and the signer are left alone.
			conns, calls := f.agent.Connections(), f.issuer.calls
			f.r.autoRotate(context.Background(), f.dn)
			assert.Equal(t, conns, f.agent.Connections())
			assert.Equal(t, calls, f.issuer.calls)
			assert.Equal(t, before.Data, f.secret(t).Data)
		})
	}
}

// deniedSecretUpdates rebuilds f's client over the same Secrets with an
// Update that fails; when dryRunOK is set a dry run still succeeds.
func (f *agentRotateFixture) deniedSecretUpdates(t *testing.T, dryRunOK bool) {
	t.Helper()
	var objs []client.Object
	for _, name := range []string{"keys", "control-tls"} {
		s := &corev1.Secret{}
		require.NoError(t, f.c.Get(
			context.Background(),
			types.NamespacedName{Name: name, Namespace: "pools"}, s,
		))
		s.ResourceVersion = ""
		objs = append(objs, s)
	}
	denied := fake.NewClientBuilder().WithScheme(f.c.Scheme()).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context, c client.WithWatch, o client.Object,
				opts ...client.UpdateOption,
			) error {
				var uo client.UpdateOptions
				uo.ApplyOptions(opts)
				if dryRunOK && len(uo.DryRun) > 0 {
					return c.Update(ctx, o, opts...)
				}
				return apierrors.NewForbidden(
					corev1.Resource("secrets"), "keys",
					errors.New("update not granted"),
				)
			},
		}).Build()
	f.c, f.r.Client, f.r.APIReader = denied, denied, denied
}

// Without update access to the keys Secret the write would fail after the
// agent had promoted its new key; the operator checks the write first.
func TestAutoRotateThroughAgentChecksSecretWriteFirst(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.deniedSecretUpdates(t, false)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Empty(t, f.agent.Commands())
	assert.Zero(t, f.issuer.calls)
	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, "SecretUpdateFailed", cond.Reason)
}

// allowSecretUpdates restores an Update that works, over the Secrets as they
// stand.
func (f *agentRotateFixture) allowSecretUpdates(t *testing.T) {
	t.Helper()
	var objs []client.Object
	for _, name := range []string{"keys", "control-tls"} {
		s := &corev1.Secret{}
		require.NoError(t, f.c.Get(
			context.Background(),
			types.NamespacedName{Name: name, Namespace: "pools"}, s,
		))
		s.ResourceVersion = ""
		objs = append(objs, s)
	}
	ok := fake.NewClientBuilder().WithScheme(f.c.Scheme()).
		WithObjects(objs...).Build()
	f.c, f.r.Client, f.r.APIReader = ok, ok, ok
}

// A write that fails after install-key leaves the agent serving the new key
// while the Secret holds the old opcert. The retry must publish the opcert the
// agent serves: a second counter-4 opcert would not be offered to a node that
// already took the first, and that node would stop forging.
func TestAutoRotateThroughAgentSecretWriteFailsAfterInstall(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.deniedSecretUpdates(t, true)
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "SecretUpdateFailed", cond.Reason)
	assert.Equal(t, before.Data, f.secret(t).Data)
	servedKey, issue := f.agent.Active("pool-a")
	assert.Equal(t, uint64(4), issue)
	servedCert := f.agent.ActiveOpCert("pool-a")
	require.NotEmpty(t, servedCert)

	// The retry after issueRetryInterval, with the write allowed again.
	f.allowSecretUpdates(t)
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	cmdsBefore := len(f.agent.Commands())
	f.r.autoRotate(context.Background(), f.dn)

	cond = f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls, "no second opcert is issued")
	for _, c := range f.agent.Commands()[cmdsBefore:] {
		assert.Equal(t, "info", c.Command, "no second key is staged or installed")
	}
	key, issue := f.agent.Active("pool-a")
	assert.Equal(t, servedKey, key)
	assert.Equal(t, uint64(4), issue)
	assert.Equal(t, servedCert, f.agent.ActiveOpCert("pool-a"))

	after := f.secret(t)
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
	inSecret, _, err := opcert.Parse(after.Data[keyFileOpCert])
	require.NoError(t, err)
	served, _, err := opcert.ParseCBOR(servedCert)
	require.NoError(t, err)
	assert.Equal(t, served.KesVkey, inSecret.KesVkey)
	assert.Equal(t, served.ColdSignature, inSecret.ColdSignature)
	assert.NotContains(t, after.Data, keyFileKES)
}

// An agent serving an opcert that is not the next one, or one the cold key did
// not sign, is not adopted: the rotation proceeds as usual.
func TestAutoRotateThroughAgentDoesNotAdoptForeignOpCert(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, func(p *fakeagent.Profile) {
		// The pool's running key at the counter the chain already holds.
		p.ActiveIssue = 3
		p.ActiveOpCert = []byte("not an opcert")
	})

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls)
	_, issue := f.agent.Active("pool-a")
	assert.Equal(t, uint64(4), issue)
}

// A retry whose info fails must not stage a fresh key: after a lost Secret
// write the agent may already serve a counter-4 opcert the node has taken, and
// a second one at the same counter would strand it. The attempt fails instead,
// and a later retry whose info answers adopts the served opcert.
func TestAutoRotateThroughAgentInfoFailureDoesNotRestage(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.deniedSecretUpdates(t, true)
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)
	servedKey, issue := f.agent.Active("pool-a")
	require.Equal(t, uint64(4), issue)
	servedCert := f.agent.ActiveOpCert("pool-a")

	f.allowSecretUpdates(t)
	f.agent.SetInfoError("pool-a", "connection reset")
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	cmdsBefore := len(f.agent.Commands())
	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AgentInfoFailed", cond.Reason)
	assert.Contains(t, cond.Message, "connection reset")
	assert.Equal(t, 1, f.issuer.calls, "no second opcert is issued")
	for _, c := range f.agent.Commands()[cmdsBefore:] {
		assert.Equal(t, "info", c.Command, "no second key is staged or installed")
	}
	key, issue := f.agent.Active("pool-a")
	assert.Equal(t, servedKey, key)
	assert.Equal(t, uint64(4), issue)
	assert.Equal(t, servedCert, f.agent.ActiveOpCert("pool-a"))
	assert.Equal(t, before.Data, f.secret(t).Data)

	f.agent.SetInfoError("pool-a", "")
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	f.r.autoRotate(context.Background(), f.dn)

	cond = f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls)
	inSecret, _, err := opcert.Parse(f.secret(t).Data[keyFileOpCert])
	require.NoError(t, err)
	served, _, err := opcert.ParseCBOR(servedCert)
	require.NoError(t, err)
	assert.Equal(t, served.ColdSignature, inSecret.ColdSignature)
}

// An agent serving the next counter under another pool's cold key is not
// adopted: only an opcert the Secret's cold key signed is published.
func TestAutoRotateThroughAgentDoesNotAdoptOtherColdKey(t *testing.T) {
	t.Parallel()
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	env, err := other.IssueOpCert(4, 160)
	require.NoError(t, err)
	oc, cold, err := opcert.Parse(env)
	require.NoError(t, err)
	foreign, err := opcert.EncodeCBOR(oc, cold)
	require.NoError(t, err)
	f := newAgentRotateFixture(t, func(p *fakeagent.Profile) {
		p.ActiveVKey = other.KESVKey
		p.ActiveIssue = 4
		p.ActiveOpCert = foreign
	})

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls, "the rotation proceeds")
	inSecret, _, err := opcert.Parse(f.secret(t).Data[keyFileOpCert])
	require.NoError(t, err)
	assert.NotEqual(t, oc.ColdSignature, inSecret.ColdSignature)
	key, _ := f.agent.Active("pool-a")
	assert.Equal(t, inSecret.KesVkey, key)
}
