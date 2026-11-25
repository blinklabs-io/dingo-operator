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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/coldsigner"
	"github.com/blinklabs-io/dingo-operator/internal/kesagent"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/dingo-operator/internal/test/fakeagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
	return newAgentRotateFixtureKeys(
		t, func(_ *devnet.PoolKeys, p *fakeagent.Profile) {
			if mutate != nil {
				mutate(p)
			}
		},
	)
}

// newAgentRotateFixtureKeys is newAgentRotateFixture with the pool's keys
// passed to mutate, for a profile that needs an opcert the pool signed.
func newAgentRotateFixtureKeys(
	t *testing.T,
	mutate func(*devnet.PoolKeys, *fakeagent.Profile),
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
		mutate(f.keys, &p)
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

// An agent retains its staged KES key after the signer refuses it. Retrying
// must offer that exact key again so a Bursa per-key grant can take effect.
func TestAutoRotateThroughAgentReusesBursaGrantedKey(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.dn.Spec.BlockProducer.Rotation.ColdSigner.Type =
		dingov1alpha1.ColdSignerBursa
	f.issuer.err = errors.New("KES verification key is not granted")
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)

	require.Len(t, f.issuer.vkeys, 1)
	first := append([]byte(nil), f.issuer.vkeys[0]...)
	assert.Equal(t, first, f.agent.Staged("pool-a"))
	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Contains(t, cond.Message, hex.EncodeToString(first))

	f.issuer.err = nil
	f.r.issueMu.Lock()
	at := f.r.issueFailures[client.ObjectKeyFromObject(f.dn)]
	at.at = at.at.Add(-issueRetryInterval)
	f.r.issueFailures[client.ObjectKeyFromObject(f.dn)] = at
	f.r.issueMu.Unlock()
	f.r.autoRotate(ctx, f.dn)

	require.Len(t, f.issuer.vkeys, 2)
	assert.Equal(t, first, f.issuer.vkeys[1])
	var generated int
	for _, command := range f.agent.Commands() {
		if command.Command == "gen-staged-key" {
			generated++
		}
	}
	assert.Equal(t, 1, generated)
	active, issue := f.agent.Active("pool-a")
	assert.Equal(t, first, active)
	assert.Equal(t, uint64(4), issue)
}

func attestingFixture(t *testing.T) *agentRotateFixture {
	t.Helper()
	f := newAgentRotateFixture(t, func(p *fakeagent.Profile) { p.Attest = true })
	f.dn.Spec.BlockProducer.Rotation.ColdSigner.Type =
		dingov1alpha1.ColdSignerBursa
	return f
}

func (f *agentRotateFixture) retryNow() {
	f.r.issueMu.Lock()
	defer f.r.issueMu.Unlock()
	key := client.ObjectKeyFromObject(f.dn)
	at := f.r.issueFailures[key]
	at.at = at.at.Add(-issueRetryInterval)
	f.r.issueFailures[key] = at
}

func (f *agentRotateFixture) generated() int {
	var n int
	for _, c := range f.agent.Commands() {
		if c.Command == "gen-staged-key" {
			n++
		}
	}
	return n
}

// attests asserts att is the attestation the fake agent gave for vkey.
func attests(t *testing.T, att *kesagent.StagedKeyAttestation, vkey []byte) {
	t.Helper()
	require.NotNil(t, att, "the signer was sent no attestation")
	assert.Equal(t, "pool-a", att.Profile)
	assert.Equal(t, vkey, att.KESVKey)
	assert.Equal(t, fakeagent.AttestationSignature(vkey), att.Signature)
}

// The attestation the agent returns with a staged key reaches the Bursa
// signer, which authorizes the key through it.
func TestAutoRotateThroughAgentSendsAttestation(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)

	f.r.autoRotate(context.Background(), f.dn)

	require.Len(t, f.issuer.atts, 1)
	attests(t, f.issuer.atts[0], f.issuer.vkeys[0])
}

// Info does not report the attestation, so a retry that reuses the staged key
// sends the one the operator kept from gen-staged-key.
func TestAutoRotateThroughAgentReuseKeepsAttestation(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.err = errors.New("attestation refused")
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)
	f.issuer.err = nil
	f.retryNow()
	f.r.autoRotate(ctx, f.dn)

	require.Len(t, f.issuer.atts, 2)
	assert.Equal(t, f.issuer.vkeys[0], f.issuer.vkeys[1])
	attests(t, f.issuer.atts[1], f.issuer.vkeys[1])
	assert.Equal(t, 1, f.generated())
}

// A staged key whose attestation the operator no longer holds is not offered
// bare: the agent stages a new key and attests that one.
func TestAutoRotateThroughAgentRestagesWithoutKeptAttestation(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.err = errors.New("signer unreachable")
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)
	// An operator restart loses what it kept.
	f.r.stagedMu.Lock()
	f.r.stagedAttestations = nil
	f.r.stagedMu.Unlock()
	f.issuer.err = nil
	f.retryNow()
	f.r.autoRotate(ctx, f.dn)

	require.Len(t, f.issuer.atts, 2)
	assert.NotEqual(t, f.issuer.vkeys[0], f.issuer.vkeys[1])
	attests(t, f.issuer.atts[1], f.issuer.vkeys[1])
	assert.Equal(t, 2, f.generated())
}

// bursaGrant applies the rule of Bursa's opcert endpoint: a request with an
// attestation is decided by the pinned agents alone, one without by the
// granted KES keys alone.
func bursaGrant(
	f *agentRotateFixture,
	agentPinned, keyGranted bool,
) func([]byte, *kesagent.StagedKeyAttestation) error {
	return func(vkey []byte, att *kesagent.StagedKeyAttestation) error {
		if att != nil {
			if agentPinned &&
				bytes.Equal(att.SPKI, fakeagent.AttestationSPKI) &&
				bytes.Equal(att.Signature, fakeagent.AttestationSignature(vkey)) {
				return nil
			}
			return fmt.Errorf(
				"%w: attestation signer is not a trusted KES agent",
				coldsigner.ErrAttestationRefused,
			)
		}
		if keyGranted && bytes.Equal(vkey, f.agent.Staged("pool-a")) {
			return nil
		}
		return errors.New("caller is not authorized for this opcert")
	}
}

// Every attesting agent attests, so a signer whose grant lists only the KES
// key refuses the attestation; the same key is then offered without it.
func TestAutoRotateThroughAgentFallsBackToKESKeyGrant(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.authorize = bursaGrant(f, false, true)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	require.Len(t, f.issuer.atts, 2)
	attests(t, f.issuer.atts[0], f.issuer.vkeys[0])
	assert.Nil(t, f.issuer.atts[1])
	assert.Equal(t, f.issuer.vkeys[0], f.issuer.vkeys[1])
	assert.Equal(t, 1, f.generated())
}

// A grant that pins the agent accepts the attestation at once.
func TestAutoRotateThroughAgentAgentPinGrant(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.authorize = bursaGrant(f, true, false)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Len(t, f.issuer.atts, 1)
}

// With neither the agent pinned nor the key granted the rotation is refused
// and the message names the key to grant.
func TestAutoRotateThroughAgentRefusalNamesKey(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.authorize = bursaGrant(f, false, false)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.NotEqual(t, metav1.ConditionTrue, cond.Status)
	require.Len(t, f.issuer.atts, 2)
	assert.Contains(t, cond.Message, hex.EncodeToString(f.issuer.vkeys[0]))
}

// When both requests are refused, the message keeps why the attestation was
// refused: a misconfigured agent pin is otherwise reported only as a KES key
// that is not granted.
func TestAutoRotateThroughAgentRefusalKeepsAttestationReason(t *testing.T) {
	t.Parallel()
	f := attestingFixture(t)
	f.issuer.authorize = bursaGrant(f, false, false)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Contains(t, cond.Message, "not a trusted KES agent")
	assert.Contains(t, cond.Message, "not authorized for this opcert")
}

// An agent with no attestation identity sends none, and its staged key is
// still reused for an exact-key grant.
func TestAutoRotateThroughAgentWithoutAttestation(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.dn.Spec.BlockProducer.Rotation.ColdSigner.Type =
		dingov1alpha1.ColdSignerBursa
	f.issuer.err = errors.New("KES verification key is not granted")
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)
	f.issuer.err = nil
	f.retryNow()
	f.r.autoRotate(ctx, f.dn)

	assert.Equal(t, []*kesagent.StagedKeyAttestation{nil, nil}, f.issuer.atts)
	assert.Equal(t, f.issuer.vkeys[0], f.issuer.vkeys[1])
	assert.Equal(t, 1, f.generated())
}

// The attestation is for the Bursa signer alone.
func TestAutoRotateThroughAgentSecretSignerSendsNoAttestation(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, func(p *fakeagent.Profile) { p.Attest = true })

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, []*kesagent.StagedKeyAttestation{nil}, f.issuer.atts)
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

// An agent serving the next counter's opcert inside its renewal window is not
// adopted: a pool that forged no block since the last rotation still has the
// chain at counter 3, and adopting the same near-expiry opcert on every retry
// would never rotate. A fresh opcert at counter 4 for a new key is issued.
func TestAutoRotateThroughAgentDoesNotAdoptExpiringOpCert(t *testing.T) {
	t.Parallel()
	var served []byte
	f := newAgentRotateFixtureKeys(
		t, func(k *devnet.PoolKeys, p *fakeagent.Profile) {
			// Starts at period 100: 100+62-160 = 2 periods left, inside the
			// renewal window of 8.
			env, err := k.IssueOpCert(4, 100)
			require.NoError(t, err)
			oc, cold, err := opcert.Parse(env)
			require.NoError(t, err)
			served, err = opcert.EncodeCBOR(oc, cold)
			require.NoError(t, err)
			p.ActiveIssue = 4
			p.ActiveOpCert = served
		},
	)
	oldKey, _ := f.agent.Active("pool-a")

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls, "a fresh opcert is issued")
	cmds := []string{}
	for _, c := range f.agent.Commands() {
		cmds = append(cmds, c.Command)
	}
	assert.Equal(t, []string{"info", "gen-staged-key", "install-key"}, cmds)

	newKey, issue := f.agent.Active("pool-a")
	assert.NotEqual(t, oldKey, newKey, "a new key is generated")
	assert.Equal(t, uint64(4), issue)
	assert.NotEqual(t, served, f.agent.ActiveOpCert("pool-a"))

	inSecret, _, err := opcert.Parse(f.secret(t).Data[keyFileOpCert])
	require.NoError(t, err)
	assert.Equal(t, uint64(4), inSecret.IssueNumber)
	assert.Equal(t, uint64(160), inSecret.KesPeriod)
	assert.Equal(t, newKey, inSecret.KesVkey)
}

// failInstallThenReload runs a rotation whose Secret write fails after
// install-key, then moves the node to what a reloadInPlace node reports once it
// has loaded the agent's new opcert: counter 4 in a fresh KES window, so
// RotationDue is False. It returns the Secret as the failed write left it.
func failInstallThenReload(t *testing.T, f *agentRotateFixture) *corev1.Secret {
	t.Helper()
	f.deniedSecretUpdates(t, true)
	f.r.autoRotate(context.Background(), f.dn)
	_, issue := f.agent.Active("pool-a")
	require.Equal(t, uint64(4), issue)
	stale := f.secret(t)

	f.allowSecretUpdates(t)
	f.dn.Status.KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    160,
		OpCertKESPeriod:  160,
		ExpiryPeriod:     222,
		RemainingPeriods: 62,
	}
	f.dn.Status.OpCert.LoadedCounter = new(int64(4))
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionFalse,
		Reason: "KESWindowHealthy",
	})
	return stale
}

// assertAdoptedServed checks the Secret now carries the opcert the agent
// serves, and that adopting it issued and staged nothing.
func assertAdoptedServed(t *testing.T, f *agentRotateFixture, cmdsBefore int) {
	t.Helper()
	cond := f.issued(t)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls, "no second opcert is issued")
	for _, c := range f.agent.Commands()[cmdsBefore:] {
		assert.Equal(t, "info", c.Command, "no second key is staged or installed")
	}
	after := f.secret(t)
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
	inSecret, _, err := opcert.Parse(after.Data[keyFileOpCert])
	require.NoError(t, err)
	served, _, err := opcert.ParseCBOR(f.agent.ActiveOpCert("pool-a"))
	require.NoError(t, err)
	assert.Equal(t, served.KesVkey, inSecret.KesVkey)
	assert.Equal(t, served.ColdSignature, inSecret.ColdSignature)
	assert.NotContains(t, after.Data, keyFileKES)
}

// A lost Secret write is repaired even after the node has reloaded the new
// opcert in place and stopped reporting the rotation as due.
func TestAutoRotateThroughAgentAdoptsWhenNoLongerDue(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	failInstallThenReload(t, f)
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	cmdsBefore := len(f.agent.Commands())

	f.r.autoRotate(context.Background(), f.dn)

	assertAdoptedServed(t, f, cmdsBefore)
}

// Once the node forges with the new opcert the chain counter reaches it, and
// the Secret's opcert is below the chain: it is still repaired.
func TestAutoRotateThroughAgentAdoptsAfterChainAdvances(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	failInstallThenReload(t, f)
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	observedOnChain(f.dn, 4, time.Minute)
	cmdsBefore := len(f.agent.Commands())

	f.r.autoRotate(context.Background(), f.dn)

	assertAdoptedServed(t, f, cmdsBefore)
}

// A healthy node whose Secret matches what it loaded is not rotated: the agent
// is asked only whether its profile holds a key, and no condition is set.
func TestAutoRotateThroughAgentNotDueOnlyAsksForKeyState(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	f.dn.Status.OpCert.LoadedCounter = new(int64(3))
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionFalse,
		Reason: "KESWindowHealthy",
	})
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, []string{"info"}, f.commandNames())
	assert.Nil(t, f.issued(t))
	assert.Equal(t, before.Data, f.secret(t).Data)
	assert.Equal(t, 0, f.issuer.calls)
}

// A pending repair whose info fails waits issueRetryInterval like any failed
// attempt, instead of asking the agent on every reconcile.
func TestAutoRotateThroughAgentPendingAdoptionBacksOff(t *testing.T) {
	t.Parallel()
	f := newAgentRotateFixture(t, nil)
	failInstallThenReload(t, f)
	f.r.forgetIssueFailure(client.ObjectKeyFromObject(f.dn))
	f.agent.SetInfoError("pool-a", "connection reset")
	cmdsBefore := len(f.agent.Commands())

	f.r.autoRotate(context.Background(), f.dn)
	f.r.autoRotate(context.Background(), f.dn)

	assert.Len(t, f.agent.Commands()[cmdsBefore:], 1, "one info per retry interval")
	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "AgentInfoFailed", cond.Reason)
}

// The opcert the agent serves is written as served, not re-encoded: Dingo
// admits the agent only when the key file's opcert is byte-equal to it.
func TestServedOpCertSecretKeepsServedBytes(t *testing.T) {
	t.Parallel()
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	staged, err := keys.IssueOpCert(3, 0)
	require.NoError(t, err)
	next, err := keys.IssueOpCert(4, 0)
	require.NoError(t, err)
	canonical, err := opcert.Payload(next)
	require.NoError(t, err)
	// The same opcert with the KES vkey's length written in two bytes.
	require.Equal(t, []byte{0x82, 0x84, 0x58, 0x20}, canonical[:4])
	served := append([]byte{0x82, 0x84, 0x59, 0x00, 0x20}, canonical[4:]...)
	secret := &corev1.Secret{Data: map[string][]byte{keyFileOpCert: staged}}

	oc, got, err := servedOpCertSecret(&kesagent.Active{
		KESVKey:     keys.KESVKey,
		IssueNumber: 4,
		OpCert:      served,
	}, secret, keyFileOpCert)

	require.NoError(t, err)
	require.NotNil(t, oc, "the served opcert verifies")
	require.NotNil(t, got)
	payload, err := opcert.Payload(got.Data[keyFileOpCert])
	require.NoError(t, err)
	assert.Equal(t, served, payload)
}

// newKeylessFixture is an agent-signed Auto producer whose agent profile holds
// no key and whose keys Secret holds an opcert with most of its KES window
// left. RotationDue is False, as the node reports it.
func newKeylessFixture(t *testing.T) *agentRotateFixture {
	t.Helper()
	return newKeylessFixtureFor(t, true)
}

// newKeylessFixtureFor is newKeylessFixture with the profile keyless or, when
// keyless is false, serving its running key.
func newKeylessFixtureFor(t *testing.T, keyless bool) *agentRotateFixture {
	t.Helper()
	f := newAgentRotateFixture(t, func(p *fakeagent.Profile) {
		if keyless {
			p.ActiveVKey, p.ActiveIssue = nil, 0
		}
	})
	f.dn.Spec.BlockProducer.MaxKESEvolutions = 62
	f.dn.Status.KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    105,
		OpCertKESPeriod:  100,
		ExpiryPeriod:     162,
		RemainingPeriods: 57,
	}
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionFalse,
		Reason: "KESWindowHealthy",
	})
	return f
}

func (f *agentRotateFixture) commandNames() []string {
	var got []string
	for _, c := range f.agent.Commands() {
		got = append(got, c.Command)
	}
	return got
}

// An agent that lost its key serves no sign session, and the Secret's opcert
// being valid does not change that: a new key is issued without RotationDue.
func TestAutoRotateRekeysKeylessProfileBeforeDue(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond, "a keyless profile was not re-keyed")
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(t, 1, f.issuer.calls)
	assert.Equal(t,
		[]string{"info", "info", "gen-staged-key", "install-key"},
		f.commandNames(),
	)

	key, issue := f.agent.Active("pool-a")
	assert.NotEmpty(t, key)
	assert.Equal(t, uint64(4), issue)
	after := f.secret(t)
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
	assert.Equal(t, int64(105), state.KESPeriod)
	inSecret, _, err := opcert.Parse(after.Data[keyFileOpCert])
	require.NoError(t, err)
	assert.Equal(t, key, inSecret.KesVkey)
	assert.NotEqual(t, before.Data[keyFileOpCert], after.Data[keyFileOpCert])
	assert.NotContains(t, after.Data, keyFileKES)
}

// A reconcile whose scrape carried no KES data leaves status.kes as an earlier
// scrape had it and sets RotationDue Unknown. An opcert issued from that
// retained period could start behind the node's actual one, so the agent is
// not asked and nothing is issued.
func TestAutoRotateKeylessProfileNeedsKESObservedThisReconcile(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	setRotationDueUnknown(
		f.dn, "KESMetricsUnavailable", "KES metrics are not available",
	)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Empty(t, f.commandNames())
	assert.Nil(t, f.issued(t))
	assert.Zero(t, f.issuer.calls)
}

// An agent that reports an active key is left alone: the only command is the
// info that told it so.
func TestAutoRotateKeepsKeyedProfileBeforeDue(t *testing.T) {
	t.Parallel()
	f := newKeylessFixtureFor(t, false)
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, []string{"info"}, f.commandNames())
	assert.Zero(t, f.issuer.calls)
	assert.Nil(t, f.issued(t))
	assert.Equal(t, before.Data, f.secret(t).Data)
}

// A failed info is not a keyless profile: nothing is issued or staged.
func TestAutoRotateAgentInfoErrorDoesNotRekey(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	f.agent.SetInfoError("pool-a", "kesagent: info unavailable")
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, []string{"info"}, f.commandNames())
	assert.Zero(t, f.issuer.calls)
	assert.Nil(t, f.issued(t))
	assert.Equal(t, before.Data, f.secret(t).Data)
}

// The agent is asked at most once per keylessProbeInterval.
func TestAutoRotateKeylessProbeIsThrottled(t *testing.T) {
	t.Parallel()
	f := newKeylessFixtureFor(t, false)

	f.r.autoRotate(context.Background(), f.dn)
	f.r.autoRotate(context.Background(), f.dn)
	require.Equal(t, []string{"info"}, f.commandNames())

	key := client.ObjectKeyFromObject(f.dn)
	f.r.issueMu.Lock()
	f.r.keylessProbes[key] = f.r.keylessProbes[key].Add(-keylessProbeInterval)
	f.r.issueMu.Unlock()
	f.r.autoRotate(context.Background(), f.dn)
	assert.Equal(t, []string{"info", "info"}, f.commandNames())
}

// A re-key that failed waits issueRetryInterval before the agent is asked or
// the signer reached again.
func TestAutoRotateRekeyBacksOffAfterFailure(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	f.issuer.err = errors.New("signer unreachable")

	f.r.autoRotate(context.Background(), f.dn)
	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, "IssueFailed", cond.Reason)
	conns, calls := f.agent.Connections(), f.issuer.calls

	key := client.ObjectKeyFromObject(f.dn)
	f.r.issueMu.Lock()
	f.r.keylessProbes[key] = f.r.keylessProbes[key].Add(-keylessProbeInterval)
	f.r.issueMu.Unlock()
	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, conns, f.agent.Connections())
	assert.Equal(t, calls, f.issuer.calls)
}

// Without the on-chain counter nothing is issued, and the agent is not asked.
func TestAutoRotateKeylessNeedsOnChainCounter(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	f.dn.Status.OpCert.OnChainCounterAt = nil
	f.dn.Status.OpCert.OnChainCounter = -1

	f.r.autoRotate(context.Background(), f.dn)

	assert.Empty(t, f.agent.Commands())
	assert.Zero(t, f.issuer.calls)
}

// A node without a signing agent is not probed.
func TestAutoRotateWithoutAgentIgnoresKeyless(t *testing.T) {
	t.Parallel()
	f := newRotateFixture(t)
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionFalse,
		Reason: "KESHealthy",
	})
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Zero(t, f.issuer.calls)
	assert.Equal(t, before.Data, f.secret(t).Data)
}

// The staged bundle is valid and not yet due, but the agent restarted after
// install-key and before the node reloaded: RotationDue is still True and the
// early return that leaves a staged rotation to finish must not hide the lost
// key.
func TestAutoRotateRekeysKeylessProfileWithFreshStagedBundleWhileDue(
	t *testing.T,
) {
	t.Parallel()
	f := newKeylessFixture(t)
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionTrue,
		Reason: "KESExpiringSoon",
	})

	f.r.autoRotate(context.Background(), f.dn)

	cond := f.issued(t)
	require.NotNil(t, cond, "a keyless profile was not re-keyed")
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	key, issue := f.agent.Active("pool-a")
	assert.NotEmpty(t, key)
	assert.Equal(t, uint64(4), issue)
}

// A staged bundle that a keyed agent serves is still left to finish.
func TestAutoRotateKeepsKeyedProfileWithFreshStagedBundleWhileDue(
	t *testing.T,
) {
	t.Parallel()
	f := newKeylessFixtureFor(t, false)
	meta.SetStatusCondition(&f.dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionTrue,
		Reason: "KESExpiringSoon",
	})

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, []string{"info"}, f.commandNames())
	assert.Zero(t, f.issuer.calls)
	assert.Nil(t, f.issued(t))
}

// The period an opcert starts at is the node's reported one; the agent's info
// carries none, so without it nothing is issued and the agent is not asked.
func TestAutoRotateKeylessNeedsKESStatus(t *testing.T) {
	t.Parallel()
	f := newKeylessFixture(t)
	f.dn.Status.KES = dingov1alpha1.KESStatus{}

	f.r.autoRotate(context.Background(), f.dn)

	assert.Empty(t, f.agent.Commands())
	assert.Zero(t, f.issuer.calls)
	assert.Nil(t, f.issued(t))
}
