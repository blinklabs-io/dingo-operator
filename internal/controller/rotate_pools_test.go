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
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/coldsigner"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/fakeagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// poolRotateFixture is an Auto-rotation block producer with two pools whose
// KES keys are held by one fake agent, one profile each. Pool a has two KES
// periods left and pool b is not due, until a test says otherwise.
type poolRotateFixture struct {
	pools []testPool
	dn    *dingov1alpha1.DingoNode
	c     client.Client
	r     *DingoNodeReconciler
	agent *fakeagent.Agent
	// issuers holds the cold signer of each pool, in pool order.
	issuers []countingIssuer
}

func newPoolRotateFixture(t *testing.T) *poolRotateFixture {
	t.Helper()
	const ns = "pools"
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", ns, 0, pools...)
	bp := dn.Spec.BlockProducer
	bp.Rotation = dingov1alpha1.RotationSpec{
		Mode:               dingov1alpha1.RotationModeAuto,
		RenewBeforePeriods: 8,
		ReloadInPlace:      true,
		ColdSigner: dingov1alpha1.ColdSignerSpec{
			Type:          dingov1alpha1.ColdSignerBursa,
			Endpoint:      "https://signer.example:8443",
			MTLSSecretRef: "signer-tls",
		},
	}
	bp.NodeToClient.Enabled = true
	bp.SigningAgent.ControlClientCertSecretRef = "control-tls"

	certPEM, keyPEM, spki := fakeagent.ClientCert(t)
	profiles := make([]fakeagent.Profile, 0, len(pools))
	objs := []client.Object{&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "control-tls", Namespace: ns},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}}
	issuers := make([]countingIssuer, len(pools))
	byPool := map[string]int{}
	for i, p := range pools {
		// Both Secrets hold an opcert with two periods left, so only the
		// pool's own KES status can keep pool b from being rotated.
		start, remaining := uint64(100), int64(2)
		if i == 1 {
			remaining = 52
		}
		data, err := p.keys.SecretData(3, start)
		require.NoError(t, err)
		delete(data, keyFileKES)
		objs = append(objs, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: p.secret, Namespace: ns},
			Data:       data,
		})
		profiles = append(profiles, fakeagent.Profile{
			Name:        p.profile,
			ColdVKey:    p.keys.ColdVKey,
			ControlSPKI: [][sha256.Size]byte{spki},
			ActiveVKey:  p.keys.KESVKey,
			ActiveIssue: 3,
		})

		cold, err := coldsigner.NewSecretSigner(p.keys.ColdSKey)
		require.NoError(t, err)
		issuers[i] = countingIssuer{inner: cold}
		byPool[p.id] = i

		at := metav1.NewTime(time.Now().Add(-time.Minute))
		ps := poolStatus(dn, p.id)
		ps.OnChainCounter = 3
		ps.OnChainCounterAt = &at
		ps.KES = dingov1alpha1.KESStatus{
			CurrentPeriod:    160,
			OpCertKESPeriod:  int64(start),
			ExpiryPeriod:     int64(start) + 62,
			RemainingPeriods: remaining,
		}
	}
	agent := fakeagent.New(t, profiles...)
	bp.SigningAgent.Address = agent.Addr
	bp.SigningAgent.ServerSPKISHA256 = agent.ServerSPKI

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	r := &DingoNodeReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		// The signer is chosen by the cold key hash the operator asks for,
		// which must be the pool's own.
		ColdSigner: func(
			_ context.Context, n *dingov1alpha1.DingoNode,
		) (coldsigner.Issuer, error) {
			id := n.Spec.BlockProducer.Rotation.ColdSigner.ColdKeyHash
			if i, ok := byPool[id]; ok {
				return &issuers[i], nil
			}
			return nil, assert.AnError
		},
	}
	return &poolRotateFixture{
		pools: pools, dn: dn, c: c, r: r, agent: agent, issuers: issuers,
	}
}

func (f *poolRotateFixture) secret(t *testing.T, p testPool) *corev1.Secret {
	t.Helper()
	s := &corev1.Secret{}
	require.NoError(t, f.c.Get(
		context.Background(),
		types.NamespacedName{Name: p.secret, Namespace: "pools"},
		s,
	))
	return s
}

// makeDue gives a pool an opcert with two KES periods left, in its Secret and
// in the status the node reported.
func (f *poolRotateFixture) makeDue(t *testing.T, p testPool) {
	t.Helper()
	data, err := p.keys.SecretData(3, 100)
	require.NoError(t, err)
	delete(data, keyFileKES)
	s := f.secret(t, p)
	s.Data = data
	require.NoError(t, f.c.Update(context.Background(), s))
	poolStatus(f.dn, p.id).KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    160,
		OpCertKESPeriod:  100,
		ExpiryPeriod:     162,
		RemainingPeriods: 2,
	}
}

func (f *poolRotateFixture) commandsFor(profile string) []string {
	var out []string
	for _, c := range f.agent.Commands() {
		if c.Profile == profile {
			out = append(out, c.Command)
		}
	}
	return out
}

func TestAutoRotatePoolsIssuesOnlyDuePool(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	a, b := f.pools[0], f.pools[1]
	beforeB := f.secret(t, b)
	beforeA := f.secret(t, a)

	f.r.autoRotate(context.Background(), f.dn)

	// One rotation on pool a's profile, and nothing at all on pool b's.
	assert.Equal(t,
		[]string{"info", "gen-staged-key", "install-key"},
		f.commandsFor(a.profile),
	)
	assert.Empty(t, f.commandsFor(b.profile))
	assert.Len(t, f.agent.Commands(), 3)
	assert.Equal(t, 1, f.issuers[0].calls)
	assert.Zero(t, f.issuers[1].calls)

	afterA := f.secret(t, a)
	assert.NotEqual(t, beforeA.Data[keyFileOpCert], afterA.Data[keyFileOpCert])
	assert.Equal(t, beforeA.Data[keyFileVRF], afterA.Data[keyFileVRF])
	assert.NotContains(t, afterA.Data, keyFileKES)
	staged, _, err := opcert.Parse(afterA.Data[keyFileOpCert])
	require.NoError(t, err)
	assert.Equal(t, uint64(4), staged.IssueNumber)
	assert.Equal(t, uint64(160), staged.KesPeriod)
	installed, _ := f.agent.Active(a.profile)
	assert.Equal(t, staged.KesVkey, installed)

	afterB := f.secret(t, b)
	assert.Equal(t, beforeB.Data, afterB.Data)
	assert.Equal(t, beforeB.ResourceVersion, afterB.ResourceVersion)

	cond := meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Contains(t, cond.Message, a.id)
	assert.NotContains(t, cond.Message, b.id)
}

// A pool's staged bundle stops a second rotation of that pool while the node
// still reports the old opcert.
func TestAutoRotatePoolsIssuesOnce(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)

	f.r.autoRotate(context.Background(), f.dn)
	staged := f.secret(t, f.pools[0])
	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, 1, f.issuers[0].calls)
	assert.Len(t, f.agent.Commands(), 3)
	assert.Equal(t, staged.Data, f.secret(t, f.pools[0]).Data)
}

func TestAutoRotatePoolsNoneDueLeavesAgentAlone(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	for _, p := range f.pools {
		poolStatus(f.dn, p.id).KES.RemainingPeriods = 40
	}

	f.r.autoRotate(context.Background(), f.dn)

	assert.Zero(t, f.agent.Connections())
	assert.Empty(t, f.agent.Commands())
	assert.Nil(t, meta.FindStatusCondition(
		f.dn.Status.Conditions, condOpCertIssued,
	))
}

// One pool's unreadable counter refuses that pool alone.
func TestAutoRotatePoolsUnknownCounterIsolated(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	a, b := f.pools[0], f.pools[1]
	f.makeDue(t, b)
	stale := metav1.NewTime(time.Now().Add(-2 * onChainCounterMaxAge))
	poolStatus(f.dn, a.id).OnChainCounterAt = &stale
	beforeA := f.secret(t, a)
	beforeB := f.secret(t, b)

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, beforeA.Data, f.secret(t, a).Data)
	assert.Empty(t, f.commandsFor(a.profile))
	assert.Zero(t, f.issuers[0].calls)

	afterB := f.secret(t, b)
	assert.NotEqual(t, beforeB.Data[keyFileOpCert], afterB.Data[keyFileOpCert])
	assert.Equal(t,
		[]string{"info", "gen-staged-key", "install-key"},
		f.commandsFor(b.profile),
	)
	assert.Equal(t, 1, f.issuers[1].calls)

	cond := meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "OnChainCounterUnknown", cond.Reason)
	assert.Contains(t, cond.Message, a.id)
	assert.Contains(t, cond.Message, b.id)
}

// A failed issue for one pool is retried after the backoff without holding
// back another pool.
func TestAutoRotatePoolsBacksOffPerPool(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	f.makeDue(t, f.pools[1])
	// Pools are attempted in pool ID order: the first one fails, so a backoff
	// shared with the second would hold the second back too.
	first, second := 0, 1
	if f.pools[1].id < f.pools[0].id {
		first, second = 1, 0
	}
	f.issuers[first].err = assert.AnError
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)
	assert.Equal(t, 1, f.issuers[first].calls)
	assert.Equal(t, 1, f.issuers[second].calls)

	f.r.autoRotate(ctx, f.dn)
	assert.Equal(t, 1, f.issuers[first].calls, "inside its backoff")
	cond := meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "IssueFailed", cond.Reason)
}

// A pool whose Secret write failed after install-key is repaired once the node
// has reloaded the agent's opcert in place and the pool is no longer due: a
// restart would otherwise load the Secret's older opcert, which the agent no
// longer serves.
func TestAutoRotatePoolsAdoptsWhenNoLongerDue(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	a, b := f.pools[0], f.pools[1]
	deny := true
	denied := interceptor.NewClient(
		f.c.(client.WithWatch),
		interceptor.Funcs{Update: func(
			ctx context.Context, c client.WithWatch, o client.Object,
			opts ...client.UpdateOption,
		) error {
			var uo client.UpdateOptions
			uo.ApplyOptions(opts)
			if deny && len(uo.DryRun) == 0 && o.GetName() == a.secret {
				return apierrors.NewForbidden(
					corev1.Resource("secrets"), o.GetName(),
					errors.New("update not granted"),
				)
			}
			return c.Update(ctx, o, opts...)
		}},
	)
	f.r.Client, f.r.APIReader = denied, denied
	ctx := context.Background()

	f.r.autoRotate(ctx, f.dn)
	_, issue := f.agent.Active(a.profile)
	require.Equal(t, uint64(4), issue)
	stale, _, err := opcert.Parse(f.secret(t, a).Data[keyFileOpCert])
	require.NoError(t, err)
	require.Equal(t, uint64(3), stale.IssueNumber)

	// The node loads the agent's opcert in place: counter 4 in a fresh KES
	// window, so pool a is no longer due.
	deny = false
	ps := poolStatus(f.dn, a.id)
	ps.KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    160,
		OpCertKESPeriod:  160,
		ExpiryPeriod:     222,
		RemainingPeriods: 62,
	}
	ps.LoadedCounter = new(int64(4))
	poolStatus(f.dn, b.id).LoadedCounter = new(int64(3))
	f.r.forgetNodeIssueFailures(client.ObjectKeyFromObject(f.dn))
	beforeB := f.secret(t, b)
	cmdsBefore := len(f.agent.Commands())

	f.r.autoRotate(ctx, f.dn)

	assert.Equal(t, 1, f.issuers[0].calls, "no second opcert is issued")
	assert.Zero(t, f.issuers[1].calls)
	cmds := f.agent.Commands()[cmdsBefore:]
	require.NotEmpty(t, cmds)
	for _, c := range cmds {
		assert.Equal(t, a.profile, c.Profile)
		assert.Equal(t, "info", c.Command)
	}
	inSecret, _, err := opcert.Parse(f.secret(t, a).Data[keyFileOpCert])
	require.NoError(t, err)
	served, _, err := opcert.ParseCBOR(f.agent.ActiveOpCert(a.profile))
	require.NoError(t, err)
	assert.Equal(t, uint64(4), inSecret.IssueNumber)
	assert.Equal(t, served.KesVkey, inSecret.KesVkey)
	assert.Equal(t, served.ColdSignature, inSecret.ColdSignature)
	assert.Equal(t, beforeB.Data, f.secret(t, b).Data)

	cond := meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Contains(t, cond.Message, a.id)
}

// poolAutoNode is an Auto-rotation pool-list node with the bursa cold signer,
// one pool per name.
func poolAutoNode(
	t *testing.T,
	network string,
	names ...string,
) *dingov1alpha1.DingoNode {
	t.Helper()
	pools := newTestPools(t, names...)
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.Network = network
	bp := dn.Spec.BlockProducer
	bp.Rotation = dingov1alpha1.RotationSpec{
		Mode:               dingov1alpha1.RotationModeAuto,
		RenewBeforePeriods: 8,
		ReloadInPlace:      true,
		ColdSigner: dingov1alpha1.ColdSignerSpec{
			Type:          dingov1alpha1.ColdSignerBursa,
			Endpoint:      "https://signer.example:8443",
			MTLSSecretRef: "signer-tls",
		},
	}
	bp.NodeToClient.Enabled = true
	bp.SigningAgent.ControlClientCertSecretRef = "control-tls"
	return dn
}

func TestValidatePoolsAllowsAuto(t *testing.T) {
	t.Parallel()
	dn := poolAutoNode(t, "preview", "a", "b")
	bp := dn.Spec.BlockProducer

	require.NoError(t, validatePools(dn, bp))
	require.NoError(t, validateSpec(dn))

	// The Leios refusal is not relaxed by Auto.
	dn.Spec.Network = "musashi"
	err := validatePools(dn, bp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Leios")
}

func TestValidateSpecPoolsAutoNeedsItsPrerequisites(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*dingov1alpha1.BlockProducerSpec)
		wantErr string
	}{
		{
			name: "control certificate",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ControlClientCertSecretRef = ""
			},
			wantErr: "requires controlClientCertSecretRef",
		},
		{
			name: "node-to-client",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.NodeToClient.Enabled = false
			},
			wantErr: "nodeToClient.enabled",
		},
		{
			name: "control profile beside per-pool profiles",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ControlProfile = "x"
			},
			wantErr: "controlProfile",
		},
		{
			name: "cold key hash beside pools",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.Rotation.ColdSigner = dingov1alpha1.ColdSignerSpec{
					Type:          dingov1alpha1.ColdSignerBursa,
					Endpoint:      "https://signer.example:8443",
					MTLSSecretRef: "signer-tls",
					ColdKeyHash:   bp.Pools[0].PoolID,
				}
			},
			wantErr: "coldKeyHash",
		},
		{
			name:   "bursa signer",
			mutate: func(*dingov1alpha1.BlockProducerSpec) {},
		},
		{
			name: "secret signer for several pools",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.Rotation.ColdSigner = dingov1alpha1.ColdSignerSpec{
					Type: dingov1alpha1.ColdSignerSecret, SecretRef: "cold",
				}
			},
			wantErr: "holds one cold key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dn := poolAutoNode(t, "preview", "a", "b")
			tt.mutate(dn.Spec.BlockProducer)
			err := validateSpec(dn)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// The secret signer keeps the cold key in the cluster, so it is refused on
// mainnet for a pool list as for a single pool, unless coldSigner.allowMainnet
// opts in.
func TestAutoRotatePoolsSecretColdSignerMainnetRefused(t *testing.T) {
	t.Parallel()
	secretSigner := func(network string) *dingov1alpha1.DingoNode {
		dn := poolAutoNode(t, network, "a")
		dn.Spec.BlockProducer.Rotation.ColdSigner =
			dingov1alpha1.ColdSignerSpec{
				Type: dingov1alpha1.ColdSignerSecret, SecretRef: "cold",
			}
		return dn
	}
	require.NoError(t, validateSpec(secretSigner("preview")))

	byName := secretSigner("mainnet")
	err := validateSpec(byName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret cold signer on mainnet")

	magic := int64(mainnetMagic)
	byMagic := secretSigner("custom")
	byMagic.Spec.NetworkMagic = &magic
	byMagic.Spec.ConfigRef = "genesis"
	err = validateSpec(byMagic)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret cold signer on mainnet")

	// coldSigner.allowMainnet opts a pool list in as it does a single pool.
	byMagic.Spec.BlockProducer.Rotation.ColdSigner.AllowMainnet = true
	require.NoError(t, validateSpec(byMagic))
}

func TestForgetNodeIssueFailuresClearsItsPools(t *testing.T) {
	t.Parallel()
	r := &DingoNodeReconciler{}
	node := types.NamespacedName{Name: "bp", Namespace: "ns"}
	other := types.NamespacedName{Name: "bp-2", Namespace: "ns"}
	elsewhere := types.NamespacedName{Name: "bp", Namespace: "other"}
	keys := []types.NamespacedName{
		node,
		{Name: "bp/pool-a", Namespace: "ns"},
		{Name: "bp/pool-b", Namespace: "ns"},
		other,
		{Name: "bp-2/pool-a", Namespace: "ns"},
		elsewhere,
		{Name: "bp/pool-a", Namespace: "other"},
	}
	for _, k := range keys {
		r.recordIssueFailure(k, "IssueFailed", "x")
	}

	r.forgetNodeIssueFailures(node)

	for _, k := range keys[:3] {
		_, ok := r.lastIssueFailure(k)
		assert.False(t, ok, k.String())
	}
	for _, k := range keys[3:] {
		_, ok := r.lastIssueFailure(k)
		assert.True(t, ok, k.String())
	}
}

// hookIssuer runs before ahead of every signature, so a test can act while a
// pool is being issued for.
type hookIssuer struct {
	inner  coldsigner.Issuer
	before func()
}

func (h hookIssuer) SignOpCertFromChain(
	ctx context.Context,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*coldsigner.OpCertResult, error) {
	h.before()
	return h.inner.SignOpCertFromChain(ctx, kesVkey, chainCounter, kesPeriod)
}

// The on-chain counter's age is judged when each pool is issued for: a reading
// that passes onChainCounterMaxAge while an earlier pool is issued is not used.
func TestAutoRotatePoolsJudgesCounterAgePerPool(t *testing.T) {
	t.Parallel()
	f := newPoolRotateFixture(t)
	f.makeDue(t, f.pools[1])
	first, second := 0, 1
	if f.pools[1].id < f.pools[0].id {
		first, second = 1, 0
	}
	now := time.Now()
	f.r.Now = func() time.Time { return now }
	read := metav1.NewTime(now.Add(-onChainCounterMaxAge + 30*time.Second))
	for _, p := range f.pools {
		poolStatus(f.dn, p.id).OnChainCounterAt = &read
	}
	signers := f.r.ColdSigner
	f.r.ColdSigner = func(
		ctx context.Context, n *dingov1alpha1.DingoNode,
	) (coldsigner.Issuer, error) {
		inner, err := signers(ctx, n)
		if err != nil {
			return nil, err
		}
		return hookIssuer{inner: inner, before: func() {
			now = now.Add(time.Minute)
		}}, nil
	}

	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, 1, f.issuers[first].calls)
	assert.Zero(t, f.issuers[second].calls)
	assert.Empty(t, f.commandsFor(f.pools[second].profile))
	cond := meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "OnChainCounterUnknown", cond.Reason)
	assert.Contains(t, cond.Message, f.pools[second].id)
}
