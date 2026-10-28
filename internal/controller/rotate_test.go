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
	"encoding/hex"
	"errors"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/coldsigner"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// countingIssuer wraps an Issuer and counts the opcerts it was asked for.
type countingIssuer struct {
	inner coldsigner.Issuer
	err   error
	calls int
}

func (c *countingIssuer) SignOpCertFromChain(
	ctx context.Context,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*coldsigner.OpCertResult, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return c.inner.SignOpCertFromChain(ctx, kesVkey, chainCounter, kesPeriod)
}

type rotateFixture struct {
	keys   *devnet.PoolKeys
	dn     *dingov1alpha1.DingoNode
	c      client.Client
	r      *DingoNodeReconciler
	issuer *countingIssuer
}

// newRotateFixture builds a block producer on Auto rotation whose running
// opcert (counter 3, start period 100) has two KES periods left, with the
// pool's on-chain counter read a minute ago as 3.
func newRotateFixture(t *testing.T) *rotateFixture {
	t.Helper()
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 100)
	require.NoError(t, err)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "keys", Namespace: "pools"},
		Data:       data,
	}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	poolID := hex.EncodeToString(keys.PoolID)
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "pools"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "preview",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				PoolID:       poolID,
				Keys:         dingov1alpha1.KeysSpec{SecretRef: "keys"},
				NodeToClient: dingov1alpha1.NodeToClientSpec{Enabled: true},
				Rotation: dingov1alpha1.RotationSpec{
					Mode:               dingov1alpha1.RotationModeAuto,
					RenewBeforePeriods: 8,
				},
			},
		},
	}
	dn.Status.KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    160,
		OpCertKESPeriod:  100,
		ExpiryPeriod:     162,
		RemainingPeriods: 2,
	}
	dn.Status.OpCert.OnDiskCounter = 3
	observedOnChain(dn, 3, time.Minute)
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionTrue,
		Reason: "KESExpiringSoon",
	})

	cold, err := coldsigner.NewSecretSigner(keys.ColdSKey)
	require.NoError(t, err)
	issuer := &countingIssuer{inner: cold}
	r := &DingoNodeReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		ColdSigner: func(
			context.Context, *dingov1alpha1.DingoNode,
		) (coldsigner.Issuer, error) {
			return issuer, nil
		},
	}
	return &rotateFixture{keys: keys, dn: dn, c: c, r: r, issuer: issuer}
}

func (f *rotateFixture) secret(t *testing.T) *corev1.Secret {
	t.Helper()
	s := &corev1.Secret{}
	require.NoError(t, f.c.Get(
		context.Background(),
		types.NamespacedName{Name: "keys", Namespace: "pools"},
		s,
	))
	return s
}

func (f *rotateFixture) issued(t *testing.T) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(f.dn.Status.Conditions, condOpCertIssued)
}

func TestAutoRotateIssuesFromOnChainCounter(t *testing.T) {
	f := newRotateFixture(t)
	before := f.secret(t)

	f.r.autoRotate(context.Background(), f.dn)

	require.Equal(t, 1, f.issuer.calls)
	after := f.secret(t)
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
	assert.Equal(t, int64(160), state.KESPeriod)
	assert.NotEqual(t, before.Data[keyFileKES], after.Data[keyFileKES])
	assert.Equal(t, before.Data[keyFileVRF], after.Data[keyFileVRF])
	cond := f.issued(t)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
}

// The running pod keeps reporting the old opcert until it rolls and is
// scraped again, so RotationDue stays True across several reconciles. Only
// the first may issue.
func TestAutoRotateIssuesOnce(t *testing.T) {
	f := newRotateFixture(t)

	f.r.autoRotate(context.Background(), f.dn)
	staged := f.secret(t)
	f.r.autoRotate(context.Background(), f.dn)

	assert.Equal(t, 1, f.issuer.calls)
	assert.Equal(t, staged.Data, f.secret(t).Data)
}

// A Secret that only carries the dingo chart's node.cert keeps that name.
func TestAutoRotateKeepsNodeCertName(t *testing.T) {
	f := newRotateFixture(t)
	s := f.secret(t)
	s.Data[keyFileNodeCert] = s.Data[keyFileOpCert]
	delete(s.Data, keyFileOpCert)
	require.NoError(t, f.c.Update(context.Background(), s))

	f.r.autoRotate(context.Background(), f.dn)

	after := f.secret(t)
	assert.Empty(t, after.Data[keyFileOpCert])
	state, err := validateKeysSecret(after, f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)
}

func TestAutoRotateSkips(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rotateFixture)
	}{
		{
			name: "assisted mode",
			mutate: func(f *rotateFixture) {
				f.dn.Spec.BlockProducer.Rotation.Mode =
					dingov1alpha1.RotationModeAssisted
			},
		},
		{
			name: "rotation not due",
			mutate: func(f *rotateFixture) {
				meta.SetStatusCondition(
					&f.dn.Status.Conditions,
					metav1.Condition{
						Type: condRotationDue, Status: metav1.ConditionFalse,
						Reason: "KESHealthy",
					},
				)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newRotateFixture(t)
			tc.mutate(f)
			before := f.secret(t)

			f.r.autoRotate(context.Background(), f.dn)

			assert.Zero(t, f.issuer.calls)
			assert.Equal(t, before.Data, f.secret(t).Data)
			assert.Nil(t, f.issued(t))
		})
	}
}

func TestAutoRotateRefuses(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*testing.T, *rotateFixture)
		wantReason string
		wantCalls  int
	}{
		{
			name: "on-chain counter stale",
			mutate: func(_ *testing.T, f *rotateFixture) {
				observedOnChain(f.dn, 3, time.Hour)
			},
			wantReason: "OnChainCounterUnknown",
		},
		{
			name: "KES period not scraped",
			mutate: func(_ *testing.T, f *rotateFixture) {
				f.dn.Status.KES = dingov1alpha1.KESStatus{}
			},
			wantReason: "KESPeriodUnknown",
		},
		{
			name: "signer unavailable",
			mutate: func(_ *testing.T, f *rotateFixture) {
				f.r.ColdSigner = func(
					context.Context, *dingov1alpha1.DingoNode,
				) (coldsigner.Issuer, error) {
					return nil, errors.New("mtls secret missing")
				}
			},
			wantReason: "ColdSignerUnavailable",
		},
		{
			name: "signer refuses",
			mutate: func(_ *testing.T, f *rotateFixture) {
				f.issuer.err = coldsigner.ErrCounterConflict
			},
			wantReason: "IssueFailed",
			wantCalls:  1,
		},
		{
			// Another pool's cold key signs a self-consistent opcert: the
			// operator must not stage it.
			name: "signer holds another pool's key",
			mutate: func(t *testing.T, f *rotateFixture) {
				other, err := devnet.GeneratePoolKeys(rand.Reader)
				require.NoError(t, err)
				cold, err := coldsigner.NewSecretSigner(other.ColdSKey)
				require.NoError(t, err)
				f.issuer.inner = cold
			},
			wantReason: "IssuedBundleRejected",
			wantCalls:  1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newRotateFixture(t)
			tc.mutate(t, f)
			before := f.secret(t)

			f.r.autoRotate(context.Background(), f.dn)

			assert.Equal(t, tc.wantCalls, f.issuer.calls)
			assert.Equal(t, before.Data, f.secret(t).Data)
			cond := f.issued(t)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Equal(t, tc.wantReason, cond.Reason)
		})
	}
}

func TestIssuerForReadsSignerSecrets(t *testing.T) {
	f := newRotateFixture(t)
	f.r.ColdSigner = nil
	ctx := context.Background()
	coldEnv := rawKeyEnvelope(
		t, "StakePoolSigningKey_ed25519", f.keys.ColdSKey.Seed(),
	)
	require.NoError(t, f.c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cold", Namespace: "pools"},
		Data:       map[string][]byte{keyFileCold: coldEnv},
	}))
	f.dn.Spec.BlockProducer.Rotation.ColdSigner = dingov1alpha1.ColdSignerSpec{
		Type:      dingov1alpha1.ColdSignerSecret,
		SecretRef: "cold",
	}

	f.r.autoRotate(ctx, f.dn)

	state, err := validateKeysSecret(f.secret(t), f.dn)
	require.NoError(t, err)
	assert.Equal(t, int64(4), state.Counter)

	f.dn.Spec.BlockProducer.Rotation.ColdSigner = dingov1alpha1.ColdSignerSpec{
		Type:          dingov1alpha1.ColdSignerBursa,
		Endpoint:      "https://signer:8443",
		MTLSSecretRef: "absent",
	}
	_, err = f.r.issuerFor(ctx, f.dn)
	require.ErrorContains(t, err, "absent")

	// A Secret without a client certificate would sign in unauthenticated.
	f.dn.Spec.BlockProducer.Rotation.ColdSigner.MTLSSecretRef = "cold"
	_, err = f.r.issuerFor(ctx, f.dn)
	require.ErrorContains(t, err, signerSecretCert)
}
