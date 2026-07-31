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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// withKey returns a copy of m with k set to v. Copying matters: mutating the
// shared good bundle would make subtest results order-dependent.
func withKey(
	m map[string][]byte,
	k string,
	v []byte,
) map[string][]byte {
	out := maps.Clone(m)
	out[k] = v
	return out
}

// rawKeyEnvelope renders raw as a cardano-cli text envelope of envType. It
// exists so the tests can produce envelopes that are structurally valid but
// hold the wrong amount of key material — something devnet.PoolKeys, which
// only ever emits correct keys, cannot do.
func rawKeyEnvelope(t *testing.T, envType string, raw []byte) []byte {
	t.Helper()
	payload, err := cbor.Encode(raw)
	require.NoError(t, err)
	out, err := json.Marshal(map[string]string{
		"type":        envType,
		"description": "test",
		"cborHex":     hex.EncodeToString(payload),
	})
	require.NoError(t, err)
	return out
}

// observedOnChain records an on-chain counter observation made age ago, for the
// node's own pool.
func observedOnChain(
	dn *dingov1alpha1.DingoNode,
	counter int64,
	age time.Duration,
) {
	at := metav1.NewTime(time.Now().Add(-age))
	dn.Status.OpCert.OnChainCounter = counter
	dn.Status.OpCert.OnChainCounterAt = &at
	if bp := dn.Spec.BlockProducer; bp != nil && bp.PoolID != "" {
		id, err := parsePoolID(bp.PoolID)
		if err == nil {
			dn.Status.OpCert.OnChainCounterPoolID = id.String()
		}
	}
}

func TestValidateKeysSecret(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	goodData, err := keys.SecretData(3, 5)
	require.NoError(t, err)

	// A second, unrelated pool: its opcert is internally consistent and
	// verifies against its own cold key, but it is not this pool's.
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	otherData, err := other.SecretData(3, 5)
	require.NoError(t, err)

	node := func(mutate func(*dingov1alpha1.DingoNode)) *dingov1alpha1.DingoNode {
		dn := &dingov1alpha1.DingoNode{
			Spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer,
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					PoolID:            hex.EncodeToString(keys.PoolID),
					SlotsPerKESPeriod: 120,
					MaxKESEvolutions:  62,
					Keys: dingov1alpha1.KeysSpec{
						SecretRef: "keys",
					},
				},
			},
			Status: dingov1alpha1.DingoNodeStatus{
				KES:    dingov1alpha1.KESStatus{CurrentPeriod: 6},
				OpCert: dingov1alpha1.OpCertStatus{OnDiskCounter: 3},
			},
		}
		if mutate != nil {
			mutate(dn)
		}
		return dn
	}

	tests := []struct {
		name    string
		data    map[string][]byte
		dn      *dingov1alpha1.DingoNode
		wantErr string
	}{
		{
			name: "valid",
			data: goodData,
			dn:   node(nil),
		},
		{
			name: "valid with bech32 pool id",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.PoolID = common.
					NewBlake2b224(keys.PoolID).
					Bech32("pool")
			}),
		},
		{
			name: "valid without a pool id",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.PoolID = ""
			}),
		},
		{
			name:    "missing opcert",
			data:    map[string][]byte{"kes.skey": goodData["kes.skey"]},
			dn:      node(nil),
			wantErr: "opcert.cert",
		},
		{
			name:    "empty opcert",
			data:    withKey(goodData, "opcert.cert", []byte{}),
			dn:      node(nil),
			wantErr: "opcert.cert",
		},
		{
			name:    "malformed opcert",
			data:    withKey(goodData, "opcert.cert", []byte("{{{")),
			dn:      node(nil),
			wantErr: "parse",
		},
		{
			name:    "opcert signed by another cold key",
			data:    withKey(goodData, "opcert.cert", otherData["opcert.cert"]),
			dn:      node(nil),
			wantErr: "pool",
		},
		{
			name:    "malformed kes skey",
			data:    withKey(goodData, "kes.skey", []byte("not-json")),
			dn:      node(nil),
			wantErr: "kes.skey",
		},
		{
			name:    "vrf and kes keys swapped",
			data:    withKey(goodData, "vrf.skey", goodData["kes.skey"]),
			dn:      node(nil),
			wantErr: "vrf.skey",
		},
		{
			// The assisted-rotation mistake this check exists for: a new
			// opcert delivered without the matching KES key. Everything else
			// about the bundle is valid, so only the vkey binding catches it.
			name:    "kes skey does not match the opcert",
			data:    withKey(goodData, "kes.skey", otherData["kes.skey"]),
			dn:      node(nil),
			wantErr: "does not match the opcert's KES verification key",
		},
		{
			// Must be refused on length before kes.PublicKey is reached: it
			// slices the key data at fixed offsets and would panic.
			name: "truncated kes skey",
			data: withKey(goodData, "kes.skey", rawKeyEnvelope(
				t,
				"KesSigningKey_ed25519_kes_2^6",
				make([]byte, 100),
			)),
			dn:      node(nil),
			wantErr: "expected 608",
		},
		{
			name: "oversized kes skey",
			data: withKey(goodData, "kes.skey", rawKeyEnvelope(
				t,
				"KesSigningKey_ed25519_kes_2^6",
				make([]byte, kes.CardanoKesSecretKeySize+1),
			)),
			dn:      node(nil),
			wantErr: "expected 608",
		},
		{
			name: "wrong length vrf skey",
			data: withKey(goodData, "vrf.skey", rawKeyEnvelope(
				t,
				"VrfSigningKey_PraosVRF",
				make([]byte, 48),
			)),
			dn:      node(nil),
			wantErr: "vrf.skey holds 48 bytes",
		},
		{
			// The cardano-cli seed||pubkey form is the other accepted shape.
			name: "vrf skey with seed and public key",
			data: withKey(goodData, "vrf.skey", rawKeyEnvelope(
				t,
				"VrfSigningKey_PraosVRF",
				make([]byte, vrf.SeedSize+vrf.PublicKeySize),
			)),
			dn: node(nil),
		},
		{
			name: "pool id mismatch",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.PoolID = hex.EncodeToString(
					bytes.Repeat([]byte{0xab}, 28),
				)
			}),
			wantErr: "pool",
		},
		{
			name: "unparseable pool id",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.PoolID = "not-a-pool-id"
			}),
			wantErr: "pool",
		},
		{
			name: "counter regression",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.OpCert.OnDiskCounter = 9
			}),
			wantErr: "counter",
		},
		{
			// The authoritative floor: the chain has already accepted counter
			// 5, so a certificate numbered 3 cannot forge. onDiskCounter is
			// also 3 here, so only the on-chain check can catch this.
			name: "counter below a freshly observed on-chain counter",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				observedOnChain(dn, 5, 0)
			}),
			wantErr: "below the on-chain counter 5",
		},
		{
			// Equal is fine: the chain accepts a re-presentation of the counter
			// it already holds, and Dingo's own rule is counter >= on-chain.
			name: "counter equal to the on-chain counter",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				observedOnChain(dn, 3, 0)
			}),
		},
		{
			name: "counter above the on-chain counter",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				observedOnChain(dn, 2, 0)
			}),
		},
		{
			// Fail open. An unreachable node, an unsynced node or a pool that
			// has never minted all leave the counter unobserved, and none of
			// them is a reason to refuse a rotation: behaviour falls back to
			// the on-disk floor, which counter 3 satisfies.
			name: "unknown on-chain counter falls back to the on-disk floor",
			data: goodData,
			dn:   node(nil),
		},
		{
			// ... and the on-disk floor still bites when it should, with its
			// own message rather than the on-chain one.
			name: "on-disk regression still refused without an on-chain floor",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.OpCert.OnDiskCounter = 9
			}),
			wantErr: "regresses below the last observed counter 9",
		},
		{
			// A counter observed before the freshness window must not wedge a
			// legitimate rotation: the operator can no longer vouch that the
			// number still describes this pool on this chain.
			name: "stale on-chain counter does not wedge a rotation",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				observedOnChain(dn, 9, onChainCounterMaxAge+time.Minute)
			}),
		},
		{
			// A counter with no observation timestamp (e.g. written by an older
			// operator version) is treated as absent for the same reason.
			name: "on-chain counter without a timestamp is ignored",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.OpCert.OnChainCounter = 9
			}),
		},
		{
			// An observation of some *other* pool must not gate this pool's
			// rotation, however fresh it is: after a poolId edit the stored
			// number describes a pool that is no longer being validated.
			name: "on-chain counter observed for another pool is ignored",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				observedOnChain(dn, 9, 0)
				dn.Status.OpCert.OnChainCounterPoolID = hex.EncodeToString(
					bytes.Repeat([]byte{0xcd}, poolIDSize),
				)
			}),
		},
		{
			// The lower (future-dated) bound only applies when the counter is
			// not moving forward: counter 3 with onDiskCounter 3 is not a
			// rotation, so a start period ahead of the observed one is refused.
			name: "future start period rejected when the counter stands still",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.KES.CurrentPeriod = 1 // opcert starts at 5
			}),
			wantErr: "period",
		},
		{
			// The other leg: status.kes.currentPeriod froze at 1 (metrics
			// endpoint unreachable) while a genuine rotation delivered counter
			// 3 over the accepted counter 2. Refusing this would leave the node
			// forging on keys it can no longer renew, so it must be accepted.
			name: "future start period accepted when the counter moves forward",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.KES.CurrentPeriod = 1
				dn.Status.OpCert.OnDiskCounter = 2
			}),
		},
		{
			name: "opcert expired",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				// start 5 + maxEvolutions 62 = 67; current well past it
				dn.Status.KES.CurrentPeriod = 200
			}),
			wantErr: "period",
		},
		{
			// Expiry is unconditional, unlike the lower bound: a forward
			// counter does not buy a dead opcert a pod roll.
			name: "expired opcert rejected even when the counter moves forward",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Status.KES.CurrentPeriod = 200
				dn.Status.OpCert.OnDiskCounter = 2
			}),
			wantErr: "expired",
		},
		{
			name: "not a block producer",
			data: goodData,
			dn: node(func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer = nil
			}),
			wantErr: "block producer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := &corev1.Secret{Data: tt.data}
			state, err := validateKeysSecret(secret, tt.dn)
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, state)
				// The parsed counter and start period are what the reconciler
				// records in status; they must be the certificate's own values.
				assert.Equal(t, int64(3), state.Counter)
				assert.Equal(t, int64(5), state.KESPeriod)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, state)
		})
	}
}

// TestOnChainFloor pins the fail-open contract at the boundaries, which the
// table above exercises only through whole-bundle validation.
func TestOnChainFloor(t *testing.T) {
	now := time.Now()
	at := func(age time.Duration) *metav1.Time {
		v := metav1.NewTime(now.Add(-age))
		return &v
	}
	// The node under validation, and an unrelated pool.
	const (
		poolHex  = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c"
		otherHex = "1102030405060708090a0b0c0d0e0f101112131415161718191a1b1c"
	)

	tests := []struct {
		name      string
		specPool  string
		status    dingov1alpha1.OpCertStatus
		wantFloor int64
		wantOK    bool
	}{
		{
			name:     "nothing observed",
			specPool: poolHex,
			status:   dingov1alpha1.OpCertStatus{},
		},
		{
			name:     "counter without a timestamp",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterPoolID: poolHex,
			},
		},
		{
			name:     "timestamp without a counter",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounterAt:     at(0),
				OnChainCounterPoolID: poolHex,
			},
		},
		{
			// Zero is indistinguishable from unset on an omitempty field, and a
			// floor of zero would reject nothing anyway.
			name:     "zero counter is absent, not a floor of zero",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       0,
				OnChainCounterAt:     at(0),
				OnChainCounterPoolID: poolHex,
			},
		},
		{
			name:     "fresh observation",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterAt:     at(time.Minute),
				OnChainCounterPoolID: poolHex,
			},
			wantFloor: 7,
			wantOK:    true,
		},
		{
			name:     "at the freshness limit",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterAt:     at(onChainCounterMaxAge),
				OnChainCounterPoolID: poolHex,
			},
			wantFloor: 7,
			wantOK:    true,
		},
		{
			name:     "past the freshness limit",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterAt:     at(onChainCounterMaxAge + time.Second),
				OnChainCounterPoolID: poolHex,
			},
		},
		{
			// After a poolId edit the stored counter describes a pool that is no
			// longer being validated, however fresh it is.
			name:     "observed for another pool",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterAt:     at(time.Minute),
				OnChainCounterPoolID: otherHex,
			},
		},
		{
			// Written by an operator version that did not record provenance.
			name:     "no recorded pool",
			specPool: poolHex,
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:   7,
				OnChainCounterAt: at(time.Minute),
			},
		},
		{
			name:     "spec has no pool id",
			specPool: "",
			status: dingov1alpha1.OpCertStatus{
				OnChainCounter:       7,
				OnChainCounterAt:     at(time.Minute),
				OnChainCounterPoolID: poolHex,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dn := &dingov1alpha1.DingoNode{
				Spec: dingov1alpha1.DingoNodeSpec{
					BlockProducer: &dingov1alpha1.BlockProducerSpec{
						PoolID: tt.specPool,
					},
				},
				Status: dingov1alpha1.DingoNodeStatus{OpCert: tt.status},
			}
			floor, ok := onChainFloor(dn, now)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantFloor, floor)
		})
	}
}

func TestValidateKeysSecretSkipsPeriodChecksWithoutStatus(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(0, 40)
	require.NoError(t, err)

	// A fresh node has no KES status yet; period checks must not fire on zero.
	dn := &dingov1alpha1.DingoNode{
		Spec: dingov1alpha1.DingoNodeSpec{
			Role: dingov1alpha1.RoleBlockProducer,
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				SlotsPerKESPeriod: 120,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: "keys"},
			},
		},
	}
	state, err := validateKeysSecret(&corev1.Secret{Data: data}, dn)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, int64(0), state.Counter)
	assert.Equal(t, int64(40), state.KESPeriod)
}

// TestValidateKeysSecretAcceptsRotatedBundle covers the assisted-rotation happy
// path: a fresh KES key with a counter one above the last observed value.
func TestValidateKeysSecretAcceptsRotatedBundle(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, keys.RotateKES(rand.Reader))
	data, err := keys.SecretData(1, 2)
	require.NoError(t, err)

	dn := &dingov1alpha1.DingoNode{
		Spec: dingov1alpha1.DingoNodeSpec{
			Role: dingov1alpha1.RoleBlockProducer,
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				PoolID:            hex.EncodeToString(keys.PoolID),
				SlotsPerKESPeriod: 120,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: "keys"},
			},
		},
		Status: dingov1alpha1.DingoNodeStatus{
			KES:    dingov1alpha1.KESStatus{CurrentPeriod: 2},
			OpCert: dingov1alpha1.OpCertStatus{OnDiskCounter: 0},
		},
	}
	state, err := validateKeysSecret(&corev1.Secret{Data: data}, dn)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, int64(1), state.Counter)
}
