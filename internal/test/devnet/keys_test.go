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

package devnet_test

import (
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratePoolKeys(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	assert.Len(t, pk.ColdVKey, 32)
	assert.Len(t, pk.PoolID, 28, "pool ID is blake2b-224 of the cold vkey")
	assert.Len(t, pk.KESVKey, 32)
	assert.NotEmpty(t, pk.VRFVKey)
}

// TestVRFVKeyMatchesSeed pins VRFVKey to the verification key that actually
// pairs with VRFSeed. vrf.KeyGen returns (publicKey, secretKey) and the
// secret key is the seed itself, so a swapped assignment yields a VRFVKey
// that is the right length but hashes to the wrong value in genesis — which
// the node only reports as a devnet warning.
func TestVRFVKeyMatchesSeed(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	assert.NotEqual(t, pk.VRFSeed, pk.VRFVKey,
		"VRFVKey must be the public key, not the seed")

	msg := []byte("devnet vrf pairing check")
	proof, output, err := vrf.Prove(pk.VRFSeed, msg)
	require.NoError(t, err)

	ok, err := vrf.Verify(pk.VRFVKey, proof, output, msg)
	require.NoError(t, err)
	assert.True(t, ok, "VRFVKey does not verify proofs made with VRFSeed")
}

func TestVRFVKeyHashIs32Bytes(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	h, err := keys.VRFVKeyHash()
	require.NoError(t, err)
	assert.Len(t, h, 32)
}

func TestGeneratePoolKeysIsFresh(t *testing.T) {
	a, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	b, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	assert.NotEqual(t, a.PoolID, b.PoolID)
	assert.NotEqual(t, a.KESVKey, b.KESVKey)
}

func TestIssueOpCertVerifiesAgainstColdKey(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	env, err := pk.IssueOpCert(0, 0)
	require.NoError(t, err)

	oc, coldVkey, err := opcert.Parse(env)
	require.NoError(t, err)
	assert.Equal(t, []byte(pk.ColdVKey), coldVkey)
	assert.Equal(t, pk.KESVKey, oc.KesVkey)
	assert.NoError(t, ledger.VerifyOpCertSignature(oc, coldVkey))
}

func TestIssueOpCertHonoursCounterAndPeriod(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	env, err := pk.IssueOpCert(3, 17)
	require.NoError(t, err)

	oc, _, err := opcert.Parse(env)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), oc.IssueNumber)
	assert.Equal(t, uint64(17), oc.KesPeriod)
}

func TestRotateKESChangesKeyButNotColdIdentity(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	oldKES := append([]byte(nil), pk.KESVKey...)
	oldPoolID := append([]byte(nil), pk.PoolID...)

	require.NoError(t, pk.RotateKES(rand.Reader))

	assert.NotEqual(t, oldKES, pk.KESVKey)
	assert.Equal(
		t,
		oldPoolID,
		pk.PoolID,
		"rotation must not change pool identity",
	)
}

func TestEnvelopeTypesMatchDingoKeystore(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	tests := []struct {
		name     string
		envelope func() ([]byte, error)
		wantType string
	}{
		{"kes", pk.KESSKeyEnvelope, "KesSigningKey_ed25519_kes_2^6"},
		{"vrf", pk.VRFSKeyEnvelope, "VrfSigningKey_PraosVRF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.envelope()
			require.NoError(t, err)
			var fields struct {
				Type    string `json:"type"`
				CborHex string `json:"cborHex"`
			}
			require.NoError(t, json.Unmarshal(raw, &fields))
			assert.Equal(t, tt.wantType, fields.Type)
			assert.NotEmpty(t, fields.CborHex)
		})
	}
}

func TestSecretDataKeyNames(t *testing.T) {
	pk, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)

	data, err := pk.SecretData(0, 0)
	require.NoError(t, err)

	// These filenames are what the operator points Dingo at.
	require.Contains(t, data, "kes.skey")
	require.Contains(t, data, "vrf.skey")
	require.Contains(t, data, "opcert.cert")
	assert.Len(t, data, 3)
}
