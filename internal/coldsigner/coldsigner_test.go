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

package coldsigner

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretSignerRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     []byte
		message   []byte
		wantValid bool
	}{
		{
			name:      "from full private key",
			input:     priv,
			message:   []byte("opcert-signable"),
			wantValid: true,
		},
		{
			name:      "from seed",
			input:     priv.Seed(),
			message:   []byte("msg"),
			wantValid: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSecretSigner(tt.input)
			require.NoError(t, err)
			sig, err := s.Sign(context.Background(), tt.message)
			require.NoError(t, err)
			assert.Len(t, sig, SignatureSize)
			assert.Equal(
				t,
				tt.wantValid,
				ed25519.Verify(pub, tt.message, sig),
			)
			assert.Equal(t, pub, s.PublicKey())
		})
	}
}

func TestSecretSignerInvalidLength(t *testing.T) {
	_, err := NewSecretSigner([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestBursaSignerNotImplemented(t *testing.T) {
	_, err := NewBursaSigner(BursaConfig{})
	assert.Error(t, err, "empty endpoint must fail")

	s, err := NewBursaSigner(BursaConfig{Endpoint: "https://signer:8443"})
	require.NoError(t, err)
	_, err = s.Sign(context.Background(), []byte("x"))
	assert.ErrorIs(t, err, ErrNotImplemented)
}

func TestSecretSignerSignOpCertFromChain(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	s, err := NewSecretSigner(priv.Seed())
	require.NoError(t, err)
	kesVkey := make([]byte, 32)
	kesVkey[0] = 7

	var issuer Issuer = s
	res, err := issuer.SignOpCertFromChain(
		context.Background(), kesVkey, 4, 120,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), res.IssueCounter)
	assert.Equal(t, pub, res.ColdVkey)
	assert.True(t, ed25519.Verify(
		pub, lcommon.OpCertSignableBytes(kesVkey, 5, 120), res.Signature,
	))

	_, err = s.SignOpCertFromChain(
		context.Background(), kesVkey[:31], 4, 120,
	)
	require.Error(t, err)
	_, err = s.SignOpCertFromChain(
		context.Background(), kesVkey, math.MaxUint64, 120,
	)
	require.Error(t, err)
}
