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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretSignerRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	t.Run("from full private key", func(t *testing.T) {
		s, err := NewSecretSigner(priv)
		require.NoError(t, err)
		sig, err := s.Sign(context.Background(), []byte("opcert-signable"))
		require.NoError(t, err)
		assert.Len(t, sig, SignatureSize)
		assert.True(t, ed25519.Verify(pub, []byte("opcert-signable"), sig))
		assert.Equal(t, pub, s.PublicKey())
	})

	t.Run("from seed", func(t *testing.T) {
		s, err := NewSecretSigner(priv.Seed())
		require.NoError(t, err)
		sig, err := s.Sign(context.Background(), []byte("msg"))
		require.NoError(t, err)
		assert.True(t, ed25519.Verify(pub, []byte("msg"), sig))
	})

	t.Run("invalid length", func(t *testing.T) {
		_, err := NewSecretSigner([]byte{1, 2, 3})
		assert.Error(t, err)
	})
}

func TestBursaSignerNotImplemented(t *testing.T) {
	_, err := NewBursaSigner(BursaConfig{})
	assert.Error(t, err, "empty endpoint must fail")

	s, err := NewBursaSigner(BursaConfig{Endpoint: "https://signer:8443"})
	require.NoError(t, err)
	_, err = s.Sign(context.Background(), []byte("x"))
	assert.ErrorIs(t, err, ErrNotImplemented)
}
