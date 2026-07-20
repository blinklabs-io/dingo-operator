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

// Package coldsigner abstracts cold-key signing for operational-certificate
// issuance. The operator builds the opcert signable bytes
// (KES verification key || big-endian counter || big-endian KES period) and a
// Signer returns the 64-byte Ed25519 signature. The cold key itself never has
// to reside in the operator: the Bursa backend delegates to an out-of-cluster
// signer service (HSM/Vault/KMS/SOPS backed). Only the dev/testnet Secret
// backend holds key material in the cluster.
package coldsigner

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
)

// SignatureSize is the length of an Ed25519 signature.
const SignatureSize = ed25519.SignatureSize

// ErrNotImplemented is returned by backends whose upstream support is not yet
// available. See CLAUDE.md (Design & roadmap -> Upstream dependencies).
var ErrNotImplemented = errors.New("cold signer backend not yet implemented")

// Signer produces an Ed25519 signature over an operational-certificate signable.
type Signer interface {
	// Sign returns a 64-byte Ed25519 signature over signable. Implementations
	// must not mutate signable.
	Sign(ctx context.Context, signable []byte) ([]byte, error)
}

// SecretSigner signs with a cold Ed25519 private key held in memory. It is
// intended for dev/testnet only: production block producers should keep the
// cold key out of the cluster and use a remote backend (see BursaSigner).
type SecretSigner struct {
	key ed25519.PrivateKey
}

// NewSecretSigner builds a SecretSigner from a 64-byte Ed25519 private key or a
// 32-byte Ed25519 seed.
func NewSecretSigner(keyOrSeed []byte) (*SecretSigner, error) {
	switch len(keyOrSeed) {
	case ed25519.PrivateKeySize:
		return &SecretSigner{key: ed25519.PrivateKey(keyOrSeed)}, nil
	case ed25519.SeedSize:
		return &SecretSigner{key: ed25519.NewKeyFromSeed(keyOrSeed)}, nil
	default:
		return nil, fmt.Errorf(
			"invalid cold key length %d: want %d (key) or %d (seed)",
			len(keyOrSeed), ed25519.PrivateKeySize, ed25519.SeedSize,
		)
	}
}

// Sign implements Signer.
func (s *SecretSigner) Sign(_ context.Context, signable []byte) ([]byte, error) {
	if len(s.key) != ed25519.PrivateKeySize {
		return nil, errors.New("cold signer not initialized")
	}
	return ed25519.Sign(s.key, signable), nil
}

// PublicKey returns the cold verification key.
func (s *SecretSigner) PublicKey() ed25519.PublicKey {
	return s.key.Public().(ed25519.PublicKey)
}

// BursaConfig configures a remote Bursa cold-signer.
type BursaConfig struct {
	// Endpoint is the Bursa signer base URL (https).
	Endpoint string
	// ColdKeyHash (blake2b224 of the cold vkey) selects the key at the signer.
	ColdKeyHash string
	// TLS holds the client mTLS material (cert/key/ca). Loaded from a Secret by
	// the caller.
	CACert     []byte
	ClientCert []byte
	ClientKey  []byte
}

// BursaSigner delegates cold signing to a Bursa signer service over mTLS. The
// cold key never enters the cluster.
//
// NOTE: issuing an opcert requires a Bursa `/v1/sign` request type that signs
// an arbitrary digest/opcert-signable. That request type is not yet released
// upstream (blinklabs-io/bursa#592); until it lands Sign returns
// ErrNotImplemented. The configuration and wiring are in place so the
// controller can adopt it without a schema or interface change.
type BursaSigner struct {
	cfg BursaConfig
}

// NewBursaSigner constructs a BursaSigner. It validates configuration but does
// not dial the endpoint until Sign is called.
func NewBursaSigner(cfg BursaConfig) (*BursaSigner, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("bursa signer requires an endpoint")
	}
	return &BursaSigner{cfg: cfg}, nil
}

// Sign implements Signer.
func (b *BursaSigner) Sign(_ context.Context, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf(
		"%w: bursa remote opcert signing awaits upstream /v1/sign opcert type",
		ErrNotImplemented,
	)
}

// Ensure the backends satisfy the interface.
var (
	_ Signer = (*SecretSigner)(nil)
	_ Signer = (*BursaSigner)(nil)
)
