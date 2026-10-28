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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

// Issuer issues an operational certificate from the pool's on-chain opcert
// counter: the issue number is chainCounter + 1, the next one the chain
// accepts. Implementations return a result whose signature verifies under
// ColdVkey over the opcert signable bytes.
type Issuer interface {
	SignOpCertFromChain(
		ctx context.Context,
		kesVkey []byte,
		chainCounter, kesPeriod uint64,
	) (*OpCertResult, error)
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

// SignOpCertFromChain implements Issuer.
func (s *SecretSigner) SignOpCertFromChain(
	ctx context.Context,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*OpCertResult, error) {
	if err := checkOpCertInput(kesVkey, chainCounter); err != nil {
		return nil, err
	}
	issue := chainCounter + 1
	sig, err := s.Sign(
		ctx, lcommon.OpCertSignableBytes(kesVkey, issue, kesPeriod),
	)
	if err != nil {
		return nil, err
	}
	return &OpCertResult{
		IssueCounter: issue,
		Signature:    sig,
		ColdVkey:     s.PublicKey(),
	}, nil
}

func checkOpCertInput(kesVkey []byte, chainCounter uint64) error {
	if len(kesVkey) != kesVkeySize {
		return fmt.Errorf(
			"KES vkey must be %d bytes, got %d",
			kesVkeySize, len(kesVkey),
		)
	}
	if chainCounter == math.MaxUint64 {
		return errors.New("chain counter out of range")
	}
	return nil
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
// Bursa issues an opcert from the pool's on-chain counter and chooses the
// issue number itself (SignOpCertFromChain); it does not sign an arbitrary
// signable, so Sign returns ErrNotImplemented.
type BursaSigner struct {
	cfg    BursaConfig
	client *http.Client
}

const (
	kesVkeySize = 32
	// bursaTimeout bounds one signing request, a hardware signer included.
	bursaTimeout = 30 * time.Second
	// bursaMaxResponse bounds the response body read from the signer.
	bursaMaxResponse = 64 << 10
)

// ErrCounterConflict is returned when the signer refuses the chain counter
// because it is below one already reported for the cold key.
var ErrCounterConflict = errors.New("opcert counter conflict")

// OpCertResult is an operational certificate issued by the signer.
type OpCertResult struct {
	// IssueCounter is the issue number the signer chose (chain counter + 1).
	IssueCounter uint64
	// Signature is the cold-key signature over the opcert signable bytes.
	Signature []byte
	// ColdVkey is the cold verification key that signed.
	ColdVkey ed25519.PublicKey
}

type bursaOpCertRequest struct {
	Type         string `json:"type"`
	Key          string `json:"key"`
	KesVkey      string `json:"kes_vkey"`
	ChainCounter uint64 `json:"chain_counter"`
	KesPeriod    uint64 `json:"kes_period"`
}

type bursaOpCertResponse struct {
	Signature    string `json:"signature"`
	ColdVkey     string `json:"cold_vkey"`
	Key          string `json:"key"`
	IssueCounter uint64 `json:"issue_counter"`
}

// NewBursaSigner constructs a BursaSigner. It validates configuration and the
// TLS material but does not dial the endpoint until a request is made.
func NewBursaSigner(cfg BursaConfig) (*BursaSigner, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("bursa signer requires an endpoint")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf(
			"bursa signer endpoint must be an https URL: %q",
			cfg.Endpoint,
		)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(cfg.CACert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CACert) {
			return nil, errors.New(
				"bursa signer CA certificate is not valid PEM",
			)
		}
		tlsCfg.RootCAs = pool
	}
	if len(cfg.ClientCert) > 0 || len(cfg.ClientKey) > 0 {
		pair, err := tls.X509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("bursa signer client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	return &BursaSigner{
		cfg: cfg,
		client: &http.Client{
			Timeout:   bursaTimeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

// Sign implements Signer.
func (b *BursaSigner) Sign(_ context.Context, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf(
		"%w: bursa issues opcerts from the on-chain counter; "+
			"use SignOpCertFromChain",
		ErrNotImplemented,
	)
}

// SignOpCertFromChain asks Bursa for an opcert at chainCounter + 1, where
// chainCounter is the pool's on-chain opcert counter. A block header's counter
// must be at least the on-chain one and at most one more, so reissuing the
// same number is valid while the pool has not forged with it. The result is
// verified before it is returned: the issue number, the cold key against
// ColdKeyHash, and the signature over the opcert signable bytes.
func (b *BursaSigner) SignOpCertFromChain(
	ctx context.Context,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*OpCertResult, error) {
	if err := checkOpCertInput(kesVkey, chainCounter); err != nil {
		return nil, err
	}
	reqBody, err := json.Marshal(bursaOpCertRequest{
		Type:         "opcert",
		Key:          b.cfg.ColdKeyHash,
		KesVkey:      hex.EncodeToString(kesVkey),
		ChainCounter: chainCounter,
		KesPeriod:    kesPeriod,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimSuffix(b.cfg.Endpoint, "/")+"/v1/sign",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bursa signer: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, bursaMaxResponse))
	if err != nil {
		return nil, fmt.Errorf("bursa signer: read response: %w", err)
	}
	switch {
	case res.StatusCode == http.StatusConflict:
		return nil, fmt.Errorf(
			"%w: %s", ErrCounterConflict, bytes.TrimSpace(body),
		)
	case res.StatusCode != http.StatusOK:
		return nil, fmt.Errorf(
			"bursa signer: %s: %s", res.Status, bytes.TrimSpace(body),
		)
	}
	var out bursaOpCertResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bursa signer: decode response: %w", err)
	}
	return b.verify(out, kesVkey, chainCounter, kesPeriod)
}

func (b *BursaSigner) verify(
	out bursaOpCertResponse,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*OpCertResult, error) {
	if out.IssueCounter != chainCounter+1 {
		return nil, fmt.Errorf(
			"bursa signer issued counter %d for chain counter %d",
			out.IssueCounter, chainCounter,
		)
	}
	cold, err := hex.DecodeString(out.ColdVkey)
	if err != nil || len(cold) != ed25519.PublicKeySize {
		return nil, errors.New("bursa signer returned a malformed cold vkey")
	}
	hash := lcommon.Blake2b224Hash(cold)
	if !strings.EqualFold(hex.EncodeToString(hash[:]), b.cfg.ColdKeyHash) {
		return nil, errors.New(
			"bursa signer cold vkey does not match the cold key hash",
		)
	}
	sig, err := hex.DecodeString(out.Signature)
	if err != nil || len(sig) != SignatureSize {
		return nil, errors.New("bursa signer returned a malformed signature")
	}
	signable := lcommon.OpCertSignableBytes(
		kesVkey, out.IssueCounter, kesPeriod,
	)
	if !ed25519.Verify(cold, signable, sig) {
		return nil, errors.New(
			"bursa signer signature does not verify over the opcert",
		)
	}
	return &OpCertResult{
		IssueCounter: out.IssueCounter,
		Signature:    sig,
		ColdVkey:     cold,
	}, nil
}

// Ensure the backends satisfy the interface.
var (
	_ Signer = (*SecretSigner)(nil)
	_ Signer = (*BursaSigner)(nil)
	_ Issuer = (*SecretSigner)(nil)
	_ Issuer = (*BursaSigner)(nil)
)
