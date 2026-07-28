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

// Package devnet generates the key material for a self-contained single-pool
// Cardano devnet: a fresh cold/VRF/KES key set that can issue operational
// certificates. It exists for the e2e suite and for controller tests that need
// real key bundles rather than placeholder bytes, but is a normal
// (non-test-tagged) package so tooling can import it.
package devnet

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/vrf"
	"golang.org/x/crypto/blake2b"
)

// kesDepth is the Cardano KES tree depth (Sum6, 2^6 = 64 periods).
const kesDepth = 6

// credentialHashSize is the length of a Cardano key hash credential.
const credentialHashSize = 28

// Envelope type strings accepted by Dingo's keystore
// (dingo/keystore/keyfile.go).
const (
	kesEnvelopeType = "KesSigningKey_ed25519_kes_2^6"
	vrfEnvelopeType = "VrfSigningKey_PraosVRF"
)

// PoolKeys is the full key set for one devnet stake pool. The cold key stays
// in memory here — this is a throwaway devnet, and callers need it to mint
// replacement opcerts.
type PoolKeys struct {
	ColdSKey ed25519.PrivateKey
	ColdVKey ed25519.PublicKey
	// PoolID is blake2b-224 of the cold verification key.
	PoolID []byte

	VRFSeed []byte
	VRFVKey []byte

	KESSeed []byte
	KESSKey *kes.SecretKey
	KESVKey []byte
}

// GeneratePoolKeys mints a fresh cold key, VRF key and KES key.
func GeneratePoolKeys(r io.Reader) (*PoolKeys, error) {
	coldVKey, coldSKey, err := ed25519.GenerateKey(r)
	if err != nil {
		return nil, fmt.Errorf("generate cold key: %w", err)
	}
	poolID, err := poolIDFromColdVKey(coldVKey)
	if err != nil {
		return nil, err
	}
	pk := &PoolKeys{
		ColdSKey: coldSKey,
		ColdVKey: coldVKey,
		PoolID:   poolID,
	}
	if err := pk.generateVRF(r); err != nil {
		return nil, err
	}
	if err := pk.RotateKES(r); err != nil {
		return nil, err
	}
	return pk, nil
}

// poolIDFromColdVKey returns blake2b-224 of the cold verification key, which
// is the pool ID used in genesis staking and on chain.
func poolIDFromColdVKey(coldVKey ed25519.PublicKey) ([]byte, error) {
	return keyHash28(coldVKey)
}

// keyHash28 returns blake2b-224 of a verification key, the hash width Cardano
// uses for pool IDs and key-hash credentials.
func keyHash28(vkey []byte) ([]byte, error) {
	h, err := blake2b.New(credentialHashSize, nil)
	if err != nil {
		return nil, fmt.Errorf("blake2b-224: %w", err)
	}
	if _, err := h.Write(vkey); err != nil {
		return nil, fmt.Errorf("hash vkey: %w", err)
	}
	return h.Sum(nil), nil
}

// VRFVKeyHash returns blake2b-256 of the VRF verification key. Genesis staking
// and the on-chain pool registration record the hash, not the key itself.
func (p *PoolKeys) VRFVKeyHash() ([]byte, error) {
	h, err := blake2b.New(32, nil)
	if err != nil {
		return nil, fmt.Errorf("blake2b-256: %w", err)
	}
	if _, err := h.Write(p.VRFVKey); err != nil {
		return nil, fmt.Errorf("hash vrf vkey: %w", err)
	}
	return h.Sum(nil), nil
}

func (p *PoolKeys) generateVRF(r io.Reader) error {
	seed := make([]byte, vrf.SeedSize)
	if _, err := io.ReadFull(r, seed); err != nil {
		return fmt.Errorf("read vrf seed: %w", err)
	}
	// vrf.KeyGen returns (publicKey, secretKey): the secret key is just the
	// seed we passed in, so only the first return is new information. Taking
	// the wrong one here silently puts blake2b-256(seed) in the genesis
	// staking record and the node reports a VRF key hash mismatch.
	pub, _, err := vrf.KeyGen(seed)
	if err != nil {
		return fmt.Errorf("generate vrf key: %w", err)
	}
	p.VRFSeed, p.VRFVKey = seed, pub
	return nil
}

// RotateKES replaces the KES keypair in place, leaving the cold identity
// alone.
func (p *PoolKeys) RotateKES(r io.Reader) error {
	seed := make([]byte, kes.SeedSize)
	if _, err := io.ReadFull(r, seed); err != nil {
		return fmt.Errorf("read kes seed: %w", err)
	}
	sk, pub, err := kes.KeyGen(kesDepth, seed)
	if err != nil {
		return fmt.Errorf("generate kes key: %w", err)
	}
	p.KESSeed, p.KESSKey, p.KESVKey = seed, sk, pub
	return nil
}

// IssueOpCert signs an operational certificate with the cold key and returns
// it as a cardano-cli text envelope. It verifies its own output before
// returning, so a bad certificate can never reach a cluster by accident.
func (p *PoolKeys) IssueOpCert(counter, startKESPeriod uint64) ([]byte, error) {
	oc, err := ledger.CreateOpCert(
		p.KESVKey, counter, startKESPeriod, p.ColdSKey,
	)
	if err != nil {
		return nil, fmt.Errorf("create opcert: %w", err)
	}
	if err := ledger.VerifyOpCertSignature(oc, p.ColdVKey); err != nil {
		return nil, fmt.Errorf("verify freshly issued opcert: %w", err)
	}
	return opcert.Encode(oc, p.ColdVKey)
}

// KESSKeyEnvelope renders the KES signing key as a cardano-cli text
// envelope.
func (p *PoolKeys) KESSKeyEnvelope() ([]byte, error) {
	return encodeKeyEnvelope(
		kesEnvelopeType, "KES Signing Key", p.KESSKey.Data,
	)
}

// VRFSKeyEnvelope renders the VRF signing key as a cardano-cli text
// envelope. Dingo re-derives the public key from the seed, so the seed
// alone is enough.
func (p *PoolKeys) VRFSKeyEnvelope() ([]byte, error) {
	return encodeKeyEnvelope(vrfEnvelopeType, "VRF Signing Key", p.VRFSeed)
}

// SecretData renders the three files the operator mounts for a block
// producer.
func (p *PoolKeys) SecretData(
	counter, startKESPeriod uint64,
) (map[string][]byte, error) {
	kesEnv, err := p.KESSKeyEnvelope()
	if err != nil {
		return nil, err
	}
	vrfEnv, err := p.VRFSKeyEnvelope()
	if err != nil {
		return nil, err
	}
	certEnv, err := p.IssueOpCert(counter, startKESPeriod)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		"kes.skey":    kesEnv,
		"vrf.skey":    vrfEnv,
		"opcert.cert": certEnv,
	}, nil
}

func encodeKeyEnvelope(
	envType, description string, raw []byte,
) ([]byte, error) {
	payload, err := cbor.Encode(raw)
	if err != nil {
		return nil, fmt.Errorf("encode %s cbor: %w", envType, err)
	}
	return marshalEnvelope(envType, description, payload)
}

// keyEnvelope is the cardano-cli text-envelope JSON structure, matching the
// shape internal/opcert emits for operational certificates.
type keyEnvelope struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// marshalEnvelope renders payload as a cardano-cli text envelope. It is kept
// in this package rather than exported from internal/opcert, which owns
// only the opcert envelope shape.
func marshalEnvelope(
	envType, description string,
	payload []byte,
) ([]byte, error) {
	return json.MarshalIndent(keyEnvelope{
		Type:        envType,
		Description: description,
		CborHex:     hex.EncodeToString(payload),
	}, "", "    ")
}
