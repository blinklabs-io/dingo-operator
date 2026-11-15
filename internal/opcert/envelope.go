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

// Package opcert reads and writes the cardano-cli operational-certificate text
// envelope. gouroboros supplies the opcert crypto but no file codec: its
// ledger.OpCert omits the cold verification key, which is the second element
// of the on-disk CBOR and is required to verify the certificate's signature.
package opcert

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// EnvelopeType is the only "type" value Dingo's keystore accepts for an
// operational certificate.
const EnvelopeType = "NodeOperationalCertificate"

const envelopeDescription = "Operational Certificate"

// envelope is the cardano-cli text-envelope JSON structure.
type envelope struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// Encode renders an operational certificate as a cardano-cli text envelope.
func Encode(oc *ledger.OpCert, coldVkey []byte) ([]byte, error) {
	payload, err := EncodeCBOR(oc, coldVkey)
	if err != nil {
		return nil, err
	}
	return EncodeEnvelope(payload)
}

// EncodeEnvelope wraps the bare CBOR of an operational certificate in a
// cardano-cli text envelope, keeping its bytes as they are. Dingo admits an
// agent only when the agent's opcert is byte-equal to the one in its key file,
// so an opcert the agent serves is written without being re-encoded.
func EncodeEnvelope(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("opcert cbor is empty")
	}
	return json.MarshalIndent(envelope{
		Type:        EnvelopeType,
		Description: envelopeDescription,
		CborHex:     hex.EncodeToString(payload),
	}, "", "    ")
}

// Payload returns the bare CBOR an operational-certificate envelope wraps,
// as Dingo reads it from its key file.
func Payload(data []byte) ([]byte, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse opcert envelope: %w", err)
	}
	if env.Type != EnvelopeType {
		return nil, fmt.Errorf(
			"expected type %q, got %q", EnvelopeType, env.Type,
		)
	}
	payload, err := hex.DecodeString(env.CborHex)
	if err != nil {
		return nil, fmt.Errorf("decode opcert cborHex: %w", err)
	}
	if len(payload) == 0 {
		return nil, errors.New("opcert cborHex is empty")
	}
	return payload, nil
}

// EncodeCBOR renders an operational certificate as the bare CBOR the envelope
// wraps: [[kes_vkey, issue_number, kes_period, cold_signature], cold_vkey].
// It is the form a KES agent's install-key takes.
func EncodeCBOR(oc *ledger.OpCert, coldVkey []byte) ([]byte, error) {
	if oc == nil {
		return nil, errors.New("opcert is nil")
	}
	if len(coldVkey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"cold vkey must be %d bytes, got %d",
			ed25519.PublicKeySize,
			len(coldVkey),
		)
	}
	inner := []any{
		oc.KesVkey,
		oc.IssueNumber,
		oc.KesPeriod,
		oc.ColdSignature,
	}
	payload, err := cbor.Encode([]any{inner, coldVkey})
	if err != nil {
		return nil, fmt.Errorf("encode opcert cbor: %w", err)
	}
	return payload, nil
}

// Parse decodes a cardano-cli operational-certificate envelope into an OpCert
// and the cold verification key that signed it. It does not verify the
// signature; callers pass both results to ledger.VerifyOpCertSignature.
func Parse(data []byte) (*ledger.OpCert, []byte, error) {
	payload, err := Payload(data)
	if err != nil {
		return nil, nil, err
	}
	return ParseCBOR(payload)
}

// ParseCBOR decodes the bare CBOR of an operational certificate, as EncodeCBOR
// writes it. Like Parse it does not verify the signature.
func ParseCBOR(payload []byte) (*ledger.OpCert, []byte, error) {
	var outer []any
	if _, err := cbor.Decode(payload, &outer); err != nil {
		return nil, nil, fmt.Errorf("decode opcert cbor: %w", err)
	}
	if len(outer) != 2 {
		return nil, nil, fmt.Errorf(
			"invalid opcert: expected 2-element outer array, got %d",
			len(outer),
		)
	}

	inner, ok := outer[0].([]any)
	if !ok {
		return nil, nil, errors.New(
			"invalid opcert: first element is not an array",
		)
	}
	if len(inner) != 4 {
		return nil, nil, fmt.Errorf(
			"invalid opcert: expected 4-element cert array, got %d",
			len(inner),
		)
	}

	coldVkey, ok := outer[1].([]byte)
	if !ok {
		return nil, nil, errors.New("invalid opcert: cold vkey is not bytes")
	}

	kesVkey, ok := inner[0].([]byte)
	if !ok {
		return nil, nil, errors.New("invalid opcert: kes vkey is not bytes")
	}

	issueNumber, err := toUint64(inner[1], "issue number")
	if err != nil {
		return nil, nil, err
	}

	kesPeriod, err := toUint64(inner[2], "kes period")
	if err != nil {
		return nil, nil, err
	}

	signature, ok := inner[3].([]byte)
	if !ok {
		return nil, nil, errors.New(
			"invalid opcert: cold signature is not bytes",
		)
	}

	return &ledger.OpCert{
		KesVkey:       kesVkey,
		IssueNumber:   issueNumber,
		KesPeriod:     kesPeriod,
		ColdSignature: signature,
	}, coldVkey, nil
}

// toUint64 extracts a non-negative integer field from a generically decoded
// CBOR value. CBOR integers decode as uint64 or int64 depending on sign and
// magnitude.
func toUint64(v any, field string) (uint64, error) {
	switch n := v.(type) {
	case uint64:
		return n, nil
	case int64:
		if n < 0 {
			return 0, fmt.Errorf("invalid opcert: %s cannot be negative", field)
		}
		return uint64(n), nil
	default:
		return 0, fmt.Errorf(
			"invalid opcert: %s has unexpected type %T", field, v,
		)
	}
}
