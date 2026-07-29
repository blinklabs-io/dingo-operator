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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	corev1 "k8s.io/api/core/v1"
)

// File names the operator mounts into the node from the keys Secret. They match
// the paths passed to Dingo in internal/resources (CARDANO_SHELLEY_*_KEY).
const (
	keyFileKES    = "kes.skey"
	keyFileVRF    = "vrf.skey"
	keyFileOpCert = "opcert.cert"
)

// Envelope "type" prefixes cardano-cli writes for the two signing keys. Only
// the prefix is checked: the KES type string carries the tree depth
// (KesSigningKey_ed25519_kes_2^6), which is a property of the network's
// parameters rather than something the operator should pin.
const (
	kesEnvelopeTypePrefix = "KesSigningKey"
	vrfEnvelopeTypePrefix = "VrfSigningKey"
)

// poolIDSize is the width of a pool ID: blake2b-224 of the cold verification
// key.
const poolIDSize = 28

// keyState is what the reconciler learns from a validated key bundle and
// records in status. It exists so the reconciler does not have to re-parse the
// certificate to publish status.opcert.onDiskCounter.
type keyState struct {
	// Counter is the certificate's issue number (the opcert counter).
	Counter int64
	// KESPeriod is the certificate's start KES period.
	KESPeriod int64
}

// validateKeysSecret checks an externally-delivered block-producer key bundle
// before the operator rolls the pod onto it. Without this, any opcert a user
// drops into the Secret reaches the node and is caught only by Dingo's startup
// validation — as a CrashLoop of the one pod that forges.
//
// The authoritative on-chain counter check is not possible yet: it needs the
// node's LSQ opcert counter (gouroboros GetOpCertCounters, P2). Until then the
// operator only guards against regression below the last counter it observed on
// disk, which still catches the common "re-delivered an older bundle" mistake
// but cannot see a counter that over-increments past the chain.
func validateKeysSecret(
	secret *corev1.Secret,
	dn *dingov1alpha1.DingoNode,
) (*keyState, error) {
	bp := dn.Spec.BlockProducer
	if bp == nil {
		return nil, errors.New("block producer spec is nil")
	}

	var missing []string
	for _, k := range []string{keyFileKES, keyFileVRF, keyFileOpCert} {
		if len(secret.Data[k]) == 0 {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"keys secret is missing %s", strings.Join(missing, ", "),
		)
	}

	// A truncated or wrong-kind signing key is worth catching here rather than
	// at node startup.
	kesRaw, err := checkKeyEnvelope(
		secret.Data[keyFileKES], keyFileKES, kesEnvelopeTypePrefix,
	)
	if err != nil {
		return nil, err
	}
	vrfRaw, err := checkKeyEnvelope(
		secret.Data[keyFileVRF], keyFileVRF, vrfEnvelopeTypePrefix,
	)
	if err != nil {
		return nil, err
	}
	if err := checkVRFKeyLength(vrfRaw); err != nil {
		return nil, err
	}

	oc, coldVkey, err := opcert.Parse(secret.Data[keyFileOpCert])
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", keyFileOpCert, err)
	}
	if err := ledger.VerifyOpCertSignature(oc, coldVkey); err != nil {
		return nil, fmt.Errorf("verify opcert signature: %w", err)
	}

	// Bind the certificate to this pool: a self-consistent opcert signed by
	// some other cold key would otherwise pass every check above.
	if bp.PoolID != "" {
		if err := checkPoolID(bp.PoolID, coldVkey); err != nil {
			return nil, err
		}
	}

	// Bind the certificate to the delivered KES key. Everything above passes
	// when an operator ships a new opcert but forgets to swap kes.skey, which
	// is the single most likely assisted-rotation mistake; the keys checksum
	// still changes, so the operator would roll the one forging pod onto a
	// bundle Dingo refuses at startup (ledger/forging/keys.go: "KES
	// verification key mismatch").
	if err := checkKESBinding(kesRaw, oc.KesVkey); err != nil {
		return nil, err
	}

	counter, err := certField(oc.IssueNumber, "counter")
	if err != nil {
		return nil, err
	}
	startPeriod, err := certField(oc.KesPeriod, "kes start period")
	if err != nil {
		return nil, err
	}

	if last := dn.Status.OpCert.OnDiskCounter; last > 0 && counter < last {
		// status.opcert.onDiskCounter is published on acceptance, before the
		// pod has come up on the new bundle, so an admin backing out a roll
		// that never completed lands here. Say how to get out: the floor is
		// the operator's own record, and a re-issued certificate at or above
		// it is accepted.
		return nil, fmt.Errorf(
			"opcert counter %d regresses below the last observed counter %d; "+
				"to restore an earlier bundle, re-issue its certificate with "+
				"a counter of at least %d",
			counter, last, last,
		)
	}

	// Skip the period checks until the node has reported a KES period at least
	// once: a zero value means "not yet scraped", not "period 0", and failing
	// on it would reject a healthy node's own valid keys on the first reconcile.
	if cur := dn.Status.KES.CurrentPeriod; cur > 0 {
		end := startPeriod + bp.MaxKESEvolutions
		// Expiry is unconditional. An opcert whose KES window has already
		// closed cannot be made to work by rolling the pod onto it; the node
		// would refuse to start and the one forging pod would CrashLoop.
		if cur > end {
			return nil, fmt.Errorf(
				"opcert expired: kes period range [%d,%d] ends before the "+
					"current period %d",
				startPeriod, end, cur,
			)
		}
		// The lower bound is defence in depth rather than the last line: Dingo
		// validates at startup that the opcert is not future-dated. And
		// status.kes.currentPeriod is only as fresh as the last successful
		// metrics scrape — refreshForgeStatus returns early on a fetch error or
		// missing KES data without clearing the old value, so a healthy forging
		// node whose metrics endpoint became unreachable freezes this field
		// indefinitely. Enforcing the bound against a stale period would then
		// refuse a *correct* replacement bundle and leave the node forging on
		// keys it can no longer renew, which is worse than what the bound
		// protects against. A counter above the last accepted one is
		// unambiguous evidence of a deliberate forward rotation, so trust it.
		if counter <= dn.Status.OpCert.OnDiskCounter && cur < startPeriod {
			return nil, fmt.Errorf(
				"opcert kes start period %d is in the future (current period "+
					"%d) and its counter %d does not advance past the last "+
					"accepted counter %d",
				startPeriod, cur, counter, dn.Status.OpCert.OnDiskCounter,
			)
		}
	}

	return &keyState{Counter: counter, KESPeriod: startPeriod}, nil
}

// keyEnvelope is the subset of the cardano-cli text envelope needed to tell a
// well-formed signing key from a truncated or misfiled one.
type keyEnvelope struct {
	Type    string `json:"type"`
	CborHex string `json:"cborHex"`
}

// checkKeyEnvelope verifies that file is a cardano-cli text envelope of the
// expected kind holding a non-empty CBOR byte string, and returns the decoded
// key material for the per-kind checks below.
func checkKeyEnvelope(
	data []byte,
	file, wantTypePrefix string,
) ([]byte, error) {
	var env keyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse %s envelope: %w", file, err)
	}
	if !strings.HasPrefix(env.Type, wantTypePrefix) {
		return nil, fmt.Errorf(
			"%s has envelope type %q, expected a %s key",
			file, env.Type, wantTypePrefix,
		)
	}
	payload, err := hex.DecodeString(env.CborHex)
	if err != nil {
		return nil, fmt.Errorf("decode %s cborHex: %w", file, err)
	}
	var raw []byte
	if _, err := cbor.Decode(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode %s cbor: %w", file, err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("%s holds no key material", file)
	}
	return raw, nil
}

// checkVRFKeyLength mirrors the bound Dingo's keystore enforces
// (dingo/keystore/keyfile.go decodeVRFSKey): either a bare 32-byte seed or the
// cardano-cli seed||pubkey form. Catching it here turns a truncated VRF key
// into a refused rotation instead of a CrashLoop after the roll.
func checkVRFKeyLength(raw []byte) error {
	switch len(raw) {
	case vrf.SeedSize, vrf.SeedSize + vrf.PublicKeySize:
		return nil
	default:
		return fmt.Errorf(
			"%s holds %d bytes, expected %d or %d",
			keyFileVRF, len(raw), vrf.SeedSize,
			vrf.SeedSize+vrf.PublicKeySize,
		)
	}
}

// checkKESBinding verifies that the delivered KES signing key is the one the
// certificate was issued for, deriving the public key the same way Dingo's
// keystore does (dingo/keystore/keyfile.go decodeKESSKey).
//
// The length check must come first and must be exact: kes.PublicKey slices the
// key data at fixed depth-6 offsets with no bounds checking, so a short key
// panics rather than erroring (verified against gouroboros kes/sign.go
// publicKeyInternal, which reads data[544:608] for depth 6).
func checkKESBinding(raw, certKESVkey []byte) error {
	if len(raw) != kes.CardanoKesSecretKeySize {
		return fmt.Errorf(
			"%s holds %d bytes, expected %d",
			keyFileKES, len(raw), kes.CardanoKesSecretKeySize,
		)
	}
	sk := &kes.SecretKey{Depth: kes.CardanoKesDepth, Data: raw}
	if !bytes.Equal(kes.PublicKey(sk), certKESVkey) {
		return fmt.Errorf(
			"%s does not match the opcert's KES verification key: the "+
				"certificate was issued for a different KES key",
			keyFileKES,
		)
	}
	return nil
}

// checkPoolID compares the configured pool ID against blake2b-224 of the cold
// verification key that signed the certificate. spec.blockProducer.poolId
// accepts either bech32 ("pool1...") or hex.
func checkPoolID(poolID string, coldVkey []byte) error {
	want, err := parsePoolID(poolID)
	if err != nil {
		return err
	}
	got := common.Blake2b224Hash(coldVkey)
	if got != want {
		return fmt.Errorf(
			"opcert cold key hashes to pool id %s, but spec.blockProducer."+
				"poolId is %s",
			got.String(), want.String(),
		)
	}
	return nil
}

// parsePoolID decodes a pool ID in either hex or bech32 form.
func parsePoolID(poolID string) (common.Blake2b224, error) {
	var zero common.Blake2b224
	if raw, err := hex.DecodeString(poolID); err == nil {
		if len(raw) != poolIDSize {
			return zero, fmt.Errorf(
				"spec.blockProducer.poolId must be %d bytes, got %d",
				poolIDSize, len(raw),
			)
		}
		return common.NewBlake2b224(raw), nil
	}
	id, err := ledger.NewPoolIdFromBech32(poolID)
	if err != nil {
		return zero, fmt.Errorf(
			"spec.blockProducer.poolId %q is neither hex nor bech32: %w",
			poolID, err,
		)
	}
	return common.NewBlake2b224(id[:]), nil
}

// certField narrows an unsigned certificate field to the int64 the status API
// uses. A value that cannot round-trip is not a real certificate.
func certField(v uint64, field string) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("opcert %s %d is out of range", field, v)
	}
	return int64(v), nil
}
