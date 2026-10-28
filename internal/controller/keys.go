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
	"math/big"
	"slices"
	"strings"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	corev1 "k8s.io/api/core/v1"
)

// File names the operator mounts into the node from the keys Secret. They match
// the paths passed to Dingo in internal/resources (CARDANO_SHELLEY_*_KEY).
const (
	keyFileKES    = "kes.skey"
	keyFileVRF    = "vrf.skey"
	keyFileOpCert = "opcert.cert"
	// keyFileVote is optional: the Leios BLS vote signing key. A Secret
	// without it stays valid.
	keyFileVote = resources.VoteKeyFile
	// keyFileBLS is the name the dingo Helm chart gives the vote signing key.
	// It is accepted in place of keyFileVote.
	keyFileBLS = "bls.skey"
	// keyFileNodeCert is the name the dingo Helm chart gives the operational
	// certificate. It is accepted in place of keyFileOpCert so one Secret can
	// serve both.
	keyFileNodeCert = "node.cert"
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

// onChainCounterMaxAge bounds how old an observation of the on-chain opcert
// counter may be and still gate a rotation.
//
// The bound is not needed for monotonicity — the on-chain counter only ever
// increases, so an old reading remains a valid lower bound. It exists because
// the *pool* the reading describes may no longer be the pool being validated: a
// spec.blockProducer.poolId edit, a repoint at another network, or a chain
// rollback all break the link between the stored number and the current node,
// and none of them updates the stored number. A window a few multiples of
// requeueInterval wide means a healthy operator refreshes several times over
// before the value ever ages out, while an operator that has not been able to
// talk to the node for a quarter of an hour stops enforcing a number it can no
// longer vouch for.
const onChainCounterMaxAge = 15 * time.Minute

// onChainCounterRefreshInterval is how often the operator re-reads the on-chain
// counter. It is a third of onChainCounterMaxAge so two consecutive refreshes
// can fail without the floor ever going stale, and it caps the aggregate cost
// of the node-to-client dial across every managed block producer, which
// per-call timeouts do not (see refreshOnChainCounter).
const onChainCounterRefreshInterval = onChainCounterMaxAge / 3

// validateKeysSecret checks an externally-delivered block-producer key bundle
// before the operator rolls the pod onto it. Without this, any opcert a user
// drops into the Secret reaches the node and is caught only by Dingo's startup
// validation — as a CrashLoop of the one pod that forges.
//
// Counter checks are two-tiered. The authoritative floor is the on-chain
// counter read from the node (status.opcert.onChainCounter): the chain rejects
// any block whose opcert issue number is below what it has already accepted, so
// a bundle under that floor cannot forge whatever the operator does. That floor
// exists only when the node exposes node-to-client and the pool has minted, so
// the operator additionally — and always — guards against regression below the
// last counter it accepted itself. Over-incrementing *past* the chain is still
// not caught here; it remains Dingo's startup check.
func validateKeysSecret(
	secret *corev1.Secret,
	dn *dingov1alpha1.DingoNode,
) (*keyState, error) {
	bp := dn.Spec.BlockProducer
	if bp == nil {
		return nil, errors.New("block producer spec is nil")
	}

	var missing []string
	for _, k := range []string{keyFileKES, keyFileVRF} {
		if len(secret.Data[k]) == 0 {
			missing = append(missing, k)
		}
	}
	certData, certFile, err := opCertFile(secret.Data)
	if err != nil {
		return nil, err
	}
	if certData == nil {
		missing = append(missing, keyFileOpCert+" or "+keyFileNodeCert)
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

	voteData, voteFile, err := voteKeyFile(secret.Data)
	if err != nil {
		return nil, err
	}
	if voteData != nil {
		if err := checkVoteKey(voteData, voteFile); err != nil {
			return nil, err
		}
	}

	oc, coldVkey, err := opcert.Parse(certData)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", certFile, err)
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

	// The chain's own floor first: it is authoritative, and its message is the
	// more useful one when both bounds are violated.
	if floor, ok := onChainFloor(dn, time.Now()); ok && counter < floor {
		return nil, fmt.Errorf(
			"opcert counter %d is below the on-chain counter %d observed from "+
				"the node; the chain has already accepted a higher "+
				"certificate for this pool and would reject blocks forged "+
				"with this one; re-issue the certificate with a counter of at "+
				"least %d",
			counter, floor, floor,
		)
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

// opCertFile returns the operational certificate from a keys Secret and the
// file it came from. The certificate is opcert.cert or the dingo chart's
// node.cert; an empty file counts as absent. Both may be present only when they
// hold the same bytes, since the operator cannot tell which one the owner
// meant. It returns nil data when neither is present.
func opCertFile(data map[string][]byte) ([]byte, string, error) {
	opcertData, nodeData := data[keyFileOpCert], data[keyFileNodeCert]
	switch {
	case len(opcertData) > 0 && len(nodeData) > 0:
		if !bytes.Equal(opcertData, nodeData) {
			return nil, "", fmt.Errorf(
				"keys secret holds both %s and %s with different contents; "+
					"keep only one",
				keyFileOpCert, keyFileNodeCert,
			)
		}
		return opcertData, keyFileOpCert, nil
	case len(opcertData) > 0:
		return opcertData, keyFileOpCert, nil
	case len(nodeData) > 0:
		return nodeData, keyFileNodeCert, nil
	}
	return nil, "", nil
}

// voteKeyFile returns the optional Leios vote signing key from a keys Secret
// and the file it came from. The key is vote.skey or the dingo chart's
// bls.skey; an empty file counts as absent. Both may be present only when they
// hold the same bytes. It returns nil data when neither is present.
func voteKeyFile(data map[string][]byte) ([]byte, string, error) {
	voteData, blsData := data[keyFileVote], data[keyFileBLS]
	switch {
	case len(voteData) > 0 && len(blsData) > 0:
		if !bytes.Equal(voteData, blsData) {
			return nil, "", fmt.Errorf(
				"keys secret holds both %s and %s with different contents; "+
					"keep only one",
				keyFileVote, keyFileBLS,
			)
		}
		return voteData, keyFileVote, nil
	case len(voteData) > 0:
		return voteData, keyFileVote, nil
	case len(blsData) > 0:
		return blsData, keyFileBLS, nil
	}
	return nil, "", nil
}

// onChainFloor returns the on-chain opcert counter to enforce against, and
// whether there is one to enforce at all.
//
// It fails open — no floor rather than a rejection — whenever the counter is
// unknown or stale. Every reason the counter can be missing is a normal state
// of the system, not a fault of the bundle being validated: the node may be
// absent, starting, syncing, unreachable, not exposing node-to-client, or
// the pool may simply not have minted yet. Rejecting rotations in those states
// would strand an operator precisely when rotation matters most — mid-incident,
// with the node down and KES marching toward expiry — and it would do so to
// enforce a bound the node itself re-checks at startup anyway. The same
// reasoning already narrowed the KES lower-bound check below; see the
// rotation-mode discussion in CLAUDE.md.
//
// A zero counter is treated as absent: the field is omitempty and a pool that
// has never minted has no counter, so "0" is not distinguishable from unset —
// and a floor of 0 rejects nothing regardless.
//
// The observation must also name the pool being validated. Without that, a
// spec.blockProducer.poolId edit would leave the previous pool's counter
// enforced until it aged out — precisely the provenance failure the age bound
// exists to limit, and one the refresh interval would otherwise widen.
func onChainFloor(
	dn *dingov1alpha1.DingoNode,
	now time.Time,
) (int64, bool) {
	oc := dn.Status.OpCert
	if oc.OnChainCounter <= 0 || oc.OnChainCounterAt == nil {
		return 0, false
	}
	if now.Sub(oc.OnChainCounterAt.Time) > onChainCounterMaxAge {
		return 0, false
	}
	bp := dn.Spec.BlockProducer
	if oc.OnChainCounterPoolID == "" || bp == nil || bp.PoolID == "" {
		return 0, false
	}
	want, err := parsePoolID(bp.PoolID)
	if err != nil || want.String() != oc.OnChainCounterPoolID {
		return 0, false
	}
	return oc.OnChainCounter, true
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

// voteKeyMaxSize and voteKeySize mirror Dingo's loader
// (dingo/ledger/leios/keys.go): the file is read up to 1 KiB and holds a
// 32-byte big-endian BLS12-381 scalar, either in a text envelope or as legacy
// raw hex. Dingo's ParseVoteSigningKey also requires the scalar to lie in
// [1, r), r being the scalar field modulus; checkVoteKey enforces the same
// range so a key Dingo would refuse at startup never rolls the pod.
const (
	voteKeyMaxSize = 1024
	voteKeySize    = 32
)

// checkVoteKey validates the optional Leios vote signing key so a truncated or
// misfiled key is a refused rotation rather than a node that starts with
// voting disabled or fails at startup.
func checkVoteKey(data []byte, name string) error {
	if len(data) > voteKeyMaxSize {
		return fmt.Errorf(
			"%s exceeds %d bytes", name, voteKeyMaxSize,
		)
	}
	var scalar []byte
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(trimmed, []byte("{")) {
		var env keyEnvelope
		if err := json.Unmarshal(trimmed, &env); err != nil {
			return fmt.Errorf("parse %s envelope: %w", name, err)
		}
		// Dingo accepts any envelope whose type names a BLS or Leios key.
		lower := strings.ToLower(env.Type)
		if !strings.Contains(lower, "bls") &&
			!strings.Contains(lower, "leios") {
			return fmt.Errorf(
				"%s has envelope type %q, expected a BLS vote signing key",
				name, env.Type,
			)
		}
		payload, err := hex.DecodeString(env.CborHex)
		if err != nil {
			return fmt.Errorf("decode %s cborHex: %w", name, err)
		}
		n, err := cbor.Decode(payload, &scalar)
		if err != nil {
			return fmt.Errorf("decode %s cbor: %w", name, err)
		}
		if n != len(payload) {
			return fmt.Errorf(
				"%s has trailing bytes after the key", name,
			)
		}
	} else {
		var err error
		scalar, err = hex.DecodeString(string(trimmed))
		if err != nil {
			return fmt.Errorf(
				"%s is neither a text envelope nor a hex scalar: %w",
				name, err,
			)
		}
	}
	if len(scalar) != voteKeySize {
		return fmt.Errorf(
			"%s holds %d bytes, expected %d",
			name, len(scalar), voteKeySize,
		)
	}
	if !slices.ContainsFunc(scalar, func(b byte) bool { return b != 0 }) {
		return fmt.Errorf("%s is the zero scalar", name)
	}
	if new(big.Int).SetBytes(scalar).Cmp(fr.Modulus()) >= 0 {
		return fmt.Errorf(
			"%s scalar is not below the BLS12-381 field modulus",
			name,
		)
	}
	return nil
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
