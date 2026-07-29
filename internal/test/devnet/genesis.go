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

package devnet

import (
	"crypto/ed25519"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"text/template"
	"time"

	"golang.org/x/crypto/blake2b"
)

// Templates were captured from Dingo's own devnet configurator (the
// cardano-foundation testnet-generation-tool) and then parameterised, so the
// cost models, Byron genesis and protocol parameters are known-good rather
// than hand-written. Only the fields listed in Params, plus the pool and
// genesis-UTxO identities, are substituted; everything else is verbatim.
//
//go:embed templates/*.json.tmpl
var templateFS embed.FS

const (
	templateDir = "templates"
	templateExt = ".tmpl"

	// systemStartLayout is the ISO8601 form Shelley genesis requires.
	systemStartLayout = "2006-01-02T15:04:05Z"

	// addrTypeBaseTestnet is the Shelley address header byte for a base
	// address (payment key hash + stake key hash) on a testnet: high nibble
	// 0 selects the type, low nibble 0 the network.
	addrTypeBaseTestnet = 0x00
)

// Domain separators for deriving this devnet's genesis-UTxO keys. Deriving
// them from the pool's cold verification key keeps RenderGenesis a pure
// function of its arguments, which makes a rendered bundle reproducible from
// the key set alone.
const (
	genesisPaymentKeyInfo = "dingo-operator/devnet/genesis-utxo-payment"
	genesisStakeKeyInfo   = "dingo-operator/devnet/genesis-utxo-stake"
)

// Params are the devnet consensus parameters that vary between runs or that
// the e2e suite needs to agree with the operator's DingoNode spec.
type Params struct {
	NetworkMagic      uint32
	SlotLength        float64
	EpochLength       int
	ActiveSlotsCoeff  float64
	SecurityParam     int
	SlotsPerKESPeriod int64
	MaxKESEvolutions  int64
	SystemStart       time.Time
}

// DefaultParams returns the single-pool devnet parameters: one-second slots,
// short epochs and a short KES period so the e2e suite can observe forging
// and rotation within a test's lifetime. SystemStart is left zero for the
// caller to set.
func DefaultParams() Params {
	return Params{
		NetworkMagic:      42,
		SlotLength:        1,
		EpochLength:       500,
		ActiveSlotsCoeff:  0.4,
		SecurityParam:     40,
		SlotsPerKESPeriod: 120,
		MaxKESEvolutions:  62,
	}
}

// templateData is what the embedded templates render against. Params is
// embedded so its fields are addressable directly as {{ .EpochLength }} and
// friends.
type templateData struct {
	Params

	// PoolIDHex is the pool's blake2b-224 cold-key hash.
	PoolIDHex string
	// VRFVKeyHashHex is the blake2b-256 hash of the VRF verification key,
	// which is what genesis staking records — not the key itself.
	VRFVKeyHashHex string
	// StakeKeyHashHex is the sole stake credential in genesis. It owns all
	// initial funds and delegates them to the one pool, making that pool a
	// leader candidate from the first slot.
	StakeKeyHashHex string
	// InitialFundsAddrHex is the base address holding every genesis UTxO.
	InitialFundsAddrHex string

	SystemStartISO  string
	SystemStartUnix int64
}

// RenderGenesis renders the devnet configuration bundle as a map of filename
// to file contents. It is deterministic: the same params and key set always
// produce the same bytes.
func RenderGenesis(p Params, keys *PoolKeys) (map[string]string, error) {
	data, err := newTemplateData(p, keys)
	if err != nil {
		return nil, err
	}
	// No missingkey option: it only governs map-keyed data, and templateData is
	// a struct — a typo'd field there is a parse-time error already.
	tmpl, err := template.New(templateDir).
		ParseFS(templateFS, templateDir+"/*"+templateExt)
	if err != nil {
		return nil, fmt.Errorf("parse genesis templates: %w", err)
	}
	entries, err := fs.ReadDir(templateFS, templateDir)
	if err != nil {
		return nil, fmt.Errorf("list genesis templates: %w", err)
	}
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		var buf strings.Builder
		if err := tmpl.ExecuteTemplate(&buf, entry.Name(), data); err != nil {
			return nil, fmt.Errorf("render %s: %w", entry.Name(), err)
		}
		out[strings.TrimSuffix(entry.Name(), templateExt)] = buf.String()
	}
	return out, nil
}

func newTemplateData(p Params, keys *PoolKeys) (templateData, error) {
	if keys == nil {
		return templateData{}, errors.New("nil pool keys")
	}
	if p.SystemStart.IsZero() {
		return templateData{}, errors.New("params: SystemStart is unset")
	}
	// Genesis records whole seconds, so silently dropping a sub-second
	// component here would leave the caller's Params disagreeing with the
	// bytes we render — and any slot arithmetic done against that struct
	// skewed. Make the caller truncate instead.
	if p.SystemStart.Nanosecond() != 0 {
		return templateData{}, fmt.Errorf(
			"params: SystemStart %s has sub-second precision; genesis records"+
				" whole seconds, so truncate it (Truncate(time.Second)) before"+
				" rendering",
			p.SystemStart.Format(time.RFC3339Nano),
		)
	}
	vrfHash, err := keys.VRFVKeyHash()
	if err != nil {
		return templateData{}, err
	}
	paymentHash, err := deriveGenesisKeyHash(
		keys.ColdVKey, genesisPaymentKeyInfo,
	)
	if err != nil {
		return templateData{}, fmt.Errorf("genesis payment key: %w", err)
	}
	stakeHash, err := deriveGenesisKeyHash(keys.ColdVKey, genesisStakeKeyInfo)
	if err != nil {
		return templateData{}, fmt.Errorf("genesis stake key: %w", err)
	}
	// Byron's startTime and Shelley's systemStart must resolve to the same
	// instant or Dingo refuses the bundle, so derive both from one value.
	start := p.SystemStart.UTC()
	p.SystemStart = start
	return templateData{
		Params:          p,
		PoolIDHex:       hex.EncodeToString(keys.PoolID),
		VRFVKeyHashHex:  hex.EncodeToString(vrfHash),
		StakeKeyHashHex: hex.EncodeToString(stakeHash),
		InitialFundsAddrHex: hex.EncodeToString(
			baseAddress(paymentHash, stakeHash),
		),
		SystemStartISO:  start.Format(systemStartLayout),
		SystemStartUnix: start.Unix(),
	}, nil
}

// deriveGenesisKeyHash derives a throwaway ed25519 key from the pool's cold
// verification key and a domain separator, and returns its key hash. The
// signing key is never needed: genesis records credentials, and nothing in
// the e2e suite spends the genesis UTxO.
func deriveGenesisKeyHash(
	coldVKey ed25519.PublicKey, info string,
) ([]byte, error) {
	seed := blake2b.Sum256(append([]byte(info), coldVKey...))
	skey := ed25519.NewKeyFromSeed(seed[:])
	// An ed25519 private key is seed||public, so the trailing bytes are the
	// verification key.
	return keyHash28(ed25519.PublicKey(skey[ed25519.SeedSize:]))
}

// baseAddress assembles a Shelley testnet base address from a payment key
// hash and a stake key hash.
func baseAddress(paymentHash, stakeHash []byte) []byte {
	addr := make([]byte, 0, 1+len(paymentHash)+len(stakeHash))
	addr = append(addr, addrTypeBaseTestnet)
	addr = append(addr, paymentHash...)
	addr = append(addr, stakeHash...)
	return addr
}
