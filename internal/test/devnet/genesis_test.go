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
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderGenesisProducesAllFiles(t *testing.T) {
	dn, err := devnet.Generate(rand.Reader, time.Now().Add(45*time.Second))
	require.NoError(t, err)

	for _, name := range []string{
		"config.json",
		"byron-genesis.json",
		"shelley-genesis.json",
		"alonzo-genesis.json",
		"conway-genesis.json",
	} {
		assert.Contains(t, dn.ConfigFiles, name)
	}
}

func TestRenderedFilesAreValidJSON(t *testing.T) {
	dn, err := devnet.Generate(rand.Reader, time.Now().Add(45*time.Second))
	require.NoError(t, err)

	for name, content := range dn.ConfigFiles {
		t.Run(name, func(t *testing.T) {
			var v any
			assert.NoError(t, json.Unmarshal([]byte(content), &v),
				"template produced invalid JSON")
		})
	}
}

func TestShelleyGenesisCarriesParamsAndPool(t *testing.T) {
	start := time.Now().Add(45 * time.Second).UTC().Truncate(time.Second)
	dn, err := devnet.Generate(rand.Reader, start)
	require.NoError(t, err)

	var shelley struct {
		SystemStart       string `json:"systemStart"`
		NetworkMagic      int    `json:"networkMagic"`
		EpochLength       int    `json:"epochLength"`
		SlotsPerKESPeriod int64  `json:"slotsPerKESPeriod"`
		MaxKESEvolutions  int64  `json:"maxKESEvolutions"`
		SecurityParam     int    `json:"securityParam"`
		Staking           struct {
			Pools map[string]struct {
				VRF string `json:"vrf"`
			} `json:"pools"`
		} `json:"staking"`
	}
	require.NoError(t, json.Unmarshal(
		[]byte(dn.ConfigFiles["shelley-genesis.json"]), &shelley,
	))

	assert.Equal(t, 42, shelley.NetworkMagic)
	assert.Equal(t, 500, shelley.EpochLength)
	assert.Equal(t, int64(120), shelley.SlotsPerKESPeriod)
	assert.Equal(t, int64(62), shelley.MaxKESEvolutions)
	assert.Equal(t, 40, shelley.SecurityParam)
	assert.Equal(t,
		start.Format("2006-01-02T15:04:05Z"), shelley.SystemStart)

	require.Len(t, shelley.Staking.Pools, 1, "exactly one pool")
	for id, pool := range shelley.Staking.Pools {
		assert.Len(t, id, 56, "pool ID is 28 bytes hex-encoded")
		assert.Len(t, pool.VRF, 64,
			"vrf must be the 32-byte blake2b-256 hash, hex-encoded")
	}
}

// TestShelleyGenesisPoolMatchesKeys pins the template wiring: the staking
// record must name the generated pool and the hash of its VRF verification
// key, and every stake reference must be the one credential that holds all
// initial funds — otherwise the pool does not control 100% of stake and is
// not a leader from the first slot.
func TestShelleyGenesisPoolMatchesKeys(t *testing.T) {
	dn, err := devnet.Generate(rand.Reader, time.Now().Add(45*time.Second))
	require.NoError(t, err)

	vrfHash, err := dn.Keys.VRFVKeyHash()
	require.NoError(t, err)

	var shelley struct {
		InitialFunds map[string]int64 `json:"initialFunds"`
		Staking      struct {
			Pools map[string]struct {
				VRF           string   `json:"vrf"`
				PublicKey     string   `json:"publicKey"`
				Owners        []string `json:"owners"`
				RewardAccount struct {
					Credential struct {
						KeyHash string `json:"keyHash"`
					} `json:"credential"`
				} `json:"rewardAccount"`
			} `json:"pools"`
			Stake map[string]string `json:"stake"`
		} `json:"staking"`
	}
	require.NoError(t, json.Unmarshal(
		[]byte(dn.ConfigFiles["shelley-genesis.json"]), &shelley,
	))

	poolIDHex := hex.EncodeToString(dn.Keys.PoolID)
	require.Contains(t, shelley.Staking.Pools, poolIDHex)
	pool := shelley.Staking.Pools[poolIDHex]
	assert.Equal(t, hex.EncodeToString(vrfHash), pool.VRF,
		"genesis records the VRF key hash, not the key")
	assert.Equal(t, poolIDHex, pool.PublicKey)

	require.Len(t, shelley.Staking.Stake, 1, "one stake credential")
	var stakeKeyHash string
	for cred, poolID := range shelley.Staking.Stake {
		stakeKeyHash = cred
		assert.Equal(t, poolIDHex, poolID, "stake delegates to our pool")
	}
	assert.Equal(t, []string{stakeKeyHash}, pool.Owners)
	assert.Equal(t, stakeKeyHash, pool.RewardAccount.Credential.KeyHash)

	require.Len(t, shelley.InitialFunds, 1, "all funds at one address")
	for addr, amount := range shelley.InitialFunds {
		assert.Len(t, addr, 114, "testnet base address is 57 bytes hex")
		assert.True(t, strings.HasSuffix(addr, stakeKeyHash),
			"initial funds must sit under the delegated stake credential")
		assert.Positive(t, amount)
	}
}

func TestConfigJSONHasNoGenesisHashes(t *testing.T) {
	dn, err := devnet.Generate(rand.Reader, time.Now().Add(45*time.Second))
	require.NoError(t, err)

	var cfg map[string]any
	require.NoError(t, json.Unmarshal(
		[]byte(dn.ConfigFiles["config.json"]), &cfg,
	))
	for _, k := range []string{
		"ByronGenesisHash", "ShelleyGenesisHash",
		"AlonzoGenesisHash", "ConwayGenesisHash",
	} {
		assert.NotContains(t, cfg, k,
			"stale genesis hashes fail startup against regenerated genesis")
	}
}

// TestConfigJSONGenesisFilesAreAllRendered guards the invariant that every
// genesis file config.json names is actually in the bundle. Dingo reads each
// *GenesisFile unconditionally and returns the os.ReadFile error verbatim
// (config/cardano/node.go:275), so a template rename, a dropped file or a new
// *GenesisFile key surfaces as a pod crash-loop with a bare "no such file or
// directory" rather than a test failure. The five-file list in this task's
// brief already missed dijkstra-genesis.json once.
func TestConfigJSONGenesisFilesAreAllRendered(t *testing.T) {
	dn, err := devnet.Generate(rand.Reader, time.Now().Add(45*time.Second))
	require.NoError(t, err)

	var cfg map[string]any
	require.NoError(t, json.Unmarshal(
		[]byte(dn.ConfigFiles["config.json"]), &cfg,
	))

	// Compare against a name list rather than the map so a failure prints six
	// filenames instead of the whole rendered bundle.
	rendered := slices.Sorted(maps.Keys(dn.ConfigFiles))

	referenced := 0
	for key, value := range cfg {
		if !strings.HasSuffix(key, "GenesisFile") {
			continue
		}
		referenced++
		name, ok := value.(string)
		require.True(t, ok, "%s must name a file, got %T", key, value)
		assert.Contains(t, rendered, name,
			"config.json %s references %q, which the bundle does not render;"+
				" Dingo would fail to start", key, name)
	}
	assert.NotZero(t, referenced,
		"config.json names no genesis files, so this test proves nothing")
}

func TestByronStartTimeMatchesSystemStart(t *testing.T) {
	start := time.Now().Add(45 * time.Second).UTC().Truncate(time.Second)
	dn, err := devnet.Generate(rand.Reader, start)
	require.NoError(t, err)

	var byron struct {
		StartTime int64 `json:"startTime"`
	}
	require.NoError(t, json.Unmarshal(
		[]byte(dn.ConfigFiles["byron-genesis.json"]), &byron,
	))
	assert.Equal(t, start.Unix(), byron.StartTime)
}

func TestRenderGenesisRejectsIncompleteInput(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	withStart := devnet.DefaultParams()
	withStart.SystemStart = time.Now().Truncate(time.Second)

	subSecond := devnet.DefaultParams()
	subSecond.SystemStart = time.Now().
		Truncate(time.Second).
		Add(500 * time.Millisecond)

	tests := []struct {
		name   string
		params devnet.Params
		keys   *devnet.PoolKeys
		errMsg string
	}{
		{
			name:   "nil keys",
			params: withStart,
			keys:   nil,
			errMsg: "nil pool keys",
		},
		{
			// DefaultParams deliberately leaves SystemStart zero; rendering
			// it anyway would date the chain to year 1.
			name:   "unset system start",
			params: devnet.DefaultParams(),
			keys:   keys,
			errMsg: "SystemStart is unset",
		},
		{
			// Truncating silently would leave the caller's Params disagreeing
			// with the rendered bytes, skewing any slot math derived from it.
			name:   "sub-second system start",
			params: subSecond,
			keys:   keys,
			errMsg: "sub-second precision",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := devnet.RenderGenesis(tt.params, tt.keys)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}
