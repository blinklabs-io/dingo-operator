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

package kesagent

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/test/fakeagent"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func golden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	require.NoError(t, err)
	return b
}

// The frames are copied from bursa's kesagent/wire/testdata/frames at afd19ef.
func TestWireShapesMatchBursaFrames(t *testing.T) {
	t.Parallel()
	nonce := make([]byte, nonceSize)
	for i := range nonce {
		nonce[i] = byte(i + 1)
	}
	req, err := json.Marshal(attestationRequest{
		Type: "attestation_request", Protocol: attestedProtocolID,
		Nonce: nonce, Profile: "pool-a", Role: roleControl,
	})
	require.NoError(t, err)
	assert.JSONEq(
		t,
		string(golden(t, "attestation_request_control")),
		string(req),
	)

	var cert attestationResponse
	require.NoError(t, json.Unmarshal(
		golden(t, "attestation_response_control"), &cert,
	))
	assert.Equal(t, roleControl, cert.Role)
	assert.Equal(t, nonce, cert.Nonce)

	opc := []byte("@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefg")
	// The golden opcert field is these bytes in base64.
	inst, err := json.Marshal(command{Command: cmdInstallKey, OpCert: opc})
	require.NoError(t, err)
	assert.JSONEq(t, string(golden(t, "command_install_key")), string(inst))
	gen, err := json.Marshal(command{Command: cmdGenStagedKey})
	require.NoError(t, err)
	assert.JSONEq(t, string(golden(t, "command_gen_staged_key")), string(gen))

	var vk reply
	require.NoError(t, json.Unmarshal(golden(t, "reply_kes_vkey"), &vk))
	assert.True(t, vk.Ok)
	assert.Len(t, vk.KESVKey, kesVKeySize)
	var bad reply
	require.NoError(t, json.Unmarshal(golden(t, "reply_error"), &bad))
	assert.False(t, bad.Ok)
	assert.Equal(t, "no staged key", bad.Error)
	var info reply
	require.NoError(t, json.Unmarshal(golden(t, "reply_info"), &info))
	require.NotNil(t, info.Info)
	assert.True(t, info.Info.HasActiveKey)
	assert.Equal(t, uint64(3), info.Info.ActiveIssueNumber)
	assert.Len(t, info.Info.ActiveKESVKey, kesVKeySize)
	assert.NotEmpty(t, info.Info.ActiveOpCert)
}

type rig struct {
	agent *fakeagent.Agent
	cfg   Config
	cold  ed25519.PrivateKey
}

func newRig(t *testing.T, mutate func(*fakeagent.Profile)) *rig {
	t.Helper()
	certPEM, keyPEM, spki := fakeagent.ClientCert(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p := fakeagent.Profile{
		Name: "pool-a", ColdVKey: pub,
		ControlSPKI: [][sha256.Size]byte{spki},
		ActiveVKey:  make([]byte, 32), ActiveIssue: 7,
	}
	if mutate != nil {
		mutate(&p)
	}
	a := fakeagent.New(t, p)
	return &rig{agent: a, cold: priv, cfg: Config{
		Address: a.Addr, ServerSPKISHA256: a.ServerSPKI,
		ClientCert: certPEM, ClientKey: keyPEM, Profile: "pool-a",
	}}
}

func (r *rig) opcert(t *testing.T, vkey []byte, issue uint64) []byte {
	t.Helper()
	oc := &ledger.OpCert{KesVkey: vkey, IssueNumber: issue, KesPeriod: 100}
	signable := make([]byte, 0, 48)
	signable = append(signable, vkey...)
	signable = append(signable,
		0, 0, 0, 0, 0, 0, 0, byte(issue), 0, 0, 0, 0, 0, 0, 0, 100)
	oc.ColdSignature = ed25519.Sign(r.cold, signable)
	require.NoError(t, ledger.VerifyOpCertSignature(
		oc, r.cold.Public().(ed25519.PublicKey),
	))
	b, err := opcert.EncodeCBOR(oc, r.cold.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	return b
}

func TestGenerateThenInstall(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil)
	vkey, err := GenStagedKey(t.Context(), r.cfg)
	require.NoError(t, err)
	assert.Equal(t, vkey, r.agent.Staged("pool-a"))

	// install-key is its own session: the agent drops one idle for 10s.
	conns := r.agent.Connections()
	require.NoError(
		t,
		InstallKey(t.Context(), r.cfg, r.opcert(t, vkey, 8), vkey, 8),
	)
	assert.Equal(t, conns+1, r.agent.Connections())
	got, issue := r.agent.Active("pool-a")
	assert.Equal(t, vkey, got)
	assert.Equal(t, uint64(8), issue)
	cmds := r.agent.Commands()
	require.Len(t, cmds, 2)
	assert.Equal(t, "gen-staged-key", cmds[0].Command)
	assert.Equal(t, "install-key", cmds[1].Command)
	assert.Equal(t, "pool-a", cmds[1].Profile)
}

// info reports the serving opcert once install-key has promoted a key.
func TestActiveKeyReportsInstalledOpCert(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil)
	vkey, err := GenStagedKey(t.Context(), r.cfg)
	require.NoError(t, err)
	raw := r.opcert(t, vkey, 8)
	require.NoError(t, InstallKey(t.Context(), r.cfg, raw, vkey, 8))

	got, err := ActiveKey(t.Context(), r.cfg)
	require.NoError(t, err)
	assert.Equal(t, vkey, got.KESVKey)
	assert.Equal(t, uint64(8), got.IssueNumber)
	assert.Equal(t, raw, got.OpCert)
	cmds := r.agent.Commands()
	assert.Equal(t, "info", cmds[len(cmds)-1].Command)
	assert.Equal(t, "pool-a", cmds[len(cmds)-1].Profile)
}

// A profile without a key, as after an agent restart, answers control
// sessions: KeyState reports no active key, and ActiveKey refuses it.
func TestKeyStateKeylessProfile(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(p *fakeagent.Profile) {
		p.ActiveVKey, p.ActiveIssue = nil, 0
	})

	state, err := KeyState(t.Context(), r.cfg)
	require.NoError(t, err)
	assert.Nil(t, state.Active)

	_, err = ActiveKey(t.Context(), r.cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active key")

	vkey, err := GenStagedKey(t.Context(), r.cfg)
	require.NoError(t, err)
	require.NoError(
		t,
		InstallKey(t.Context(), r.cfg, r.opcert(t, vkey, 1), vkey, 1),
	)
	got, err := ActiveKey(t.Context(), r.cfg)
	require.NoError(t, err)
	assert.Equal(t, vkey, got.KESVKey)
}

func TestRefusals(t *testing.T) {
	t.Parallel()
	t.Run("profile outside the permitted set", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, nil)
		r.cfg.Profile = "pool-b"
		_, err := GenStagedKey(t.Context(), r.cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fakeagent.ErrRoleNotPermitted)
		assert.Empty(t, r.agent.Commands())
	})
	t.Run("client key not pinned", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, nil)
		r.cfg.ClientCert, r.cfg.ClientKey, _ = fakeagent.ClientCert(t)
		_, err := GenStagedKey(t.Context(), r.cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fakeagent.ErrRoleNotPermitted)
	})
	t.Run("server key does not match the pin", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, nil)
		r.cfg.ServerSPKISHA256 = hexOf(sha256.Sum256([]byte("other")))
		_, err := GenStagedKey(t.Context(), r.cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match the pin")
		assert.Empty(t, r.agent.Commands())
	})
	t.Run("install-key refused", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, func(p *fakeagent.Profile) {
			p.InstallError = "kesagent: opcert expired"
		})
		vkey, err := GenStagedKey(t.Context(), r.cfg)
		require.NoError(t, err)
		err = InstallKey(t.Context(), r.cfg, r.opcert(t, vkey, 8), vkey, 8)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "opcert expired")
	})
	t.Run("install-key for another key", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, nil)
		vkey, err := GenStagedKey(t.Context(), r.cfg)
		require.NoError(t, err)
		other := make([]byte, 32)
		err = InstallKey(t.Context(), r.cfg, r.opcert(t, other, 8), other, 8)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not commit to the staged key")
		active, _ := r.agent.Active("pool-a")
		assert.NotEqual(t, vkey, active)
	})
}

func hexOf(sum [sha256.Size]byte) string {
	return hex.EncodeToString(sum[:])
}
