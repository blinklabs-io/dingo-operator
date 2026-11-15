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

package opcert_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOpCert(t *testing.T) (*ledger.OpCert, []byte) {
	t.Helper()
	coldPub, coldPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	kesVkey := make([]byte, 32)
	for i := range kesVkey {
		kesVkey[i] = byte(i)
	}
	oc, err := ledger.CreateOpCert(kesVkey, 7, 42, coldPriv)
	require.NoError(t, err)
	return oc, coldPub
}

func TestEncodeParseRoundTrip(t *testing.T) {
	oc, coldVkey := testOpCert(t)

	env, err := opcert.Encode(oc, coldVkey)
	require.NoError(t, err)

	got, gotCold, err := opcert.Parse(env)
	require.NoError(t, err)
	assert.Equal(t, oc.KesVkey, got.KesVkey)
	assert.Equal(t, uint64(7), got.IssueNumber)
	assert.Equal(t, uint64(42), got.KesPeriod)
	assert.Equal(t, oc.ColdSignature, got.ColdSignature)
	assert.Equal(t, coldVkey, gotCold)
}

func TestEncodeProducesDingoCompatibleEnvelope(t *testing.T) {
	oc, coldVkey := testOpCert(t)

	env, err := opcert.Encode(oc, coldVkey)
	require.NoError(t, err)

	var fields struct {
		Type    string `json:"type"`
		CborHex string `json:"cborHex"`
	}
	require.NoError(t, json.Unmarshal(env, &fields))
	// Dingo's keystore rejects any other type string.
	assert.Equal(t, "NodeOperationalCertificate", fields.Type)
	assert.NotEmpty(t, fields.CborHex)
}

func TestParsedOpCertVerifies(t *testing.T) {
	oc, coldVkey := testOpCert(t)
	env, err := opcert.Encode(oc, coldVkey)
	require.NoError(t, err)

	got, gotCold, err := opcert.Parse(env)
	require.NoError(t, err)
	assert.NoError(t, ledger.VerifyOpCertSignature(got, gotCold))
}

func TestParseRejectsMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"not json", "{{{"},
		{"wrong type", `{"type":"VrfSigningKey_PraosVRF","cborHex":"40"}`},
		{"bad hex", `{"type":"NodeOperationalCertificate","cborHex":"zz"}`},
		{"empty cbor", `{"type":"NodeOperationalCertificate","cborHex":""}`},
		{
			"cbor not an array",
			`{"type":"NodeOperationalCertificate","cborHex":"40"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := opcert.Parse([]byte(tt.input))
			assert.Error(t, err)
		})
	}
}

// EncodeCBOR is the byte layout a KES agent's install-key takes:
// [[kes_vkey, issue_number, kes_period, cold_signature], cold_vkey].
func TestEncodeCBORLayout(t *testing.T) {
	t.Parallel()
	oc := &ledger.OpCert{
		KesVkey:       bytes.Repeat([]byte{1}, 32),
		IssueNumber:   7,
		KesPeriod:     300,
		ColdSignature: bytes.Repeat([]byte{2}, 64),
	}
	cold := bytes.Repeat([]byte{3}, 32)

	got, err := opcert.EncodeCBOR(oc, cold)
	require.NoError(t, err)

	want := []byte{0x82, 0x84, 0x58, 0x20}
	want = append(want, oc.KesVkey...)
	want = append(want, 0x07, 0x19, 0x01, 0x2c, 0x58, 0x40)
	want = append(want, oc.ColdSignature...)
	want = append(want, 0x58, 0x20)
	want = append(want, cold...)
	assert.Equal(t, want, got)

	back, backCold, err := opcert.ParseCBOR(got)
	require.NoError(t, err)
	assert.Equal(t, oc, back)
	assert.Equal(t, cold, backCold)

	_, err = opcert.EncodeCBOR(oc, cold[:31])
	require.Error(t, err)
}
