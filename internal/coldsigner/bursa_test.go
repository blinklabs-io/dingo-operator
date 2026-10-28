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

package coldsigner

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBursa answers POST /v1/sign type=opcert with chain_counter the way the
// Bursa signer does, with knobs to misbehave.
type fakeBursa struct {
	t       *testing.T
	key     ed25519.PrivateKey
	keyHash string
	status  int
	// issueOffset is added to chain_counter + 1 in the response.
	issueOffset int
	// signedIssue, when non-zero, is the counter actually signed.
	signedIssue uint64
	otherKey    ed25519.PrivateKey
	// errBody replaces the error text sent with status.
	errBody string
	got     map[string]any
}

func (f *fakeBursa) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	require.Equal(f.t, "/v1/sign", r.URL.Path)
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&f.got))
	if f.status != 0 {
		msg := "opcert chain counter is below the recorded on-chain counter"
		if f.errBody != "" {
			msg = f.errBody
		}
		http.Error(w, msg, f.status)
		return
	}
	kes, _ := hex.DecodeString(f.got["kes_vkey"].(string))
	chain := uint64(f.got["chain_counter"].(float64))
	period := uint64(f.got["kes_period"].(float64))
	issue := uint64(int64(chain+1) + int64(f.issueOffset))
	signed := issue
	if f.signedIssue != 0 {
		signed = f.signedIssue
	}
	key := f.key
	if f.otherKey != nil {
		key = f.otherKey
	}
	sig := ed25519.Sign(key, lcommon.OpCertSignableBytes(kes, signed, period))
	_ = json.NewEncoder(w).Encode(map[string]any{
		"audit_id":      "a",
		"signature":     hex.EncodeToString(sig),
		"cold_vkey":     hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		"key":           f.keyHash,
		"issue_counter": issue,
	})
}

func newFakeBursa(t *testing.T) *fakeBursa {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	h := lcommon.Blake2b224Hash(priv.Public().(ed25519.PublicKey))
	return &fakeBursa{t: t, key: priv, keyHash: hex.EncodeToString(h[:])}
}

func caPEM(srv *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

func startBursa(t *testing.T, f *fakeBursa) (*httptest.Server, *BursaSigner) {
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	s, err := NewBursaSigner(BursaConfig{Endpoint: srv.URL, ColdKeyHash: f.keyHash, CACert: caPEM(srv)})
	require.NoError(t, err)
	return srv, s
}

func TestBursaSignOpCertFromChain(t *testing.T) {
	f := newFakeBursa(t)
	_, s := startBursa(t, f)
	kes := make([]byte, 32)
	kes[0] = 7

	res, err := s.SignOpCertFromChain(context.Background(), kes, 4, 900)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), res.IssueCounter)
	assert.Equal(t, f.key.Public(), res.ColdVkey)
	assert.True(t, ed25519.Verify(res.ColdVkey, lcommon.OpCertSignableBytes(kes, 5, 900), res.Signature))

	assert.Equal(t, "opcert", f.got["type"])
	assert.Equal(t, f.keyHash, f.got["key"])
	assert.Equal(t, hex.EncodeToString(kes), f.got["kes_vkey"])
	assert.EqualValues(t, 4, f.got["chain_counter"])
	assert.EqualValues(t, 900, f.got["kes_period"])
	assert.NotContains(t, f.got, "issue_counter")
}

func TestBursaSignOpCertFromChainRejects(t *testing.T) {
	kes := make([]byte, 32)
	for name, tweak := range map[string]func(f *fakeBursa){
		"issue not chain plus one": func(f *fakeBursa) { f.issueOffset = 1 },
		"signature over another issue": func(f *fakeBursa) {
			f.signedIssue = 9
		},
		"another cold key": func(f *fakeBursa) {
			_, f.otherKey, _ = ed25519.GenerateKey(rand.Reader)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeBursa(t)
			tweak(f)
			_, s := startBursa(t, f)
			_, err := s.SignOpCertFromChain(context.Background(), kes, 4, 900)
			assert.Error(t, err)
		})
	}
}

func TestBursaSignOpCertFromChainConflict(t *testing.T) {
	f := newFakeBursa(t)
	f.status = http.StatusConflict
	_, s := startBursa(t, f)
	_, err := s.SignOpCertFromChain(context.Background(), make([]byte, 32), 4, 900)
	assert.ErrorIs(t, err, ErrCounterConflict)
}

func TestBursaSignOpCertFromChainBadInput(t *testing.T) {
	f := newFakeBursa(t)
	_, s := startBursa(t, f)
	_, err := s.SignOpCertFromChain(context.Background(), make([]byte, 31), 4, 900)
	assert.Error(t, err)
	_, err = s.SignOpCertFromChain(context.Background(), make([]byte, 32), ^uint64(0), 900)
	assert.Error(t, err)
}

func TestBursaSignerUntrustedServer(t *testing.T) {
	f := newFakeBursa(t)
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	// no CA: the test server's certificate is not trusted
	s, err := NewBursaSigner(BursaConfig{Endpoint: srv.URL, ColdKeyHash: f.keyHash})
	require.NoError(t, err)
	_, err = s.SignOpCertFromChain(context.Background(), make([]byte, 32), 4, 900)
	assert.Error(t, err)
}

func TestNewBursaSignerRequiresHTTPS(t *testing.T) {
	_, err := NewBursaSigner(BursaConfig{Endpoint: "http://signer:8443", ColdKeyHash: "00"})
	assert.Error(t, err)
}

// selfSigned returns a PEM certificate and key for a TLS client.
func selfSigned(t *testing.T) (certPEM, keyPEM []byte) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "dingo-operator"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	k, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: k})
}

func TestBursaSignerPresentsClientCertificate(t *testing.T) {
	f := newFakeBursa(t)
	srv := httptest.NewUnstartedServer(f)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cert, key := selfSigned(t)
	s, err := NewBursaSigner(BursaConfig{
		Endpoint: srv.URL, ColdKeyHash: f.keyHash, CACert: caPEM(srv),
		ClientCert: cert, ClientKey: key,
	})
	require.NoError(t, err)
	_, err = s.SignOpCertFromChain(context.Background(), make([]byte, 32), 4, 900)
	require.NoError(t, err)
}

// The error reaches a status condition, which has a size limit; a large error
// page from the signer or a proxy in front of it must not fill it.
func TestBursaSignerBoundsErrorText(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusBadGateway} {
		f := newFakeBursa(t)
		f.status = status
		f.errBody = strings.Repeat("x", 60<<10)
		_, s := startBursa(t, f)
		_, err := s.SignOpCertFromChain(
			context.Background(), make([]byte, 32), 4, 900,
		)
		require.Error(t, err)
		assert.Less(t, len(err.Error()), 1024)
	}
}
