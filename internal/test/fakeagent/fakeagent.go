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

// Package fakeagent is a TLS listener that answers the remote control role of
// a Bursa KES agent as documented in docs/signer/kes-agent-protocol.md in
// blinklabs-io/bursa at afd19ef, for tests. It keeps its own copy of the wire
// shapes so a mistake in the operator's client cannot be mirrored here.
package fakeagent

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// Error texts of the agent, from bursa's attested.go and attested_control.go.
const (
	ErrRoleNotPermitted = "kesagent: client is not permitted the requested role"
	errNoActiveKey      = "kesagent: no active KES key installed"
	errDropKey          = "kesagent: drop-key is not served on a remote control session"
)

// Profile configures one agent profile.
type Profile struct {
	Name string
	// ColdVKey is the cold verification key opcerts must carry and be signed by.
	ColdVKey []byte
	// ControlSPKI lists the client key fingerprints allowed to control it, as
	// ClientCert returns them.
	ControlSPKI [][sha256.Size]byte
	// ActiveVKey and ActiveIssue are the key the profile serves at start.
	ActiveVKey  []byte
	ActiveIssue uint64
	// ActiveOpCert is the CBOR opcert of the active key, as info reports it.
	ActiveOpCert []byte
	// InstallError, when set, makes install-key refuse with this text.
	InstallError string
	// InfoError, when set, makes info refuse with this text.
	InfoError string
}

// Command is a control command the agent received.
type Command struct {
	Command string
	Profile string
	OpCert  []byte
}

type profile struct {
	Profile
	staged []byte
}

// Agent is a running fake agent.
type Agent struct {
	// Addr is the listen address.
	Addr string
	// ServerSPKI is the hex SHA-256 of the agent certificate's SPKI, the pin
	// a client uses.
	ServerSPKI string

	mu       sync.Mutex
	profiles map[string]*profile
	cmds     []Command
	conns    int
}

// New starts an agent with profiles, closed when the test ends.
func New(t *testing.T, profiles ...Profile) *Agent {
	t.Helper()
	leaf, cert := newCert(t, "127.0.0.1")
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// Any certificate is taken and judged by its key afterwards, which
		// is what pinning in control_client_spki_sha256 amounts to.
		ClientAuth: tls.RequireAnyClientCert,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a := &Agent{
		Addr:       ln.Addr().String(),
		ServerSPKI: hex.EncodeToString(sum[:]),
		profiles:   map[string]*profile{},
	}
	for _, p := range profiles {
		a.profiles[p.Name] = &profile{Profile: p}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			a.mu.Lock()
			a.conns++
			a.mu.Unlock()
			wg.Go(func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				a.serve(conn.(*tls.Conn))
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return a
}

// Commands returns the control commands received so far, in order.
func (a *Agent) Commands() []Command {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Command(nil), a.cmds...)
}

// Connections returns the number of connections accepted.
func (a *Agent) Connections() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conns
}

// Active returns the verification key and issue number a profile serves.
func (a *Agent) Active(name string) ([]byte, uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.profile(name)
	return append([]byte(nil), p.ActiveVKey...), p.ActiveIssue
}

// ActiveOpCert returns the CBOR opcert a profile serves, or nil.
func (a *Agent) ActiveOpCert(name string) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]byte(nil), a.profile(name).ActiveOpCert...)
}

// SetInfoError makes info on a profile refuse with msg, or answer again when
// msg is empty.
func (a *Agent) SetInfoError(name, msg string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.profile(name).InfoError = msg
}

// Staged returns the verification key staged for a profile, or nil.
func (a *Agent) Staged(name string) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]byte(nil), a.profile(name).staged...)
}

// profile returns a configured profile; the caller holds a.mu.
func (a *Agent) profile(name string) *profile {
	p, ok := a.profiles[name]
	if !ok {
		panic("fakeagent: no profile " + name)
	}
	return p
}

type wireReply struct {
	Ok       bool       `json:"ok"`
	Error    string     `json:"error,omitempty"`
	KESVKey  []byte     `json:"kes_vkey,omitempty"`
	Info     *wireInfo  `json:"info,omitempty"`
	Profiles []wireInfo `json:"profiles,omitempty"`
}

type wireInfo struct {
	Profile           string `json:"profile,omitempty"`
	HasActiveKey      bool   `json:"has_active_key"`
	ActiveKESVKey     []byte `json:"active_kes_vkey"`
	ActiveIssueNumber uint64 `json:"active_issue_number"`
	ActiveOpCert      []byte `json:"active_opcert"`
	StagedKESVKey     []byte `json:"staged_kes_vkey"`
}

func (a *Agent) serve(conn *tls.Conn) {
	if err := conn.Handshake(); err != nil {
		return
	}
	if write(conn, map[string]string{
		"protocol": "bursa-attested-kes/1", "mode": "sign",
	}) != nil {
		return
	}
	var req struct {
		Type     string `json:"type"`
		Protocol string `json:"protocol"`
		Nonce    []byte `json:"nonce"`
		Profile  string `json:"profile"`
		Role     string `json:"role"`
	}
	if read(conn, &req) != nil {
		return
	}
	resp := map[string]any{
		"type": "attestation_response", "protocol": "bursa-attested-kes/1",
		"nonce": req.Nonce, "provider": "none",
	}
	refuse := func(msg string) {
		resp["error"] = msg
		_ = write(conn, resp)
	}
	if req.Role != "control" {
		refuse(ErrRoleNotPermitted)
		return
	}
	permitted := a.permitted(conn)
	if len(permitted) == 0 {
		refuse(ErrRoleNotPermitted)
		return
	}
	var bound *profile
	switch {
	case req.Profile != "":
		for _, p := range permitted {
			if p.Name == req.Profile {
				bound = p
			}
		}
		if bound == nil {
			refuse(ErrRoleNotPermitted)
			return
		}
	case len(permitted) == 1:
		bound = permitted[0]
	default:
		refuse("kesagent: request does not select exactly one profile")
		return
	}
	a.mu.Lock()
	active := len(bound.ActiveVKey) > 0
	a.mu.Unlock()
	if !active {
		refuse(errNoActiveKey)
		return
	}
	resp["role"] = "control"
	if write(conn, resp) != nil {
		return
	}
	for {
		var cmd struct {
			Command string `json:"command"`
			OpCert  []byte `json:"opcert"`
			Profile string `json:"profile"`
		}
		if read(conn, &cmd) != nil {
			return
		}
		a.mu.Lock()
		a.cmds = append(a.cmds, Command{
			Command: cmd.Command, Profile: cmd.Profile, OpCert: cmd.OpCert,
		})
		a.mu.Unlock()
		if write(
			conn,
			a.dispatch(cmd.Command, cmd.Profile, cmd.OpCert, permitted),
		) != nil {
			return
		}
	}
}

func (a *Agent) permitted(conn *tls.Conn) []*profile {
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil
	}
	sum := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []*profile
	for _, p := range a.profiles {
		for _, e := range p.ControlSPKI {
			if subtle.ConstantTimeCompare(sum[:], e[:]) == 1 {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

func (a *Agent) dispatch(
	cmd, name string,
	cert []byte,
	permitted []*profile,
) wireReply {
	fail := func(msg string) wireReply { return wireReply{Error: msg} }
	switch cmd {
	case "gen-staged-key", "install-key", "info":
	case "drop-key":
		return fail(errDropKey)
	default:
		return fail(fmt.Sprintf("unknown command %q", cmd))
	}
	var p *profile
	for _, c := range permitted {
		if c.Name == name || (name == "" && len(permitted) == 1) {
			p = c
		}
	}
	if p == nil {
		return fail(fmt.Sprintf(
			"kesagent: client is not permitted to control profile %q", name,
		))
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	info := func() *wireInfo {
		return &wireInfo{
			Profile: p.Name, HasActiveKey: len(p.ActiveVKey) > 0,
			ActiveKESVKey: p.ActiveVKey, ActiveIssueNumber: p.ActiveIssue,
			ActiveOpCert:  p.ActiveOpCert,
			StagedKESVKey: p.staged,
		}
	}
	switch cmd {
	case "gen-staged-key":
		seed := make([]byte, kes.SeedSize)
		_, _ = rand.Read(seed)
		_, vkey, err := kes.KeyGen(kes.CardanoKesDepth, seed)
		if err != nil {
			return fail(err.Error())
		}
		p.staged = vkey
		return wireReply{Ok: true, KESVKey: vkey, Info: info()}
	case "install-key":
		if p.InstallError != "" {
			return fail(p.InstallError)
		}
		oc, cold, err := opcert.ParseCBOR(cert)
		if err != nil {
			return fail(err.Error())
		}
		switch {
		case p.staged == nil:
			return fail("kesagent: no staged key")
		case !bytes.Equal(cold, p.ColdVKey):
			return fail(
				"kesagent: opcert cold key differs from the configured one",
			)
		case ledger.VerifyOpCertSignature(oc, p.ColdVKey) != nil:
			return fail("kesagent: opcert signature invalid")
		case !bytes.Equal(oc.KesVkey, p.staged):
			return fail("kesagent: opcert does not commit to the staged key")
		}
		p.ActiveVKey, p.ActiveIssue, p.staged = p.staged, oc.IssueNumber, nil
		p.ActiveOpCert = append([]byte(nil), cert...)
		return wireReply{Ok: true, Info: info()}
	default:
		if p.InfoError != "" {
			return fail(p.InfoError)
		}
		return wireReply{Ok: true, Info: info()}
	}
}

// ClientCert issues a throwaway client certificate and returns its PEM pair
// and the key fingerprint to list in Profile.ControlSPKI.
func ClientCert(t *testing.T) (certPEM, keyPEM []byte, spki [sha256.Size]byte) {
	t.Helper()
	leaf, cert := newCert(t, "")
	keyDER, err := x509.MarshalECPrivateKey(cert.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

func newCert(t *testing.T, ip string) (*x509.Certificate, tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fakeagent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		},
	}
	if ip != "" {
		tmpl.IPAddresses = []net.IP{net.ParseIP(ip)}
	}
	der, err := x509.CreateCertificate(
		rand.Reader,
		tmpl,
		tmpl,
		&key.PublicKey,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return leaf, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func write(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	hdr := binary.BigEndian.AppendUint32(
		nil, uint32(len(payload)), //nolint:gosec // small test frames
	)
	_, err = w.Write(append(hdr, payload...))
	return err
}

func read(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > 1<<20 {
		return errors.New("bad frame length")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	return json.Unmarshal(payload, v)
}
