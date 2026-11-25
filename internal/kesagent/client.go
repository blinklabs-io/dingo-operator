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

// Package kesagent is a minimal client for the remote control role of a Bursa
// KES agent: gen-staged-key and install-key over the attested listener.
//
// The wire shapes follow docs/signer/kes-agent-protocol.md and
// kesagent/wire/messages.go in Bursa v0.26. They are copied here for exactly
// the commands the operator needs instead of importing the signer binary.
package kesagent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const (
	attestedProtocolID = "bursa-attested-kes/1"
	roleControl        = "control"
	cmdGenStagedKey    = "gen-staged-key"
	cmdInstallKey      = "install-key"
	cmdInfo            = "info"

	nonceSize = 32
	// maxFrameLen is the agent's cap on one frame payload.
	maxFrameLen = 1 << 20
	// kesVKeySize is the size of a KES verification key.
	kesVKeySize = 32

	// requestTimeout bounds one session when ctx has no earlier deadline.
	requestTimeout = 30 * time.Second
)

type hello struct {
	Protocol string `json:"protocol"`
	Mode     string `json:"mode"`
}

type attestationRequest struct {
	Type     string `json:"type"`
	Protocol string `json:"protocol"`
	Nonce    []byte `json:"nonce"`
	Profile  string `json:"profile,omitempty"`
	Role     string `json:"role,omitempty"`
}

type attestationResponse struct {
	Type     string `json:"type"`
	Protocol string `json:"protocol"`
	Nonce    []byte `json:"nonce"`
	Role     string `json:"role,omitempty"`
	Error    string `json:"error,omitempty"`
}

type command struct {
	Command string `json:"command"`
	OpCert  []byte `json:"opcert,omitempty"`
	Profile string `json:"profile,omitempty"`
}

// agentInfo holds the AgentInfo fields the operator reads back.
type agentInfo struct {
	HasActiveKey      bool   `json:"has_active_key"`
	ActiveKESVKey     []byte `json:"active_kes_vkey"`
	ActiveIssueNumber uint64 `json:"active_issue_number"`
	ActiveOpCert      []byte `json:"active_opcert"`
	StagedKESVKey     []byte `json:"staged_kes_vkey"`
}

type reply struct {
	Ok      bool       `json:"ok"`
	Error   string     `json:"error,omitempty"`
	KESVKey []byte     `json:"kes_vkey,omitempty"`
	Info    *agentInfo `json:"info,omitempty"`
	// StagedKeyAttestation accompanies gen-staged-key from an agent that has
	// an attestation identity key.
	StagedKeyAttestation *StagedKeyAttestation `json:"staged_key_attestation,omitempty"`
}

// StagedKeyAttestation is the agent's signed statement that it generated a
// staged KES key for a profile, as the Bursa signer verifies it in an opcert
// request. The operator forwards it untouched and does not verify it.
type StagedKeyAttestation struct {
	Profile     string `json:"profile"`
	KESVKey     []byte `json:"kes_vkey"`
	ColdKeyHash []byte `json:"cold_key_hash"`
	SPKI        []byte `json:"spki"`
	Signature   []byte `json:"signature"`
}

// Config addresses one profile of an agent's attested listener.
type Config struct {
	// Address is the host:port of the listener.
	Address string
	// ServerSPKISHA256 is the hex SHA-256 of the agent certificate's
	// SubjectPublicKeyInfo. Empty verifies the certificate against the
	// address's host name instead.
	ServerSPKISHA256 string
	// ClientCert and ClientKey are the PEM certificate and key the agent has
	// pinned for the control role.
	ClientCert, ClientKey []byte
	// Profile names the agent profile; empty when the agent has one.
	Profile string
}

// GenStagedKey has the agent generate a staged KES key for the profile and
// returns its verification key and the agent's attestation of it, which is nil
// when the agent has no attestation identity. The agent never reveals the
// signing key. Info does not report the attestation, so a caller that wants
// to offer it again for the same staged key must keep it.
func GenStagedKey(
	ctx context.Context,
	cfg Config,
) ([]byte, *StagedKeyAttestation, error) {
	r, err := cfg.do(
		ctx,
		command{Command: cmdGenStagedKey, Profile: cfg.Profile},
	)
	if err != nil {
		return nil, nil, err
	}
	if len(r.KESVKey) != kesVKeySize {
		return nil, nil, fmt.Errorf(
			"agent returned a %d-byte staged kes vkey, want %d",
			len(r.KESVKey), kesVKeySize,
		)
	}
	return r.KESVKey, r.StagedKeyAttestation, nil
}

// InstallKey installs opcert, the CBOR operational certificate issued for the
// staged key, and promotes that key. It requires the agent to report the
// certificate's KES verification key and issue number as active. It opens its
// own session: the agent closes one left waiting more than 10 seconds for a
// frame, which an opcert signature can outlast.
func InstallKey(
	ctx context.Context,
	cfg Config,
	opcert, kesVKey []byte,
	issueNumber uint64,
) error {
	r, err := cfg.do(ctx, command{
		Command: cmdInstallKey, OpCert: opcert, Profile: cfg.Profile,
	})
	if err != nil {
		return err
	}
	if r.Info == nil || !r.Info.HasActiveKey ||
		!bytes.Equal(r.Info.ActiveKESVKey, kesVKey) ||
		r.Info.ActiveIssueNumber != issueNumber {
		return errors.New(
			"agent accepted install-key but does not report the " +
				"installed key and issue number as active",
		)
	}
	return nil
}

// Active is the key an agent profile serves.
type Active struct {
	// KESVKey is the verification key of the serving KES key.
	KESVKey []byte
	// IssueNumber is the issue number of the serving opcert.
	IssueNumber uint64
	// OpCert is the serving operational certificate, CBOR.
	OpCert []byte
}

// State is the active key and any key already staged for the next rotation.
type State struct {
	// Active is nil when the profile holds no key, as after an agent restart
	// without a shared key store.
	Active        *Active
	StagedKESVKey []byte
}

// KeyState returns the key state reported by the profile. A profile with no
// installed key is not an error: Active is nil. A staged key is
// retained by Bursa until it is installed or explicitly replaced, so callers
// can retry an exact-key authorization without generating a different key.
func KeyState(ctx context.Context, cfg Config) (*State, error) {
	r, err := cfg.do(ctx, command{Command: cmdInfo, Profile: cfg.Profile})
	if err != nil {
		return nil, err
	}
	if r.Info == nil {
		return nil, errors.New("agent returned no info")
	}
	if len(r.Info.StagedKESVKey) != 0 &&
		len(r.Info.StagedKESVKey) != kesVKeySize {
		return nil, fmt.Errorf(
			"agent returned a %d-byte staged kes vkey, want %d",
			len(r.Info.StagedKESVKey), kesVKeySize,
		)
	}
	if !r.Info.HasActiveKey {
		return &State{StagedKESVKey: r.Info.StagedKESVKey}, nil
	}
	return &State{
		Active: &Active{
			KESVKey:     r.Info.ActiveKESVKey,
			IssueNumber: r.Info.ActiveIssueNumber,
			OpCert:      r.Info.ActiveOpCert,
		},
		StagedKESVKey: r.Info.StagedKESVKey,
	}, nil
}

// ActiveKey returns the key the profile serves, from the agent's info
// command. It is how a rotation interrupted after install-key finds the
// opcert the agent already serves.
func ActiveKey(ctx context.Context, cfg Config) (*Active, error) {
	state, err := KeyState(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if state.Active == nil {
		return nil, errors.New("agent reports no active key")
	}
	return state.Active, nil
}

// do opens a control session, sends cmd and returns an accepted reply.
func (cfg Config) do(ctx context.Context, cmd command) (*reply, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	conn, err := cfg.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	// The deadline does not follow a cancellation.
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer stop()

	if err := cfg.admit(conn); err != nil {
		return nil, wrapCtx(ctx, "attest control session", err)
	}
	if err := writeFrame(conn, cmd); err != nil {
		return nil, wrapCtx(ctx, "send "+cmd.Command, err)
	}
	var r reply
	if err := readFrame(conn, &r); err != nil {
		return nil, wrapCtx(ctx, "read "+cmd.Command+" reply", err)
	}
	if !r.Ok {
		return nil, fmt.Errorf("agent refused %s: %s", cmd.Command, r.Error)
	}
	return &r, nil
}

func wrapCtx(ctx context.Context, what string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", what, ctx.Err())
	}
	return fmt.Errorf("%s: %w", what, err)
}

// admit reads the hello and runs the attestation exchange for the control
// role. The evidence is not verified: the operator trusts the pinned server
// key, and a control client reads no key state from the response.
func (cfg Config) admit(conn net.Conn) error {
	var h hello
	if err := readFrame(conn, &h); err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if h.Protocol != attestedProtocolID {
		return fmt.Errorf(
			"agent speaks %q, not %q", h.Protocol, attestedProtocolID,
		)
	}
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("read nonce: %w", err)
	}
	if err := writeFrame(conn, attestationRequest{
		Type:     "attestation_request",
		Protocol: attestedProtocolID,
		Nonce:    nonce,
		Profile:  cfg.Profile,
		Role:     roleControl,
	}); err != nil {
		return fmt.Errorf("send attestation request: %w", err)
	}
	var resp attestationResponse
	if err := readFrame(conn, &resp); err != nil {
		return fmt.Errorf("read attestation response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("agent refused the control role: %s", resp.Error)
	}
	if resp.Protocol != attestedProtocolID ||
		!bytes.Equal(resp.Nonce, nonce) {
		return errors.New("attestation response does not answer the request")
	}
	if resp.Role != roleControl {
		return fmt.Errorf(
			"agent admitted role %q, not %q",
			resp.Role,
			roleControl,
		)
	}
	return nil
}

// dial opens the TLS 1.3 connection with the client certificate.
func (cfg Config) dial(ctx context.Context) (*tls.Conn, error) {
	pair, err := tls.X509KeyPair(cfg.ClientCert, cfg.ClientKey)
	if err != nil {
		return nil, fmt.Errorf("load control client certificate: %w", err)
	}
	host, _, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("agent address %q: %w", cfg.Address, err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		ServerName:   host,
	}
	if pin := strings.TrimSpace(cfg.ServerSPKISHA256); pin != "" {
		want, err := hex.DecodeString(pin)
		if err != nil || len(want) != sha256.Size {
			return nil, errors.New(
				"agent server pin is not a SHA-256 hex digest",
			)
		}
		// The pin replaces chain verification, as it does for the node's own
		// link to the agent.
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // pinned below
		tlsCfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("agent presented no certificate")
			}
			got := sha256.Sum256(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
			if subtle.ConstantTimeCompare(got[:], want) != 1 {
				return errors.New("agent server key does not match the pin")
			}
			return nil
		}
	}
	d := &tls.Dialer{Config: tlsCfg, NetDialer: &net.Dialer{}}
	conn, err := d.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("connect to agent %s: %w", cfg.Address, err)
	}
	return conn.(*tls.Conn), nil
}

func writeFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(payload) > maxFrameLen {
		return fmt.Errorf("frame of %d bytes exceeds the limit", len(payload))
	}
	// The length is bounded by maxFrameLen above.
	buf := binary.BigEndian.AppendUint32(
		nil, uint32(len(payload)), //nolint:gosec // bounded above
	)
	_, err = w.Write(append(buf, payload...))
	return err
}

func readFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrameLen {
		return fmt.Errorf("invalid frame length %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	return json.Unmarshal(payload, v)
}
