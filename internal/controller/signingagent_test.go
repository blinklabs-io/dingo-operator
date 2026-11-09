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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"maps"
	"math/big"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testSPKIPin = "0123456789abcdef0123456789abcdef" +
	"0123456789abcdef0123456789abcdef"

func withoutKey(m map[string][]byte, k string) map[string][]byte {
	out := maps.Clone(m)
	delete(out, k)
	return out
}

func TestValidateKeysSecretSigningAgent(t *testing.T) {
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	goodData, err := keys.SecretData(3, 5)
	require.NoError(t, err)
	agentData := withoutKey(goodData, keyFileKES)

	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	otherData, err := other.SecretData(3, 5)
	require.NoError(t, err)
	// A KES key that does not match the opcert: refused without an agent,
	// ignored with one.
	otherKES := otherData[keyFileKES]

	node := func(
		agent bool,
		mutate func(*dingov1alpha1.DingoNode),
	) *dingov1alpha1.DingoNode {
		dn := &dingov1alpha1.DingoNode{
			Spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer,
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					PoolID:            hex.EncodeToString(keys.PoolID),
					SlotsPerKESPeriod: 120,
					MaxKESEvolutions:  62,
					Keys:              dingov1alpha1.KeysSpec{SecretRef: "keys"},
				},
			},
			Status: dingov1alpha1.DingoNodeStatus{
				KES:    dingov1alpha1.KESStatus{CurrentPeriod: 6},
				OpCert: dingov1alpha1.OpCertStatus{OnDiskCounter: 3},
			},
		}
		if agent {
			dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
				Address: "kes-agent:7000",
			}
		}
		if mutate != nil {
			mutate(dn)
		}
		return dn
	}

	tests := []struct {
		name       string
		agent      bool
		data       map[string][]byte
		mutate     func(*dingov1alpha1.DingoNode)
		wantErr    string
		wantUnused bool
	}{
		{
			name:  "agent without kes.skey",
			agent: true,
			data:  agentData,
		},
		{
			name:    "kes.skey still required without an agent",
			data:    agentData,
			wantErr: "missing kes.skey",
		},
		{
			name:       "agent ignores a stray kes.skey of another pool",
			agent:      true,
			data:       withKey(agentData, keyFileKES, otherKES),
			wantUnused: true,
		},
		{
			name:       "agent ignores a malformed stray kes.skey",
			agent:      true,
			data:       withKey(agentData, keyFileKES, []byte("not a key")),
			wantUnused: true,
		},
		{
			name:    "mismatched kes.skey refused without an agent",
			data:    withKey(goodData, keyFileKES, otherKES),
			wantErr: "kes",
		},
		{
			name:    "agent still requires vrf.skey",
			agent:   true,
			data:    withoutKey(agentData, keyFileVRF),
			wantErr: "missing vrf.skey",
		},
		{
			name:    "agent still requires the opcert",
			agent:   true,
			data:    withoutKey(agentData, keyFileOpCert),
			wantErr: "opcert.cert or node.cert",
		},
		{
			name:  "agent with a vote key",
			agent: true,
			data:  withKey(agentData, keyFileVote, voteEnvelope(t, 42)),
		},
		{
			name:  "agent still checks the vote key",
			agent: true,
			data: withKey(
				agentData, keyFileVote,
				rawKeyEnvelope(t, voteEnvelopeType, modulusScalar(t, 0)),
			),
			wantErr: "vote.skey",
		},
		{
			name:  "agent still binds the opcert to the pool",
			agent: true,
			data: withKey(
				agentData, keyFileOpCert, otherData[keyFileOpCert],
			),
			wantErr: "pool",
		},
		{
			name:  "agent still refuses a counter regression",
			agent: true,
			data:  agentData,
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Status.OpCert.OnDiskCounter = 4
			},
			wantErr: "regresses",
		},
		{
			name:  "agent still refuses an expired opcert",
			agent: true,
			data:  agentData,
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Status.KES.CurrentPeriod = 100
			},
			wantErr: "expired",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			secret := &corev1.Secret{Data: tc.data}
			state, err := validateKeysSecret(secret, node(tc.agent, tc.mutate))
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, state)
			assert.Equal(t, int64(3), state.Counter)
			assert.Equal(t, int64(5), state.KESPeriod)
			assert.Equal(t, tc.wantUnused, state.UnusedKESKey)
		})
	}
}

func TestValidateSpecSigningAgent(t *testing.T) {
	t.Parallel()
	poolID := strings.Repeat("ab", 28)
	node := func(
		mutate func(*dingov1alpha1.BlockProducerSpec),
	) *dingov1alpha1.DingoNode {
		bp := &dingov1alpha1.BlockProducerSpec{
			PoolID: poolID,
			Keys:   dingov1alpha1.KeysSpec{SecretRef: "keys"},
			SigningAgent: &dingov1alpha1.SigningAgentSpec{
				Address: "kes-agent.signing.svc:7000",
			},
		}
		if mutate != nil {
			mutate(bp)
		}
		return &dingov1alpha1.DingoNode{
			ObjectMeta: metav1.ObjectMeta{Name: "bp"},
			Spec: dingov1alpha1.DingoNodeSpec{
				Role:          dingov1alpha1.RoleBlockProducer,
				Network:       "preview",
				Topology:      validTopology,
				BlockProducer: bp,
			},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*dingov1alpha1.BlockProducerSpec)
		wantErr string
	}{
		{name: "address only"},
		{
			name: "pinned",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ServerSPKISHA256 = testSPKIPin
			},
		},
		{
			name: "pin and client certificate",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ServerSPKISHA256 = testSPKIPin
				bp.SigningAgent.ClientCertSecretRef = "agent-client"
			},
		},
		{
			name: "plaintext",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Plaintext = true
			},
		},
		{
			name: "IPv6 address",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "[fd00::1]:7000"
			},
		},
		{
			name: "Assisted rotation",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.Rotation.Mode = dingov1alpha1.RotationModeAssisted
			},
		},
		{
			name: "MonitorOnly rotation",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.Rotation.Mode = dingov1alpha1.RotationModeMonitorOnly
			},
		},
		{
			name: "Auto rotation refused",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				// A complete Auto setup, so the refusal is the agent's.
				bp.NodeToClient.Enabled = true
				bp.Rotation = dingov1alpha1.RotationSpec{
					Mode: dingov1alpha1.RotationModeAuto,
					ColdSigner: dingov1alpha1.ColdSignerSpec{
						Type:          dingov1alpha1.ColdSignerBursa,
						Endpoint:      "https://signer:8443",
						MTLSSecretRef: "signer-mtls",
						ColdKeyHash:   poolID,
					},
				}
			},
			wantErr: "rotation.mode Auto",
		},
		{
			name: "address without a port",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent"
			},
			wantErr: "host:port",
		},
		{
			name: "address with an empty port",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent:"
			},
			wantErr: "host:port",
		},
		{
			name: "address with a non-numeric port",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent:http"
			},
			wantErr: "port from 1 to 65535",
		},
		{
			name: "address with port 0",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent:0"
			},
			wantErr: "port from 1 to 65535",
		},
		{
			name: "address with a port above 65535",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent:65536"
			},
			wantErr: "port from 1 to 65535",
		},
		{
			name: "address with port 65535",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Address = "kes-agent:65535"
			},
		},
		{
			name: "padded pin",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ServerSPKISHA256 = " " + testSPKIPin + " "
			},
		},
		{
			name: "plaintext with a pin",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Plaintext = true
				bp.SigningAgent.ServerSPKISHA256 = testSPKIPin
			},
			wantErr: "plaintext takes neither",
		},
		{
			name: "plaintext with a client certificate",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.Plaintext = true
				bp.SigningAgent.ClientCertSecretRef = "agent-client"
			},
			wantErr: "plaintext takes neither",
		},
		{
			name: "short pin",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ServerSPKISHA256 = "abcd"
			},
			wantErr: "64 hex characters",
		},
		{
			name: "non-hex pin",
			mutate: func(bp *dingov1alpha1.BlockProducerSpec) {
				bp.SigningAgent.ServerSPKISHA256 = strings.Repeat("zz", 32)
			},
			wantErr: "64 hex characters",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateSpec(node(tc.mutate))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	t.Run("local KES key override refused", func(t *testing.T) {
		t.Parallel()
		dn := node(nil)
		dn.Spec.Environment = map[string]string{
			"CARDANO_SHELLEY_KES_KEY": "/keys/kes.skey",
		}
		err := validateSpec(dn)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CARDANO_SHELLEY_KES_KEY")
	})
}

// testClientCertPair returns a PEM self-signed client certificate and its key.
func testClientCertPair(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "producer"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestCheckSigningAgentCertData(t *testing.T) {
	t.Parallel()
	crt, key := testClientCertPair(t)
	_, otherKey := testClientCertPair(t)
	for _, tc := range []struct {
		name string
		data map[string][]byte
		want string
	}{
		{
			name: "both halves",
			data: map[string][]byte{"tls.crt": crt, "tls.key": key},
		},
		{
			name: "certificate only",
			data: map[string][]byte{"tls.crt": crt},
			want: "missing tls.key",
		},
		{
			name: "key only",
			data: map[string][]byte{"tls.key": key},
			want: "missing tls.crt",
		},
		{
			name: "empty certificate",
			data: map[string][]byte{"tls.crt": {}, "tls.key": key},
			want: "missing tls.crt",
		},
		{
			name: "not PEM",
			data: map[string][]byte{
				"tls.crt": []byte("cert"),
				"tls.key": []byte("key"),
			},
			want: "usable PEM certificate and matching key",
		},
		{
			name: "key of another certificate",
			data: map[string][]byte{"tls.crt": crt, "tls.key": otherKey},
			want: "usable PEM certificate and matching key",
		},
		{
			name: "neither",
			data: nil,
			want: "missing tls.crt, tls.key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkSigningAgentCertData("agent-client", tc.data)
			if tc.want == "" {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tc.want)
		})
	}
}
