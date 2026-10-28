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
	"crypto/rand"
	"encoding/hex"
	"maps"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// The dingo Helm chart names the operational certificate node.cert; either
// name is accepted, exactly one of them, or both when identical.
func TestValidateKeysSecretNodeCert(t *testing.T) {
	t.Parallel()
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	good, err := keys.SecretData(3, 5)
	require.NoError(t, err)
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	otherData, err := other.SecretData(3, 5)
	require.NoError(t, err)

	renamed := func(data map[string][]byte, cert []byte) map[string][]byte {
		out := maps.Clone(data)
		delete(out, "opcert.cert")
		out["node.cert"] = cert
		return out
	}
	node := func() *dingov1alpha1.DingoNode {
		return &dingov1alpha1.DingoNode{
			Spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer,
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					PoolID:            hex.EncodeToString(keys.PoolID),
					SlotsPerKESPeriod: 120,
					MaxKESEvolutions:  62,
					Keys: dingov1alpha1.KeysSpec{
						SecretRef: "keys",
					},
				},
			},
			Status: dingov1alpha1.DingoNodeStatus{
				KES:    dingov1alpha1.KESStatus{CurrentPeriod: 6},
				OpCert: dingov1alpha1.OpCertStatus{OnDiskCounter: 3},
			},
		}
	}
	both := maps.Clone(good)
	both["node.cert"] = good["opcert.cert"]
	bothDiffer := maps.Clone(good)
	bothDiffer["node.cert"] = otherData["opcert.cert"]
	emptyOpcert := renamed(good, good["opcert.cert"])
	emptyOpcert["opcert.cert"] = []byte{}
	neither := maps.Clone(good)
	delete(neither, "opcert.cert")

	for _, tc := range []struct {
		name    string
		data    map[string][]byte
		wantErr string
	}{
		{"opcert.cert alone", good, ""},
		{"node.cert alone", renamed(good, good["opcert.cert"]), ""},
		{"both and identical", both, ""},
		{"empty opcert.cert with node.cert", emptyOpcert, ""},
		{"both and different", bothDiffer, "both opcert.cert and node.cert"},
		{"neither", neither, "opcert.cert or node.cert"},
		{"node.cert malformed", renamed(good, []byte("{{{")), "node.cert"},
		{"node.cert for another pool", renamed(good, otherData["opcert.cert"]), "pool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state, err := validateKeysSecret(
				&corev1.Secret{Data: tc.data}, node(),
			)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, int64(3), state.Counter)
		})
	}
}

func TestKeysChecksumNodeCert(t *testing.T) {
	t.Parallel()
	secret := func(data map[string]string) *corev1.Secret {
		s := &corev1.Secret{Data: map[string][]byte{}}
		for k, v := range data {
			s.Data[k] = []byte(v)
		}
		return s
	}
	opcert := keysChecksum(secret(map[string]string{
		"kes.skey": "k", "vrf.skey": "v", "opcert.cert": "cert",
	}))
	node := keysChecksum(secret(map[string]string{
		"kes.skey": "k", "vrf.skey": "v", "node.cert": "cert",
	}))
	// Renaming the certificate file does not change what the node runs on, so
	// it must not roll the block producer.
	assert.Equal(t, opcert, node)
	both := keysChecksum(secret(map[string]string{
		"kes.skey": "k", "vrf.skey": "v", "opcert.cert": "cert", "node.cert": "cert",
	}))
	assert.Equal(t, opcert, both)
	// A new certificate under either name does.
	rotated := keysChecksum(secret(map[string]string{
		"kes.skey": "k", "vrf.skey": "v", "node.cert": "cert-2",
	}))
	assert.NotEqual(t, node, rotated)
}
