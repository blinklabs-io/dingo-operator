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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    dingov1alpha1.DingoNodeSpec
		wantErr bool
	}{
		{
			name: "valid relay",
			spec: dingov1alpha1.DingoNodeSpec{Role: dingov1alpha1.RoleRelay, Network: "mainnet"},
		},
		{
			// A bare `kind: DingoNode` with no spec must be rejected, not
			// silently reconciled as a relay.
			name:    "empty spec",
			spec:    dingov1alpha1.DingoNodeSpec{},
			wantErr: true,
		},
		{
			name:    "missing network",
			spec:    dingov1alpha1.DingoNodeSpec{Role: dingov1alpha1.RoleRelay},
			wantErr: true,
		},
		{
			name:    "invalid role",
			spec:    dingov1alpha1.DingoNodeSpec{Role: "gateway", Network: "mainnet"},
			wantErr: true,
		},
		{
			name:    "custom network without magic",
			spec:    dingov1alpha1.DingoNodeSpec{Role: dingov1alpha1.RoleRelay, Network: "custom"},
			wantErr: true,
		},
		{
			// Only the native Secret backend is implemented; reserved backends
			// must be rejected rather than silently mounting a plain Secret.
			name: "unsupported keys sourceType",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys: dingov1alpha1.KeysSpec{
						SourceType: dingov1alpha1.KeySourceExternalSecret,
						SecretRef:  "keys",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "custom network with magic",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom", NetworkMagic: new(int64(42)),
			},
		},
		{
			name:    "block producer without blockProducer section",
			spec:    dingov1alpha1.DingoNodeSpec{Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet"},
			wantErr: true,
		},
		{
			name: "block producer without secretRef",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{},
			},
			wantErr: true,
		},
		{
			name: "valid block producer monitor only",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys:     dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{Mode: dingov1alpha1.RotationModeMonitorOnly},
				},
			},
		},
		{
			name: "valid block producer assisted rotation",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys:     dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{Mode: dingov1alpha1.RotationModeAssisted},
				},
			},
		},
		{
			// Auto rotation is not implemented yet; it must be rejected rather
			// than silently accepted, even with a cold signer configured.
			name: "auto rotation rejected as unsupported",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys: dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{
						Mode: dingov1alpha1.RotationModeAuto,
						ColdSigner: dingov1alpha1.ColdSignerSpec{
							Type:     dingov1alpha1.ColdSignerBursa,
							Endpoint: "https://signer:8443",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			// ActiveStandby HA is not implemented yet; accepting it would give a
			// single-replica node while the user believes they have failover.
			name: "active standby rejected as unsupported",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys: dingov1alpha1.KeysSpec{SecretRef: "keys"},
					HA: dingov1alpha1.HASpec{
						Strategy: dingov1alpha1.HAActiveStandby,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dn := &dingov1alpha1.DingoNode{Spec: tc.spec}
			err := validateSpec(dn)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestChecksum(t *testing.T) {
	a := checksum("hello")
	b := checksum("hello")
	c := checksum("world")
	assert.Equal(t, a, b, "checksum must be stable")
	assert.NotEqual(t, a, c, "different input must differ")
	assert.Len(t, a, 16)
}
