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
	"k8s.io/utils/ptr"
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
			name:    "custom network without magic",
			spec:    dingov1alpha1.DingoNodeSpec{Role: dingov1alpha1.RoleRelay, Network: "custom"},
			wantErr: true,
		},
		{
			name: "custom network with magic",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom", NetworkMagic: ptr.To(uint32(42)),
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
			name: "auto rotation without cold signer",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys:     dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{Mode: dingov1alpha1.RotationModeAuto},
				},
			},
			wantErr: true,
		},
		{
			name: "auto rotation with bursa signer",
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
