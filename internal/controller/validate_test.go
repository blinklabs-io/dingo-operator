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
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    dingov1alpha1.DingoNodeSpec
		wantErr bool
	}{
		{
			name: "valid relay",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:    dingov1alpha1.RoleRelay,
				Network: "mainnet",
			},
		},
		{
			name: "core storage without API listener",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:        dingov1alpha1.RoleRelay,
				Network:     "mainnet",
				StorageMode: dingov1alpha1.StorageModeCore,
				APIs:        &dingov1alpha1.APISpec{},
			},
		},
		{
			name: "digest without tag",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "mainnet",
				Image: dingov1alpha1.ImageSpec{
					Digest: "sha256:" + strings.Repeat("a", 64),
				},
			},
			wantErr: true,
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
			name: "invalid load balancer CIDR",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:    dingov1alpha1.RoleRelay,
				Network: "mainnet",
				RelayService: dingov1alpha1.RelayServiceSpec{
					Type:                     corev1.ServiceTypeLoadBalancer,
					LoadBalancerSourceRanges: []string{"not-a-cidr"},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid role",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:    "gateway",
				Network: "mainnet",
			},
			wantErr: true,
		},
		{
			name: "custom network without magic",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:    dingov1alpha1.RoleRelay,
				Network: "custom",
			},
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
			// custom now also requires a config bundle, and Mithril must be
			// disabled (or given an aggregator) since a custom net has none.
			name: "custom network with magic, configRef, mithril disabled",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom",
				NetworkMagic: new(int64(42)), ConfigRef: "devnet-config",
				Mithril: dingov1alpha1.MithrilSpec{Enabled: new(false)},
			},
		},
		{
			// custom has no genesis built into Dingo, so configRef is required.
			name: "custom network without configRef",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom",
				NetworkMagic: new(int64(42)),
			},
			wantErr: true,
		},
		{
			// configRef + default Mithril (enabled) + no aggregator would
			// crash-loop the bootstrap init container; reject at admission.
			name: "configRef with mithril enabled but no aggregator",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom",
				NetworkMagic: new(int64(42)), ConfigRef: "devnet-config",
			},
			wantErr: true,
		},
		{
			name: "configRef with mithril aggregator set",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "custom",
				NetworkMagic: new(int64(42)), ConfigRef: "devnet-config",
				Mithril: dingov1alpha1.MithrilSpec{
					AggregatorURL: "https://aggregator.example/aggregator",
				},
			},
		},
		{
			name: "block producer without blockProducer section",
			spec: dingov1alpha1.DingoNodeSpec{
				Role:    dingov1alpha1.RoleBlockProducer,
				Network: "mainnet",
			},
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
					Keys: dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{
						Mode: dingov1alpha1.RotationModeMonitorOnly,
					},
				},
			},
		},
		{
			name: "valid block producer assisted rotation",
			spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleBlockProducer, Network: "mainnet",
				BlockProducer: &dingov1alpha1.BlockProducerSpec{
					Keys: dingov1alpha1.KeysSpec{SecretRef: "keys"},
					Rotation: dingov1alpha1.RotationSpec{
						Mode: dingov1alpha1.RotationModeAssisted,
					},
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

func TestValidateSpecRejectsManagedServiceSuffixes(t *testing.T) {
	t.Parallel()
	for _, suffix := range []string{"-private", "-relay", "-metrics", "-headless"} {
		dn := &dingov1alpha1.DingoNode{
			ObjectMeta: metav1.ObjectMeta{Name: "foo" + suffix},
			Spec: dingov1alpha1.DingoNodeSpec{
				Role: dingov1alpha1.RoleRelay, Network: "mainnet",
			},
		}
		require.ErrorContains(t, validateSpec(dn), "managed Service name")
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

func TestValidateSpecTracingImage(t *testing.T) {
	spec := func(tag string) *dingov1alpha1.DingoNode {
		dn := &dingov1alpha1.DingoNode{Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "mainnet",
			Tracing: dingov1alpha1.TracingSpec{Enabled: true},
			Image:   dingov1alpha1.ImageSpec{Tag: tag},
		}}
		return dn
	}

	require.NoError(t, validateSpec(spec("")))
	require.ErrorContains(t, validateSpec(spec("0.81.0")), "v0.82.0")
	require.NoError(t, validateSpec(spec("0.82.0")))

	disabled := spec("0.81.0")
	disabled.Spec.Tracing.Enabled = false
	require.NoError(t, validateSpec(disabled))
}

func TestValidateSpecRejectsListenerPortEnvOverrides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		key, value string
		wantErr    bool
	}{
		{"CARDANO_PORT", "4001", true},
		{"CARDANO_PRIVATE_PORT", "4002", true},
		{"CARDANO_METRICS_PORT", "9000", true},
		{"DINGO_HEALTH_PORT", "9001", true},
		{"CARDANO_PORT", "not-a-port", true},
		// Restating the operator's own port moves nothing.
		{"CARDANO_PORT", "3001", false},
		{"CARDANO_PRIVATE_PORT", "3002", false},
		{"CARDANO_METRICS_PORT", "12798", false},
		{"DINGO_HEALTH_PORT", "12799", false},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Parallel()
			dn := &dingov1alpha1.DingoNode{
				ObjectMeta: metav1.ObjectMeta{Name: "relay"},
				Spec: dingov1alpha1.DingoNodeSpec{
					Role: dingov1alpha1.RoleRelay, Network: "mainnet",
					Environment: map[string]string{tc.key: tc.value},
				},
			}
			err := validateSpec(dn)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.key)
			require.ErrorContains(t, err, "managed by the operator")
		})
	}
}
