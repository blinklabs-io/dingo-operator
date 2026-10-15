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

package resources

import (
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func apiNode() *dingov1alpha1.DingoNode {
	dn := relayNode()
	dn.Spec.StorageMode = dingov1alpha1.StorageModeAPI
	dn.Spec.APIs = &dingov1alpha1.APISpec{
		UTxORPC:    9090,
		Blockfrost: 3000,
		Mesh:       8080,
		Kupo:       1442,
		Bark:       3003,
		Midnight:   50051,
	}
	return dn
}

func TestAPIExposure(t *testing.T) {
	for _, producer := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "relay", true: "producer"}[producer],
			func(t *testing.T) {
				dn := apiNode()
				if producer {
					dn.Spec.Role = dingov1alpha1.RoleBlockProducer
					dn.Spec.BlockProducer = bpNode().Spec.BlockProducer
				}
				require.NoError(t, ValidateAPIs(dn))
				env := envMap(dn, RenderOptions{})
				ports := containerPorts(dn)
				for _, l := range apiListeners(dn) {
					assert.Contains(
						t,
						ports,
						corev1.ContainerPort{
							Name:          l.name,
							ContainerPort: l.port,
							Protocol:      corev1.ProtocolTCP,
						},
					)
					for _, svc := range []*corev1.Service{BuildClientService(dn), BuildHeadlessService(dn)} {
						var found bool
						for _, p := range svc.Spec.Ports {
							if p.Name == l.name {
								found = true
								assert.Equal(t, l.port, p.Port)
								assert.Equal(t, l.name, p.TargetPort.StrVal)
							}
						}
						assert.True(t, found, l.name)
					}
					rules := rulesForPort(BuildNetworkPolicy(dn), int(l.port))
					require.Len(t, rules, 1)
					assert.Equal(
						t,
						labelledClientPeers(APIAccessLabel),
						rules[0].From,
					)
					assert.Equal(t, "0.0.0.0", env[l.hostEnv])
				}
				assert.Equal(t, "9090", env["DINGO_UTXORPC_PORT"])
				assert.Equal(
					t,
					"1442",
					env["DINGO_PLUGINS_API_KUPO_CONFIG_PORT"],
				)
				assert.Equal(t, "true", env["DINGO_MIDNIGHT_ENABLED"])
				assert.Equal(t, "true", env["DINGO_MIDNIGHT_SERVER_ENABLED"])
				assert.Empty(
					t,
					rulesForPort(BuildNetworkPolicy(dn), PortNodeToClient),
				)
			},
		)
	}
}

func TestAPIDisabledAndCustomAccess(t *testing.T) {
	dn := apiNode()
	dn.Spec.APIs = &dingov1alpha1.APISpec{
		Kupo: 1442,
		IngressFrom: []networkingv1.NetworkPolicyPeer{
			{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "gateway"},
				},
			},
		},
	}
	require.NoError(t, ValidateAPIs(dn))
	rules := rulesForPort(BuildNetworkPolicy(dn), 1442)
	require.Len(t, rules, 1)
	assert.Equal(t, dn.Spec.APIs.IngressFrom, rules[0].From)
	rules[0].From[0].PodSelector.MatchLabels["app"] = "other"
	assert.Equal(
		t,
		"gateway",
		dn.Spec.APIs.IngressFrom[0].PodSelector.MatchLabels["app"],
	)
	assert.Empty(t, rulesForPort(BuildNetworkPolicy(dn), 9090))
	assert.Equal(t, "0", envMap(dn, RenderOptions{})["DINGO_UTXORPC_PORT"])
	assert.Len(t, BuildClientService(dn).Spec.Ports, 4)
	dn.Spec.APIs = nil
	assert.Len(t, BuildClientService(dn).Spec.Ports, 3)
	assert.NotContains(t, envMap(dn, RenderOptions{}), "DINGO_UTXORPC_PORT")
}

func TestAPIEnvironmentCompatibility(t *testing.T) {
	dn := relayNode()
	dn.Spec.StorageMode = dingov1alpha1.StorageModeAPI
	dn.Spec.Environment = map[string]string{
		"DINGO_BLOCKFROST_PORT":              "3000",
		"DINGO_PLUGINS_API_KUPO_CONFIG_PORT": "1442",
	}
	require.NoError(t, ValidateAPIs(dn))
	assert.Len(t, BuildClientService(dn).Spec.Ports, 5)
	assert.Len(t, rulesForPort(BuildNetworkPolicy(dn), 3000), 1)
	assert.Equal(
		t,
		"3000",
		envMap(dn, RenderOptions{})["DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT"],
	)
}

func TestAPIValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*dingov1alpha1.DingoNode)
		message string
	}{
		{
			"core",
			func(dn *dingov1alpha1.DingoNode) { dn.Spec.StorageMode = dingov1alpha1.StorageModeCore },
			"storageMode=api",
		},
		{
			"duplicate",
			func(dn *dingov1alpha1.DingoNode) { dn.Spec.APIs.Mesh = 9090 },
			"conflicts",
		},
		{
			"relay collision",
			func(dn *dingov1alpha1.DingoNode) { dn.Spec.APIs.Kupo = 3001 },
			"conflicts",
		},
		{
			"invalid port",
			func(dn *dingov1alpha1.DingoNode) { dn.Spec.APIs.Kupo = 65536 },
			"between",
		},
		{"environment mismatch", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"DINGO_UTXORPC_PORT": "9091",
			}
		}, "disagrees"},
		{"plugin mismatch", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"DINGO_PLUGINS_API_KUPO_CONFIG_PORT": "0",
			}
		}, "disagrees"},
		{"malformed", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"DINGO_UTXORPC_PORT": "broken",
			}
		}, "between"},
		{"loopback", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"CARDANO_BIND_ADDR": "127.0.0.1",
			}
		}, "0.0.0.0"},
		{"midnight server", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"DINGO_MIDNIGHT_SERVER_ENABLED": "false",
			}
		}, "must be true"},
		{"storage override", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{
				"DINGO_STORAGE_MODE": "core",
			}
		}, "must be api"},
		{"tls disabled", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.APIs.UTxORPC = 0
			dn.Spec.APIs.UTxORPCTLSSecretRef = "rpc-tls"
		}, "requires"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dn := apiNode()
			tt.mutate(dn)
			require.ErrorContains(t, ValidateAPIs(dn), tt.message)
		})
	}
}

func TestAPITLSProjection(t *testing.T) {
	dn := apiNode()
	dn.Spec.APIs.UTxORPCTLSSecretRef = "rpc-tls"
	require.NoError(t, ValidateAPIs(dn))
	sts := BuildStatefulSet(dn, RenderOptions{})
	var found bool
	for _, vol := range sts.Spec.Template.Spec.Volumes {
		if vol.Name == apiTLSVolume {
			found = true
			require.NotNil(t, vol.Secret)
			assert.Equal(t, "rpc-tls", vol.Secret.SecretName)
			assert.Len(t, vol.Secret.Items, 2)
		}
	}
	require.True(t, found)
	assert.Contains(
		t,
		sts.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      apiTLSVolume,
			MountPath: apiTLSPath,
			ReadOnly:  true,
		},
	)
	assert.Equal(
		t,
		apiTLSPath+"/tls.crt",
		envMap(dn, RenderOptions{})["TLS_CERT_FILE_PATH"],
	)
}

func TestTieredServiceIsolation(t *testing.T) {
	dn := apiNode()
	dn.Spec.RelayService = dingov1alpha1.RelayServiceSpec{
		Type:                     corev1.ServiceTypeLoadBalancer,
		ExternalTrafficPolicy:    corev1.ServiceExternalTrafficPolicyLocal,
		LoadBalancerSourceRanges: []string{"192.0.2.0/24"},
		HostPort:                 3001,
		Annotations:              map[string]string{"example.com/lb": "relay"},
	}
	relay := BuildRelayService(dn)
	require.Len(t, relay.Spec.Ports, 1)
	assert.Equal(t, "relay", relay.Spec.Ports[0].Name)
	assert.Equal(t, corev1.ServiceTypeLoadBalancer, relay.Spec.Type)
	assert.Equal(
		t,
		corev1.ServiceExternalTrafficPolicyLocal,
		relay.Spec.ExternalTrafficPolicy,
	)
	assert.Equal(
		t,
		[]string{"192.0.2.0/24"},
		relay.Spec.LoadBalancerSourceRanges,
	)
	assert.Equal(t, "relay", relay.Annotations["example.com/lb"])
	assert.Equal(t, int32(3001), containerPorts(dn)[0].HostPort)
	private := BuildPrivateService(dn)
	assert.Equal(t, corev1.ServiceTypeClusterIP, private.Spec.Type)
	require.Len(t, private.Spec.Ports, 7)
	for _, p := range private.Spec.Ports {
		assert.NotEqual(t, "relay", p.Name)
		assert.NotEqual(t, "metrics", p.Name)
	}
	metrics := BuildMetricsService(dn)
	assert.Equal(t, corev1.ServiceTypeClusterIP, metrics.Spec.Type)
	require.Len(t, metrics.Spec.Ports, 1)
	assert.Equal(t, "metrics", metrics.Spec.Ports[0].Name)
}
