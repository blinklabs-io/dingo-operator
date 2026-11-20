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
						[]networkingv1.NetworkPolicyPeer{
							{
								PodSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"dingo.blinklabs.io/api": "allowed",
									},
								},
							},
							{
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"dingo.blinklabs.io/api": "allowed",
									},
								},
								PodSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"dingo.blinklabs.io/api": "allowed",
									},
								},
							},
						},
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

func TestAPIIngressPeerValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		peer networkingv1.NetworkPolicyPeer
	}{
		{name: "empty peer"},
		{
			name: "ip block with selectors",
			peer: networkingv1.NetworkPolicyPeer{
				IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"},
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "gateway"},
				},
			},
		},
		{
			name: "invalid CIDR",
			peer: networkingv1.NetworkPolicyPeer{
				IPBlock: &networkingv1.IPBlock{CIDR: "not-a-cidr"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dn := apiNode()
			dn.Spec.APIs.IngressFrom = []networkingv1.NetworkPolicyPeer{tc.peer}
			require.Error(t, ValidateAPIs(dn))
		})
	}
}

func TestRelayIngressPeerValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		peer      networkingv1.NetworkPolicyPeer
		wantError string
	}{
		{name: "empty peer", wantError: "relayService.ingressFrom"},
		{
			name:      "ip block with selectors",
			wantError: "relayService.ingressFrom",
			peer: networkingv1.NetworkPolicyPeer{
				IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"},
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "relay"},
				},
			},
		},
		{
			name:      "invalid CIDR",
			wantError: "relayService.ingressFrom",
			peer: networkingv1.NetworkPolicyPeer{
				IPBlock: &networkingv1.IPBlock{CIDR: "not-a-cidr"},
			},
		},
		{
			name:      "invalid pod selector",
			wantError: ".podSelector",
			peer: networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: "app", Operator: metav1.LabelSelectorOpIn,
				}}},
			},
		},
		{
			name:      "invalid namespace selector",
			wantError: ".namespaceSelector",
			peer: networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: "relay-zone", Operator: metav1.LabelSelectorOpIn,
				}}},
			},
		},
		{
			name:      "unrelated exclusion",
			wantError: "strict subnet",
			peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{
				CIDR: "192.0.2.0/24", Except: []string{"198.51.100.0/24"},
			}},
		},
		{
			name:      "equal exclusion",
			wantError: "strict subnet",
			peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{
				CIDR: "192.0.2.0/24", Except: []string{"192.0.2.0/24"},
			}},
		},
		{
			name: "strict subnet exclusion",
			peer: networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{
				CIDR: "192.0.2.0/24", Except: []string{"192.0.2.128/25"},
			}},
		},
		{
			name: "empty selector",
			peer: networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dn := apiNode()
			dn.Spec.RelayService.IngressFrom =
				[]networkingv1.NetworkPolicyPeer{tc.peer}
			err := ValidateAPIs(dn)
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantError)
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
			assert.Equal(t, []corev1.KeyToPath{
				{Key: "tls.crt", Path: "tls.crt"},
				{Key: "tls.key", Path: "tls.key"},
			}, vol.Secret.Items)
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

// Dingo's Blockfrost (v0.86.0+) and UTxORPC (v0.88.0+) providers default their
// own host to 127.0.0.1 and use the node bind address only when it is empty.
// Earlier releases have no such field and reject the variable as unknown.
var providerHostEnvs = map[string]string{
	"blockfrost": "DINGO_PLUGINS_API_BLOCKFROST_CONFIG_HOST",
	"utxorpc":    "DINGO_PLUGINS_API_UTXORPC_CONFIG_HOST",
}

func TestAPIProviderHostByImageVersion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		repository string
		tag        string
		want       map[string]bool
	}{
		{"default tag", "", "", map[string]bool{}},
		{"0.85.0", "", "0.85.0", map[string]bool{}},
		{"0.86.0", "", "0.86.0", map[string]bool{"blockfrost": true}},
		{"0.87.9", "", "0.87.9", map[string]bool{"blockfrost": true}},
		{
			"0.88.0", "", "0.88.0",
			map[string]bool{"blockfrost": true, "utxorpc": true},
		},
		{"v0.94.2", "", "v0.94.2", map[string]bool{
			"blockfrost": true, "utxorpc": true,
		}},
		{"1.0.0", "", "1.0.0", map[string]bool{
			"blockfrost": true, "utxorpc": true,
		}},
		{"custom repository", "example.com/dingo", "0.50.0", map[string]bool{}},
		{"mirror of old release", "example.com/mirror/dingo", "0.85.0",
			map[string]bool{}},
		{"non-semver tag", "", "latest", map[string]bool{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := apiNode()
			dn.Spec.Image.Repository = tc.repository
			dn.Spec.Image.Tag = tc.tag
			require.NoError(t, ValidateAPIs(dn))
			env := envMap(dn, RenderOptions{})
			for name, key := range providerHostEnvs {
				if tc.want[name] {
					assert.Equal(t, "0.0.0.0", env[key], name)
				} else {
					assert.NotContains(t, env, key, name)
				}
			}
		})
	}
}

func TestAPIProviderHostUserValuePassesThroughOnUnknownVersion(t *testing.T) {
	t.Parallel()
	for name, key := range providerHostEnvs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dn := apiNode()
			dn.Spec.Image.Repository = "example.com/mirror/dingo"
			dn.Spec.Image.Tag = "custom"
			dn.Spec.Environment = map[string]string{key: "0.0.0.0"}
			require.NoError(t, ValidateAPIs(dn))
			assert.Equal(t, "0.0.0.0", envMap(dn, RenderOptions{})[key])
		})
	}
}

func TestAPIProviderHostOmittedWithoutListener(t *testing.T) {
	t.Parallel()
	dn := apiNode()
	dn.Spec.Image.Tag = "0.94.2"
	dn.Spec.APIs.Blockfrost = 0
	dn.Spec.APIs.UTxORPC = 0
	env := envMap(dn, RenderOptions{})
	for name, key := range providerHostEnvs {
		assert.NotContains(t, env, key, name)
	}
}

func TestAPIProviderHostLoopbackRejected(t *testing.T) {
	t.Parallel()
	for name, key := range providerHostEnvs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dn := apiNode()
			dn.Spec.Image.Tag = "0.94.2"
			dn.Spec.Environment = map[string]string{key: "127.0.0.1"}
			require.ErrorContains(t, ValidateAPIs(dn), key)
			dn.Spec.Environment = map[string]string{key: "0.0.0.0"}
			require.NoError(t, ValidateAPIs(dn))
		})
	}
}

// Dingo v0.86.0 through v0.92.3 refuse to start Blockfrost on a non-loopback
// host when an auth token is set without server TLS.
func TestAPIBlockfrostHostWithAuthToken(t *testing.T) {
	t.Parallel()
	const (
		token     = "DINGO_PLUGINS_API_BLOCKFROST_CONFIG_AUTH_TOKEN"
		tokenFile = "DINGO_PLUGINS_API_BLOCKFROST_CONFIG_AUTH_TOKEN_FILE"
		tls       = "DINGO_PLUGINS_API_BLOCKFROST_CONFIG_TLS"
		apiTLS    = "DINGO_API_TLS_MODE"
	)
	cases := []struct {
		name string
		tag  string
		env  map[string]string
		want bool
	}{
		{"0.86.0 token", "0.86.0", map[string]string{token: "t"}, false},
		{"0.92.3 token", "0.92.3", map[string]string{token: "t"}, false},
		{"0.92.3 token file", "0.92.3",
			map[string]string{tokenFile: "/run/token"}, false},
		{"0.92.3 empty token", "0.92.3", map[string]string{token: ""}, true},
		{"0.92.3 no token", "0.92.3", nil, true},
		{"0.92.3 token with API TLS", "0.92.3",
			map[string]string{token: "t", apiTLS: "server"}, true},
		{"0.92.3 token with API TLS disabled", "0.92.3",
			map[string]string{token: "t", apiTLS: "disabled"}, false},
		{"0.92.3 token with provider TLS override", "0.92.3",
			map[string]string{token: "t", apiTLS: "server",
				tls: "{mode: disabled}"}, false},
		{"0.92.3 token with provider TLS", "0.92.3",
			map[string]string{token: "t",
				tls: "{mode: server, certFilePath: /c, keyFilePath: /k}"},
			true},
		{"0.92.3 token with provider TLS inheriting API TLS", "0.92.3",
			map[string]string{token: "t", apiTLS: "server",
				tls: "{certFilePath: /c, keyFilePath: /k}"}, true},
		{"0.92.3 token with malformed provider TLS", "0.92.3",
			map[string]string{token: "t", tls: "{mode: server"}, false},
		{"0.92.3 token with provider TLS file", "0.92.3",
			map[string]string{token: "t", apiTLS: "server",
				tls + "_FILE": "/run/tls"}, false},
		{"0.92.4 token", "0.92.4", map[string]string{token: "t"}, true},
		{"0.94.2 token", "0.94.2", map[string]string{token: "t"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := apiNode()
			dn.Spec.Image.Tag = tc.tag
			dn.Spec.Environment = tc.env
			require.NoError(t, ValidateAPIs(dn))
			env := envMap(dn, RenderOptions{})
			key := providerHostEnvs["blockfrost"]
			if tc.want {
				assert.Equal(t, "0.0.0.0", env[key])
			} else {
				assert.NotContains(t, env, key)
			}
			if tc.tag != "0.86.0" {
				assert.Equal(t, "0.0.0.0", env[providerHostEnvs["utxorpc"]])
			}
		})
	}
}
