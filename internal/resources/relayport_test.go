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
	"k8s.io/apimachinery/pkg/util/intstr"
)

const chartRelayPort = 3010

func relayPortContainerPort(
	t *testing.T,
	dn *dingov1alpha1.DingoNode,
) corev1.ContainerPort {
	t.Helper()
	sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})
	for _, p := range sts.Spec.Template.Spec.Containers[0].Ports {
		if p.Name == "relay" {
			return p
		}
	}
	require.Fail(t, "relay container port not found")
	return corev1.ContainerPort{}
}

func TestRelayPortDefaultsTo3001(t *testing.T) {
	t.Parallel()
	dn := relayNode()
	assert.Equal(t, int32(3001), RelayPort(dn))
	assert.Equal(t, int32(3001), relayPortContainerPort(t, dn).ContainerPort)
	// The default must not add an env var to existing pod templates.
	assert.NotContains(t, envMap(dn, RenderOptions{}), "CARDANO_PORT")
	for _, svc := range []*corev1.Service{
		BuildHeadlessService(dn), BuildClientService(dn),
		BuildRelayService(dn),
	} {
		assert.Equal(t, int32(3001), svc.Spec.Ports[0].Port, svc.Name)
	}
}

func TestRelayPortCustom(t *testing.T) {
	t.Parallel()
	dn := relayNode()
	dn.Spec.RelayService.Port = chartRelayPort
	dn.Spec.RelayService.HostPort = 31000

	assert.Equal(t, int32(chartRelayPort), RelayPort(dn))
	cp := relayPortContainerPort(t, dn)
	assert.Equal(t, int32(chartRelayPort), cp.ContainerPort)
	assert.Equal(t, int32(31000), cp.HostPort)
	assert.Equal(t, "3010", envMap(dn, RenderOptions{})["CARDANO_PORT"])

	for _, svc := range []*corev1.Service{
		BuildHeadlessService(dn), BuildClientService(dn),
		BuildRelayService(dn),
	} {
		assert.Equal(t, "relay", svc.Spec.Ports[0].Name, svc.Name)
		assert.Equal(t, int32(chartRelayPort), svc.Spec.Ports[0].Port, svc.Name)
		assert.Equal(
			t,
			intstr.FromString("relay"),
			svc.Spec.Ports[0].TargetPort,
			svc.Name,
		)
	}
	// Private and metrics Services keep their fixed ports.
	assert.Equal(
		t,
		int32(PortNodeToClient),
		BuildPrivateService(dn).Spec.Ports[0].Port,
	)
	assert.Equal(
		t,
		int32(portMetrics),
		BuildMetricsService(dn).Spec.Ports[0].Port,
	)

	// Tcp readiness probe (images without the health endpoint) follows it.
	legacy := relayNode()
	legacy.Spec.RelayService.Port = chartRelayPort
	legacy.Spec.Image.Tag = "0.70.0"
	sts := BuildStatefulSet(legacy, RenderOptions{Replicas: 1})
	probe := sts.Spec.Template.Spec.Containers[0].ReadinessProbe
	require.NotNil(t, probe.TCPSocket)
	assert.Equal(t, int32(chartRelayPort), probe.TCPSocket.Port.IntVal)
}

func TestRelayPortNetworkPolicy(t *testing.T) {
	t.Parallel()
	relay := relayNode()
	relay.Spec.RelayService.Port = chartRelayPort
	policy := BuildNetworkPolicy(relay)
	assert.Len(t, rulesForPort(policy, chartRelayPort), 1)
	assert.Empty(t, rulesForPort(policy, 3001))

	bp := bpNode()
	bp.Spec.RelayService.Port = chartRelayPort
	bp.Spec.Topology.RelayRefs = []string{"relay"}
	policy = BuildNetworkPolicy(bp)
	assert.Len(t, rulesForPort(policy, chartRelayPort), 1)
	assert.Empty(t, rulesForPort(policy, 3001))
}

func TestValidateRelayPort(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		mutate  func(dn *dingov1alpha1.DingoNode)
		wantErr string
	}{
		{"default", func(*dingov1alpha1.DingoNode) {}, ""},
		{"custom", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = chartRelayPort
		}, ""},
		{"env matches custom port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = chartRelayPort
			dn.Spec.Environment = map[string]string{"CARDANO_PORT": "3010"}
		}, ""},
		{"env restates default", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{"CARDANO_PORT": "3001"}
		}, ""},
		{"env disagrees with custom port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = chartRelayPort
			dn.Spec.Environment = map[string]string{"CARDANO_PORT": "3001"}
		}, "CARDANO_PORT"},
		{"env moves default port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.Environment = map[string]string{"CARDANO_PORT": "3010"}
		}, "CARDANO_PORT"},
		{"private port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = PortNodeToClient
		}, "relayService.port"},
		{"metrics port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = portMetrics
		}, "relayService.port"},
		{"health port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = portHealth
		}, "relayService.port"},
		{"negative", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = -1
		}, "relayService.port"},
		{"too large", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = 65536
		}, "relayService.port"},
		{"api port collides with custom relay port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = chartRelayPort
			dn.Spec.StorageMode = dingov1alpha1.StorageModeAPI
			dn.Spec.APIs = &dingov1alpha1.APISpec{Blockfrost: chartRelayPort}
		}, "conflicts with relay"},
		{"api may use the freed default relay port", func(dn *dingov1alpha1.DingoNode) {
			dn.Spec.RelayService.Port = chartRelayPort
			dn.Spec.StorageMode = dingov1alpha1.StorageModeAPI
			dn.Spec.APIs = &dingov1alpha1.APISpec{Blockfrost: 3001}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			tc.mutate(dn)
			err := ValidateAPIs(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestTemplateRelayPort(t *testing.T) {
	t.Parallel()
	node := func(c corev1.Container) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{c}},
		}
	}
	tests := []struct {
		name string
		tmpl corev1.PodTemplateSpec
		port int32
		ok   bool
	}{
		{
			name: "named container port",
			tmpl: node(corev1.Container{
				Name: containerName,
				Ports: []corev1.ContainerPort{
					{Name: "metrics", ContainerPort: 12798},
					{Name: "relay", ContainerPort: 4001},
				},
			}),
			port: 4001, ok: true,
		},
		{
			name: "CARDANO_PORT without a named port",
			tmpl: node(corev1.Container{
				Name: containerName,
				Env:  []corev1.EnvVar{{Name: "CARDANO_PORT", Value: "4002"}},
			}),
			port: 4002, ok: true,
		},
		{
			name: "default when neither is set",
			tmpl: node(corev1.Container{Name: containerName}),
			port: portRelay, ok: true,
		},
		{
			name: "no node container",
			tmpl: node(corev1.Container{Name: "sidecar"}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			port, ok := TemplateRelayPort(tt.tmpl)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.port, port)
		})
	}
}
