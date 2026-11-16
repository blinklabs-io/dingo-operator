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
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// operatorPeer is the metrics peer that admits the operator's own pod from
// its namespace, which need not carry the metrics access label.
func operatorPeer(ns string) networkingv1.NetworkPolicyPeer {
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns},
		},
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{MetricsAccessLabel: "allowed"},
		},
	}
}

// TestMetricsAdmitsOperatorNamespace checks that the operator can scrape a
// node in another namespace when only its pod carries the metrics access
// label: KES and wedge status come from that scrape.
func TestMetricsAdmitsOperatorNamespace(t *testing.T) {
	for _, dn := range []*dingov1alpha1.DingoNode{relayNode(), bpNode()} {
		t.Run(string(dn.Spec.Role), func(t *testing.T) {
			policy := BuildNetworkPolicyFor(
				dn, PolicyOptions{OperatorNamespace: "operators"},
			)
			from := ruleForPort(t, policy, portMetrics).From
			assertKeepsLabelledMetricsPeers(t, from)
			require.Len(t, from, 3)
			assert.Equal(t, operatorPeer("operators"), from[2])
		})
	}
}

func TestMetricsOperatorPeerOrderAndOmission(t *testing.T) {
	dn := relayNode()
	extra := networkingv1.NetworkPolicyPeer{
		IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.0/8"},
	}
	dn.Spec.Metrics.IngressFrom = []networkingv1.NetworkPolicyPeer{extra}

	from := ruleForPort(t, BuildNetworkPolicyFor(
		dn, PolicyOptions{OperatorNamespace: "operators"},
	), portMetrics).From
	require.Len(t, from, 4)
	assert.Equal(t, operatorPeer("operators"), from[2])
	assert.Equal(t, extra, from[3])

	// Unknown namespace: today's peers only.
	from = ruleForPort(t, BuildNetworkPolicyFor(dn, PolicyOptions{}), portMetrics).From
	require.Len(t, from, 3)
	assert.Equal(t, extra, from[2])

	// Same namespace as the node: the pod-only peer already admits it.
	from = ruleForPort(t, BuildNetworkPolicyFor(
		dn, PolicyOptions{OperatorNamespace: dn.Namespace},
	), portMetrics).From
	require.Len(t, from, 3)

	assert.Equal(t,
		BuildNetworkPolicyFor(dn, PolicyOptions{}),
		BuildNetworkPolicy(dn),
	)
}

func podMonitorRelabelings(t *testing.T, dn *dingov1alpha1.DingoNode) []any {
	t.Helper()
	endpoints, _, err := unstructured.NestedSlice(
		BuildPodMonitor(dn).Object, "spec", "podMetricsEndpoints",
	)
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	relabelings, found, err := unstructured.NestedSlice(
		endpoints[0].(map[string]any), "relabelings",
	)
	require.NoError(t, err)
	require.True(t, found)
	return relabelings
}

// TestBuildPodMonitorAddsMetricLabels checks that fixed series labels are
// added by the PodMonitor, so changing them does not restart the pod.
func TestBuildPodMonitorAddsMetricLabels(t *testing.T) {
	dn := relayNode()
	dn.Spec.Metrics.PodMonitor.MetricLabels = map[string]string{
		"group": "core",
		"alias": "r1",
		"price": "$1",
	}
	relabelings := podMonitorRelabelings(t, dn)
	require.Len(t, relabelings, 4)
	assert.Equal(t, map[string]any{
		"sourceLabels": []any{"__meta_kubernetes_pod_name"},
		"targetLabel":  "instance",
	}, relabelings[0])
	assert.Equal(t, []any{
		map[string]any{"targetLabel": "alias", "replacement": "r1"},
		map[string]any{"targetLabel": "group", "replacement": "core"},
		// Prometheus expands $1 in a replacement; $$ is a literal $.
		map[string]any{"targetLabel": "price", "replacement": "$$1"},
	}, relabelings[1:])

	template := BuildStatefulSet(relayNode(), RenderOptions{Replicas: 1}).Spec.Template
	assert.Equal(t,
		template,
		BuildStatefulSet(dn, RenderOptions{Replicas: 1}).Spec.Template,
		"metric labels must not change the pod template",
	)
}

func TestValidateMetricLabels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		labels    map[string]string
		podLabels map[string]string
		wantErr   string
	}{
		{name: "plain", labels: map[string]string{"group": "a", "az_1": "b", "_x": "c"}},
		{name: "starts with digit", labels: map[string]string{"1az": "x"}, wantErr: "1az"},
		{name: "dash", labels: map[string]string{"my-label": "x"}, wantErr: "my-label"},
		{name: "dot", labels: map[string]string{"a.b": "x"}, wantErr: "a.b"},
		{name: "reserved prefix", labels: map[string]string{"__name__": "x"}, wantErr: "__name__"},
		{name: "instance", labels: map[string]string{"instance": "x"}, wantErr: "instance"},
		{name: "job", labels: map[string]string{"job": "x"}, wantErr: "job"},
		{name: "namespace", labels: map[string]string{"namespace": "x"}, wantErr: "namespace"},
		{name: "pod", labels: map[string]string{"pod": "x"}, wantErr: "pod"},
		{name: "container", labels: map[string]string{"container": "x"}, wantErr: "container"},
		{name: "endpoint", labels: map[string]string{"endpoint": "x"}, wantErr: "endpoint"},
		{name: "cardano_network", labels: map[string]string{"cardano_network": "x"}, wantErr: "cardano_network"},
		{name: "cardano_service", labels: map[string]string{"cardano_service": "x"}, wantErr: "cardano_service"},
		{
			name:      "also a pod label",
			labels:    map[string]string{"group": "x"},
			podLabels: map[string]string{"group": "y"},
			wantErr:   "podLabels",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			dn.Spec.PodLabels = tc.podLabels
			dn.Spec.Metrics.PodMonitor.MetricLabels = tc.labels
			err := ValidateMetrics(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, err, "metricLabels")
		})
	}
}
