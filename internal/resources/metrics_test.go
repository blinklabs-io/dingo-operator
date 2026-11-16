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
)

// TestBuildPodMonitorAddsSpecLabels checks that a Prometheus selecting
// PodMonitors by its own labels can pick up a DingoNode's PodMonitor, and that
// the operator's identity labels survive.
func TestBuildPodMonitorAddsSpecLabels(t *testing.T) {
	dn := relayNode()
	dn.Spec.Metrics.PodMonitor.Labels = map[string]string{
		"app.kubernetes.io/component": "metrics",
		"app.kubernetes.io/part-of":   "observability",
	}
	got := BuildPodMonitor(dn).GetLabels()

	assert.Equal(t, "metrics", got["app.kubernetes.io/component"])
	assert.Equal(t, "observability", got["app.kubernetes.io/part-of"])
	assert.Equal(t, ManagedByName, got["app.kubernetes.io/managed-by"])
	for k, v := range SelectorLabels(dn) {
		assert.Equal(t, v, got[k], k)
	}
	assert.Equal(t, "mainnet", got["cardano_network"])
}

// TestBuildPodMonitorSkipsReservedLabels checks that the builder keeps the
// operator's identity labels even for a spec that bypassed validation.
func TestBuildPodMonitorSkipsReservedLabels(t *testing.T) {
	dn := relayNode()
	dn.Spec.Metrics.PodMonitor.Labels = map[string]string{
		"app.kubernetes.io/name":       "x",
		"app.kubernetes.io/instance":   "x",
		"app.kubernetes.io/managed-by": "x",
		NodeLabel:                      "x",
		"dingo.blinklabs.io/other":     "x",
	}
	assert.Equal(t, Labels(dn), BuildPodMonitor(dn).GetLabels())
}

// TestBuildPodMonitorDefaultLabels checks that without spec labels the
// PodMonitor keeps the operator's labels, with the node role as component.
func TestBuildPodMonitorDefaultLabels(t *testing.T) {
	dn := relayNode()
	assert.Equal(t, Labels(dn), BuildPodMonitor(dn).GetLabels())
}

func TestMetricsCustomAccess(t *testing.T) {
	dn := relayNode()
	dn.Spec.Metrics.IngressFrom = []networkingv1.NetworkPolicyPeer{
		{
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"kubernetes.io/metadata.name": "monitoring",
				},
			},
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": "prometheus",
				},
			},
		},
	}
	require.NoError(t, ValidateMetrics(dn))

	policy := BuildNetworkPolicy(dn)
	metrics := ruleForPort(t, policy, portMetrics)
	assertKeepsLabelledMetricsPeers(t, metrics.From)
	assert.Equal(t, dn.Spec.Metrics.IngressFrom, metrics.From[2:])
	relayRules := rulesForPort(policy, portRelay)
	require.Len(t, relayRules, 1)
	assert.Empty(t, relayRules[0].From, "relay stays open to peers")

	metrics.From[2].PodSelector.MatchLabels["app.kubernetes.io/name"] = "x"
	assert.Equal(
		t,
		"prometheus",
		dn.Spec.Metrics.IngressFrom[0].PodSelector.MatchLabels["app.kubernetes.io/name"],
		"the policy must not alias the spec",
	)
}

func TestMetricsCustomAccessBlockProducer(t *testing.T) {
	dn := bpNode()
	dn.Spec.Metrics.IngressFrom = []networkingv1.NetworkPolicyPeer{
		{IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.0/8"}},
	}
	require.NoError(t, ValidateMetrics(dn))
	policy := BuildNetworkPolicy(dn)
	require.Len(t, policy.Spec.Ingress, 1)
	from := ruleForPort(t, policy, portMetrics).From
	assertKeepsLabelledMetricsPeers(t, from)
	assert.Equal(t, dn.Spec.Metrics.IngressFrom, from[2:])
}

// assertKeepsLabelledMetricsPeers checks that extra metrics peers never take
// away the labelled-client grant: the operator itself carries the metrics
// access label and scrapes every node for KES and wedge status.
func assertKeepsLabelledMetricsPeers(
	t *testing.T,
	from []networkingv1.NetworkPolicyPeer,
) {
	t.Helper()
	require.GreaterOrEqual(t, len(from), 2)
	want := map[string]string{MetricsAccessLabel: "allowed"}
	require.NotNil(t, from[0].PodSelector)
	assert.Equal(t, want, from[0].PodSelector.MatchLabels)
	assert.Nil(t, from[0].NamespaceSelector)
	require.NotNil(t, from[1].PodSelector)
	require.NotNil(t, from[1].NamespaceSelector)
	assert.Equal(t, want, from[1].PodSelector.MatchLabels)
	assert.Equal(t, want, from[1].NamespaceSelector.MatchLabels)
}

func TestValidateMetrics(t *testing.T) {
	t.Parallel()
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "prometheus"},
	}
	for _, tc := range []struct {
		name    string
		labels  map[string]string
		peers   []networkingv1.NetworkPolicyPeer
		wantErr string
	}{
		{name: "none"},
		{
			name: "component and part-of",
			labels: map[string]string{
				"app.kubernetes.io/component": "metrics",
				"app.kubernetes.io/part-of":   "observability",
				"release":                     "prom",
			},
		},
		{
			name:    "name is the operator's",
			labels:  map[string]string{"app.kubernetes.io/name": "x"},
			wantErr: "app.kubernetes.io/name",
		},
		{
			name:    "instance is the operator's",
			labels:  map[string]string{"app.kubernetes.io/instance": "x"},
			wantErr: "app.kubernetes.io/instance",
		},
		{
			name:    "managed-by is the operator's",
			labels:  map[string]string{"app.kubernetes.io/managed-by": "x"},
			wantErr: "app.kubernetes.io/managed-by",
		},
		{
			name:    "dingo.blinklabs.io prefix is the operator's",
			labels:  map[string]string{NodeLabel: "x"},
			wantErr: NodeLabel,
		},
		{
			name:    "invalid key",
			labels:  map[string]string{"bad key": "x"},
			wantErr: "bad key",
		},
		{
			name:    "invalid value",
			labels:  map[string]string{"release": "not valid!"},
			wantErr: "release",
		},
		{
			name:  "selector peer",
			peers: []networkingv1.NetworkPolicyPeer{{PodSelector: selector}},
		},
		{
			name: "ipBlock peer",
			peers: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{
					CIDR:   "10.0.0.0/8",
					Except: []string{"10.1.0.0/16"},
				},
			}},
		},
		{
			name:    "empty peer",
			peers:   []networkingv1.NetworkPolicyPeer{{}},
			wantErr: "metrics.ingressFrom[0] must set ipBlock or a selector",
		},
		{
			name: "ipBlock with selector",
			peers: []networkingv1.NetworkPolicyPeer{{
				PodSelector: selector,
				IPBlock:     &networkingv1.IPBlock{CIDR: "10.0.0.0/8"},
			}},
			wantErr: "metrics.ingressFrom[0] cannot combine ipBlock with selectors",
		},
		{
			name: "bad cidr",
			peers: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.0"},
			}},
			wantErr: "metrics.ingressFrom[0].ipBlock.cidr is invalid",
		},
		{
			name: "bad except",
			peers: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{
					CIDR:   "10.0.0.0/8",
					Except: []string{"nope"},
				},
			}},
			wantErr: "metrics.ingressFrom[0].ipBlock.except[0] is invalid",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			dn.Spec.Metrics = dingov1alpha1.MetricsSpec{
				PodMonitor:  dingov1alpha1.PodMonitorSpec{Labels: tc.labels},
				IngressFrom: tc.peers,
			}
			err := ValidateMetrics(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
