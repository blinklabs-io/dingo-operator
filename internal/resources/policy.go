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
	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// BuildPodDisruptionBudget keeps at least one node available during voluntary
// disruptions. For a single-active block producer this blocks voluntary evicts
// that would leave zero forgers.
func BuildPodDisruptionBudget(
	dn *dingov1alpha1.DingoNode,
) *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name,
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: SelectorLabels(dn),
			},
		},
	}
}

// BuildNetworkPolicy restricts inbound traffic to a block producer. When relay
// refs are declared, node-to-node ingress is limited to those relays; metrics
// scraping is always allowed from within the namespace. Egress is left open so
// the node can reach DNS, its peers, and Mithril aggregators.
func BuildNetworkPolicy(
	dn *dingov1alpha1.DingoNode,
) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	metricsPort := intstr.FromInt32(portMetrics)

	metricsIngress := networkingv1.NetworkPolicyIngressRule{
		// Metrics scraping is allowed from any namespace: the cluster-scoped
		// operator (which scrapes KES/opcert state to drive rotation) and
		// Prometheus normally run outside the node's namespace, so a
		// same-namespace-only rule would silently break KES monitoring. Only the
		// metrics port is opened this way; the node-to-node ports stay
		// restricted to declared relays below.
		From: []networkingv1.NetworkPolicyPeer{
			{NamespaceSelector: &metav1.LabelSelector{}},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &metricsPort},
		},
	}
	ingress := []networkingv1.NetworkPolicyIngressRule{metricsIngress}
	if refs := dn.Spec.Topology.RelayRefs; len(refs) > 0 {
		relayPort := intstr.FromInt32(portRelay)
		privatePort := intstr.FromInt32(portPrivate)
		peerSelector := metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      NodeLabel,
					Operator: metav1.LabelSelectorOpIn,
					Values:   refs,
				},
			},
		}
		ingress = []networkingv1.NetworkPolicyIngressRule{{
			From: []networkingv1.NetworkPolicyPeer{
				{PodSelector: &peerSelector},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{Protocol: &tcp, Port: &relayPort},
				{Protocol: &tcp, Port: &privatePort},
			},
		}, metricsIngress}
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name,
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: SelectorLabels(dn)},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: ingress,
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{},
			}, // allow all egress
		},
	}
}

// BuildServiceAccount constructs the per-node ServiceAccount referenced by the
// StatefulSet pod template. Automount is disabled: a Dingo node needs no API
// access.
func BuildServiceAccount(dn *dingov1alpha1.DingoNode) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name,
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		AutomountServiceAccountToken: new(false),
	}
}
