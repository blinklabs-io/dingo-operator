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

// NodeToClientAccessLabel opts a client into a managed block producer's
// node-to-client port. It must be set on the client's pod, and — when the
// client runs in another namespace — on that namespace as well.
//
// This label is the *only* way in. Nothing carries it by default, so the
// default posture is unchanged: NtC stays closed. It is named for what it
// grants because granting it is not a formality. Node-to-client is a far
// heavier interface than a metrics scrape — arbitrary ledger-state queries,
// transaction submission and mempool inspection against a forging node — so it
// is opt-in per client rather than allowed namespace-wide the way metrics
// scraping is, and it is not implied by any other relationship to the node.
const NodeToClientAccessLabel = "dingo.blinklabs.io/node-to-client"

// NodeToClientAccessAllowed is the only value NodeToClientAccessLabel honours.
// An explicit value (rather than mere presence) keeps the grant readable in
// `kubectl get pods --show-labels` and leaves room for future values.
const NodeToClientAccessAllowed = "allowed"

// BuildNetworkPolicy restricts inbound traffic to a block producer. When relay
// refs are declared, node-to-node ingress (port 3001 only) is limited to those
// relays; the node-to-client port is admitted only for clients carrying
// NodeToClientAccessLabel, and only when the node actually serves NtC; metrics
// scraping is allowed from any namespace (the operator and Prometheus normally
// run outside the node's own namespace). Egress is left open so the node can
// reach DNS, peers, and Mithril aggregators.
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
		// metrics port is opened this way; the node-to-node port stays
		// restricted to declared relays below, and node-to-client to labelled
		// clients.
		From: []networkingv1.NetworkPolicyPeer{
			{NamespaceSelector: &metav1.LabelSelector{}},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &metricsPort},
		},
	}

	var ingress []networkingv1.NetworkPolicyIngressRule
	if refs := dn.Spec.Topology.RelayRefs; len(refs) > 0 {
		// Relay peers get the node-to-node port and nothing else. They used to
		// be handed the node-to-client port in the same rule, which was inert
		// only because Dingo binds NtC to loopback: enabling the listener would
		// have let any pod carrying this node's NodeLabel value open
		// local-state-query, tx submission and mempool queries against a forging
		// node with no NtC grant at all. Peering is a node-to-node
		// relationship; it is not consent to drive the node as a client.
		relayPort := intstr.FromInt32(portRelay)
		peerSelector := metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      NodeLabel,
					Operator: metav1.LabelSelectorOpIn,
					Values:   refs,
				},
			},
		}
		ingress = append(ingress, networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{
				{PodSelector: &peerSelector},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{Protocol: &tcp, Port: &relayPort},
			},
		})
	}
	// Only open the port the node is actually serving. Emitting this rule
	// unconditionally would let a hand-set CARDANO_PRIVATE_BIND_ADDR expose NtC
	// to labelled peers while the operator, which keys off the spec field, never
	// queries it — policy and listener disagreeing in the direction that grants
	// access.
	if bp := dn.Spec.BlockProducer; bp != nil && bp.NodeToClient.Enabled {
		ingress = append(ingress, nodeToClientIngress(tcp))
	}
	ingress = append(ingress, metricsIngress)

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

// nodeToClientIngress allows the node-to-client port from clients that have
// explicitly opted in with NodeToClientAccessLabel, and is the only rule that
// opens that port. With no such client in the cluster it matches nothing, so it
// does not widen the default posture.
//
// Two peers, deliberately asymmetric. Within the node's own namespace the pod
// label alone is enough: whoever can label a pod there is already inside the
// blast radius. From any other namespace both the pod *and* its namespace must
// carry the label — the two selectors in a single peer are ANDed — so a tenant
// cannot reach a block producer in someone else's namespace just by labelling
// their own pod. A cluster-scoped controller (this operator, dingoctl, an SPO's
// own tooling) is granted access by labelling its pod and its namespace, with
// no operator-specific exception in this policy.
func nodeToClientIngress(
	tcp corev1.Protocol,
) networkingv1.NetworkPolicyIngressRule {
	port := intstr.FromInt32(PortNodeToClient)
	// A fresh selector per field: sharing one pointer across three selector
	// fields would make any later mutation of one silently change the others.
	allowed := func() *metav1.LabelSelector {
		return &metav1.LabelSelector{
			MatchLabels: map[string]string{
				NodeToClientAccessLabel: NodeToClientAccessAllowed,
			},
		}
	}
	return networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{
			{PodSelector: allowed()},
			{
				NamespaceSelector: allowed(),
				PodSelector:       allowed(),
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &port},
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
