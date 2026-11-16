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
	"fmt"
	"net"
	"regexp"
	"slices"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateMetrics rejects PodMonitor labels that would replace the
// operator's identity labels, and metrics peers a NetworkPolicy would reject
// or misread.
func ValidateMetrics(dn *dingov1alpha1.DingoNode) error {
	labels := dn.Spec.Metrics.PodMonitor.Labels
	for _, key := range sortedKeys(labels) {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return fmt.Errorf(
				"metrics.podMonitor.labels key %q is invalid: %s",
				key, strings.Join(errs, "; "),
			)
		}
		if errs := validation.IsValidLabelValue(labels[key]); len(errs) > 0 {
			return fmt.Errorf(
				"metrics.podMonitor.labels[%q] value is invalid: %s",
				key, strings.Join(errs, "; "),
			)
		}
		if podMonitorLabelReserved(key) {
			return fmt.Errorf(
				"metrics.podMonitor.labels key %q is reserved for the operator",
				key,
			)
		}
	}
	if err := validateMetricLabels(dn); err != nil {
		return err
	}
	return validateIngressPeers("metrics.ingressFrom", dn.Spec.Metrics.IngressFrom)
}

// reservedMetricLabels are the series labels Prometheus or the PodMonitor
// already set; a fixed value would overwrite them on every series.
var reservedMetricLabels = []string{
	"instance", "job", "namespace", "pod", "container", "endpoint",
	"cardano_network", "cardano_service",
}

// validateMetricLabels rejects metricLabels keys that are not Prometheus
// label names, that Prometheus reserves (the __ prefix), that would
// overwrite a target label, or that podLabels already copies onto the series
// (two sources for one label would fight).
func validateMetricLabels(dn *dingov1alpha1.DingoNode) error {
	for _, key := range sortedKeys(dn.Spec.Metrics.PodMonitor.MetricLabels) {
		if !prometheusLabelName.MatchString(key) {
			return fmt.Errorf(
				"metrics.podMonitor.metricLabels key %q is not a Prometheus "+
					"label name ([a-zA-Z_][a-zA-Z0-9_]*)",
				key,
			)
		}
		if strings.HasPrefix(key, "__") {
			return fmt.Errorf(
				"metrics.podMonitor.metricLabels key %q: the __ prefix is "+
					"reserved by Prometheus",
				key,
			)
		}
		if slices.Contains(reservedMetricLabels, key) {
			return fmt.Errorf(
				"metrics.podMonitor.metricLabels key %q would overwrite a "+
					"target label",
				key,
			)
		}
		if _, ok := dn.Spec.PodLabels[key]; ok {
			return fmt.Errorf(
				"metrics.podMonitor.metricLabels key %q is also in podLabels, "+
					"which already copies it onto the series",
				key,
			)
		}
	}
	return nil
}

var prometheusLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func podMonitorLabelReserved(key string) bool {
	switch key {
	case "app.kubernetes.io/name",
		"app.kubernetes.io/instance",
		"app.kubernetes.io/managed-by":
		return true
	}
	return strings.HasPrefix(key, "dingo.blinklabs.io/")
}

// metricsPeers returns who may scrape the metrics port: labelled clients, the
// operator's labelled pod in its own namespace, then the spec's peers. The
// labelled grant always stays, because the operator carries that label and
// scrapes every node for KES and wedge status; spec peers replacing it would
// blind the operator. The operator-namespace peer lets that scrape through
// when the operator's namespace does not carry the label; in the node's own
// namespace the pod-only peer already admits it. The spec's peers are copied
// so the policy never aliases the DingoNode it was built from.
func metricsPeers(
	dn *dingov1alpha1.DingoNode,
	operatorNamespace string,
) []networkingv1.NetworkPolicyPeer {
	peers := labelledClientPeers(MetricsAccessLabel)
	if operatorNamespace != "" && operatorNamespace != dn.Namespace {
		peers = append(peers, networkingv1.NetworkPolicyPeer{
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					corev1.LabelMetadataName: operatorNamespace,
				},
			},
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					MetricsAccessLabel: NodeToClientAccessAllowed,
				},
			},
		})
	}
	for i := range dn.Spec.Metrics.IngressFrom {
		var peer networkingv1.NetworkPolicyPeer
		dn.Spec.Metrics.IngressFrom[i].DeepCopyInto(&peer)
		peers = append(peers, peer)
	}
	return peers
}

// validateIngressPeers rejects NetworkPolicy peers the API server would refuse
// (ipBlock with a selector, an invalid selector, bad CIDRs, an except that is
// not a strict subset of its cidr) or read as "everyone" (an empty peer),
// naming the field so the error points at the spec. A peer the API server
// refuses fails the whole NetworkPolicy apply, and the reconcile stops there.
func validateIngressPeers(
	prefix string,
	peers []networkingv1.NetworkPolicyPeer,
) error {
	for i, peer := range peers {
		selectors := peer.NamespaceSelector != nil || peer.PodSelector != nil
		if peer.IPBlock != nil && selectors {
			return fmt.Errorf(
				"%s[%d] cannot combine ipBlock with selectors",
				prefix, i,
			)
		}
		if peer.IPBlock == nil && !selectors {
			return fmt.Errorf(
				"%s[%d] must set ipBlock or a selector",
				prefix, i,
			)
		}
		path := field.NewPath(fmt.Sprintf("%s[%d]", prefix, i))
		for _, sel := range []struct {
			name     string
			selector *metav1.LabelSelector
		}{
			{"podSelector", peer.PodSelector},
			{"namespaceSelector", peer.NamespaceSelector},
		} {
			errs := metav1validation.ValidateLabelSelector(
				sel.selector,
				metav1validation.LabelSelectorValidationOptions{},
				path.Child(sel.name),
			)
			if len(errs) > 0 {
				return errs.ToAggregate()
			}
		}
		if peer.IPBlock == nil {
			continue
		}
		_, cidr, err := net.ParseCIDR(peer.IPBlock.CIDR)
		if err != nil {
			return fmt.Errorf(
				"%s[%d].ipBlock.cidr is invalid: %w",
				prefix, i, err,
			)
		}
		cidrBits, _ := cidr.Mask.Size()
		for j, excluded := range peer.IPBlock.Except {
			_, except, err := net.ParseCIDR(excluded)
			if err != nil {
				return fmt.Errorf(
					"%s[%d].ipBlock.except[%d] is invalid: %w",
					prefix, i, j, err,
				)
			}
			exceptBits, _ := except.Mask.Size()
			if !cidr.Contains(except.IP) || exceptBits <= cidrBits {
				return fmt.Errorf(
					"%s[%d].ipBlock.except[%d] must be a strict subset of cidr %s",
					prefix, i, j, peer.IPBlock.CIDR,
				)
			}
		}
	}
	return nil
}
