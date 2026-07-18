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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// PodMonitorGVK is the GroupVersionKind of the Prometheus Operator PodMonitor.
var PodMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "PodMonitor",
}

// BuildPodMonitor constructs a Prometheus Operator PodMonitor as an
// unstructured object. It is built unstructured so the operator does not carry
// a hard dependency on the prometheus-operator API module and can skip creation
// gracefully when the CRD is absent.
func BuildPodMonitor(dn *dingov1alpha1.DingoNode) *unstructured.Unstructured {
	interval := dn.Spec.Metrics.PodMonitor.Interval
	if interval == "" {
		interval = "30s"
	}
	selector := toInterfaceMap(SelectorLabels(dn))

	pm := &unstructured.Unstructured{}
	pm.SetGroupVersionKind(PodMonitorGVK)
	pm.SetName(dn.Name)
	pm.SetNamespace(dn.Namespace)
	pm.SetLabels(Labels(dn))
	pm.Object["spec"] = map[string]any{
		"selector": map[string]any{
			"matchLabels": selector,
		},
		"namespaceSelector": map[string]any{
			"matchNames": []any{dn.Namespace},
		},
		// Required for Prometheus >= v3.0.0 with Dingo's text exposition format.
		"fallbackScrapeProtocol": "PrometheusText0.0.4",
		"podMetricsEndpoints": []any{
			map[string]any{
				"port":     "metrics",
				"path":     metricsPath,
				"interval": interval,
			},
		},
	}
	return pm
}

func toInterfaceMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
