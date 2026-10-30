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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestBuildPodMonitorKeepsPodLabels checks that scraped series carry the pod's
// cardano_network and cardano_service labels, and that instance is the pod
// name rather than its address.
func TestBuildPodMonitorKeepsPodLabels(t *testing.T) {
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
	pm := BuildPodMonitor(dn)

	targetLabels, found, err := unstructured.NestedStringSlice(
		pm.Object, "spec", "podTargetLabels",
	)
	require.NoError(t, err)
	require.True(t, found, "podTargetLabels not set")
	assert.ElementsMatch(
		t,
		[]string{"cardano_network", "cardano_service"},
		targetLabels,
	)

	endpoints, _, err := unstructured.NestedSlice(
		pm.Object, "spec", "podMetricsEndpoints",
	)
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	relabelings, found, err := unstructured.NestedSlice(
		endpoints[0].(map[string]any), "relabelings",
	)
	require.NoError(t, err)
	require.True(t, found, "relabelings not set")
	assert.Contains(t, relabelings, map[string]any{
		"sourceLabels": []any{"__meta_kubernetes_pod_name"},
		"targetLabel":  "instance",
	})
}

// TestBuildPodMonitorKeepsSpecPodLabels checks that labels added through
// spec.podLabels are copied onto scraped series too, in a stable order.
func TestBuildPodMonitorKeepsSpecPodLabels(t *testing.T) {
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:      dingov1alpha1.RoleRelay,
			Network:   "preview",
			PodLabels: map[string]string{"group": "a", "alias": "r1"},
		},
	}
	targetLabels, _, err := unstructured.NestedStringSlice(
		BuildPodMonitor(dn).Object, "spec", "podTargetLabels",
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"cardano_network", "cardano_service", "alias", "group"},
		targetLabels,
	)
}
