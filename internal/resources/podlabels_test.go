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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPodLabelsApplyToPodTemplateOnly(t *testing.T) {
	t.Parallel()
	dn := bpNode()
	dn.Spec.PodLabels = map[string]string{
		"alias": "musashi",
		"azs":   "us-east-1a",
		"group": "relays",
	}
	sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})
	tmpl := sts.Spec.Template.Labels
	for k, v := range dn.Spec.PodLabels {
		assert.Equal(t, v, tmpl[k], k)
	}
	// Operator labels are untouched.
	for k, v := range Labels(dn) {
		assert.Equal(t, v, tmpl[k], k)
	}
	// Selectors, StatefulSet metadata and Services do not carry them.
	assert.Equal(t, SelectorLabels(dn), sts.Spec.Selector.MatchLabels)
	assert.NotContains(t, sts.Labels, "alias")
	assert.NotContains(t, BuildClientService(dn).Labels, "alias")
	assert.NotContains(t, BuildClientService(dn).Spec.Selector, "alias")
}

func TestPodLabelsUnsetLeavesTemplateUnchanged(t *testing.T) {
	t.Parallel()
	dn := relayNode()
	sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})
	assert.Equal(t, Labels(dn), sts.Spec.Template.Labels)
}

func TestPodLabelsCannotOverrideOperatorLabels(t *testing.T) {
	t.Parallel()
	dn := relayNode()
	// Validation rejects these; the builder must still not let them win.
	dn.Spec.PodLabels = map[string]string{NodeLabel: "other", "cardano_network": "x"}
	got := BuildStatefulSet(dn, RenderOptions{Replicas: 1}).Spec.Template.Labels
	assert.Equal(t, "relay", got[NodeLabel])
	assert.Equal(t, "mainnet", got["cardano_network"])
}

func TestValidatePodLabels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		labels  map[string]string
		wantErr string
	}{
		{"none", nil, ""},
		{"chart style", map[string]string{"alias": "musashi", "network": "mainnet", "group": "a"}, ""},
		{"prefixed foreign key", map[string]string{"example.com/team": "ops"}, ""},
		{"empty value", map[string]string{"alias": ""}, ""},
		{"app.kubernetes.io/name", map[string]string{"app.kubernetes.io/name": "x"}, "app.kubernetes.io/name"},
		{"app.kubernetes.io/part-of", map[string]string{"app.kubernetes.io/part-of": "x"}, "app.kubernetes.io/part-of"},
		{"dingo.blinklabs.io/node", map[string]string{NodeLabel: "x"}, NodeLabel},
		{"dingo.blinklabs.io/metrics", map[string]string{MetricsAccessLabel: "allowed"}, MetricsAccessLabel},
		{"cardano_network", map[string]string{"cardano_network": "x"}, "cardano_network"},
		{"cardano_service", map[string]string{"cardano_service": "x"}, "cardano_service"},
		{"invalid key", map[string]string{"bad key": "x"}, "bad key"},
		{"invalid value", map[string]string{"alias": "not valid!"}, "alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			dn.Spec.PodLabels = tc.labels
			err := ValidatePodLabels(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, err, "podLabels")
		})
	}
}
