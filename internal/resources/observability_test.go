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
	"slices"
	"strings"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const (
	testPoolHex    = "0123456789abcdef0123456789abcdef0123456789abcdef01234567"
	testPoolBech32 = "pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
)

func envVar(t *testing.T, env []corev1.EnvVar, name string) corev1.EnvVar {
	t.Helper()
	i := slices.IndexFunc(env, func(e corev1.EnvVar) bool {
		return e.Name == name
	})
	require.GreaterOrEqual(t, i, 0, "%s not rendered", name)
	return env[i]
}

func TestTracingEnvIncludesAlwaysSamplePools(t *testing.T) {
	dn := relayNode()
	dn.Spec.Tracing = dingov1alpha1.TracingSpec{
		Enabled:           true,
		AlwaysSamplePools: []string{testPoolHex, testPoolBech32},
	}
	env := envMap(dn, RenderOptions{})
	assert.Equal(
		t,
		testPoolHex+","+testPoolBech32,
		env["DINGO_TRACING_ALWAYS_SAMPLE_POOLS"],
	)

	dn.Spec.Tracing.AlwaysSamplePools = nil
	assert.NotContains(
		t,
		envMap(dn, RenderOptions{}),
		"DINGO_TRACING_ALWAYS_SAMPLE_POOLS",
	)
}

func TestOTLPEnvRendersExportersAndHeadersSecret(t *testing.T) {
	dn := relayNode()
	dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{
		Endpoint: "http://otel-collector.observability:4317",
		Protocol: dingov1alpha1.OTLPProtocol("grpc"),
		Logs:     true,
		Metrics:  true,
		HeadersSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "otlp"},
			Key:                  "headers",
		},
	}
	env := BuildEnv(dn, RenderOptions{})
	m := envMap(dn, RenderOptions{})
	assert.Equal(t, "otlp", m["OTEL_LOGS_EXPORTER"])
	assert.Equal(t, "otlp", m["OTEL_METRICS_EXPORTER"])
	assert.Equal(t, "http://otel-collector.observability:4317",
		m["OTEL_EXPORTER_OTLP_ENDPOINT"])
	assert.Equal(t, "grpc", m["OTEL_EXPORTER_OTLP_PROTOCOL"])

	headers := envVar(t, env, "OTEL_EXPORTER_OTLP_HEADERS")
	assert.Empty(t, headers.Value)
	require.NotNil(t, headers.ValueFrom)
	ref := headers.ValueFrom.SecretKeyRef
	require.NotNil(t, ref)
	assert.Equal(t, "otlp", ref.Name)
	assert.Equal(t, "headers", ref.Key)
	require.NotNil(
		t,
		ref.Optional,
		"a missing headers Secret must stop the pod",
	)
	assert.False(t, *ref.Optional)
	assert.NotSame(t, dn.Spec.OTLP.HeadersSecretRef, ref)

	dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{Logs: true}
	m = envMap(dn, RenderOptions{})
	assert.Equal(t, "otlp", m["OTEL_LOGS_EXPORTER"])
	for _, name := range []string{
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_HEADERS",
	} {
		assert.NotContains(t, m, name)
	}
}

func TestResourceAttributesFromDownwardAPI(t *testing.T) {
	dn := relayNode()
	dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{
		Metrics: true,
		ResourceAttributes: map[string]string{
			"team":                   "a b",
			"deployment.environment": "production",
			"cost":                   "$(HOME)",
		},
	}
	env := BuildEnv(dn, RenderOptions{})

	for name, path := range map[string]string{
		"OTEL_K8S_POD_NAME":       "metadata.name",
		"OTEL_K8S_NAMESPACE_NAME": "metadata.namespace",
		"OTEL_K8S_NODE_NAME":      "spec.nodeName",
	} {
		v := envVar(t, env, name)
		require.NotNil(t, v.ValueFrom, name)
		require.NotNil(t, v.ValueFrom.FieldRef, name)
		assert.Equal(t, path, v.ValueFrom.FieldRef.FieldPath, name)
	}

	attrs := envVar(t, env, "OTEL_RESOURCE_ATTRIBUTES")
	assert.Equal(
		t,
		"k8s.pod.name=$(OTEL_K8S_POD_NAME),"+
			"k8s.namespace.name=$(OTEL_K8S_NAMESPACE_NAME),"+
			"k8s.node.name=$(OTEL_K8S_NODE_NAME),"+
			"cardano.network.name=mainnet,"+
			"cost=$$%28HOME%29,"+
			"deployment.environment=production,"+
			"team=a%20b",
		attrs.Value,
	)

	// Kubernetes expands $(VAR) only from variables defined earlier in the
	// list, so the downward-API variables must come first.
	index := func(name string) int {
		return slices.IndexFunc(env, func(e corev1.EnvVar) bool {
			return e.Name == name
		})
	}
	for _, name := range []string{
		"OTEL_K8S_POD_NAME", "OTEL_K8S_NAMESPACE_NAME", "OTEL_K8S_NODE_NAME",
	} {
		assert.Less(t, index(name), index("OTEL_RESOURCE_ATTRIBUTES"), name)
	}
}

// TestValidateObservabilityHeadersSecretRef checks that an incomplete
// headersSecretRef is refused before rendering: the API server rejects a
// secretKeyRef without a name or key only when the StatefulSet is written.
func TestValidateObservabilityHeadersSecretRef(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ref     corev1.SecretKeySelector
		wantErr string
	}{
		{
			name: "complete",
			ref: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "otlp"},
				Key:                  "headers",
			},
		},
		{
			name:    "no name",
			ref:     corev1.SecretKeySelector{Key: "headers"},
			wantErr: "otlp.headersSecretRef",
		},
		{
			name: "no key",
			ref: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "otlp"},
			},
			wantErr: "otlp.headersSecretRef",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{HeadersSecretRef: &tc.ref}
			err := ValidateObservability(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestEmptyOTLPAddsResourceAttributesOnly checks that otlp: {} on a tracing
// node adds the resource attributes without enabling any exporter.
func TestEmptyOTLPAddsResourceAttributesOnly(t *testing.T) {
	dn := relayNode()
	dn.Spec.Tracing.Enabled = true
	dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{}
	m := envMap(dn, RenderOptions{})
	assert.Contains(
		t,
		m["OTEL_RESOURCE_ATTRIBUTES"],
		"k8s.pod.name=$(OTEL_K8S_POD_NAME)",
	)
	assert.Contains(
		t,
		m["OTEL_RESOURCE_ATTRIBUTES"],
		"cardano.network.name=mainnet",
	)
	for _, name := range []string{
		"OTEL_LOGS_EXPORTER",
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_HEADERS",
	} {
		assert.NotContains(t, m, name)
	}
}

// TestEnvUnchangedForSpecWithoutNewFields checks that a DingoNode written
// before these fields existed renders no new variables, so upgrading the
// operator does not roll its pod.
func TestEnvUnchangedForSpecWithoutNewFields(t *testing.T) {
	for _, tracing := range []bool{false, true} {
		dn := relayNode()
		dn.Spec.Tracing.Enabled = tracing
		for _, e := range BuildEnv(dn, RenderOptions{}) {
			assert.False(t, strings.HasPrefix(e.Name, "OTEL_"), e.Name)
			assert.NotEqual(t, "DINGO_TRACING_ALWAYS_SAMPLE_POOLS", e.Name)
			assert.Nil(t, e.ValueFrom, e.Name)
		}
	}
}

// TestEnvironmentOverridesSecretBackedVariable checks that a spec.environment
// value replaces a variable the operator sources from a Secret, rather than
// leaving both value and valueFrom set, which the API server rejects.
func TestEnvironmentOverridesSecretBackedVariable(t *testing.T) {
	dn := relayNode()
	dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{
		Logs: true,
		HeadersSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "otlp"},
			Key:                  "headers",
		},
	}
	dn.Spec.Environment = map[string]string{
		"OTEL_EXPORTER_OTLP_HEADERS": "x-token=abc",
	}
	headers := envVar(
		t,
		BuildEnv(dn, RenderOptions{}),
		"OTEL_EXPORTER_OTLP_HEADERS",
	)
	assert.Equal(t, "x-token=abc", headers.Value)
	assert.Nil(t, headers.ValueFrom)
}

func TestSupportsObservabilityExport(t *testing.T) {
	for _, tc := range []struct {
		name              string
		repo              string
		tag               string
		digest            string
		capabilityVersion string
		wants             bool
	}{
		{name: "default tag predates it"},
		{name: "first release", tag: "0.91.0", wants: true},
		{name: "v prefix", tag: "v0.91.0", wants: true},
		{name: "one minor below", tag: "0.90.9"},
		{name: "later", tag: "0.94.2", wants: true},
		{name: "major above zero", tag: "1.0.0", wants: true},
		{name: "unparseable tag is trusted", tag: "main", wants: true},
		{name: "custom repository is trusted", repo: "example/dingo", wants: true},
		{
			name:              "digest capability version overrides tag",
			tag:               "0.91.0",
			digest:            "sha256:" + strings.Repeat("a", 64),
			capabilityVersion: "0.90.9",
		},
		{
			name:              "digest capability version enables export",
			tag:               "0.90.9",
			digest:            "sha256:" + strings.Repeat("a", 64),
			capabilityVersion: "0.91.0",
			wants:             true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dn := relayNode()
			dn.Spec.Image.Repository = tc.repo
			dn.Spec.Image.Tag = tc.tag
			dn.Spec.Image.Digest = tc.digest
			dn.Spec.Image.CapabilityVersion = tc.capabilityVersion
			assert.Equal(t, tc.wants, SupportsObservabilityExport(dn))
		})
	}
}

func TestValidateObservability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		attrs   map[string]string
		wantErr string
	}{
		{name: "none"},
		{name: "plain", attrs: map[string]string{"service.namespace": "cardano"}},
		{
			name:    "operator key",
			attrs:   map[string]string{"k8s.pod.name": "x"},
			wantErr: "k8s.pod.name",
		},
		{
			name:    "network key",
			attrs:   map[string]string{"cardano.network.name": "x"},
			wantErr: "cardano.network.name",
		},
		{
			name:    "comma in key",
			attrs:   map[string]string{"a,b": "x"},
			wantErr: "a,b",
		},
		{
			name:    "equals in key",
			attrs:   map[string]string{"a=b": "x"},
			wantErr: "a=b",
		},
		{
			name:    "empty key",
			attrs:   map[string]string{"": "x"},
			wantErr: "otlp.resourceAttributes",
		},
		{
			name:    "newline in key",
			attrs:   map[string]string{"a\nb": "x"},
			wantErr: "otlp.resourceAttributes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := relayNode()
			dn.Spec.OTLP = &dingov1alpha1.OTLPSpec{ResourceAttributes: tc.attrs}
			err := ValidateObservability(dn)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
