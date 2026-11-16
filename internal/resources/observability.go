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
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// minObservabilityExportMinor is the first 0.x release that reads
// DINGO_TRACING_ALWAYS_SAMPLE_POOLS and exports logs and metrics through the
// OTEL_* variables.
const minObservabilityExportMinor = 91

// SupportsObservabilityExport reports whether the selected image honours
// spec.tracing.alwaysSamplePools and spec.otlp. As for SupportsTracing, an
// image whose version cannot be determined is trusted.
func SupportsObservabilityExport(dn *dingov1alpha1.DingoNode) bool {
	major, minor, _, ok := canonicalImageVersion(dn)
	return !ok || major > 0 || minor >= minObservabilityExportMinor
}

// UsesObservabilityExport reports whether the spec sets any field that needs
// SupportsObservabilityExport.
func UsesObservabilityExport(dn *dingov1alpha1.DingoNode) bool {
	return len(dn.Spec.Tracing.AlwaysSamplePools) > 0 || dn.Spec.OTLP != nil
}

// Downward-API variables that OTEL_RESOURCE_ATTRIBUTES refers to.
const (
	envOTELPodName       = "OTEL_K8S_POD_NAME"
	envOTELNamespaceName = "OTEL_K8S_NAMESPACE_NAME"
	envOTELNodeName      = "OTEL_K8S_NODE_NAME"
)

// operatorResourceAttributes are the keys otlpEnv always sets.
var operatorResourceAttributes = []string{
	"k8s.pod.name",
	"k8s.namespace.name",
	"k8s.node.name",
	"cardano.network.name",
}

// ValidateObservability rejects an incomplete headersSecretRef, which the API
// server would refuse only when the StatefulSet is written, and resource
// attribute keys that the operator sets itself or that
// OTEL_RESOURCE_ATTRIBUTES cannot carry: the format is comma-separated
// key=value pairs with only the values percent-encoded.
func ValidateObservability(dn *dingov1alpha1.DingoNode) error {
	if dn.Spec.OTLP == nil {
		return nil
	}
	if ref := dn.Spec.OTLP.HeadersSecretRef; ref != nil &&
		(ref.Name == "" || ref.Key == "") {
		return errors.New(
			"otlp.headersSecretRef requires both name and key",
		)
	}
	for _, key := range sortedKeys(dn.Spec.OTLP.ResourceAttributes) {
		if key == "" || strings.ContainsAny(key, ",=%$") ||
			strings.ContainsFunc(key, unicode.IsSpace) {
			return fmt.Errorf(
				"otlp.resourceAttributes key %q is invalid: it must be "+
					"non-empty and contain no comma, equals sign, percent "+
					"sign, dollar sign or whitespace",
				key,
			)
		}
		if slices.Contains(operatorResourceAttributes, key) {
			return fmt.Errorf(
				"otlp.resourceAttributes key %q is set by the operator",
				key,
			)
		}
	}
	return nil
}

// otlpEnv returns the OTEL_* settings for spec.otlp. Nothing is emitted
// without it, so a DingoNode that does not use it renders as before.
func otlpEnv(dn *dingov1alpha1.DingoNode) []corev1.EnvVar {
	o := dn.Spec.OTLP
	if o == nil {
		return nil
	}
	var env []corev1.EnvVar
	if o.Logs {
		env = append(env, corev1.EnvVar{Name: "OTEL_LOGS_EXPORTER", Value: "otlp"})
	}
	if o.Metrics {
		env = append(env, corev1.EnvVar{Name: "OTEL_METRICS_EXPORTER", Value: "otlp"})
	}
	if o.Endpoint != "" {
		env = append(env, corev1.EnvVar{
			Name:  "OTEL_EXPORTER_OTLP_ENDPOINT",
			Value: o.Endpoint,
		})
	}
	if o.Protocol != "" {
		env = append(env, corev1.EnvVar{
			Name:  "OTEL_EXPORTER_OTLP_PROTOCOL",
			Value: string(o.Protocol),
		})
	}
	if o.HeadersSecretRef != nil {
		ref := o.HeadersSecretRef.DeepCopy()
		ref.Optional = new(false)
		env = append(env, corev1.EnvVar{
			Name:      "OTEL_EXPORTER_OTLP_HEADERS",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref},
		})
	}
	env = append(env,
		fieldEnv(envOTELPodName, "metadata.name"),
		fieldEnv(envOTELNamespaceName, "metadata.namespace"),
		fieldEnv(envOTELNodeName, "spec.nodeName"),
		corev1.EnvVar{
			Name:  "OTEL_RESOURCE_ATTRIBUTES",
			Value: resourceAttributes(dn),
		},
	)
	return env
}

func fieldEnv(name, path string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: path},
		},
	}
}

// resourceAttributes renders OTEL_RESOURCE_ATTRIBUTES. The pod, namespace and
// node names come from the downward API through $(VAR) references, which
// Kubernetes expands from variables defined earlier in the list. User values
// are percent-encoded for the OTEL format, and then every $ is doubled so
// Kubernetes cannot expand a $(...) in them.
func resourceAttributes(dn *dingov1alpha1.DingoNode) string {
	attrs := dn.Spec.OTLP.ResourceAttributes
	pairs := make([]string, 0, len(operatorResourceAttributes)+len(attrs))
	pairs = append(pairs,
		"k8s.pod.name=$("+envOTELPodName+")",
		"k8s.namespace.name=$("+envOTELNamespaceName+")",
		"k8s.node.name=$("+envOTELNodeName+")",
		"cardano.network.name="+url.PathEscape(dn.Spec.Network),
	)
	for _, key := range sortedKeys(attrs) {
		value := strings.ReplaceAll(url.PathEscape(attrs[key]), "$", "$$")
		pairs = append(pairs, key+"="+value)
	}
	return strings.Join(pairs, ",")
}
