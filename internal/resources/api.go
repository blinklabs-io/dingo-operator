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
	"net"
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// APIAccessLabel grants access to enabled APIs. Cross-namespace clients must
// carry the label on both their pod and namespace.
const APIAccessLabel = "dingo.blinklabs.io/api"

const (
	apiTLSVolume = "api-tls"
	apiTLSPath   = "/var/run/dingo-api-tls"
)

type apiListener struct {
	name     string
	port     int32
	portEnvs []string
	hostEnv  string
}

func apiListeners(dn *dingov1alpha1.DingoNode) []apiListener {
	a := dn.Spec.APIs
	if a == nil {
		a = &dingov1alpha1.APISpec{}
	}
	listeners := []apiListener{
		{name: "utxorpc", port: a.UTxORPC},
		{name: "blockfrost", port: a.Blockfrost},
		{name: "mesh", port: a.Mesh},
		{name: "kupo", port: a.Kupo},
		{name: "bark", port: a.Bark},
		{name: "midnight", port: a.Midnight},
	}
	for i := range listeners {
		l := &listeners[i]
		upper := strings.ToUpper(l.name)
		switch l.name {
		case "bark", "midnight":
			l.portEnvs = []string{"DINGO_" + upper + "_PORT"}
			l.hostEnv = "DINGO_" + upper + "_HOST"
		default:
			l.portEnvs = []string{"DINGO_PLUGINS_API_" + upper + "_CONFIG_PORT"}
			l.hostEnv = "CARDANO_BIND_ADDR"
			if l.name != "kupo" {
				l.portEnvs = append(l.portEnvs, "DINGO_"+upper+"_PORT")
			}
		}
		if dn.Spec.APIs == nil {
			for _, key := range l.portEnvs {
				if value, ok := dn.Spec.Environment[key]; ok {
					port, err := strconv.ParseInt(value, 10, 32)
					if err == nil && port >= 0 && port <= 65535 {
						l.port = int32(port)
					}
					break
				}
			}
		}
	}
	return listeners
}

// ValidateAPIs rejects listener/Service disagreement before resources are changed.
func ValidateAPIs(dn *dingov1alpha1.DingoNode) error {
	if a := dn.Spec.APIs; a != nil {
		for i, peer := range a.IngressFrom {
			selectors := peer.NamespaceSelector != nil ||
				peer.PodSelector != nil
			if peer.IPBlock != nil && selectors {
				return fmt.Errorf(
					"apis.ingressFrom[%d] cannot combine ipBlock with selectors",
					i,
				)
			}
			if peer.IPBlock == nil && !selectors {
				return fmt.Errorf(
					"apis.ingressFrom[%d] must set ipBlock or a selector",
					i,
				)
			}
			if peer.IPBlock != nil {
				if _, _, err := net.ParseCIDR(peer.IPBlock.CIDR); err != nil {
					return fmt.Errorf(
						"apis.ingressFrom[%d].ipBlock.cidr is invalid: %w",
						i,
						err,
					)
				}
				for j, excluded := range peer.IPBlock.Except {
					if _, _, err := net.ParseCIDR(excluded); err != nil {
						return fmt.Errorf(
							"apis.ingressFrom[%d].ipBlock.except[%d] is invalid: %w",
							i,
							j,
							err,
						)
					}
				}
			}
		}
	}
	used := map[int32]string{
		portRelay: "relay", PortNodeToClient: "private",
		portMetrics: "metrics", portHealth: "health",
	}
	anyEnabled := false
	for _, l := range apiListeners(dn) {
		for _, key := range l.portEnvs {
			if value, ok := dn.Spec.Environment[key]; ok {
				port, err := strconv.ParseInt(value, 10, 32)
				if err != nil || port < 0 || port > 65535 {
					return fmt.Errorf(
						"%s must be a port between 0 and 65535",
						key,
					)
				}
				if port != int64(l.port) {
					return fmt.Errorf(
						"%s disagrees with the %s API port",
						key,
						l.name,
					)
				}
			}
		}
		if l.port < 0 || l.port > 65535 {
			return fmt.Errorf("%s API port must be between 0 and 65535", l.name)
		}
		if l.port == 0 {
			continue
		}
		anyEnabled = true
		if other, ok := used[l.port]; ok {
			return fmt.Errorf("%s API port conflicts with %s", l.name, other)
		}
		used[l.port] = l.name
		if host, ok := dn.Spec.Environment[l.hostEnv]; ok && host != "0.0.0.0" {
			return fmt.Errorf(
				"%s must be 0.0.0.0 for Service access",
				l.hostEnv,
			)
		}
		if l.name == "midnight" {
			for _, key := range []string{"DINGO_MIDNIGHT_ENABLED", "DINGO_MIDNIGHT_SERVER_ENABLED"} {
				if v, ok := dn.Spec.Environment[key]; ok && v != "true" {
					return fmt.Errorf(
						"%s must be true when the Midnight API is enabled",
						key,
					)
				}
			}
		}
	}
	if anyEnabled {
		for key, want := range map[string]string{"CARDANO_PORT": "3001", "CARDANO_PRIVATE_PORT": "3002", "CARDANO_METRICS_PORT": "12798"} {
			if v, ok := dn.Spec.Environment[key]; ok && v != want {
				return fmt.Errorf(
					"%s must be %s when API listeners are enabled",
					key,
					want,
				)
			}
		}
		if storageMode(dn) != "api" {
			return errors.New("API listeners require spec.storageMode=api")
		}
		if v, ok := dn.Spec.Environment["DINGO_STORAGE_MODE"]; ok &&
			v != "api" {
			return errors.New(
				"DINGO_STORAGE_MODE must be api when API listeners are enabled",
			)
		}
	}
	if a := dn.Spec.APIs; a != nil && a.UTxORPCTLSSecretRef != "" {
		if a.UTxORPC == 0 {
			return errors.New(
				"utxorpcTLSSecretRef requires the UTxORPC API port",
			)
		}
		for key, want := range map[string]string{
			"TLS_CERT_FILE_PATH": apiTLSPath + "/tls.crt",
			"TLS_KEY_FILE_PATH":  apiTLSPath + "/tls.key",
		} {
			if value, ok := dn.Spec.Environment[key]; ok && value != want {
				return fmt.Errorf("%s conflicts with utxorpcTLSSecretRef", key)
			}
		}
	}
	return nil
}

func apiEnv(dn *dingov1alpha1.DingoNode) []corev1.EnvVar {
	var env []corev1.EnvVar
	for _, l := range apiListeners(dn) {
		if dn.Spec.APIs == nil && l.port == 0 {
			continue
		}
		for _, key := range l.portEnvs {
			env = append(
				env,
				corev1.EnvVar{
					Name:  key,
					Value: strconv.FormatInt(int64(l.port), 10),
				},
			)
		}
		if l.port == 0 {
			continue
		}
		env = append(env, corev1.EnvVar{Name: l.hostEnv, Value: "0.0.0.0"})
		if l.name == "midnight" {
			env = append(
				env,
				corev1.EnvVar{Name: "DINGO_MIDNIGHT_ENABLED", Value: "true"},
				corev1.EnvVar{
					Name:  "DINGO_MIDNIGHT_SERVER_ENABLED",
					Value: "true",
				},
			)
		}
	}
	if a := dn.Spec.APIs; a != nil && a.UTxORPCTLSSecretRef != "" {
		env = append(
			env,
			corev1.EnvVar{
				Name:  "TLS_CERT_FILE_PATH",
				Value: apiTLSPath + "/tls.crt",
			},
			corev1.EnvVar{
				Name:  "TLS_KEY_FILE_PATH",
				Value: apiTLSPath + "/tls.key",
			},
		)
	}
	out := make([]corev1.EnvVar, 0, len(env))
	seen := map[string]bool{}
	for _, e := range env {
		if !seen[e.Name] {
			out = append(out, e)
			seen[e.Name] = true
		}
	}
	return out
}
