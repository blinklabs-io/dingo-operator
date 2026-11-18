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
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// Dingo settings for a remote KES signing agent. Dingo reads them through
// envconfig.Process("cardano", ...), hence the CARDANO_ prefix.
const (
	// KESKeyEnv names a local KES signing key. Dingo refuses it alongside an
	// agent address.
	KESKeyEnv                = "CARDANO_SHELLEY_KES_KEY"
	signingAgentAddressEnv   = "CARDANO_SHELLEY_KES_AGENT_ADDRESS"
	signingAgentPinEnv       = "CARDANO_SHELLEY_KES_AGENT_SERVER_SPKI_SHA256"
	signingAgentPlaintextEnv = "CARDANO_SHELLEY_KES_AGENT_PLAINTEXT"
	signingAgentCertEnv      = "CARDANO_SHELLEY_KES_AGENT_CLIENT_CERT_FILE"
	signingAgentKeyEnv       = "CARDANO_SHELLEY_KES_AGENT_CLIENT_KEY_FILE"
	// signingAgentSocketEnv and signingAgentModeEnv are Dingo's local-socket
	// agent settings; with an address Dingo refuses a socket and accepts only
	// the sign mode.
	signingAgentSocketEnv = "CARDANO_SHELLEY_KES_AGENT_SOCKET"
	signingAgentModeEnv   = "CARDANO_SHELLEY_KES_AGENT_MODE"

	signingAgentTLSVolume = "signing-agent-tls"
	signingAgentTLSPath   = "/var/run/dingo-signing-agent-tls"
)

// UsesSigningAgent reports whether dn is a block producer that signs through a
// remote agent instead of a local kes.skey.
func UsesSigningAgent(dn *dingov1alpha1.DingoNode) bool {
	return IsBlockProducer(dn) && dn.Spec.BlockProducer != nil &&
		dn.Spec.BlockProducer.SigningAgent != nil
}

// signingAgentEnv returns Dingo's agent settings for a block producer that
// signs through an agent.
func signingAgentEnv(sa *dingov1alpha1.SigningAgentSpec) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: signingAgentAddressEnv, Value: sa.Address},
	}
	// The pin is validated trimmed, so it is rendered trimmed.
	if pin := strings.TrimSpace(sa.ServerSPKISHA256); pin != "" {
		env = append(env, corev1.EnvVar{
			Name:  signingAgentPinEnv,
			Value: pin,
		})
	}
	if sa.Plaintext {
		env = append(env, corev1.EnvVar{
			Name:  signingAgentPlaintextEnv,
			Value: strconv.FormatBool(true),
		})
	}
	if sa.ClientCertSecretRef != "" {
		env = append(
			env,
			corev1.EnvVar{
				Name:  signingAgentCertEnv,
				Value: signingAgentTLSPath + "/tls.crt",
			},
			corev1.EnvVar{
				Name:  signingAgentKeyEnv,
				Value: signingAgentTLSPath + "/tls.key",
			},
		)
	}
	return env
}

// signingAgentCertSecret returns the agent client certificate Secret to mount,
// or "" when there is none. Like the keys it is mounted only where the keys
// are, so a keyless pod never gets agent credentials.
func signingAgentCertSecret(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) string {
	if !mountsBlockProducerKeys(dn, opts) {
		return ""
	}
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.SigningAgent == nil {
		return ""
	}
	return bp.SigningAgent.ClientCertSecretRef
}

// signingAgentManagedEnv lists the KES settings the operator owns on a block
// producer that signs through an agent.
var signingAgentManagedEnv = []string{
	KESKeyEnv,
	signingAgentAddressEnv,
	signingAgentPinEnv,
	signingAgentPlaintextEnv,
	signingAgentCertEnv,
	signingAgentKeyEnv,
	signingAgentSocketEnv,
	signingAgentModeEnv,
}

// validateSigningAgentEnv rejects spec.environment overrides of the KES
// settings on a block producer that signs through an agent. Dingo refuses a
// local KES key or agent socket alongside an agent address, so an override
// would crash-loop the forging pod; the link settings come from
// spec.blockProducer.signingAgent.
func validateSigningAgentEnv(dn *dingov1alpha1.DingoNode) error {
	if !UsesSigningAgent(dn) {
		return nil
	}
	for _, name := range signingAgentManagedEnv {
		if _, ok := dn.Spec.Environment[name]; ok {
			return fmt.Errorf(
				"%s is managed by the operator when "+
					"spec.blockProducer.signingAgent is set; remove it from "+
					"spec.environment",
				name,
			)
		}
	}
	return nil
}
