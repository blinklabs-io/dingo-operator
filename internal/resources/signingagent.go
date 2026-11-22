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
	"sigs.k8s.io/yaml"
)

// Dingo settings for a remote KES signing agent. Dingo reads them through
// envconfig.Process("cardano", ...), hence the CARDANO_ prefix.
const (
	// KESKeyEnv names a local KES signing key. Dingo refuses it alongside an
	// agent address.
	KESKeyEnv = "CARDANO_SHELLEY_KES_KEY"
	// unprefixedKESKeyEnv is KESKeyEnv in the spelling envconfig also reads.
	unprefixedKESKeyEnv      = "SHELLEY_KES_KEY"
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

// PodSignsThroughAgent reports whether a pod spec or template signs block
// headers through a remote agent and so holds no KES key. It reads what the
// workload carries, not the DingoNode spec, which can name an agent while a
// held template or a running pod still has a local KES key. Only an agent
// address on the dingo container with no CARDANO_SHELLEY_KES_KEY proves it;
// envFrom, a command or arguments could set either where it cannot be read, so
// they, a missing dingo container and a missing address read as a local key.
// A pool-list pod has no agent address env: each pool's agent settings are in
// the pool list file, so the pool list ConfigMap mounted where Dingo reads its
// configuration, with no CARDANO_SHELLEY_KES_KEY or SHELLEY_KES_KEY, proves
// it instead.
func PodSignsThroughAgent(spec corev1.PodSpec) bool {
	c, ok := DingoContainer(spec)
	if !ok || len(c.EnvFrom) > 0 || len(c.Command) > 0 || len(c.Args) > 0 {
		return false
	}
	if mountsPoolList(spec, c) {
		for _, e := range c.Env {
			if e.Name == KESKeyEnv || e.Name == unprefixedKESKeyEnv {
				return false
			}
		}
		return true
	}
	agent := false
	for _, e := range c.Env {
		switch e.Name {
		case KESKeyEnv:
			return false
		case signingAgentAddressEnv:
			agent = e.ValueFrom == nil && e.Value != ""
		}
	}
	return agent
}

// SigningAgentLink returns the agent address and server key pin the dingo
// container of a pod spec is wired to, and whether it has an address. A pod
// signing through another agent can reach another slot guard.
func SigningAgentLink(spec corev1.PodSpec) (address, pin string, ok bool) {
	c, found := DingoContainer(spec)
	if !found {
		return "", "", false
	}
	for _, e := range c.Env {
		switch e.Name {
		case signingAgentAddressEnv:
			address = e.Value
		case signingAgentPinEnv:
			pin = e.Value
		}
	}
	return address, pin, address != ""
}

// PodMountsPoolList reports whether the dingo container of a pod spec reads
// the pool list ConfigMap as its configuration file, where each pool names its
// agent.
func PodMountsPoolList(spec corev1.PodSpec) bool {
	c, ok := DingoContainer(spec)
	if !ok {
		return false
	}
	return mountsPoolList(spec, c)
}

// PoolsConfigAgentLink returns the agent address and server key pin a rendered
// pool list signs through, and whether every pool names that one agent.
func PoolsConfigAgentLink(config string) (address, pin string, ok bool) {
	var cfg struct {
		Pools []struct {
			Address string `json:"shelleyKesAgentAddress"`
			Pin     string `json:"shelleyKesAgentServerSpkiSha256"`
		} `json:"blockProducerPools"`
	}
	if err := yaml.Unmarshal([]byte(config), &cfg); err != nil ||
		len(cfg.Pools) == 0 {
		return "", "", false
	}
	address, pin = cfg.Pools[0].Address, cfg.Pools[0].Pin
	for _, p := range cfg.Pools[1:] {
		if p.Address != address || p.Pin != pin {
			return "", "", false
		}
	}
	return address, pin, address != ""
}

// mountsPoolList reports whether c, the dingo container of spec, reads the
// pool list ConfigMap as its configuration file.
func mountsPoolList(spec corev1.PodSpec, c *corev1.Container) bool {
	fromConfigMap := false
	for _, v := range spec.Volumes {
		if v.Name == poolsConfigVolumeName {
			fromConfigMap = v.ConfigMap != nil
		}
	}
	if !fromConfigMap {
		return false
	}
	for _, m := range c.VolumeMounts {
		if m.Name == poolsConfigVolumeName &&
			m.MountPath == poolsConfigMountPath &&
			m.SubPath == PoolsConfigKey {
			return true
		}
	}
	return false
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
