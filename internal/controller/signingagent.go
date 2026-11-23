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

package controller

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/kesagent"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// unusedKESKeyMessage is appended to KeysValid when a node that signs through
// a remote agent has a kes.skey in its keys Secret anyway.
const unusedKESKeyMessage = "kes.skey in the keys Secret is ignored; " +
	"spec.blockProducer.signingAgent signs block headers"

// reasonSigningAgentCertInvalid is the KeysValid and Degraded reason for an
// agent client certificate Secret that is missing or incomplete.
const reasonSigningAgentCertInvalid = "SigningAgentCertInvalid"

// validateSigningAgent mirrors the checks Dingo applies to a remote KES agent
// at startup, so a spec it would refuse is refused here instead of
// crash-looping the forging pod. It also refuses a port that is not a number
// from 1 to 65535, which Dingo accepts at startup but cannot dial.
func validateSigningAgent(bp *dingov1alpha1.BlockProducerSpec) error {
	sa := bp.SigningAgent
	if sa == nil {
		return nil
	}
	host, port, err := net.SplitHostPort(sa.Address)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf(
			"spec.blockProducer.signingAgent.address %q must be host:port",
			sa.Address,
		)
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return fmt.Errorf(
			"spec.blockProducer.signingAgent.address %q must have a port "+
				"from 1 to 65535",
			sa.Address,
		)
	}
	pin := strings.TrimSpace(sa.ServerSPKISHA256)
	if sa.Plaintext && (pin != "" || sa.ClientCertSecretRef != "") {
		return errors.New(
			"spec.blockProducer.signingAgent.plaintext takes neither " +
				"serverSpkiSha256 nor clientCertSecretRef; both need TLS",
		)
	}
	if pin != "" {
		if b, err := hex.DecodeString(pin); err != nil ||
			len(b) != sha256.Size {
			return fmt.Errorf(
				"spec.blockProducer.signingAgent.serverSpkiSha256 must be "+
					"%d hex characters",
				2*sha256.Size,
			)
		}
	}
	return validateAgentControl(bp, pin)
}

// validateAgentControl checks the control role settings. Auto rotation drives
// the agent's control role, which the agent offers only over mutually
// authenticated TLS to a client whose key is pinned on the pool's profile, so
// it needs a control client certificate and TLS; the server key is pinned for
// the control link as for the node's. Elsewhere the settings do nothing, so
// they are refused rather than ignored.
func validateAgentControl(
	bp *dingov1alpha1.BlockProducerSpec,
	pin string,
) error {
	sa := bp.SigningAgent
	if bp.Rotation.Mode != dingov1alpha1.RotationModeAuto {
		if sa.ControlClientCertSecretRef != "" || sa.ControlProfile != "" {
			return errors.New(
				"spec.blockProducer.signingAgent.controlClientCertSecretRef " +
					"and controlProfile apply only with rotation.mode Auto",
			)
		}
		return nil
	}
	if len(bp.Pools) > 0 && sa.ControlProfile != "" {
		return errors.New(
			"spec.blockProducer.signingAgent.controlProfile cannot be set " +
				"with spec.blockProducer.pools: each pool's agentProfile " +
				"names the profile to control",
		)
	}
	switch {
	case sa.ControlClientCertSecretRef == "":
		return errors.New(
			"spec.blockProducer.signingAgent with rotation.mode Auto " +
				"requires controlClientCertSecretRef: the operator rotates " +
				"the key through the agent's control role; otherwise use " +
				"Assisted or MonitorOnly",
		)
	case sa.Plaintext:
		return errors.New(
			"spec.blockProducer.signingAgent with rotation.mode Auto " +
				"cannot be plaintext: the control role needs TLS",
		)
	case pin == "":
		return errors.New(
			"spec.blockProducer.signingAgent with rotation.mode Auto " +
				"requires serverSpkiSha256 to pin the control connection",
		)
	case !bp.Rotation.ReloadInPlace:
		return errors.New(
			"spec.blockProducer.signingAgent with rotation.mode Auto " +
				"requires rotation.reloadInPlace: the node takes the " +
				"rotated opcert from the agent, and a pod roll would " +
				"interrupt forging",
		)
	}
	return nil
}

// checkSigningAgentCertData describes what an agent client certificate Secret
// lacks, or returns "" when it holds a usable certificate and key pair. Dingo
// refuses a certificate without its key and the reverse, and one it cannot
// load as a pair, and the volume mount fails on either file missing. Expiry
// and the issuer are left to the TLS handshake.
func checkSigningAgentCertData(name string, data map[string][]byte) string {
	var missing []string
	for _, k := range []string{
		corev1.TLSCertKey,
		corev1.TLSPrivateKeyKey,
	} {
		if len(data[k]) == 0 {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Sprintf(
			"signing agent client certificate Secret %q is missing %s; "+
				"tls.crt and tls.key are set as a pair",
			name, strings.Join(missing, ", "),
		)
	}
	if _, err := tls.X509KeyPair(
		data[corev1.TLSCertKey],
		data[corev1.TLSPrivateKeyKey],
	); err != nil {
		return fmt.Sprintf(
			"signing agent client certificate Secret %q does not hold a "+
				"usable PEM certificate and matching key: %v",
			name, err,
		)
	}
	return ""
}

// signingAgentCertChecksum returns a digest of a validated agent client
// certificate pair. Only tls.crt and tls.key are hashed, the files Dingo
// loads, so other keys in the Secret do not roll the pod.
func signingAgentCertChecksum(data map[string][]byte) string {
	return checksumData(map[string][]byte{
		corev1.TLSCertKey:       data[corev1.TLSCertKey],
		corev1.TLSPrivateKeyKey: data[corev1.TLSPrivateKeyKey],
	})
}

// checkSigningAgentCert reads the agent client certificate Secret, when the
// spec names one. It returns a description of what is wrong with the Secret,
// or "" when it is usable or not needed; the checksum of a usable pair, for
// the pod template, or "" otherwise; and an error only for a failed read
// other than not-found. The Secret is read with the same uncached reader, and
// the same permission, as the keys Secret.
func (r *DingoNodeReconciler) checkSigningAgentCert(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (problem, sum string, err error) {
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.SigningAgent == nil ||
		bp.SigningAgent.ClientCertSecretRef == "" || r.APIReader == nil {
		return "", "", nil
	}
	name := bp.SigningAgent.ClientCertSecretRef
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: name, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf(
				"signing agent client certificate Secret %q not found",
				name,
			), "", nil
		}
		return "", "", fmt.Errorf(
			"read signing agent client certificate secret: %w", err,
		)
	}
	if p := checkSigningAgentCertData(name, secret.Data); p != "" {
		return p, "", nil
	}
	return "", signingAgentCertChecksum(secret.Data), nil
}

// agentControlConfig reads the control client certificate Secret and returns
// the settings for the agent's control role on profile, which is the agent
// profile of the pool the command is for. problem describes a Secret that is
// missing or unusable; err is a failed read other than not-found.
func (r *DingoNodeReconciler) agentControlConfig(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	profile string,
) (cfg kesagent.Config, problem string, err error) {
	sa := dn.Spec.BlockProducer.SigningAgent
	if r.APIReader == nil {
		return cfg, "", errors.New(
			"no reader for the control client certificate",
		)
	}
	name := sa.ControlClientCertSecretRef
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: name, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return cfg, fmt.Sprintf(
				"control client certificate Secret %q not found", name,
			), nil
		}
		return cfg, "", fmt.Errorf(
			"read control client certificate secret: %w", err,
		)
	}
	if p := checkSigningAgentCertData(name, secret.Data); p != "" {
		return cfg, p, nil
	}
	return kesagent.Config{
		Address:          sa.Address,
		ServerSPKISHA256: sa.ServerSPKISHA256,
		ClientCert:       secret.Data[corev1.TLSCertKey],
		ClientKey:        secret.Data[corev1.TLSPrivateKeyKey],
		Profile:          profile,
	}, "", nil
}

// rejectSigningAgentCert reports an agent client certificate Secret the pod
// could not mount, as KeysValid=False and Degraded.
func (r *DingoNodeReconciler) rejectSigningAgentCert(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	problem string,
) {
	log.FromContext(ctx).Info(
		"signing agent client certificate unusable",
		"problem", problem,
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn,
			nil,
			corev1.EventTypeWarning,
			reasonSigningAgentCertInvalid,
			"ValidateKeys",
			"%s",
			problem,
		)
	}
	for _, t := range []string{condKeysValid, condDegraded} {
		status := metav1.ConditionFalse
		if t == condDegraded {
			status = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type:               t,
			Status:             status,
			Reason:             reasonSigningAgentCertInvalid,
			Message:            problem,
			ObservedGeneration: dn.Generation,
		})
	}
}
