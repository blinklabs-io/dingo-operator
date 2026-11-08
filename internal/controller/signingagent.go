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
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
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
// crash-looping the forging pod.
func validateSigningAgent(bp *dingov1alpha1.BlockProducerSpec) error {
	sa := bp.SigningAgent
	if sa == nil {
		return nil
	}
	// Auto generates the KES key in the operator and writes it to the keys
	// Secret, which a node signing through an agent never uses.
	if bp.Rotation.Mode == dingov1alpha1.RotationModeAuto {
		return errors.New(
			"spec.blockProducer.signingAgent cannot be combined with " +
				"rotation.mode Auto: Auto generates the KES key in the " +
				"operator, while the signing agent holds it; use Assisted " +
				"or MonitorOnly",
		)
	}
	host, port, err := net.SplitHostPort(sa.Address)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf(
			"spec.blockProducer.signingAgent.address %q must be host:port",
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
	return nil
}

// checkSigningAgentCertData describes what an agent client certificate Secret
// lacks, or returns "" when it holds both halves of the pair. Dingo refuses a
// certificate without its key and the reverse, and the volume mount fails on
// either missing.
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
	return ""
}

// checkSigningAgentCert reads the agent client certificate Secret, when the
// spec names one. It returns a description of what is wrong with the Secret,
// or "" when it is usable or not needed, and an error only for a failed read
// other than not-found. The Secret is read with the same uncached reader, and
// the same permission, as the keys Secret.
func (r *DingoNodeReconciler) checkSigningAgentCert(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (string, error) {
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.SigningAgent == nil ||
		bp.SigningAgent.ClientCertSecretRef == "" || r.APIReader == nil {
		return "", nil
	}
	name := bp.SigningAgent.ClientCertSecretRef
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: name, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf(
				"signing agent client certificate Secret %q not found",
				name,
			), nil
		}
		return "", fmt.Errorf(
			"read signing agent client certificate secret: %w", err,
		)
	}
	return checkSigningAgentCertData(name, secret.Data), nil
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
