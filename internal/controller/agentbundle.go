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
	"bytes"
	"context"
	"errors"
	"fmt"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/kesagent"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reasonOpCertUnverified is the KeysValid reason while a delivered bundle waits
// for the signing agent to say which opcert it serves.
const reasonOpCertUnverified = "OpCertUnverified"

// agentBundleProblem reports whether Dingo could start on the keys Secret's
// opcert against the opcert the signing agent serves. Dingo admits the agent
// only when the agent's opcert is byte-equal to the one in its key file, and
// otherwise takes the agent's only when it is for the same cold key at a
// higher counter; any other pairing leaves a restarted node unable to sign.
// rejected describes such a bundle. unverified is set when the agent could not
// be asked, so the bundle is neither rolled out nor rejected yet.
//
// Only an Auto node holds the control certificate info needs. A bundle is
// checked only while it differs from the live template and has not been
// confirmed in place, so an accepted bundle does not ask the agent again.
func (r *DingoNodeReconciler) agentBundleProblem(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	secret *corev1.Secret,
) (rejected, unverified, err error) {
	bp := dn.Spec.BlockProducer
	if !resources.UsesSigningAgent(dn) ||
		bp.Rotation.Mode != dingov1alpha1.RotationModeAuto ||
		bp.SigningAgent.ControlClientCertSecretRef == "" {
		return nil, nil, nil
	}
	full := keysChecksum(secret)
	if prev := dn.Status.OpCert.InPlaceReload; prev != nil &&
		prev.Checksum == full && prev.Applied {
		return nil, nil, nil
	}
	live, err := r.liveKeysTemplate(ctx, dn)
	if err != nil {
		return nil, nil, err
	}
	if live.exists && live.checksum == full {
		return nil, nil, nil
	}
	// validateKeysSecret accepted the bundle, so its opcert is present and
	// parses.
	certData, _, err := opCertFile(secret.Data)
	if err != nil || certData == nil {
		return nil, nil, nil //nolint:nilerr // validateKeysSecret reports it
	}
	local, err := opcert.Payload(certData)
	if err != nil {
		return nil, nil, nil //nolint:nilerr // validateKeysSecret reports it
	}
	ours, ourCold, err := opcert.ParseCBOR(local)
	if err != nil {
		return nil, nil, nil //nolint:nilerr // validateKeysSecret reports it
	}

	cfg, problem, err := r.agentControlConfig(
		ctx, dn, bp.SigningAgent.ControlProfile,
	)
	if err != nil {
		return nil, err, nil
	}
	if problem != "" {
		return nil, errors.New(problem), nil
	}
	active, err := kesagent.ActiveKey(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf(
			"ask the signing agent which opcert it serves: %w", err,
		), nil
	}
	if bytes.Equal(active.OpCert, local) {
		return nil, nil, nil
	}
	served, servedCold, perr := opcert.ParseCBOR(active.OpCert)
	if perr == nil && bytes.Equal(servedCold, ourCold) &&
		served.IssueNumber > ours.IssueNumber {
		// The node takes the agent's newer opcert, and adoptPendingInstall
		// writes it to the Secret.
		return nil, nil, nil
	}
	return fmt.Errorf(
		"the signing agent serves opcert counter %d, not the keys Secret's "+
			"opcert (counter %d); Dingo would refuse the agent on restart",
		active.IssueNumber, ours.IssueNumber,
	), nil, nil
}

// holdUnverifiedKeys reports a delivered bundle that waits for the signing
// agent. The node keeps forging on what it loaded, so it is not Degraded.
func (r *DingoNodeReconciler) holdUnverifiedKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	unverified error,
) {
	log.FromContext(ctx).Info(
		"holding delivered block-producer keys until the signing agent answers",
		"error", unverified.Error(),
	)
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condKeysValid,
		Status:             metav1.ConditionFalse,
		Reason:             reasonOpCertUnverified,
		Message:            unverified.Error(),
		ObservedGeneration: dn.Generation,
	})
}
