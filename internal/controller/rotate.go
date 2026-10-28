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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/coldsigner"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// condOpCertIssued reports the outcome of the last Auto rotation attempt.
const condOpCertIssued = "OpCertIssued"

// issueRetryInterval is how long Auto rotation waits after a failed issuance
// before it reaches the cold signer again. A KES period is hours long and
// rotation starts renewBeforePeriods ahead of expiry, so a few retries an hour
// cost nothing.
const issueRetryInterval = 10 * time.Minute

type issueFailure struct {
	at              time.Time
	reason, message string
}

// Keys the operator reads from the cold-signer Secrets. The mTLS Secret uses
// the kubernetes.io/tls names plus ca.crt; the cold key is a cardano-cli text
// envelope.
const (
	signerSecretCA   = "ca.crt"
	signerSecretCert = "tls.crt"
	signerSecretKey  = "tls.key"
	keyFileCold      = "cold.skey"

	coldEnvelopeTypePrefix = "StakePoolSigningKey"
	kesEnvelopeType        = "KesSigningKey_ed25519_kes_2^6"
)

// ColdSignerFactory returns the cold signer that issues a node's opcerts.
type ColdSignerFactory func(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (coldsigner.Issuer, error)

// autoRotate issues a new KES key and opcert into the keys Secret when an
// Auto-rotation block producer's KES key is due for renewal. Writing the
// Secret is the whole job: the next reconcile validates the new bundle like
// any delivered one and rolls the pod onto it.
//
// The opcert is issued from the on-chain counter, never from the operator's
// own bookkeeping, and starts at the node's current KES period. Nothing is
// issued while either is unknown.
//
// RotationDue comes from the running pod, which keeps reporting its old opcert
// until it rolls and is scraped again. The bundle staged in the Secret is
// therefore checked first, and a rotation already staged is left to finish.
func (r *DingoNodeReconciler) autoRotate(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.Rotation.Mode != dingov1alpha1.RotationModeAuto ||
		!meta.IsStatusConditionTrue(dn.Status.Conditions, condRotationDue) {
		return
	}
	floor, ok := observedOnChainCounter(dn, time.Now())
	if !ok {
		r.refuseIssue(
			ctx, dn, "OnChainCounterUnknown",
			"no fresh on-chain opcert counter for this pool",
		)
		return
	}
	st := dn.Status.KES
	if st.CurrentPeriod <= 0 || st.ExpiryPeriod <= st.OpCertKESPeriod {
		r.refuseIssue(
			ctx, dn, "KESPeriodUnknown",
			"the node's KES period has not been scraped",
		)
		return
	}
	maxEvolutions := st.ExpiryPeriod - st.OpCertKESPeriod
	renewBefore := int64(bp.Rotation.RenewBeforePeriods)
	if renewBefore >= maxEvolutions {
		r.refuseIssue(
			ctx, dn, "RenewWindowTooLong",
			fmt.Sprintf(
				"renewBeforePeriods %d is not below the %d-period KES "+
					"window, so every new opcert would be due at once",
				renewBefore, maxEvolutions,
			),
		)
		return
	}

	if r.APIReader == nil {
		r.refuseIssue(
			ctx, dn, "KeysSecretUnavailable", "no reader for the keys Secret",
		)
		return
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: bp.Keys.SecretRef, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		r.refuseIssue(ctx, dn, "KeysSecretUnavailable", err.Error())
		return
	}
	certData, certFile, err := opCertFile(secret.Data)
	if err == nil && certData == nil {
		err = errors.New("keys secret holds no opcert")
	}
	if err != nil {
		r.refuseIssue(ctx, dn, "KeysSecretInvalid", err.Error())
		return
	}
	staged, _, err := opcert.Parse(certData)
	if err != nil {
		r.refuseIssue(ctx, dn, "KeysSecretInvalid", err.Error())
		return
	}
	stagedStart, err := certField(staged.KesPeriod, "kes period")
	if err != nil {
		r.refuseIssue(ctx, dn, "KeysSecretInvalid", err.Error())
		return
	}
	// A staged bundle that does not validate can never be rolled out, so it
	// is replaced rather than waited on.
	if _, verr := validateKeysSecret(secret, dn); verr == nil &&
		stagedStart+maxEvolutions-st.CurrentPeriod > renewBefore {
		return
	}

	nodeKey := client.ObjectKeyFromObject(dn)
	if last, ok := r.lastIssueFailure(nodeKey); ok &&
		time.Since(last.at) < issueRetryInterval {
		setIssueCondition(
			dn, metav1.ConditionFalse, last.reason, last.message,
		)
		return
	}
	fail := func(reason, message string) {
		r.recordIssueFailure(nodeKey, reason, message)
		r.refuseIssue(ctx, dn, reason, message)
	}

	// Check the write is allowed before a signature is spent on it.
	if err := r.Update(ctx, secret.DeepCopy(), client.DryRunAll); err != nil {
		fail("SecretUpdateFailed", err.Error())
		return
	}
	issuer, err := r.issuerFor(ctx, dn)
	if err != nil {
		fail("ColdSignerUnavailable", err.Error())
		return
	}
	// Both are positive: onChainFloor and the period check above.
	kesEnv, cert, err := issueBundle(
		ctx, issuer, uint64(floor), uint64(st.CurrentPeriod), //nolint:gosec
	)
	if err != nil {
		fail("IssueFailed", err.Error())
		return
	}

	candidate := secret.DeepCopy()
	candidate.Data[keyFileKES] = kesEnv
	candidate.Data[certFile] = cert
	if len(candidate.Data[keyFileNodeCert]) > 0 {
		// opCertFile accepted both names only because they were identical.
		candidate.Data[keyFileNodeCert] = cert
	}
	state, err := validateKeysSecret(candidate, dn)
	if err != nil {
		fail("IssuedBundleRejected", err.Error())
		return
	}
	// The update carries the resourceVersion read above, so a concurrent edit
	// of the Secret fails the write instead of being overwritten.
	if err := r.Update(ctx, candidate); err != nil {
		fail("SecretUpdateFailed", err.Error())
		return
	}

	r.forgetIssueFailure(nodeKey)
	msg := fmt.Sprintf(
		"issued opcert counter %d, kes start period %d",
		state.Counter, state.KESPeriod,
	)
	log.FromContext(ctx).Info(msg, "secret", bp.Keys.SecretRef)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn, nil, corev1.EventTypeNormal, "OpCertIssued", "RotateKeys",
			"%s", msg,
		)
	}
	setIssueCondition(dn, metav1.ConditionTrue, "OpCertIssued", msg)
}

// issueBundle generates a KES key and has issuer sign an opcert for it. It
// returns the kes.skey and opcert envelopes.
func issueBundle(
	ctx context.Context,
	issuer coldsigner.Issuer,
	chainCounter, kesPeriod uint64,
) ([]byte, []byte, error) {
	seed := make([]byte, kes.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, nil, fmt.Errorf("read kes seed: %w", err)
	}
	sk, vkey, err := kes.KeyGen(kes.CardanoKesDepth, seed)
	clear(seed)
	if err != nil {
		return nil, nil, fmt.Errorf("generate kes key: %w", err)
	}
	defer sk.Zeroize()

	res, err := issuer.SignOpCertFromChain(ctx, vkey, chainCounter, kesPeriod)
	if err != nil {
		return nil, nil, err
	}
	oc := &ledger.OpCert{
		KesVkey:       vkey,
		IssueNumber:   res.IssueCounter,
		KesPeriod:     kesPeriod,
		ColdSignature: res.Signature,
	}
	if err := ledger.VerifyOpCertSignature(oc, res.ColdVkey); err != nil {
		return nil, nil, fmt.Errorf("verify issued opcert: %w", err)
	}
	cert, err := opcert.Encode(oc, res.ColdVkey)
	if err != nil {
		return nil, nil, err
	}
	payload, err := cbor.Encode(sk.Data)
	if err != nil {
		return nil, nil, fmt.Errorf("encode kes key: %w", err)
	}
	kesEnv, err := json.MarshalIndent(struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		CborHex     string `json:"cborHex"`
	}{kesEnvelopeType, "KES Signing Key", hex.EncodeToString(payload)}, "", "    ")
	if err != nil {
		return nil, nil, err
	}
	return kesEnv, cert, nil
}

// issuerFor returns the node's configured cold signer, reading its client
// credentials or cold key from Secrets in the node's namespace.
func (r *DingoNodeReconciler) issuerFor(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (coldsigner.Issuer, error) {
	if r.ColdSigner != nil {
		return r.ColdSigner(ctx, dn)
	}
	if r.APIReader == nil {
		return nil, errors.New("no reader for cold signer secrets")
	}
	cs := dn.Spec.BlockProducer.Rotation.ColdSigner
	read := func(name string) (*corev1.Secret, error) {
		s := &corev1.Secret{}
		key := types.NamespacedName{Name: name, Namespace: dn.Namespace}
		if err := r.APIReader.Get(ctx, key, s); err != nil {
			return nil, fmt.Errorf("read secret %q: %w", name, err)
		}
		return s, nil
	}
	switch cs.Type {
	case dingov1alpha1.ColdSignerBursa, "":
		s, err := read(cs.MTLSSecretRef)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{signerSecretCert, signerSecretKey} {
			if len(s.Data[k]) == 0 {
				return nil, fmt.Errorf(
					"secret %q is missing %s", cs.MTLSSecretRef, k,
				)
			}
		}
		return coldsigner.NewBursaSigner(coldsigner.BursaConfig{
			Endpoint:    cs.Endpoint,
			ColdKeyHash: cs.ColdKeyHash,
			CACert:      s.Data[signerSecretCA],
			ClientCert:  s.Data[signerSecretCert],
			ClientKey:   s.Data[signerSecretKey],
		})
	case dingov1alpha1.ColdSignerSecret:
		s, err := read(cs.SecretRef)
		if err != nil {
			return nil, err
		}
		raw, err := checkKeyEnvelope(
			s.Data[keyFileCold], keyFileCold, coldEnvelopeTypePrefix,
		)
		if err != nil {
			return nil, err
		}
		return coldsigner.NewSecretSigner(raw)
	case dingov1alpha1.ColdSignerNone:
	}
	return nil, fmt.Errorf("cold signer type %q cannot issue", cs.Type)
}

func (r *DingoNodeReconciler) refuseIssue(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	reason, message string,
) {
	log.FromContext(ctx).Info(
		"auto rotation did not issue", "reason", reason, "error", message,
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn, nil, corev1.EventTypeWarning, reason, "RotateKeys",
			"%s", message,
		)
	}
	setIssueCondition(dn, metav1.ConditionFalse, reason, message)
}

func setIssueCondition(
	dn *dingov1alpha1.DingoNode,
	status metav1.ConditionStatus,
	reason, message string,
) {
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condOpCertIssued,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: dn.Generation,
	})
}

func (r *DingoNodeReconciler) lastIssueFailure(
	key types.NamespacedName,
) (issueFailure, bool) {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	f, ok := r.issueFailures[key]
	return f, ok
}

func (r *DingoNodeReconciler) recordIssueFailure(
	key types.NamespacedName,
	reason, message string,
) {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	if r.issueFailures == nil {
		r.issueFailures = make(map[types.NamespacedName]issueFailure)
	}
	r.issueFailures[key] = issueFailure{
		at: time.Now(), reason: reason, message: message,
	}
}

func (r *DingoNodeReconciler) forgetIssueFailure(key types.NamespacedName) {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	delete(r.issueFailures, key)
}
