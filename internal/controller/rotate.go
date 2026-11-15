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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/coldsigner"
	"github.com/blinklabs-io/dingo-operator/internal/kesagent"
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
	if bp == nil || bp.Rotation.Mode != dingov1alpha1.RotationModeAuto {
		return
	}
	// A node that reloads in place takes the agent's new opcert and stops
	// reporting the rotation as due, so a lost Secret write is repaired here
	// rather than behind RotationDue.
	if r.adoptPendingInstall(ctx, dn) {
		return
	}
	if !meta.IsStatusConditionTrue(dn.Status.Conditions, condRotationDue) {
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
	chainCounter := uint64(floor)         //nolint:gosec
	kesPeriod := uint64(st.CurrentPeriod) //nolint:gosec
	var (
		candidate *corev1.Secret
		install   func() error
	)
	if bp.SigningAgent != nil {
		var reason string
		candidate, install, reason, err = r.issueThroughAgent(
			ctx, dn, issuer, secret, certFile, chainCounter, kesPeriod,
			maxEvolutions, renewBefore,
		)
		if err != nil {
			fail(reason, err.Error())
			return
		}
	} else {
		kesEnv, cert, err := issueBundle(
			ctx, issuer, chainCounter, kesPeriod,
		)
		if err != nil {
			fail("IssueFailed", err.Error())
			return
		}
		candidate = secret.DeepCopy()
		candidate.Data[keyFileKES] = kesEnv
		candidate.Data[certFile] = cert
		if len(candidate.Data[keyFileNodeCert]) > 0 {
			// opCertFile accepted both names only because they were identical.
			candidate.Data[keyFileNodeCert] = cert
		}
	}
	state, err := validateKeysSecret(candidate, dn)
	if err != nil {
		fail("IssuedBundleRejected", err.Error())
		return
	}
	// install promotes the new key in the agent, so the write below is the
	// only step left that can fail. If it does, the agent serves the new
	// opcert while the Secret holds the old one, and the next attempt
	// publishes the opcert the agent serves instead of issuing another.
	if install != nil {
		if err := install(); err != nil {
			fail("AgentInstallFailed", err.Error())
			return
		}
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

// issueOpCert has issuer sign an opcert for kesVkey and verifies it.
func issueOpCert(
	ctx context.Context,
	issuer coldsigner.Issuer,
	kesVkey []byte,
	chainCounter, kesPeriod uint64,
) (*ledger.OpCert, []byte, error) {
	res, err := issuer.SignOpCertFromChain(
		ctx, kesVkey, chainCounter, kesPeriod,
	)
	if err != nil {
		return nil, nil, err
	}
	oc := &ledger.OpCert{
		KesVkey:       kesVkey,
		IssueNumber:   res.IssueCounter,
		KesPeriod:     kesPeriod,
		ColdSignature: res.Signature,
	}
	if err := ledger.VerifyOpCertSignature(oc, res.ColdVkey); err != nil {
		return nil, nil, fmt.Errorf("verify issued opcert: %w", err)
	}
	return oc, res.ColdVkey, nil
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

	oc, coldVkey, err := issueOpCert(
		ctx, issuer, vkey, chainCounter, kesPeriod,
	)
	if err != nil {
		return nil, nil, err
	}
	cert, err := opcert.Encode(oc, coldVkey)
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

// issueThroughAgent rotates a key held by a signing agent. The agent generates
// the key and returns only its verification key; the cold signer issues the
// opcert for it, and the returned install promotes the key in the agent by
// installing that opcert. The returned candidate is the keys Secret with the
// public opcert replaced and no KES private key. The agent is touched only by
// the staging command here: everything that can reject the new opcert runs
// before install, since install makes the new key the serving one.
//
// The reason names the failed step for the OpCertIssued condition.
func (r *DingoNodeReconciler) issueThroughAgent(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	issuer coldsigner.Issuer,
	secret *corev1.Secret,
	certFile string,
	chainCounter, kesPeriod uint64,
	maxEvolutions, renewBefore int64,
) (candidate *corev1.Secret, install func() error, reason string, err error) {
	cfg, problem, err := r.agentControlConfig(ctx, dn)
	if err != nil {
		return nil, nil, "ControlCertUnavailable", err
	}
	if problem != "" {
		return nil, nil, "ControlCertInvalid", errors.New(problem)
	}
	// A rotation that failed after install-key left the agent serving the
	// opcert it issued. Publish that one: issuing another at the same counter
	// would not be offered to a node that already took the first. Without an
	// answer from info that cannot be ruled out, so the attempt fails.
	adopted, err := r.adoptServedOpCert(
		ctx, cfg, secret, certFile, chainCounter,
		kesPeriod, maxEvolutions, renewBefore,
	)
	if err != nil {
		return nil, nil, "AgentInfoFailed", err
	}
	if adopted != nil {
		return adopted, nil, "", nil
	}
	vkey, err := kesagent.GenStagedKey(ctx, cfg)
	if err != nil {
		return nil, nil, "AgentGenStagedKeyFailed", err
	}
	oc, coldVkey, err := issueOpCert(ctx, issuer, vkey, chainCounter, kesPeriod)
	if err != nil {
		return nil, nil, "IssueFailed", err
	}
	envelope, err := opcert.Encode(oc, coldVkey)
	if err != nil {
		return nil, nil, "IssueFailed", err
	}
	raw, err := opcert.EncodeCBOR(oc, coldVkey)
	if err != nil {
		return nil, nil, "IssueFailed", err
	}
	candidate = secret.DeepCopy()
	candidate.Data[certFile] = envelope
	if len(candidate.Data[keyFileNodeCert]) > 0 {
		candidate.Data[keyFileNodeCert] = envelope
	}
	install = func() error {
		return kesagent.InstallKey(ctx, cfg, raw, vkey, oc.IssueNumber)
	}
	return candidate, install, "", nil
}

// adoptServedOpCert returns the keys Secret carrying the opcert the agent
// serves, when that opcert is the next one for the pool: its issue number is
// chainCounter+1 and the cold key the Secret's opcert names signed it for the
// key it commits to. It returns nil, nil when the agent serves anything else,
// so the caller rotates as usual, and an error only when info fails. An opcert
// with no more than renewBefore periods left at kesPeriod is not adopted
// either: it would stay due, so a fresh one is issued instead. The agent is
// only read.
func (r *DingoNodeReconciler) adoptServedOpCert(
	ctx context.Context,
	cfg kesagent.Config,
	secret *corev1.Secret,
	certFile string,
	chainCounter, kesPeriod uint64,
	maxEvolutions, renewBefore int64,
) (*corev1.Secret, error) {
	active, err := kesagent.ActiveKey(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if active.IssueNumber != chainCounter+1 {
		return nil, nil
	}
	oc, candidate, err := servedOpCertSecret(active, secret, certFile)
	if oc == nil || err != nil {
		return nil, err
	}
	start, err := certField(oc.KesPeriod, "kes period")
	if err != nil {
		return nil, nil //nolint:nilerr // an unreadable opcert is not adopted
	}
	if start+maxEvolutions-int64(kesPeriod) <= renewBefore { //nolint:gosec
		return nil, nil
	}
	return candidate, nil
}

// servedOpCertSecret returns the opcert the agent serves and the keys Secret
// carrying it, when the cold key the Secret's opcert names signed it for the
// key the agent commits to. It returns nil, nil, nil for any opcert that does
// not verify, and an error only when encoding fails.
func servedOpCertSecret(
	active *kesagent.Active,
	secret *corev1.Secret,
	certFile string,
) (*ledger.OpCert, *corev1.Secret, error) {
	oc, coldVkey, err := opcert.ParseCBOR(active.OpCert)
	if err != nil {
		return nil, nil, nil //nolint:nilerr // an unreadable opcert is not adopted
	}
	_, known, err := opcert.Parse(secret.Data[certFile])
	if err != nil {
		return nil, nil, nil //nolint:nilerr // autoRotate parsed it already
	}
	sigErr := ledger.VerifyOpCertSignature(oc, coldVkey)
	if sigErr != nil || !bytes.Equal(coldVkey, known) ||
		oc.IssueNumber != active.IssueNumber ||
		!bytes.Equal(oc.KesVkey, active.KESVKey) {
		return nil, nil, nil //nolint:nilerr // an unverifiable opcert is not adopted
	}
	// The served bytes, not a re-encoding: Dingo admits the agent only when
	// its opcert is byte-equal to the key file's.
	envelope, err := opcert.EncodeEnvelope(active.OpCert)
	if err != nil {
		return nil, nil, err
	}
	candidate := secret.DeepCopy()
	candidate.Data[certFile] = envelope
	if len(candidate.Data[keyFileNodeCert]) > 0 {
		candidate.Data[keyFileNodeCert] = envelope
	}
	return oc, candidate, nil
}

// adoptPendingInstall writes the opcert the agent serves into the keys Secret
// when the node reports forging with a later counter than the Secret holds,
// which is what a rotation leaves when install-key succeeded and the Secret
// write did not. A restart would otherwise load the Secret's older opcert. The
// served opcert must be the one the node reports, and the next for the pool or
// already on chain. It reports whether it wrote the Secret.
//
// Nothing here is due, so a node with nothing pending, or whose state is not
// yet known, is left without a condition and the agent is not asked.
func (r *DingoNodeReconciler) adoptPendingInstall(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) bool {
	bp := dn.Spec.BlockProducer
	loaded := dn.Status.OpCert.LoadedCounter
	if bp.SigningAgent == nil || loaded == nil || *loaded < 0 ||
		r.APIReader == nil {
		return false
	}
	chain, ok := observedOnChainCounter(dn, time.Now())
	if !ok {
		return false
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: bp.Keys.SecretRef, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		return false
	}
	certData, certFile, err := opCertFile(secret.Data)
	if err != nil || certData == nil {
		return false
	}
	staged, _, err := opcert.Parse(certData)
	if err != nil || uint64(*loaded) <= staged.IssueNumber { //nolint:gosec
		return false
	}

	// The node runs a later opcert than the Secret holds: from here on the
	// repair is pending, and a failure is reported and retried after
	// issueRetryInterval, sharing the backoff of a rotation attempt.
	nodeKey := client.ObjectKeyFromObject(dn)
	if last, ok := r.lastIssueFailure(nodeKey); ok &&
		time.Since(last.at) < issueRetryInterval {
		setIssueCondition(
			dn, metav1.ConditionFalse, last.reason, last.message,
		)
		return false
	}
	fail := func(reason, message string) {
		r.recordIssueFailure(nodeKey, reason, message)
		r.refuseIssue(ctx, dn, reason, message)
	}
	cfg, problem, err := r.agentControlConfig(ctx, dn)
	if err != nil {
		fail("ControlCertUnavailable", err.Error())
		return false
	}
	if problem != "" {
		fail("ControlCertInvalid", problem)
		return false
	}
	active, err := kesagent.ActiveKey(ctx, cfg)
	if err != nil {
		fail("AgentInfoFailed", err.Error())
		return false
	}
	// Both are non-negative: checked above and by observedOnChainCounter.
	if active.IssueNumber != uint64(*loaded) || //nolint:gosec
		active.IssueNumber < uint64(chain) || //nolint:gosec
		active.IssueNumber > uint64(chain)+1 { //nolint:gosec
		fail(
			"AgentServesUnexpectedOpCert",
			fmt.Sprintf(
				"the node reports counter %d, the agent serves %d and the "+
					"chain holds %d",
				*loaded, active.IssueNumber, chain,
			),
		)
		return false
	}
	_, candidate, err := servedOpCertSecret(active, secret, certFile)
	if err != nil {
		fail("AgentServesUnexpectedOpCert", err.Error())
		return false
	}
	if candidate == nil {
		fail(
			"AgentServesUnexpectedOpCert",
			"the opcert the agent serves does not verify",
		)
		return false
	}
	state, err := validateKeysSecret(candidate, dn)
	if err != nil {
		fail("IssuedBundleRejected", err.Error())
		return false
	}
	// The update carries the resourceVersion read above, so a concurrent edit
	// of the Secret fails the write instead of being overwritten.
	if err := r.Update(ctx, candidate); err != nil {
		fail("SecretUpdateFailed", err.Error())
		return false
	}

	r.forgetIssueFailure(nodeKey)
	msg := fmt.Sprintf(
		"adopted the opcert the agent serves, counter %d, kes start period %d",
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
	return true
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
			Profile:     cs.Profile,
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
