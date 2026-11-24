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
	"slices"
	"strings"
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
	"k8s.io/client-go/util/retry"
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

// keylessProbeInterval is the least time between two questions to the signing
// agent about whether its profile holds a key. A producer without a key cannot
// forge, so this is much shorter than issueRetryInterval; one short session a
// minute is cheap.
const keylessProbeInterval = time.Minute

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

// issueOutcome is what one rotation attempt reports: an issued opcert or the
// reason nothing was issued.
type issueOutcome struct {
	issued          bool
	reason, message string
	// replayed marks a refusal repeated from the backoff after an earlier
	// failure, which is shown again but not logged or evented again.
	replayed bool
}

func refused(reason, message string) *issueOutcome {
	return &issueOutcome{reason: reason, message: message}
}

// rotationTarget is the one key a rotation attempt issues for: the node's own
// pool, or one pool of spec.blockProducer.pools.
type rotationTarget struct {
	// pool is the pool's hex ID; empty for the node's single pool.
	pool string
	// secretRef names the keys Secret that is read and written.
	secretRef string
	// profile is the signing agent profile that holds the KES key; empty
	// selects the agent's only profile.
	profile string
	// kes is the KES state the node reports for the pool.
	kes dingov1alpha1.KESStatus
	// chain is the pool's on-chain opcert counter, valid when chainKnown.
	chain      int64
	chainKnown bool
	// validate checks a candidate keys Secret as the reconcile will.
	validate func(*corev1.Secret) (*keyState, error)
	// failKey keys the backoff after a failed attempt.
	failKey types.NamespacedName
	// signerFor is the node the cold signer is built for. A pool list sets
	// the cold key hash to the pool's own.
	signerFor *dingov1alpha1.DingoNode
	// rekey is set when the signing agent holds no key for the profile: a
	// valid opcert in the keys Secret then does not make the rotation wait.
	rekey bool
	// recordInstall, when set, durably records the counter of the opcert
	// the agent is about to serve, before the agent is changed.
	recordInstall func(counter int64) error
}

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
//
// A node signing through an agent is also rotated when it is not due but the
// agent holds no key for its profile (see agentKeyless).
func (r *DingoNodeReconciler) autoRotate(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.Rotation.Mode != dingov1alpha1.RotationModeAuto {
		return
	}
	if len(bp.Pools) > 0 {
		r.autoRotatePools(ctx, dn)
		return
	}
	floor, known := observedOnChainCounter(dn, time.Now())
	t := rotationTarget{
		secretRef:  bp.Keys.SecretRef,
		kes:        dn.Status.KES,
		chain:      floor,
		chainKnown: known,
		validate: func(s *corev1.Secret) (*keyState, error) {
			return validateKeysSecret(s, dn)
		},
		failKey:   client.ObjectKeyFromObject(dn),
		signerFor: dn,
	}
	if bp.SigningAgent != nil {
		t.profile = bp.SigningAgent.ControlProfile
	}
	// A node that reloads in place takes the agent's new opcert and stops
	// reporting the rotation as due, so a lost Secret write is repaired here
	// rather than behind RotationDue.
	if o := r.adoptPendingInstall(
		ctx, dn, t, dn.Status.OpCert.LoadedCounter,
	); o != nil {
		r.reportIssue(ctx, dn, "", o)
		setIssueCondition(dn, outcomeStatus(o), o.reason, o.message)
		if o.issued {
			return
		}
	}
	if !meta.IsStatusConditionTrue(dn.Status.Conditions, condRotationDue) {
		if !r.agentKeyless(ctx, dn, t) {
			return
		}
		t.rekey = true
	}
	o := r.rotateKey(ctx, dn, t)
	if o == nil {
		return
	}
	r.reportIssue(ctx, dn, "", o)
	setIssueCondition(dn, outcomeStatus(o), o.reason, o.message)
}

// agentKeyless reports whether the signing agent holds no key for t's profile.
// An agent that keeps keys in memory only loses them on restart, and Bursa
// refuses sign sessions on a keyless profile, so the producer cannot forge
// until a key is issued; the node's KES state does not show it. The agent is
// asked at most once per keylessProbeInterval, and not while a failed attempt
// is backing off, or when the node's KES period or the on-chain counter is
// unknown, since a new opcert starts at the former and follows the latter and
// the agent's info carries neither. A failed info is not a keyless profile.
func (r *DingoNodeReconciler) agentKeyless(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	t rotationTarget,
) bool {
	if dn.Spec.BlockProducer.SigningAgent == nil || !t.chainKnown ||
		t.kes.CurrentPeriod <= 0 || t.kes.ExpiryPeriod <= t.kes.OpCertKESPeriod {
		return false
	}
	if last, ok := r.lastIssueFailure(t.failKey); ok &&
		time.Since(last.at) < issueRetryInterval {
		return false
	}
	if !r.probeDue(t.failKey) {
		return false
	}
	logger := log.FromContext(ctx)
	cfg, problem, err := r.agentControlConfig(ctx, dn, t.profile)
	if err == nil && problem != "" {
		err = errors.New(problem)
	}
	if err != nil {
		logger.V(1).Info(
			"cannot ask the signing agent for its key", "error", err,
		)
		return false
	}
	state, err := kesagent.KeyState(ctx, cfg)
	if err != nil {
		logger.V(1).Info("signing agent key state unavailable", "error", err)
		return false
	}
	return state.Active == nil
}

// markProbed records that key's agent holds a key it was just given, so it is
// not asked again within keylessProbeInterval.
func (r *DingoNodeReconciler) markProbed(key types.NamespacedName) {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	if r.keylessProbes == nil {
		r.keylessProbes = make(map[types.NamespacedName]time.Time)
	}
	r.keylessProbes[key] = time.Now()
}

// probeDue reports whether key's agent may be asked now, and records that it
// is.
func (r *DingoNodeReconciler) probeDue(key types.NamespacedName) bool {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	if at, ok := r.keylessProbes[key]; ok &&
		time.Since(at) < keylessProbeInterval {
		return false
	}
	if r.keylessProbes == nil {
		r.keylessProbes = make(map[types.NamespacedName]time.Time)
	}
	r.keylessProbes[key] = time.Now()
	return true
}

// autoRotatePools rotates each pool of spec.blockProducer.pools that is due on
// its own KES state, through the signing agent with the pool's agentProfile,
// writing only that pool's keys Secret. A pool that is not due is rotated only
// when the agent holds no key for its profile (see agentKeyless), and only
// while the node reports its KES state; otherwise its Secret is only read to
// find a lost write. One pool's refusal does not hold back another. The
// outcome of every attempted pool is reported in the OpCertIssued condition
// and as an Event naming the pool.
func (r *DingoNodeReconciler) autoRotatePools(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	node := client.ObjectKeyFromObject(dn)
	renewBefore := int64(bp.Rotation.RenewBeforePeriods)

	pools := slices.Clone(bp.Pools)
	slices.SortFunc(pools, func(a, b dingov1alpha1.BlockProducerPool) int {
		return strings.Compare(
			normalizedPoolID(a.PoolID), normalizedPoolID(b.PoolID),
		)
	})
	type poolOutcome struct {
		id string
		*issueOutcome
	}
	var outcomes []poolOutcome
	for _, p := range pools {
		id := normalizedPoolID(p.PoolID)
		ps := observedPoolStatus(dn, id)
		if ps == nil {
			continue
		}
		signer := *dn
		signerSpec := *bp
		signerSpec.Rotation.ColdSigner.ColdKeyHash = id
		signer.Spec.BlockProducer = &signerSpec
		t := rotationTarget{
			pool:      id,
			secretRef: p.Keys.SecretRef,
			profile:   p.AgentProfile,
			kes:       ps.KES,
			validate: func(s *corev1.Secret) (*keyState, error) {
				return validateBundle(s, poolChecks(dn, p, r.now()))
			},
			failKey: types.NamespacedName{
				Name: dn.Name + "/" + id, Namespace: dn.Namespace,
			},
			signerFor: &signer,
			recordInstall: func(counter int64) error {
				return r.recordPendingInstall(ctx, dn, id, counter)
			},
		}
		// Judged per pool: the agent and signer calls of an earlier pool
		// can age the reading past onChainCounterMaxAge.
		if ps.OnChainCounterAt != nil && ps.OnChainCounter >= 0 &&
			r.now().Sub(ps.OnChainCounterAt.Time) <= onChainCounterMaxAge {
			t.chain, t.chainKnown = ps.OnChainCounter, true
		}
		// As for one pool, a lost Secret write is repaired whether or not
		// the pool is still due. A failed repair holds the pool's backoff,
		// so it is not followed by a rotation attempt.
		o := r.adoptPendingInstall(ctx, dn, t, ps.LoadedCounter)
		if o == nil {
			// A pool the node did not report this reconcile has only the
			// KES status it last had, which is neither rotated from nor
			// used to issue a first key. The node loads its credentials
			// from the opcert, not from the agent's key, so a running pod
			// reports every pool whether or not its profile holds a key.
			if !r.kesObservedFor(node, id) || ps.KES.CurrentPeriod <= 0 {
				continue
			}
			if renewBefore <= 0 || ps.KES.RemainingPeriods > renewBefore {
				if !r.agentKeyless(ctx, dn, t) {
					continue
				}
				t.rekey = true
			}
			o = r.rotateKey(ctx, dn, t)
		}
		if o == nil {
			continue
		}
		r.reportIssue(ctx, dn, id, o)
		outcomes = append(outcomes, poolOutcome{id, o})
	}
	if len(outcomes) == 0 {
		return
	}
	status, reason := metav1.ConditionTrue, "OpCertIssued"
	parts := make([]string, 0, len(outcomes))
	for _, o := range outcomes {
		if status == metav1.ConditionTrue && !o.issued {
			status, reason = metav1.ConditionFalse, o.reason
		}
		parts = append(parts, fmt.Sprintf("pool %s: %s", o.id, o.message))
	}
	setIssueCondition(dn, status, reason, joinPoolMessages(parts))
}

// recordPendingInstall writes counter as pool's pendingInstallCounter to the
// stored node's status, then to dn. Only that field is patched, so the status
// the reconcile has built so far is written by its own status update.
func (r *DingoNodeReconciler) recordPendingInstall(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	pool string,
	counter int64,
) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dingov1alpha1.DingoNode{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(dn), latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		poolStatus(latest, pool).PendingInstallCounter = counter
		return r.Status().Patch(ctx, latest, client.MergeFromWithOptions(
			base, client.MergeFromWithOptimisticLock{},
		))
	})
	if err != nil {
		return fmt.Errorf("record pending install: %w", err)
	}
	poolStatus(dn, pool).PendingInstallCounter = counter
	return nil
}

// maxConditionMessage is the CRD's bound on a condition message. A longer one
// fails the whole status update.
const maxConditionMessage = 32768

// joinPoolMessages joins per-pool messages for a condition within
// maxConditionMessage. Trailing messages that do not fit are left out and
// counted; each pool's Event carries its own.
func joinPoolMessages(parts []string) string {
	const sep = "; "
	omitted := func(n int) string {
		if n == 0 {
			return ""
		}
		return fmt.Sprintf("%s%d more left out, see the pool Events", sep, n)
	}
	var out string
	for i, p := range parts {
		next := p
		if i > 0 {
			next = out + sep + p
		}
		// The room kept for the count of what follows p is what the count
		// needs if the next message is left out.
		if len(next)+len(omitted(len(parts)-i-1)) <= maxConditionMessage {
			out = next
			continue
		}
		if i == 0 {
			room := maxConditionMessage - len(omitted(len(parts)-1))
			return strings.ToValidUTF8(p[:room], "") + omitted(len(parts)-1)
		}
		return out + omitted(len(parts)-i)
	}
	return out
}

// setKESObserved records the pools whose KES state the node reported in this
// reconcile; nil forgets the node.
func (r *DingoNodeReconciler) setKESObserved(
	node types.NamespacedName,
	pools map[string]bool,
) {
	r.kesMu.Lock()
	defer r.kesMu.Unlock()
	if pools == nil {
		delete(r.kesObserved, node)
		return
	}
	if r.kesObserved == nil {
		r.kesObserved = make(map[types.NamespacedName]map[string]bool)
	}
	r.kesObserved[node] = pools
}

// kesObservedFor reports whether the node reported pool's KES state in this
// reconcile.
func (r *DingoNodeReconciler) kesObservedFor(
	node types.NamespacedName,
	pool string,
) bool {
	r.kesMu.Lock()
	defer r.kesMu.Unlock()
	return r.kesObserved[node][pool]
}

// observedPoolStatus returns the status entry of a listed pool, or nil when
// none has been recorded for it yet.
func observedPoolStatus(
	dn *dingov1alpha1.DingoNode,
	id string,
) *dingov1alpha1.PoolStatus {
	for i := range dn.Status.Pools {
		if dn.Status.Pools[i].PoolID == id {
			return &dn.Status.Pools[i]
		}
	}
	return nil
}

func outcomeStatus(o *issueOutcome) metav1.ConditionStatus {
	if o.issued {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// reportIssue logs an attempt and records it as an Event, naming the pool of a
// pool list. The OpCertIssued condition is the caller's.
func (r *DingoNodeReconciler) reportIssue(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	pool string,
	o *issueOutcome,
) {
	if o.replayed {
		return
	}
	logger := log.FromContext(ctx)
	if pool != "" {
		logger = logger.WithValues("pool", pool)
	}
	msg := o.message
	if pool != "" {
		msg = fmt.Sprintf("pool %s: %s", pool, msg)
	}
	eventType, reason := corev1.EventTypeWarning, o.reason
	if o.issued {
		eventType, reason = corev1.EventTypeNormal, "OpCertIssued"
		logger.Info(o.message)
	} else {
		logger.Info(
			"auto rotation did not issue",
			"reason", o.reason, "error", o.message,
		)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn, nil, eventType, reason, "RotateKeys", "%s", msg,
		)
	}
}

// rotateKey issues a new opcert for one key when it is due, or t.rekey is set,
// and nothing is staged for it, and writes the keys Secret. It returns nil
// when there was nothing to report: a rotation already staged is left to
// finish.
func (r *DingoNodeReconciler) rotateKey(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	t rotationTarget,
) *issueOutcome {
	bp := dn.Spec.BlockProducer
	if !t.chainKnown {
		return refused(
			"OnChainCounterUnknown",
			"no fresh on-chain opcert counter for this pool",
		)
	}
	floor := t.chain
	st := t.kes
	if st.CurrentPeriod <= 0 || st.ExpiryPeriod <= st.OpCertKESPeriod {
		return refused(
			"KESPeriodUnknown", "the node's KES period has not been scraped",
		)
	}
	maxEvolutions := st.ExpiryPeriod - st.OpCertKESPeriod
	renewBefore := int64(bp.Rotation.RenewBeforePeriods)
	if renewBefore >= maxEvolutions {
		return refused(
			"RenewWindowTooLong",
			fmt.Sprintf(
				"renewBeforePeriods %d is not below the %d-period KES "+
					"window, so every new opcert would be due at once",
				renewBefore, maxEvolutions,
			),
		)
	}

	if r.APIReader == nil {
		return refused("KeysSecretUnavailable", "no reader for the keys Secret")
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: t.secretRef, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		return refused("KeysSecretUnavailable", err.Error())
	}
	certData, certFile, err := opCertFile(secret.Data)
	if err == nil && certData == nil {
		err = errors.New("keys secret holds no opcert")
	}
	if err != nil {
		return refused("KeysSecretInvalid", err.Error())
	}
	staged, _, err := opcert.Parse(certData)
	if err != nil {
		return refused("KeysSecretInvalid", err.Error())
	}
	stagedStart, err := certField(staged.KesPeriod, "kes period")
	if err != nil {
		return refused("KeysSecretInvalid", err.Error())
	}
	// A staged bundle that does not validate can never be rolled out, so it
	// is replaced rather than waited on.
	// An agent that restarted after install-key holds no key for the staged
	// opcert, so it is asked before the rotation is left to finish.
	if _, verr := t.validate(secret); verr == nil && !t.rekey &&
		stagedStart+maxEvolutions-st.CurrentPeriod > renewBefore {
		if !r.agentKeyless(ctx, dn, t) {
			return nil
		}
		t.rekey = true
	}

	if last, ok := r.lastIssueFailure(t.failKey); ok &&
		time.Since(last.at) < issueRetryInterval {
		return &issueOutcome{
			reason: last.reason, message: last.message, replayed: true,
		}
	}
	fail := func(reason, message string) *issueOutcome {
		r.recordIssueFailure(t.failKey, reason, message)
		return refused(reason, message)
	}

	// Check the write is allowed before a signature is spent on it.
	if err := r.Update(ctx, secret.DeepCopy(), client.DryRunAll); err != nil {
		return fail("SecretUpdateFailed", err.Error())
	}
	issuer, err := r.issuerFor(ctx, t.signerFor)
	if err != nil {
		return fail("ColdSignerUnavailable", err.Error())
	}
	// Both are positive: the counter is known and the period check above.
	chainCounter := uint64(floor)         //nolint:gosec
	kesPeriod := uint64(st.CurrentPeriod) //nolint:gosec
	var (
		candidate *corev1.Secret
		install   func() error
	)
	if bp.SigningAgent != nil {
		var reason string
		candidate, install, reason, err = r.issueThroughAgent(
			ctx, dn, t.profile, issuer, secret, certFile, chainCounter,
			kesPeriod, maxEvolutions, renewBefore,
		)
		if err != nil {
			return fail(reason, err.Error())
		}
	} else {
		var kesEnv []byte
		if bp.Rotation.ColdSigner.Type == dingov1alpha1.ColdSignerBursa {
			kesEnv = secret.Data[keyFilePendingKES]
		}
		var vkey []byte
		if len(kesEnv) == 0 {
			kesEnv, vkey, err = generateKESKey()
			if err != nil {
				return fail("PendingKESKeyFailed", err.Error())
			}
			if bp.Rotation.ColdSigner.Type == dingov1alpha1.ColdSignerBursa {
				pending := secret.DeepCopy()
				pending.Data[keyFilePendingKES] = kesEnv
				if err := r.Update(ctx, pending); err != nil {
					return fail("SecretUpdateFailed", err.Error())
				}
				secret = pending
			}
		} else {
			vkey, err = pendingKESVKey(kesEnv)
		}
		if err != nil {
			return fail("PendingKESKeyFailed", err.Error())
		}
		cert, err := issueBundle(
			ctx, issuer, vkey, chainCounter, kesPeriod,
		)
		if err != nil {
			return fail("IssueFailed", err.Error())
		}
		candidate = secret.DeepCopy()
		candidate.Data[keyFileKES] = kesEnv
		delete(candidate.Data, keyFilePendingKES)
		candidate.Data[certFile] = cert
		if len(candidate.Data[keyFileNodeCert]) > 0 {
			// opCertFile accepted both names only because they were identical.
			candidate.Data[keyFileNodeCert] = cert
		}
	}
	state, err := t.validate(candidate)
	if err != nil {
		return fail("IssuedBundleRejected", err.Error())
	}
	// The next reconcile applies resources before it scrapes the node, so
	// only this record holds a pool whose Secret write below fails. It is
	// written before the agent changes: a restart after the install would
	// otherwise leave no evidence and drop the pool.
	if bp.SigningAgent != nil && t.recordInstall != nil {
		if err := t.recordInstall(state.Counter); err != nil {
			return fail("StatusUpdateFailed", err.Error())
		}
	}
	// install promotes the new key in the agent, so the write below is the
	// only step left that can fail. If it does, the agent serves the new
	// opcert while the Secret holds the old one, and the next attempt
	// publishes the opcert the agent serves instead of issuing another.
	if install != nil {
		if err := install(); err != nil {
			return fail("AgentInstallFailed", err.Error())
		}
	}
	// The update carries the resourceVersion read above, so a concurrent edit
	// of the Secret fails the write instead of being overwritten.
	if err := r.Update(ctx, candidate); err != nil {
		return fail("SecretUpdateFailed", err.Error())
	}

	r.forgetIssueFailure(t.failKey)
	r.markProbed(t.failKey)
	return &issueOutcome{
		issued: true,
		reason: "OpCertIssued",
		message: fmt.Sprintf(
			"issued opcert counter %d, kes start period %d",
			state.Counter, state.KESPeriod,
		),
	}
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
		return nil, nil, fmt.Errorf(
			"sign opcert for KES verification key %x: %w", kesVkey, err,
		)
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

// generateKESKey returns a new KES signing-key envelope and its verification
// key.
func generateKESKey() ([]byte, []byte, error) {
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
	return kesEnv, vkey, nil
}

// pendingKESVKey validates a saved pending KES key and derives its public key.
func pendingKESVKey(kesEnv []byte) ([]byte, error) {
	raw, err := checkKeyEnvelope(
		kesEnv, keyFilePendingKES, kesEnvelopeTypePrefix,
	)
	if err != nil {
		return nil, err
	}
	if len(raw) != kes.CardanoKesSecretKeySize {
		return nil, fmt.Errorf(
			"%s holds %d bytes, expected %d", keyFilePendingKES,
			len(raw), kes.CardanoKesSecretKeySize,
		)
	}
	sk := &kes.SecretKey{Depth: kes.CardanoKesDepth, Data: raw}
	return append([]byte(nil), kes.PublicKey(sk)...), nil
}

// issueBundle has issuer sign an opcert for vkey and returns its envelope.
func issueBundle(
	ctx context.Context,
	issuer coldsigner.Issuer,
	vkey []byte,
	chainCounter, kesPeriod uint64,
) ([]byte, error) {
	oc, coldVkey, err := issueOpCert(
		ctx, issuer, vkey, chainCounter, kesPeriod,
	)
	if err != nil {
		return nil, err
	}
	cert, err := opcert.Encode(oc, coldVkey)
	if err != nil {
		return nil, err
	}
	return cert, nil
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
	profile string,
	issuer coldsigner.Issuer,
	secret *corev1.Secret,
	certFile string,
	chainCounter, kesPeriod uint64,
	maxEvolutions, renewBefore int64,
) (candidate *corev1.Secret, install func() error, reason string, err error) {
	cfg, problem, err := r.agentControlConfig(ctx, dn, profile)
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
	state, err := kesagent.KeyState(ctx, cfg)
	if err != nil {
		return nil, nil, "AgentInfoFailed", err
	}
	if state.Active != nil {
		adopted, err := r.adoptServedOpCert(
			state.Active, secret, certFile, chainCounter,
			kesPeriod, maxEvolutions, renewBefore,
		)
		if err != nil {
			return nil, nil, "AgentInfoFailed", err
		}
		if adopted != nil {
			return adopted, nil, "", nil
		}
	}
	var vkey []byte
	if dn.Spec.BlockProducer.Rotation.ColdSigner.Type ==
		dingov1alpha1.ColdSignerBursa {
		vkey = state.StagedKESVKey
	}
	if len(vkey) == 0 {
		vkey, err = kesagent.GenStagedKey(ctx, cfg)
		if err != nil {
			return nil, nil, "AgentGenStagedKeyFailed", err
		}
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
	active *kesagent.Active,
	secret *corev1.Secret,
	certFile string,
	chainCounter, kesPeriod uint64,
	maxEvolutions, renewBefore int64,
) (*corev1.Secret, error) {
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

// installPending reports whether an Auto-rotation node signing through an
// agent forges with a later opcert (loaded) than the keys Secret holds: what a
// rotation leaves when install-key succeeded and the Secret write did not.
func installPending(
	dn *dingov1alpha1.DingoNode,
	secret *corev1.Secret,
	loaded *int64,
) bool {
	bp := dn.Spec.BlockProducer
	if bp == nil || bp.Rotation.Mode != dingov1alpha1.RotationModeAuto ||
		bp.SigningAgent == nil || loaded == nil || *loaded < 0 {
		return false
	}
	certData, _, err := opCertFile(secret.Data)
	if err != nil || certData == nil {
		return false
	}
	staged, _, err := opcert.Parse(certData)
	if err != nil || staged == nil {
		return false
	}
	return uint64(*loaded) > staged.IssueNumber //nolint:gosec
}

// adoptPendingInstall writes the opcert the agent serves into t's keys Secret
// when the node reports forging with a later counter (loaded) than the Secret
// holds, which is what a rotation leaves when install-key succeeded and the
// Secret write did not. A restart would otherwise load the Secret's older
// opcert. The served opcert must be the one the node reports, and the next for
// the pool or already on chain. It returns nil when nothing is pending, so a
// node or pool whose state is not yet known is left without a condition and
// the agent is not asked.
func (r *DingoNodeReconciler) adoptPendingInstall(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	t rotationTarget,
	loaded *int64,
) *issueOutcome {
	if dn.Spec.BlockProducer.SigningAgent == nil || loaded == nil ||
		r.APIReader == nil || !t.chainKnown {
		return nil
	}
	chain := t.chain
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: t.secretRef, Namespace: dn.Namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		return nil
	}
	if !installPending(dn, secret, loaded) {
		return nil
	}
	_, certFile, _ := opCertFile(secret.Data)

	// The node runs a later opcert than the Secret holds: from here on the
	// repair is pending, and a failure is reported and retried after
	// issueRetryInterval, sharing the backoff of a rotation attempt.
	if last, ok := r.lastIssueFailure(t.failKey); ok &&
		time.Since(last.at) < issueRetryInterval {
		return &issueOutcome{
			reason: last.reason, message: last.message, replayed: true,
		}
	}
	fail := func(reason, message string) *issueOutcome {
		r.recordIssueFailure(t.failKey, reason, message)
		return refused(reason, message)
	}
	cfg, problem, err := r.agentControlConfig(ctx, dn, t.profile)
	if err != nil {
		return fail("ControlCertUnavailable", err.Error())
	}
	if problem != "" {
		return fail("ControlCertInvalid", problem)
	}
	active, err := kesagent.ActiveKey(ctx, cfg)
	if err != nil {
		return fail("AgentInfoFailed", err.Error())
	}
	// Both are non-negative: checked above and by the counter's source.
	if active.IssueNumber != uint64(*loaded) || //nolint:gosec
		active.IssueNumber < uint64(chain) || //nolint:gosec
		active.IssueNumber > uint64(chain)+1 { //nolint:gosec
		return fail(
			"AgentServesUnexpectedOpCert",
			fmt.Sprintf(
				"the node reports counter %d, the agent serves %d and the "+
					"chain holds %d",
				*loaded, active.IssueNumber, chain,
			),
		)
	}
	_, candidate, err := servedOpCertSecret(active, secret, certFile)
	if err != nil {
		return fail("AgentServesUnexpectedOpCert", err.Error())
	}
	if candidate == nil {
		return fail(
			"AgentServesUnexpectedOpCert",
			"the opcert the agent serves does not verify",
		)
	}
	state, err := t.validate(candidate)
	if err != nil {
		return fail("IssuedBundleRejected", err.Error())
	}
	// The update carries the resourceVersion read above, so a concurrent edit
	// of the Secret fails the write instead of being overwritten.
	if err := r.Update(ctx, candidate); err != nil {
		return fail("SecretUpdateFailed", err.Error())
	}

	r.forgetIssueFailure(t.failKey)
	return &issueOutcome{
		issued: true,
		reason: "OpCertIssued",
		message: fmt.Sprintf(
			"adopted the opcert the agent serves, counter %d, kes start "+
				"period %d",
			state.Counter, state.KESPeriod,
		),
	}
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

// forgetNodeIssueFailures drops the remembered failure of a node and of each
// of its pools, which are keyed "<node>/<pool id>": no object name holds a
// slash, so no other node's key matches.
func (r *DingoNodeReconciler) forgetNodeIssueFailures(
	node types.NamespacedName,
) {
	r.issueMu.Lock()
	defer r.issueMu.Unlock()
	delete(r.issueFailures, node)
	delete(r.keylessProbes, node)
	prefix := node.Name + "/"
	for k := range r.issueFailures {
		if k.Namespace == node.Namespace && strings.HasPrefix(k.Name, prefix) {
			delete(r.issueFailures, k)
		}
	}
	for k := range r.keylessProbes {
		if k.Namespace == node.Namespace && strings.HasPrefix(k.Name, prefix) {
			delete(r.keylessProbes, k)
		}
	}
}
