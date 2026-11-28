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
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Reasons a pool's keys are reported with.
const (
	reasonPoolKeysRejected = "PoolKeysRejected"
	reasonPoolKeysAccepted = "OpCertAccepted"
	reasonPoolSecretAbsent = "KeysSecretMissing"
)

// poolBundle is one pool of spec.blockProducer.pools and the verdict on its
// key bundle.
type poolBundle struct {
	spec dingov1alpha1.BlockProducerPool
	// id is the pool's hex cold-key hash.
	id string
	// secret is the keys Secret; nil when it does not exist.
	secret *corev1.Secret
	// state is what validation learned from an accepted bundle.
	state *keyState
	// problem says why the bundle was refused; nil for an accepted one.
	problem error
	// reason is the condition reason of problem.
	reason string
}

func (b *poolBundle) accepted() bool { return b.problem == nil }

// poolIDs returns the normalized hex ID of every pool of the list, in spec
// order. An ID that does not parse is validateSpec's to refuse.
func poolIDs(bp *dingov1alpha1.BlockProducerSpec) []string {
	ids := make([]string, 0, len(bp.Pools))
	for _, p := range bp.Pools {
		ids = append(ids, normalizedPoolID(p.PoolID))
	}
	return ids
}

// poolStatus returns the status entry of a pool, creating it when absent.
func poolStatus(
	dn *dingov1alpha1.DingoNode,
	id string,
) *dingov1alpha1.PoolStatus {
	for i := range dn.Status.Pools {
		if dn.Status.Pools[i].PoolID == id {
			return &dn.Status.Pools[i]
		}
	}
	dn.Status.Pools = append(
		dn.Status.Pools, dingov1alpha1.PoolStatus{PoolID: id},
	)
	return &dn.Status.Pools[len(dn.Status.Pools)-1]
}

// syncPoolStatuses makes status.pools hold exactly the listed pools, in pool ID
// order, keeping what is already observed for each.
func syncPoolStatuses(dn *dingov1alpha1.DingoNode) {
	bp := dn.Spec.BlockProducer
	if bp == nil || len(bp.Pools) == 0 {
		dn.Status.Pools = nil
		return
	}
	ids := poolIDs(bp)
	slices.Sort(ids)
	synced := make([]dingov1alpha1.PoolStatus, 0, len(ids))
	for _, id := range ids {
		synced = append(synced, *poolStatus(dn, id))
	}
	dn.Status.Pools = synced
}

// poolChecks returns what a pool's bundle is validated against: the pool's own
// counters, and the KES period the node last reported (nodeKESPeriod).
func poolChecks(
	dn *dingov1alpha1.DingoNode,
	p dingov1alpha1.BlockProducerPool,
	now time.Time,
) bundleChecks {
	bp := dn.Spec.BlockProducer
	ps := poolStatus(dn, normalizedPoolID(p.PoolID))
	var floor int64
	if ps.OnChainCounterAt != nil && ps.OnChainCounter > 0 &&
		now.Sub(ps.OnChainCounterAt.Time) <= onChainCounterMaxAge {
		floor = ps.OnChainCounter
	}
	return bundleChecks{
		poolID:           p.PoolID,
		poolIDField:      "spec.blockProducer.pools[].poolId",
		agent:            true,
		onChainFloor:     floor,
		onDisk:           ps.OnDiskCounter,
		kesPeriod:        nodeKESPeriod(dn),
		maxKESEvolutions: bp.MaxKESEvolutions,
	}
}

// nodeKESPeriod is the current KES period the node last reported for any of
// its pools. The period follows from the slot, so it is the same for every
// pool: one the node has not reported on yet, or a pool left out of the node,
// is judged by the others'. 0 means none has been reported.
func nodeKESPeriod(dn *dingov1alpha1.DingoNode) int64 {
	cur := dn.Status.KES.CurrentPeriod
	for _, ps := range dn.Status.Pools {
		cur = max(cur, ps.KES.CurrentPeriod)
	}
	return cur
}

// evaluatePools reads and validates each pool's keys Secret. A pool whose
// bundle is missing or fails validation is reported in its poolBundle, not as
// an error; an error is a failed read, which is neither acceptance nor refusal.
func (r *DingoNodeReconciler) evaluatePools(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) ([]*poolBundle, error) {
	bp := dn.Spec.BlockProducer
	now := time.Now()
	bundles := make([]*poolBundle, 0, len(bp.Pools))
	for _, p := range bp.Pools {
		b := &poolBundle{spec: p, id: normalizedPoolID(p.PoolID)}
		bundles = append(bundles, b)
		secret := &corev1.Secret{}
		key := types.NamespacedName{
			Name: p.Keys.SecretRef, Namespace: dn.Namespace,
		}
		if err := r.APIReader.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				b.problem = fmt.Errorf(
					"keys secret %q not found", p.Keys.SecretRef,
				)
				b.reason = reasonPoolSecretAbsent
				continue
			}
			return nil, fmt.Errorf(
				"read keys secret %q of pool %s: %w",
				p.Keys.SecretRef, b.id, err,
			)
		}
		b.secret = secret
		state, err := validateBundle(secret, poolChecks(dn, p, now))
		if err == nil && len(bp.Pools) > 1 {
			if vote, _, _ := voteKeyFile(secret.Data); vote != nil {
				err = errors.New(
					"keys secret holds a Leios vote key, which Dingo refuses " +
						"with more than one pool",
				)
			}
		}
		if err != nil {
			b.problem = err
			b.reason = reasonPoolKeysRejected
			continue
		}
		b.state = state
	}
	return bundles, nil
}

// acceptedPools returns the bundles that passed validation.
func acceptedPools(bundles []*poolBundle) []*poolBundle {
	var out []*poolBundle
	for _, b := range bundles {
		if b.accepted() {
			out = append(out, b)
		}
	}
	return out
}

// renderPools returns the pools of accepted bundles for the pod, in pool ID
// order.
func renderPools(accepted []*poolBundle) []resources.RenderPool {
	pools := make([]resources.RenderPool, 0, len(accepted))
	for _, b := range accepted {
		vote, _, _ := voteKeyFile(b.secret.Data)
		pools = append(pools, resources.RenderPool{
			PoolID:       b.id,
			SecretName:   b.spec.Keys.SecretRef,
			AgentProfile: b.spec.AgentProfile,
			HasVote:      vote != nil,
		})
	}
	return resources.SortedPools(pools)
}

// poolsKeysChecksum is the pod template's keys checksum of a pool list: it
// covers every accepted pool's Secret except the opcert, which reaches the
// running node without a roll, and the KES key, which the agent holds. A
// pool added, removed or dropped changes it, as does any change to a pool's VRF
// key, vote key or other entry.
func poolsKeysChecksum(accepted []*poolBundle) string {
	data := make(map[string][]byte, len(accepted))
	for _, b := range accepted {
		static := staticKeysChecksum(b.secret.Data)
		data[b.id] = []byte(static)
	}
	return checksumData(data)
}

// upsertPoolsConfigMap writes the rendered pool list.
func (r *DingoNodeReconciler) upsertPoolsConfigMap(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	pools []resources.RenderPool,
) error {
	desired := resources.BuildPoolsConfigMap(dn, pools)
	cm := &corev1.ConfigMap{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	return createOrUpdateWithRetry(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return ctrl.SetControllerReference(dn, cm, r.Scheme)
	})
}

// reconcilePoolKeys validates every pool's bundle and decides what the pod is
// rendered with: the pools whose bundle is accepted, the checksum that rolls
// the pod when any of their static key material or the pool list changes, and
// the approval that lets a new opcert reach the running node without a roll.
// A pool whose bundle is refused is left out of the pool list and reported in
// status.pools, so the other pools keep forging; when every bundle is refused
// the live pod template is kept, as for a refused single-pool bundle. In Auto
// rotation the live template is also kept while a refused pool's Secret holds
// an older opcert than its node forges with or than autoRotate recorded
// installing in the agent (poolInstallPending), until autoRotate writes that
// opcert back. It reports whether the template is held and why.
func (r *DingoNodeReconciler) reconcilePoolKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	opts *resources.RenderOptions,
) (keepTemplate bool, holdMessage string, err error) {
	holdMessage = templateHeldMessage
	syncPoolStatuses(dn)
	bundles, err := r.evaluatePools(ctx, dn)
	if err != nil {
		return false, "", err
	}
	accepted := acceptedPools(bundles)
	r.recordPoolKeys(ctx, dn, bundles)

	live, err := r.liveKeysTemplate(ctx, dn)
	if err != nil {
		return false, "", err
	}
	if len(accepted) == 0 {
		// Nothing to run: keep the live template, and carry its keys
		// annotations so the held render does not record the refused bundles.
		opts.KeysChecksum = live.checksum
		opts.KeysPool = live.pool
		opts.MountKeys = false
		r.rejectAllPoolKeys(ctx, dn, bundles)
		return true, holdMessage, nil
	}

	rendered := renderPools(accepted)
	opts.Pools = rendered
	opts.KeysChecksum = poolsKeysChecksum(accepted)
	opts.KeysPool = resources.JoinKeysPools(poolIDsOf(rendered))
	opts.PoolsConfigChecksum = checksum(resources.PoolsConfig(dn, rendered))
	monitorOnly := dn.Spec.BlockProducer.Rotation.Mode ==
		dingov1alpha1.RotationModeMonitorOnly
	liveHasKeys := live.exists && live.checksum != ""
	if monitorOnly && liveHasKeys {
		if opts.KeysChecksum != live.checksum {
			// MonitorOnly reports replacement bundles but does not change the
			// pool set or static key material the live pod stages.
			opts.KeysChecksum = live.checksum
			opts.KeysPool = live.pool
			keepTemplate = true
			holdMessage = monitorOnlyHeldMessage
		}
	}

	// A refused pool with a later opcert in its agent than its Secret holds
	// waits on autoRotate to write that opcert back. Dropping it would roll
	// the pod without the pool, and the node would stop reporting the
	// counter the repair is made from.
	if liveHasKeys && slices.ContainsFunc(bundles, func(b *poolBundle) bool {
		return !b.accepted() && b.secret != nil && poolInstallPending(dn, b)
	}) {
		opts.KeysChecksum = live.checksum
		opts.KeysPool = live.pool
		keepTemplate = true
		holdMessage = installPendingHeldMessage
	}

	// The agent client certificate is mounted from its own Secret, as for a
	// single pool: one that cannot be mounted holds the template.
	problem, certSum, err := r.checkSigningAgentCert(ctx, dn)
	if err != nil {
		return false, "", err
	}
	opts.SigningAgentCertChecksum = certSum
	if problem != "" {
		opts.KeysChecksum = live.checksum
		opts.KeysPool = live.pool
		keepTemplate = true
		holdMessage = signingAgentCertHeldMessage
		r.rejectSigningAgentCert(ctx, dn, problem)
	}

	// A pool-list node signs through the agent, so it runs no staging
	// container and keeps no approval ConfigMap.
	if err := r.deleteKeysApprovalConfigMap(ctx, dn); err != nil {
		return false, "", err
	}
	// The pool list is read at startup only, so a held template keeps the
	// list its pod started with. So does a template fenced from moving to
	// another agent: a pod restarted while several run would otherwise read
	// the new agent beside pods on the old one.
	writePools := !keepTemplate
	if writePools {
		fenced, err := r.fencesPoolsAgentMove(ctx, dn, rendered)
		if err != nil {
			return false, "", err
		}
		writePools = !fenced
	}
	if writePools {
		if err := r.upsertPoolsConfigMap(ctx, dn, rendered); err != nil {
			return false, "", fmt.Errorf("apply pools configmap: %w", err)
		}
	}
	if problem == "" {
		r.setPoolKeysCondition(dn, bundles)
	}
	return keepTemplate, holdMessage, nil
}

// fencesPoolsAgentMove reports whether the rendered pool list moves the live
// pods to another agent while several run, the case upsertStatefulSet fences.
func (r *DingoNodeReconciler) fencesPoolsAgentMove(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	rendered []resources.RenderPool,
) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(dn), sts); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read statefulset for agent move: %w", err)
	}
	link, linked := poolsAgentLink(resources.PoolsConfig(dn, rendered))
	moves, _, err := r.movesToAnotherAgent(
		ctx, dn, sts, link, linked,
		checksum(resources.PoolsConfig(dn, rendered)),
	)
	return moves && runsSeveralPods(sts), err
}

// poolInstallPending reports whether a pool's Secret holds an older opcert
// than its node reports loaded or than Auto rotation recorded installing in
// the agent. The loaded counter is the previous reconcile's: resources are
// applied before the node is scraped, so only the recorded install covers the
// reconcile right after a failed write.
func poolInstallPending(dn *dingov1alpha1.DingoNode, b *poolBundle) bool {
	ps := poolStatus(dn, b.id)
	if installPending(dn, b.secret, ps.LoadedCounter) {
		return true
	}
	return ps.PendingInstallCounter > 0 &&
		installPending(dn, b.secret, &ps.PendingInstallCounter)
}

func poolIDsOf(pools []resources.RenderPool) []string {
	ids := make([]string, 0, len(pools))
	for _, p := range pools {
		ids = append(ids, p.PoolID)
	}
	return ids
}

// recordPoolKeys writes each pool's verdict into status.pools, with a Warning
// Event when a pool newly fails.
func (r *DingoNodeReconciler) recordPoolKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	bundles []*poolBundle,
) {
	for _, b := range bundles {
		ps := poolStatus(dn, b.id)
		wasRejected := !ps.KeysValid && ps.KeysReason == reasonPoolKeysRejected
		if b.accepted() {
			ps.KeysValid = true
			ps.KeysReason = reasonPoolKeysAccepted
			ps.KeysMessage = fmt.Sprintf(
				"opcert counter %d, kes start period %d",
				b.state.Counter, b.state.KESPeriod,
			)
			ps.OnDiskCounter = b.state.Counter
			if b.state.Counter >= ps.PendingInstallCounter {
				ps.PendingInstallCounter = 0
			}
			continue
		}
		ps.KeysValid = false
		ps.KeysReason = b.reason
		ps.KeysMessage = b.problem.Error()
		log.FromContext(ctx).Info(
			"leaving a pool out of the node: its keys were refused",
			"pool", b.id, "secret", b.spec.Keys.SecretRef,
			"error", b.problem.Error(),
		)
		if r.Recorder != nil && !wasRejected {
			r.Recorder.Eventf(
				dn, nil, corev1.EventTypeWarning, "OpCertRejected",
				"ValidateKeys", "pool %s: %s", b.id, b.problem.Error(),
			)
		}
	}
}

// poolProblems describes the pools whose bundles were refused, in pool ID
// order.
func poolProblems(bundles []*poolBundle) string {
	sorted := slices.Clone(bundles)
	sort.Slice(
		sorted,
		func(i, j int) bool { return sorted[i].id < sorted[j].id },
	)
	var parts []string
	for _, b := range sorted {
		if !b.accepted() {
			parts = append(parts, fmt.Sprintf("pool %s: %s", b.id, b.problem))
		}
	}
	return strings.Join(parts, "; ")
}

// setPoolKeysCondition summarizes the pools' verdicts: KeysValid is true only
// while every pool is accepted. A pool left out is not forging, so the node is
// Degraded while the others run.
func (r *DingoNodeReconciler) setPoolKeysCondition(
	dn *dingov1alpha1.DingoNode,
	bundles []*poolBundle,
) {
	if msg := poolProblems(bundles); msg != "" {
		for _, t := range []string{condKeysValid, condDegraded} {
			status := metav1.ConditionFalse
			if t == condDegraded {
				status = metav1.ConditionTrue
			}
			meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
				Type:               t,
				Status:             status,
				Reason:             reasonPoolKeysRejected,
				Message:            msg + "; left out of the node's pool list",
				ObservedGeneration: dn.Generation,
			})
		}
		return
	}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:   condKeysValid,
		Status: metav1.ConditionTrue,
		Reason: reasonPoolKeysAccepted,
		Message: fmt.Sprintf(
			"keys of all %d pools accepted", len(bundles),
		),
		ObservedGeneration: dn.Generation,
	})
}

// rejectAllPoolKeys refuses the node as a whole: no pool's bundle passed, so
// there is nothing to render and the live template is kept.
func (r *DingoNodeReconciler) rejectAllPoolKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	bundles []*poolBundle,
) {
	reason := reasonPoolKeysRejected
	if !slices.ContainsFunc(bundles, func(b *poolBundle) bool {
		return b.reason != reasonPoolSecretAbsent
	}) {
		reason = reasonPoolSecretAbsent
	}
	msg := "no pool's keys are usable; " + poolProblems(bundles)
	log.FromContext(ctx).Info(
		"refusing delivered block-producer keys", "error", msg,
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn, nil, corev1.EventTypeWarning, reason, "ValidateKeys", "%s", msg,
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
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: dn.Generation,
		})
	}
}

// scrapedPodRead is the pod refreshForgeStatus scrapes, as read at one moment:
// nil when there is none, err when it could not be read.
type scrapedPodRead struct {
	pod *corev1.Pod
	err error
}

// readScrapedPod reads the pod of dn called name, one of the pods whose
// metrics a block producer's status is taken from. Pods are listed, not read by
// name: the operator may list them but not get them, and holds no Pod informer.
func (r *DingoNodeReconciler) readScrapedPod(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	name string,
) scrapedPodRead {
	if r.APIReader == nil && r.Client == nil {
		return scrapedPodRead{}
	}
	var pods corev1.PodList
	if err := r.uncachedReader().List(ctx, &pods,
		client.InNamespace(dn.Namespace),
		client.MatchingLabels{resources.NodeLabel: dn.Name},
	); err != nil {
		return scrapedPodRead{err: err}
	}
	for i := range pods.Items {
		if pods.Items[i].Name == name {
			return scrapedPodRead{pod: &pods.Items[i]}
		}
	}
	return scrapedPodRead{}
}

// runningPools returns the pools the pod called name stages, from the keys-pool
// annotation its template gave it, and whether they are known. before is the
// pod as read before the scrape; it is read again here, and only the same pod
// at both reads is the one that answered. During a roll the StatefulSet
// template already records the next pools while the old pod still runs and
// exports its series, so the template does not say whose they are. When the
// pod changed, records no pools, or either read failed, the pools are
// unknown. With no pod at either read, the pools the live StatefulSet
// template stages stand in. Its pod forges them with the keys it loaded
// whatever their bundles' verdict is now: a template held because every
// bundle, or the signing agent certificate, was refused keeps running pools
// that are not accepted. The result is empty when the template records no
// pools.
func (r *DingoNodeReconciler) runningPools(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	name string,
	before scrapedPodRead,
) (map[string]bool, bool) {
	running := map[string]bool{}
	after := r.readScrapedPod(ctx, dn, name)
	if before.err != nil || after.err != nil {
		return nil, false
	}
	var annotations map[string]string
	switch {
	case before.pod == nil && after.pod == nil:
		if r.Client == nil {
			return running, true
		}
		sts := &appsv1.StatefulSet{}
		if err := r.Get(
			ctx, client.ObjectKeyFromObject(dn), sts,
		); err != nil {
			return running, true
		}
		annotations = sts.Spec.Template.Annotations
	case before.pod == nil || after.pod == nil ||
		before.pod.UID != after.pod.UID:
		return nil, false
	default:
		// A pod that records no pools, started from a template rendered
		// before the record existed, does not say whose series it exports.
		if len(resources.SplitKeysPools(
			after.pod.Annotations[resources.KeysPoolAnnotation],
		)) == 0 {
			return nil, false
		}
		annotations = after.pod.Annotations
	}
	for _, p := range resources.SplitKeysPools(
		annotations[resources.KeysPoolAnnotation],
	) {
		running[normalizedPoolID(p)] = true
	}
	return running, true
}

// mergePoolPods folds the scrapes of a pool-list producer's pods into merged,
// the one status observePools reads, and returns the pools that run and
// whether they are known. Each pod's pools are attributed by that pod's own
// before/after read, then every pool keeps the metrics of the pod that is
// furthest behind on it, and a pool counts as running when any pod runs it. A
// pod that runs a pool without reporting metrics for it leaves the pool
// unobserved.
// The pools are known only when they are known for every pod. statuses holds
// one scrape per pod of podNames, in order.
func (r *DingoNodeReconciler) mergePoolPods(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	podNames []string,
	statuses []*forgestatus.Status,
	before map[string]scrapedPodRead,
	merged *forgestatus.Status,
) (map[string]bool, bool) {
	if len(statuses) == 1 {
		if st := statuses[0]; st != nil {
			*merged = *st
			return r.runningPools(ctx, dn, podNames[0], before[podNames[0]])
		}
	}
	merged.Pools = map[string]*forgestatus.PoolMetrics{}
	running := map[string]bool{}
	allKnown := true
	for i, st := range statuses {
		if st == nil {
			continue
		}
		podRunning, known := r.runningPools(
			ctx, dn, podNames[i], before[podNames[i]],
		)
		allKnown = allKnown && known
		maps.Copy(running, podRunning)
		byID := poolMetricsByID(podRunning, st)
		// A pod that stages a pool and reports nothing for it is the one
		// furthest behind on it, not a pod to skip.
		for id := range podRunning {
			if _, ok := byID[id]; !ok {
				byID[id] = &forgestatus.PoolMetrics{}
			}
		}
		for id, pm := range byID {
			if prev, ok := merged.Pools[id]; ok {
				pm = furthestBehindPool(prev, pm)
			}
			merged.Pools[id] = pm
		}
	}
	return running, allKnown
}

// furthestBehindPool is furthestBehind for one pool's metrics of two pods.
func furthestBehindPool(
	a, b *forgestatus.PoolMetrics,
) *forgestatus.PoolMetrics {
	worst := a
	if a.HasKESData &&
		(!b.HasKESData || b.RemainingKESPeriods < a.RemainingKESPeriods) {
		worst = b
	}
	merged := *worst
	merged.HasOpCertCounter = a.HasOpCertCounter && b.HasOpCertCounter
	merged.OpCertUnloaded = !merged.HasOpCertCounter &&
		(a.OpCertUnloaded || b.OpCertUnloaded)
	merged.OpCertCounter = min(a.OpCertCounter, b.OpCertCounter)
	return &merged
}

// poolMetricsByID returns the node's per-pool metrics keyed by normalized pool
// ID. Dingo labels the series with pool_id only while it forges for more than
// one pool; a node running a single pool exposes them unlabelled, and they
// are that pool's.
func poolMetricsByID(
	running map[string]bool,
	st *forgestatus.Status,
) map[string]*forgestatus.PoolMetrics {
	byID := make(map[string]*forgestatus.PoolMetrics, len(st.Pools))
	for label, pm := range st.Pools {
		byID[normalizedPoolID(label)] = pm
	}
	if len(st.Pools) > 0 {
		return byID
	}
	var rendered []string
	for id := range running {
		rendered = append(rendered, id)
	}
	if len(rendered) == 1 &&
		(st.HasKESData || st.HasOpCertCounter || st.OpCertUnloaded) {
		byID[rendered[0]] = &forgestatus.PoolMetrics{
			CurrentKESPeriod:      st.CurrentKESPeriod,
			RemainingKESPeriods:   st.RemainingKESPeriods,
			OpCertStartKESPeriod:  st.OpCertStartKESPeriod,
			OpCertExpiryKESPeriod: st.OpCertExpiryKESPeriod,
			HasKESData:            st.HasKESData,
			OpCertCounter:         st.OpCertCounter,
			HasOpCertCounter:      st.HasOpCertCounter,
			OpCertUnloaded:        !st.HasOpCertCounter && st.OpCertUnloaded,
		}
	}
	return byID
}

// observePools records each pool's KES state and loaded opcert counter from
// the node's metrics, and summarizes them for the node: status.kes is the pool
// with the fewest KES periods left, and RotationDue is true while any forging
// pool is due. A pool forges while the running template stages it (running)
// or the node reports series labelled with it, whatever its bundle's verdict;
// with no running template recorded, the pools whose keys were accepted stand
// in for it. When known is false the scraped pod's pools are unknown: only
// series labelled with a pool are recorded, and every other pool keeps its
// status and counts as unobserved.
func (r *DingoNodeReconciler) observePools(
	dn *dingov1alpha1.DingoNode,
	st *forgestatus.Status,
	running map[string]bool,
	known bool,
) {
	syncPoolStatuses(dn)
	if !known {
		running = map[string]bool{}
	} else if len(running) == 0 {
		running = map[string]bool{}
		for _, ps := range dn.Status.Pools {
			if ps.KeysValid {
				running[ps.PoolID] = true
			}
		}
	}
	byID := poolMetricsByID(running, st)
	renewBefore := int64(dn.Spec.BlockProducer.Rotation.RenewBeforePeriods)

	var due, unobserved []string
	var lowest *dingov1alpha1.PoolStatus
	observed := map[string]bool{}
	for i := range dn.Status.Pools {
		ps := &dn.Status.Pools[i]
		pm := byID[ps.PoolID]
		if known && !running[ps.PoolID] && pm == nil {
			// Left out of the node: nothing is forging for it.
			ps.KES = dingov1alpha1.KESStatus{}
			ps.LoadedCounter = nil
			continue
		}
		if pm == nil {
			unobserved = append(unobserved, ps.PoolID)
			continue
		}
		switch {
		case pm.HasOpCertCounter:
			ps.LoadedCounter = new(pm.OpCertCounter)
		case pm.OpCertUnloaded:
			ps.LoadedCounter = nil
		}
		if !pm.HasKESData {
			unobserved = append(unobserved, ps.PoolID)
			continue
		}
		ps.KES = dingov1alpha1.KESStatus{
			CurrentPeriod:    pm.CurrentKESPeriod,
			OpCertKESPeriod:  pm.OpCertStartKESPeriod,
			ExpiryPeriod:     pm.OpCertExpiryKESPeriod,
			RemainingPeriods: pm.RemainingKESPeriods,
		}
		observed[ps.PoolID] = true
		if renewBefore > 0 && pm.RemainingKESPeriods <= renewBefore {
			due = append(due, fmt.Sprintf(
				"pool %s: %d KES periods remaining (threshold %d)",
				ps.PoolID, pm.RemainingKESPeriods, renewBefore,
			))
		}
		if lowest == nil ||
			ps.KES.RemainingPeriods < lowest.KES.RemainingPeriods {
			lowest = ps
		}
	}
	r.setKESObserved(client.ObjectKeyFromObject(dn), observed)
	if lowest != nil {
		dn.Status.KES = lowest.KES
	}
	switch {
	case len(due) > 0:
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condRotationDue, Status: metav1.ConditionTrue,
			Reason:             "KESExpiringSoon",
			Message:            strings.Join(due, "; "),
			ObservedGeneration: dn.Generation,
		})
	case len(unobserved) > 0 || lowest == nil:
		setRotationDueUnknown(
			dn, "KESMetricsUnavailable",
			"KES metrics are not available for every pool",
		)
	default:
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condRotationDue, Status: metav1.ConditionFalse,
			Reason: "KESHealthy",
			Message: fmt.Sprintf(
				"%d KES periods remaining on the pool with the fewest",
				lowest.KES.RemainingPeriods,
			),
			ObservedGeneration: dn.Generation,
		})
	}
}

// refreshPoolOnChainCounters reads every listed pool's on-chain opcert counter
// with one query and publishes each in status.pools. It is
// refreshOnChainCounter for a pool list, with the same rate limit, failure
// handling and condition: a counter that cannot be read leaves the last one in
// place and never fails the reconcile.
func (r *DingoNodeReconciler) refreshPoolOnChainCounters(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	fetcher, ok := r.OnChain.(onchain.PoolsFetcher)
	if !ok {
		r.setOnChainUnavailable(dn, "Unsupported",
			"the node-to-client fetcher cannot read several pools' counters")
		return
	}
	syncPoolStatuses(dn)
	bp := dn.Spec.BlockProducer
	ids := poolIDs(bp)
	slices.Sort(ids)
	listKey := strings.Join(ids, ",")
	key := client.ObjectKeyFromObject(dn)

	// A remembered attempt of another pool list, or of poolId, says nothing
	// about this one.
	if last, ok := r.lastOnChainAttempt(key); ok {
		if !last.list || last.poolID != listKey {
			r.forgetOnChainAttempt(key)
		} else if time.Since(last.at) < onChainCounterRefreshInterval {
			r.applyPoolCounterAttempt(dn, last)
			return
		}
	}

	magic, ok := networkMagic(dn)
	if !ok {
		r.setOnChainUnavailable(dn, "UnknownNetworkMagic",
			fmt.Sprintf(
				"network %q is not a known network; set spec.networkMagic so "+
					"the node-to-client handshake can be attempted",
				dn.Spec.Network,
			))
		return
	}
	hashes := make([]common.Blake2b224, 0, len(bp.Pools))
	for _, p := range bp.Pools {
		parsed, err := parsePoolID(p.PoolID)
		if err != nil {
			r.setOnChainUnavailable(dn, "InvalidPoolID", err.Error())
			return
		}
		hashes = append(hashes, parsed)
	}
	addr := net.JoinHostPort(
		fmt.Sprintf("%s.%s.svc.cluster.local", dn.Name, dn.Namespace),
		strconv.Itoa(resources.PortNodeToClient),
	)

	attempt := onChainAttempt{at: time.Now(), poolID: listKey, list: true}
	counters, err := fetcher.FetchPools(ctx, onchain.Query{
		Address:      addr,
		NetworkMagic: magic,
	}, hashes)
	found := make(map[string]int64, len(counters))
	for _, h := range hashes {
		if c, ok := counters[h]; ok && c.Found {
			found[h.String()] = c.Value
		}
	}
	switch {
	case len(found) > 0:
		attempt.counters = found
		attempt.observedAt = attempt.at
		attempt.reason = "Observed"
		attempt.message = "on-chain opcert counters: " + counterList(found)
		if err != nil {
			attempt.message += "; " + err.Error()
		}
	case err != nil:
		log.FromContext(ctx).V(1).Info(
			"on-chain opcert counters unavailable", "error", err.Error(),
		)
		attempt.reason, attempt.message = r.onChainFailureReason(ctx, dn, err)
	default:
		attempt.reason = "PoolNotOnChain"
		attempt.message = "the chain has no opcert counter for any listed " +
			"pool yet; none has minted a block under an operational " +
			"certificate"
	}
	r.recordOnChainAttempt(key, attempt)
	r.applyPoolCounterAttempt(dn, attempt)
}

// counterList renders pool counters in pool ID order.
func counterList(counters map[string]int64) string {
	ids := make([]string, 0, len(counters))
	for id := range counters {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s=%d", id, counters[id]))
	}
	return strings.Join(parts, ", ")
}

// applyPoolCounterAttempt writes an attempt's counters into status.pools and
// its outcome into the condition. A failed attempt leaves the counters already
// observed, for the reason refreshOnChainCounter gives.
func (r *DingoNodeReconciler) applyPoolCounterAttempt(
	dn *dingov1alpha1.DingoNode,
	attempt onChainAttempt,
) {
	if len(attempt.counters) == 0 {
		if attempt.reason != "" {
			r.setOnChainUnavailable(dn, attempt.reason, attempt.message)
		}
		return
	}
	for id, v := range attempt.counters {
		ps := poolStatus(dn, id)
		ps.OnChainCounter = v
		at := metav1.NewTime(attempt.observedAt)
		ps.OnChainCounterAt = &at
	}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condOnChainCounter,
		Status:             metav1.ConditionTrue,
		Reason:             attempt.reason,
		Message:            attempt.message,
		ObservedGeneration: dn.Generation,
	})
}
