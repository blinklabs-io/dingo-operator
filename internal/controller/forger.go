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
	"fmt"
	"slices"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// forgerHeldError refuses a block producer its keys. Any other error from
// forgerHeld is a failed lookup, which must neither halt a running forger nor
// admit a new one. halt is false when the only evidence is a workload no live
// DingoNode accounts for: anyone who may create a pod can forge one, so it may
// keep a producer from starting or its template from rolling, but must not
// scale a running one to zero.
type forgerHeldError struct {
	msg  string
	halt bool
}

func (e *forgerHeldError) Error() string { return e.msg }

func heldf(format string, a ...any) error {
	return &forgerHeldError{msg: fmt.Sprintf(format, a...), halt: true}
}

// unverifiedHeldf refuses without halting; see forgerHeldError.
func unverifiedHeldf(format string, a ...any) error {
	return &forgerHeldError{msg: fmt.Sprintf(format, a...)}
}

// workloadState is what a workload proves about the keys it runs.
type workloadState int

const (
	workloadDown workloadState = iota
	// workloadUnverified runs, but nothing ties it to the DingoNode: it only
	// has the DingoNode's name or name label.
	workloadUnverified
	// workloadVerified is the StatefulSet the DingoNode controls, by UID, or
	// a pod that StatefulSet controls.
	workloadVerified
)

// forgerIdentity is what makes two block producers the same forger: a pool
// (from poolId, from the cold key in the delivered opcert, or recorded on the
// pod that staged the keys) or a keys Secret. checksum is the keys Secret's
// keys checksum, compared against what a pod recorded when it staged its keys.
type forgerIdentity struct {
	pools     []string
	namespace string
	secret    string
	checksum  string
}

// sharedWith describes what a and b share, or returns "" when they are
// different forgers.
func (a forgerIdentity) sharedWith(b forgerIdentity) string {
	for _, p := range a.pools {
		if slices.Contains(b.pools, p) {
			return "pool " + p
		}
	}
	if a.secret != "" && a.namespace == b.namespace && a.secret == b.secret {
		return fmt.Sprintf("keys Secret %q", a.secret)
	}
	return ""
}

// keysPoolID returns the pool the keys Secret's opcert belongs to, and the
// Secret's keys checksum: a copy of the same bundle under another Secret name
// still hashes to the same cold key, which poolId (optional) does not reveal.
// An absent Secret yields "" for both, and one without a parseable opcert ""
// for the pool. A failed read is an error, since treating it as "no pool"
// would admit a copy of the keys.
func (r *DingoNodeReconciler) keysPoolID(
	ctx context.Context,
	namespace, secretName string,
) (pool, sum string, err error) {
	if r.APIReader == nil || secretName == "" {
		return "", "", nil
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("read keys Secret %s: %w", key, err)
	}
	return opCertPoolID(secret.Data), keysChecksum(secret), nil
}

// opCertPoolID returns the pool of the opcert in a keys Secret's data, or ""
// when there is no parseable opcert.
func opCertPoolID(data map[string][]byte) string {
	cert, _, err := opCertFile(data)
	if err != nil || cert == nil {
		return ""
	}
	_, coldVkey, err := opcert.Parse(cert)
	if err != nil {
		return ""
	}
	return common.Blake2b224Hash(coldVkey).String()
}

func (r *DingoNodeReconciler) nodeIdentity(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (forgerIdentity, error) {
	id := forgerIdentity{namespace: dn.Namespace}
	if bp := dn.Spec.BlockProducer; bp != nil {
		id.secret = bp.Keys.SecretRef
		if p := normalizedPoolID(bp.PoolID); p != "" {
			id.pools = append(id.pools, p)
		}
	}
	p, sum, err := r.keysPoolID(ctx, id.namespace, id.secret)
	if err != nil {
		return id, err
	}
	if p != "" {
		id.pools = append(id.pools, p)
	}
	id.checksum = sum
	return id, nil
}

// forgerHeld returns an error when dn may not run its keys yet: an older live
// block producer holds them (by spec, or by the cold key in the Secret), or a
// newer one or a DingoNode that is gone or being deleted still runs them. The
// DingoNode going away does not stop its pod; the StatefulSet and pod outlive
// it through garbage collection and the termination grace period.
func (r *DingoNodeReconciler) forgerHeld(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	all []dingov1alpha1.DingoNode,
) error {
	if err := forgerConflict(dn, all); err != nil {
		return err
	}
	mine, err := r.nodeIdentity(ctx, dn)
	if err != nil {
		return err
	}
	for i := range all {
		o := &all[i]
		if !o.DeletionTimestamp.IsZero() {
			continue
		}
		if client.ObjectKeyFromObject(o) == client.ObjectKeyFromObject(dn) ||
			!resources.IsBlockProducer(o) {
			continue
		}
		theirs, err := r.nodeIdentity(ctx, o)
		if err != nil {
			// A denied read is a limit of the operator's current permissions,
			// not proof the node never ran: fail closed while its workload
			// runs, and otherwise compare only its poolId and Secret name.
			// Failing on every denial would stall all block producers on one
			// unreadable namespace.
			if !apierrors.IsForbidden(err) {
				return err
			}
			up, uerr := r.workloadUp(ctx, o)
			if uerr != nil {
				return uerr
			}
			if up != workloadDown {
				return err
			}
		}
		what := mine.sharedWith(theirs)
		if what == "" {
			continue
		}
		// An older node whose spec fails validation is not reconciled, so it
		// keeps the pool only while the workload it already has runs.
		if forgerOutranks(o, dn) && validateSpec(o) == nil {
			return heldf(
				"block producer %s/%s already forges with the same %s; only "+
					"one DingoNode may stage a pool's keys",
				o.Namespace, o.Name, what,
			)
		}
		// o is refused and halts itself, or is invalid and keeps only the
		// workload it has, so this cannot wait forever; until its pod is gone
		// it still forges.
		up, err := r.workloadUp(ctx, o)
		if err != nil {
			return err
		}
		held := heldf
		switch up {
		case workloadDown:
			continue
		case workloadUnverified:
			held = unverifiedHeldf
		case workloadVerified:
		}
		return held(
			"block producer %s/%s still runs with the same %s; waiting "+
				"for it to stop",
			o.Namespace, o.Name, what,
		)
	}
	return r.orphanForger(ctx, dn, mine)
}

// workloadUp reports whether o's StatefulSet wants a replica or any of its
// pods has not finished, and whether that workload verifies as o's: the
// StatefulSet o controls, or a pod that StatefulSet controls. One that only
// has o's name, or a pod that only carries its name label, is unverified.
// Controller references are set by whoever creates the object, so a verified
// workload shows only that its creator may write in o's namespace.
func (r *DingoNodeReconciler) workloadUp(
	ctx context.Context,
	o *dingov1alpha1.DingoNode,
) (workloadState, error) {
	state := workloadDown
	sts := &appsv1.StatefulSet{}
	err := r.uncachedReader().Get(ctx, client.ObjectKeyFromObject(o), sts)
	owned := false
	switch {
	case err == nil:
		owned = metav1.IsControlledBy(sts, o)
		if sts.Spec.Replicas == nil || *sts.Spec.Replicas > 0 {
			if owned {
				return workloadVerified, nil
			}
			state = workloadUnverified
		}
	case !apierrors.IsNotFound(err):
		return workloadDown, fmt.Errorf("read statefulset %s/%s: %w",
			o.Namespace, o.Name, err)
	}
	var pods corev1.PodList
	if err := r.uncachedReader().List(ctx, &pods,
		client.InNamespace(o.Namespace),
		client.MatchingLabels{resources.NodeLabel: o.Name},
	); err != nil {
		return workloadDown, fmt.Errorf("list pods of %s/%s: %w",
			o.Namespace, o.Name, err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if !podRunning(p) {
			continue
		}
		if owned && metav1.IsControlledBy(p, sts) {
			return workloadVerified, nil
		}
		state = workloadUnverified
	}
	return state, nil
}

// uncachedReader reads workloads for the fence from the API server: no
// cluster-wide Pod informer is held, and a StatefulSet the cache has not seen
// yet must not read as stopped.
func (r *DingoNodeReconciler) uncachedReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func podRunning(p *corev1.Pod) bool {
	return p.Status.Phase != corev1.PodSucceeded &&
		p.Status.Phase != corev1.PodFailed
}

// orphanForger returns an error when any block-producer workload other than
// dn's own still runs keys dn shares, whatever DingoNode now carries its name:
// a name reused by a relay or another pool does not own the old pod. A
// StatefulSet at zero replicas with no pods is down. Live block producers that
// share dn's keys were already decided by forgerHeld, so this finds only
// workloads they no longer account for. Such a workload refuses without
// halting: a running dn was admitted when none held its keys, and none of
// this is proof the operator started it.
func (r *DingoNodeReconciler) orphanForger(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	mine forgerIdentity,
) error {
	self := client.ObjectKeyFromObject(dn)
	selector := client.MatchingLabels{
		"app.kubernetes.io/managed-by": resources.ManagedByName,
		"app.kubernetes.io/component":  resources.ComponentBlockProducer,
	}
	var sets appsv1.StatefulSetList
	if err := r.uncachedReader().List(ctx, &sets, selector); err != nil {
		return fmt.Errorf("list block producer StatefulSets: %w", err)
	}
	for i := range sets.Items {
		s := &sets.Items[i]
		if s.Spec.Replicas != nil && *s.Spec.Replicas == 0 {
			continue
		}
		// A pod a template starts stages the Secret as it is then, so only
		// a pod can have staged a pool its Secret no longer shows.
		if _, err := r.orphanHolds(
			ctx, mine, self, "StatefulSet", s.Namespace, s.Name, s.Name,
			s.Spec.Template.Annotations, "", s.Spec.Template.Spec,
		); err != nil {
			return err
		}
	}

	var pods corev1.PodList
	if err := r.uncachedReader().List(ctx, &pods, selector); err != nil {
		return fmt.Errorf("list block producer pods: %w", err)
	}
	var unknown *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if !podRunning(p) {
			continue
		}
		u, err := r.orphanHolds(
			ctx, mine, self, "pod", p.Namespace, p.Name,
			p.Labels[resources.NodeLabel], p.Annotations,
			p.Annotations[resources.KeysChecksumAnnotation], p.Spec,
		)
		if err != nil {
			return err
		}
		if u && unknown == nil {
			unknown = p
		}
	}
	if unknown == nil {
		return nil
	}
	rolls, err := r.rollsPastUnknownPool(ctx, dn, mine)
	if err != nil || rolls {
		return err
	}
	return unverifiedHeldf(
		"pod %s/%s of another block producer staged keys its keys Secret no "+
			"longer holds, for a pool recorded nowhere; waiting for it to stop",
		unknown.Namespace, unknown.Name,
	)
}

// rollsPastUnknownPool reports whether dn may proceed while another producer's
// pod of unknown staged pool runs. dn must already run a pod controlled by the
// StatefulSet dn controls. Each such pod must have staged the pool dn's keys
// now name, so the roll is the same forger's, as for a KES rotation, or itself
// be of unknown pool, which only dn's roll replaces; holding those would
// deadlock producers whose keys change together. A producer not running, or
// one whose keys move to another pool, waits for the pod to stop.
func (r *DingoNodeReconciler) rollsPastUnknownPool(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	mine forgerIdentity,
) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.uncachedReader().Get(
		ctx, client.ObjectKeyFromObject(dn), sts,
	); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(sts, dn) {
		return false, nil
	}
	var pods corev1.PodList
	if err := r.uncachedReader().List(ctx, &pods,
		client.InNamespace(dn.Namespace),
		client.MatchingLabels{resources.NodeLabel: dn.Name},
	); err != nil {
		return false, fmt.Errorf("list pods of %s/%s: %w",
			dn.Namespace, dn.Name, err)
	}
	runs := false
	for i := range pods.Items {
		p := &pods.Items[i]
		if !podRunning(p) || !metav1.IsControlledBy(p, sts) {
			continue
		}
		runs = true
		// An unannotated pod staged either the bundle dn has now, and so
		// its pool, or one of unknown pool.
		pool := normalizedPoolID(p.Annotations[resources.KeysPoolAnnotation])
		if pool != "" && !slices.Contains(mine.pools, pool) {
			return false, nil
		}
	}
	return runs, nil
}

// orphanHolds returns an error when a workload other than self's holds keys
// mine shares. A pod keeps the keys its init container copied until it stops,
// whatever its Secret now holds, so the pool recorded on it when it was created
// counts as well as the Secret's current one. A template rendered before that
// record existed gains it only at its next keys change; until then its pods are
// judged by their Secret, and by stagedSum, the keys checksum the pod recorded:
// equal to mine's, it staged the same bundle. Such a pod's Secret names its
// pool only while stagedSum equals the Secret's checksum; otherwise unknown is
// true, since the pod may still forge a pool recorded nowhere. A pod that
// recorded no checksum started from a template rendered before its keys were
// first accepted, and is judged by its Secret.
func (r *DingoNodeReconciler) orphanHolds(
	ctx context.Context,
	mine forgerIdentity,
	self types.NamespacedName,
	kind, namespace, name, owner string,
	annotations map[string]string,
	stagedSum string,
	spec corev1.PodSpec,
) (unknown bool, err error) {
	if self == (types.NamespacedName{Namespace: namespace, Name: owner}) {
		return false, nil
	}
	secret, ok := resources.KeysSecretName(spec)
	if !ok {
		return false, nil
	}
	theirs := forgerIdentity{namespace: namespace, secret: secret}
	recorded := normalizedPoolID(annotations[resources.KeysPoolAnnotation])
	if recorded != "" {
		theirs.pools = append(theirs.pools, recorded)
	}
	p, sum, err := r.keysPoolID(ctx, namespace, secret)
	if err != nil {
		return false, err
	}
	if p != "" {
		theirs.pools = append(theirs.pools, p)
	}
	what := mine.sharedWith(theirs)
	if what == "" && stagedSum != "" && stagedSum == mine.checksum {
		what = "keys bundle"
	}
	if what == "" {
		return recorded == "" && stagedSum != "" && stagedSum != sum, nil
	}
	return false, unverifiedHeldf(
		"%s %s/%s of another block producer still holds the same %s; "+
			"waiting for it to stop",
		kind, namespace, name, what,
	)
}
