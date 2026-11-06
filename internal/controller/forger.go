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
// admit a new one. halt is false when the only evidence is a workload that
// does not verify, or a pod: that may keep a producer from starting or
// rolling, but must not scale a running one to zero.
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
	// workloadUnverified runs, but nothing ties it to a DingoNode: a pod
	// creator can set its labels and annotations, and name any StatefulSet
	// in its namespace as its controller.
	workloadUnverified
	// workloadVerified is a StatefulSet that wants a replica and is
	// controlled by the live DingoNode of its name, by UID.
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
	return r.orphanForger(ctx, dn, mine, all)
}

// workloadUp reports whether o's StatefulSet wants a replica or any of its
// pods has not finished, and whether that workload verifies as o's: only a
// StatefulSet o controls does. One that only has o's name is unverified, and
// so is every pod, since a pod can name any StatefulSet as its controller.
func (r *DingoNodeReconciler) workloadUp(
	ctx context.Context,
	o *dingov1alpha1.DingoNode,
) (workloadState, error) {
	state := workloadDown
	sts := &appsv1.StatefulSet{}
	err := r.uncachedReader().Get(ctx, client.ObjectKeyFromObject(o), sts)
	switch {
	case err == nil:
		if sts.Spec.Replicas == nil || *sts.Spec.Replicas > 0 {
			if metav1.IsControlledBy(sts, o) {
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
		if podRunning(&pods.Items[i]) {
			return workloadUnverified, nil
		}
	}
	return state, nil
}

// statefulSetVerified reports whether s is controlled by the DingoNode in all
// with its namespace, name, and the controller reference's UID: the operator
// names each node's StatefulSet after it. A DingoNode being deleted still
// counts: foreground deletion keeps it until its StatefulSet is gone.
// Controller references are set by whoever creates the object, so this shows
// only that s's creator could write StatefulSets in that namespace.
func statefulSetVerified(
	s *appsv1.StatefulSet,
	all []dingov1alpha1.DingoNode,
) bool {
	for i := range all {
		if all[i].Namespace == s.Namespace && all[i].Name == s.Name &&
			metav1.IsControlledBy(s, &all[i]) {
			return true
		}
	}
	return false
}

// podVerified reports whether p is controlled by a live StatefulSet with the
// same UID that statefulSetVerified accepts. Any pod creator can name such a
// StatefulSet, so this only lets a pod's recorded keys checksum count.
func (r *DingoNodeReconciler) podVerified(
	ctx context.Context,
	p *corev1.Pod,
	all []dingov1alpha1.DingoNode,
) (bool, error) {
	ref := metav1.GetControllerOf(p)
	if ref == nil || ref.Kind != "StatefulSet" ||
		ref.APIVersion != appsv1.SchemeGroupVersion.String() {
		return false, nil
	}
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: p.Namespace, Name: ref.Name}
	if err := r.uncachedReader().Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read statefulset %s: %w", key, err)
	}
	return sts.UID == ref.UID && statefulSetVerified(sts, all), nil
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
// workloads they no longer account for. Only a StatefulSet that
// statefulSetVerified accepts halts; any other workload, and every pod,
// refuses without halting.
func (r *DingoNodeReconciler) orphanForger(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	mine forgerIdentity,
	all []dingov1alpha1.DingoNode,
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
		verify := func() (bool, error) {
			return statefulSetVerified(s, all), nil
		}
		if err := r.orphanHolds(
			ctx, mine, self, "StatefulSet", s.Namespace, s.Name, s.Name,
			s.Spec.Template.Annotations, "", s.Spec.Template.Spec,
			verify, true,
		); err != nil {
			return err
		}
	}

	var pods corev1.PodList
	if err := r.uncachedReader().List(ctx, &pods, selector); err != nil {
		return fmt.Errorf("list block producer pods: %w", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if !podRunning(p) {
			continue
		}
		verify := func() (bool, error) { return r.podVerified(ctx, p, all) }
		if err := r.orphanHolds(
			ctx, mine, self, "pod", p.Namespace, p.Name,
			p.Labels[resources.NodeLabel], p.Annotations,
			p.Annotations[resources.KeysChecksumAnnotation], p.Spec,
			verify, false,
		); err != nil {
			return err
		}
	}
	return nil
}

// orphanHolds returns an error when a workload other than self's holds keys
// mine shares. A pod keeps the keys its init container copied until it stops,
// whatever its Secret now holds, so the pool recorded on it when it was created
// counts as well as the Secret's current one. A template rendered before that
// record existed gains it only at its next keys change; until then its pods are
// judged by their Secret, and by stagedSum, the keys checksum the pod recorded:
// equal to mine's, it staged the same bundle. stagedSum counts only on a
// verified workload, since any pod can copy a checksum. verify is called only
// for a workload that shares mine's keys; a verified match halts only when
// canHalt is set.
func (r *DingoNodeReconciler) orphanHolds(
	ctx context.Context,
	mine forgerIdentity,
	self types.NamespacedName,
	kind, namespace, name, owner string,
	annotations map[string]string,
	stagedSum string,
	spec corev1.PodSpec,
	verify func() (bool, error),
	canHalt bool,
) error {
	if self == (types.NamespacedName{Namespace: namespace, Name: owner}) {
		return nil
	}
	secret, ok := resources.KeysSecretName(spec)
	if !ok {
		return nil
	}
	theirs := forgerIdentity{namespace: namespace, secret: secret}
	if p := normalizedPoolID(
		annotations[resources.KeysPoolAnnotation],
	); p != "" {
		theirs.pools = append(theirs.pools, p)
	}
	p, _, err := r.keysPoolID(ctx, namespace, secret)
	if err != nil {
		return err
	}
	if p != "" {
		theirs.pools = append(theirs.pools, p)
	}
	what := mine.sharedWith(theirs)
	sumOnly := false
	if what == "" && stagedSum != "" && stagedSum == mine.checksum {
		what, sumOnly = "keys bundle", true
	}
	if what == "" {
		return nil
	}
	verified, err := verify()
	if err != nil {
		return err
	}
	if sumOnly && !verified {
		return nil
	}
	held := unverifiedHeldf
	if verified && canHalt {
		held = heldf
	}
	return held(
		"%s %s/%s of another block producer still holds the same %s; "+
			"waiting for it to stop",
		kind, namespace, name, what,
	)
}
