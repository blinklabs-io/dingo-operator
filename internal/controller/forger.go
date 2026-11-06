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

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/opcert"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// forgerIdentity is what makes two block producers the same forger: a pool
// (from poolId or from the cold key in the delivered opcert) or a keys Secret.
type forgerIdentity struct {
	pools     []string
	namespace string
	secret    string
}

// sharedWith describes what a and b share, or returns "" when they are
// different forgers.
func (a forgerIdentity) sharedWith(b forgerIdentity) string {
	for _, p := range a.pools {
		for _, q := range b.pools {
			if p == q {
				return "pool " + p
			}
		}
	}
	if a.secret != "" && a.namespace == b.namespace && a.secret == b.secret {
		return fmt.Sprintf("keys Secret %q", a.secret)
	}
	return ""
}

// keysPoolID returns the pool the keys Secret's opcert belongs to, or "" when
// it cannot be read: a copy of the same bundle under another Secret name still
// hashes to the same cold key, which poolId (optional) does not reveal.
func (r *DingoNodeReconciler) keysPoolID(
	ctx context.Context,
	namespace, secretName string,
) string {
	if r.APIReader == nil || secretName == "" {
		return ""
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: namespace}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		return ""
	}
	cert, _, err := opCertFile(secret.Data)
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
) forgerIdentity {
	id := forgerIdentity{namespace: dn.Namespace}
	if bp := dn.Spec.BlockProducer; bp != nil {
		id.secret = bp.Keys.SecretRef
		if p := normalizedPoolID(bp.PoolID); p != "" {
			id.pools = append(id.pools, p)
		}
	}
	if p := r.keysPoolID(ctx, id.namespace, id.secret); p != "" {
		id.pools = append(id.pools, p)
	}
	return id
}

// forgerHeld returns an error when dn may not run its keys yet: an older live
// block producer holds them (by spec, or by the cold key in the Secret), or a
// workload of a DingoNode that is gone or being deleted still does. The
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
	mine := r.nodeIdentity(ctx, dn)
	live := make(map[types.NamespacedName]bool, len(all))
	for i := range all {
		o := &all[i]
		if !o.DeletionTimestamp.IsZero() {
			continue
		}
		live[client.ObjectKeyFromObject(o)] = true
		if client.ObjectKeyFromObject(o) == client.ObjectKeyFromObject(dn) ||
			!resources.IsBlockProducer(o) || !forgerOutranks(o, dn) {
			continue
		}
		if what := mine.sharedWith(r.nodeIdentity(ctx, o)); what != "" {
			return fmt.Errorf(
				"block producer %s/%s already forges with the same %s; only "+
					"one DingoNode may stage a pool's keys",
				o.Namespace, o.Name, what,
			)
		}
	}
	return r.orphanForger(ctx, mine, live)
}

// orphanForger returns an error when a block-producer workload whose DingoNode
// is gone or deleting is still present with keys dn shares. A StatefulSet at
// zero replicas with no pods is down.
func (r *DingoNodeReconciler) orphanForger(
	ctx context.Context,
	mine forgerIdentity,
	live map[types.NamespacedName]bool,
) error {
	selector := client.MatchingLabels{
		"app.kubernetes.io/managed-by": resources.ManagedByName,
		"app.kubernetes.io/component":  resources.ComponentBlockProducer,
	}
	var sets appsv1.StatefulSetList
	if err := r.List(ctx, &sets, selector); err != nil {
		return fmt.Errorf("list block producer StatefulSets: %w", err)
	}
	for i := range sets.Items {
		s := &sets.Items[i]
		if s.Spec.Replicas != nil && *s.Spec.Replicas == 0 {
			continue
		}
		if err := r.orphanHolds(
			ctx, mine, live, "StatefulSet", s.Namespace, s.Name, s.Name,
			s.Spec.Template.Spec,
		); err != nil {
			return err
		}
	}

	// Pods are listed uncached so no cluster-wide Pod informer is held.
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, selector); err != nil {
		return fmt.Errorf("list block producer pods: %w", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded ||
			p.Status.Phase == corev1.PodFailed {
			continue
		}
		if err := r.orphanHolds(
			ctx, mine, live, "pod", p.Namespace, p.Name,
			p.Labels[resources.NodeLabel], p.Spec,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *DingoNodeReconciler) orphanHolds(
	ctx context.Context,
	mine forgerIdentity,
	live map[types.NamespacedName]bool,
	kind, namespace, name, owner string,
	spec corev1.PodSpec,
) error {
	if live[types.NamespacedName{Namespace: namespace, Name: owner}] {
		return nil
	}
	secret, ok := resources.KeysSecretName(spec)
	if !ok {
		return nil
	}
	theirs := forgerIdentity{namespace: namespace, secret: secret}
	if p := r.keysPoolID(ctx, namespace, secret); p != "" {
		theirs.pools = append(theirs.pools, p)
	}
	if what := mine.sharedWith(theirs); what != "" {
		return fmt.Errorf(
			"%s %s/%s of a block producer that is gone or deleting still "+
				"holds the same %s; waiting for it to stop",
			kind, namespace, name, what,
		)
	}
	return nil
}
