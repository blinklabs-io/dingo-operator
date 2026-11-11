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

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The key bundle watch. Block-producer Secrets are read only through the
// uncached APIReader, so a delivered bundle used to wait for the periodic
// requeue. With key namespaces configured, the operator watches Secret
// metadata in those namespaces and enqueues the DingoNodes that reference the
// changed Secret; the reconcile then reads it through the APIReader as before.
//
// Only metadata is watched, so no key material enters the operator's cache.
// The watch still needs list and watch on Secrets in the key namespaces, and
// the cache is scoped to them because the operator holds no cluster-wide
// Secret access. Without key namespaces nothing is watched and a delivered
// bundle applies on the requeue, as before.

// KeyBundleCacheOptions scopes the manager's Secret cache to the key
// namespaces. Only Secret metadata is ever cached; the scope keeps the
// informer's list and watch inside the namespaces the operator may read.
func KeyBundleCacheOptions(namespaces []string) cache.Options {
	if len(namespaces) == 0 {
		return cache.Options{}
	}
	scoped := make(map[string]cache.Config, len(namespaces))
	for _, ns := range namespaces {
		scoped[ns] = cache.Config{}
	}
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {Namespaces: scoped},
		},
	}
}

// keyBundleRefs returns the Secrets a DingoNode reads during reconcile: the
// keys Secret, the signing agent's client certificate, and the Bursa cold
// signer's mTLS credentials.
func keyBundleRefs(dn *dingov1alpha1.DingoNode) []string {
	bp := dn.Spec.BlockProducer
	if bp == nil {
		return nil
	}
	refs := []string{bp.Keys.SecretRef}
	if bp.SigningAgent != nil && bp.SigningAgent.ClientCertSecretRef != "" {
		refs = append(refs, bp.SigningAgent.ClientCertSecretRef)
	}
	if bp.Rotation.ColdSigner.MTLSSecretRef != "" {
		refs = append(refs, bp.Rotation.ColdSigner.MTLSSecretRef)
	}
	return refs
}

// dingoNodesForKeyBundle enqueues every DingoNode in the Secret's namespace
// that references it. A failed list is logged and enqueues nothing; the
// periodic requeue still applies the change.
func (r *DingoNodeReconciler) dingoNodesForKeyBundle(
	ctx context.Context,
	obj client.Object,
) []ctrl.Request {
	var nodes dingov1alpha1.DingoNodeList
	if err := r.List(
		ctx,
		&nodes,
		client.InNamespace(obj.GetNamespace()),
	); err != nil {
		log.FromContext(ctx).Error(
			err,
			"list DingoNodes for a key bundle",
			"namespace", obj.GetNamespace(),
		)
		return nil
	}
	var reqs []ctrl.Request
	for i := range nodes.Items {
		dn := &nodes.Items[i]
		for _, ref := range keyBundleRefs(dn) {
			if ref == obj.GetName() {
				reqs = append(reqs, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      dn.Name,
						Namespace: dn.Namespace,
					},
				})
				break
			}
		}
	}
	return reqs
}
