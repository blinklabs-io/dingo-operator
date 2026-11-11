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
	"slices"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The mappers below enqueue the DingoNodes that reference an object the
// operator does not own, so a change to it is applied at once instead of on
// the next periodic reconcile. Each lists the DingoNodes in the object's
// namespace and filters in Go rather than through a field index: a namespace
// holds few DingoNodes, and the same code then works against the API server.
// A failed list is logged and enqueues nothing; the periodic requeue still
// applies the change.
//
// The keys Secret is deliberately not mapped. Watching it would need a Secret
// informer, and list/watch on Secrets, which this operator must not have.

// dingoNodesForConfigMap enqueues every DingoNode whose configRef names the
// ConfigMap.
func (r *DingoNodeReconciler) dingoNodesForConfigMap(
	ctx context.Context,
	obj client.Object,
) []ctrl.Request {
	return r.referrers(ctx, obj.GetNamespace(), func(
		dn *dingov1alpha1.DingoNode,
	) bool {
		return dn.Spec.ConfigRef == obj.GetName()
	})
}

// dingoNodesForRelayRef enqueues every DingoNode whose topology.relayRefs
// names the DingoNode, so a peer's relay port change, creation or deletion
// reaches the topology of the nodes that dial it.
func (r *DingoNodeReconciler) dingoNodesForRelayRef(
	ctx context.Context,
	obj client.Object,
) []ctrl.Request {
	return r.relayRefReferrers(ctx, obj.GetNamespace(), obj.GetName())
}

// dingoNodesForPeerStatefulSet enqueues every DingoNode whose
// topology.relayRefs names the DingoNode controlling the StatefulSet. A block
// producer whose pod template is held serves the relay port of its live
// template rather than its spec, so the port its peers dial changes when the
// template is released, without any change to the DingoNode.
func (r *DingoNodeReconciler) dingoNodesForPeerStatefulSet(
	ctx context.Context,
	obj client.Object,
) []ctrl.Request {
	owner := metav1.GetControllerOf(obj)
	if owner == nil || owner.Kind != "DingoNode" ||
		owner.APIVersion != dingov1alpha1.GroupVersion.String() {
		return nil
	}
	return r.relayRefReferrers(ctx, obj.GetNamespace(), owner.Name)
}

func (r *DingoNodeReconciler) relayRefReferrers(
	ctx context.Context,
	namespace, name string,
) []ctrl.Request {
	return r.referrers(ctx, namespace, func(dn *dingov1alpha1.DingoNode) bool {
		return dn.Name != name &&
			slices.Contains(dn.Spec.Topology.RelayRefs, name)
	})
}

func (r *DingoNodeReconciler) referrers(
	ctx context.Context,
	namespace string,
	refers func(*dingov1alpha1.DingoNode) bool,
) []ctrl.Request {
	var nodes dingov1alpha1.DingoNodeList
	if err := r.List(ctx, &nodes, client.InNamespace(namespace)); err != nil {
		log.FromContext(ctx).Error(
			err,
			"list DingoNodes for a referenced object",
			"namespace", namespace,
		)
		return nil
	}
	var reqs []ctrl.Request
	for i := range nodes.Items {
		dn := &nodes.Items[i]
		if refers(dn) {
			reqs = append(reqs, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      dn.Name,
					Namespace: dn.Namespace,
				},
			})
		}
	}
	return reqs
}
