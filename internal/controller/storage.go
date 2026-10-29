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
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	condStorageReady = "StorageReady"
	// legacyDataVolumeName is the data claim template name of StatefulSets
	// created before the default became resources.DefaultDataVolumeName.
	legacyDataVolumeName = "data"
)

// claimOrdinal returns the ordinal of a StatefulSet claim named
// <template>-<node>-<ordinal>, rejecting other nodes and non-canonical ordinals.
func claimOrdinal(name, template, node string) (int64, bool) {
	suffix, ok := strings.CutPrefix(name, template+"-"+node+"-")
	if !ok {
		return 0, false
	}
	ordinal, err := strconv.ParseInt(suffix, 10, 32)
	if err != nil || ordinal < 0 || strconv.FormatInt(ordinal, 10) != suffix {
		return 0, false
	}
	return ordinal, true
}

func (r *DingoNodeReconciler) reconcileStorage(
	ctx context.Context, dn *dingov1alpha1.DingoNode,
) error {
	condition := metav1.Condition{
		Type: condStorageReady, Status: metav1.ConditionTrue,
		Reason: "VolumesReady", Message: "data volumes have the requested capacity",
		ObservedGeneration: dn.Generation,
	}
	defer func() { meta.SetStatusCondition(&dn.Status.Conditions, condition) }()
	fail := func(err error) error {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "VolumeResizeFailed"
		condition.Message = err.Error()
		return err
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, claims, client.InNamespace(dn.Namespace),
		client.MatchingLabels{"cardano_network": dn.Spec.Network}); err != nil {
		return fail(fmt.Errorf("list data claims: %w", err))
	}
	// Claim templates are immutable. Resize the claims instead, including retained
	// ordinals while scaled down, without adopting them into DingoNode garbage collection.
	dataName, err := r.dataVolumeName(ctx, dn)
	if err != nil {
		return fail(err)
	}
	template := resources.BuildStatefulSet(
		dn,
		resources.RenderOptions{DataVolumeName: dataName},
	).Spec.VolumeClaimTemplates[0]
	size := template.Spec.Resources.Requests[corev1.ResourceStorage]
	activeClaims := int64(0)
	replicas := int64(desiredReplicas(dn))
	for i := range claims.Items {
		pvc := &claims.Items[i]
		ordinal, ok := claimOrdinal(pvc.Name, template.Name, dn.Name)
		if !ok {
			continue
		}
		if ordinal < replicas {
			activeClaims++
		}
		if err := r.expandClaim(ctx, pvc, size); err != nil {
			return fail(fmt.Errorf("resize data claim %s: %w", pvc.Name, err))
		}
		capacity := pvc.Status.Capacity.Storage()
		requested := pvc.Spec.Resources.Requests.Storage()
		if capacity == nil || pvc.Status.Phase != corev1.ClaimBound {
			condition.Status = metav1.ConditionFalse
			condition.Reason = "ClaimsPending"
			condition.Message = "waiting for data volume claims to bind"
		} else if requested != nil && capacity.Cmp(*requested) < 0 ||
			claimResizePending(pvc) {
			condition.Status = metav1.ConditionFalse
			condition.Reason = "ExpansionPending"
			condition.Message = "waiting for data volume capacity or filesystem expansion"
		}
	}
	if activeClaims < replicas {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "ClaimsPending"
		condition.Message = "waiting for StatefulSet data claims"
	}

	return nil
}

func (r *DingoNodeReconciler) expandClaim(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	size resource.Quantity,
) error {
	key := client.ObjectKeyFromObject(pvc)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, key, pvc); err != nil {
			return err
		}
		if !pvc.DeletionTimestamp.IsZero() {
			return errors.New("claim is being deleted")
		}
		current := pvc.Spec.Resources.Requests.Storage()
		// A stale spec or concurrent manual expansion must never shrink a volume.
		if current.Cmp(size) >= 0 {
			return nil
		}
		before := pvc.DeepCopy()
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = size.DeepCopy()
		err := r.Patch(ctx, pvc, client.MergeFromWithOptions(
			before, client.MergeFromWithOptimisticLock{},
		))
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("claim disappeared during expansion: %w", err)
		}
		return err
	})
}

func claimResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Status == corev1.ConditionTrue &&
			(condition.Type == corev1.PersistentVolumeClaimResizing ||
				condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending) {
			return true
		}
	}
	return false
}
