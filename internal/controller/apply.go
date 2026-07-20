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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// objectMeta returns a minimal ObjectMeta for a CreateOrUpdate target.
func objectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace}
}

func (r *DingoNodeReconciler) upsertServiceAccount(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	desired := resources.BuildServiceAccount(dn)
	sa := &corev1.ServiceAccount{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = desired.Labels
		sa.AutomountServiceAccountToken = desired.AutomountServiceAccountToken
		return ctrl.SetControllerReference(dn, sa, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertService(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	desired *corev1.Service,
) error {
	svc := &corev1.Service{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		// Preserve the API-assigned cluster IP on update (immutable).
		clusterIP := svc.Spec.ClusterIP
		clusterIPs := svc.Spec.ClusterIPs
		ipFamilies := svc.Spec.IPFamilies
		ipFamilyPolicy := svc.Spec.IPFamilyPolicy
		svc.Spec = desired.Spec
		if clusterIP != "" {
			svc.Spec.ClusterIP = clusterIP
			svc.Spec.ClusterIPs = clusterIPs
		}
		if len(ipFamilies) > 0 {
			svc.Spec.IPFamilies = ipFamilies
		}
		if ipFamilyPolicy != nil {
			svc.Spec.IPFamilyPolicy = ipFamilyPolicy
		}
		return ctrl.SetControllerReference(dn, svc, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertConfigMap(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	topologyJSON string,
) error {
	desired := resources.BuildTopologyConfigMap(dn, topologyJSON)
	cm := &corev1.ConfigMap{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return ctrl.SetControllerReference(dn, cm, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertStatefulSet(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	opts resources.RenderOptions,
) error {
	desired := resources.BuildStatefulSet(dn, opts)
	sts := &appsv1.StatefulSet{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = desired.Labels
		if sts.CreationTimestamp.IsZero() {
			// Create: set the full spec, including immutable fields.
			sts.Spec = desired.Spec
		} else {
			// Update: only mutable fields (selector, serviceName,
			// volumeClaimTemplates and podManagementPolicy are immutable).
			sts.Spec.Replicas = desired.Spec.Replicas
			sts.Spec.Template = desired.Spec.Template
			sts.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
			sts.Spec.MinReadySeconds = desired.Spec.MinReadySeconds
		}
		return ctrl.SetControllerReference(dn, sts, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertPDB(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	desired := resources.BuildPodDisruptionBudget(dn)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = desired.Labels
		if pdb.CreationTimestamp.IsZero() {
			pdb.Spec = desired.Spec
		} else {
			// The selector is immutable once set.
			pdb.Spec.MinAvailable = desired.Spec.MinAvailable
		}
		return ctrl.SetControllerReference(dn, pdb, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertNetworkPolicy(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	desired := resources.BuildNetworkPolicy(dn)
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		np.Labels = desired.Labels
		np.Spec = desired.Spec
		return ctrl.SetControllerReference(dn, np, r.Scheme)
	})
	return err
}

func (r *DingoNodeReconciler) upsertPodMonitor(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	desired := resources.BuildPodMonitor(dn)
	pm := &unstructured.Unstructured{}
	pm.SetGroupVersionKind(resources.PodMonitorGVK)
	pm.SetName(dn.Name)
	pm.SetNamespace(dn.Namespace)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pm, func() error {
		pm.SetLabels(desired.GetLabels())
		if pm.Object == nil {
			pm.Object = make(map[string]any)
		}
		pm.Object["spec"] = desired.Object["spec"]
		return ctrl.SetControllerReference(dn, pm, r.Scheme)
	})
	return err
}
