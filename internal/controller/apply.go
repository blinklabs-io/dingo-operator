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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// createOrUpdateWithRetry runs controllerutil.CreateOrUpdate, retrying on
// conflict.
//
// A conflict here is ordinary and self-inflicted: CreateOrUpdate reads through
// the manager's cache, and a reconcile that has just written an object can read
// its own pre-write revision on the next pass and submit a stale
// resourceVersion. Without a retry the whole reconcile fails, and because
// Reconcile returns before reconcileStatus runs, every condition that pass had
// computed — KeysValid and Degraded among them — is discarded until the next
// requeue. The work is idempotent, so retrying costs nothing and keeps a
// cache-timing artifact from suppressing real status.
//
// RetryOnConflict only retries genuine conflicts; every other error is returned
// unchanged. Each attempt re-enters CreateOrUpdate, which issues a fresh Get, so
// a retry operates on current state rather than resubmitting stale bytes.
func createOrUpdateWithRetry(
	ctx context.Context,
	c client.Client,
	obj client.Object,
	mutate controllerutil.MutateFn,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, c, obj, mutate)
		return err
	})
}

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
	err := createOrUpdateWithRetry(ctx, r.Client, sa, func() error {
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
	err := createOrUpdateWithRetry(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Annotations = desired.Annotations
		allocatedPorts := map[string]int32{}
		for _, p := range svc.Spec.Ports {
			allocatedPorts[p.Name] = p.NodePort
		}
		// Preserve the API-assigned cluster IP on update (immutable).
		clusterIP := svc.Spec.ClusterIP
		clusterIPs := svc.Spec.ClusterIPs
		ipFamilies := svc.Spec.IPFamilies
		ipFamilyPolicy := svc.Spec.IPFamilyPolicy
		svc.Spec = desired.Spec
		if svc.Spec.Type == corev1.ServiceTypeNodePort || svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].NodePort == 0 {
					svc.Spec.Ports[i].NodePort = allocatedPorts[svc.Spec.Ports[i].Name]
				}
			}
		}
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
	err := createOrUpdateWithRetry(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return ctrl.SetControllerReference(dn, cm, r.Scheme)
	})
	return err
}

// upsertStatefulSet applies the node's StatefulSet. keepTemplate leaves the pod
// template of an existing StatefulSet as it is live: set while the keys Secret
// is rejected or missing, so no template change, including one from an operator
// upgrade, rolls the pod onto the rejected Secret or onto one that cannot start.
// It reports whether a template change was withheld.
func (r *DingoNodeReconciler) upsertStatefulSet(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	opts resources.RenderOptions,
	keepTemplate bool,
) (bool, error) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: objectMeta(dn.Name, dn.Namespace),
	}
	var withheld *corev1.PodTemplateSpec
	err := createOrUpdateWithRetry(ctx, r.Client, sts, func() error {
		withheld = nil
		renderOpts := opts
		// Claim templates are immutable. Mount the data volume under the
		// template name of the StatefulSet this update is based on, so an
		// earlier stale read cannot render a mount for a claim it lacks.
		if !sts.CreationTimestamp.IsZero() &&
			len(sts.Spec.VolumeClaimTemplates) > 0 {
			renderOpts.DataVolumeName = sts.Spec.VolumeClaimTemplates[0].Name
		}
		// A held template is compared with the render, not applied. Carry the
		// live vote key state, as the keys checksum is, so the refused or
		// missing Secret alone does not count as a template change.
		if keepTemplate && !sts.CreationTimestamp.IsZero() {
			renderOpts.MountVoteKey = setsVoteKey(sts.Spec.Template)
		}
		desired := resources.BuildStatefulSet(dn, renderOpts)
		sts.Labels = desired.Labels
		if sts.CreationTimestamp.IsZero() {
			// Create: set the full spec, including immutable fields.
			sts.Spec = desired.Spec
		} else {
			// Update: only mutable fields (selector, serviceName,
			// volumeClaimTemplates and podManagementPolicy are immutable).
			sts.Spec.Replicas = desired.Spec.Replicas
			if keepTemplate {
				withheld = &desired.Spec.Template
			} else {
				sts.Spec.Template = desired.Spec.Template
			}
			sts.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
			sts.Spec.MinReadySeconds = desired.Spec.MinReadySeconds
			retention := desired.Spec.PersistentVolumeClaimRetentionPolicy
			sts.Spec.PersistentVolumeClaimRetentionPolicy = retention
		}
		return ctrl.SetControllerReference(dn, sts, r.Scheme)
	})
	if err != nil || withheld == nil {
		return false, err
	}
	// The live template carries server-side defaults the render lacks, so
	// compare against the render as the API server would store it.
	probe := sts.DeepCopy()
	probe.Spec.Template = *withheld
	if err := r.Update(ctx, probe, client.DryRunAll); err != nil {
		// A failed comparison cannot show that nothing is held, so report the
		// update as held.
		log.FromContext(ctx).Info(
			"unable to compare withheld pod template",
			"error", err.Error(),
		)
		return true, nil
	}
	return !equality.Semantic.DeepEqual(
		probe.Spec.Template,
		sts.Spec.Template,
	), nil
}

// setsVoteKey reports whether a pod template sets the vote key variable.
func setsVoteKey(tmpl corev1.PodTemplateSpec) bool {
	for _, c := range tmpl.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == resources.VoteKeyEnv {
				return true
			}
		}
	}
	return false
}

func (r *DingoNodeReconciler) upsertPDB(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	desired := resources.BuildPodDisruptionBudget(dn)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	err := createOrUpdateWithRetry(ctx, r.Client, pdb, func() error {
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
	err := createOrUpdateWithRetry(ctx, r.Client, np, func() error {
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
	err := createOrUpdateWithRetry(ctx, r.Client, pm, func() error {
		pm.SetLabels(desired.GetLabels())
		if pm.Object == nil {
			pm.Object = make(map[string]any)
		}
		pm.Object["spec"] = desired.Object["spec"]
		return ctrl.SetControllerReference(dn, pm, r.Scheme)
	})
	return err
}

// dataVolumeName returns the data claim template name for the node's
// StatefulSet, or "" to use the default. Claim templates are immutable, so an
// existing StatefulSet keeps its name. Without one, claims retained from an
// earlier StatefulSet decide it: dingo-data when any exist, as the dingo chart
// creates them, else data, as earlier operator versions did.
//
// Kubernetes binds a claim template to an existing PVC by name without
// checking its labels, so a retained claim under the selected name that is not
// labelled with the node's network would mount another network's database.
// Such a claim is refused rather than skipped.
func (r *DingoNodeReconciler) dataVolumeName(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (string, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      dn.Name,
		Namespace: dn.Namespace,
	}, sts)
	if err == nil {
		if len(sts.Spec.VolumeClaimTemplates) == 0 {
			return "", nil
		}
		return sts.Spec.VolumeClaimTemplates[0].Name, nil
	}
	if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("read statefulset: %w", err)
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, claims, client.InNamespace(dn.Namespace)); err != nil {
		return "", fmt.Errorf("list data claims: %w", err)
	}
	matched := map[string]bool{}
	foreign := map[string]string{}
	for i := range claims.Items {
		pvc := &claims.Items[i]
		for _, template := range []string{
			resources.DefaultDataVolumeName, legacyDataVolumeName,
		} {
			if _, ok := claimOrdinal(pvc.Name, template, dn.Name); !ok {
				continue
			}
			if pvc.Labels["cardano_network"] == dn.Spec.Network {
				matched[template] = true
			} else if _, seen := foreign[template]; !seen {
				foreign[template] = pvc.Name
			}
		}
	}
	selected := resources.DefaultDataVolumeName
	if !matched[selected] && matched[legacyDataVolumeName] {
		selected = legacyDataVolumeName
	}
	if name, ok := foreign[selected]; ok {
		return "", fmt.Errorf(
			"retained claim %s is not labelled cardano_network=%s and would be mounted as this node's data volume",
			name, dn.Spec.Network,
		)
	}
	if selected == resources.DefaultDataVolumeName && !matched[selected] {
		return "", nil
	}
	return selected, nil
}
