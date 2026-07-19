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

// Package controller contains the DingoNode reconciler.
package controller

import (
	"context"
	"fmt"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/topology"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// requeueInterval refreshes status (KES/forge state) periodically.
	requeueInterval = 2 * time.Minute

	condReady       = "Ready"
	condDegraded    = "Degraded"
	condRotationDue = "RotationDue"

	metricsPort = 12798
)

// DingoNodeReconciler reconciles a DingoNode object.
type DingoNodeReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	ForgeStatus   forgestatus.Fetcher
	PodMonitorCRD bool // whether the PodMonitor CRD is installed
}

// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

// Reconcile drives a DingoNode toward its desired state.
func (r *DingoNodeReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	dn := &dingov1alpha1.DingoNode{}
	if err := r.Get(ctx, req.NamespacedName, dn); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !dn.DeletionTimestamp.IsZero() {
		// Owner references handle garbage collection; nothing else to do.
		return ctrl.Result{}, nil
	}

	if err := validateSpec(dn); err != nil {
		logger.Info("invalid DingoNode spec", "error", err.Error())
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type:               condDegraded,
			Status:             metav1.ConditionTrue,
			Reason:             "InvalidSpec",
			Message:            err.Error(),
			ObservedGeneration: dn.Generation,
		})
		dn.Status.Phase = "Degraded"
		return ctrl.Result{}, r.updateStatus(ctx, dn)
	}

	if err := r.reconcileResources(ctx, dn); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileStatus(ctx, dn); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileResources applies all child objects for the node.
func (r *DingoNodeReconciler) reconcileResources(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	topoJSON, hasTopo, err := topology.Render(dn, dn.Namespace)
	if err != nil {
		return err
	}

	opts := resources.RenderOptions{
		HasTopology: hasTopo,
		Replicas:    desiredReplicas(dn),
		MountKeys:   resources.IsBlockProducer(dn),
	}
	if hasTopo {
		opts.TopologyChecksum = checksum(topoJSON)
	}

	if err := r.upsertServiceAccount(ctx, dn); err != nil {
		return fmt.Errorf("apply serviceaccount: %w", err)
	}
	if err := r.upsertService(ctx, dn, resources.BuildHeadlessService(dn)); err != nil {
		return fmt.Errorf("apply headless service: %w", err)
	}
	if err := r.upsertService(ctx, dn, resources.BuildClientService(dn)); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}
	if hasTopo {
		if err := r.upsertConfigMap(ctx, dn, topoJSON); err != nil {
			return fmt.Errorf("apply topology configmap: %w", err)
		}
	}
	if err := r.upsertStatefulSet(ctx, dn, opts); err != nil {
		return fmt.Errorf("apply statefulset: %w", err)
	}
	if resources.IsBlockProducer(dn) {
		if err := r.upsertPDB(ctx, dn); err != nil {
			return fmt.Errorf("apply poddisruptionbudget: %w", err)
		}
		if err := r.upsertNetworkPolicy(ctx, dn); err != nil {
			return fmt.Errorf("apply networkpolicy: %w", err)
		}
	}

	// PodMonitor is best-effort and skipped when the CRD is absent.
	if dn.Spec.Metrics.PodMonitor.Enabled && r.PodMonitorCRD {
		if err := r.upsertPodMonitor(ctx, dn); err != nil {
			log.FromContext(ctx).V(1).Info(
				"unable to apply PodMonitor",
				"error",
				err,
			)
		}
	}
	return nil
}

// reconcileStatus refreshes the DingoNode status from live children.
func (r *DingoNodeReconciler) reconcileStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	sts := &appsv1.StatefulSet{}
	ready := false
	if err := r.Get(ctx, types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}, sts); err == nil {
		ready = sts.Status.ReadyReplicas > 0 &&
			sts.Status.ReadyReplicas == desiredReplicas(dn)
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	if resources.IsBlockProducer(dn) {
		r.refreshForgeStatus(ctx, dn)
	}

	if ready {
		dn.Status.Phase = "Ready"
		if resources.IsBlockProducer(dn) && dn.Status.KES.RemainingPeriods > 0 {
			dn.Status.Phase = "Forging"
		}
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionTrue, Reason: "StatefulSetReady",
			Message: "node is ready", ObservedGeneration: dn.Generation,
		})
	} else {
		dn.Status.Phase = "Pending"
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionFalse, Reason: "StatefulSetNotReady",
			Message: "waiting for node to become ready", ObservedGeneration: dn.Generation,
		})
	}
	meta.RemoveStatusCondition(&dn.Status.Conditions, condDegraded)
	return r.updateStatus(ctx, dn)
}

// refreshForgeStatus scrapes the node's metrics to populate KES status. Errors
// are non-fatal: a not-yet-ready node simply has no metrics.
func (r *DingoNodeReconciler) refreshForgeStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	if r.ForgeStatus == nil {
		return
	}
	logger := log.FromContext(ctx)
	url := fmt.Sprintf(
		"http://%s.%s.svc.cluster.local:%d/metrics",
		dn.Name,
		dn.Namespace,
		metricsPort,
	)
	st, err := r.ForgeStatus.Fetch(ctx, url)
	if err != nil {
		logger.V(1).Info("forge status unavailable", "error", err.Error())
		return
	}
	if !st.HasKESData {
		return
	}
	dn.Status.KES = dingov1alpha1.KESStatus{
		CurrentPeriod:    st.CurrentKESPeriod,
		OpCertKESPeriod:  st.OpCertStartKESPeriod,
		ExpiryPeriod:     st.OpCertExpiryKESPeriod,
		RemainingPeriods: st.RemainingKESPeriods,
	}

	renewBefore := int64(dn.Spec.BlockProducer.Rotation.RenewBeforePeriods)
	if renewBefore > 0 && st.RemainingKESPeriods <= renewBefore {
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condRotationDue, Status: metav1.ConditionTrue, Reason: "KESExpiringSoon",
			Message: fmt.Sprintf(
				"%d KES periods remaining (threshold %d)",
				st.RemainingKESPeriods,
				renewBefore,
			),
			ObservedGeneration: dn.Generation,
		})
	} else {
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condRotationDue, Status: metav1.ConditionFalse, Reason: "KESHealthy",
			Message:            fmt.Sprintf("%d KES periods remaining", st.RemainingKESPeriods),
			ObservedGeneration: dn.Generation,
		})
	}
}

// updateStatus writes the status subresource, tracking observedGeneration.
func (r *DingoNodeReconciler) updateStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	status := dn.Status
	// Re-fetch and retry on conflict: the cached object we reconciled from can
	// lag a prior status write, causing a benign resourceVersion conflict.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dingov1alpha1.DingoNode{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(dn), latest); err != nil {
			return err
		}
		latest.Status = status
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

// desiredReplicas returns the StatefulSet replica count for the node's role and
// HA strategy. Block producers run a single forging pod in v1; active/standby
// (multiple pods, one keyed) is handled by the HA controller in a later phase.
func desiredReplicas(dn *dingov1alpha1.DingoNode) int32 {
	if resources.IsBlockProducer(dn) {
		return 1
	}
	if dn.Spec.Replicas != nil {
		return *dn.Spec.Replicas
	}
	return 1
}

// SetupWithManager registers the reconciler and its owned types.
func (r *DingoNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dingov1alpha1.DingoNode{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("dingonode").
		Complete(r)
}
