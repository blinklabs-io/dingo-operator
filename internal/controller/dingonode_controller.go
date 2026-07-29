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
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// requeueInterval refreshes status (KES/forge state) periodically.
	requeueInterval = 2 * time.Minute

	condReady          = "Ready"
	condDegraded       = "Degraded"
	condRotationDue    = "RotationDue"
	condKeysValid      = "KeysValid"
	condOnChainCounter = "OnChainCounterAvailable"

	metricsPort = 12798
)

// DingoNodeReconciler reconciles a DingoNode object.
type DingoNodeReconciler struct {
	client.Client
	// APIReader is an uncached reader used to fetch the one named keys Secret on
	// demand. It bypasses the manager's cache so the operator never holds a
	// cluster-wide Secret informer/watch/cache.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	// Recorder surfaces refused key material as an Event on the DingoNode, so a
	// human can see why a delivered opcert was not rolled out. Optional: a nil
	// Recorder only loses the Event, never the status condition.
	Recorder    events.EventRecorder
	ForgeStatus forgestatus.Fetcher
	// OnChain reads the authoritative on-chain opcert counter from the node over
	// node-to-client. Optional: a nil Fetcher (or a node that does not expose
	// node-to-client) leaves status.opcert.onChainCounter unpopulated, and
	// counter validation falls back to the operator's own last accepted counter.
	OnChain       onchain.Fetcher
	PodMonitorCRD bool // whether the PodMonitor CRD is installed

	// onChainMu guards onChainAttempts.
	onChainMu sync.Mutex
	// onChainAttempts remembers, per node, when the operator last *attempted* an
	// on-chain counter read and what came of it.
	//
	// Rate-limiting has to key off attempts, not results. Gating on
	// status.opcert.onChainCounterAt — which only the success path writes — leaves
	// the failing case ungated: an operator whose pod is not labelled for the
	// node-to-client NetworkPolicy never succeeds, so the timestamp stays nil and
	// every reconcile burns the full dial timeout again. That is the state this
	// feature ships in until the Helm chart sets the label, and with
	// MaxConcurrentReconciles at its default of 1 it is enough to starve every
	// other node's rollout.
	//
	// Caching the last outcome also means a reconcile that skips the dial can
	// still restore the observation into status and re-assert the condition, so a
	// successful read is not lost when reconcileResources fails after it (the
	// reconcile returns before reconcileStatus persists anything) and a
	// controller-runtime backoff retry does not re-dial.
	//
	// In-memory on purpose: it resets on operator restart, which only means one
	// fresh read per node, and the persisted status still carries the floor across
	// the restart. Entries are dropped when a node is deleted (see Reconcile), so
	// the map cannot grow without bound.
	//
	// The cost, stated plainly: after a transient node outage the floor is
	// unavailable for up to one refresh interval rather than until the next
	// reconcile, because the recovered node is not re-dialled immediately. That is
	// well inside onChainCounterMaxAge and fails open in the same direction the
	// check already does, which is why five minutes is the chosen trade.
	onChainAttempts map[types.NamespacedName]onChainAttempt
}

// onChainAttempt is what the reconciler remembers between reconciles about one
// node's on-chain counter read.
type onChainAttempt struct {
	// at is when the read was attempted, successful or not.
	at time.Time
	// reason and message are the OnChainCounterAvailable condition the attempt
	// produced, re-asserted on reconciles that skip the dial.
	reason  string
	message string
	// counter, observedAt and poolID are set only when the attempt yielded a
	// usable counter. poolID is the normalised pool the counter belongs to.
	counter    int64
	observedAt time.Time
	poolID     string
	// ok reports whether counter/observedAt/poolID are set.
	ok bool
}

// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dingo.blinklabs.io,resources=dingonodes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
//
// The operator also reads block-producer key Secrets (get-only, uncached) to
// detect key/opcert rotation. That grant is deliberately NOT declared here:
// adding secrets to this always-cluster-wide manager ClusterRole would give
// read access to every Secret in the cluster. The deployment provides it
// instead — the Helm chart renders a cluster-wide keys-reader ClusterRole by
// default, or namespaced Roles when rbac.keySecretsNamespaces is set.
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
		if apierrors.IsNotFound(err) {
			r.forgetOnChainAttempt(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !dn.DeletionTimestamp.IsZero() {
		// Owner references handle garbage collection; nothing else to do.
		r.forgetOnChainAttempt(req.NamespacedName)
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
		// An invalid spec is not Ready; clear any stale Ready=True so the node
		// does not report Ready and Degraded simultaneously.
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type:               condReady,
			Status:             metav1.ConditionFalse,
			Reason:             "InvalidSpec",
			Message:            err.Error(),
			ObservedGeneration: dn.Generation,
		})
		dn.Status.Phase = "Degraded"
		return ctrl.Result{}, r.updateStatus(ctx, dn)
	}

	// Before reconcileResources, because reconcileResources is what validates a
	// delivered key bundle and acts on the verdict. Read the counter after it and
	// the floor is missing on the one pass that acts.
	//
	// What that buys depends on whether the node already exists, and it is worth
	// being exact because restore-from-backup is this check's primary case:
	//   - StatefulSet already present: the refusal is a genuine roll-prevention.
	//     The live keys-checksum is carried forward, the pod template stays
	//     byte-identical, and the running process keeps forging on its loaded
	//     keys.
	//   - Fresh cluster, no StatefulSet yet: nothing is prevented.
	//     reconcileResources applies the StatefulSet unconditionally, the keys
	//     Secret is mounted, and Dingo CrashLoops on the below-chain opcert
	//     exactly as it would have. What the ordering buys here is *correct
	//     signalling* — KeysValid=False, Degraded, a Warning Event, and no
	//     published onDiskCounter — instead of the operator reporting a healthy
	//     rotation it had not yet checked. Same distinction as "refusing declines
	//     to initiate a roll, it does not fence one" in CLAUDE.md.
	//
	// The attempt gate inside keeps this to at most one dial per node per refresh
	// interval, so moving it ahead of resource application does not put a dead
	// dial in front of every node's rollout.
	if resources.IsBlockProducer(dn) {
		r.refreshOnChainCounter(ctx, dn)
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
	// Roll the block-producer pod when its mounted key Secret changes so an
	// externally-delivered key/opcert swap takes effect. The Secret is read via
	// the uncached APIReader (get-only, no informer/watch) to avoid holding the
	// cluster-wide Secret cache this operator must not have.
	if bp := dn.Spec.BlockProducer; resources.IsBlockProducer(dn) &&
		r.APIReader != nil && bp != nil {
		secret := &corev1.Secret{}
		key := types.NamespacedName{
			Name:      bp.Keys.SecretRef,
			Namespace: dn.Namespace,
		}
		if err := r.APIReader.Get(ctx, key, secret); err == nil {
			state, verr := validateKeysSecret(secret, dn)
			if verr != nil {
				// Refuse the delivered bundle: carry the checksum already on
				// the live pod template forward so the rendered template stays
				// byte-identical and nothing rolls. Leaving the checksum empty
				// would *remove* the annotation, which is itself a template
				// change and would roll the pod onto the rejected keys.
				prev, err := r.liveKeysChecksum(ctx, dn)
				if err != nil {
					return err
				}
				opts.KeysChecksum = prev
				r.rejectKeys(ctx, dn, verr)
			} else {
				opts.KeysChecksum = keysChecksum(secret)
				dn.Status.OpCert.OnDiskCounter = state.Counter
				meta.SetStatusCondition(
					&dn.Status.Conditions,
					metav1.Condition{
						Type:   condKeysValid,
						Status: metav1.ConditionTrue,
						Reason: "OpCertAccepted",
						Message: fmt.Sprintf(
							"opcert counter %d, kes start period %d",
							state.Counter, state.KESPeriod,
						),
						ObservedGeneration: dn.Generation,
					},
				)
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read keys secret: %w", err)
		}
		// NotFound: the keys Secret is absent; leave the checksum empty. The pod
		// cannot start without it, which is surfaced elsewhere.
	}
	// Roll the pod when the referenced config bundle (config.json + genesis)
	// changes. The ConfigMap is non-secret and already cached by the manager, so
	// the ordinary cached client is used here (unlike the keys Secret above).
	if dn.Spec.ConfigRef != "" {
		cm := &corev1.ConfigMap{}
		key := types.NamespacedName{
			Name:      dn.Spec.ConfigRef,
			Namespace: dn.Namespace,
		}
		if err := r.Get(ctx, key, cm); err == nil {
			opts.ConfigChecksum = configChecksum(cm)
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read config bundle: %w", err)
		}
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

// rejectKeys records that the delivered key bundle was refused: a log line, a
// Warning Event so the reason is visible with `kubectl describe`, and a
// KeysValid=False condition. It deliberately does not fail the reconcile:
// the node keeps running on its last known-good keys, the safe state.
func (r *DingoNodeReconciler) rejectKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	verr error,
) {
	var secretRef string
	if bp := dn.Spec.BlockProducer; bp != nil {
		secretRef = bp.Keys.SecretRef
	}
	log.FromContext(ctx).Info(
		"refusing delivered block-producer keys",
		"secret", secretRef,
		"error", verr.Error(),
	)
	if r.Recorder != nil {
		// "%s" rather than the error text as the format string: a validation
		// message can contain a literal % (hex/bech32 values cannot, but the
		// wrapped errors are not ours to constrain).
		r.Recorder.Eventf(
			dn,
			nil,
			corev1.EventTypeWarning,
			"OpCertRejected",
			"ValidateKeys",
			"%s",
			verr.Error(),
		)
	}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condKeysValid,
		Status:             metav1.ConditionFalse,
		Reason:             "OpCertRejected",
		Message:            verr.Error(),
		ObservedGeneration: dn.Generation,
	})
	// Degraded as well as KeysValid=False. The node keeps forging on the keys
	// its process already loaded, so every readiness signal stays green and
	// `kubectl get dingonode` would otherwise show a healthy row while
	// rotations have silently stopped and KES marches toward expiry.
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             "OpCertRejected",
		Message:            verr.Error(),
		ObservedGeneration: dn.Generation,
	})
}

// liveKeysChecksum returns the keys-checksum annotation currently on the
// node's StatefulSet pod template. Re-rendering with this value keeps the pod
// template unchanged, so a rejected key bundle cannot trigger a rollout.
//
// It reads through the same cached client controllerutil.CreateOrUpdate uses
// below, so both see one informer's view of the StatefulSet — but they are two
// separate cache reads with several API round-trips between them, and the cache
// can be updated in between. The guarantee is "the same source of truth", not
// "the same snapshot". The race needs a Secret to go bad inside the
// cache-propagation window of the very reconcile that just accepted new keys,
// so it is not worth restructuring for; the race-free shape, if it ever
// matters, is a PreserveKeysChecksum flag on resources.RenderOptions that
// copies the annotation inside the mutate func, from the object CreateOrUpdate
// itself read.
//
// Only a missing StatefulSet yields "" (nothing to carry forward, and an absent
// annotation is correct for a first render). Every other read failure is
// returned: "" is the one value that *removes* the annotation, so on the single
// path whose whole purpose is never rendering without the live checksum, an
// unclassified error must not be indistinguishable from absence.
func (r *DingoNodeReconciler) liveKeysChecksum(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (string, error) {
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}
	if err := r.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read statefulset for keys checksum: %w", err)
	}
	return sts.Spec.Template.Annotations[resources.KeysChecksumAnnotation], nil
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
	// Degraded is cleared here because everything that sets it earlier in the
	// reconcile returns before reaching this point — except rejectKeys, which
	// deliberately falls through so the rest of the resources still reconcile.
	// A refused key bundle must survive to the status write, so keep Degraded
	// while KeysValid is False.
	if !meta.IsStatusConditionFalse(dn.Status.Conditions, condKeysValid) {
		meta.RemoveStatusCondition(&dn.Status.Conditions, condDegraded)
	}
	return r.updateStatus(ctx, dn)
}

// refreshForgeStatus scrapes the node's metrics to populate KES status. Errors
// are non-fatal: a not-yet-ready node simply has no metrics.
func (r *DingoNodeReconciler) refreshForgeStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	if r.ForgeStatus == nil || bp == nil {
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

	renewBefore := int64(bp.Rotation.RenewBeforePeriods)
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

// refreshOnChainCounter reads the authoritative on-chain opcert counter from
// the node over node-to-client, publishing it as status.opcert.onChainCounter.
//
// Like refreshForgeStatus, every failure is non-fatal and leaves any previously
// observed value in place. That is deliberate for two different reasons. Not
// clearing keeps a usable floor: the on-chain counter only ever moves forward,
// so a value read an hour ago is still a valid lower bound, whereas clearing to
// zero would silently disable the counter check the moment the node blipped.
// Freshness is handled where the value is *used* (see onChainFloor in keys.go),
// not by throwing it away here.
//
// The OnChainCounterAvailable condition exists so this cannot fail invisibly. A
// node whose node-to-client port is unreachable — most likely because nothing
// carries the NetworkPolicy access label — would otherwise leave the operator
// permanently, silently falling back to the weaker on-disk check.
//
// Dials are rate-limited to onChainCounterRefreshInterval per node, keyed on
// the last *attempt* rather than the last success — see onChainAttempts for why
// the difference matters.
func (r *DingoNodeReconciler) refreshOnChainCounter(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	if r.OnChain == nil || bp == nil {
		return
	}
	// Node-to-client is off by default (Dingo binds it to loopback), so there is
	// nothing to query. Report it as an explicit Disabled rather than removing
	// the condition: an absent condition is indistinguishable from a feature
	// that was never wired up, and "looked applied but wasn't" is a failure mode
	// this repo has already been bitten by more than once.
	if !bp.NodeToClient.Enabled {
		r.setOnChainUnavailable(dn, "Disabled",
			"spec.blockProducer.nodeToClient.enabled is false, so the node "+
				"binds node-to-client to loopback and the on-chain counter "+
				"cannot be read; opcert counters are validated against "+
				"status.opcert.onDiskCounter only")
		return
	}

	logger := log.FromContext(ctx)
	key := client.ObjectKeyFromObject(dn)

	// Resolve the pool first: it identifies what any stored observation is *of*,
	// and the counter map is keyed by it.
	var wantPool string
	if bp.PoolID != "" {
		parsed, err := parsePoolID(bp.PoolID)
		if err != nil {
			r.setOnChainUnavailable(dn, "InvalidPoolID", err.Error())
			return
		}
		wantPool = parsed.String()
	}
	// Drop an observation that belongs to a different pool the moment poolId
	// changes, rather than letting the freshness window keep enforcing it for up
	// to onChainCounterMaxAge. Provenance is the whole justification for that
	// window, so an observation we know is of another pool must not survive it.
	if got := dn.Status.OpCert.OnChainCounterPoolID; got != "" &&
		got != wantPool {
		logger.V(1).Info(
			"discarding on-chain counter observed for another pool",
			"observedFor", got, "want", wantPool,
		)
		dn.Status.OpCert.OnChainCounter = 0
		dn.Status.OpCert.OnChainCounterAt = nil
		dn.Status.OpCert.OnChainCounterPoolID = ""
		r.forgetOnChainAttempt(key)
	}
	if wantPool == "" {
		r.setOnChainUnavailable(dn, "PoolIDUnset",
			"spec.blockProducer.poolId is required to look up the on-chain "+
				"opcert counter (it is keyed by the pool's cold-key hash)")
		return
	}

	// Within the interval, reuse what the last attempt learned instead of
	// dialling: restore the observation into status (the reconcile that fetched
	// it may have failed before persisting anything) and re-assert its condition.
	//
	// The pool check has to happen here too, not only against status above: a read
	// that succeeded for the previous pool and was then lost to a failed reconcile
	// leaves nothing in status to compare, so only the remembered attempt knows it
	// is of the wrong pool. Dropping it re-reads the new pool now rather than
	// after the interval, and keeps applyOnChainAttempt from publishing an
	// Observed condition about a pool that is no longer configured.
	if last, ok := r.lastOnChainAttempt(key); ok &&
		time.Since(last.at) < onChainCounterRefreshInterval {
		if last.ok && last.poolID != wantPool {
			r.forgetOnChainAttempt(key)
		} else {
			r.applyOnChainAttempt(dn, last, wantPool)
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
	addr := net.JoinHostPort(
		fmt.Sprintf("%s.%s.svc.cluster.local", dn.Name, dn.Namespace),
		strconv.Itoa(resources.PortNodeToClient),
	)
	poolID, err := parsePoolID(bp.PoolID)
	if err != nil {
		r.setOnChainUnavailable(dn, "InvalidPoolID", err.Error())
		return
	}

	// Record the attempt before making it, so a dial that fails — or a reconcile
	// that errors out after it — is rate-limited exactly like a success.
	attempt := onChainAttempt{at: time.Now()}
	counter, err := r.OnChain.Fetch(ctx, onchain.Query{
		Address:      addr,
		NetworkMagic: magic,
		PoolID:       poolID,
	})
	switch {
	case err != nil:
		logger.V(1).Info(
			"on-chain opcert counter unavailable", "error", err.Error(),
		)
		attempt.reason, attempt.message = r.onChainFailureReason(ctx, dn, err)
	case !counter.Found:
		attempt.reason = "PoolNotOnChain"
		attempt.message = fmt.Sprintf(
			"the chain has no opcert counter for pool %s yet; it has not "+
				"minted a block under an operational certificate",
			bp.PoolID,
		)
	default:
		attempt.ok = true
		attempt.counter = counter.Value
		attempt.observedAt = attempt.at
		attempt.poolID = wantPool
		attempt.reason = "Observed"
		attempt.message = fmt.Sprintf(
			"on-chain opcert counter %d for pool %s", counter.Value, wantPool,
		)
	}
	r.recordOnChainAttempt(key, attempt)
	r.applyOnChainAttempt(dn, attempt, wantPool)
}

// onChainFailureReason classifies a failed read. A node whose Service does not
// exist yet is simply not up — this reconcile may be the one creating it — and
// saying "check your NetworkPolicy label" there would be actively misleading on
// every healthy new block producer. The dial is still attempted first: skipping
// it until the Service exists would cost the counter on the very first pass,
// which is the pass that validates a delivered bundle.
func (r *DingoNodeReconciler) onChainFailureReason(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	cause error,
) (reason, message string) {
	svc := &corev1.Service{}
	key := client.ObjectKeyFromObject(dn)
	// Only an unambiguous NotFound justifies claiming the node is not up. Any
	// other read failure (RBAC, an API hiccup) establishes nothing about the
	// Service, so it must not be reported as if it did — fall through to the
	// reachability message instead.
	if err := r.Get(ctx, key, svc); apierrors.IsNotFound(err) {
		return "NodeNotReady", fmt.Sprintf(
			"the node's Service does not exist yet, so node-to-client cannot "+
				"be reached (%s); the on-chain counter will be read once the "+
				"node is up",
			cause.Error(),
		)
	} else if err != nil {
		log.FromContext(ctx).V(1).Info(
			"unable to read node service", "error", err.Error(),
		)
	}
	return "QueryFailed", fmt.Sprintf(
		"%s; until the node's node-to-client port is reachable, opcert "+
			"counters are only validated against status.opcert.onDiskCounter "+
			"(a client needs the label %s=%s on its pod, and on its namespace "+
			"when that differs from the node's)",
		cause.Error(),
		resources.NodeToClientAccessLabel,
		resources.NodeToClientAccessAllowed,
	)
}

// applyOnChainAttempt writes an attempt's outcome into status: the observation
// when it produced one, and its condition either way.
func (r *DingoNodeReconciler) applyOnChainAttempt(
	dn *dingov1alpha1.DingoNode,
	attempt onChainAttempt,
	wantPool string,
) {
	// A remembered observation of another pool says nothing about this one, and
	// its "Observed" reason would contradict a False condition. The caller drops
	// such entries and re-reads, so this is a guard rather than a live path.
	if attempt.ok && attempt.poolID != wantPool {
		return
	}
	if attempt.ok {
		dn.Status.OpCert.OnChainCounter = attempt.counter
		at := metav1.NewTime(attempt.observedAt)
		dn.Status.OpCert.OnChainCounterAt = &at
		dn.Status.OpCert.OnChainCounterPoolID = attempt.poolID
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type:               condOnChainCounter,
			Status:             metav1.ConditionTrue,
			Reason:             attempt.reason,
			Message:            attempt.message,
			ObservedGeneration: dn.Generation,
		})
		return
	}
	// A failed attempt leaves any previously observed counter alone. The on-chain
	// counter only ever moves forward, so a value read an hour ago is still a
	// valid lower bound, whereas clearing it to zero would silently disable the
	// check the moment the node blipped. Freshness is handled where the value is
	// *used* (onChainFloor in keys.go), not by throwing it away here.
	if attempt.reason != "" {
		r.setOnChainUnavailable(dn, attempt.reason, attempt.message)
	}
}

// lastOnChainAttempt returns what the operator remembers of the last on-chain
// counter read for a node.
func (r *DingoNodeReconciler) lastOnChainAttempt(
	key types.NamespacedName,
) (onChainAttempt, bool) {
	r.onChainMu.Lock()
	defer r.onChainMu.Unlock()
	attempt, ok := r.onChainAttempts[key]
	return attempt, ok
}

// recordOnChainAttempt remembers an attempt, rate-limiting the next one.
func (r *DingoNodeReconciler) recordOnChainAttempt(
	key types.NamespacedName,
	attempt onChainAttempt,
) {
	r.onChainMu.Lock()
	defer r.onChainMu.Unlock()
	if r.onChainAttempts == nil {
		r.onChainAttempts = make(map[types.NamespacedName]onChainAttempt)
	}
	r.onChainAttempts[key] = attempt
}

// forgetOnChainAttempt drops a node's remembered attempt, so the map does not
// retain deleted DingoNodes and a pool change re-reads immediately.
func (r *DingoNodeReconciler) forgetOnChainAttempt(key types.NamespacedName) {
	r.onChainMu.Lock()
	defer r.onChainMu.Unlock()
	delete(r.onChainAttempts, key)
}

// setOnChainUnavailable records that the on-chain counter could not be
// determined. It is not Degraded: an unsynced node, or a pool that has never
// minted, is a normal state, and validation falls back to the on-disk counter.
func (r *DingoNodeReconciler) setOnChainUnavailable(
	dn *dingov1alpha1.DingoNode,
	reason, message string,
) {
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condOnChainCounter,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: dn.Generation,
	})
}

// networkMagic resolves the network magic the node-to-client handshake must
// present: the explicit spec value if set, otherwise the magic of the named
// network. A network gouroboros does not know (e.g. "devnet") without an
// explicit magic cannot be handshaked, and reports false rather than guessing —
// a wrong magic is refused by the node, which would look like an unreachable
// node forever.
func networkMagic(dn *dingov1alpha1.DingoNode) (uint32, bool) {
	if m := dn.Spec.NetworkMagic; m != nil {
		if *m < 0 || *m > math.MaxUint32 {
			return 0, false
		}
		return uint32(*m), true
	}
	if network, ok := ouroboros.NetworkByName(dn.Spec.Network); ok {
		return network.NetworkMagic, true
	}
	return 0, false
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
