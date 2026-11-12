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
	"errors"
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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// requeueInterval refreshes status (KES/forge state) periodically.
	requeueInterval = 2 * time.Minute

	condReady          = "Ready"
	condDegraded       = "Degraded"
	condRotationDue    = "RotationDue"
	condKeysValid      = "KeysValid"
	condOnChainCounter = "OnChainCounterAvailable"
	// condPipelineHealthy reports whether the node is still following the
	// chain. A wedged ledger pipeline keeps the process live and the probes
	// green, so neither pod state nor Ready can express it.
	condPipelineHealthy = "LedgerPipelineHealthy"
	// reasonTipNotAdvancing marks a node whose tip slot has been unchanged for
	// longer than the stall threshold while its own pipeline gauges are clear.
	reasonTipNotAdvancing = "TipNotAdvancing"

	metricsPort = 12798

	// templateHeldMessage is appended to the KeysValid and Degraded messages
	// while a pod template change is withheld because the keys are refused or
	// missing.
	templateHeldMessage = "pod template update held until the keys Secret validates"
	// signingAgentCertHeldMessage replaces templateHeldMessage when the
	// template is held for the signing agent client certificate Secret.
	signingAgentCertHeldMessage = "pod template update held until the " +
		"signing agent client certificate Secret validates"
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
	// ColdSigner overrides the cold signer Auto rotation issues opcerts with.
	// Nil builds it from spec.blockProducer.rotation.coldSigner.
	ColdSigner ColdSignerFactory
	// TipStallThreshold is how long the node's tip slot may stay unchanged
	// before LedgerPipelineHealthy turns False. Zero disables the check.
	TipStallThreshold time.Duration
	// Now overrides the clock; nil uses time.Now.
	Now func() time.Time

	// issueMu guards issueFailures.
	issueMu sync.Mutex
	// issueFailures remembers, per node, the last Auto rotation that failed
	// once it had set out to reach the cold signer. RotationDue stays True
	// until a rotation lands, so without this a signer that is down, or a
	// Secret the operator may not update, would be retried on every reconcile:
	// a signing round trip (and a timeout, for an unreachable signer) each
	// time. In-memory for the same reasons as onChainAttempts.
	issueFailures map[types.NamespacedName]issueFailure

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
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
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
			r.forgetIssueFailure(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !dn.DeletionTimestamp.IsZero() {
		// Owner references handle garbage collection; nothing else to do.
		r.forgetOnChainAttempt(req.NamespacedName)
		r.forgetIssueFailure(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	if err := validateSpec(dn); err != nil {
		logger.Info("invalid DingoNode spec", "error", err.Error())
		// Resources are left as they are, but the running node is still
		// watched: its KES window keeps closing whatever the spec says. Observe
		// before setting the InvalidSpec conditions so they win over any
		// Degraded the observation raises. No autoRotate: it would run without
		// the validation that rejected this spec (for one, the mainnet refusal
		// of the in-cluster cold key).
		r.refreshForgeStatus(ctx, dn)
		if resources.IsBlockProducer(dn) && dn.Spec.BlockProducer == nil {
			// refreshForgeStatus reads KES only through spec.blockProducer, so
			// without it the earlier KES status would read as current.
			dn.Status.KES = dingov1alpha1.KESStatus{}
			setRotationDueUnknown(
				dn,
				"InvalidSpec",
				"spec.blockProducer is missing; KES status is not observed",
			)
		}
		// The template is held as well, and a pod recreated under it stages
		// what the keys Secret holds then.
		restages, rerr := r.restagesOtherPool(ctx, dn)
		if rerr != nil {
			return ctrl.Result{}, rerr
		}
		if restages {
			if herr := r.haltForger(ctx, dn); herr != nil {
				return ctrl.Result{}, herr
			}
		}
		// Requeue: nothing else reconciles the node until it or an owned
		// resource changes, which would stop the observation above.
		return ctrl.Result{RequeueAfter: requeueInterval}, r.markNotReady(
			ctx, dn, "InvalidSpec", err,
		)
	}

	// validateSpec sees one object; two block producers staging the same pool
	// keys would both forge, so this needs the other DingoNodes.
	if resources.IsBlockProducer(dn) {
		var all dingov1alpha1.DingoNodeList
		if err := r.List(ctx, &all); err != nil {
			return ctrl.Result{}, fmt.Errorf("list DingoNodes: %w", err)
		}
		if err := r.forgerHeld(ctx, dn, all.Items); err != nil {
			held, ok := errors.AsType[*forgerHeldError](err)
			if !ok {
				return ctrl.Result{}, fmt.Errorf(
					"check block producer keys holder: %w", err,
				)
			}
			logger.Info("duplicate block producer", "error", err.Error())
			// A node already running would keep forging beside the holder.
			// Evidence that does not verify only keeps the node from
			// starting or rolling: anyone who may create a pod can forge it.
			// forgerHeld still halts a node whose held template would stage
			// another pool's keys.
			if held.halt {
				if herr := r.haltForger(ctx, dn); herr != nil {
					return ctrl.Result{}, herr
				}
			}
			// Nothing watches the holder, so poll for it going away.
			return ctrl.Result{RequeueAfter: requeueInterval}, r.markNotReady(
				ctx, dn, reasonDuplicateForger, err,
			)
		}
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

	storageErr := r.reconcileStorage(ctx, dn)

	if err := r.reconcileStatus(ctx, dn); err != nil {
		return ctrl.Result{}, err
	}

	if storageErr != nil {
		return ctrl.Result{}, storageErr
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// markNotReady records a spec the operator refuses to run: Degraded, and not
// Ready, so a stale Ready=True is cleared rather than shown beside Degraded.
func (r *DingoNodeReconciler) markNotReady(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	reason string,
	cause error,
) error {
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: dn.Generation,
	})
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: dn.Generation,
	})
	dn.Status.Phase = "Degraded"
	return r.updateStatus(ctx, dn)
}

// haltForger scales the StatefulSet dn controls to zero so a refused block
// producer stops forging. An absent StatefulSet is already halted. The next
// reconcile that is not refused restores the replica count.
func (r *DingoNodeReconciler) haltForger(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	key := types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, key, sts); err != nil {
			return client.IgnoreNotFound(err)
		}
		// Only a StatefulSet this DingoNode controls is its workload. Another
		// one that merely has the name is left alone, so a DingoNode cannot be
		// used to stop a workload its author could not update.
		if !metav1.IsControlledBy(sts, dn) {
			return nil
		}
		if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0 {
			return nil
		}
		sts.Spec.Replicas = new(int32)
		return r.Update(ctx, sts)
	})
	if err != nil {
		return fmt.Errorf("scale statefulset %q to zero: %w", dn.Name, err)
	}
	return nil
}

// relayRefPorts resolves the relay port of every in-cluster node listed in
// topology.relayRefs, so peers are dialed on the port the referenced DingoNode
// serves rather than an assumed one. A ref with no DingoNode is left out and
// keeps the default port: it may be created later, or be a node this operator
// does not manage. The peer's creation, deletion or spec change enqueues this
// node (dingoNodesForRelayRef), as does a change to the peer's pod template,
// which is when a new port is served (dingoNodesForPeerStatefulSet).
func (r *DingoNodeReconciler) relayRefPorts(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (map[string]int32, error) {
	ports := make(map[string]int32, len(dn.Spec.Topology.RelayRefs))
	for _, ref := range dn.Spec.Topology.RelayRefs {
		peer := &dingov1alpha1.DingoNode{}
		key := types.NamespacedName{Name: ref, Namespace: dn.Namespace}
		if err := r.Get(ctx, key, peer); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get relayRef %q: %w", ref, err)
		}
		port, err := r.servedRelayPort(ctx, peer)
		if err != nil {
			return nil, fmt.Errorf("relayRef %q: %w", ref, err)
		}
		ports[ref] = port
	}
	return ports, nil
}

// servedRelayPort returns the relay port the node's pods listen on: the port
// of its live pod template. That differs from the spec port until the node's
// own reconcile updates its StatefulSet, and for as long as a block
// producer's template is held. A failed read is returned rather than answered
// with the spec port, which the live template may not serve; only an absent
// StatefulSet means the spec port.
func (r *DingoNodeReconciler) servedRelayPort(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (int32, error) {
	port := resources.RelayPort(dn)
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}
	if err := r.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return port, nil
		}
		return 0, fmt.Errorf("read statefulset %q: %w", dn.Name, err)
	}
	if live, ok := resources.TemplateRelayPort(sts.Spec.Template); ok {
		return live, nil
	}
	return port, nil
}

// liveConfigChecksum returns the config-bundle checksum the live pod template
// carries. Only a missing StatefulSet yields an empty result; every other read
// failure is returned, since an empty checksum removes the annotation and
// rolls the pod.
func (r *DingoNodeReconciler) liveConfigChecksum(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (string, error) {
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}
	if err := r.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read statefulset for config checksum: %w", err)
	}
	return sts.Spec.Template.Annotations[resources.ConfigChecksumAnnotation], nil
}

// reconcileResources applies all child objects for the node.
func (r *DingoNodeReconciler) reconcileResources(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	peerPorts, err := r.relayRefPorts(ctx, dn)
	if err != nil {
		return err
	}
	topoJSON, hasTopo, err := topology.Render(dn, dn.Namespace, peerPorts)
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
	// Claim templates are immutable: a new StatefulSet takes the name of
	// retained claims; upsertStatefulSet keeps an existing one's name.
	dataName, err := r.dataVolumeName(ctx, dn)
	if err != nil {
		return err
	}
	opts.DataVolumeName = dataName
	// Set when the keys Secret is rejected or missing: the live pod template is
	// kept.
	keepTemplate := false
	holdMessage := templateHeldMessage
	// The key bundle's rollout decision and the live keys checksum it was
	// made against. Its reload status is recorded once the template is known
	// not to be held from rolling.
	var decided *keysRollout
	decidedLive := ""
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
				// Refuse the delivered bundle: keep the live pod template so
				// nothing rolls. Any template change would replace the pod,
				// and the replacement would stage the rejected Secret; that
				// includes a template an earlier operator rendered. The
				// checksum already on the live template is carried forward as
				// well, so the rendered template never records the rejected
				// material.
				live, err := r.liveKeysTemplate(ctx, dn)
				if err != nil {
					return err
				}
				opts.KeysChecksum = live.checksum
				opts.KeysStaticChecksum = live.static
				keepTemplate = true
				r.rejectKeys(ctx, dn, verr)
			} else {
				// validateKeysSecret bound the opcert to poolId, so either
				// names the pool these keys forge for.
				pool := opCertPoolID(secret.Data)
				if pool == "" {
					pool = normalizedPoolID(bp.PoolID)
				}
				live, err := r.liveKeysTemplate(ctx, dn)
				if err != nil {
					return err
				}
				rollout := decideKeysRollout(
					resources.ReloadsKeysInPlace(dn), live,
					keysChecksum(secret), staticKeysChecksum(secret.Data),
					pool, state.Counter, dn.Status.OpCert.LoadedCounter,
					dn.Status.OpCert.RolledCounter,
					dn.Status.OpCert.InPlaceReload, r.now(),
				)
				opts.KeysChecksum = rollout.checksum
				opts.KeysStaticChecksum = rollout.static
				opts.KeysPool = pool
				opts.KeysCounter = strconv.FormatInt(state.Counter, 10)
				decided, decidedLive = &rollout, live.checksum
				if rollout.approve {
					if err := r.upsertKeysApprovalConfigMap(
						ctx, dn,
						approvedKeysDigest(
							secret.Data, resources.UsesSigningAgent(dn),
						),
					); err != nil {
						return fmt.Errorf("apply keys approval configmap: %w", err)
					}
				}
				if rollout.timedOut && r.Recorder != nil {
					r.Recorder.Eventf(
						dn, nil, corev1.EventTypeNormal, "KeysReloadTimedOut",
						"ReloadKeys",
						"node did not report opcert counter %d within %s; "+
							"rolling the pod onto it",
						state.Counter, keysReloadTimeout,
					)
				}
				voteData, _, _ := voteKeyFile(secret.Data)
				opts.MountVoteKey = voteData != nil
				dn.Status.OpCert.OnDiskCounter = state.Counter
				msg := fmt.Sprintf(
					"opcert counter %d, kes start period %d",
					state.Counter, state.KESPeriod,
				)
				if state.UnusedKESKey {
					msg += "; " + unusedKESKeyMessage
				}
				meta.SetStatusCondition(
					&dn.Status.Conditions,
					metav1.Condition{
						Type:               condKeysValid,
						Status:             metav1.ConditionTrue,
						Reason:             "OpCertAccepted",
						Message:            msg,
						ObservedGeneration: dn.Generation,
					},
				)
			}
		} else if apierrors.IsNotFound(err) {
			// The keys Secret is absent. Rendering without the checksum would
			// drop the annotation and replace the template of a healthy
			// forging pod with one that cannot start (the Secret volume is
			// not optional), so keep the live template and carry its checksum
			// forward, as for a rejected bundle. With no StatefulSet yet,
			// prev is empty and the first render proceeds unchanged.
			live, lerr := r.liveKeysTemplate(ctx, dn)
			if lerr != nil {
				return lerr
			}
			opts.KeysChecksum = live.checksum
			opts.KeysStaticChecksum = live.static
			keepTemplate = true
			r.missingKeys(ctx, dn, bp.Keys.SecretRef)
		} else {
			return fmt.Errorf("read keys secret: %w", err)
		}
		// The agent client certificate is mounted from its own Secret. One
		// that is missing, or holds only half the pair, fails the volume
		// mount, so hold the template rather than roll the forging pod onto
		// one that cannot start.
		if !keepTemplate {
			problem, certSum, err := r.checkSigningAgentCert(ctx, dn)
			if err != nil {
				return err
			}
			opts.SigningAgentCertChecksum = certSum
			if problem != "" {
				live, err := r.liveKeysTemplate(ctx, dn)
				if err != nil {
					return err
				}
				opts.KeysChecksum = live.checksum
				opts.KeysStaticChecksum = live.static
				opts.KeysPool = live.pool
				keepTemplate = true
				holdMessage = signingAgentCertHeldMessage
				r.rejectSigningAgentCert(ctx, dn, problem)
			}
		}
		// A roll held above never reaches the pod, so the status keeps
		// describing the running one: its rolled counter and any reload
		// still pending in it. An in-place load carries the live checksum
		// and is approved already, so it is recorded either way.
		if decided != nil &&
			(!keepTemplate || decided.checksum == decidedLive) {
			dn.Status.OpCert.InPlaceReload = decided.pending
			dn.Status.OpCert.RolledCounter = decided.rolled
		}
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
		} else if apierrors.IsNotFound(err) {
			// A briefly absent bundle (deleted and recreated) must not
			// change the template: an empty checksum drops the annotation
			// and rolls the pod onto a config volume that cannot mount, and
			// the bundle's return rolls it again. Carry the live checksum
			// forward; with no StatefulSet yet the first render has none.
			live, err := r.liveConfigChecksum(ctx, dn)
			if err != nil {
				return err
			}
			opts.ConfigChecksum = live
		} else {
			return fmt.Errorf("read config bundle: %w", err)
		}
	}

	// Services and the policy follow the port the live pod listens on: while
	// the template is held, a spec port change would otherwise cut off traffic
	// the pod cannot yet serve.
	served := dn
	if keepTemplate {
		port, err := r.servedRelayPort(ctx, dn)
		if err != nil {
			return err
		}
		if port != resources.RelayPort(dn) {
			// A shallow copy suffices: the builders only read the spec.
			shallow := *dn
			shallow.Spec.RelayService.Port = port
			served = &shallow
		}
	}

	if err := r.upsertServiceAccount(ctx, dn); err != nil {
		return fmt.Errorf("apply serviceaccount: %w", err)
	}
	if err := r.upsertService(ctx, dn, resources.BuildHeadlessService(served)); err != nil {
		return fmt.Errorf("apply headless service: %w", err)
	}
	for _, svc := range []*corev1.Service{
		resources.BuildRelayService(served),
		resources.BuildPrivateService(served),
		resources.BuildMetricsService(served),
	} {
		if err := r.upsertService(ctx, dn, svc); err != nil {
			return fmt.Errorf("apply Service %s: %w", svc.Name, err)
		}
	}
	if err := r.upsertService(ctx, dn, resources.BuildClientService(served)); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}
	if hasTopo {
		if err := r.upsertConfigMap(ctx, dn, topoJSON); err != nil {
			return fmt.Errorf("apply topology configmap: %w", err)
		}
	}
	held, err := r.upsertStatefulSet(ctx, dn, opts, keepTemplate)
	if err != nil {
		return fmt.Errorf("apply statefulset: %w", err)
	}
	if held {
		for _, t := range []string{condKeysValid, condDegraded} {
			if c := meta.FindStatusCondition(dn.Status.Conditions, t); c != nil {
				c.Message += "; " + holdMessage
			}
		}
	}
	if resources.IsBlockProducer(dn) {
		if resources.PDBEnabled(dn) {
			if err := r.upsertPDB(ctx, dn); err != nil {
				return fmt.Errorf("apply poddisruptionbudget: %w", err)
			}
		} else if err := r.deletePDB(ctx, dn); err != nil {
			return fmt.Errorf("delete poddisruptionbudget: %w", err)
		}
	}
	if err := r.upsertNetworkPolicy(
		ctx, dn, resources.BuildNetworkPolicy(served),
	); err != nil {
		return fmt.Errorf("apply networkpolicy: %w", err)
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

// missingKeys records an absent keys Secret as KeysValid=False and
// Degraded=True, plus a Warning Event. The pod keeps running on its staged keys
// and the live template is held, so without this the node would report the
// last accepted bundle as valid while every template change is withheld.
func (r *DingoNodeReconciler) missingKeys(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	secretRef string,
) {
	msg := fmt.Sprintf("keys secret %q not found", secretRef)
	log.FromContext(ctx).Info(
		"block-producer keys secret missing",
		"secret", secretRef,
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn,
			nil,
			corev1.EventTypeWarning,
			"KeysSecretMissing",
			"ReadKeys",
			"%s",
			msg,
		)
	}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condKeysValid,
		Status:             metav1.ConditionFalse,
		Reason:             "KeysSecretMissing",
		Message:            msg,
		ObservedGeneration: dn.Generation,
	})
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type:               condDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             "KeysSecretMissing",
		Message:            msg,
		ObservedGeneration: dn.Generation,
	})
}

// statefulSetReady reports whether the StatefulSet has the desired number of
// ready pods and has finished rolling out its current spec. ReadyReplicas alone
// stays at the desired count while the controller replaces pods one by one, and
// before it has observed a new generation, so it would report Ready through a
// rollout.
func statefulSetReady(sts *appsv1.StatefulSet, desired int32) bool {
	st := sts.Status
	if st.ObservedGeneration < sts.Generation {
		return false
	}
	if st.ReadyReplicas == 0 || st.ReadyReplicas != desired {
		return false
	}
	return st.UpdateRevision != "" && st.CurrentRevision == st.UpdateRevision
}

// statefulSetRollingOut reports whether the StatefulSet is moving to a new
// spec, judged from generation and revision alone. It must not depend on the
// ready count: replacing the only pod drops ReadyReplicas to zero, which is
// exactly when the rollout should be reported. A StatefulSet that has not
// reported a revision yet (a first create) is not rolling out.
func statefulSetRollingOut(sts *appsv1.StatefulSet) bool {
	st := sts.Status
	if st.ObservedGeneration > 0 && st.ObservedGeneration < sts.Generation {
		return true
	}
	return st.CurrentRevision != "" && st.UpdateRevision != "" &&
		st.CurrentRevision != st.UpdateRevision
}

// reconcileStatus refreshes the DingoNode status from live children.
func (r *DingoNodeReconciler) reconcileStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) error {
	sts := &appsv1.StatefulSet{}
	ready := false
	rollingOut := false
	if err := r.Get(ctx, types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}, sts); err == nil {
		ready = statefulSetReady(sts, desiredReplicas(dn))
		rollingOut = !ready && statefulSetRollingOut(sts)
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	// Scraped for every node, not just block producers: a relay that stops
	// following the chain serves stale answers to everything downstream.
	r.refreshForgeStatus(ctx, dn)
	r.autoRotate(ctx, dn)

	// A wedged node keeps its pod Ready, so Ready alone would report it
	// healthy. Phase is downgraded to Degraded while the pipeline is unhealthy,
	// but Ready still reports StatefulSet truth -- conflating the two would
	// hide which of the two failed.
	wedged := meta.IsStatusConditionFalse(
		dn.Status.Conditions,
		condPipelineHealthy,
	)
	// A refused key bundle leaves the pod Ready on its last accepted keys, but
	// rotation has stopped, so the node is not healthy and must not be
	// reported as Forging.
	keysRefused := meta.IsStatusConditionFalse(
		dn.Status.Conditions,
		condKeysValid,
	)
	switch {
	case ready && (wedged || keysRefused):
		dn.Status.Phase = "Degraded"
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionTrue, Reason: "StatefulSetReady",
			Message: "node is ready", ObservedGeneration: dn.Generation,
		})
	case ready:
		dn.Status.Phase = "Ready"
		if resources.IsBlockProducer(dn) && dn.Status.KES.RemainingPeriods > 0 {
			dn.Status.Phase = "Forging"
		}
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionTrue, Reason: "StatefulSetReady",
			Message: "node is ready", ObservedGeneration: dn.Generation,
		})
	case rollingOut:
		dn.Status.Phase = "Pending"
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionFalse, Reason: "RolloutInProgress",
			Message: "waiting for the StatefulSet rollout to complete", ObservedGeneration: dn.Generation,
		})
	default:
		dn.Status.Phase = "Pending"
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condReady, Status: metav1.ConditionFalse, Reason: "StatefulSetNotReady",
			Message: "waiting for node to become ready", ObservedGeneration: dn.Generation,
		})
	}
	// Degraded is cleared here because everything that sets it earlier in the
	// reconcile returns before reaching this point — except rejectKeys and
	// missingKeys, which deliberately fall through so the rest of the resources
	// still reconcile. A refused or missing key bundle must survive to the
	// status write, so keep Degraded while KeysValid is False. A wedged
	// pipeline is kept for the same reason: it is set during this reconcile,
	// after everything that returns early.
	if !meta.IsStatusConditionFalse(dn.Status.Conditions, condKeysValid) &&
		!wedged {
		meta.RemoveStatusCondition(&dn.Status.Conditions, condDegraded)
	}
	return r.updateStatus(ctx, dn)
}

// refreshForgeStatus scrapes the node's metrics to populate KES and ledger
// pipeline status. Errors are non-fatal, but mark the derived conditions
// Unknown so stale data is never mistaken for a fresh observation.
func (r *DingoNodeReconciler) refreshForgeStatus(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) {
	bp := dn.Spec.BlockProducer
	isBP := bp != nil
	if desiredReplicas(dn) == 0 {
		// There is no running node whose pipeline can be observed. Drop an old
		// result from before scale-down, then report the current state as unknown.
		meta.RemoveStatusCondition(&dn.Status.Conditions, condPipelineHealthy)
		dn.Status.Tip = dingov1alpha1.TipStatus{}
		setPipelineUnknown(dn, "NoReplicas", "node has no desired replicas")
		return
	}
	if r.ForgeStatus == nil {
		dn.Status.Tip = dingov1alpha1.TipStatus{}
		setPipelineUnknown(
			dn,
			"MetricsUnavailable",
			"forge status fetcher is not configured",
		)
		if isBP {
			setRotationDueUnknown(
				dn,
				"MetricsUnavailable",
				"forge status fetcher is not configured",
			)
		}
		return
	}
	logger := log.FromContext(ctx)
	// Scrape through the headless Service, not the client Service. The client
	// Service gates its endpoints on readiness, so KES status would disappear
	// exactly when /readyz fails -- a node behind, stalled or catching up is
	// when its remaining KES periods matter most. The headless Service sets
	// PublishNotReadyAddresses, so an unready node stays scrapable.
	var statuses []*forgestatus.Status
	var scrapeFailed bool
	for replica := range desiredReplicas(dn) {
		podName := fmt.Sprintf("%s-%d", dn.Name, replica)
		url := fmt.Sprintf(
			"http://%s.%s.%s.svc.cluster.local:%d/metrics",
			podName,
			resources.HeadlessServiceName(dn),
			dn.Namespace,
			metricsPort,
		)
		st, err := r.ForgeStatus.Fetch(ctx, url)
		if err != nil {
			logger.V(1).
				Info("forge status unavailable", "pod", podName, "error", err.Error())
			scrapeFailed = true
			continue
		}
		if st == nil {
			logger.V(1).
				Info("forge status unavailable", "pod", podName, "error", "empty metrics")
			scrapeFailed = true
			continue
		}
		statuses = append(statuses, st)
	}
	if scrapeFailed || len(statuses) != int(desiredReplicas(dn)) {
		// The node was not watched, so the persisted last-progress time says
		// nothing about what it does once reachable again.
		dn.Status.Tip = dingov1alpha1.TipStatus{}
		setPipelineUnknown(
			dn,
			"MetricsUnavailable",
			"metrics could not be fetched from every node replica",
		)
		if isBP {
			setRotationDueUnknown(
				dn,
				"MetricsUnavailable",
				"forge status metrics could not be fetched",
			)
		}
		return
	}
	aggregate := &forgestatus.Status{
		HasPipelineStuckMetric:  true,
		HasPipelineHaltedMetric: true,
	}
	aggregate.HasTipSlot = true
	for i, st := range statuses {
		aggregate.HasTipSlot = aggregate.HasTipSlot && st.HasTipSlot
		if i == 0 || st.TipSlot < aggregate.TipSlot {
			aggregate.TipSlot = st.TipSlot
		}
		aggregate.PipelineStuck = aggregate.PipelineStuck || st.PipelineStuck
		aggregate.PipelineHalted = aggregate.PipelineHalted || st.PipelineHalted
		aggregate.HasPipelineStuckMetric =
			aggregate.HasPipelineStuckMetric && st.HasPipelineStuckMetric
		aggregate.HasPipelineHaltedMetric =
			aggregate.HasPipelineHaltedMetric && st.HasPipelineHaltedMetric
	}
	previous := meta.FindStatusCondition(dn.Status.Conditions, condPipelineHealthy)
	wasStalled := previous != nil && previous.Reason == reasonTipNotAdvancing
	stall := r.observeTip(dn, aggregate)
	setPipelineCondition(dn, aggregate, stall)
	if cond := meta.FindStatusCondition(
		dn.Status.Conditions, condPipelineHealthy,
	); !wasStalled && r.Recorder != nil && cond != nil &&
		cond.Reason == reasonTipNotAdvancing {
		r.Recorder.Eventf(
			dn, nil, corev1.EventTypeWarning, reasonTipNotAdvancing,
			"ObserveTip", "%s", cond.Message,
		)
	}
	if !isBP {
		return
	}
	// Block producers have one replica in v1; keep KES status tied to that
	// replica's keys rather than an aggregate pipeline observation.
	st := statuses[0]
	r.observeLoadedOpCert(dn, st)
	if !st.HasKESData {
		setRotationDueUnknown(
			dn,
			"KESMetricsUnavailable",
			"KES metrics are not available",
		)
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

func setRotationDueUnknown(
	dn *dingov1alpha1.DingoNode,
	reason, message string,
) {
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionUnknown, Reason: reason,
		Message: message, ObservedGeneration: dn.Generation,
	})
}

func setPipelineUnknown(dn *dingov1alpha1.DingoNode, reason, message string) {
	// Keep a confirmed unhealthy observation until a successful scrape includes
	// both gauges. A transient scrape failure or an older node without the
	// gauges must not erase a known wedge and clear Degraded. A tip stall is
	// not kept: it is inferred over an observation window, which every caller
	// here breaks (status.tip is cleared and the clock restarts).
	if prev := meta.FindStatusCondition(
		dn.Status.Conditions, condPipelineHealthy,
	); prev != nil && prev.Status == metav1.ConditionFalse &&
		prev.Reason != reasonTipNotAdvancing {
		return
	}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condPipelineHealthy, Status: metav1.ConditionUnknown, Reason: reason,
		Message: message, ObservedGeneration: dn.Generation,
	})
}

// tipStall describes a tip slot frozen beyond the threshold.
type tipStall struct {
	slot      int64
	since     time.Time
	threshold time.Duration
}

func (r *DingoNodeReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// observeTip records the tip slot in status and returns the stall, if any.
//
// Only a change of slot counts as progress, in either direction, so a rollback
// or a restart from an older tip is not mistaken for a freeze. State lives in
// status, so an operator restart keeps the frozen duration. Any pass that does
// not observe the tip clears it, so the clock restarts: a node that was
// unreachable, or whose gauge was missing, must not be judged by progress
// recorded before the gap. Slot 0 is not an observation: Dingo serves metrics
// before it has loaded its tip, and the gauge reads 0 until then.
//
// Status is written on every reconcile and a status change triggers another
// reconcile, so nothing here may change while the slot does not.
func (r *DingoNodeReconciler) observeTip(
	dn *dingov1alpha1.DingoNode,
	st *forgestatus.Status,
) *tipStall {
	tip := &dn.Status.Tip
	if r.TipStallThreshold <= 0 || !st.HasTipSlot || st.TipSlot <= 0 {
		*tip = dingov1alpha1.TipStatus{}
		return nil
	}
	now := r.now()
	if tip.LastProgressAt == nil || tip.Slot != st.TipSlot {
		tip.LastProgressAt = &metav1.Time{Time: now}
	}
	tip.Slot = st.TipSlot
	if now.Sub(tip.LastProgressAt.Time) <= r.TipStallThreshold {
		return nil
	}
	return &tipStall{
		slot:      tip.Slot,
		since:     tip.LastProgressAt.Time,
		threshold: r.TipStallThreshold,
	}
}

// setPipelineCondition reports whether the node is still following the chain.
//
// Both gauges mean the node keeps running while no longer applying blocks. A
// block producer in that state goes on forging at its leader slots and nobody
// adopts the result, so the condition is surfaced rather than folded into
// Ready: the pod is genuinely ready, and the chain is genuinely not advancing.
// Halted is terminal and ranks above stuck.
func setPipelineCondition(
	dn *dingov1alpha1.DingoNode,
	st *forgestatus.Status,
	stall *tipStall,
) {
	cond := metav1.Condition{
		Type: condPipelineHealthy, Status: metav1.ConditionTrue,
		Reason: "Following", Message: "ledger pipeline is following the chain",
		ObservedGeneration: dn.Generation,
	}
	switch {
	case st.PipelineHalted:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "LedgerPipelineHalted"
		cond.Message = "ledger pipeline stopped on an unrepairable validation " +
			"failure; the node is no longer following the chain and requires " +
			"operator intervention"
	case st.PipelineStuck:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "LedgerPipelineStuck"
		cond.Message = "ledger pipeline is restarting without tip progress and " +
			"requires operator intervention"
	case !st.HasPipelineStuckMetric || !st.HasPipelineHaltedMetric:
		setPipelineUnknown(
			dn,
			"PipelineMetricsUnavailable",
			"node metrics do not include ledger pipeline health gauges",
		)
		return
	case stall != nil:
		cond.Status = metav1.ConditionFalse
		cond.Reason = reasonTipNotAdvancing
		cond.Message = fmt.Sprintf(
			"tip slot %d has not advanced since %s (threshold %s)",
			stall.slot, stall.since.UTC().Format(time.RFC3339), stall.threshold,
		)
	}
	if cond.Status == metav1.ConditionFalse {
		meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
			Type: condDegraded, Status: metav1.ConditionTrue,
			Reason: cond.Reason, Message: cond.Message,
			ObservedGeneration: dn.Generation,
		})
	}
	meta.SetStatusCondition(&dn.Status.Conditions, cond)
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
	parsed, err := parsePoolID(bp.PoolID)
	if err != nil {
		r.setOnChainUnavailable(dn, "InvalidPoolID", err.Error())
		return
	}
	wantPool := parsed.String()
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

	// Record the attempt before making it, so a dial that fails — or a reconcile
	// that errors out after it — is rate-limited exactly like a success.
	attempt := onChainAttempt{at: time.Now()}
	counter, err := r.OnChain.Fetch(ctx, onchain.Query{
		Address:      addr,
		NetworkMagic: magic,
		PoolID:       parsed,
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
		// Objects a DingoNode references: its configRef ConfigMap, and the
		// relayRefs peers with the StatefulSets they control. Peer watches
		// see only spec changes, creation and deletion: status is written
		// every reconcile, and two nodes listing each other in relayRefs
		// would otherwise reconcile each other on every status write.
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.dingoNodesForConfigMap),
		).
		Watches(
			&dingov1alpha1.DingoNode{},
			handler.EnqueueRequestsFromMapFunc(r.dingoNodesForRelayRef),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Watches(
			&appsv1.StatefulSet{},
			handler.EnqueueRequestsFromMapFunc(r.dingoNodesForPeerStatefulSet),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("dingonode").
		Complete(r)
}
