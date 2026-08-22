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

//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// pdbStatusTimeout bounds the disruption controller filling in the PDB's
// status. minAvailable and the selector are set at creation, but
// status.currentHealthy / status.disruptionsAllowed are computed by
// kube-controller-manager once the pod is running — and those are the fields
// that say the budget is actually accounted for rather than merely declared.
const pdbStatusTimeout = 2 * time.Minute

// evictPod submits a real policy/v1 Eviction against the node's pod — the same
// subresource `kubectl drain` uses — and returns the API server's verdict. nil
// means the eviction was *accepted*, which for a single-replica block producer
// under a minAvailable=1 budget is a failure of the guarantee, not of the test.
func (h *harness) evictPod(ctx context.Context) error {
	h.t.Helper()
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.podKey().Name,
			Namespace: h.namespace,
		},
	}
	return h.clientset.CoreV1().Pods(h.namespace).EvictV1(ctx, eviction)
}

// hasDisruptionBudgetCause reports whether a failed eviction names a
// PodDisruptionBudget as the reason. The API server attaches this cause to the
// 429 it returns when a budget blocks an eviction, so it is what separates "the
// PDB refused this" from any other reason a request can be rejected.
func hasDisruptionBudgetCause(err error) bool {
	// errors.As rather than a type assertion, so a wrapped StatusError is not
	// silently reported as "no budget cause".
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	if details == nil {
		return false
	}
	for _, cause := range details.Causes {
		if cause.Type == policyv1.DisruptionBudgetCause {
			return true
		}
	}
	return false
}

// TestPodDisruptionBudgetProtectsForgingNode covers disruption of a live
// forging node, which nothing else in the suite touches. Two halves, in the
// order a cluster meets them.
//
// B1, the valuable half: a voluntary eviction of the only block producer must
// be refused. BuildPodDisruptionBudget sets minAvailable=1 against a
// single-replica StatefulSet, so evicting the one pod would take availability
// below the minimum and the API server must return 429. That refusal is what
// ha.strategy: SingleActive actually promises — "exactly one forger, and a
// drain cannot take it to zero" — and until now the promise was only asserted
// as rendered YAML in internal/resources' unit tests. Eviction admission is an
// apiserver + disruption-controller behaviour, so it needs a real cluster.
//
// B2: an involuntary loss must recover. A plain pod delete is the eviction
// bypass — no PDB can refuse it, and it is what a node failure, a forced delete
// or a preemption amount to — so the StatefulSet has to put the pod back, on
// the same volume, and the node has to resume forging.
func TestPodDisruptionBudgetProtectsForgingNode(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "blocks eviction and recovers from pod loss"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			h := newHarness(t)
			t.Cleanup(func() {
				if t.Failed() {
					t.Log(h.diagnostics(ctx))
				}
			})

			dn := h.applyDevNet(ctx)
			h.applyDingoNode(ctx, dn)

			oldPod := h.waitPodReady(ctx)

			// Wait for a forged block before disrupting anything. Same discipline
			// rotation_test.go documents, and for the same reason: replacing the pod of
			// a node that is still writing its genesis into the data directory tests
			// pod replacement racing genesis, not disruption recovery. That race was
			// also once fatal — dingo #2959 left the PVC permanently unrecoverable,
			// fixed in 0.68.0, which is at or below every version this suite pins — but
			// the wait stands on its own merit: an SPO drains or loses a producer that
			// is already on chain, and that is the state this test should be in.
			h.waitForged(ctx, 1)

			before := h.scrapeMetrics(ctx)
			require.Positive(t, before.ForgedBlocks,
				"the node must be forging before it is disrupted, or \"forging "+
					"resumed\" below means nothing")
			t.Logf("pod %s (UID %s) forging, %d block(s)",
				oldPod.Name, oldPod.UID, before.ForgedBlocks)

			// --- B1: the voluntary eviction must be refused --------------------------

			// Non-vacuity: the budget has to be in effect before the eviction is
			// attempted. disruptionsAllowed=0 with one healthy pod and desiredHealthy=1
			// is the disruption controller's own statement that it will block the next
			// eviction; without it a 429 could be coming from anywhere.
			pdb := &policyv1.PodDisruptionBudget{}
			h.waitFor(ctx, pdbStatusTimeout,
				"the PodDisruptionBudget to account for the running pod",
				func(ctx context.Context) (bool, error) {
					got := &policyv1.PodDisruptionBudget{}
					if err := h.client.Get(ctx, h.nodeKey(), got); err != nil {
						return false, err
					}
					if got.Status.ObservedGeneration != got.Generation {
						return false, nil
					}
					pdb = got
					return got.Status.DesiredHealthy == 1 &&
						got.Status.CurrentHealthy == 1 &&
						got.Status.DisruptionsAllowed == 0, nil
				})
			minAvailable := pdb.Spec.MinAvailable
			require.NotNil(t, minAvailable,
				"the block producer's PDB must set minAvailable")
			assert.Equal(t, "1", minAvailable.String(),
				"SingleActive promises exactly one forger stays up")
			assert.Equal(t, int32(1), pdb.Status.DesiredHealthy,
				"PDB status: %+v", pdb.Status)
			assert.Equal(t, int32(1), pdb.Status.CurrentHealthy,
				"PDB status: %+v", pdb.Status)
			require.Equal(t, int32(0), pdb.Status.DisruptionsAllowed,
				"the PDB must allow no voluntary disruption before the eviction "+
					"below, or its refusal proves nothing; status: %+v", pdb.Status)

			err := h.evictPod(ctx)

			// If the eviction succeeded, SingleActive's guarantee is false. Report that
			// rather than softening the assertion.
			require.Error(t, err,
				"the API server accepted an eviction of the only block producer: a "+
					"drain would take the pool to zero forgers, which is exactly what "+
					"the minAvailable=1 PodDisruptionBudget exists to prevent")

			// "An error" is not the assertion. A 404 (wrong pod name), a 403 (missing
			// RBAC on the eviction subresource) or a 400 would all be errors while
			// saying nothing at all about the budget, so rule them out explicitly
			// before accepting the denial.
			require.False(t, apierrors.IsNotFound(err),
				"eviction targeted a pod that does not exist, so the denial below "+
					"would be meaningless: %v", err)
			require.False(t, apierrors.IsForbidden(err),
				"eviction was refused by RBAC, not by the PDB: %v", err)
			require.False(t, apierrors.IsUnauthorized(err),
				"eviction was refused by authentication, not by the PDB: %v", err)
			require.False(t, apierrors.IsBadRequest(err),
				"the eviction request itself was malformed: %v", err)

			assert.True(t, apierrors.IsTooManyRequests(err),
				"a PDB-blocked eviction must come back as 429 TooManyRequests, got: "+
					"%v", err)
			assert.True(t, hasDisruptionBudgetCause(err),
				"the refusal must name a PodDisruptionBudget as its cause, or it is "+
					"not the budget doing the refusing: %v", err)
			t.Logf("eviction denied: %v", err)

			// And the consequence: the pod was not touched.
			stillThere, perr := h.podOrNil(ctx)
			require.NoError(t, perr, "read the pod after the refused eviction")
			require.NotNil(t, stillThere, "the pod is gone after a refused eviction")
			assert.Equal(t, oldPod.UID, stillThere.UID,
				"the pod was replaced despite the eviction being refused")
			assert.True(t, stillThere.DeletionTimestamp.IsZero(),
				"the refused eviction still marked the pod for deletion: %v",
				stillThere.DeletionTimestamp)

			// --- B2: an involuntary loss must recover -------------------------------

			sts := &appsv1.StatefulSet{}
			require.NoError(t, h.client.Get(ctx, h.nodeKey(), sts),
				"read the StatefulSet for its claim template")
			pvcKey := types.NamespacedName{
				Name:      dataPVCName(t, sts, h.podKey().Name),
				Namespace: h.namespace,
			}
			pvc := &corev1.PersistentVolumeClaim{}
			require.NoError(t, h.client.Get(ctx, pvcKey, pvc),
				"read the data PVC %s", pvcKey.Name)
			require.NotEmpty(t, pvc.UID, "the data PVC has no UID")
			require.Equal(t, corev1.ClaimBound, pvc.Status.Phase,
				"the data PVC must be bound before the pod is destroyed, or "+
					"\"reused the same volume\" is not a meaningful claim")
			t.Logf("data PVC %s UID %s bound", pvcKey.Name, pvc.UID)

			// The bypass: a plain DELETE on the pod resource, which is what a node
			// failure, a preemption or `kubectl delete pod` amount to. No PDB gates
			// this path — that is the point of testing it separately from B1.
			//
			// Deliberately not --grace-period=0: the pod keeps its
			// terminationGracePeriodSeconds so Dingo gets its SIGTERM and flushes the
			// database it derives CARDANO_SHUTDOWN_TIMEOUT from. Killing it outright
			// would be testing crash recovery, which is a different property.
			require.NoError(t, h.client.Delete(ctx, stillThere),
				"delete pod %s", h.podKey().Name)

			newPod := h.waitPodReplaced(ctx, oldPod.UID)
			t.Logf("pod replaced after involuntary loss: %s -> %s",
				oldPod.UID, newPod.UID)

			// Same volume, by UID. A name match alone would not catch a re-provision:
			// the StatefulSet controller would give a freshly created claim the same
			// deterministic name, and a block producer that silently starts on an empty
			// volume resyncs the whole chain.
			after := &corev1.PersistentVolumeClaim{}
			require.NoError(t, h.client.Get(ctx, pvcKey, after),
				"read the data PVC after the pod was replaced")
			assert.Equal(t, pvc.UID, after.UID,
				"the replacement pod is running on a re-provisioned volume, not the "+
					"chain data the old one wrote")
			assert.True(t, after.DeletionTimestamp.IsZero(),
				"the data PVC is marked for deletion after the pod was replaced")

			// ...and the replacement pod actually references that claim, rather than
			// the claim merely still existing next to a pod using something else.
			var claims []string
			for _, vol := range newPod.Spec.Volumes {
				if src := vol.PersistentVolumeClaim; src != nil {
					claims = append(claims, src.ClaimName)
				}
			}
			assert.Contains(t, claims, pvcKey.Name,
				"the replacement pod does not mount the retained data claim; it "+
					"mounts %v", claims)

			// Forging resumed. Forge_forged_int is a per-process counter reset on every
			// start, so climbing from zero a second time is itself the evidence that a
			// new node process is producing blocks — not a stale reading of the pod
			// that was destroyed.
			h.waitForged(ctx, 1)
			resumed := h.scrapeMetrics(ctx)
			assert.Positive(t, resumed.ForgedBlocks,
				"the replacement node must forge; metrics: %+v", resumed)
			t.Logf("forging resumed on pod %s: %d block(s) (was %d before the loss)",
				newPod.Name, resumed.ForgedBlocks, before.ForgedBlocks)
		})
	}
}
