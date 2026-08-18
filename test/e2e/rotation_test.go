//go:build e2e

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

package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// noRollWindow is how long the pod must demonstrably stay put after a
	// refused bundle. It is deliberately shorter than the operator's 2m
	// requeue: the reconcile that refused the bundle has already re-rendered
	// the pod template, so a rollout — if the checksum carry-forward were
	// broken — would already be in flight. The window only has to outlast the
	// StatefulSet controller reacting to a changed template, which is seconds.
	noRollWindow = 90 * time.Second
	noRollPoll   = 10 * time.Second
)

// Every test in this file is one leg of the Assisted-mode rotation contract:
// a valid replacement opcert rolls the pod and forging resumes on it, and an
// invalid one is refused without touching the running node. They bring up a
// devnet each rather than sharing one, so a failure in either is attributable
// on its own — see the task-9 report for the wall-clock cost.

// kesPeriodPastZero waits until the node has advanced past KES period 0 and
// returns the period it is in. Replacement certificates in the roll test are
// dated here rather than at 0 because 0 is where the devnet's *original*
// certificate starts: an assertion that the node loaded the replacement is
// vacuous if the two share a start period.
func (h *harness) kesPeriodPastZero(ctx context.Context) int64 {
	h.t.Helper()
	h.waitFor(ctx, kesPeriodTimeout,
		"the node's KES period to advance past 0",
		func(ctx context.Context) (bool, error) {
			st, err := h.tryScrapeMetrics(ctx)
			if err != nil {
				return false, err
			}
			return st.HasKESData && st.CurrentKESPeriod > 0, nil
		})
	return h.currentKESPeriod(ctx)
}

// deliverOpCert mints a fresh KES keypair, issues a cold-signed opcert for it
// at the given counter and start period, and writes the bundle into the mounted
// keys Secret. That is exactly what an external rotation tool does in Assisted
// mode: the operator never sees the cold key, only the delivered result.
func (h *harness) deliverOpCert(
	ctx context.Context,
	dn *devnet.DevNet,
	counter uint64,
	startPeriod int64,
) {
	h.t.Helper()
	// The only narrowing of a scraped KES period in the suite. A negative
	// reading is not a real period and would wrap to an astronomical start
	// period, minting a certificate no node would ever accept.
	if startPeriod < 0 {
		h.t.Fatalf("refusing to issue an opcert at KES period %d", startPeriod)
		return
	}
	require.NoError(h.t, dn.Keys.RotateKES(rand.Reader), "rotate the KES key")
	data, err := dn.Keys.SecretData(counter, uint64(startPeriod))
	require.NoError(h.t, err, "issue opcert at counter %d", counter)
	h.updateKeysSecret(ctx, data)
	h.t.Logf("delivered opcert counter %d starting at KES period %d",
		counter, startPeriod)
}

// waitKeysAccepted blocks until the operator has validated the delivered bundle
// and published its counter. Both halves matter: the condition says validation
// ran and passed, and onDiskCounter says which certificate it passed on.
func (h *harness) waitKeysAccepted(ctx context.Context, counter int64) {
	h.t.Helper()
	h.waitFor(ctx, keysDeliveryTimeout,
		fmt.Sprintf("the operator to accept the opcert at counter %d", counter),
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			return conditionIs(node, condKeysValid, metav1.ConditionTrue,
				reasonOpCertAccepted) &&
				node.Status.OpCert.OnDiskCounter == counter, nil
		})
}

// waitPodReplaced blocks until the node's pod has been recreated under a UID
// other than old and reports Ready, returning the replacement. Dingo has no
// credential hot-reload, so a new pod is the only way new key material can
// reach a running node. That makes the pod UID the ground truth for "the
// operator rolled it".
func (h *harness) waitPodReplaced(
	ctx context.Context,
	old types.UID,
) *corev1.Pod {
	h.t.Helper()
	h.waitFor(ctx, podRollTimeout,
		fmt.Sprintf("pod %s to be replaced (was UID %s)",
			h.podKey().Name, old),
		func(ctx context.Context) (bool, error) {
			pod, err := h.podOrNil(ctx)
			if err != nil || pod == nil {
				return false, err
			}
			return pod.UID != old, nil
		})
	pod := h.waitPodReady(ctx)
	require.NotEqual(h.t, old, pod.UID,
		"the pod that became Ready is the one that was supposed to be rolled")
	return pod
}

// TestAssistedRotationRollsPodAndResumesForging is the happy path of the
// rotation contract: an externally-delivered, correctly-signed opcert at a
// higher counter is accepted, the pod is rolled onto it, and the pool goes back
// to producing blocks under the new credentials.
func TestAssistedRotationRollsPodAndResumesForging(t *testing.T) {
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

	oldUID := h.waitPodReady(ctx).UID
	h.waitForged(ctx, 1)

	before, err := h.stsKeysChecksum(ctx)
	require.NoError(t, err, "read the live keys-checksum annotation")
	require.NotEmpty(t, before,
		"the operator must already have checksummed the devnet's own keys; "+
			"with no starting value the change assertion below proves nothing")

	period := h.kesPeriodPastZero(ctx)
	h.deliverOpCert(ctx, dn, 1, period)

	h.waitKeysAccepted(ctx, 1)

	after, err := h.stsKeysChecksum(ctx)
	require.NoError(t, err, "read the keys-checksum annotation after delivery")
	assert.NotEqual(t, before, after,
		"accepting new key material must change the pod template's "+
			"keys-checksum annotation — that change is what makes the "+
			"StatefulSet roll the pod onto the new keys")

	newPod := h.waitPodReplaced(ctx, oldUID)
	t.Logf("pod rolled: %s -> %s", oldUID, newPod.UID)

	// Forge_forged_int is per-process, so climbing from zero a second time is
	// itself proof that this is a new node process forging, not a stale
	// reading of the pod that was rolled.
	h.waitForged(ctx, 1)

	// What the node itself says it loaded. Without this the test would only
	// show that *a* pod restarted and forged; this is the part that says it
	// forged on the delivered certificate rather than the original one.
	st := h.scrapeMetrics(ctx)
	assert.Equal(t, period, st.OpCertStartKESPeriod,
		"the node must report the delivered certificate's start KES period "+
			"(the original devnet certificate starts at 0)")
}

// TestAssistedRotationRejectsCounterRegression is the other leg: a
// correctly-signed, correctly-bound certificate whose counter goes backwards
// is refused and — the part that actually protects the pool — the running node
// is left alone on its last known-good keys.
//
// Kept last in the file: it ends with a rejected bundle sitting in the Secret.
// Ordering is not load-bearing (each test gets its own namespace and devnet),
// but a reader should meet the happy path first.
func TestAssistedRotationRejectsCounterRegression(t *testing.T) {
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

	firstUID := h.waitPodReady(ctx).UID

	// Wait for the node to forge before rotating anything. Keep this — it is
	// not about the assertions below, it is what makes the test faithful to the
	// operation being tested: rotating a block producer whose chain is already
	// up, which is the only state a real rotation happens in. Rotating a node
	// that is still initializing tests pod replacement racing genesis, not
	// rotation.
	//
	// Historically this wait was also load-bearing for a different reason: an
	// earlier revision rotated as soon as the pod was Ready, and Dingo came
	// back to "failed to create genesis block: set genesis staking: create
	// genesis pool registration: constraint failed: FOREIGN KEY constraint
	// failed", CrashLooping forever on a half-written database. That was dingo
	// #2959 (https://github.com/blinklabs-io/dingo/issues/2959), fixed in
	// 0.68.0, which is at or below every version this suite pins, so the wait
	// no longer works around an upstream bug. It stays for the reason above.
	h.waitForged(ctx, 1)

	// A regression is only expressible once the operator has observed a
	// counter above zero: status.opcert.onDiskCounter starts at 0, and the
	// anti-regression check has nothing below that to reject. So roll forward
	// to counter 2 first. Unlike the roll test there is no need to wait for the
	// KES period to leave 0 — the discriminator here is the counter.
	h.deliverOpCert(ctx, dn, 2, h.currentKESPeriod(ctx))
	h.waitKeysAccepted(ctx, 2)
	goodPod := h.waitPodReplaced(ctx, firstUID)
	h.waitForged(ctx, 1)

	good := h.scrapeMetrics(ctx)
	require.Positive(t, good.ForgedBlocks,
		"the counter-2 pod must be forging before the rejection, or "+
			"\"forging continues\" below means nothing")

	checksum, err := h.stsKeysChecksum(ctx)
	require.NoError(t, err, "read the keys-checksum annotation")
	require.NotEmpty(t, checksum, "the accepted bundle must be checksummed")

	// Now the bad bundle: freshly minted, cold-signed by the same pool, dated
	// to a period the node is in — its only fault is the counter going
	// backwards.
	h.deliverOpCert(ctx, dn, 1, h.currentKESPeriod(ctx))

	h.waitFor(ctx, keysDeliveryTimeout,
		"the operator to refuse the counter-regressed opcert",
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			return conditionIs(node, condKeysValid, metav1.ConditionFalse,
				reasonOpCertRejected), nil
		})

	node := h.getNode(ctx)
	assert.Equal(t, int64(2), node.Status.OpCert.OnDiskCounter,
		"a refused bundle must not overwrite the last accepted counter")

	// Degraded is the only thing that shows in `kubectl get dingonode`: the
	// node keeps forging on the keys its process already loaded, so Phase and
	// readiness both stay green while rotation has stopped.
	assert.True(t,
		conditionIs(node, condDegraded, metav1.ConditionTrue,
			reasonOpCertRejected),
		"a refused bundle must mark the node Degraded")

	// The Warning Event, through the real recorder and the real
	// events.k8s.io RBAC grant.
	note := h.waitEvent(ctx, corev1.EventTypeWarning, reasonOpCertRejected)
	assert.Contains(t, note, "counter",
		"the Event must say why the bundle was refused")

	// The mechanism. The reconciler carries the live keys-checksum forward on
	// rejection so the rendered pod template stays byte-identical; rendering an
	// empty checksum would *remove* the annotation, which is itself a template
	// change and would roll the pod onto the rejected keys.
	afterReject, err := h.stsKeysChecksum(ctx)
	require.NoError(t, err, "read the keys-checksum annotation after refusal")
	assert.Equal(t, checksum, afterReject,
		"a refused bundle must leave the pod template's keys-checksum "+
			"annotation exactly as it was")

	// And the consequence.
	assert.Never(t, func() bool {
		pod, err := h.podOrNil(ctx)
		return err == nil && pod != nil && pod.UID != goodPod.UID
	}, noRollWindow, noRollPoll,
		"the operator rolled the pod onto a rejected opcert")

	// Still the same node process, and still producing blocks on the keys it
	// had before the refused delivery.
	h.waitForged(ctx, good.ForgedBlocks+1)
	pod, err := h.podOrNil(ctx)
	require.NoError(t, err, "read the node pod after the refusal")
	require.NotNil(t, pod, "the node pod must still exist after a refusal")
	assert.Equal(t, goodPod.UID, pod.UID,
		"the pod forging at the end must be the one that was forging before "+
			"the refused delivery")
}
