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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Log substrings from Dingo's ledger-aware credential cross-check
// (dingo/node_forging.go validateBlockProducerLedgerWithView).
const (
	// vrfMismatchLog is forging.ErrVRFKeyHashMismatch's text: the VRF key the
	// node loaded does not hash to the value the pool's genesis registration
	// recorded. A key-binding bug earlier in this plan put blake2b-256(seed)
	// where the VRF verification-key hash belonged; every length assertion
	// still passed and the node still forged, because Dingo downgrades this to
	// a WARN when the network is literally named "devnet". Nothing in the
	// metrics surface can reveal it — the log is the only signal.
	vrfMismatchLog = "VRF key hash mismatch"

	// vrfVerifiedLog is the one branch of that cross-check that means the keys
	// really do bind to the registration. Asserting it positively is what keeps
	// the check above from passing vacuously: the two remaining branches log
	// "pool not yet registered on chain" and "VRF cross-check skipped
	// (seed-only VRF key)", and neither contains vrfMismatchLog either.
	vrfVerifiedLog = "block producer pool registration verified on chain"
)

// TestBlockProducerForges is the payoff assertion for the whole suite: an
// operator-managed block producer, given nothing but a DingoNode and a
// generated devnet, forges a block on its own chain.
func TestBlockProducerForges(t *testing.T) {
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

	pod := h.waitPodReady(ctx)
	t.Logf("pod %s ready", pod.Name)

	// Ground truth: the node itself reports a forged block.
	h.waitForged(ctx, 1)

	// The credentials must be genuinely correct, not merely accepted: a forged
	// block on its own does not prove the key material binds to the genesis
	// pool registration. Only the node's log carries that verdict.
	logs, err := h.podLogs(ctx)
	require.NoError(t, err, "read node logs for the VRF cross-check")
	require.NotEmpty(t, strings.TrimSpace(logs),
		"node log is empty, so the VRF checks below would be vacuous")
	assert.NotContains(t, logs, vrfMismatchLog,
		"node logged a VRF key hash mismatch: the devnet's VRF key does not "+
			"match its genesis pool registration")
	assert.Contains(t, logs, vrfVerifiedLog,
		"node never confirmed its pool registration and VRF key on chain; "+
			"the cross-check was skipped or the pool was not registered, so "+
			"forging proves nothing about the key binding")

	// The operator's own observation path must agree. remainingKESPeriods sits
	// at 0 until the slot clock starts — before systemStart Dingo publishes
	// currentKESPeriod=0, expiry=64 and remaining=0 — so this is really waiting
	// for the chain to start *and* for the operator to scrape it afterwards.
	h.waitFor(ctx, kesStatusTimeout,
		"the operator to populate status.kes from the node's metrics",
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			return node.Status.KES.RemainingPeriods > 0, nil
		})

	// currentPeriod only leaves 0 once the chain passes slot slotsPerKESPeriod,
	// and the operator refreshes status on a 2-minute requeue, so this needs a
	// longer budget than the check above.
	h.waitFor(ctx, kesPeriodTimeout,
		"status.kes.currentPeriod to advance past 0",
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			return node.Status.KES.CurrentPeriod > 0, nil
		})

	node := h.getNode(ctx)
	// KeysValid is what makes the counter assertion below mean anything: the
	// operator only writes status.opcert.onDiskCounter for a bundle that passed
	// validation, and the devnet issues its opcert at counter 0 — the same as
	// the field's zero value. Assert the condition first so a validation
	// failure (which would leave onDiskCounter at 0 *and* refuse to roll the
	// pod) cannot masquerade as success here.
	require.True(t,
		conditionIs(node, condKeysValid, metav1.ConditionTrue,
			reasonOpCertAccepted),
		"the operator must accept the devnet's own freshly generated keys; "+
			"conditions: %+v", node.Status.Conditions)
	assert.Equal(t, int64(0), node.Status.OpCert.OnDiskCounter,
		"the devnet's opcert was issued at counter 0")
	// Load-bearing, unlike the above: refreshForgeStatus writes OpCertKESPeriod
	// from the node's operationalCertificateStartKESPeriod metric.
	assert.Equal(t, int64(0), node.Status.KES.OpCertKESPeriod,
		"the devnet's opcert starts at KES period 0")
}
