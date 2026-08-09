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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// privateBindEnv is the environment variable the operator sets on the node
// container to move Dingo's node-to-client listener off loopback. Without it no
// NetworkPolicy change can help, so it is asserted directly rather than
// inferred from the query succeeding.
const privateBindEnv = "CARDANO_PRIVATE_BIND_ADDR"

// Local aliases for the operator's own exported constants, so the label the
// suite sets can never drift from the one the NetworkPolicy builder matches on
// — and so the assertions below fit in 80 columns.
const (
	ntcLabel   = resources.NodeToClientAccessLabel
	ntcAllowed = resources.NodeToClientAccessAllowed
)

// requireOperatorNtCAccess checks the half of the node-to-client grant that
// lives outside the DingoNode: the operator's own pod and namespace must both
// carry resources.NodeToClientAccessLabel.
//
// It runs before the devnet is generated, and it exists to turn the most likely
// wiring mistake into a five-second failure with a name attached. Miss either
// label and k3s (which enforces NetworkPolicy) simply drops the operator's
// dial: the test would otherwise spend its whole onChainCounterTimeout waiting
// and then report a bare QueryFailed, which reads like a protocol break rather
// than a missing label.
func (h *harness) requireOperatorNtCAccess(ctx context.Context) {
	h.t.Helper()

	ns := &corev1.Namespace{}
	require.NoError(h.t,
		h.client.Get(ctx, types.NamespacedName{Name: operatorNamespace}, ns),
		"get the operator's namespace %s", operatorNamespace)
	require.Equal(h.t, ntcAllowed, ns.Labels[ntcLabel],
		"namespace %s must carry %s=%s: the block producer's NetworkPolicy "+
			"admits a cross-namespace client only when its namespace and its "+
			"pod both carry it (hack/e2e/k3d-up.sh sets this one)",
		operatorNamespace, ntcLabel, ntcAllowed)

	// Listed by the label itself rather than by the operator's app label: this
	// is the selector the NetworkPolicy peer uses, so an empty result is
	// exactly what the policy would see.
	pods := &corev1.PodList{}
	require.NoError(h.t, h.client.List(ctx, pods,
		client.InNamespace(operatorNamespace),
		client.MatchingLabels{ntcLabel: ntcAllowed}),
		"list operator pods carrying %s", ntcLabel)
	require.NotEmpty(h.t, pods.Items,
		"no pod in %s carries %s=%s, so the block producer's NetworkPolicy "+
			"matches nothing and the operator's node-to-client dial is "+
			"dropped (test/e2e/manifests/manager.yaml sets this one, on the "+
			"pod template only — the Deployment selector is immutable)",
		operatorNamespace, ntcLabel, ntcAllowed)
}

// requireNodeToClientExposed checks the half of the grant that the DingoNode
// spec drives: the node container binds node-to-client to the pod network, and
// the rendered NetworkPolicy opens that port. Both are rendered from
// spec.blockProducer.nodeToClient.enabled, so a CRD or builder change that
// dropped the field fails here in seconds instead of surfacing eight minutes
// later as an unexplained "no counter".
func (h *harness) requireNodeToClientExposed(ctx context.Context) {
	h.t.Helper()

	sts := &appsv1.StatefulSet{}
	require.NoError(h.t, h.client.Get(ctx, h.nodeKey(), sts),
		"get StatefulSet %s", nodeName)
	var bind string
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != nodeContainerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == privateBindEnv {
				bind = e.Value
			}
		}
	}
	require.Equal(h.t, "0.0.0.0", bind,
		"the node container must set %s=0.0.0.0; Dingo otherwise binds "+
			"node-to-client to 127.0.0.1 and no NetworkPolicy can help",
		privateBindEnv)

	np := &networkingv1.NetworkPolicy{}
	require.NoError(h.t, h.client.Get(ctx, h.nodeKey(), np),
		"get NetworkPolicy %s", nodeName)
	labelled := false
	for _, rule := range np.Spec.Ingress {
		opensNtC := false
		for _, p := range rule.Ports {
			if p.Port != nil &&
				p.Port.IntValue() == resources.PortNodeToClient {
				opensNtC = true
			}
		}
		if !opensNtC {
			continue
		}
		for _, peer := range rule.From {
			if peer.PodSelector == nil {
				continue
			}
			if peer.PodSelector.MatchLabels[ntcLabel] == ntcAllowed {
				labelled = true
			}
		}
	}
	require.True(h.t, labelled,
		"NetworkPolicy %s must admit port %d from pods labelled %s=%s; "+
			"ingress rules: %+v",
		nodeName, resources.PortNodeToClient, ntcLabel, ntcAllowed,
		np.Spec.Ingress)
}

// waitOnChainCounterObserved blocks until the operator reports it actually read
// the counter — OnChainCounterAvailable=True with reason Observed — and returns
// the DingoNode it saw that on.
//
// The condition it waits for is deliberately narrow. QueryFailed, NodeNotReady,
// PoolNotOnChain and Disabled are all "the operator never got an answer", and
// every one of them is what a broken node-to-client handshake, protocol or
// decode would produce; waiting for the condition merely to exist, or to be
// False-with-any-reason, would pass on all of them. Each poll that sees another
// reason reports it as the wait's last error, so a timeout names the state the
// operator was actually in rather than saying only that eight minutes passed.
func (h *harness) waitOnChainCounterObserved(
	ctx context.Context,
) *dingoNodeSnapshot {
	h.t.Helper()
	var snap dingoNodeSnapshot
	h.waitFor(ctx, onChainCounterTimeout,
		fmt.Sprintf("the operator to read the on-chain opcert counter "+
			"(%s=True/%s)", condOnChainCounter, reasonOnChainObserved),
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			cond := meta.FindStatusCondition(
				node.Status.Conditions, condOnChainCounter)
			if cond == nil {
				return false, fmt.Errorf(
					"%s condition not set yet", condOnChainCounter)
			}
			if cond.Status == metav1.ConditionTrue &&
				cond.Reason == reasonOnChainObserved {
				snap = dingoNodeSnapshot{
					reason:     cond.Reason,
					message:    cond.Message,
					counter:    node.Status.OpCert.OnChainCounter,
					poolID:     node.Status.OpCert.OnChainCounterPoolID,
					observedAt: node.Status.OpCert.OnChainCounterAt != nil,
				}
				return true, nil
			}
			return false, fmt.Errorf(
				"%s is %s with reason %q: %s",
				condOnChainCounter, cond.Status, cond.Reason, cond.Message)
		})
	return &snap
}

// dingoNodeSnapshot is the part of the DingoNode status this test asserts on,
// captured inside the poll so the assertions below cannot race a later
// reconcile that overwrites it.
type dingoNodeSnapshot struct {
	reason     string
	message    string
	counter    int64
	poolID     string
	observedAt bool
}

// TestOnChainOpCertCounterObserved is the only test in the suite that exercises
// node-to-client at all. The operator's on-chain opcert counter floor
// (internal/onchain, gouroboros GetOpCertCounters over local-state-query) is
// off by default and every other test leaves it off, so without this test the
// suite cannot tell a working NtC path from one broken by a handshake,
// protocol or CBOR-decode incompatibility — a live risk, since Dingo and the
// operator pin different gouroboros versions and the drift widens with every
// bump.
//
// It enables both gates (the spec field here, the two access labels in
// test/e2e/manifests/manager.yaml and hack/e2e/k3d-up.sh), waits for the pool
// to mint — the chain has no counter for a pool that has never forged — and
// then asserts the operator really got an answer back.
//
// # Known failure at the time of writing
//
// This test does not pass yet, and that is a true positive rather than a
// wiring fault: the first run against a live node reported
//
//	OnChainCounterAvailable=False reason=QueryFailed
//	  msg=query on-chain opcert counters: cbor: cannot unmarshal array into
//	  Go value of type struct { cbor.StructAsArray; Version uint64;
//	  Inner cbor.RawMessage }
//
// which is a *decode* failure — reached only after the dial, the NtC
// handshake, GetCurrentEra, the acquire and the query round trip had all
// succeeded, so the NetworkPolicy grant and both opt-ins demonstrably work.
// Cardano wraps every era-dependent (QueryIfCurrent) result in a 1-element
// array, and gouroboros unwraps it everywhere else in that client
// (GetEpochNo decodes []int and takes [0]; GetGenesisConfig does the same;
// StakeDistributionResult models the wrapper as a one-field StructAsArray).
// Client.DebugChainDepState — which GetOpCertCounters is built on — decodes
// the wrapper straight into the versioned 2-element record instead, so it
// cannot decode a conformant node's reply. Dingo's encoder is correct
// (ledger/queries_chaindepstate.go returns []any{versionedChainDepState{…}},
// matching every other handler). Verified against gouroboros v0.191.2 and
// v0.192.2, whose chain_dep_state.go files are byte-identical.
//
// So the operator's on-chain counter floor has never worked against any real
// node, which is exactly the blind spot this test exists to close. Leave the
// assertion as it is; fix gouroboros.
func TestOnChainOpCertCounterObserved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	h := newHarness(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(h.diagnostics(ctx))
		}
	})

	h.requireOperatorNtCAccess(ctx)

	dn := h.applyDevNet(ctx)
	h.applyDingoNode(ctx, dn, withNodeToClient())

	pod := h.waitPodReady(ctx)
	t.Logf("pod %s ready", pod.Name)
	h.requireNodeToClientExposed(ctx)

	// The counter map is populated from blocks: a pool that has never minted
	// under an operational certificate is legitimately absent from it, which
	// the operator reports as PoolNotOnChain. So forging first is not
	// incidental — it is what makes an Observed result possible at all.
	h.waitForged(ctx, 1)

	snap := h.waitOnChainCounterObserved(ctx)
	t.Logf("on-chain counter observed: counter=%d pool=%s (%s)",
		snap.counter, snap.poolID, snap.message)

	// The spec field took effect. Redundant given the reason above — Disabled
	// and Observed are mutually exclusive — but it is the assertion that would
	// have caught the case this whole test exists to rule out being untested:
	// a node whose nodeToClient stayed off, where the operator never dials and
	// nothing about the protocol is exercised.
	assert.NotEqual(t, reasonOnChainDisabled, snap.reason,
		"spec.blockProducer.nodeToClient.enabled did not reach the node, so "+
			"no node-to-client query was ever attempted")

	// The decode proof. Observed on its own says the operator got *something*
	// back; this says the map came back keyed by the devnet pool's cold-key
	// hash and that the operator matched it, which is what a broken CBOR
	// decode or a wrongly-keyed lookup would fail.
	assert.Equal(t, hex.EncodeToString(dn.Keys.PoolID), snap.poolID,
		"the observed counter must be recorded against the devnet's own pool")
	assert.True(t, snap.observedAt,
		"status.opcert.onChainCounterAt must be stamped: without it the "+
			"counter is never fresh enough to be enforced as a floor")

	// Deliberately not asserted: a non-zero counter. The devnet forges under
	// the opcert this suite generates at counter 0, so 0 is the correct
	// on-chain value and is also this field's zero value — an equality
	// assertion on it would hold just as well if nothing had been read at all.
	// "Observed" and "non-zero" are separate claims, and only the first is
	// true here; proving the second would mean rotating the pool forward to a
	// higher counter and waiting out another 5-minute refresh interval, for no
	// extra coverage of the wire protocol. What is asserted instead is the
	// range: a counter the chain cannot have produced means the decode is
	// wrong even though the query "worked".
	assert.GreaterOrEqual(t, snap.counter, int64(0),
		"an opcert counter cannot be negative")
	assert.Less(t, snap.counter, int64(10),
		"a devnet that has just started cannot have rotated its opcert this "+
			"many times; a large counter means the query decoded wrongly")
}
