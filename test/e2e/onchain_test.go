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

	pods := &corev1.PodList{}
	require.NoError(h.t, h.client.List(ctx, pods,
		client.InNamespace(operatorNamespace),
		client.MatchingLabels{
			"app":    "dingo-operator",
			ntcLabel: ntcAllowed,
		}),
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
			if peer.PodSelector != nil &&
				peer.PodSelector.MatchLabels[ntcLabel] == ntcAllowed &&
				peer.NamespaceSelector != nil &&
				peer.NamespaceSelector.MatchLabels[ntcLabel] == ntcAllowed {
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
// This guards the wrapped chain-dependent-state response decoding fixed by
// gouroboros v0.193.3. A regression leaves OnChainCounterAvailable false with
// reason QueryFailed instead of producing the pool's counter.
func TestOnChainOpCertCounterObserved(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "observes counter from a forging node"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(), testTimeout,
			)
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

			// The counter map is populated from blocks: a pool that has never
			// minted under an operational certificate is legitimately absent.
			h.waitForged(ctx, 1)

			snap := h.waitOnChainCounterObserved(ctx)
			t.Logf("on-chain counter observed: counter=%d pool=%s (%s)",
				snap.counter, snap.poolID, snap.message)

			assert.NotEqual(
				t,
				reasonOnChainDisabled,
				snap.reason,
				"spec.blockProducer.nodeToClient.enabled did not reach the node",
			)
			assert.Equal(t, hex.EncodeToString(dn.Keys.PoolID), snap.poolID,
				"the counter must be recorded against the devnet pool")
			assert.True(t, snap.observedAt,
				"status.opcert.onChainCounterAt must be stamped")
			assert.GreaterOrEqual(t, snap.counter, int64(0),
				"an opcert counter cannot be negative")
			assert.Less(t, snap.counter, int64(10),
				"a new devnet cannot have this many opcert rotations")
		})
	}
}
