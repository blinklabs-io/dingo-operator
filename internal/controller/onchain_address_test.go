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
	"errors"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
)

// addrOnChain records the address of every query and answers per call: a call
// beyond len(errs) succeeds.
type addrOnChain struct {
	errs  []error
	addrs []string
}

func (s *addrOnChain) next(addr string) error {
	n := len(s.addrs)
	s.addrs = append(s.addrs, addr)
	if n < len(s.errs) {
		return s.errs[n]
	}
	return nil
}

func (s *addrOnChain) Fetch(
	_ context.Context,
	q onchain.Query,
) (onchain.Counter, error) {
	if err := s.next(q.Address); err != nil {
		return onchain.Counter{}, err
	}
	return onchain.Counter{Value: 4, Found: true}, nil
}

func (s *addrOnChain) FetchPools(
	_ context.Context,
	q onchain.Query,
	pools []common.Blake2b224,
) (map[common.Blake2b224]onchain.Counter, error) {
	if err := s.next(q.Address); err != nil {
		return nil, err
	}
	out := make(map[common.Blake2b224]onchain.Counter, len(pools))
	for _, p := range pools {
		out[p] = onchain.Counter{Value: 4, Found: true}
	}
	return out, nil
}

func agentSigned(dn *dingov1alpha1.DingoNode, replicas int32) {
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{}
	dn.Spec.Replicas = &replicas
}

// The client Service has no endpoint for an unready pod, and a keyless agent
// producer is unready until its first key is issued, so the counter has to be
// read from a pod through the headless Service.
func TestOnChainCounterDialsPodThroughHeadlessService(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")

	t.Run("poolId", func(t *testing.T) {
		t.Parallel()
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		agentSigned(dn, 1)
		stub := &addrOnChain{}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t,
			[]string{"bp-0.bp-headless.ns.svc.cluster.local:3002"}, stub.addrs)
		require.NotNil(t, dn.Status.OpCert.OnChainCounterAt)
	})
	t.Run("pool list", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode("bp", "ns", 0, pools...)
		agentSigned(dn, 1)
		stub := &addrOnChain{}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t,
			[]string{"bp-0.bp-headless.ns.svc.cluster.local:3002"}, stub.addrs)
		assert.Equal(t, int64(4), poolStatus(dn, pools[0].id).OnChainCounter)
	})
}

// Every pod of an agent-signed producer serves the same chain state, so the
// next pod is tried when one does not answer, in ordinal order.
func TestOnChainCounterFallsBackToNextPod(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	want := []string{
		"bp-0.bp-headless.ns.svc.cluster.local:3002",
		"bp-1.bp-headless.ns.svc.cluster.local:3002",
	}

	t.Run("poolId", func(t *testing.T) {
		t.Parallel()
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		agentSigned(dn, 3)
		stub := &addrOnChain{errs: []error{errors.New("refused")}}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, want, stub.addrs)
		require.NotNil(t, dn.Status.OpCert.OnChainCounterAt)
	})
	t.Run("pool list", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode("bp", "ns", 0, pools...)
		agentSigned(dn, 3)
		stub := &addrOnChain{errs: []error{errors.New("refused")}}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, want, stub.addrs)
		assert.Equal(t, int64(4), poolStatus(dn, pools[0].id).OnChainCounter)
	})
	t.Run("every pod refuses", func(t *testing.T) {
		t.Parallel()
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		agentSigned(dn, 2)
		refused := errors.New("refused")
		stub := &addrOnChain{errs: []error{refused, refused}}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Len(t, stub.addrs, 2)
		assert.Nil(t, dn.Status.OpCert.OnChainCounterAt)
		// A second pass inside the interval does not dial again.
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Len(t, stub.addrs, 2)
	})
}
