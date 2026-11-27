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
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// A pod that does not answer does not stop the others from being asked, in
// ordinal order.
func TestOnChainCounterFallsBackToNextPod(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	want := []string{
		"bp-0.bp-headless.ns.svc.cluster.local:3002",
		"bp-1.bp-headless.ns.svc.cluster.local:3002",
		"bp-2.bp-headless.ns.svc.cluster.local:3002",
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

// podReply is one pod's answer: an error, or the counters it reports, in pool
// order (a single read uses the first).
type podReply struct {
	err  error
	vals []int64
}

// podOnChain answers per call, in ordinal order.
type podOnChain struct {
	replies []podReply
	calls   int
}

func (s *podOnChain) next() podReply {
	r := s.replies[s.calls]
	s.calls++
	return r
}

func (s *podOnChain) Fetch(
	_ context.Context,
	_ onchain.Query,
) (onchain.Counter, error) {
	r := s.next()
	if r.err != nil {
		return onchain.Counter{}, r.err
	}
	return onchain.Counter{Value: r.vals[0], Found: true}, nil
}

func (s *podOnChain) FetchPools(
	_ context.Context,
	_ onchain.Query,
	pools []common.Blake2b224,
) (map[common.Blake2b224]onchain.Counter, error) {
	r := s.next()
	if r.err != nil {
		return nil, r.err
	}
	out := map[common.Blake2b224]onchain.Counter{}
	for i, v := range r.vals {
		out[pools[i]] = onchain.Counter{Value: v, Found: true}
	}
	return out, nil
}

// A pod that is still catching up reports an older counter, and unready pods
// are dialled, so the highest answer wins rather than the first.
func TestOnChainCounterTakesHighestAcrossPods(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	refused := errors.New("refused")
	replies := func(a, b int64) []podReply {
		return []podReply{
			{vals: []int64{3, b - 1}},
			{vals: []int64{a, b}},
			{err: refused},
		}
	}

	t.Run("poolId", func(t *testing.T) {
		t.Parallel()
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		agentSigned(dn, 3)
		stub := &podOnChain{replies: replies(7, 0)}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 3, stub.calls)
		assert.Equal(t, int64(7), dn.Status.OpCert.OnChainCounter)
	})
	t.Run("pool list", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode("bp", "ns", 0, pools...)
		agentSigned(dn, 3)
		stub := &podOnChain{replies: replies(7, 5)}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 3, stub.calls)
		assert.Equal(t, int64(7), poolStatus(dn, pools[0].id).OnChainCounter)
		assert.Equal(t, int64(5), poolStatus(dn, pools[1].id).OnChainCounter)
	})
}

// On-chain counters only increase, so a lower reading of the same pool is a
// stale one and leaves the stored counter and its timestamp alone.
func TestOnChainCounterLowerReadingKeepsStored(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	was := metav1.NewTime(time.Unix(1_700_000_000, 0))

	t.Run("poolId", func(t *testing.T) {
		t.Parallel()
		dn := forgerNode("bp", "ns", pools[0].id, pools[0].secret, 0)
		agentSigned(dn, 1)
		parsed, err := parsePoolID(pools[0].id)
		require.NoError(t, err)
		oc := &dn.Status.OpCert
		oc.OnChainCounter = 9
		oc.OnChainCounterAt = &was
		oc.OnChainCounterPoolID = parsed.String()
		stub := &podOnChain{replies: []podReply{{vals: []int64{4}}}}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 1, stub.calls)
		assert.Equal(t, int64(9), oc.OnChainCounter)
		assert.True(t, oc.OnChainCounterAt.Equal(&was))
	})
	t.Run("pool list", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode("bp", "ns", 0, pools...)
		agentSigned(dn, 1)
		syncPoolStatuses(dn)
		for _, p := range pools {
			ps := poolStatus(dn, p.id)
			ps.OnChainCounter = 9
			ps.OnChainCounterAt = &was
		}
		stub := &podOnChain{replies: []podReply{{vals: []int64{4, 12}}}}
		c := conflictClient(t)
		r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
		r.refreshOnChainCounter(context.Background(), dn)
		assert.Equal(t, 1, stub.calls)
		lower, higher := poolStatus(dn, pools[0].id), poolStatus(dn, pools[1].id)
		assert.Equal(t, int64(9), lower.OnChainCounter)
		assert.True(t, lower.OnChainCounterAt.Equal(&was))
		assert.Equal(t, int64(12), higher.OnChainCounter)
		assert.False(t, higher.OnChainCounterAt.Equal(&was))
	})
}
