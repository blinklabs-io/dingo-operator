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

// Package onchain reads the authoritative on-chain operational-certificate
// counter for a stake pool from a running Dingo node.
//
// The counter lives in the node's chain-dependent state, not in its metrics, so
// it is read over node-to-client (NtC) local-state-query rather than scraped
// like internal/forgestatus does. Dingo serves NtC over TCP on its private port
// as well as over its UNIX socket, so the operator dials the pod directly and
// never needs access to /ipc.
//
// The value is the highest opcert issue number the chain has accepted for the
// pool. A block presenting a lower number is rejected by every node on the
// network, which is why the operator refuses to roll a block producer onto such
// a certificate.
package onchain

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// Timeout defaults. Every one of these bounds a call the operator makes from
// inside a reconcile, so they are deliberately short: a reconcile that blocks
// on a dead or wedged node is a worse failure than not having the counter at
// all, because it stalls every other thing the reconciler does for that node.
const (
	// DefaultDialTimeout bounds the TCP connect.
	DefaultDialTimeout = 5 * time.Second
	// DefaultAcquireTimeout bounds acquiring the node's ledger state.
	DefaultAcquireTimeout = 5 * time.Second
	// DefaultQueryTimeout bounds a single local-state-query round trip.
	DefaultQueryTimeout = 10 * time.Second
	// DefaultMaxDuration hard-caps the whole Fetch, handshake included. It is
	// the only bound that covers the Ouroboros handshake, which the gouroboros
	// API performs synchronously with no timeout of its own.
	DefaultMaxDuration = 20 * time.Second

	// connCloseTimeout bounds how long closing the query connection waits for
	// its goroutines to finish.
	connCloseTimeout = 10 * time.Second
)

// MaxPlausibleCounter is the largest opcert issue number this package will
// report. An operational certificate covers roughly 93 days, so a pool that has
// run since Shelley has a counter in the tens; a million is many thousands of
// years of rotations and cannot be a real value.
//
// The bound matters because the counter is used as a *floor*: a wrong-but-huge
// value (a decode bug, or a node that is not the one we think it is) would turn
// a check designed to fail open into one that refuses every rotation until the
// observation ages out, then refuses again on the next refresh. An implausible
// counter is therefore reported as "could not determine", which falls back to
// the operator's own on-disk floor.
const MaxPlausibleCounter = 1_000_000

// Counter is the result of a counter lookup.
//
// Found distinguishes "the chain has no counter for this pool" from "the
// counter is N". A pool that has never minted a block under an operational
// certificate — a freshly registered pool, or any pool on a node that has not
// yet synced past its first block — legitimately has no counter, and callers
// must not treat that as a counter of 0.
type Counter struct {
	// Value is the highest opcert issue number the chain has accepted for the
	// pool. Only meaningful when Found is true.
	Value int64
	// Found reports whether the chain has recorded a counter for the pool.
	Found bool
}

// Query identifies the node to ask and the pool to ask about.
type Query struct {
	// Address is the node's node-to-client listener as host:port.
	Address string
	// NetworkMagic is the network magic the NtC handshake must present. A
	// mismatch is refused by the node, so the caller must resolve it from the
	// DingoNode spec rather than guessing.
	NetworkMagic uint32
	// PoolID is the pool's cold-key hash (blake2b-224), which is how the
	// on-chain counter map is keyed.
	PoolID common.Blake2b224
}

// Fetcher retrieves the on-chain opcert counter for a pool from a node.
//
// Implementations return an error for every "could not determine" case
// (unreachable node, handshake failure, query failure) and a Counter with
// Found=false when the node answered but the chain holds no counter for the
// pool. Callers are expected to treat both as non-fatal.
type Fetcher interface {
	Fetch(ctx context.Context, q Query) (Counter, error)
}

// PoolsFetcher is implemented by a Fetcher that reads the counters of several
// pools with one query: the chain answers with every pool's counter, so one
// dial serves a node that forges for several pools.
type PoolsFetcher interface {
	// FetchPools returns the counters of the pools the chain has one for; a
	// pool the chain has no counter for is absent from the result. The error
	// is set when the query failed, or when a counter was implausible (the
	// result then still holds the pools that were usable).
	FetchPools(
		ctx context.Context,
		q Query,
		pools []common.Blake2b224,
	) (map[common.Blake2b224]Counter, error)
}

// NtCFetcher is the default Fetcher: it dials the node's node-to-client TCP
// listener and runs a local-state-query.
type NtCFetcher struct {
	DialTimeout    time.Duration
	AcquireTimeout time.Duration
	QueryTimeout   time.Duration
	// MaxDuration hard-caps a single Fetch.
	MaxDuration time.Duration
}

// NewNtCFetcher returns a Fetcher with all timeouts bounded by the defaults.
func NewNtCFetcher() *NtCFetcher {
	return &NtCFetcher{
		DialTimeout:    DefaultDialTimeout,
		AcquireTimeout: DefaultAcquireTimeout,
		QueryTimeout:   DefaultQueryTimeout,
		MaxDuration:    DefaultMaxDuration,
	}
}

// Fetch dials the node, queries the chain-dependent state and returns the
// pool's counter.
func (f *NtCFetcher) Fetch(
	ctx context.Context,
	q Query,
) (Counter, error) {
	counters, err := f.fetchRaw(ctx, q)
	if err != nil {
		return Counter{}, err
	}
	raw, ok := counters[q.PoolID]
	if !ok {
		// The node answered; the chain simply has no counter for this pool.
		return Counter{Found: false}, nil
	}
	return counterFromRaw(raw)
}

// FetchPools reads the counters of several pools with one query; q.PoolID is
// not used.
func (f *NtCFetcher) FetchPools(
	ctx context.Context,
	q Query,
	pools []common.Blake2b224,
) (map[common.Blake2b224]Counter, error) {
	counters, err := f.fetchRaw(ctx, q)
	if err != nil {
		return nil, err
	}
	return countersFor(counters, pools)
}

// countersFor picks the listed pools out of the chain's counter map. A pool
// the chain has no counter for is absent from the result; an implausible
// counter is reported in the error and left out, so one bad value does not
// hide the other pools'.
func countersFor(
	raw map[common.Blake2b224]uint64,
	pools []common.Blake2b224,
) (map[common.Blake2b224]Counter, error) {
	out := make(map[common.Blake2b224]Counter, len(pools))
	var errs []error
	for _, pool := range pools {
		v, ok := raw[pool]
		if !ok {
			continue
		}
		c, err := counterFromRaw(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", pool, err))
			continue
		}
		out[pool] = c
	}
	return out, errors.Join(errs...)
}

// fetchRaw dials the node and returns the chain's counter map.
func (f *NtCFetcher) fetchRaw(
	ctx context.Context,
	q Query,
) (map[common.Blake2b224]uint64, error) {
	if q.Address == "" {
		return nil, errors.New("node-to-client address is empty")
	}
	ctx, cancel := context.WithTimeout(ctx, f.maxDuration())
	defer cancel()

	dialer := net.Dialer{Timeout: f.timeout(f.DialTimeout, DefaultDialTimeout)}
	conn, err := dialer.DialContext(ctx, "tcp", q.Address)
	if err != nil {
		return nil, fmt.Errorf("dial node-to-client %s: %w", q.Address, err)
	}

	type outcome struct {
		counters map[common.Blake2b224]uint64
		err      error
	}
	// Buffered so the worker can always deliver and exit, even after Fetch has
	// already given up on the context deadline below.
	done := make(chan outcome, 1)
	go func() {
		c, err := f.query(ctx, conn, q)
		done <- outcome{counters: c, err: err}
	}()

	select {
	case res := <-done:
		return res.counters, res.err
	case <-ctx.Done():
		// The gouroboros handshake and query calls are blocking and take no
		// context. Closing the raw connection is what unblocks whichever read
		// the worker is parked on, so it finishes and exits rather than leaking
		// for as long as the peer keeps the socket open.
		_ = conn.Close()
		return nil, fmt.Errorf(
			"node-to-client query to %s: %w", q.Address, ctx.Err(),
		)
	}
}

// query performs the handshake and the counter lookup on an established
// connection. It always disposes of conn.
func (f *NtCFetcher) query(
	ctx context.Context,
	conn net.Conn,
	q Query,
) (map[common.Blake2b224]uint64, error) {
	lsqCfg := localstatequery.NewConfig(
		localstatequery.WithAcquireTimeout(
			f.timeout(f.AcquireTimeout, DefaultAcquireTimeout),
		),
		localstatequery.WithQueryTimeout(
			f.timeout(f.QueryTimeout, DefaultQueryTimeout),
		),
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(q.NetworkMagic),
		// Node-to-client: the mini-protocol set that carries local-state-query.
		ouroboros.WithNodeToNode(false),
		// One query and out; nothing here outlives the reconcile.
		ouroboros.WithKeepAlive(false),
		ouroboros.WithLocalStateQueryConfig(lsqCfg),
	)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("node-to-client handshake: %w", err)
	}
	// Drain the asynchronous error channel. It is buffered, and a full buffer
	// would block the muxer's error goroutine, which in turn would keep Close
	// from returning. The loop ends when the connection shuts down and closes
	// the channel.
	go func() {
		for range oConn.ErrorChan() { //nolint:revive // drain and discard
		}
	}()
	// No local-state-query callbacks are registered and no lock is held, so
	// the close cannot wait on anything this goroutine owns. ctx is usually
	// cancelled by the time the query ends, so only its values carry over.
	defer func() {
		closeCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			connCloseTimeout,
		)
		defer cancel()
		_ = oConn.Close(closeCtx)
	}()

	lsq := oConn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, errors.New(
			"node did not offer the local-state-query mini-protocol",
		)
	}
	counters, err := lsq.Client.GetOpCertCounters()
	if err != nil {
		return nil, fmt.Errorf("query on-chain opcert counters: %w", err)
	}
	return counters, nil
}

// counterFromRaw narrows a counter read off the wire to the int64 the status
// API uses, rejecting values that cannot be real. A counter of 0 is legitimate:
// a pool's first operational certificate is numbered 0 and the chain records it
// once the pool mints, so only the upper bound is enforced here.
func counterFromRaw(raw uint64) (Counter, error) {
	if raw > MaxPlausibleCounter {
		return Counter{}, fmt.Errorf(
			"on-chain opcert counter %d is implausible (over %d); refusing to "+
				"use it as a validation floor",
			raw, MaxPlausibleCounter,
		)
	}
	return Counter{Value: int64(raw), Found: true}, nil
}

// maxDuration returns the hard cap for a single Fetch.
func (f *NtCFetcher) maxDuration() time.Duration {
	return f.timeout(f.MaxDuration, DefaultMaxDuration)
}

// timeout falls back to a default for a zero or negative value, so a
// zero-valued NtCFetcher is still bounded rather than blocking forever.
func (f *NtCFetcher) timeout(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
