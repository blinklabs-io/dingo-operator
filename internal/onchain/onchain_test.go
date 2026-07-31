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

package onchain

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The wire format itself is covered upstream (gouroboros
// protocol/localstatequery). What matters here is that no failure mode can hang
// a reconcile: every path out of Fetch must be bounded.

func TestFetchRequiresAddress(t *testing.T) {
	_, err := NewNtCFetcher().Fetch(context.Background(), Query{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address is empty")
}

// A refused connection must surface as an error naming the dial and the
// address, never a zero Counter a caller could mistake for "the chain says 0".
// (This says nothing about timeouts: a closed localhost port refuses in
// microseconds. TestFetchSilentPeerHitsMaxDuration is the bound test.)
func TestFetchDialFailureIsReported(t *testing.T) {
	// Bind and immediately close to get a port nothing is listening on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	counter, err := NewNtCFetcher().
		Fetch(context.Background(), Query{Address: addr})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dial node-to-client")
	assert.Contains(t, err.Error(), addr)
	assert.False(t, counter.Found)
	assert.Zero(t, counter.Value)
}

// TestFetchSilentPeerHitsMaxDuration is the important one: a peer that accepts
// the TCP connection then says nothing parks the gouroboros handshake, which is
// a blocking call with no context and no timeout of its own. Only MaxDuration
// bounds it, and a reconcile blocked on a wedged node is worse than having no
// counter at all.
func TestFetchSilentPeerHitsMaxDuration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	accepted := make(chan struct{})
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			close(accepted)
			return
		}
		close(accepted)
		// Hold the connection open without ever completing a handshake.
		<-time.After(30 * time.Second)
		_ = conn.Close()
	}()

	f := &NtCFetcher{
		DialTimeout: time.Second,
		MaxDuration: 300 * time.Millisecond,
	}
	start := time.Now()
	_, err = f.Fetch(context.Background(), Query{
		Address:      ln.Addr().String(),
		NetworkMagic: 2,
	})
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 5*time.Second, "Fetch must not outlive MaxDuration")
	<-accepted
}

// A caller's own cancellation must win too, even when it is tighter than
// MaxDuration.
func TestFetchHonoursCallerContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		<-time.After(30 * time.Second)
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(
		context.Background(), 200*time.Millisecond,
	)
	defer cancel()
	start := time.Now()
	_, err = NewNtCFetcher().Fetch(ctx, Query{Address: ln.Addr().String()})
	require.Error(t, err)
	assert.Less(t, time.Since(start), DefaultMaxDuration)
}

// The struct is exported, so nothing stops a caller from constructing one
// directly instead of via NewNtCFetcher. Every unset (or negative) timeout must
// resolve to its default rather than to zero, which would mean "no timeout".
// This checks the resolution, not the resulting wall-clock bound — proving the
// latter for the 20s default would cost 20s of suite time, and
// TestFetchSilentPeerHitsMaxDuration already proves the mechanism.
func TestZeroValueFetcherUsesDefaultTimeouts(t *testing.T) {
	f := &NtCFetcher{}
	assert.Equal(t, DefaultMaxDuration, f.maxDuration())
	assert.Equal(
		t,
		DefaultDialTimeout,
		f.timeout(f.DialTimeout, DefaultDialTimeout),
	)
	assert.Equal(
		t,
		DefaultQueryTimeout,
		f.timeout(-time.Second, DefaultQueryTimeout),
	)
	assert.Equal(t, time.Second, f.timeout(time.Second, DefaultQueryTimeout))
}

// An implausible counter must not become a validation floor: used as one it
// would refuse every rotation until the observation aged out, re-arming on each
// refresh — fail-open inverted into fail-closed.
func TestCounterFromRaw(t *testing.T) {
	tests := []struct {
		name    string
		raw     uint64
		want    Counter
		wantErr bool
	}{
		{
			// A pool's first opcert is numbered 0 and the chain records it once
			// the pool mints, so this is a real observation, not an absence.
			name: "zero is a legitimate counter",
			raw:  0,
			want: Counter{Value: 0, Found: true},
		},
		{
			name: "ordinary counter",
			raw:  7,
			want: Counter{Value: 7, Found: true},
		},
		{
			name: "at the plausibility limit",
			raw:  MaxPlausibleCounter,
			want: Counter{Value: MaxPlausibleCounter, Found: true},
		},
		{
			name:    "past the plausibility limit",
			raw:     MaxPlausibleCounter + 1,
			wantErr: true,
		},
		{
			name:    "decode garbage",
			raw:     math.MaxUint64,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := counterFromRaw(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				assert.False(t, got.Found, "must not be usable as a floor")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
