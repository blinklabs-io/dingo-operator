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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPool(b byte) common.Blake2b224 {
	var h common.Blake2b224
	for i := range h {
		h[i] = b
	}
	return h
}

// One answer from the node holds every pool's counter, so three pools are
// served by one query.
func TestCountersForThreePools(t *testing.T) {
	t.Parallel()
	a, b, c, other := testPool(1), testPool(2), testPool(3), testPool(9)
	chain := map[common.Blake2b224]uint64{a: 4, b: 0, c: 17, other: 3}

	got, err := countersFor(chain, []common.Blake2b224{a, b, c})
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, Counter{Value: 4, Found: true}, got[a])
	// A counter of 0 is a real counter, not "unknown".
	assert.Equal(t, Counter{Value: 0, Found: true}, got[b])
	assert.Equal(t, Counter{Value: 17, Found: true}, got[c])
	assert.NotContains(t, got, other)
}

func TestCountersForAbsentAndImplausiblePools(t *testing.T) {
	t.Parallel()
	a, b, c := testPool(1), testPool(2), testPool(3)
	chain := map[common.Blake2b224]uint64{a: 5, b: MaxPlausibleCounter + 1}

	got, err := countersFor(chain, []common.Blake2b224{a, b, c})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "implausible")
	// The usable pool survives; the implausible and the absent one do not.
	assert.Equal(t, map[common.Blake2b224]Counter{
		a: {Value: 5, Found: true},
	}, got)
}

func TestFetchPoolsRequiresAddress(t *testing.T) {
	t.Parallel()
	_, err := NewNtCFetcher().FetchPools(
		context.Background(), Query{}, []common.Blake2b224{testPool(1)},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address is empty")
}
