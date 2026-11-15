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

package forgestatus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPoolA = "pool1ynxc4rk6vhjm4hpzc4kmjqgfgs3eg4ztlj9z08ulvwudclymzt8"
	testPoolB = "pool1a42ap7auup90zs3kl6r9yd8hjt9rq06jg4v5lnsjj74fwk34lcu"
	testPoolC = "pool1mmzw0j3ef9esqsnyu9gq2w6gdm7av2e664v7hpvg94lkxxr5qgh"
)

// multiPoolMetrics is what Dingo v0.94.0 exposes for a node that forges for
// three pools: the forging and KES series carry a pool_id label (bech32, from
// lcommon.PoolId.String), the node-wide series do not. It is derived from
// node_pool_producers.go poolMetricsRegistry and ledger/forging/forger.go
// registerCredentialMetrics, not captured from a running node.
const multiPoolMetrics = `# HELP cardano_node_metrics_currentKESPeriod_int current KES period
# TYPE cardano_node_metrics_currentKESPeriod_int gauge
cardano_node_metrics_currentKESPeriod_int{pool_id="` + testPoolA + `"} 900
cardano_node_metrics_currentKESPeriod_int{pool_id="` + testPoolB + `"} 900
cardano_node_metrics_currentKESPeriod_int{pool_id="` + testPoolC + `"} 900
cardano_node_metrics_remainingKESPeriods_int{pool_id="` + testPoolA + `"} 55
cardano_node_metrics_remainingKESPeriods_int{pool_id="` + testPoolB + `"} 7
cardano_node_metrics_remainingKESPeriods_int{pool_id="` + testPoolC + `"} 31
cardano_node_metrics_operationalCertificateStartKESPeriod_int{pool_id="` + testPoolA + `"} 838
cardano_node_metrics_operationalCertificateStartKESPeriod_int{pool_id="` + testPoolB + `"} 845
cardano_node_metrics_operationalCertificateStartKESPeriod_int{pool_id="` + testPoolC + `"} 869
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int{pool_id="` + testPoolA + `"} 955
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int{pool_id="` + testPoolB + `"} 907
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int{pool_id="` + testPoolC + `"} 931
dingo_forge_opcert_counter_loaded{pool_id="` + testPoolA + `"} 4
dingo_forge_opcert_counter_loaded{pool_id="` + testPoolB + `"} 9
dingo_forge_opcert_counter_loaded{pool_id="` + testPoolC + `"} NaN
cardano_node_metrics_slotNum_int 123456
dingo_ledger_pipeline_stuck 0
dingo_ledger_pipeline_halted 0
`

func TestParseMultiPoolLabels(t *testing.T) {
	t.Parallel()
	st, err := Parse(strings.NewReader(multiPoolMetrics))
	require.NoError(t, err)

	require.Len(t, st.Pools, 3)
	a, b, c := st.Pools[testPoolA], st.Pools[testPoolB], st.Pools[testPoolC]
	require.NotNil(t, a)
	require.NotNil(t, b)
	require.NotNil(t, c)

	assert.True(t, a.HasKESData)
	assert.Equal(t, int64(900), a.CurrentKESPeriod)
	assert.Equal(t, int64(55), a.RemainingKESPeriods)
	assert.Equal(t, int64(838), a.OpCertStartKESPeriod)
	assert.Equal(t, int64(955), a.OpCertExpiryKESPeriod)
	assert.True(t, a.HasOpCertCounter)
	assert.Equal(t, int64(4), a.OpCertCounter)

	// Each pool keeps its own value: a parser that drops the label keeps only
	// the last sample of each series.
	assert.Equal(t, int64(7), b.RemainingKESPeriods)
	assert.Equal(t, int64(845), b.OpCertStartKESPeriod)
	assert.Equal(t, int64(9), b.OpCertCounter)
	assert.Equal(t, int64(31), c.RemainingKESPeriods)
	assert.Equal(t, int64(931), c.OpCertExpiryKESPeriod)

	// NaN means the node has no opcert loaded for the pool; it is not a
	// counter.
	assert.False(t, c.HasOpCertCounter)

	// The labelled series are not folded into the node-wide fields.
	assert.False(t, st.HasKESData)
	assert.False(t, st.HasOpCertCounter)
	assert.True(t, st.HasTipSlot)
	assert.Equal(t, int64(123456), st.TipSlot)
	assert.True(t, st.HasPipelineStuckMetric)
}

func TestParseSinglePoolHasNoPoolSeries(t *testing.T) {
	t.Parallel()
	st, err := Parse(strings.NewReader(
		"cardano_node_metrics_remainingKESPeriods_int 55\n" +
			"cardano_node_metrics_currentKESPeriod_int 900\n" +
			"dingo_forge_opcert_counter_loaded 3\n",
	))
	require.NoError(t, err)
	assert.Nil(t, st.Pools)
	assert.True(t, st.HasKESData)
	assert.Equal(t, int64(55), st.RemainingKESPeriods)
}

func TestParseLabelledSeriesWithOtherLabels(t *testing.T) {
	t.Parallel()
	st, err := Parse(strings.NewReader(
		`cardano_node_metrics_remainingKESPeriods_int{network="preview",` +
			`pool_id="` + testPoolA + `"} 12` + "\n",
	))
	require.NoError(t, err)
	pm := st.Pools[testPoolA]
	require.NotNil(t, pm)
	assert.Equal(t, int64(12), pm.RemainingKESPeriods)
}
