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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node that forges for one pool exposes its forge series unlabelled, and
// Dingo names the loaded counter dingo_forge_opcert_counter_loaded there too.
func TestRefreshForgeStatusOnePoolReadsUnlabelledLoadedCounter(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	poolStatus(dn, pools[0].id).KeysValid = true // pool b was dropped
	st, err := forgestatus.Parse(strings.NewReader(`
cardano_node_metrics_currentKESPeriod_int 900
cardano_node_metrics_remainingKESPeriods_int 20
cardano_node_metrics_operationalCertificateStartKESPeriod_int 858
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int 920
dingo_forge_opcert_counter_loaded 6
dingo_forge_opcert_counter_onchain 5
dingo_ledger_pipeline_stuck 0
dingo_ledger_pipeline_halted 0
`))
	require.NoError(t, err)
	r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
	r.refreshForgeStatus(context.Background(), dn)

	ps := poolStatus(dn, pools[0].id)
	assert.Equal(t, int64(20), ps.KES.RemainingPeriods)
	require.NotNil(t, ps.LoadedCounter)
	assert.Equal(t, int64(6), *ps.LoadedCounter)
}
