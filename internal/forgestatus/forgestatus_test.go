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

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		want     Status
		wantErr  bool
		checkKES bool
	}{
		{
			name: "block producer with kes metrics",
			body: `# HELP cardano_node_metrics_currentKESPeriod_int kes
cardano_node_metrics_currentKESPeriod_int{network="mainnet"} 900
cardano_node_metrics_remainingKESPeriods_int{network="mainnet"} 55
cardano_node_metrics_operationalCertificateStartKESPeriod_int{network="mainnet"} 838
cardano_node_metrics_operationalCertificateExpiryKESPeriod_int{network="mainnet"} 900
cardano_node_metrics_Forge_forged_int{network="mainnet"} 42
`,
			want: Status{
				CurrentKESPeriod:      900,
				RemainingKESPeriods:   55,
				OpCertStartKESPeriod:  838,
				OpCertExpiryKESPeriod: 900,
				ForgedBlocks:          42,
				HasKESData:            true,
			},
			checkKES: true,
		},
		{
			name:     "relay without kes metrics",
			body:     "some_other_metric 1\n",
			want:     Status{HasKESData: false},
			checkKES: true,
		},
		{
			name:     "malformed lines are ignored",
			body:     "this is not { valid prometheus\ngarbage line\n",
			want:     Status{HasKESData: false},
			checkKES: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(strings.NewReader(tc.body))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.checkKES {
				assert.Equal(t, tc.want.HasKESData, got.HasKESData)
			}
			assert.Equal(t, tc.want.CurrentKESPeriod, got.CurrentKESPeriod)
			assert.Equal(t, tc.want.RemainingKESPeriods, got.RemainingKESPeriods)
			assert.Equal(t, tc.want.OpCertExpiryKESPeriod, got.OpCertExpiryKESPeriod)
			assert.Equal(t, tc.want.ForgedBlocks, got.ForgedBlocks)
		})
	}
}

// The wedge gauges are what distinguish a node that is merely responding from
// one that is still following the chain. Absent gauges must read as healthy,
// since relays and older nodes do not emit them.
func TestParsePipelineWedgeGauges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		wantStuck  bool
		wantHalted bool
	}{
		{
			name:       "absent gauges are not a wedge",
			body:       "cardano_node_metrics_Forge_forged_int 1\n",
			wantStuck:  false,
			wantHalted: false,
		},
		{
			name: "zero gauges are healthy",
			body: "dingo_ledger_pipeline_stuck 0\n" +
				"dingo_ledger_pipeline_halted 0\n",
		},
		{
			name:      "stuck is reported",
			body:      `dingo_ledger_pipeline_stuck{network="preview"} 1` + "\n",
			wantStuck: true,
		},
		{
			name:       "halted is reported",
			body:       `dingo_ledger_pipeline_halted{network="preview"} 1` + "\n",
			wantHalted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(strings.NewReader(tt.body))
			require.NoError(t, err)
			assert.Equal(t, tt.wantStuck, got.PipelineStuck)
			assert.Equal(t, tt.wantHalted, got.PipelineHalted)
		})
	}
}
