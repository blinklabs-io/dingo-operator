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
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

// agentForger is forgerNode signing through a plaintext remote agent, with a
// UID so the StatefulSet it creates is controlled by it.
func agentForger(
	name, ns, poolID, secret string,
	age time.Duration,
) *dingov1alpha1.DingoNode {
	dn := forgerNode(name, ns, poolID, secret, age)
	dn.UID = types.UID("uid-" + ns + "-" + name)
	dn.Spec.Image.Tag = agentImageTag
	dn.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:   "kes-agent.signing.svc:7000",
		Plaintext: true,
	}
	return dn
}

// Producers that sign through an agent are fenced like any other pair: the
// operator cannot prove they share one signing authority, profile and slot
// watermark, so the newer is refused, and a running newer is scaled to zero.
func TestReconcileAgentSignedForgersAreFenced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		older  *dingov1alpha1.DingoNode
		newer  *dingov1alpha1.DingoNode
		sameNS bool
	}{
		{
			name:  "same pool in another namespace",
			older: agentForger("bp", "ns-a", conflictPoolID, "keys-a", time.Hour),
			newer: agentForger("bp", "ns-b", conflictPoolID, "keys-b", 0),
		},
		{
			name: "same pool in one namespace",
			older: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys-a", time.Hour,
			),
			newer:  agentForger("bp-2", "ns-a", conflictPoolID, "keys-b", 0),
			sameNS: true,
		},
		{
			name: "same keys Secret name in one namespace",
			older: agentForger(
				"bp-1", "ns-a", conflictPoolID, "keys", time.Hour,
			),
			newer:  agentForger("bp-2", "ns-a", otherPoolID, "keys", 0),
			sameNS: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			// The newer runs first, then the older appears.
			c := conflictClient(t, tt.newer)
			r := reconcilerFor(c)
			reconcileNode(t, r, tt.newer)
			require.True(t, forging(t, c, tt.newer))
			require.NoError(t, c.Create(ctx, tt.older))
			reconcileNode(t, r, tt.older)
			reconcileNode(t, r, tt.newer)
			assert.True(t, refusedAsDuplicate(t, c, tt.newer),
				degradedMessage(t, c, tt.newer))
			if tt.sameNS {
				assert.False(t, forging(t, c, tt.newer),
					"a running newer producer is scaled to zero")
				reconcileNode(t, r, tt.older)
				assert.True(t, forging(t, c, tt.older))
				assert.False(t, refusedAsDuplicate(t, c, tt.older),
					degradedMessage(t, c, tt.older))
			}
		})
	}
}
