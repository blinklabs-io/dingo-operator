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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A producer scaled to zero has no pod whose keys it could judge: RotationDue
// and the KES status from before the scale-down must not outlive the pods.
func TestRefreshForgeStatusZeroProducerReplicasClearsKES(t *testing.T) {
	t.Parallel()
	dn := agentProducer(0)
	dn.Status.KES = dingov1alpha1.KESStatus{
		CurrentPeriod: 5, OpCertKESPeriod: 2, ExpiryPeriod: 62,
		RemainingPeriods: 3,
	}
	dn.Status.Pools = []dingov1alpha1.PoolStatus{{
		PoolID: conflictPoolID, KeysValid: true, KES: dn.Status.KES,
	}}
	meta.SetStatusCondition(&dn.Status.Conditions, metav1.Condition{
		Type: condRotationDue, Status: metav1.ConditionTrue,
		Reason: "KESExpiringSoon", Message: "3 KES periods remaining",
	})
	fetcher := &statusFetcher{st: kesScrape(3, 2, 0)}
	r := &DingoNodeReconciler{ForgeStatus: fetcher}

	r.refreshForgeStatus(t.Context(), dn)

	assert.Empty(t, fetcher.urls, "there are no replicas to scrape")
	cond := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionUnknown, cond.Status)
	assert.Equal(t, "NoReplicas", cond.Reason)
	assert.Equal(t, dingov1alpha1.KESStatus{}, dn.Status.KES)
	assert.Equal(t, dingov1alpha1.KESStatus{}, dn.Status.Pools[0].KES)
}
