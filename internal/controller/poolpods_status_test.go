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

	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A pod that stages a pool but reports no metrics for it leaves that pool
// unobserved: the other pod's healthy series must not stand for both.
func TestMergePoolPodsPodWithoutPoolMetricsIsUnobserved(t *testing.T) {
	t.Parallel()
	dn := agentProducer(2)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: dn.Name, Namespace: dn.Namespace},
	}
	sts.Spec.Template.Annotations = map[string]string{
		resources.KeysPoolAnnotation: conflictPoolID,
	}
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
		WithObjects(sts).Build()

	healthy := kesScrape(40, 900, 3)
	silent := &forgestatus.Status{}
	var merged forgestatus.Status
	running, known := reconcilerFor(c).mergePoolPods(
		t.Context(), dn, []string{"bp-0", "bp-1"},
		[]*forgestatus.Status{healthy, silent},
		map[string]scrapedPodRead{}, &merged,
	)

	require.True(t, known)
	pool := normalizedPoolID(conflictPoolID)
	require.True(t, running[pool])
	pm := merged.Pools[pool]
	require.NotNil(t, pm, "the pool is staged by both pods")
	assert.False(t, pm.HasKESData,
		"one pod reports nothing for the pool, so its KES state is unknown")
	assert.False(t, pm.HasOpCertCounter,
		"a counter is confirmed only once every pod reports it")
}
