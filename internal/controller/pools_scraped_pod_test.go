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

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// scrapedPoolPod is replica 0 of the pool-list node bp, which staged pool.
func scrapedPoolPod(uid, pool string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bp-0",
			Namespace: "ns",
			UID:       types.UID(uid),
			Labels:    map[string]string{resources.NodeLabel: "bp"},
			Annotations: map[string]string{
				resources.KeysPoolAnnotation: pool,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// replacingFetcher returns st, and before returning replaces the scraped pod
// with next, as a roll does between the scrape and a later read.
type replacingFetcher struct {
	c    client.Client
	st   *forgestatus.Status
	next *corev1.Pod
}

func (f replacingFetcher) Fetch(
	ctx context.Context,
	_ string,
) (*forgestatus.Status, error) {
	if f.next != nil {
		old := &corev1.Pod{}
		if err := f.c.Get(
			ctx, client.ObjectKeyFromObject(f.next), old,
		); err != nil {
			return nil, err
		}
		if err := f.c.Delete(ctx, old); err != nil {
			return nil, err
		}
		if err := f.c.Create(ctx, f.next.DeepCopy()); err != nil {
			return nil, err
		}
	}
	return f.st, nil
}

// During a roll from pool a to pool b the template already stages b while
// bp-0 still runs a and exports a's series unlabelled. They are a's: b is not
// given a's KES state or loaded counter, and a's status is not cleared.
func TestRefreshForgeStatusAttributesUnlabelledMetricsToTheScrapedPod(
	t *testing.T,
) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	a, b := pools[0], pools[1]
	scrape := &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		HasKESData: true, CurrentKESPeriod: 900, RemainingKESPeriods: 7,
		OpCertExpiryKESPeriod: 907,
		OpCertCounter:         5, HasOpCertCounter: true,
	}
	setup := func(t *testing.T) (*dingov1alpha1.DingoNode, []client.Object) {
		t.Helper()
		dn := poolListNode("bp", "ns", 0, pools...)
		dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
		// a was refused and left out of the template; b replaced it.
		pa := poolStatus(dn, a.id)
		pa.KES.RemainingPeriods = 30
		pa.LoadedCounter = new(int64(2))
		poolStatus(dn, b.id).KeysValid = true
		return dn, []client.Object{
			dn, runningPoolsStatefulSet("bp", "ns", b.id),
		}
	}

	t.Run("the scraped pod runs a", func(t *testing.T) {
		t.Parallel()
		dn, objs := setup(t)
		c := conflictClient(t, append(objs, scrapedPoolPod("pod-a", a.id))...)
		r := poolReconciler(c)
		r.ForgeStatus = poolMetricsFetcher{st: scrape}
		r.refreshForgeStatus(context.Background(), dn)

		pa, pb := poolStatus(dn, a.id), poolStatus(dn, b.id)
		assert.Equal(t, dingov1alpha1.KESStatus{}, pb.KES)
		assert.Nil(t, pb.LoadedCounter)
		assert.Equal(t, int64(7), pa.KES.RemainingPeriods)
		require.NotNil(t, pa.LoadedCounter)
		assert.Equal(t, int64(5), *pa.LoadedCounter)
	})

	t.Run("bp-0 is replaced by b's pod during the scrape", func(t *testing.T) {
		t.Parallel()
		dn, objs := setup(t)
		c := conflictClient(t, append(objs, scrapedPoolPod("pod-a", a.id))...)
		r := poolReconciler(c)
		r.ForgeStatus = replacingFetcher{
			c: c, st: scrape, next: scrapedPoolPod("pod-b", b.id),
		}
		r.refreshForgeStatus(context.Background(), dn)

		// Which pod answered is unknown: nothing is attributed, and a keeps
		// what it had.
		pa, pb := poolStatus(dn, a.id), poolStatus(dn, b.id)
		assert.Equal(t, dingov1alpha1.KESStatus{}, pb.KES)
		assert.Nil(t, pb.LoadedCounter)
		assert.Equal(t, int64(30), pa.KES.RemainingPeriods)
		require.NotNil(t, pa.LoadedCounter)
		assert.Equal(t, int64(2), *pa.LoadedCounter)
	})
}
