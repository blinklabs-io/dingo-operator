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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The CRD admits a drain delay only below the grace period less the shutdown
// headroom, and every admitted value renders unclamped: the CEL bound and the
// builder's clamp must agree, or an accepted spec silently sleeps less.
func TestCRDDrainDelayBound(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	const ns = "drain-bound"
	createNamespace(t, ctx, c, ns)
	r := reconcilerFor(c)
	r.PreStopSleep = true

	for i, tc := range []struct {
		grace, drain int64
		admitted     bool
	}{
		{grace: 60, drain: 49, admitted: true},
		{grace: 60, drain: 50},
		{grace: 15, drain: 4, admitted: true},
		{grace: 15, drain: 5},
		{grace: 15, drain: 0, admitted: true},
	} {
		name := "drain-" + strconv.Itoa(i)
		dn := relayNode(name, ns, "preview")
		dn.Spec.TerminationGracePeriodSeconds = new(tc.grace)
		dn.Spec.DrainDelaySeconds = new(tc.drain)
		err := c.Create(ctx, dn)
		if !tc.admitted {
			require.Error(t, err, "grace %d drain %d", tc.grace, tc.drain)
			assert.True(t, apierrors.IsInvalid(err), err.Error())
			assert.Contains(t, err.Error(), "drainDelaySeconds")
			continue
		}
		require.NoError(t, err, "grace %d drain %d", tc.grace, tc.drain)
		reconcile(t, ctx, r, name, ns)
		sts := getStatefulSet(t, ctx, c, name, ns)
		dingo := sts.Spec.Template.Spec.Containers[0]
		if tc.drain == 0 {
			assert.Nil(t, dingo.Lifecycle, "a zero delay renders no hook")
			continue
		}
		require.NotNil(t, dingo.Lifecycle)
		require.NotNil(t, dingo.Lifecycle.PreStop)
		require.NotNil(t, dingo.Lifecycle.PreStop.Sleep)
		assert.Equal(t, tc.drain, dingo.Lifecycle.PreStop.Sleep.Seconds,
			"an admitted delay is rendered as given")
	}
}
