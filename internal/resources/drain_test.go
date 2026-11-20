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

package resources

import (
	"strconv"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func dingoContainer(
	t *testing.T,
	dn *dingov1alpha1.DingoNode,
) corev1.Container {
	t.Helper()
	sts := BuildStatefulSet(dn, RenderOptions{Replicas: 1})
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return c
		}
	}
	require.FailNow(t, "no dingo container in the pod template")
	return corev1.Container{}
}

// preStopSeconds returns the preStop sleep, or -1 when the container has no
// sleep hook.
func preStopSeconds(c corev1.Container) int64 {
	if c.Lifecycle == nil || c.Lifecycle.PreStop == nil ||
		c.Lifecycle.PreStop.Sleep == nil {
		return -1
	}
	return c.Lifecycle.PreStop.Sleep.Seconds
}

func TestDrainDelayPreStop(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		node  func() *dingov1alpha1.DingoNode
		drain *int64
		grace *int64
		want  int64
	}{
		{
			name: "relay default delays SIGTERM",
			node: relayNode,
			want: DefaultDrainDelaySeconds,
		},
		{
			name: "block producer default does not delay forging pod",
			node: bpNode,
			want: -1,
		},
		{
			name:  "relay zero disables the hook",
			node:  relayNode,
			drain: new(int64(0)),
			want:  -1,
		},
		{
			name:  "relay explicit value",
			node:  relayNode,
			drain: new(int64(25)),
			grace: new(int64(120)),
			want:  25,
		},
		{
			name:  "block producer explicit value",
			node:  bpNode,
			drain: new(int64(20)),
			want:  20,
		},
		{
			name:  "relay default skipped when it would starve Dingo",
			node:  relayNode,
			grace: new(int64(40)),
			want:  -1,
		},
		{
			name:  "explicit value is clamped under the grace period",
			node:  relayNode,
			drain: new(int64(100)),
			grace: new(int64(20)),
			want:  9,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := tc.node()
			dn.Spec.DrainDelaySeconds = tc.drain
			dn.Spec.TerminationGracePeriodSeconds = tc.grace
			c := dingoContainer(t, dn)

			assert.Equal(t, tc.want, preStopSeconds(c),
				"preStop sleep before SIGTERM")
			if tc.want < 0 {
				assert.Nil(t, c.Lifecycle,
					"no hook, and never a zero-second sleep")
			}
		})
	}
}

// TestDrainDelayStaysInsideGracePeriod: the grace period clock covers the
// preStop sleep, so Dingo's shutdown budget must shrink by the delay or kubelet
// SIGKILLs it mid-flush.
func TestDrainDelayStaysInsideGracePeriod(t *testing.T) {
	t.Parallel()
	for _, grace := range []int64{15, 40, 60, 120} {
		for _, drain := range []int64{0, 5, 30, 100} {
			t.Run(strconv.FormatInt(grace, 10)+"/"+strconv.FormatInt(drain, 10),
				func(t *testing.T) {
					t.Parallel()
					dn := relayNode()
					dn.Spec.TerminationGracePeriodSeconds = new(grace)
					dn.Spec.DrainDelaySeconds = new(drain)
					c := dingoContainer(t, dn)

					slept := max(preStopSeconds(c), 0)
					timeout := int64(dingoShutdownTimeout(dn).Seconds())
					assert.Positive(t, timeout)
					assert.Less(t, slept+timeout, grace,
						"preStop sleep plus Dingo's shutdown budget must fit "+
							"inside the grace period")
				})
		}
	}
}
