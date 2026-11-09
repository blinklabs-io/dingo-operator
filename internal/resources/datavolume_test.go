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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func initContainerNames(pod corev1.PodSpec) []string {
	names := make([]string, 0, len(pod.InitContainers))
	for _, c := range pod.InitContainers {
		names = append(names, c.Name)
	}
	return names
}

func TestDataVolumeStickyInitContainer(t *testing.T) {
	t.Parallel()
	noMithril := relayNode()
	noMithril.Spec.Mithril.Enabled = new(false)
	for _, tc := range []struct {
		name  string
		dn    *dingov1alpha1.DingoNode
		opts  RenderOptions
		names []string
	}{
		{
			name:  "relay",
			dn:    relayNode(),
			opts:  RenderOptions{Replicas: 1},
			names: []string{dataStickyInitName, mithrilInitName},
		},
		{
			name: "block producer",
			dn:   bpNode(),
			opts: RenderOptions{Replicas: 1, MountKeys: true},
			names: []string{
				dataStickyInitName, keysInitName, mithrilInitName,
			},
		},
		{
			name:  "mithril disabled",
			dn:    noMithril,
			opts:  RenderOptions{Replicas: 1},
			names: []string{dataStickyInitName},
		},
		{
			name: "legacy data volume",
			dn:   relayNode(),
			opts: RenderOptions{Replicas: 1, DataVolumeName: "data"},
			names: []string{
				dataStickyInitName, mithrilInitName,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pod := BuildStatefulSet(tc.dn, tc.opts).Spec.Template.Spec
			require.Equal(t, tc.names, initContainerNames(pod))
			c := pod.InitContainers[0]
			assert.Equal(t, imageRef(tc.dn), c.Image)
			assert.Equal(t, []string{
				"/bin/sh", "-c",
				"[ -k /data ] || chmod +t /data",
			}, c.Command)
			assert.Equal(t, []corev1.VolumeMount{{
				Name:      tc.opts.dataVolume(),
				MountPath: dataMountPath,
			}}, c.VolumeMounts)

			security := c.SecurityContext
			require.NotNil(t, security)
			require.NotNil(t, security.RunAsUser)
			assert.Equal(t, int64(0), *security.RunAsUser)
			require.NotNil(t, security.RunAsNonRoot)
			assert.False(t, *security.RunAsNonRoot)
			require.NotNil(t, security.ReadOnlyRootFilesystem)
			assert.True(t, *security.ReadOnlyRootFilesystem)
			require.NotNil(t, security.AllowPrivilegeEscalation)
			assert.False(t, *security.AllowPrivilegeEscalation)
			assert.Nil(t, security.Privileged)
			require.NotNil(t, security.Capabilities)
			assert.Equal(t, []corev1.Capability{"ALL"},
				security.Capabilities.Drop)
			assert.Equal(t, []corev1.Capability{"FOWNER"},
				security.Capabilities.Add)
		})
	}
}

func mithrilInitContainer(t *testing.T, pod corev1.PodSpec) corev1.Container {
	t.Helper()
	for _, c := range pod.InitContainers {
		if c.Name == mithrilInitName {
			return c
		}
	}
	t.Fatal("no mithril init container")
	return corev1.Container{}
}
