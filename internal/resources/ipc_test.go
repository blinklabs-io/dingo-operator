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

// TestIPCSocketDirectory checks that Dingo's UNIX socket lives in a directory
// the node's own user creates with mode 0755. Dingo v0.94.0 and later refuse a
// socket parent that group or other can write, and the /ipc emptyDir root is
// world-writable.
func TestIPCSocketDirectory(t *testing.T) {
	t.Parallel()
	legacy := relayNode()
	legacy.Spec.Image.Tag = "0.69.0"
	for _, tc := range []struct {
		name string
		dn   *dingov1alpha1.DingoNode
		opts RenderOptions
	}{
		{"relay", relayNode(), RenderOptions{Replicas: 1}},
		{"block producer", bpNode(), RenderOptions{Replicas: 1, MountKeys: true}},
		{"legacy image user", legacy, RenderOptions{Replicas: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				"/ipc/dingo/dingo.socket",
				envMap(tc.dn, tc.opts)["DINGO_SOCKET_PATH"],
			)

			pod := BuildStatefulSet(tc.dn, tc.opts).Spec.Template.Spec
			names := initContainerNames(pod)
			require.GreaterOrEqual(t, len(names), 2)
			assert.Equal(t, ipcInitName, names[1])
			c := pod.InitContainers[1]
			assert.Equal(t, imageRef(tc.dn), c.Image)
			assert.Equal(t, []string{
				"/bin/sh", "-c",
				"mkdir -p /ipc/dingo && chmod 0755 /ipc/dingo",
			}, c.Command)

			mounts := map[string]corev1.VolumeMount{}
			for _, m := range c.VolumeMounts {
				mounts[m.MountPath] = m
			}
			require.Contains(t, mounts, "/ipc")
			assert.Equal(t, ipcVolumeName, mounts["/ipc"].Name)
			assert.False(t, mounts["/ipc"].ReadOnly)

			// It must run as the pod's (Dingo's) user so the directory is
			// owned by the process that creates the socket in it.
			security := c.SecurityContext
			require.NotNil(t, security)
			assert.Nil(t, security.RunAsUser)
			require.NotNil(t, security.RunAsNonRoot)
			assert.True(t, *security.RunAsNonRoot)
			require.NotNil(t, security.Capabilities)
			assert.Equal(t, []corev1.Capability{"ALL"},
				security.Capabilities.Drop)
			assert.Empty(t, security.Capabilities.Add)
		})
	}
}

// TestIPCSocketPathOverride checks that a socket path set in spec.environment
// still wins over the operator's default.
func TestIPCSocketPathOverride(t *testing.T) {
	dn := relayNode()
	dn.Spec.Environment = map[string]string{
		"DINGO_SOCKET_PATH": "/ipc/custom/node.socket",
	}
	assert.Equal(
		t,
		"/ipc/custom/node.socket",
		envMap(dn, RenderOptions{})["DINGO_SOCKET_PATH"],
	)
}
