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

// A pool-list pod carries no agent address env: each pool's agent settings are
// in the pool list file. It signs through the agent while that file is mounted
// and nothing could set a local KES key.
func TestPodSignsThroughAgentPoolList(t *testing.T) {
	t.Parallel()
	dn := poolListNode(dingov1alpha1.SigningAgentSpec{
		Address: "kes-agent.signing.svc:7000", Plaintext: true,
	})
	rendered := func() corev1.PodSpec {
		return renderPod(dn, poolListOpts())
	}
	require.NotEmpty(t, KeysSecretNames(rendered()), "the pools' keys are mounted")
	assert.True(t, PodSignsThroughAgent(rendered()), "rendered for a pool list")

	dingo := func(spec *corev1.PodSpec) *corev1.Container {
		for i := range spec.Containers {
			if spec.Containers[i].Name == containerName {
				return &spec.Containers[i]
			}
		}
		t.Fatal("no dingo container")
		return nil
	}
	for name, mutate := range map[string]func(*corev1.PodSpec){
		"prefixed KES key": func(s *corev1.PodSpec) {
			c := dingo(s)
			c.Env = append(c.Env, corev1.EnvVar{
				Name: KESKeyEnv, Value: "/keys/kes.skey",
			})
		},
		"unprefixed KES key": func(s *corev1.PodSpec) {
			c := dingo(s)
			c.Env = append(c.Env, corev1.EnvVar{
				Name: "SHELLEY_KES_KEY", Value: "/keys/kes.skey",
			})
		},
		"envFrom": func(s *corev1.PodSpec) {
			dingo(s).EnvFrom = []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
				},
			}}
		},
		"command": func(s *corev1.PodSpec) { dingo(s).Command = []string{"dingo"} },
		"args":    func(s *corev1.PodSpec) { dingo(s).Args = []string{"--config", "x"} },
		"pool list not mounted": func(s *corev1.PodSpec) {
			c := dingo(s)
			var mounts []corev1.VolumeMount
			for _, m := range c.VolumeMounts {
				if m.Name != poolsConfigVolumeName {
					mounts = append(mounts, m)
				}
			}
			c.VolumeMounts = mounts
		},
		"pool list at another path": func(s *corev1.PodSpec) {
			c := dingo(s)
			for i := range c.VolumeMounts {
				if c.VolumeMounts[i].Name == poolsConfigVolumeName {
					c.VolumeMounts[i].MountPath = "/etc/dingo/other.yaml"
				}
			}
		},
		"pool list volume from a Secret": func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Name == poolsConfigVolumeName {
					s.Volumes[i].VolumeSource = corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: "x"},
					}
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			spec := rendered()
			mutate(&spec)
			assert.False(t, PodSignsThroughAgent(spec))
		})
	}
}
