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
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const testAgentPin = "0123456789abcdef0123456789abcdef" +
	"0123456789abcdef0123456789abcdef"

func agentNode(sa dingov1alpha1.SigningAgentSpec) *dingov1alpha1.DingoNode {
	dn := bpNode()
	dn.Spec.BlockProducer.SigningAgent = &sa
	return dn
}

func keysInitScript(t *testing.T, spec corev1.PodSpec) []string {
	t.Helper()
	for _, c := range spec.InitContainers {
		if c.Name == keysInitName {
			return c.Command
		}
	}
	t.Fatalf("no %s init container", keysInitName)
	return nil
}

func renderPod(
	dn *dingov1alpha1.DingoNode,
	opts RenderOptions,
) corev1.PodSpec {
	opts.Replicas = 1
	return BuildStatefulSet(dn, opts).Spec.Template.Spec
}

func TestBuildEnvSigningAgent(t *testing.T) {
	t.Parallel()

	t.Run("local KES key without an agent", func(t *testing.T) {
		t.Parallel()
		env := envMap(bpNode(), RenderOptions{MountKeys: true})
		assert.Equal(t, "/keys/kes.skey", env[KESKeyEnv])
		for _, name := range signingAgentManagedEnv[1:] {
			assert.NotContains(t, env, name)
		}
	})

	t.Run("agent address replaces the KES key", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:          "kes-agent.signing:7000",
				ServerSPKISHA256: testAgentPin,
			}),
			RenderOptions{MountKeys: true, MountVoteKey: true},
		)
		assert.NotContains(t, env, KESKeyEnv)
		assert.Equal(t, "kes-agent.signing:7000", env[signingAgentAddressEnv])
		assert.Equal(t, testAgentPin, env[signingAgentPinEnv])
		assert.NotContains(t, env, signingAgentPlaintextEnv)
		assert.NotContains(t, env, signingAgentCertEnv)
		assert.NotContains(t, env, signingAgentKeyEnv)
		// The rest of the forging setup is unchanged, vote key included.
		assert.Equal(t, "true", env["CARDANO_BLOCK_PRODUCER"])
		assert.Equal(t, "/keys/vrf.skey", env["CARDANO_SHELLEY_VRF_KEY"])
		assert.Equal(
			t, "/keys/opcert.cert",
			env["CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE"],
		)
		assert.Equal(t, "62", env["DINGO_MAX_KES_EVOLUTIONS"])
		assert.Equal(t, VoteKeyPath, env[VoteKeyEnv])
	})

	t.Run("plaintext", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:   "kes-agent:7000",
				Plaintext: true,
			}),
			RenderOptions{MountKeys: true},
		)
		assert.Equal(t, "true", env[signingAgentPlaintextEnv])
		assert.NotContains(t, env, signingAgentPinEnv)
	})

	t.Run("padded pin rendered trimmed", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:          "kes-agent:7000",
				ServerSPKISHA256: " " + testAgentPin + "\n",
			}),
			RenderOptions{MountKeys: true},
		)
		assert.Equal(t, testAgentPin, env[signingAgentPinEnv])
	})

	t.Run("blank pin renders no pin", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:          "kes-agent:7000",
				ServerSPKISHA256: "  ",
			}),
			RenderOptions{MountKeys: true},
		)
		assert.NotContains(t, env, signingAgentPinEnv)
	})

	t.Run("client certificate files", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:             "kes-agent:7000",
				ClientCertSecretRef: "agent-client",
			}),
			RenderOptions{MountKeys: true},
		)
		assert.Equal(t, signingAgentTLSPath+"/tls.crt", env[signingAgentCertEnv])
		assert.Equal(t, signingAgentTLSPath+"/tls.key", env[signingAgentKeyEnv])
	})

	t.Run("keyless pod gets no agent settings", func(t *testing.T) {
		t.Parallel()
		env := envMap(
			agentNode(dingov1alpha1.SigningAgentSpec{
				Address:             "kes-agent:7000",
				ClientCertSecretRef: "agent-client",
			}),
			RenderOptions{MountKeys: false},
		)
		for _, name := range signingAgentManagedEnv {
			assert.NotContains(t, env, name)
		}
	})
}

func TestSigningAgentCertVolume(t *testing.T) {
	t.Parallel()

	findVolume := func(spec corev1.PodSpec) *corev1.Volume {
		return findVolumeByName(spec, signingAgentTLSVolume)
	}
	findMount := func(spec corev1.PodSpec) *corev1.VolumeMount {
		c, ok := DingoContainer(spec)
		if !ok {
			return nil
		}
		for i := range c.VolumeMounts {
			if c.VolumeMounts[i].Name == signingAgentTLSVolume {
				return &c.VolumeMounts[i]
			}
		}
		return nil
	}

	withCert := agentNode(dingov1alpha1.SigningAgentSpec{
		Address:             "kes-agent:7000",
		ClientCertSecretRef: "agent-client",
	})
	spec := renderPod(withCert, RenderOptions{MountKeys: true})
	vol := findVolume(spec)
	require.NotNil(t, vol)
	require.NotNil(t, vol.Secret)
	assert.Equal(t, "agent-client", vol.Secret.SecretName)
	require.NotNil(t, vol.Secret.DefaultMode)
	assert.Equal(t, int32(0o440), *vol.Secret.DefaultMode)
	assert.Equal(
		t,
		[]corev1.KeyToPath{
			{Key: "tls.crt", Path: "tls.crt"},
			{Key: "tls.key", Path: "tls.key"},
		},
		vol.Secret.Items,
	)
	mount := findMount(spec)
	require.NotNil(t, mount)
	assert.True(t, mount.ReadOnly)
	assert.Equal(t, signingAgentTLSPath, mount.MountPath)

	// No certificate, or no keys on this pod: nothing is mounted.
	noCert := agentNode(dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"})
	spec = renderPod(noCert, RenderOptions{MountKeys: true})
	assert.Nil(t, findVolume(spec))
	assert.Nil(t, findMount(spec))
	spec = renderPod(withCert, RenderOptions{MountKeys: false})
	assert.Nil(t, findVolume(spec))
	assert.Nil(t, findMount(spec))
}

// A node without an agent renders exactly the template it did before the
// field existed, so an operator upgrade rolls no block producer.
func TestSigningAgentLeavesLocalKeyTemplateUnchanged(t *testing.T) {
	t.Parallel()
	pod := renderPod(bpNode(), RenderOptions{MountKeys: true})
	script := keysInitScript(t, pod)
	require.Len(t, script, 3)
	assert.Equal(t, prepareBlockProducerKeysScript, script[2])
	assert.Nil(t, findVolumeByName(pod, signingAgentTLSVolume))
}

func findVolumeByName(spec corev1.PodSpec, name string) *corev1.Volume {
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == name {
			return &spec.Volumes[i]
		}
	}
	return nil
}

// In agent mode the staging script never copies kes.skey, even when the Secret
// still holds one, and stages everything else as before.
func TestSigningAgentKeysScriptSkipsKESKey(t *testing.T) {
	t.Parallel()
	pod := renderPod(
		agentNode(dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"}),
		RenderOptions{MountKeys: true},
	)
	script := keysInitScript(t, pod)
	require.Len(t, script, 3)
	assert.Equal(t, prepareSigningAgentKeysScript, script[2])
	assert.NotContains(t, script[2], "kes.skey")

	for _, tc := range []struct {
		name  string
		files map[string]string
	}{
		{"without kes.skey", map[string]string{
			"vrf.skey": "vrf", "node.cert": "cert", "bls.skey": "vote",
		}},
		{"with a stray kes.skey", map[string]string{
			"vrf.skey": "vrf", "opcert.cert": "cert", "vote.skey": "vote",
			"kes.skey": "stray",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src, dst := t.TempDir(), t.TempDir()
			for name, body := range tc.files {
				require.NoError(
					t,
					os.WriteFile(filepath.Join(src, name), []byte(body), 0o600),
				)
			}
			cmd := exec.Command(script[0], script[1:]...)
			cmd.Env = append(
				os.Environ(),
				"DINGO_KEYS_SOURCE="+src,
				"DINGO_KEYS_DESTINATION="+dst,
			)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			assert.NoFileExists(t, filepath.Join(dst, "kes.skey"))
			for name, want := range map[string]string{
				"vrf.skey":    "vrf",
				"opcert.cert": "cert",
				"vote.skey":   "vote",
			} {
				got, err := os.ReadFile(filepath.Join(dst, name))
				require.NoError(t, err, name)
				assert.Equal(t, want, string(got), name)
				info, err := os.Stat(filepath.Join(dst, name))
				require.NoError(t, err)
				assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), name)
			}
		})
	}
}

func TestValidateSigningAgentEnv(t *testing.T) {
	t.Parallel()
	for _, name := range signingAgentManagedEnv {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dn := agentNode(dingov1alpha1.SigningAgentSpec{Address: "a:1"})
			dn.Spec.Environment = map[string]string{name: "x"}
			err := ValidateKeyEnv(dn)
			require.Error(t, err)
			assert.Contains(t, err.Error(), name)

			// Without an agent the operator does not claim these names.
			plain := bpNode()
			plain.Spec.Environment = map[string]string{name: "x"}
			require.NoError(t, ValidateKeyEnv(plain))
		})
	}
}
