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
	"slices"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const (
	poolA = "24cd8a8eda65e5badc22c56db90109442394544bfc8a279f9f63b8dc"
	poolB = "dec4e7ca394973004264e150053b486efdd62b3ad559eb85882d7f63"
	poolC = "ed55d0fbbce04af14236fe865234f792ca303f5245594fce1297aa97"
)

// singlePoolEnv names the settings Dingo refuses beside blockProducerPools.
var singlePoolEnv = []string{
	"CARDANO_SHELLEY_VRF_KEY",
	"CARDANO_SHELLEY_KES_KEY",
	"CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE",
	"CARDANO_SHELLEY_KES_AGENT_SOCKET",
	"CARDANO_SHELLEY_KES_AGENT_MODE",
	"CARDANO_SHELLEY_KES_AGENT_ADDRESS",
	"CARDANO_SHELLEY_KES_AGENT_ADDRESSES",
	"CARDANO_SHELLEY_KES_AGENT_CLIENT_CERT_FILE",
	"CARDANO_SHELLEY_KES_AGENT_CLIENT_KEY_FILE",
	"CARDANO_SHELLEY_KES_AGENT_SERVER_SPKI_SHA256",
	"CARDANO_SHELLEY_KES_AGENT_PLAINTEXT",
	"CARDANO_SHELLEY_KES_AGENT_PROFILE",
}

func poolListNode(sa dingov1alpha1.SigningAgentSpec) *dingov1alpha1.DingoNode {
	dn := bpNode()
	dn.Spec.Image.Tag = "0.94.0"
	bp := dn.Spec.BlockProducer
	bp.Keys = dingov1alpha1.KeysSpec{}
	bp.SigningAgent = &sa
	bp.Pools = []dingov1alpha1.BlockProducerPool{
		{
			PoolID:       poolB,
			AgentProfile: "pool-b",
			Keys:         dingov1alpha1.KeysSpec{SecretRef: "keys-b"},
		},
		{
			PoolID:       poolA,
			AgentProfile: "pool-a",
			Keys:         dingov1alpha1.KeysSpec{SecretRef: "keys-a"},
		},
		{
			PoolID:       poolC,
			AgentProfile: "pool-c",
			Keys:         dingov1alpha1.KeysSpec{SecretRef: "keys-c"},
		},
	}
	return dn
}

func poolListOpts() RenderOptions {
	return RenderOptions{
		MountKeys: true,
		Pools: []RenderPool{
			{PoolID: poolB, SecretName: "keys-b", AgentProfile: "pool-b"},
			{PoolID: poolA, SecretName: "keys-a", AgentProfile: "pool-a"},
			{PoolID: poolC, SecretName: "keys-c", AgentProfile: "pool-c"},
		},
	}
}

func TestPoolsConfigRendersExactlyTheListedPools(t *testing.T) {
	t.Parallel()
	dn := poolListNode(dingov1alpha1.SigningAgentSpec{
		Address:             "kes-agent.signing:7000",
		ServerSPKISHA256:    " " + testAgentPin + " ",
		ClientCertSecretRef: "agent-client",
	})
	// Pools render in pool ID order, whatever the order of the spec.
	want := `blockProducerPools:
  - shelleyVrfKey: "/keys/` + poolA + `/vrf.skey"
    shelleyOperationalCertificate: "/keys/` + poolA + `/opcert.cert"
    shelleyKesAgentAddress: "kes-agent.signing:7000"
    shelleyKesAgentServerSpkiSha256: "` + testAgentPin + `"
    shelleyKesAgentClientCertFile: "/var/run/dingo-signing-agent-tls/tls.crt"
    shelleyKesAgentClientKeyFile: "/var/run/dingo-signing-agent-tls/tls.key"
    shelleyKesAgentProfile: "pool-a"
  - shelleyVrfKey: "/keys/` + poolB + `/vrf.skey"
    shelleyOperationalCertificate: "/keys/` + poolB + `/opcert.cert"
    shelleyKesAgentAddress: "kes-agent.signing:7000"
    shelleyKesAgentServerSpkiSha256: "` + testAgentPin + `"
    shelleyKesAgentClientCertFile: "/var/run/dingo-signing-agent-tls/tls.crt"
    shelleyKesAgentClientKeyFile: "/var/run/dingo-signing-agent-tls/tls.key"
    shelleyKesAgentProfile: "pool-b"
  - shelleyVrfKey: "/keys/` + poolC + `/vrf.skey"
    shelleyOperationalCertificate: "/keys/` + poolC + `/opcert.cert"
    shelleyKesAgentAddress: "kes-agent.signing:7000"
    shelleyKesAgentServerSpkiSha256: "` + testAgentPin + `"
    shelleyKesAgentClientCertFile: "/var/run/dingo-signing-agent-tls/tls.crt"
    shelleyKesAgentClientKeyFile: "/var/run/dingo-signing-agent-tls/tls.key"
    shelleyKesAgentProfile: "pool-c"
`
	assert.Equal(t, want, PoolsConfig(dn, poolListOpts().Pools))

	cm := BuildPoolsConfigMap(dn, poolListOpts().Pools)
	assert.Equal(t, "bp-pools", cm.Name)
	assert.Equal(t, "cardano", cm.Namespace)
	assert.Equal(t, map[string]string{PoolsConfigKey: want}, cm.Data)
}

func TestPoolsConfigOmitsAPoolThatWasDropped(t *testing.T) {
	t.Parallel()
	dn := poolListNode(dingov1alpha1.SigningAgentSpec{
		Address:   "kes-agent:7000",
		Plaintext: true,
	})
	opts := poolListOpts()
	// Pool B's bundle failed validation: it is not in the render set.
	opts.Pools = slices.DeleteFunc(opts.Pools, func(p RenderPool) bool {
		return p.PoolID == poolB
	})
	want := `blockProducerPools:
  - shelleyVrfKey: "/keys/` + poolA + `/vrf.skey"
    shelleyOperationalCertificate: "/keys/` + poolA + `/opcert.cert"
    shelleyKesAgentAddress: "kes-agent:7000"
    shelleyKesAgentPlaintext: true
    shelleyKesAgentProfile: "pool-a"
  - shelleyVrfKey: "/keys/` + poolC + `/vrf.skey"
    shelleyOperationalCertificate: "/keys/` + poolC + `/opcert.cert"
    shelleyKesAgentAddress: "kes-agent:7000"
    shelleyKesAgentPlaintext: true
    shelleyKesAgentProfile: "pool-c"
`
	assert.Equal(t, want, PoolsConfig(dn, opts.Pools))
}

func TestBuildEnvPoolListRendersNoSinglePoolSetting(t *testing.T) {
	t.Parallel()
	dn := poolListNode(dingov1alpha1.SigningAgentSpec{
		Address:             "kes-agent.signing:7000",
		ServerSPKISHA256:    testAgentPin,
		ClientCertSecretRef: "agent-client",
	})
	env := envMap(dn, poolListOpts())
	for _, name := range singlePoolEnv {
		assert.NotContains(t, env, name)
		assert.NotContains(t, env, name[len("CARDANO_"):])
	}
	assert.NotContains(t, env, VoteKeyEnv)
	// Still per node: the producer switch and the credential watch that
	// reloads a pool's opcert.
	assert.Equal(t, "true", env["CARDANO_BLOCK_PRODUCER"])
	assert.Equal(t, keyWatchInterval, env[KeyWatchIntervalEnv])
	assert.Equal(t, "62", env["DINGO_MAX_KES_EVOLUTIONS"])
}

func TestBuildEnvSinglePoolStillRendersItsSettings(t *testing.T) {
	t.Parallel()
	env := envMap(bpNode(), RenderOptions{MountKeys: true})
	assert.Equal(t, "/keys/vrf.skey", env["CARDANO_SHELLEY_VRF_KEY"])
	assert.Equal(t, "/keys/kes.skey", env[KESKeyEnv])
	assert.Equal(
		t, "/keys/opcert.cert", env["CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE"],
	)
	assert.NotContains(t, env, KeyWatchIntervalEnv)
}

func TestPoolListVoteKeyOnlyForASinglePool(t *testing.T) {
	t.Parallel()
	dn := poolListNode(
		dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
	)
	opts := poolListOpts()
	opts.Pools[0].HasVote = true
	assert.NotContains(t, envMap(dn, opts), VoteKeyEnv,
		"Dingo refuses a vote key beside several pools")

	dn.Spec.BlockProducer.Pools = dn.Spec.BlockProducer.Pools[:1]
	opts.Pools = opts.Pools[:1]
	assert.Equal(
		t,
		PoolKeysDir(poolB)+"/vote.skey",
		envMap(dn, opts)[VoteKeyEnv],
	)
}

func mountByPath(c corev1.Container, path string) *corev1.VolumeMount {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].MountPath == path {
			return &c.VolumeMounts[i]
		}
	}
	return nil
}

func TestPoolListPodStagesEachPoolSeparately(t *testing.T) {
	t.Parallel()
	dn := poolListNode(dingov1alpha1.SigningAgentSpec{
		Address:             "kes-agent.signing:7000",
		ServerSPKISHA256:    testAgentPin,
		ClientCertSecretRef: "agent-client",
	})
	opts := poolListOpts()
	opts.KeysChecksum = "ck"
	opts.PoolsConfigChecksum = "pk"
	opts.KeysPool = poolA + "," + poolB + "," + poolC
	sts := BuildStatefulSet(
		dn,
		func() RenderOptions { opts.Replicas = 1; return opts }(),
	)
	pod := sts.Spec.Template.Spec

	// One Secret volume per pool, in pool ID order.
	secretOf := map[string]string{}
	for _, v := range pod.Volumes {
		if v.Secret != nil {
			secretOf[v.Name] = v.Secret.SecretName
		}
	}
	assert.Equal(t, "keys-a", secretOf["block-producer-keys-source-0"])
	assert.Equal(t, "keys-b", secretOf["block-producer-keys-source-1"])
	assert.Equal(t, "keys-c", secretOf["block-producer-keys-source-2"])
	assert.NotContains(t, secretOf, keysSourceName)
	assert.Equal(
		t, []string{"keys-a", "keys-b", "keys-c"}, KeysSecretNames(pod),
	)

	// The init container mounts each pool's Secret under its own directory,
	// and writes into the shared /keys volume.
	var initC, syncC, node *corev1.Container
	for i := range pod.InitContainers {
		if pod.InitContainers[i].Name == keysInitName {
			initC = &pod.InitContainers[i]
		}
	}
	for i := range pod.Containers {
		switch pod.Containers[i].Name {
		case keysSyncName:
			syncC = &pod.Containers[i]
		case containerName:
			node = &pod.Containers[i]
		}
	}
	require.NotNil(t, initC)
	assert.Nil(t, syncC, "a pool list takes a new opcert from the agent")
	require.NotNil(t, node)
	assert.Equal(t, preparePoolListKeysScript, initC.Command[2])
	for i, pool := range []string{poolA, poolB, poolC} {
		m := mountByPath(*initC, "/var/run/dingo-keys-source/"+pool)
		require.NotNil(t, m, "%s mounts pool %s", initC.Name, pool)
		assert.Equal(t, poolSourceVolume(i), m.Name)
		assert.True(t, m.ReadOnly)
	}
	assert.Nil(t, mountByPath(*initC, keysSourcePath))

	// Dingo reads the pool list from its default config path, by subPath.
	cfg := mountByPath(*node, "/etc/dingo/dingo.yaml")
	require.NotNil(t, cfg)
	assert.Equal(t, "dingo.yaml", cfg.SubPath)
	assert.True(t, cfg.ReadOnly)
	assert.Empty(t, node.Command)
	assert.Empty(t, node.Args)
	var cfgVol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == cfg.Name {
			cfgVol = &pod.Volumes[i]
		}
	}
	require.NotNil(t, cfgVol)
	assert.Equal(t, "bp-pools", cfgVol.ConfigMap.Name)
	require.NotNil(t, mountByPath(*node, "/keys"))
	require.NotNil(t, mountByPath(*node, "/var/run/dingo-signing-agent-tls"))

	ann := sts.Spec.Template.Annotations
	assert.Equal(t, "ck", ann[KeysChecksumAnnotation])
	assert.Equal(t, "pk", ann[PoolsConfigChecksumAnnotation])
	assert.Equal(t, poolA+","+poolB+","+poolC, ann[KeysPoolAnnotation])
	assert.NotContains(t, ann, KeysStaticChecksumAnnotation)
	assert.NotContains(t, ann, KeysCounterAnnotation)
}

func TestPoolListTemplateIgnoresSpecOrder(t *testing.T) {
	t.Parallel()
	dn := poolListNode(
		dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
	)
	opts := poolListOpts()
	opts.Replicas = 1
	first := BuildStatefulSet(dn, opts).Spec.Template

	slices.Reverse(opts.Pools)
	second := BuildStatefulSet(dn, opts).Spec.Template
	assert.Equal(t, first, second)
}

func TestPoolListWithNoUsablePoolMountsNoKeys(t *testing.T) {
	t.Parallel()
	dn := poolListNode(
		dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
	)
	opts := RenderOptions{MountKeys: true, Replicas: 1}
	pod := BuildStatefulSet(dn, opts).Spec.Template.Spec
	assert.Empty(t, KeysSecretNames(pod))
	env := envMap(dn, opts)
	assert.NotContains(t, env, "CARDANO_BLOCK_PRODUCER")
	for _, name := range singlePoolEnv {
		assert.NotContains(t, env, name)
	}
}

func TestSinglePoolPodKeepsItsSingleSecretVolume(t *testing.T) {
	t.Parallel()
	pod := renderPod(bpNode(), RenderOptions{MountKeys: true})
	assert.Equal(t, []string{"pool-keys"}, KeysSecretNames(pod))
	name, ok := KeysSecretName(pod)
	assert.True(t, ok)
	assert.Equal(t, "pool-keys", name)
}

func TestKeysPoolAnnotationRoundTrip(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{poolA}, SplitKeysPools(poolA))
	assert.Equal(
		t,
		[]string{poolA, poolB},
		SplitKeysPools(poolA+", "+poolB+","),
	)
	assert.Empty(t, SplitKeysPools(""))
	assert.Equal(
		t, poolA+","+poolB, JoinKeysPools([]string{poolB, poolA}),
	)
}

func TestValidateKeyEnvPoolList(t *testing.T) {
	t.Parallel()
	for _, name := range singlePoolEnv {
		for _, variant := range []string{name, name[len("CARDANO_"):]} {
			t.Run(variant, func(t *testing.T) {
				t.Parallel()
				dn := poolListNode(
					dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
				)
				dn.Spec.Environment = map[string]string{variant: "x"}
				err := ValidateKeyEnv(dn)
				require.Error(t, err)
				assert.Contains(t, err.Error(), variant)

				// The same override is not this rule's business on a node
				// without a pool list.
				single := bpNode()
				single.Spec.Environment = map[string]string{variant: "x"}
				assert.NoError(t, ValidateKeyEnv(single))
			})
		}
	}
	t.Run("vote key path", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode(
			dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
		)
		dn.Spec.Environment = map[string]string{VoteKeyEnv: VoteKeyPath}
		require.Error(t, ValidateKeyEnv(dn))
	})
	t.Run("per node settings stay allowed", func(t *testing.T) {
		t.Parallel()
		dn := poolListNode(
			dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"},
		)
		dn.Spec.Environment = map[string]string{
			"CARDANO_BLOCK_PRODUCER":                 "true",
			"CARDANO_SHELLEY_KES_AGENT_SIGN_TIMEOUT": "5s",
			KeyWatchIntervalEnv:                      "30s",
		}
		assert.NoError(t, ValidateKeyEnv(dn))
	})
}

func TestSupportsPoolList(t *testing.T) {
	t.Parallel()
	for tag, want := range map[string]bool{
		"0.85.0": false, "0.93.1": false, "0.94.0": true, "v0.94.0": true,
		"0.95.2": true, "1.0.0": true,
		// A tag whose version cannot be read is trusted.
		"latest": true,
	} {
		dn := bpNode()
		dn.Spec.Image.Tag = tag
		assert.Equal(t, want, SupportsPoolList(dn), tag)
	}
	custom := bpNode()
	custom.Spec.Image.Repository = "registry.example/dingo"
	custom.Spec.Image.Tag = "0.50.0"
	assert.True(t, SupportsPoolList(custom))
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
}

func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

func TestPreparePoolListKeysScript(t *testing.T) {
	t.Parallel()
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, poolA, "vrf.skey"), "vrf-a")
	writeFile(t, filepath.Join(src, poolA, "opcert.cert"), "cert-a")
	writeFile(t, filepath.Join(src, poolA, "kes.skey"), "kes-a")
	writeFile(t, filepath.Join(src, poolB, "vrf.skey"), "vrf-b")
	writeFile(t, filepath.Join(src, poolB, "node.cert"), "cert-b")
	writeFile(t, filepath.Join(src, poolB, "bls.skey"), "vote-b")

	cmd := exec.Command("/bin/sh", "-c", preparePoolListKeysScript)
	cmd.Env = append(
		os.Environ(),
		"DINGO_KEYS_SOURCE="+src,
		"DINGO_KEYS_DESTINATION="+dst,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	assert.Equal(t, "vrf-a", readFile(filepath.Join(dst, poolA, "vrf.skey")))
	assert.Equal(
		t,
		"cert-a",
		readFile(filepath.Join(dst, poolA, "opcert.cert")),
	)
	assert.Equal(t, "vrf-b", readFile(filepath.Join(dst, poolB, "vrf.skey")))
	assert.Equal(
		t,
		"cert-b",
		readFile(filepath.Join(dst, poolB, "opcert.cert")),
	)
	assert.Equal(t, "vote-b", readFile(filepath.Join(dst, poolB, "vote.skey")))
	// The agent holds the KES keys; none is copied.
	assert.NoFileExists(t, filepath.Join(dst, poolA, "kes.skey"))
	assert.NoFileExists(t, filepath.Join(dst, poolA, "vote.skey"))
	for _, f := range []string{
		filepath.Join(dst, poolA, "vrf.skey"),
		filepath.Join(dst, poolB, "opcert.cert"),
		filepath.Join(dst, poolB, "vote.skey"),
	} {
		info, err := os.Stat(f)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), f)
	}
}
