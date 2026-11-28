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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestReloadInPlaceRendering(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		enabled   bool
		mountKeys bool
		want      bool
	}{
		{name: "disabled", enabled: false, mountKeys: true, want: false},
		{name: "enabled", enabled: true, mountKeys: true, want: true},
		{name: "keyless pod", enabled: true, mountKeys: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := bpNode()
			dn.Spec.BlockProducer.Rotation.ReloadInPlace = tc.enabled
			opts := RenderOptions{
				Replicas:           1,
				MountKeys:          tc.mountKeys,
				KeysChecksum:       "full",
				KeysStaticChecksum: "static",
			}
			tmpl := BuildStatefulSet(dn, opts).Spec.Template

			var sync *corev1.Container
			for i := range tmpl.Spec.Containers {
				if tmpl.Spec.Containers[i].Name == keysSyncName {
					sync = &tmpl.Spec.Containers[i]
				}
			}
			var approval *corev1.Volume
			for i := range tmpl.Spec.Volumes {
				if tmpl.Spec.Volumes[i].Name == keysApprovalVolumeName {
					approval = &tmpl.Spec.Volumes[i]
				}
			}
			static, hasStatic := tmpl.Annotations[KeysStaticChecksumAnnotation]
			interval, hasInterval := envMap(dn, opts)[KeyWatchIntervalEnv]

			if !tc.want {
				assert.Nil(t, sync)
				assert.Nil(t, approval)
				assert.False(t, hasStatic)
				assert.False(t, hasInterval)
				return
			}
			require.NotNil(t, sync)
			require.NotNil(t, approval)
			require.NotNil(t, approval.ConfigMap)
			assert.Equal(t, "bp-keys-approval", approval.ConfigMap.Name)
			require.NotNil(t, approval.ConfigMap.Optional)
			assert.True(t, *approval.ConfigMap.Optional)
			assert.Equal(t, "static", static)
			assert.Equal(t, keyWatchInterval, interval)
			// The node keeps its key directory read-only; only the staging
			// container writes to it.
			mount := func(c corev1.Container) *corev1.VolumeMount {
				for i := range c.VolumeMounts {
					if c.VolumeMounts[i].Name == keysVolumeName {
						return &c.VolumeMounts[i]
					}
				}
				return nil
			}
			nodeMount := mount(tmpl.Spec.Containers[0])
			require.NotNil(t, nodeMount, "node container mounts the keys")
			assert.True(t, nodeMount.ReadOnly)
			syncMount := mount(*sync)
			require.NotNil(t, syncMount, "staging container mounts the keys")
			assert.False(t, syncMount.ReadOnly)
			// Requests equal limits, so the container never changes a
			// Guaranteed pod's QoS class.
			require.NotEmpty(t, sync.Resources.Limits)
			assert.Equal(t, sync.Resources.Limits, sync.Resources.Requests)
		})
	}
}

func keysDigest(kes, cert string) string {
	sum := sha256.Sum256([]byte(kes + cert))
	return hex.EncodeToString(sum[:])
}

// The staging container stages the KES key and opcert only when the Secret
// volume holds the approved pair, and presents node.cert as opcert.cert.
func TestKeysSyncScript(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		certFile string
		approved string
		wantKES  string
		wantCert string
	}{
		{
			name:     "approved opcert.cert",
			certFile: "opcert.cert",
			approved: keysDigest("kes-new", "cert-new"),
			wantKES:  "kes-new",
			wantCert: "cert-new",
		},
		{
			name:     "approved node.cert",
			certFile: "node.cert",
			approved: keysDigest("kes-new", "cert-new"),
			wantKES:  "kes-new",
			wantCert: "cert-new",
		},
		{
			name:     "not approved",
			certFile: "opcert.cert",
			approved: keysDigest("kes-other", "cert-other"),
			wantKES:  "kes-old",
			wantCert: "cert-old",
		},
		{
			name:     "nothing approved",
			certFile: "opcert.cert",
			wantKES:  "kes-old",
			wantCert: "cert-old",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src, dst, apr := t.TempDir(), t.TempDir(), t.TempDir()
			write := func(dir, name, body string) {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, name), []byte(body), 0o600,
				))
			}
			write(src, "kes.skey", "kes-new")
			write(src, tc.certFile, "cert-new")
			write(dst, "kes.skey", "kes-old")
			write(dst, "opcert.cert", "cert-old")
			if tc.approved != "" {
				write(apr, KeysApprovalKey, tc.approved)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cmd := exec.CommandContext(
				ctx, "/bin/sh", "-c", syncBlockProducerKeysScript,
			)
			cmd.Env = append(
				os.Environ(),
				"DINGO_KEYS_SOURCE="+src,
				"DINGO_KEYS_DESTINATION="+dst,
				"DINGO_KEYS_APPROVAL="+filepath.Join(apr, KeysApprovalKey),
				"DINGO_KEYS_SYNC_INTERVAL=0.05",
			)
			require.NoError(t, cmd.Start())
			// The script loops forever: stop it before waiting for it.
			t.Cleanup(func() {
				cancel()
				_ = cmd.Wait()
			})

			read := func(name string) string {
				b, err := os.ReadFile(filepath.Join(dst, name))
				if err != nil {
					return ""
				}
				return string(b)
			}
			if tc.wantKES == "kes-new" {
				require.Eventually(t, func() bool {
					return read("kes.skey") == tc.wantKES &&
						read("opcert.cert") == tc.wantCert
				}, 5*time.Second, 20*time.Millisecond)
				for _, name := range []string{"kes.skey", "opcert.cert"} {
					info, err := os.Stat(filepath.Join(dst, name))
					require.NoError(t, err)
					assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
				}
				assert.NoFileExists(t, filepath.Join(dst, "node.cert"))
				return
			}
			time.Sleep(500 * time.Millisecond)
			assert.Equal(t, tc.wantKES, read("kes.skey"))
			assert.Equal(t, tc.wantCert, read("opcert.cert"))
			// Control: the loop is still running and stages the pair once it
			// is approved, so the unchanged files above came from the gate,
			// not from a script that had stopped.
			write(apr, KeysApprovalKey, keysDigest("kes-new", "cert-new"))
			require.Eventually(t, func() bool {
				return read("kes.skey") == "kes-new" &&
					read("opcert.cert") == "cert-new"
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}

// The script runs as the container's PID 1, which ignores SIGTERM without a
// handler. It must exit on SIGTERM, promptly and with status 0, even while
// waiting out a long poll interval.
func TestKeysSyncScriptExitsOnSIGTERM(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("/bin/sh", "-c", syncBlockProducerKeysScript)
	cmd.Env = append(
		os.Environ(),
		"DINGO_KEYS_SOURCE="+t.TempDir(),
		"DINGO_KEYS_DESTINATION="+t.TempDir(),
		"DINGO_KEYS_APPROVAL="+filepath.Join(t.TempDir(), KeysApprovalKey),
		"DINGO_KEYS_SYNC_INTERVAL=60",
	)
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	// Let the shell install its trap and enter the wait.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	select {
	case err := <-done:
		require.NoError(t, err, "the script exits 0 on SIGTERM")
	case <-time.After(5 * time.Second):
		t.Fatal("the script did not exit within 5s of SIGTERM")
	}
}

// The keys counter is recorded only on a template that reloads keys in place.
func TestKeysCounterAnnotation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		enabled   bool
		mountKeys bool
		want      bool
	}{
		{name: "disabled", enabled: false, mountKeys: true},
		{name: "enabled", enabled: true, mountKeys: true, want: true},
		{name: "keyless pod", enabled: true, mountKeys: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := bpNode()
			dn.Spec.BlockProducer.Rotation.ReloadInPlace = tc.enabled
			tmpl := BuildStatefulSet(dn, RenderOptions{
				Replicas:     1,
				MountKeys:    tc.mountKeys,
				KeysChecksum: "full",
				KeysCounter:  "4",
			}).Spec.Template
			got, ok := tmpl.Annotations[KeysCounterAnnotation]
			if !tc.want {
				assert.False(t, ok)
				return
			}
			assert.Equal(t, "4", got)
		})
	}
}

func opCertDigest(cert string) string {
	sum := sha256.Sum256([]byte(cert))
	return hex.EncodeToString(sum[:])
}

// A pod that signs through a signing agent gets a new opcert from the agent,
// so it renders no staging container and no approval volume. Its pod template
// still records the key checksums and Dingo still watches its key files, so
// the node still loads a new opcert without a roll.
func TestAgentSignedKeysAreNotStaged(t *testing.T) {
	t.Parallel()
	agent := dingov1alpha1.SigningAgentSpec{Address: "kes-agent:7000"}
	for _, tc := range []struct {
		name       string
		node       func() *dingov1alpha1.DingoNode
		opts       RenderOptions
		wantStaged bool
	}{
		{
			name: "local KES key",
			node: func() *dingov1alpha1.DingoNode {
				dn := bpNode()
				dn.Spec.BlockProducer.Rotation.ReloadInPlace = true
				return dn
			},
			opts:       RenderOptions{MountKeys: true},
			wantStaged: true,
		},
		{
			name: "signing agent",
			node: func() *dingov1alpha1.DingoNode {
				dn := agentNode(agent)
				dn.Spec.BlockProducer.Rotation.ReloadInPlace = true
				return dn
			},
			opts: RenderOptions{MountKeys: true},
		},
		{
			name: "signing agent with a pool list",
			node: func() *dingov1alpha1.DingoNode {
				return poolListNode(agent)
			},
			opts: poolListOpts(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := tc.node()
			opts := tc.opts
			opts.Replicas = 1
			opts.KeysChecksum = "full"
			opts.KeysStaticChecksum = "static"
			opts.KeysCounter = "4"
			tmpl := BuildStatefulSet(dn, opts).Spec.Template

			var hasSync, hasApproval, hasApprovalMount bool
			for _, c := range tmpl.Spec.Containers {
				hasSync = hasSync || c.Name == keysSyncName
				for _, m := range c.VolumeMounts {
					hasApprovalMount = hasApprovalMount ||
						m.Name == keysApprovalVolumeName
				}
			}
			for _, v := range tmpl.Spec.Volumes {
				hasApproval = hasApproval || v.Name == keysApprovalVolumeName
			}
			assert.Equal(t, tc.wantStaged, hasApprovalMount, "approval mount")
			assert.Equal(t, tc.wantStaged, hasSync, "staging container")
			assert.Equal(t, tc.wantStaged, hasApproval, "approval volume")

			// The checksums the no-roll decision reads back and the key file
			// watch stay.
			ann := tmpl.Annotations
			assert.Equal(t, "static", ann[KeysStaticChecksumAnnotation])
			assert.Equal(t, "4", ann[KeysCounterAnnotation])
			assert.Equal(
				t, keyWatchInterval, envMap(dn, opts)[KeyWatchIntervalEnv],
			)
		})
	}
}
