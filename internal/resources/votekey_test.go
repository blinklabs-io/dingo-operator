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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const voteKeyEnv = VoteKeyEnv

func TestBuildEnvLeiosVoteKey(t *testing.T) {
	t.Parallel()

	t.Run("set when the vote key is staged", func(t *testing.T) {
		t.Parallel()
		env := envMap(bpNode(), RenderOptions{
			MountKeys:    true,
			MountVoteKey: true,
		})
		assert.Equal(t, "/keys/vote.skey", env[voteKeyEnv])
	})

	t.Run("absent by default", func(t *testing.T) {
		t.Parallel()
		env := envMap(bpNode(), RenderOptions{MountKeys: true})
		assert.NotContains(t, env, voteKeyEnv)
	})

	t.Run("absent on a keyless pod", func(t *testing.T) {
		t.Parallel()
		env := envMap(bpNode(), RenderOptions{MountVoteKey: true})
		assert.NotContains(t, env, voteKeyEnv)
	})

	t.Run("absent on a relay", func(t *testing.T) {
		t.Parallel()
		env := envMap(relayNode(), RenderOptions{
			MountKeys:    true,
			MountVoteKey: true,
		})
		assert.NotContains(t, env, voteKeyEnv)
	})
}

func TestValidateLeiosVoteKeyEnv(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		env     func() map[string]string
		wantErr bool
	}{
		{"unset", func() map[string]string { return nil }, false},
		{"managed path restated", func() map[string]string {
			return map[string]string{voteKeyEnv: "/keys/vote.skey"}
		}, false},
		{"other path", func() map[string]string {
			return map[string]string{voteKeyEnv: "/elsewhere/vote.skey"}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dn := bpNode()
			dn.Spec.Environment = tc.env()
			err := ValidateKeyEnv(dn)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), voteKeyEnv)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestPrepareKeysScriptStagesOptionalVoteKey(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, names ...string) string {
		t.Helper()
		src := t.TempDir()
		dst := t.TempDir()
		for _, name := range names {
			path := filepath.Join(src, name)
			require.NoError(
				t,
				os.WriteFile(path, []byte("secret-"+name), 0o600),
			)
			require.NoError(t, os.Chmod(path, 0o640))
		}
		cmd := exec.Command("/bin/sh", "-c", prepareBlockProducerKeysScript)
		cmd.Env = append(
			os.Environ(),
			"DINGO_KEYS_SOURCE="+src,
			"DINGO_KEYS_DESTINATION="+dst,
		)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return dst
	}
	base := []string{"vrf.skey", "kes.skey", "opcert.cert"}

	t.Run("copies vote.skey owner-only when present", func(t *testing.T) {
		t.Parallel()
		dst := run(t, append(base, "vote.skey")...)
		path := filepath.Join(dst, "vote.skey")
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "secret-vote.skey", string(got))
		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("succeeds without vote.skey", func(t *testing.T) {
		t.Parallel()
		dst := run(t, base...)
		assert.NoFileExists(t, filepath.Join(dst, "vote.skey"))
		assert.FileExists(t, filepath.Join(dst, "vrf.skey"))
	})
}
