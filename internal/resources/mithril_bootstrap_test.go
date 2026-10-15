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

// mithrilScript extracts the rendered bootstrap script from the init container
// so the test exercises the shell the pod actually runs, not a copy of it.
func mithrilScript(t *testing.T) string {
	t.Helper()
	containers := initContainers(relayNode(), RenderOptions{})
	for _, c := range containers {
		if c.Name == mithrilInitName {
			require.Len(t, c.Command, 3)
			return c.Command[2]
		}
	}
	t.Fatalf("no %s init container rendered", mithrilInitName)
	return ""
}

// runBootstrap executes the script against a temporary data dir with a stub
// dingo on PATH. syncFails makes `dingo mithril sync` exit non-zero, standing
// in for an aggregator outage or a killed pod mid-restore.
func runBootstrap(
	t *testing.T,
	dataDir string,
	forceResync bool,
	syncFails bool,
) (string, error) {
	t.Helper()
	binDir := t.TempDir()
	// The stub writes the database file before succeeding or failing, which is
	// exactly the condition the old existence check could not tell apart.
	body := "#!/bin/sh\ntouch \"$CARDANO_DATABASE_PATH/metadata.sqlite\"\n"
	if syncFails {
		body += "echo 'aggregator unreachable' >&2\nexit 1\n"
	}
	require.NoError(
		t,
		os.WriteFile(filepath.Join(binDir, "dingo"), []byte(body), 0o755),
	)

	cmd := exec.Command("/bin/sh", "-c", mithrilScript(t))
	cmd.Env = append(os.Environ(),
		// binDir first so the stub shadows any real dingo, with the system
		// paths kept for touch, rm and echo.
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"CARDANO_DATABASE_PATH="+dataDir,
		"FORCE_RESYNC="+map[bool]string{true: "true", false: "false"}[forceResync],
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

// A bootstrap interrupted after metadata.sqlite appears must not be mistaken
// for a finished one. The previous script keyed completion off that file and
// handed the node a partial database on the next start.
func TestMithrilBootstrapResumesAfterInterruption(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	out, err := runBootstrap(t, dataDir, false, true)
	require.Error(t, err, "failed sync must fail the init container: %s", out)
	require.True(
		t,
		exists(t, filepath.Join(dataDir, "metadata.sqlite")),
		"stub should have left a partial database",
	)
	assert.False(
		t,
		exists(t, filepath.Join(dataDir, ".mithril-bootstrap-complete")),
		"a failed sync must not be marked complete",
	)

	out, err = runBootstrap(t, dataDir, false, false)
	require.NoError(t, err, out)
	assert.Contains(t, out, "resuming")
	assert.True(t, exists(t, filepath.Join(dataDir, ".mithril-bootstrap-complete")))
}

// A completed bootstrap must not re-run. Catch-up against a complete database
// is core-storage-mode only, so an unconditional re-run would fail in api mode.
func TestMithrilBootstrapSkipsWhenComplete(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	out, err := runBootstrap(t, dataDir, false, false)
	require.NoError(t, err, out)

	// A stub that would fail proves the second run never invokes dingo.
	out, err = runBootstrap(t, dataDir, false, true)
	require.NoError(t, err, out)
	assert.Contains(t, out, "already complete")
}

// A volume from an operator version predating the markers carries a database
// and no markers. It must be adopted, not re-synced.
func TestMithrilBootstrapAdoptsPreExistingDatabase(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(
		t,
		os.WriteFile(filepath.Join(dataDir, "metadata.sqlite"), []byte("x"), 0o600),
	)

	out, err := runBootstrap(t, dataDir, false, true)
	require.NoError(t, err, out)
	assert.Contains(t, out, "adopting as complete")
	assert.True(t, exists(t, filepath.Join(dataDir, ".mithril-bootstrap-complete")))
}

func TestMithrilBootstrapForceResyncWipesAndRebootstraps(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	out, err := runBootstrap(t, dataDir, false, false)
	require.NoError(t, err, out)
	stale := filepath.Join(dataDir, "stale-file")
	require.NoError(t, os.WriteFile(stale, []byte("x"), 0o600))

	out, err = runBootstrap(t, dataDir, true, false)
	require.NoError(t, err, out)
	assert.Contains(t, out, "force resync requested")
	assert.Contains(t, out, "mithril bootstrap complete", "force resync must invoke sync")
	assert.False(t, exists(t, stale), "force resync must clear the data dir")
	assert.True(t, exists(t, filepath.Join(dataDir, "metadata.sqlite")), "sync must recreate the database")
	assert.True(t, exists(t, filepath.Join(dataDir, ".mithril-bootstrap-complete")))
}
