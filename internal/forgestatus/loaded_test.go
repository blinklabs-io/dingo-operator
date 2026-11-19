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

package forgestatus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Dingo names the unlabelled loaded counter dingo_forge_opcert_counter_loaded,
// the series of a node that forges for one pool.
func TestParseUnlabelledLoadedCounter(t *testing.T) {
	t.Parallel()
	st, err := Parse(strings.NewReader(
		"dingo_forge_opcert_counter_loaded 6\n",
	))
	require.NoError(t, err)
	assert.True(t, st.HasOpCertCounter)
	assert.Equal(t, int64(6), st.OpCertCounter)
	assert.Nil(t, st.Pools)

	st, err = Parse(strings.NewReader(
		"dingo_forge_opcert_counter_loaded NaN\n",
	))
	require.NoError(t, err)
	assert.False(t, st.HasOpCertCounter,
		"NaN means no opcert is loaded")
	assert.True(t, st.OpCertUnloaded)
}
