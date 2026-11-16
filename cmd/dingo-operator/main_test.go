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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitNamespaces(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{in: "", want: nil},
		{in: " , ,", want: nil},
		{in: "keys", want: []string{"keys"}},
		{in: "a, b,a,,c ", want: []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, splitNamespaces(tt.in))
		})
	}
}

func TestOperatorNamespace(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "namespace")
	require.NoError(t, os.WriteFile(file, []byte("from-file\n"), 0o600))
	missing := filepath.Join(dir, "missing")

	assert.Equal(t, "from-env", operatorNamespace("from-env", file))
	assert.Equal(t, "from-env", operatorNamespace(" from-env ", file))
	assert.Equal(t, "from-file", operatorNamespace("", file))
	assert.Empty(t, operatorNamespace("", missing))
}
