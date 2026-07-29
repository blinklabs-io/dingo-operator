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

package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestFinishNamespaceRetention pins the namespace-retention matrix. Getting it
// backwards is silent in both directions and expensive in both: retaining on a
// pass leaks a namespace per test until the cluster is torn down, while
// deleting on a failure destroys the pod logs and events that
// hack/e2e/collect-diagnostics.sh (and CI's diagnostics artifact) exist to
// capture — and the tests that would notice are the ones already failing.
//
// finishNamespace deliberately sits in an untagged file so this test needs
// neither the "e2e" build tag nor a cluster: it runs in `make test` in
// milliseconds, rather than only under the ~15-minute `make e2e`.
func TestFinishNamespaceRetention(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	newNamespace := func() *corev1.Namespace {
		return &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-deadbeef"},
		}
	}

	tests := []struct {
		name           string
		keepUp, failed bool
		wantReason     string
	}{
		{
			name:       "a passing test's namespace is deleted",
			wantReason: "",
		},
		{
			name:       "a failing test's namespace is kept for diagnostics",
			failed:     true,
			wantReason: "test failed",
		},
		{
			name:       "E2E_KEEP_UP keeps a passing test's namespace",
			keepUp:     true,
			wantReason: "E2E_KEEP_UP=1",
		},
		{
			name:       "E2E_KEEP_UP keeps a failing test's namespace",
			keepUp:     true,
			failed:     true,
			wantReason: "E2E_KEEP_UP=1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := newNamespace()
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(ns).
				Build()

			reason, err := finishNamespace(
				t.Context(), c, ns, tt.keepUp, tt.failed)
			require.NoError(t, err)
			assert.Equal(t, tt.wantReason, reason)

			// The reason string is only a log line; what matters is whether
			// the namespace is still there afterwards.
			err = c.Get(t.Context(), client.ObjectKeyFromObject(ns),
				&corev1.Namespace{})
			if tt.wantReason == "" {
				assert.True(t, apierrors.IsNotFound(err),
					"namespace should have been deleted, got %v", err)
			} else {
				assert.NoError(t, err, "namespace should have been retained")
			}
		})
	}

	t.Run(
		"a namespace that is already gone is not an error",
		func(t *testing.T) {
			// Cleanup can race a cluster teardown, and a NotFound there has
			// achieved exactly what was wanted.
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			reason, err := finishNamespace(
				t.Context(), c, newNamespace(), false, false)
			require.NoError(t, err)
			assert.Empty(t, reason)
		},
	)
}
