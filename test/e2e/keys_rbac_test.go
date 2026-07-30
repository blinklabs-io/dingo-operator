//go:build e2e

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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestKeysReaderRBACIsNamespaceScoped proves the operator's Secret access is
// genuinely confined to the namespaces it was granted, rather than the suite
// merely happening to work because something broader was installed.
//
// This is the property that makes the Helm chart's rbac.keySecretsNamespaces
// option meaningful: an operator that wants least privilege needs the narrow
// grant to be both sufficient (proved by every other test in this suite, which
// runs with no cluster-wide grant at all) and actually restrictive (proved
// here).
//
// The answers come from the API server's own authorizer via
// SubjectAccessReview, not from reading the manifests back.
func TestKeysReaderRBACIsNamespaceScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	h := newHarness(t)

	// Granted: newHarness created a Role+RoleBinding for this namespace.
	assert.True(t, h.canGetSecrets(ctx, h.namespace),
		"operator must be able to read Secrets in the namespace it was granted")

	// Ungranted: a second namespace with no keys-reader Role. If a cluster-wide
	// grant crept back into the manifests, this is the assertion that fails.
	other := h.namespace + "-ungranted"
	otherNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: other}}
	require.NoError(t, h.client.Create(ctx, otherNS), "create %s", other)
	t.Cleanup(func() {
		delCtx, delCancel := context.WithTimeout(
			context.WithoutCancel(ctx), time.Minute)
		defer delCancel()
		if err := h.client.Delete(delCtx, otherNS); err != nil {
			t.Logf("delete namespace %s: %v", other, err)
		}
	})

	assert.False(t, h.canGetSecrets(ctx, other),
		"operator must NOT be able to read Secrets in a namespace it was not "+
			"granted; a cluster-wide keys-reader grant would make the chart's "+
			"rbac.keySecretsNamespaces option meaningless")

	// Guard against the review itself being vacuous: the operator must still
	// hold a permission it is genuinely granted cluster-wide, so a blanket
	// "denied" answer (misconfigured SA name, authorizer not consulted) cannot
	// masquerade as correct scoping.
	require.True(t, h.canGetDingoNodes(ctx, other),
		"sanity: the operator's cluster-wide DingoNode access must still be "+
			"visible to SubjectAccessReview in any namespace")
}
