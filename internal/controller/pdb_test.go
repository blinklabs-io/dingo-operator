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

package controller

import (
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// TestBlockProducerPDBDisabled turns the block producer's budget off. The
// budget the operator created is deleted, so a drain can evict the pod, and
// turning it back on recreates it.
func TestBlockProducerPDBDisabled(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "pdb-ns")

	dn := blockProducerNode("bp", "pdb-ns", "pool-keys", "")
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "pdb-ns")
	key := types.NamespacedName{Name: "bp", Namespace: "pdb-ns"}
	require.NoError(t, c.Get(ctx, key, &policyv1.PodDisruptionBudget{}),
		"the budget is created by default")

	setPDB := func(enabled bool) {
		t.Helper()
		got := &dingov1alpha1.DingoNode{}
		require.NoError(t, c.Get(ctx, key, got))
		got.Spec.BlockProducer.PodDisruptionBudget.Enabled = ptr.To(enabled)
		require.NoError(t, c.Update(ctx, got))
		reconcile(t, ctx, reconcilerFor(c), "bp", "pdb-ns")
	}

	setPDB(false)
	err := c.Get(ctx, key, &policyv1.PodDisruptionBudget{})
	assert.True(t, apierrors.IsNotFound(err),
		"a disabled budget must be deleted, got %v", err)
	// Reconciling again with nothing to delete must not fail.
	reconcile(t, ctx, reconcilerFor(c), "bp", "pdb-ns")

	setPDB(true)
	assert.NoError(t, c.Get(ctx, key, &policyv1.PodDisruptionBudget{}))
}
