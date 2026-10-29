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
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// previousKeysInitScript is the key init-container script an earlier operator
// rendered: it copied opcert.cert only and knew nothing of node.cert.
const previousKeysInitScript = `set -eu
source_dir="${DINGO_KEYS_SOURCE:-/var/run/dingo-keys-source}"
destination_dir="${DINGO_KEYS_DESTINATION:-/keys}"
umask 077
for name in vrf.skey kes.skey opcert.cert; do
  cp "${source_dir}/${name}" "${destination_dir}/${name}"
done
chmod 0600 \
  "${destination_dir}/vrf.skey" \
  "${destination_dir}/kes.skey" \
  "${destination_dir}/opcert.cert"
`

// keysInitCommand returns the key init container's script from a pod template.
func keysInitCommand(t *testing.T, tmpl corev1.PodTemplateSpec) string {
	t.Helper()
	for _, c := range tmpl.Spec.InitContainers {
		if c.Name == "prepare-block-producer-keys" {
			require.Len(t, c.Command, 3)
			return c.Command[2]
		}
	}
	t.Fatal("no prepare-block-producer-keys init container")
	return ""
}

// TestBlockProducerRejectedKeysKeepUpgradedTemplate upgrades a producer whose
// StatefulSet an earlier operator rendered while its keys Secret fails
// validation. The live pod template must survive unchanged: rendering the new
// template would roll the healthy pod, and the replacement would mount the
// rejected Secret. Once the Secret validates, the new template is applied.
func TestBlockProducerRejectedKeysKeepUpgradedTemplate(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "upgrade-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	good, err := keys.SecretData(2, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", "upgrade-ns"),
		Data:       good,
	}))

	dn := blockProducerNode(
		"bp", "upgrade-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "upgrade-ns")
	accepted := keysAnnotation(t, ctx, c, "bp", "upgrade-ns")
	require.NotEmpty(t, accepted)

	// Rewrite the live template into the earlier operator's rendering. For a
	// producer with default relay port and no podLabels, the key init script
	// is the only part of the template that differs.
	key := types.NamespacedName{Name: "bp", Namespace: "upgrade-ns"}
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, key, sts))
	found := false
	for i := range sts.Spec.Template.Spec.InitContainers {
		ic := &sts.Spec.Template.Spec.InitContainers[i]
		if ic.Name == "prepare-block-producer-keys" {
			ic.Command[2] = previousKeysInitScript
			found = true
		}
	}
	require.True(t, found, "keys init container not rendered")
	require.NoError(t, c.Update(ctx, sts))
	want := podTemplate(t, ctx, c, "bp", "upgrade-ns")
	require.Equal(t, previousKeysInitScript, keysInitCommand(t, want))

	// Deliver a certificate issued by another pool's cold key.
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	foreign, err := other.SecretData(3, 0)
	require.NoError(t, err)
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "upgrade-ns"},
		secret,
	))
	secret.Data["opcert.cert"] = foreign["opcert.cert"]
	require.NoError(t, c.Update(ctx, secret))

	reconcile(t, ctx, reconcilerFor(c), "bp", "upgrade-ns")
	assert.Equal(
		t,
		want,
		podTemplate(t, ctx, c, "bp", "upgrade-ns"),
		"a rejected bundle must leave the earlier operator's template live",
	)
	// The withheld template change must be visible in status.
	keysValid, degraded := keysConditions(t, ctx, c, "bp", "upgrade-ns")
	require.NotNil(t, keysValid)
	assert.Contains(t, keysValid.Message, templateHeldMessage)
	require.NotNil(t, degraded)
	assert.Contains(t, degraded.Message, templateHeldMessage)

	// A valid replacement bundle applies the current template.
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "upgrade-ns"},
		secret,
	))
	require.NoError(t, keys.RotateKES(rand.Reader))
	fixed, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	secret.Data = fixed
	require.NoError(t, c.Update(ctx, secret))

	reconcile(t, ctx, reconcilerFor(c), "bp", "upgrade-ns")
	got := podTemplate(t, ctx, c, "bp", "upgrade-ns")
	assert.NotEqual(t, previousKeysInitScript, keysInitCommand(t, got))
	assert.NotEqual(
		t,
		accepted,
		keysAnnotation(t, ctx, c, "bp", "upgrade-ns"),
	)
}

// TestBlockProducerMissingKeysKeepLiveTemplate deletes the keys Secret of a
// producer whose StatefulSet already exists. The live pod template, checksum
// annotation included, must stay unchanged: rendering without the checksum
// would roll the healthy forging pod, and the replacement could not start
// without the Secret. Once a valid Secret returns, reconcile proceeds normally.
func TestBlockProducerMissingKeysKeepLiveTemplate(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "missing-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	good, err := keys.SecretData(2, 0)
	require.NoError(t, err)
	secret := &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", "missing-ns"),
		Data:       good,
	}
	require.NoError(t, c.Create(ctx, secret))

	dn := blockProducerNode(
		"bp", "missing-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "missing-ns")
	accepted := keysAnnotation(t, ctx, c, "bp", "missing-ns")
	require.NotEmpty(t, accepted)
	want := podTemplate(t, ctx, c, "bp", "missing-ns")

	require.NoError(t, c.Delete(ctx, secret))
	recorder := events.NewFakeRecorder(10)
	r := reconcilerFor(c)
	r.Recorder = recorder
	reconcile(t, ctx, r, "bp", "missing-ns")
	assert.Equal(
		t,
		want,
		podTemplate(t, ctx, c, "bp", "missing-ns"),
		"a missing keys Secret must leave the live template unchanged",
	)
	// The live template already matches the render, so nothing is held.
	keysValid, _ := keysConditions(t, ctx, c, "bp", "missing-ns")
	require.NotNil(t, keysValid)
	assert.NotContains(t, keysValid.Message, templateHeldMessage)
	assert.Equal(
		t,
		accepted,
		keysAnnotation(t, ctx, c, "bp", "missing-ns"),
		"the live keys checksum must be carried forward",
	)
	// The pod keeps running, so the missing Secret must be visible in status.
	var degraded *metav1.Condition
	keysValid, degraded = keysConditions(t, ctx, c, "bp", "missing-ns")
	require.NotNil(t, keysValid)
	assert.Equal(t, metav1.ConditionFalse, keysValid.Status)
	assert.Equal(t, "KeysSecretMissing", keysValid.Reason)
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, "KeysSecretMissing", degraded.Reason)
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "KeysSecretMissing")
	default:
		t.Error("no Event was recorded for the missing keys Secret")
	}

	// A valid Secret returning applies the rotated bundle's checksum.
	require.NoError(t, keys.RotateKES(rand.Reader))
	fixed, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", "missing-ns"),
		Data:       fixed,
	}))
	reconcile(t, ctx, reconcilerFor(c), "bp", "missing-ns")
	now := keysAnnotation(t, ctx, c, "bp", "missing-ns")
	assert.NotEmpty(t, now)
	assert.NotEqual(t, accepted, now)
	keysValid, degraded = keysConditions(t, ctx, c, "bp", "missing-ns")
	require.NotNil(t, keysValid)
	assert.Equal(t, metav1.ConditionTrue, keysValid.Status)
	assert.Nil(t, degraded, "Degraded must clear once the Secret validates")
}

// TestBlockProducerMissingKeysFirstRender reconciles a producer whose keys
// Secret does not exist yet and no StatefulSet exists. The StatefulSet is
// created without a keys checksum, and the Secret arriving later sets one.
func TestBlockProducerMissingKeysFirstRender(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "first-missing-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	dn := blockProducerNode(
		"bp", "first-missing-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "first-missing-ns")
	assert.Empty(t, keysAnnotation(t, ctx, c, "bp", "first-missing-ns"))

	data, err := keys.SecretData(2, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: objectMeta("pool-keys", "first-missing-ns"),
		Data:       data,
	}))
	reconcile(t, ctx, reconcilerFor(c), "bp", "first-missing-ns")
	assert.NotEmpty(t, keysAnnotation(t, ctx, c, "bp", "first-missing-ns"))
}

// keysConditions returns the node's KeysValid and Degraded conditions.
func keysConditions(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) (*metav1.Condition, *metav1.Condition) {
	t.Helper()
	dn := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: ns},
		dn,
	))
	return meta.FindStatusCondition(dn.Status.Conditions, condKeysValid),
		meta.FindStatusCondition(dn.Status.Conditions, condDegraded)
}
