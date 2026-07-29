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
	"os"
	"path/filepath"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestMain silences controller-runtime's logging. Without a logger set,
// controller-runtime's delegating sink prints "log.SetLogger(...) was never
// called" plus a stack trace the first time anything logs more than 30 seconds
// into the run. Both envtest's own Start() and the reconciler's key-rejection
// path trip it, which is noise in an otherwise clean test output.
func TestMain(m *testing.M) {
	ctrl.SetLogger(logr.Discard())
	os.Exit(m.Run())
}

// startEnv brings up an envtest control plane with the DingoNode CRD installed.
// It skips the test when KUBEBUILDER_ASSETS is not configured (e.g. a plain
// `go test ./...` without `make test`).
func startEnv(t *testing.T) (client.Client, context.Context) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test`")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	return c, context.Background()
}

func reconcilerFor(c client.Client) *DingoNodeReconciler {
	return &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
}

func createNamespace(t *testing.T, ctx context.Context, c client.Client, name string) {
	t.Helper()
	require.NoError(t, c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}))
}

func reconcile(t *testing.T, ctx context.Context, r *DingoNodeReconciler, name, ns string) {
	t.Helper()
	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
	})
	require.NoError(t, err)
}

func TestReconcileRelay(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "relay-ns")

	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "relay", Namespace: "relay-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:     dingov1alpha1.RoleRelay,
			Network:  "preview",
			Replicas: new(int32(2)),
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "relay", "relay-ns")

	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "relay", Namespace: "relay-ns"}, sts))
	require.NotNil(t, sts.Spec.Replicas)
	assert.Equal(t, int32(2), *sts.Spec.Replicas)
	require.Len(t, sts.OwnerReferences, 1)
	assert.Equal(t, "relay", sts.OwnerReferences[0].Name)
	assert.True(t, ptr.Deref(sts.OwnerReferences[0].Controller, false))

	// Services and ServiceAccount exist.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "relay-headless", Namespace: "relay-ns"}, &corev1.Service{}))
	svc := &corev1.Service{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "relay", Namespace: "relay-ns"}, svc))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "relay", Namespace: "relay-ns"}, &corev1.ServiceAccount{}))
	wantIPFamilies := svc.Spec.IPFamilies
	wantIPFamilyPolicy := svc.Spec.IPFamilyPolicy
	reconcile(t, ctx, reconcilerFor(c), "relay", "relay-ns")
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "relay", Namespace: "relay-ns"}, svc))
	assert.Equal(t, wantIPFamilies, svc.Spec.IPFamilies)
	assert.Equal(t, wantIPFamilyPolicy, svc.Spec.IPFamilyPolicy)

	// A relay must NOT get a block-producer PDB or NetworkPolicy.
	err := c.Get(ctx, types.NamespacedName{Name: "relay", Namespace: "relay-ns"}, &policyv1.PodDisruptionBudget{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestReconcileBlockProducer(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "bp-ns")

	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "bp-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "preview",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				SlotsPerKESPeriod: 129600,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: "pool-keys"},
				Rotation:          dingov1alpha1.RotationSpec{Mode: dingov1alpha1.RotationModeMonitorOnly},
			},
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "bp-ns")

	// Block producer gets a StatefulSet that mounts the keys secret.
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "bp", Namespace: "bp-ns"}, sts))
	require.NotNil(t, sts.Spec.Replicas)
	assert.Equal(t, int32(1), *sts.Spec.Replicas)
	var mountsKeys bool
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == "pool-keys" {
			mountsKeys = true
		}
	}
	assert.True(t, mountsKeys, "block producer should mount the keys secret")

	// Block producer gets a PDB and a NetworkPolicy.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "bp", Namespace: "bp-ns"}, &policyv1.PodDisruptionBudget{}))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "bp", Namespace: "bp-ns"}, &networkingv1.NetworkPolicy{}))
}

// blockProducerNode returns a MonitorOnly block producer bound to the given
// pool and keys Secret.
func blockProducerNode(
	name, ns, secretRef, poolID string,
) *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "preview",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				PoolID:            poolID,
				SlotsPerKESPeriod: 129600,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: secretRef},
				Rotation: dingov1alpha1.RotationSpec{
					Mode: dingov1alpha1.RotationModeMonitorOnly,
				},
			},
		},
	}
}

// keysAnnotation returns the pod-template keys-checksum annotation currently on
// the node's StatefulSet.
func keysAnnotation(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) string {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: ns},
		sts,
	))
	return sts.Spec.Template.Annotations[resources.KeysChecksumAnnotation]
}

// podTemplate returns the node StatefulSet's pod template, so a test can assert
// that a reconcile left it byte-identical (any change rolls the pod).
func podTemplate(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	name, ns string,
) corev1.PodTemplateSpec {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: ns},
		sts,
	))
	return sts.Spec.Template
}

func TestBlockProducerKeysChecksumRollout(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "keys-ns")

	// Real generated key material: the operator now validates the bundle
	// before it will roll the pod onto it, so placeholder bytes would (and
	// should) be refused.
	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(0, 0)
	require.NoError(t, err)

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "keys-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "keys-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "keys-ns")

	first := keysAnnotation(t, ctx, c, "bp", "keys-ns")
	assert.NotEmpty(
		t,
		first,
		"keys-checksum annotation should be set from the Secret",
	)

	// A validated bundle also publishes its counter, which is what the
	// assisted-rotation status surface reports.
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "keys-ns"},
		got,
	))
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
		"KeysValid should be True",
	)

	// An assisted rotation (fresh KES key, counter+1) must change the checksum
	// so the pod rolls.
	require.NoError(t, keys.RotateKES(rand.Reader))
	rotated, err := keys.SecretData(1, 0)
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "keys-ns"},
		secret,
	))
	secret.Data = rotated
	require.NoError(t, c.Update(ctx, secret))

	reconcile(t, ctx, reconcilerFor(c), "bp", "keys-ns")
	second := keysAnnotation(t, ctx, c, "bp", "keys-ns")
	assert.NotEmpty(t, second)
	assert.NotEqual(
		t,
		first,
		second,
		"keys-checksum must change when key material changes",
	)

	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "keys-ns"},
		got,
	))
	assert.Equal(
		t,
		int64(1),
		got.Status.OpCert.OnDiskCounter,
		"status.opcert.onDiskCounter must track the accepted certificate",
	)
}

// TestBlockProducerRejectedKeysDoNotRoll is the load-bearing test for the
// validation gate: an invalid bundle delivered over a known-good one must
// leave the pod template byte-identical. Asserting only "no error" would miss
// the failure mode that matters: dropping the keys-checksum annotation removes
// it from the template, which is itself a change and would roll the single
// forging pod onto the rejected keys.
func TestBlockProducerRejectedKeysDoNotRoll(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "reject-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	good, err := keys.SecretData(2, 0)
	require.NoError(t, err)

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "reject-ns",
		},
		Data: good,
	}))

	dn := blockProducerNode(
		"bp", "reject-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "reject-ns")

	accepted := keysAnnotation(t, ctx, c, "bp", "reject-ns")
	require.NotEmpty(t, accepted, "the good bundle must set the annotation")
	wantTemplate := podTemplate(t, ctx, c, "bp", "reject-ns")

	// Deliver a bundle that fails validation for a reason Dingo would only
	// catch at startup: a certificate issued by a different pool's cold key.
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	foreign, err := other.SecretData(3, 0)
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "reject-ns"},
		secret,
	))
	secret.Data["opcert.cert"] = foreign["opcert.cert"]
	require.NoError(t, c.Update(ctx, secret))

	recorder := events.NewFakeRecorder(10)
	r := reconcilerFor(c)
	r.Recorder = recorder
	reconcile(t, ctx, r, "bp", "reject-ns")

	assert.Equal(
		t,
		accepted,
		keysAnnotation(t, ctx, c, "bp", "reject-ns"),
		"a rejected bundle must not change the keys-checksum annotation",
	)
	assert.Equal(
		t,
		wantTemplate,
		podTemplate(t, ctx, c, "bp", "reject-ns"),
		"a rejected bundle must leave the whole pod template unchanged",
	)

	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "reject-ns"},
		got,
	))
	cond := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, cond, "KeysValid condition should be set")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "OpCertRejected", cond.Reason)
	assert.Contains(t, cond.Message, "pool")
	assert.Equal(
		t,
		int64(2),
		got.Status.OpCert.OnDiskCounter,
		"onDiskCounter must keep reporting the accepted certificate",
	)
	// The node keeps forging on the keys its process already loaded, so
	// nothing else in status goes red. Degraded is the only signal that
	// rotation has stopped.
	deg := meta.FindStatusCondition(got.Status.Conditions, condDegraded)
	require.NotNil(t, deg, "a refused bundle must mark the node Degraded")
	assert.Equal(t, metav1.ConditionTrue, deg.Status)
	assert.Equal(t, "OpCertRejected", deg.Reason)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "OpCertRejected")
	default:
		t.Error("no Event was recorded for the rejected key bundle")
	}

	// Recovery: a correctly signed replacement rolls as usual.
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "reject-ns"},
		secret,
	))
	require.NoError(t, keys.RotateKES(rand.Reader))
	fixed, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	secret.Data = fixed
	require.NoError(t, c.Update(ctx, secret))

	reconcile(t, ctx, reconcilerFor(c), "bp", "reject-ns")
	assert.NotEqual(
		t,
		accepted,
		keysAnnotation(t, ctx, c, "bp", "reject-ns"),
		"a valid replacement bundle must roll the pod",
	)
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "reject-ns"},
		got,
	))
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
	)
	assert.Equal(t, int64(3), got.Status.OpCert.OnDiskCounter)
	assert.Nil(
		t,
		meta.FindStatusCondition(got.Status.Conditions, condDegraded),
		"accepting a replacement bundle must clear Degraded",
	)
}

// TestBlockProducerFirstReconcileRejectsBadKeys covers the no-StatefulSet case:
// there is no previous checksum to carry forward, so the annotation is simply
// absent — and must not be a checksum of the rejected material.
func TestBlockProducerFirstReconcileRejectsBadKeys(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "first-ns")

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "first-ns",
		},
		Data: map[string][]byte{
			"kes.skey":    []byte("kes-material"),
			"opcert.cert": []byte("opcert-material"),
			"vrf.skey":    []byte("vrf-material"),
		},
	}))

	dn := blockProducerNode("bp", "first-ns", "pool-keys", "")
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "first-ns")

	assert.Empty(
		t,
		keysAnnotation(t, ctx, c, "bp", "first-ns"),
		"rejected material must never reach the pod-template annotation",
	)
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "first-ns"},
		got,
	))
	cond := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Zero(t, got.Status.OpCert.OnDiskCounter)
}

func TestConfigRefChecksumRollout(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "cfg-ns")

	require.NoError(t, c.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "net-config", Namespace: "cfg-ns"},
		Data: map[string]string{
			"config.json":          `{"Protocol":"Cardano"}`,
			"shelley-genesis.json": `{"networkMagic":42}`,
		},
	}))

	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "custom-relay", Namespace: "cfg-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:         dingov1alpha1.RoleRelay,
			Network:      "custom",
			NetworkMagic: new(int64(42)),
			ConfigRef:    "net-config",
			// custom + configRef requires Mithril handled explicitly.
			Mithril: dingov1alpha1.MithrilSpec{Enabled: new(false)},
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "custom-relay", "cfg-ns")

	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "custom-relay", Namespace: "cfg-ns"},
		sts,
	))
	first := sts.Spec.Template.Annotations["dingo.blinklabs.io/config-checksum"]
	assert.NotEmpty(t, first, "config-checksum should be set from the ConfigMap")

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "net-config", Namespace: "cfg-ns"},
		cm,
	))
	cm.Data["shelley-genesis.json"] = `{"networkMagic":42,"epochLength":500}`
	require.NoError(t, c.Update(ctx, cm))

	reconcile(t, ctx, reconcilerFor(c), "custom-relay", "cfg-ns")
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "custom-relay", Namespace: "cfg-ns"},
		sts,
	))
	second := sts.Spec.Template.Annotations["dingo.blinklabs.io/config-checksum"]
	assert.NotEqual(
		t,
		first,
		second,
		"config-checksum must change when the config bundle changes",
	)
}

func TestReconcileInvalidSpecSetsDegraded(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "bad-ns")

	// network "custom" without networkMagic passes the CRD schema but fails the
	// controller-side cross-field validation, so it must be marked Degraded.
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "bad-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "custom",
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bad", "bad-ns")

	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "bad", Namespace: "bad-ns"}, got))
	assert.Equal(t, "Degraded", got.Status.Phase)
	// No StatefulSet should have been created for an invalid spec.
	err := c.Get(ctx, types.NamespacedName{Name: "bad", Namespace: "bad-ns"}, &appsv1.StatefulSet{})
	assert.True(t, apierrors.IsNotFound(err))
}
