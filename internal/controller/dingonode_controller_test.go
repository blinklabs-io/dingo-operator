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
	"os"
	"path/filepath"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

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

func TestBlockProducerKeysChecksumRollout(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "keys-ns")

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "keys-ns",
		},
		Data: map[string][]byte{
			"kes.skey":    []byte("kes-material"),
			"opcert.cert": []byte("opcert-material"),
			"vrf.skey":    []byte("vrf-material"),
		},
	}))

	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "keys-ns"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleBlockProducer,
			Network: "preview",
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				SlotsPerKESPeriod: 129600,
				MaxKESEvolutions:  62,
				Keys:              dingov1alpha1.KeysSpec{SecretRef: "pool-keys"},
				Rotation: dingov1alpha1.RotationSpec{
					Mode: dingov1alpha1.RotationModeMonitorOnly,
				},
			},
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "bp", "keys-ns")

	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "keys-ns"},
		sts,
	))
	first := sts.Spec.Template.Annotations["dingo.blinklabs.io/keys-checksum"]
	assert.NotEmpty(
		t,
		first,
		"keys-checksum annotation should be set from the Secret",
	)

	// Rotating the key material must change the checksum so the pod rolls.
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "pool-keys", Namespace: "keys-ns"},
		secret,
	))
	secret.Data["opcert.cert"] = []byte("opcert-material-ROTATED")
	require.NoError(t, c.Update(ctx, secret))

	reconcile(t, ctx, reconcilerFor(c), "bp", "keys-ns")
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "keys-ns"},
		sts,
	))
	second := sts.Spec.Template.Annotations["dingo.blinklabs.io/keys-checksum"]
	assert.NotEmpty(t, second)
	assert.NotEqual(
		t,
		first,
		second,
		"keys-checksum must change when key material changes",
	)
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
