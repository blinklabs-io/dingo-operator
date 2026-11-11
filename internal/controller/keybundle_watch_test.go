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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func keyBundleScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	return scheme
}

func keyBundleRequests(reqs []ctrl.Request) []string {
	names := make([]string, 0, len(reqs))
	for _, req := range reqs {
		names = append(names, req.Namespace+"/"+req.Name)
	}
	return names
}

func TestDingoNodesForKeyBundle(t *testing.T) {
	pool := strings.Repeat("ab", 28)
	agent := blockProducerNode("agent", "a", "agent-keys", pool)
	agent.Spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:             "agent:9000",
		ClientCertSecretRef: "agent-cert",
	}
	bursa := blockProducerNode("bursa", "a", "bursa-keys", pool)
	bursa.Spec.BlockProducer.Rotation.ColdSigner.MTLSSecretRef = "signer-mtls"
	nodes := []client.Object{
		blockProducerNode("bp", "a", "keys", pool),
		blockProducerNode("bp-shared", "a", "keys", pool),
		agent,
		bursa,
		blockProducerNode("bp", "b", "keys", pool),
		relayNodeForStatus("relay", "a"),
	}
	c := fake.NewClientBuilder().WithScheme(keyBundleScheme(t)).
		WithObjects(nodes...).Build()
	r := reconcilerFor(c)

	secret := func(ns, name string) *metav1.PartialObjectMetadata {
		return &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}
	}
	tests := []struct {
		name   string
		secret client.Object
		want   []string
	}{
		{
			name:   "keys Secret maps to every node in its namespace using it",
			secret: secret("a", "keys"),
			want:   []string{"a/bp", "a/bp-shared"},
		},
		{
			name:   "same name in another namespace maps only there",
			secret: secret("b", "keys"),
			want:   []string{"b/bp"},
		},
		{
			name:   "signing agent client certificate",
			secret: secret("a", "agent-cert"),
			want:   []string{"a/agent"},
		},
		{
			name:   "cold signer mTLS credentials",
			secret: secret("a", "signer-mtls"),
			want:   []string{"a/bursa"},
		},
		{
			name:   "unreferenced Secret maps to nothing",
			secret: secret("a", "unrelated"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.ElementsMatch(
				t,
				tt.want,
				keyBundleRequests(
					r.dingoNodesForKeyBundle(context.Background(), tt.secret),
				),
			)
		})
	}
}

// A failed list enqueues nothing; the periodic requeue still applies the
// change.
func TestDingoNodesForKeyBundleListError(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(keyBundleScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				context.Context,
				client.WithWatch,
				client.ObjectList,
				...client.ListOption,
			) error {
				return errors.New("list failed")
			},
		}).Build()
	reqs := reconcilerFor(c).dingoNodesForKeyBundle(
		context.Background(),
		&metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{Name: "keys", Namespace: "a"},
		},
	)
	assert.Empty(t, reqs)
}

func TestKeyBundleCacheOptions(t *testing.T) {
	assert.Empty(t, KeyBundleCacheOptions(nil).ByObject,
		"no key namespaces must leave the cache unscoped and unchanged")

	opts := KeyBundleCacheOptions([]string{"keys-a", "keys-b"})
	require.Len(t, opts.ByObject, 1)
	for obj, by := range opts.ByObject {
		assert.IsType(t, &corev1.Secret{}, obj)
		assert.Len(t, by.Namespaces, 2)
		assert.Contains(t, by.Namespaces, "keys-a")
		assert.Contains(t, by.Namespaces, "keys-b")
	}
}

// A delivered bundle must be acted on through the watch, well inside the
// requeue interval, by a manager configured the way main configures it.
func TestKeyBundleWatchAppliesDeliveredBundle(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test`")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = env.Stop() })

	const ns = "key-watch"
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 keyBundleScheme(t),
		Cache:                  KeyBundleCacheOptions([]string{ns}),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: config.Controller{
			SkipNameValidation: new(true),
		},
	})
	require.NoError(t, err)
	r := &DingoNodeReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		WatchKeyBundles: true,
	}
	require.NoError(t, r.SetupWithManager(mgr))

	c, err := client.New(cfg, client.Options{Scheme: keyBundleScheme(t)})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	createNamespace(t, ctx, c, ns)
	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()

	require.NoError(t, c.Create(
		ctx,
		blockProducerNode("bp", ns, "keys", strings.Repeat("ab", 28)),
	))
	keysReason := func() string {
		dn := &dingov1alpha1.DingoNode{}
		key := types.NamespacedName{Name: "bp", Namespace: ns}
		if err := c.Get(ctx, key, dn); err != nil {
			return ""
		}
		cond := meta.FindStatusCondition(dn.Status.Conditions, condKeysValid)
		if cond == nil {
			return ""
		}
		return cond.Reason
	}
	const deadline = 30 * time.Second
	require.Eventually(t, func() bool {
		return keysReason() == "KeysSecretMissing"
	}, deadline, 100*time.Millisecond)

	// Deliver a bundle. It does not validate, which is fine: the point is that
	// the controller looks at it now rather than on the requeue.
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "keys", Namespace: ns},
		Data:       map[string][]byte{"vrf.skey": []byte("not a key")},
	}))
	assert.Eventually(
		t,
		func() bool {
			reason := keysReason()
			return reason != "" && reason != "KeysSecretMissing"
		},
		deadline,
		100*time.Millisecond,
		"a delivered key bundle must be validated without waiting for the requeue",
	)
}
