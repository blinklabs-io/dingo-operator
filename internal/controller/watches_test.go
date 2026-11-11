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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func watchScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	return scheme
}

func watchNode(
	ns, name, configRef string,
	relayRefs ...string,
) *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:      dingov1alpha1.RoleRelay,
			Network:   "preview",
			ConfigRef: configRef,
			Topology:  dingov1alpha1.TopologySpec{RelayRefs: relayRefs},
		},
	}
}

func requestNames(reqs []ctrl.Request) []string {
	names := make([]string, 0, len(reqs))
	for _, req := range reqs {
		names = append(names, req.Namespace+"/"+req.Name)
	}
	return names
}

func TestReferencedObjectMappers(t *testing.T) {
	nodes := []client.Object{
		watchNode("a", "uses-cfg", "cfg"),
		watchNode("a", "also-cfg", "cfg", "relay"),
		watchNode("a", "other-cfg", "other"),
		watchNode("a", "relay", "", "bp"),
		watchNode("a", "bp", "", "relay"),
		watchNode("a", "self", "", "self"),
		watchNode("b", "uses-cfg", "cfg", "relay"),
	}
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
		WithObjects(nodes...).Build()
	r := reconcilerFor(c)
	ctx := context.Background()

	ownedBy := func(ns, name, kind, apiVersion string) *appsv1.StatefulSet {
		ctl := true
		return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: apiVersion,
				Kind:       kind,
				Name:       name,
				Controller: &ctl,
			}},
		}}
	}
	gv := dingov1alpha1.GroupVersion.String()

	tests := []struct {
		name string
		got  []ctrl.Request
		want []string
	}{
		{
			name: "ConfigMap maps to the nodes in its namespace naming it",
			got: r.dingoNodesForConfigMap(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "a"},
			}),
			want: []string{"a/also-cfg", "a/uses-cfg"},
		},
		{
			name: "unreferenced ConfigMap maps to nothing",
			got: r.dingoNodesForConfigMap(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bp-topology",
					Namespace: "a",
				},
			}),
		},
		{
			name: "peer maps to every node listing it in relayRefs",
			got:  r.dingoNodesForRelayRef(ctx, watchNode("a", "relay", "")),
			want: []string{"a/also-cfg", "a/bp"},
		},
		{
			name: "peer in another namespace maps only within it",
			got:  r.dingoNodesForRelayRef(ctx, watchNode("b", "relay", "")),
			want: []string{"b/uses-cfg"},
		},
		{
			name: "a node listing itself is not mapped to itself",
			got:  r.dingoNodesForRelayRef(ctx, watchNode("a", "self", "")),
		},
		{
			name: "peer StatefulSet maps through its controlling DingoNode",
			got: r.dingoNodesForPeerStatefulSet(
				ctx,
				ownedBy("a", "bp", "DingoNode", gv),
			),
			want: []string{"a/relay"},
		},
		{
			name: "StatefulSet controlled by another kind maps to nothing",
			got: r.dingoNodesForPeerStatefulSet(
				ctx,
				ownedBy("a", "bp", "Deployment", "apps/v1"),
			),
		},
		{
			name: "StatefulSet without a controller maps to nothing",
			got: r.dingoNodesForPeerStatefulSet(ctx, &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "a"},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.ElementsMatch(t, tt.want, requestNames(tt.got))
		})
	}
}

// A failed list enqueues nothing rather than panicking; the periodic requeue
// still applies the change.
func TestReferencedObjectMapperListError(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
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
	reqs := reconcilerFor(c).dingoNodesForConfigMap(
		context.Background(),
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "a"},
		},
	)
	assert.Empty(t, reqs)
}

// startManager runs the reconciler under a real manager against the envtest
// control plane, so the watches registered in SetupWithManager are exercised.
func startManager(t *testing.T) (client.Client, context.Context) {
	t.Helper()
	cfg := startEnvConfig(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 watchScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: config.Controller{
			SkipNameValidation: new(true),
		},
	})
	require.NoError(t, err)
	r := &DingoNodeReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}
	require.NoError(t, r.SetupWithManager(mgr))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	c, err := client.New(cfg, client.Options{Scheme: watchScheme(t)})
	require.NoError(t, err)
	return c, ctx
}

// watchDeadline is well inside requeueInterval, so a change seen before it was
// delivered by a watch rather than by the periodic requeue.
const watchDeadline = 30 * time.Second

func TestConfigRefChangeAppliedWithoutRequeue(t *testing.T) {
	c, ctx := startManager(t)
	createNamespace(t, ctx, c, "watch-cfg")
	require.NoError(t, c.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "net-config",
			Namespace: "watch-cfg",
		},
		Data: map[string]string{"config.json": `{"Protocol":"Cardano"}`},
	}))
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-node",
			Namespace: "watch-cfg",
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:         dingov1alpha1.RoleRelay,
			Network:      "custom",
			NetworkMagic: new(int64(42)),
			ConfigRef:    "net-config",
			Mithril:      dingov1alpha1.MithrilSpec{Enabled: new(false)},
		},
	}
	require.NoError(t, c.Create(ctx, dn))

	key := types.NamespacedName{Name: "custom-node", Namespace: "watch-cfg"}
	checksum := func() string {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, key, sts); err != nil {
			return ""
		}
		return sts.Spec.Template.Annotations[resources.ConfigChecksumAnnotation]
	}
	var first string
	require.Eventually(t, func() bool {
		first = checksum()
		return first != ""
	}, watchDeadline, 100*time.Millisecond)

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "net-config", Namespace: "watch-cfg"},
		cm,
	))
	cm.Data["config.json"] = `{"Protocol":"Cardano","Changed":true}`
	require.NoError(t, c.Update(ctx, cm))

	assert.Eventually(t, func() bool {
		got := checksum()
		return got != "" && got != first
	}, watchDeadline, 100*time.Millisecond,
		"a configRef change must roll the pod without waiting for the requeue")
}

func TestRelayRefPortChangeAppliedWithoutRequeue(t *testing.T) {
	c, ctx := startManager(t)
	createNamespace(t, ctx, c, "watch-peer")
	require.NoError(t, c.Create(ctx, watchNode("watch-peer", "peer", "")))
	require.NoError(
		t,
		c.Create(ctx, watchNode("watch-peer", "dialer", "", "peer")),
	)

	topo := func() string {
		cm := &corev1.ConfigMap{}
		key := types.NamespacedName{
			Name:      "dialer-topology",
			Namespace: "watch-peer",
		}
		if err := c.Get(ctx, key, cm); err != nil {
			return ""
		}
		return cm.Data["topology.json"]
	}
	require.Eventually(t, func() bool {
		return topo() != ""
	}, watchDeadline, 100*time.Millisecond)

	peer := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "peer", Namespace: "watch-peer"},
		peer,
	))
	peer.Spec.RelayService.Port = 4711
	require.NoError(t, c.Update(ctx, peer))

	assert.Eventually(t, func() bool {
		return strings.Contains(topo(), `"port": 4711`)
	}, watchDeadline, 100*time.Millisecond,
		"a relayRefs peer's port change must reach the dialer's topology")
}

// startEnvConfig brings up an envtest control plane with the DingoNode CRD
// installed and returns its REST config. It skips the test when
// KUBEBUILDER_ASSETS is not configured.
func startEnvConfig(t *testing.T) *rest.Config {
	t.Helper()
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
	return cfg
}
