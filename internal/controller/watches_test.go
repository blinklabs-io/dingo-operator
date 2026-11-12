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
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
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
	done := make(chan error, 1)
	go func() {
		done <- mgr.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager: %v", err)
		}
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

// A configRef ConfigMap that is briefly absent (deleted and recreated) must
// not change the pod template: dropping the checksum would roll the pod onto
// a template whose config volume cannot mount, and its return would roll it
// again. The live checksum is carried forward until the ConfigMap is back.
func TestConfigRefMissingKeepsLiveChecksum(t *testing.T) {
	dn := watchNode("cfg-ns", "custom-node", "net-config")
	dn.Spec.Network = "custom"
	dn.Spec.NetworkMagic = new(int64(42))
	dn.Spec.Mithril.Enabled = new(false)
	stsKey := types.NamespacedName{Name: "custom-node", Namespace: "cfg-ns"}

	t.Run("live checksum kept", func(t *testing.T) {
		live := resources.BuildStatefulSet(dn, resources.RenderOptions{
			Replicas:       1,
			ConfigChecksum: "live-sum",
		})
		live.CreationTimestamp = metav1.Now()
		c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
			WithObjects(live).Build()
		require.NoError(
			t,
			reconcilerFor(c).reconcileResources(context.Background(), dn.DeepCopy()),
		)
		got := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(context.Background(), stsKey, got))
		assert.Equal(
			t,
			"live-sum",
			got.Spec.Template.Annotations[resources.ConfigChecksumAnnotation],
		)
	})

	t.Run("first render has no checksum", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(watchScheme(t)).Build()
		require.NoError(
			t,
			reconcilerFor(c).reconcileResources(context.Background(), dn.DeepCopy()),
		)
		got := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(context.Background(), stsKey, got))
		assert.NotContains(
			t,
			got.Spec.Template.Annotations,
			resources.ConfigChecksumAnnotation,
		)
	})

	t.Run("statefulset read error fails the reconcile", func(t *testing.T) {
		// Only reads after the missing ConfigMap fail, so the error comes
		// from carrying its checksum forward rather than an earlier read.
		sentinel := errors.New("statefulset read failed")
		configRead := false
		c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(
					ctx context.Context, cl client.WithWatch,
					key client.ObjectKey, obj client.Object,
					opts ...client.GetOption,
				) error {
					switch obj.(type) {
					case *corev1.ConfigMap:
						if key.Name == "net-config" {
							configRead = true
						}
					case *appsv1.StatefulSet:
						if configRead {
							return sentinel
						}
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			}).Build()
		err := reconcilerFor(c).reconcileResources(
			context.Background(), dn.DeepCopy(),
		)
		require.ErrorIs(t, err, sentinel)
		// The read fails before anything is applied.
		svc := &corev1.Service{}
		assert.True(t, apierrors.IsNotFound(c.Get(
			context.Background(), stsKey, svc,
		)), "no child object is applied after a failed read")
	})
}

// A relay peer is dialed on the port its live pod template carries, not its
// spec port: until the peer's StatefulSet is updated its pods do not serve
// the new port. The StatefulSet watch moves the dialer once it is.
func TestRelayRefPortFollowsLiveRelayTemplate(t *testing.T) {
	peer := watchNode("a", "peer", "")
	served := peer.DeepCopy()
	served.Spec.RelayService.Port = 3001
	live := resources.BuildStatefulSet(
		served, resources.RenderOptions{Replicas: 1},
	)
	peer.Spec.RelayService.Port = 4711
	dialer := watchNode("a", "dialer", "", "peer")
	c := fake.NewClientBuilder().WithScheme(watchScheme(t)).
		WithObjects(peer, dialer, live).Build()

	ports, err := reconcilerFor(c).relayRefPorts(context.Background(), dialer)
	require.NoError(t, err)
	assert.Equal(t, map[string]int32{"peer": 3001}, ports)
}
