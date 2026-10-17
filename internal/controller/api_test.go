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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestAPIServicesReconfigure(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "apis")
	dn := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "apis"},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:        dingov1alpha1.RoleRelay,
			Network:     "preview",
			StorageMode: dingov1alpha1.StorageModeAPI,
			Mithril:     dingov1alpha1.MithrilSpec{Enabled: new(false)},
			APIs:        &dingov1alpha1.APISpec{Kupo: 1442},
			RelayService: dingov1alpha1.RelayServiceSpec{
				Type: corev1.ServiceTypeNodePort,
			},
		},
	}
	require.NoError(t, c.Create(ctx, dn))
	r := reconcilerFor(c)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dn)}
	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	key := func(suffix string) client.ObjectKey {
		return client.ObjectKey{Name: dn.Name + suffix, Namespace: dn.Namespace}
	}
	var relay, private, metrics corev1.Service
	require.NoError(t, c.Get(ctx, key("-relay"), &relay))
	require.NoError(t, c.Get(ctx, key("-private"), &private))
	require.NoError(t, c.Get(ctx, key("-metrics"), &metrics))
	require.Len(t, relay.Spec.Ports, 1)
	assert.Equal(t, "relay", relay.Spec.Ports[0].Name)
	assert.Positive(t, relay.Spec.Ports[0].NodePort)
	assigned := relay.Spec.Ports[0].NodePort
	assert.Equal(t, corev1.ServiceTypeClusterIP, private.Spec.Type)
	assert.Equal(t, corev1.ServiceTypeClusterIP, metrics.Spec.Type)
	require.Len(t, private.Spec.Ports, 2)
	assert.Equal(t, "kupo", private.Spec.Ports[1].Name)
	uid, ip := private.UID, private.Spec.ClusterIP
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(ctx, key(""), &sts))
	stsUID := sts.UID
	require.NoError(t, c.Get(ctx, req.NamespacedName, dn))
	dn.Spec.APIs.Kupo = 0
	dn.Spec.APIs.Blockfrost = 3100
	require.NoError(t, c.Update(ctx, dn))
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key("-private"), &private))
	assert.Equal(t, uid, private.UID)
	assert.Equal(t, ip, private.Spec.ClusterIP)
	require.Len(t, private.Spec.Ports, 2)
	assert.Equal(t, "blockfrost", private.Spec.Ports[1].Name)
	assert.Equal(t, int32(3100), private.Spec.Ports[1].Port)
	require.NoError(t, c.Get(ctx, key("-relay"), &relay))
	assert.Equal(t, assigned, relay.Spec.Ports[0].NodePort)
	require.NoError(t, c.Get(ctx, key(""), &sts))
	assert.Equal(t, stsUID, sts.UID)
	var policy networkingv1.NetworkPolicy
	require.NoError(t, c.Get(ctx, key(""), &policy))
	for _, rule := range policy.Spec.Ingress {
		for _, port := range rule.Ports {
			assert.NotEqual(t, int32(1442), port.Port.IntVal)
		}
	}
	assert.True(t, metav1.IsControlledBy(&private, dn))
	assert.NoError(t, resources.ValidateAPIs(dn))
	require.NoError(t, c.Get(ctx, req.NamespacedName, dn))
	dn.Spec.APIs.Blockfrost = 3001
	require.NoError(t, c.Update(ctx, dn))
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key("-private"), &private))
	assert.Equal(t, int32(3100), private.Spec.Ports[1].Port)
	require.NoError(t, c.Get(ctx, req.NamespacedName, dn))
	assert.Equal(t, "Degraded", dn.Status.Phase)
	dn.Spec.APIs.Blockfrost = 65536
	require.Error(
		t,
		c.Update(ctx, dn),
		"CRD admission must reject out-of-range ports",
	)
}
