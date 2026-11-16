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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestReconcileAdmitsOperatorNamespaceToMetrics checks that the reconciler
// passes its own namespace to the node's NetworkPolicy, so its KES and wedge
// scrape works from a namespace without the metrics access label.
func TestReconcileAdmitsOperatorNamespaceToMetrics(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := reconcilerFor(c)
	r.OperatorNamespace = "operators"
	dn := dataNameNode()
	require.NoError(t, r.reconcileResources(context.Background(), dn))

	np := &networkingv1.NetworkPolicy{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}, np))
	found := false
	for _, rule := range np.Spec.Ingress {
		for _, peer := range rule.From {
			if peer.NamespaceSelector != nil &&
				peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] == "operators" {
				found = true
			}
		}
	}
	require.True(t, found, "operator namespace peer missing: %+v", np.Spec.Ingress)
}
