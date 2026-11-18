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
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// chartMatchLabels is the selector of the dingo Helm chart's StatefulSet
// (dingo.matchLabels).
func chartMatchLabels(release, network string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "dingo",
		"app.kubernetes.io/instance": release,
		"cardano_network":            network,
		"cardano_service":            "dingo",
	}
}

// chartLabels is dingo.labels: the match labels plus Helm's own.
func chartLabels(release, network string) map[string]string {
	l := chartMatchLabels(release, network)
	l["helm.sh/chart"] = "dingo-0.0.0"
	l["app.kubernetes.io/version"] = "0.0.0"
	l["app.kubernetes.io/managed-by"] = "Helm"
	return l
}

// chartStatefulSet is the StatefulSet a dingo chart release leaves behind
// after `helm uninstall`: the release has set helm.sh/resource-policy: keep,
// so it has no owner and nothing but its labels says who made it. keysSecret,
// when set, is mounted as the chart mounts a block producer's keys.
func chartStatefulSet(
	release, ns, network, keysSecret string,
) *appsv1.StatefulSet {
	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: chartLabels(release, network)},
		Spec:       chartPodSpec(keysSecret),
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      release,
			Namespace: ns,
			Labels:    chartLabels(release, network),
			Annotations: map[string]string{
				"helm.sh/resource-policy":        "keep",
				"meta.helm.sh/release-name":      release,
				"meta.helm.sh/release-namespace": ns,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: release + "-headless",
			Replicas:    new(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: chartMatchLabels(release, network),
			},
			Template: tmpl,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "dingo-data",
					Labels: map[string]string{"cardano_network": network},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}},
		},
	}
}

func chartPodSpec(keysSecret string) corev1.PodSpec {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "dingo",
			Image: "ghcr.io/blinklabs-io/dingo:latest",
			VolumeMounts: []corev1.VolumeMount{{
				Name: "dingo-data", MountPath: "/var/lib/dingo",
			}},
		}},
	}
	if keysSecret != "" {
		spec.Volumes = []corev1.Volume{{
			Name: "block-producer-keys-source",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: keysSecret},
			},
		}}
	}
	return spec
}

// chartPod is the pod the chart's StatefulSet is running: controlled by that
// StatefulSet and carrying the chart's labels, not the operator's. Without a
// StatefulSet controller in envtest its data claim is an emptyDir.
func chartPod(sts *appsv1.StatefulSet, keysSecret string) *corev1.Pod {
	spec := chartPodSpec(keysSecret)
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name: "dingo-data",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sts.Name + "-0",
			Namespace: sts.Namespace,
			Labels:    sts.Spec.Template.Labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       sts.Name,
				UID:        sts.UID,
				Controller: new(true),
			}},
		},
		Spec: spec,
	}
}

func getStatefulSet(
	t *testing.T, ctx context.Context, c client.Client, name, ns string,
) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: name, Namespace: ns}, sts,
	))
	return sts
}

func relayNode(name, ns, network string) *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: network,
		},
	}
}

// A DingoNode created beside a StatefulSet a chart release left running takes
// that StatefulSet over. The object and its claim template are kept; the pod
// template is replaced with the operator's, which rolls the pod. Envtest runs
// no StatefulSet controller, so neither a pod nor a claim is created here.
func TestReconcileAdoptsChartStatefulSet(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	ns := "adopt-relay"
	createNamespace(t, ctx, c, ns)

	chart := chartStatefulSet("dingo", ns, "preview", "")
	require.NoError(t, c.Create(ctx, chart))
	require.Empty(t, chart.OwnerReferences)

	dn := relayNode("dingo", ns, "preview")
	require.NoError(t, c.Create(ctx, dn))
	reconcile(t, ctx, reconcilerFor(c), "dingo", ns)

	got := getStatefulSet(t, ctx, c, "dingo", ns)
	assert.Equal(t, chart.UID, got.UID, "the StatefulSet was recreated")
	assert.Equal(t, chart.CreationTimestamp, got.CreationTimestamp)

	require.Len(t, got.OwnerReferences, 1)
	owner := got.OwnerReferences[0]
	assert.Equal(t, "DingoNode", owner.Kind)
	assert.Equal(t, "dingo", owner.Name)
	assert.Equal(t, dn.UID, owner.UID)
	assert.True(t, ptr.Deref(owner.Controller, false))

	assert.Equal(t, chart.Spec.Selector, got.Spec.Selector)
	assert.Equal(t, chartMatchLabels("dingo", "preview"),
		got.Spec.Selector.MatchLabels)
	assert.Equal(t, chart.Spec.ServiceName, got.Spec.ServiceName)
	require.NotNil(t, got.Spec.Replicas)
	assert.Equal(t, int32(1), *got.Spec.Replicas)

	// The new template carries the operator's labels and still matches the
	// selector the chart set, which cannot change.
	tmplLabels := got.Spec.Template.Labels
	for k, v := range resources.Labels(dn) {
		assert.Equal(t, v, tmplLabels[k], "template label %s", k)
	}
	assert.True(t, labels.SelectorFromSet(got.Spec.Selector.MatchLabels).
		Matches(labels.Set(tmplLabels)))
	assert.Equal(t, resources.ManagedByName,
		got.Labels["app.kubernetes.io/managed-by"])

	// The claim template, and so every claim the pod already has, is kept,
	// and the new template mounts that claim.
	require.Len(t, got.Spec.VolumeClaimTemplates, 1)
	assert.Equal(t, "dingo-data", got.Spec.VolumeClaimTemplates[0].Name)
	assert.Equal(t, chart.Spec.VolumeClaimTemplates[0].Labels,
		got.Spec.VolumeClaimTemplates[0].Labels)
	var mountedAt string
	for _, ctr := range got.Spec.Template.Spec.Containers {
		if ctr.Name != "dingo" {
			continue
		}
		for _, m := range ctr.VolumeMounts {
			if m.Name == "dingo-data" {
				mountedAt = m.MountPath
			}
		}
	}
	assert.Equal(t, "/data", mountedAt,
		"the dingo container must mount dingo-data at the operator's data path")

	// A second pass changes nothing it should not.
	reconcile(t, ctx, reconcilerFor(c), "dingo", ns)
	again := getStatefulSet(t, ctx, c, "dingo", ns)
	assert.Equal(t, chart.UID, again.UID)
	assert.Equal(t, got.Spec.Selector, again.Spec.Selector)
	assert.Equal(t, got.Spec.VolumeClaimTemplates[0].Name,
		again.Spec.VolumeClaimTemplates[0].Name)
}

// A chart StatefulSet whose cardano_network differs from the DingoNode's
// cannot take the operator's template: the selector is immutable and the
// template would no longer match it.
func TestReconcileChartStatefulSetNetworkMismatch(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	ns := "adopt-mismatch"
	createNamespace(t, ctx, c, ns)

	chart := chartStatefulSet("dingo", ns, "preprod", "")
	require.NoError(t, c.Create(ctx, chart))
	dn := relayNode("dingo", ns, "preview")
	require.NoError(t, c.Create(ctx, dn))

	r := reconcilerFor(c)
	for pass := range 2 {
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "dingo", Namespace: ns},
		})
		require.Error(t, err, "pass %d", pass)
		assert.True(
			t,
			apierrors.IsInvalid(err),
			"not an API rejection: %v",
			err,
		)
		assert.Contains(t, err.Error(), "selector")
		assert.Contains(t, err.Error(), "apply statefulset")

		got := getStatefulSet(t, ctx, c, "dingo", ns)
		assert.Equal(t, chart.UID, got.UID)
		assert.Equal(t, chart.ResourceVersion, got.ResourceVersion,
			"the rejected update must leave the StatefulSet untouched")
		assert.Empty(t, got.OwnerReferences)
		assert.Equal(t, chart.Spec.Selector, got.Spec.Selector)
		assert.Equal(t, chart.Spec.Template.Labels, got.Spec.Template.Labels)
	}
}

// A block producer takes over the chart StatefulSet that already forges with
// its pool's keys without refusing itself as a duplicate and without a second
// forger: the chart's StatefulSet is not an operator workload for the fence
// to count, and once adopted it is the node's own.
func TestReconcileAdoptsChartBlockProducer(t *testing.T) {
	t.Parallel()
	c, ctx := startEnv(t)
	ns := "adopt-bp"
	createNamespace(t, ctx, c, ns)
	poolID, data := poolKeys(t)
	require.NoError(t, c.Create(ctx, keysSecret("pool-keys", ns, data)))

	chart := chartStatefulSet("dingo", ns, "preview", "pool-keys")
	require.NoError(t, c.Create(ctx, chart))
	pod := chartPod(chart, "pool-keys")
	require.NoError(t, c.Create(ctx, pod))

	dn := blockProducerNode("dingo", ns, "pool-keys", poolID)
	require.NoError(t, c.Create(ctx, dn))
	r := reconcilerFor(c)

	for pass := range 2 {
		reconcile(t, ctx, r, "dingo", ns)

		var node dingov1alpha1.DingoNode
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(dn), &node))
		assert.False(t, refusedAsDuplicate(t, c, dn), "pass %d", pass)
		degraded := meta.FindStatusCondition(
			node.Status.Conditions, condDegraded,
		)
		if degraded != nil {
			assert.NotEqual(t, reasonDuplicateForger, degraded.Reason)
			assert.NotEqual(t, metav1.ConditionTrue, degraded.Status,
				degraded.Message)
		}

		// One StatefulSet, the chart's, still wanting its one replica: the
		// operator neither halted it nor made another beside it.
		var sets appsv1.StatefulSetList
		require.NoError(t, c.List(ctx, &sets, client.InNamespace(ns)))
		require.Len(t, sets.Items, 1)
		got := sets.Items[0]
		assert.Equal(t, chart.UID, got.UID)
		require.NotNil(t, got.Spec.Replicas)
		assert.Equal(t, int32(1), *got.Spec.Replicas)
		assert.True(t, metav1.IsControlledBy(&got, &node))
		assert.Equal(t, chart.Spec.Selector, got.Spec.Selector)

		// It is the same keys Secret the chart's pod mounts.
		var staged bool
		for _, v := range got.Spec.Template.Spec.Volumes {
			staged = staged || (v.Secret != nil &&
				v.Secret.SecretName == "pool-keys")
		}
		assert.True(t, staged)

		// The template is the operator's, not the chart's left in place.
		assert.Equal(t, resources.ManagedByName,
			got.Spec.Template.Labels["app.kubernetes.io/managed-by"])
		assert.Equal(t, "dingo",
			got.Spec.Template.Labels[resources.NodeLabel])

		var pods corev1.PodList
		require.NoError(t, c.List(ctx, &pods, client.InNamespace(ns)))
		require.Len(t, pods.Items, 1, "the operator creates no pod itself")
		assert.Equal(t, pod.UID, pods.Items[0].UID)
	}
}
