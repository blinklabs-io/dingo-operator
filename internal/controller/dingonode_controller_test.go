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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
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

// stubOnChain is a controllable onchain.Fetcher so the controller suite can
// exercise the on-chain counter path without a Cardano node.
type stubOnChain struct {
	counter onchain.Counter
	err     error
	calls   int
}

func (s *stubOnChain) Fetch(
	context.Context,
	onchain.Query,
) (onchain.Counter, error) {
	s.calls++
	return s.counter, s.err
}

// ageOnChainAttempt backdates the reconciler's remembered attempt time so the
// next reconcile's rate limit lets a dial through, without sleeping out the
// refresh interval. Attempts are in-memory (see onChainAttempts), so this is
// the only way to simulate the interval elapsing.
func ageOnChainAttempt(
	r *DingoNodeReconciler,
	key types.NamespacedName,
	age time.Duration,
) {
	r.onChainMu.Lock()
	defer r.onChainMu.Unlock()
	if attempt, ok := r.onChainAttempts[key]; ok {
		attempt.at = attempt.at.Add(-age)
		r.onChainAttempts[key] = attempt
	}
}

// TestBlockProducerOnChainCounter walks the whole feature: the counter is read
// before validation so it gates the very pass that rolls the pod, the fetch is
// rate-limited between passes, and a failed read neither clears the value nor
// fails the reconcile.
func TestBlockProducerOnChainCounter(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "onchain-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	// Counter 3, which the chain (stubbed at 5) has already moved past. This is
	// the restore-from-backup shape: a fresh CR, empty status, and a Secret
	// holding a below-chain certificate.
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "onchain-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "onchain-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	stub := &stubOnChain{counter: onchain.Counter{Value: 5, Found: true}}
	r := reconcilerFor(c)
	r.OnChain = stub

	// First pass, and the load-bearing assertion: the counter is fetched before
	// validation, so the below-chain bundle is refused on the *first* reconcile
	// and never reaches the pod template. Reading it afterwards would accept and
	// roll here, then notice on the next pass — a CrashLoop of the one forging
	// pod, which is what this check exists to prevent.
	reconcile(t, ctx, r, "bp", "onchain-ns")
	assert.Equal(t, 1, stub.calls)
	assert.Empty(
		t,
		keysAnnotation(t, ctx, c, "bp", "onchain-ns"),
		"a below-chain bundle must not reach the pod template, first pass "+
			"included",
	)

	got := &dingov1alpha1.DingoNode{}
	key := types.NamespacedName{Name: "bp", Namespace: "onchain-ns"}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, int64(5), got.Status.OpCert.OnChainCounter)
	require.NotNil(t, got.Status.OpCert.OnChainCounterAt)
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condOnChainCounter),
		"OnChainCounterAvailable should be True once the counter is read",
	)
	cond := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "below the on-chain counter 5")
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condDegraded),
		"a refused bundle must show up as Degraded",
	)

	// Second pass: the observation is still fresh, so no new query — one dial per
	// node per refresh interval, not per reconcile. The floor still applies from
	// the stored value.
	reconcile(t, ctx, r, "bp", "onchain-ns")
	assert.Equal(
		t,
		1,
		stub.calls,
		"a fresh observation must not be re-fetched every reconcile",
	)
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, int64(5), got.Status.OpCert.OnChainCounter)
	assert.True(
		t,
		meta.IsStatusConditionFalse(got.Status.Conditions, condKeysValid),
		"the stored floor must keep applying between refreshes",
	)

	// Age the attempt past the refresh interval and make the node unreachable.
	// The failed read must not clear the counter: the value is still a valid
	// lower bound, and zeroing it would silently disable the check.
	ageOnChainAttempt(r, key, onChainCounterRefreshInterval+time.Minute)
	stub.err = errors.New("dial node-to-client: connection refused")
	reconcile(t, ctx, r, "bp", "onchain-ns")
	assert.Equal(t, 2, stub.calls, "a stale observation must be re-fetched")
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(
		t,
		int64(5),
		got.Status.OpCert.OnChainCounter,
		"a failed read must leave the last observed counter in place",
	)
	cond = meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "QueryFailed", cond.Reason)
	assert.Contains(
		t,
		cond.Message,
		resources.NodeToClientAccessLabel,
		"the message should name the label a client needs to reach the port",
	)
}

// TestBlockProducerOnChainCounterRateLimitsFailedAttempts is the regression
// test for rate-limiting *attempts* rather than successes.
//
// A node that never answers never writes status.opcert.onChainCounterAt, so a
// gate keyed on that timestamp never fires and every reconcile dials again.
// That is the state this feature ships in — the Helm chart does not yet label
// operator for the node-to-client NetworkPolicy, and a default-deny CNI drops
// rather than refuses, so each attempt burns the full dial timeout. With
// MaxConcurrentReconciles at its default of 1, enough block producers in that
// state starve every other node's rollout.
func TestBlockProducerOnChainCounterRateLimitsFailedAttempts(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "ratelimit-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(1, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "ratelimit-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "ratelimit-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	// Never succeeds, so nothing ever stamps onChainCounterAt.
	stub := &stubOnChain{err: errors.New("dial node-to-client: i/o timeout")}
	r := reconcilerFor(c)
	r.OnChain = stub
	key := types.NamespacedName{Name: "bp", Namespace: "ratelimit-ns"}

	for range 4 {
		reconcile(t, ctx, r, "bp", "ratelimit-ns")
	}
	assert.Equal(
		t,
		1,
		stub.calls,
		"a node that never answers must be dialled once per refresh "+
			"interval, not once per reconcile",
	)

	// The failure is still reported on every pass, from the remembered outcome —
	// rate-limiting the dial must not make the condition flap or vanish.
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, got))
	cond := meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	// That one dial happened before this reconcile created the node's Service, so
	// the honest diagnosis is "the node is not up yet" — telling an operator to
	// check a NetworkPolicy label here would be misleading, and every healthy new
	// block producer passes through this state.
	assert.Equal(t, "NodeNotReady", cond.Reason)

	// Once the interval has elapsed, exactly one more dial.
	ageOnChainAttempt(r, key, onChainCounterRefreshInterval+time.Second)
	reconcile(t, ctx, r, "bp", "ratelimit-ns")
	reconcile(t, ctx, r, "bp", "ratelimit-ns")
	assert.Equal(t, 2, stub.calls, "one dial per elapsed interval")

	// The Service exists now, so the same failure is re-diagnosed as the
	// reachability problem it now is.
	require.NoError(t, c.Get(ctx, key, got))
	cond = meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, "QueryFailed", cond.Reason)
	assert.Contains(t, cond.Message, resources.NodeToClientAccessLabel)

	// Validation kept working throughout: with no floor available it falls back
	// to the on-disk counter, which this bundle satisfies.
	require.NoError(t, c.Get(ctx, key, got))
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
		"an unreachable node must not block a valid rotation",
	)
	assert.Equal(t, int64(1), got.Status.OpCert.OnDiskCounter)
}

// TestBlockProducerOnChainCounterSurvivesResourceFailure covers the other half
// of attempt-gating: a successful read is used by the same reconcile that took
// it, and a reconcile that errors after the read neither loses the observation
// nor re-dials on the controller-runtime backoff retry.
func TestBlockProducerOnChainCounterSurvivesResourceFailure(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "resfail-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	// Below the chain's counter (5): the floor must survive the retry, or the
	// retry would accept this bundle.
	data, err := keys.SecretData(2, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "resfail-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "resfail-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	stub := &stubOnChain{counter: onchain.Counter{Value: 5, Found: true}}
	r := reconcilerFor(c)
	r.OnChain = stub
	key := types.NamespacedName{Name: "bp", Namespace: "resfail-ns"}

	// Simulate "reconcileResources failed after the fetch": run refresh on its
	// own working copy and throw the copy away without persisting status, exactly
	// as Reconcile does when it returns an error before reconcileStatus.
	lost := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, lost))
	r.refreshOnChainCounter(ctx, lost)
	require.Equal(t, 1, stub.calls)
	require.Equal(t, int64(5), lost.Status.OpCert.OnChainCounter)

	persisted := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, persisted))
	require.Zero(
		t,
		persisted.Status.OpCert.OnChainCounter,
		"precondition: the observation was not persisted",
	)

	// The retry must not re-dial, and must still have the floor.
	reconcile(t, ctx, r, "bp", "resfail-ns")
	assert.Equal(
		t,
		1,
		stub.calls,
		"a backoff retry inside the interval must not re-dial",
	)
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(
		t,
		int64(5),
		got.Status.OpCert.OnChainCounter,
		"the remembered observation must be restored into status",
	)
	cond := meta.FindStatusCondition(got.Status.Conditions, condKeysValid)
	require.NotNil(t, cond)
	assert.Equal(
		t,
		metav1.ConditionFalse,
		cond.Status,
		"the floor must apply on the retry, not just on the lost pass",
	)
	assert.Contains(t, cond.Message, "below the on-chain counter 5")
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condOnChainCounter),
		"the Observed condition must surface even though the fetching "+
			"reconcile never persisted it",
	)
}

// A poolId edit must not leave the previous pool's counter enforced: provenance
// is the whole justification for the freshness window, and the refresh interval
// would otherwise widen the window in which a foreign floor still bites.
func TestBlockProducerOnChainCounterClearedOnPoolChange(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "poolchange-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "poolchange-ns",
		},
		Data: data,
	}))

	// Start out pointed at another pool, with that pool's high counter observed.
	other, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	dn := blockProducerNode(
		"bp", "poolchange-ns", "pool-keys", hex.EncodeToString(other.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	stub := &stubOnChain{counter: onchain.Counter{Value: 9, Found: true}}
	r := reconcilerFor(c)
	r.OnChain = stub
	key := types.NamespacedName{Name: "bp", Namespace: "poolchange-ns"}
	reconcile(t, ctx, r, "bp", "poolchange-ns")

	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(ctx, key, got))
	require.Equal(t, int64(9), got.Status.OpCert.OnChainCounter)
	require.NotEmpty(t, got.Status.OpCert.OnChainCounterPoolID)

	// Repoint at the pool the Secret actually holds keys for. The old counter of
	// 9 would refuse this pool's counter-3 bundle for up to onChainCounterMaxAge
	// if it survived, so it must be discarded at once.
	stub.counter = onchain.Counter{Value: 1, Found: true}
	require.NoError(t, c.Get(ctx, key, got))
	got.Spec.BlockProducer.PoolID = hex.EncodeToString(keys.PoolID)
	require.NoError(t, c.Update(ctx, got))
	reconcile(t, ctx, r, "bp", "poolchange-ns")

	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(
		t,
		int64(1),
		got.Status.OpCert.OnChainCounter,
		"the counter must be re-read for the new pool, not carried over",
	)
	assert.Equal(t, 2, stub.calls, "a pool change must re-read immediately")
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
		"the new pool's bundle must not be judged against the old pool's "+
			"counter",
	)
	assert.NotEmpty(t, keysAnnotation(t, ctx, c, "bp", "poolchange-ns"))

	// The same pool change, but with the previous read lost before it reached
	// status — so only the remembered attempt knows which pool it was of. Status
	// has nothing to compare against, so a gate that consulted status alone would
	// neither re-read nor notice, and would publish the other pool's counter under
	// a False condition reasoned "Observed".
	third, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, got))
	got.Status.OpCert = dingov1alpha1.OpCertStatus{}
	require.NoError(t, c.Status().Update(ctx, got))
	stub.counter = onchain.Counter{Value: 4, Found: true}
	require.NoError(t, c.Get(ctx, key, got))
	got.Spec.BlockProducer.PoolID = hex.EncodeToString(third.PoolID)
	require.NoError(t, c.Update(ctx, got))

	reconcile(t, ctx, r, "bp", "poolchange-ns")
	assert.Equal(
		t,
		3,
		stub.calls,
		"a remembered observation of another pool must be re-read, not reused",
	)
	require.NoError(t, c.Get(ctx, key, got))
	cond := meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Observed", cond.Reason)
	assert.Equal(t, int64(4), got.Status.OpCert.OnChainCounter)
	assert.NotEqual(
		t,
		hex.EncodeToString(keys.PoolID),
		got.Status.OpCert.OnChainCounterPoolID,
		"the recorded pool must track the spec, not the previous read",
	)
}

// A valid bundle at or above the chain's counter must still roll the pod: the
// floor rejects regressions, not rotations.
func TestBlockProducerOnChainCounterAcceptsForwardBundle(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "onchain-ok-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(6, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "onchain-ok-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "onchain-ok-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	r := reconcilerFor(c)
	r.OnChain = &stubOnChain{counter: onchain.Counter{Value: 5, Found: true}}
	reconcile(t, ctx, r, "bp", "onchain-ok-ns")

	assert.NotEmpty(t, keysAnnotation(t, ctx, c, "bp", "onchain-ok-ns"))
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "onchain-ok-ns"},
		got,
	))
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
	)
	assert.Equal(t, int64(6), got.Status.OpCert.OnDiskCounter)
}

// TestBlockProducerOnChainCounterNotQueriedWhenDisabled pins the default: with
// node-to-client off, Dingo binds it to loopback, so the operator must not even
// try — and must not report a failure for a node that was never asked.
func TestBlockProducerOnChainCounterNotQueriedWhenDisabled(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "ntc-off-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(3, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "ntc-off-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "ntc-off-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	require.NoError(t, c.Create(ctx, dn))

	stub := &stubOnChain{counter: onchain.Counter{Value: 9, Found: true}}
	r := reconcilerFor(c)
	r.OnChain = stub
	reconcile(t, ctx, r, "bp", "ntc-off-ns")

	assert.Zero(t, stub.calls, "no query without spec nodeToClient.enabled")
	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "ntc-off-ns"},
		got,
	))
	assert.Zero(t, got.Status.OpCert.OnChainCounter)
	// Reported explicitly rather than by absence: an absent condition reads the
	// same as a feature that was never wired up.
	cond := meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "Disabled", cond.Reason)
	assert.Contains(t, cond.Message, "nodeToClient.enabled")
	// The keys still validate against the on-disk floor, so nothing regresses.
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
	)
}

// TestBlockProducerOnChainCounterPoolNotOnChain covers a normal state that must
// not look like a fault: a pool that has never minted has no counter, and the
// operator falls back to the on-disk floor.
func TestBlockProducerOnChainCounterPoolNotOnChain(t *testing.T) {
	c, ctx := startEnv(t)
	createNamespace(t, ctx, c, "new-pool-ns")

	keys, err := devnet.GeneratePoolKeys(rand.Reader)
	require.NoError(t, err)
	data, err := keys.SecretData(0, 0)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-keys",
			Namespace: "new-pool-ns",
		},
		Data: data,
	}))

	dn := blockProducerNode(
		"bp", "new-pool-ns", "pool-keys", hex.EncodeToString(keys.PoolID),
	)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	require.NoError(t, c.Create(ctx, dn))

	r := reconcilerFor(c)
	r.OnChain = &stubOnChain{counter: onchain.Counter{Found: false}}
	reconcile(t, ctx, r, "bp", "new-pool-ns")
	reconcile(t, ctx, r, "bp", "new-pool-ns")

	got := &dingov1alpha1.DingoNode{}
	require.NoError(t, c.Get(
		ctx,
		types.NamespacedName{Name: "bp", Namespace: "new-pool-ns"},
		got,
	))
	assert.Zero(t, got.Status.OpCert.OnChainCounter)
	cond := meta.FindStatusCondition(got.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, "PoolNotOnChain", cond.Reason)
	assert.True(
		t,
		meta.IsStatusConditionTrue(got.Status.Conditions, condKeysValid),
		"counter 0 must still be accepted when the chain knows nothing",
	)
	assert.NotEmpty(t, keysAnnotation(t, ctx, c, "bp", "new-pool-ns"))
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
