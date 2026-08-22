//go:build e2e

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

// Package e2e runs the operator against a real cluster and a throwaway
// single-pool Cardano devnet. It only builds under the "e2e" tag; `make e2e`
// brings up the k3d cluster (hack/e2e/k3d-up.sh), installs the CRDs, RBAC and
// the in-cluster manager, and points KUBECONFIG at a dedicated kubeconfig.
//
// # Failing the test from a helper
//
// Most helpers below fail the test themselves (`require`, `t.Fatalf`). That is
// only legal on the test's own goroutine, so *do not call them inside a testify
// require.Eventually/assert.Eventually condition*: testify runs those
// conditions on a fresh goroutine, where t.FailNow's runtime.Goexit is invalid
// and — because Eventually only re-arms its ticker when the condition channel
// produces a value — silently wedges the poll loop for the whole timeout,
// reporting the condition as never satisfied instead of the real error.
//
// Use (*harness).waitFor instead: it polls on the caller's goroutine, so
// anything is safe inside it. Where a condition needs raw access, the
// non-fataling variants are getNodeErr, podOrNil, tryScrapeMetrics and podLogs.
package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// operatorNamespace is where hack/e2e/k3d-up.sh installs the manager.
	operatorNamespace  = "dingo-operator-system"
	operatorDeployment = "dingo-operator"
	// operatorServiceAccount is the operator's ServiceAccount. It happens to
	// share its name with the Deployment, but the two are distinct identities:
	// RBAC subjects and SubjectAccessReview users need this one, so keep it
	// separate rather than reusing operatorDeployment and relying on them
	// staying equal.
	operatorServiceAccount = "dingo-operator"
	// keysReaderName is the Role/RoleBinding name the chart uses for the
	// namespaced keys-reader grant.
	keysReaderName = "dingo-operator-keys-reader"

	// nodeName is the DingoNode (and StatefulSet) name; its only pod is
	// therefore nodeName+"-0".
	nodeName      = "bp"
	configMapName = "devnet-config"
	// keysSecretRef is the name of a Kubernetes Secret object, not a
	// credential; gosec flags the identifier, not the value.
	keysSecretRef = "devnet-keys" //nolint:gosec // G101: object name

	// nodeContainerName mirrors the unexported containerName in
	// internal/resources.
	nodeContainerName = "dingo"

	// metricsPort is Dingo's cardano-node-compatible Prometheus port.
	metricsPort = 12798

	// slotsPerKESPeriod and maxKESEvolutions must match the rendered Shelley
	// genesis (devnet.DefaultParams), or Dingo rejects the opcert at startup.
	slotsPerKESPeriod = 120
	maxKESEvolutions  = 62

	// genesisLeadTime is how far in the future a generated devnet's systemStart
	// is placed. It has to cover pod scheduling, the image being unpacked and
	// Dingo's own startup, or the node begins life behind the slot clock.
	// Measured: ~84s elapses between applyDevNet and Dingo reaching its slot
	// clock, so the brief's 45s would have started the node behind slot 0.
	genesisLeadTime = 90 * time.Second

	// defaultDingoImage is the Dingo build the block producer runs. Pinned to a
	// concrete release rather than a floating tag: with ":main" plus a
	// pull-only-if-absent side-load, a machine that pulled once would keep
	// testing a stale build forever, while CI would silently track upstream and
	// could go red from a change nobody here made. Override with
	// E2E_DINGO_IMAGE (and DINGO_IMAGE for hack/e2e/k3d-up.sh, which side-loads
	// it) to try another build.
	defaultDingoImage = "ghcr.io/blinklabs-io/dingo:0.69.0"

	// defaultClusterName is the k3d cluster hack/e2e/k3d-up.sh creates; k3d
	// names its kubeconfig context "k3d-<cluster>". Overridable via
	// CLUSTER_NAME, the same variable the script honours.
	defaultClusterName = "dingo-operator-e2e"
)

// Deadline budget, in three layers: a per-step deadline on each wait below, a
// testTimeout context around each test, and `go test -timeout` in the Makefile
// around the suite. Each step budget is roughly 3x its measured duration.
//
//	podReadyTimeout    3m   (measured ~60s)
//	forgeTimeout       3m   (measured ~40s after pod ready)
//	kesStatusTimeout   3m   (chain start + one 2m operator requeue; ~80s)
//	kesPeriodTimeout   4m   (slot 120 at systemStart+2m, + a requeue; ~80s)
//	keysDeliveryTimeout 4m  (the operator has no Secret watch, so a delivered
//	                         bundle is seen on the next 2m requeue)
//	podRollTimeout     3m   (StatefulSet replaces the pod; measured ~60s)
//	onChainCounterTimeout 8m (a rate limit, not slowness: the operator's first
//	                          node-to-client dial always fails and the next is
//	                          5m later, plus up to a 2m requeue; ~4m after
//	                          forging)
//
// These are deliberately *not* summed to size testTimeout. An earlier revision
// did, but the rotation tests chain eight or nine waits each, and 3x margin on
// every one of them sums past 25m per test — which would force either a 60m+
// outer timeout or step budgets too tight to be reliable. Summing is also
// unnecessary: every long wait goes through waitFor, which names the step it
// was waiting for and prints diagnostics whether it was its own deadline or the
// enclosing context that expired. All that is lost when the context wins is
// that the printed "timed out after ..." figure is the step budget rather than
// the elapsed time — cross-check it against the test's own reported duration.
//
// testTimeout is instead sized against measured whole-test durations, on k3d
// with the pinned Dingo image:
//
//	TestBlockProducerForges                        260s
//	TestKeysReaderRBACIsNamespaceScoped              0s  (SubjectAccessReview)
//	TestFinishNamespaceRetention                     0s  (fake client)
//	TestOnChainOpCertCounterObserved               ~400s (projected; see below)
//	TestAssistedRotationRollsPodAndResumesForging   270s
//	TestAssistedRotationRejectsCounterRegression    261s
//	                                               -----
//	go test                                        ~1190s (~20m)
//
// against `go test -timeout 45m` in the Makefile; whole `make e2e`, including
// the image build, k3d bring-up and teardown, adds ~2m to that.
//
// The on-chain figure is *projected*, not measured, and the distinction is
// deliberate: that test has never yet completed successfully — it fails on an
// upstream decode bug in gouroboros' DebugChainDepState client (see the test's
// own comment), burning its full 8m step budget for a 575s run. 400s is what
// it costs when the read succeeds: ~150s to a forging node plus the operator's
// 5m dial rate limit and up to one 2m requeue. Re-measure when the upstream
// fix lands; if the real figure is materially higher, the arithmetic below
// moves with it.
//
// The tests run sequentially, so what the outer timeout has to cover is the
// case that matters most for diagnosis: any one test burning its whole 15m
// context while the others run at measured pace still reports that test's own
// failure rather than dying on the outer timeout. That is worst when the
// shortest test is the one that wedges: 15m + 20m = ~35m. This is why 30m no
// longer suffices — the other tests now measure ~20m rather than ~9m, so a
// single wedged test alone would exceed the old budget.
//
// Two shortest tests wedging take 15m + 15m + 20m = ~50m and can exceed the
// 45m outer timeout. The first has already printed its failure and diagnostics,
// and the panic dump identifies the second. Buying full attribution for two
// independent wedges would require a 55m+ test timeout for a ~20m suite.
//
// The CI job's timeout-minutes (55) clears even a full 45m `go test` plus
// bring-up, teardown and diagnostics (~4m measured, including k3d install).
const (
	pollInterval     = 5 * time.Second
	podReadyTimeout  = 3 * time.Minute
	forgeTimeout     = 3 * time.Minute
	kesStatusTimeout = 3 * time.Minute
	kesPeriodTimeout = 4 * time.Minute

	// keysDeliveryTimeout bounds the operator reacting to a changed keys
	// Secret. The reconciler holds no Secret informer by design (see the
	// APIReader field on DingoNodeReconciler), so it only sees a delivered
	// bundle on its next requeueInterval (2m) pass.
	keysDeliveryTimeout = 4 * time.Minute

	// podRollTimeout bounds the StatefulSet controller replacing the pod once
	// the pod template's keys-checksum annotation changes.
	podRollTimeout = 3 * time.Minute

	// onChainCounterTimeout bounds the operator observing the on-chain opcert
	// counter over node-to-client. It is the largest budget in the suite, and
	// the reason is a rate limit rather than slowness: the operator dials at
	// most once per node per onChainCounterRefreshInterval (5m), keyed on the
	// last *attempt*, and the first attempt always fails — it happens on the
	// very first reconcile, before that same reconcile has created the node's
	// Service (NodeNotReady). So no successful read is possible before 5m after
	// the DingoNode is created, and the read then waits for the next reconcile,
	// up to one requeueInterval (2m) later. 8m is those two plus a minute.
	//
	// Against a devnet that is forging within ~2.5m of creation, the wait
	// therefore resolves ~4m after waitForged returns.
	onChainCounterTimeout = 8 * time.Minute

	// operatorReadyTimeout covers cluster setup rather than test progress, so it
	// sits outside the budget above: newHarness runs before the test's own
	// deadline arithmetic matters.
	operatorReadyTimeout = 3 * time.Minute

	// scrapeRetryTimeout bounds scrapeMetrics' internal retry. It nests inside
	// whichever budget above its caller is under, so it stays small.
	scrapeRetryTimeout = time.Minute

	// testTimeout is the per-test context budget. See the arithmetic above.
	testTimeout = 15 * time.Minute
)

// expectedContext returns the only kubeconfig context this suite may run
// against. It creates and deletes namespaces, and this machine also has a real
// k3s cluster, so pointing at anything else must be a hard stop rather than a
// surprise.
func expectedContext() string {
	name := os.Getenv("CLUSTER_NAME")
	if name == "" {
		name = defaultClusterName
	}
	return "k3d-" + name
}

// harness holds the cluster plumbing shared by the e2e tests: a
// controller-runtime client, a typed clientset (for pod logs and
// port-forwarding) and a per-run namespace.
type harness struct {
	t         *testing.T
	cfg       *rest.Config
	client    client.Client
	clientset *kubernetes.Clientset
	namespace string
}

// newHarness builds clients from KUBECONFIG, waits for the in-cluster manager
// to be Available and creates a fresh namespace for the calling test.
func newHarness(t *testing.T) *harness {
	t.Helper()

	kubeconfig := os.Getenv("KUBECONFIG")
	require.NotEmpty(t, kubeconfig,
		"KUBECONFIG must point at the k3d e2e kubeconfig "+
			"(.e2e/kubeconfig); run this suite via `make e2e`")

	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{},
	)
	raw, err := loader.RawConfig()
	require.NoError(t, err, "load kubeconfig %s", kubeconfig)
	require.Equal(t, expectedContext(), raw.CurrentContext,
		"refusing to run: kubeconfig %s selects an unexpected context; "+
			"this suite creates and deletes namespaces and must only ever "+
			"target the throwaway k3d e2e cluster", kubeconfig)

	cfg, err := loader.ClientConfig()
	require.NoError(t, err, "build rest config from %s", kubeconfig)

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err, "build controller-runtime client")
	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err, "build clientset")

	h := &harness{
		t:         t,
		cfg:       cfg,
		client:    c,
		clientset: clientset,
		namespace: "e2e-" + randomSuffix(t),
	}

	h.waitOperatorAvailable(t.Context())
	h.createNamespace(t.Context())
	return h
}

// randomSuffix returns 8 hex characters for a unique namespace name.
func randomSuffix(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 4)
	_, err := rand.Read(buf)
	require.NoError(t, err, "read random namespace suffix")
	return hex.EncodeToString(buf)
}

// waitFor polls cond until it reports true, failing the test with what and the
// last error on timeout. Unlike testify's Eventually it runs cond on the
// caller's goroutine, so helpers that fail the test are safe inside it, and it
// treats an error from cond as "not yet" rather than aborting the whole poll.
func (h *harness) waitFor(
	ctx context.Context,
	timeout time.Duration,
	what string,
	cond func(context.Context) (bool, error),
) {
	h.t.Helper()
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			ok, err := cond(ctx)
			if err != nil {
				lastErr = err
				// Swallowing the error is the contract. A condition that
				// cannot read the cluster yet (pod not up, no port-forward,
				// metrics endpoint refusing) is "not ready", not a failure;
				// returning it would abort the whole poll on the first
				// transient. It is kept in lastErr and reported on timeout.
				return false, nil //nolint:nilerr // deliberate: see above
			}
			return ok, nil
		})
	if err != nil {
		h.t.Fatalf(
			"timed out after %s waiting for %s (%v; last error: %v)\n%s",
			timeout, what, err, lastErr, h.diagnostics(ctx))
	}
}

// waitOperatorAvailable blocks until the in-cluster manager Deployment reports
// Available. Nothing may generate a devnet genesis before this: systemStart is
// only genesisLeadTime in the future, so a manager that is still starting eats
// the whole budget.
func (h *harness) waitOperatorAvailable(ctx context.Context) {
	h.t.Helper()
	key := types.NamespacedName{
		Name:      operatorDeployment,
		Namespace: operatorNamespace,
	}
	err := wait.PollUntilContextTimeout(
		ctx, 2*time.Second, operatorReadyTimeout, true,
		func(ctx context.Context) (bool, error) {
			dep := &appsv1.Deployment{}
			if err := h.client.Get(ctx, key, dep); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			for _, cond := range dep.Status.Conditions {
				if cond.Type == appsv1.DeploymentAvailable {
					return cond.Status == corev1.ConditionTrue, nil
				}
			}
			return false, nil
		})
	require.NoError(h.t, err,
		"deployment %s/%s never became Available; is `make e2e` "+
			"(hack/e2e/k3d-up.sh) what brought this cluster up?",
		operatorNamespace, operatorDeployment)
}

// createNamespace creates the per-run namespace and registers its teardown.
func (h *harness) createNamespace(ctx context.Context) {
	h.t.Helper()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: h.namespace},
	}
	require.NoError(h.t, h.client.Create(ctx, ns),
		"create namespace %s", h.namespace)
	h.t.Logf("namespace %s created", h.namespace)
	h.createKeysReaderRBAC(ctx)

	h.t.Cleanup(func() {
		// A fresh context: t.Context() is already cancelled during cleanup.
		delCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		kept, err := finishNamespace(delCtx, h.client, ns,
			os.Getenv("E2E_KEEP_UP") == "1", h.t.Failed())
		switch {
		case err != nil:
			h.t.Logf("delete namespace %s: %v", h.namespace, err)
		case kept != "":
			h.t.Logf("%s — leaving namespace %s in place", kept, h.namespace)
		}
	})
}

// createKeysReaderRBAC grants the operator get-on-Secrets in this test's
// namespace only, mirroring what the Helm chart renders when
// rbac.keySecretsNamespaces is set: a Role in the workload namespace plus a
// RoleBinding for the operator's ServiceAccount, which lives elsewhere.
//
// The suite deliberately installs no cluster-wide Secret grant (see
// test/e2e/manifests/manager.yaml), so this is the operator's *only* route to
// the keys Secret. Every test that observes a keys-checksum annotation is
// therefore also evidence that the narrow grant is sufficient — the operator
// reads that Secret through an uncached client, and the annotation is only
// stamped when the read succeeds.
func (h *harness) createKeysReaderRBAC(ctx context.Context) {
	h.t.Helper()

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keysReaderName,
			Namespace: h.namespace,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"secrets"},
			Verbs:     []string{"get"},
		}},
	}
	require.NoError(h.t, h.client.Create(ctx, role),
		"create keys-reader Role in %s", h.namespace)

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keysReaderName,
			Namespace: h.namespace,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      operatorServiceAccount,
			Namespace: operatorNamespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     keysReaderName,
		},
	}
	require.NoError(h.t, h.client.Create(ctx, binding),
		"create keys-reader RoleBinding in %s", h.namespace)
	h.t.Logf("keys-reader RBAC scoped to namespace %s", h.namespace)
}

// operatorServiceAccountUser is the SA username the API server authorizes the
// operator as, for SubjectAccessReview.
func operatorServiceAccountUser() string {
	return "system:serviceaccount:" + operatorNamespace + ":" +
		operatorServiceAccount
}

// canGet asks the API server whether the operator may "get" a resource in a
// namespace. This is the authorizer's own answer, not an inference from reading
// the manifests back.
func (h *harness) canGet(
	ctx context.Context,
	namespace, group, resource string,
) bool {
	h.t.Helper()
	review := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: operatorServiceAccountUser(),
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: namespace,
				Verb:      "get",
				Group:     group,
				Resource:  resource,
			},
		},
	}
	require.NoError(h.t, h.client.Create(ctx, review),
		"SubjectAccessReview for %s in namespace %s", resource, namespace)
	return review.Status.Allowed
}

// canGetSecrets reports whether the operator may read Secrets in a namespace.
func (h *harness) canGetSecrets(ctx context.Context, namespace string) bool {
	h.t.Helper()
	return h.canGet(ctx, namespace, "", "secrets")
}

// canGetDingoNodes reports whether the operator may read DingoNodes in a
// namespace. That access is cluster-wide by design, so it doubles as a control:
// if this comes back denied too, the review is not measuring what it claims.
func (h *harness) canGetDingoNodes(
	ctx context.Context,
	namespace string,
) bool {
	h.t.Helper()
	return h.canGet(
		ctx,
		namespace,
		dingov1alpha1.GroupVersion.Group,
		"dingonodes",
	)
}

// applyDevNet generates a throwaway single-pool devnet and writes its config
// bundle and key material into the namespace. It is deliberately called at test
// time rather than in newHarness: the genesis systemStart is only
// genesisLeadTime in the future, so generating it any earlier risks slot 0
// passing before the node is running.
func (h *harness) applyDevNet(ctx context.Context) *devnet.DevNet {
	h.t.Helper()

	// Generate requires a whole-second systemStart.
	systemStart := time.Now().Add(genesisLeadTime).UTC().Truncate(time.Second)
	dn, err := devnet.Generate(rand.Reader, systemStart)
	require.NoError(h.t, err, "generate devnet")
	h.t.Logf("devnet generated: pool %s, systemStart %s",
		hex.EncodeToString(dn.Keys.PoolID), systemStart.Format(time.RFC3339))

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: h.namespace,
		},
		Data: dn.ConfigFiles,
	}
	require.NoError(h.t, h.client.Create(ctx, cm),
		"create config bundle ConfigMap %s", configMapName)

	data, err := dn.Keys.SecretData(0, 0)
	require.NoError(h.t, err, "render keys secret data")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keysSecretRef,
			Namespace: h.namespace,
		},
		Data: data,
	}
	require.NoError(h.t, h.client.Create(ctx, secret),
		"create keys Secret %s", keysSecretRef)

	return dn
}

// nodeOption customises the DingoNode applyDingoNode builds, before it is
// created. Options exist so a test can opt into a feature that is off by
// default without every other test's node changing shape: applyDingoNode with
// no options renders exactly the spec it always did.
//
// It returns an error rather than mutating blindly so an option that cannot
// apply — a spec whose BlockProducer is missing, say — fails the test loudly
// instead of silently doing nothing and leaving the test to time out against
// the feature it thought it had enabled.
type nodeOption func(*dingov1alpha1.DingoNodeSpec) error

// withNodeToClient enables the node's node-to-client listener, which is what
// lets the operator read the authoritative on-chain opcert counter. Off by
// default in the CRD (Dingo binds NtC to loopback unless told otherwise), and
// on its own it is only half the wiring: the client also needs the
// resources.NodeToClientAccessLabel label on its pod and — across namespaces,
// which is always the case here — on its namespace. See
// TestOnChainOpCertCounterObserved.
func withNodeToClient() nodeOption {
	return func(spec *dingov1alpha1.DingoNodeSpec) error {
		if spec.BlockProducer == nil {
			return errors.New(
				"withNodeToClient needs spec.blockProducer to be set")
		}
		spec.BlockProducer.NodeToClient.Enabled = true
		return nil
	}
}

// applyDingoNode creates the block-producer DingoNode for the generated devnet.
func (h *harness) applyDingoNode(
	ctx context.Context,
	dn *devnet.DevNet,
	opts ...nodeOption,
) {
	h.t.Helper()

	// E2E_DINGO_IMAGE wins, then DINGO_IMAGE, then the pinned default.
	// hack/e2e/k3d-up.sh reads DINGO_IMAGE to decide what to side-load, so
	// honouring it here means one variable is enough to move both. Setting only
	// one used to leave the pod on the pinned default while the script imported
	// something else — a silent no-op that looked like a working override.
	envVar := "E2E_DINGO_IMAGE"
	image := os.Getenv(envVar)
	if image == "" {
		envVar = "DINGO_IMAGE"
		image = os.Getenv(envVar)
	}
	if image == "" {
		envVar, image = "E2E_DINGO_IMAGE", defaultDingoImage
	}
	// Split on the last colon so a registry host:port is not mistaken for a tag.
	sep := strings.LastIndex(image, ":")
	require.Greater(h.t, sep, strings.LastIndex(image, "/"),
		"%s %q must be repository:tag", envVar, image)
	repo, tag := image[:sep], image[sep+1:]

	node := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: h.namespace,
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:         dingov1alpha1.RoleBlockProducer,
			Network:      "custom",
			NetworkMagic: new(int64(42)),
			ConfigRef:    configMapName,
			// A config bundle means a custom genesis, which the controller
			// requires Mithril be switched off for (there is no aggregator to
			// bootstrap a throwaway devnet from).
			Mithril: dingov1alpha1.MithrilSpec{Enabled: new(false)},
			Image: dingov1alpha1.ImageSpec{
				Repository: repo,
				Tag:        tag,
				// The image is side-loaded by hack/e2e/k3d-up.sh.
				PullPolicy: corev1.PullIfNotPresent,
			},
			// A solo devnet has no peers, but Dingo still needs *a* topology:
			// with none set the operator omits CARDANO_TOPOLOGY and Dingo falls
			// back to looking up built-in bootstrap peers for the network name,
			// which fails for "custom". useLedgerAfterSlot -1 renders an empty
			// topology that never consults ledger peers.
			Topology: dingov1alpha1.TopologySpec{
				UseLedgerAfterSlot: new(int64(-1)),
			},
			BlockProducer: &dingov1alpha1.BlockProducerSpec{
				PoolID:            hex.EncodeToString(dn.Keys.PoolID),
				SlotsPerKESPeriod: slotsPerKESPeriod,
				MaxKESEvolutions:  maxKESEvolutions,
				Keys: dingov1alpha1.KeysSpec{
					SecretRef: keysSecretRef,
				},
				Rotation: dingov1alpha1.RotationSpec{
					Mode: dingov1alpha1.RotationModeAssisted,
				},
				HA: dingov1alpha1.HASpec{
					Strategy: dingov1alpha1.HASingleActive,
				},
			},
		},
	}
	for _, opt := range opts {
		require.NoError(h.t, opt(&node.Spec),
			"apply a DingoNode option to %s/%s", h.namespace, nodeName)
	}
	require.NoError(h.t, h.client.Create(ctx, node),
		"create DingoNode %s/%s", h.namespace, nodeName)
}

// getNodeErr returns the current DingoNode without failing the test. Use this
// inside polling conditions.
func (h *harness) getNodeErr(
	ctx context.Context,
) (*dingov1alpha1.DingoNode, error) {
	node := &dingov1alpha1.DingoNode{}
	if err := h.client.Get(ctx, h.nodeKey(), node); err != nil {
		return nil, err
	}
	return node, nil
}

// getNode returns the current DingoNode.
// Fails the test: call it from the test goroutine or inside waitFor, never
// inside a testify Eventually condition (see the package comment).
func (h *harness) getNode(ctx context.Context) *dingov1alpha1.DingoNode {
	h.t.Helper()
	node, err := h.getNodeErr(ctx)
	require.NoError(h.t, err, "get DingoNode %s/%s", h.namespace, nodeName)
	return node
}

func (h *harness) nodeKey() types.NamespacedName {
	return types.NamespacedName{Name: nodeName, Namespace: h.namespace}
}

func (h *harness) podKey() types.NamespacedName {
	return types.NamespacedName{
		Name:      nodeName + "-0",
		Namespace: h.namespace,
	}
}

// podOrNil returns the node's pod, or (nil, nil) when it does not exist. It
// exists so callers can watch for the pod being replaced without treating the
// gap between deletion and recreation as an error.
func (h *harness) podOrNil(ctx context.Context) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := h.client.Get(ctx, h.podKey(), pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return pod, nil
}

// waitPodReady blocks until the node's pod reports Ready, returning a snapshot
// of it. The snapshot is a copy held by value, so the result is never nil even
// if waitFor's failure path is ever changed to return.
// Fails the test: see the package comment.
func (h *harness) waitPodReady(ctx context.Context) *corev1.Pod {
	h.t.Helper()
	var ready corev1.Pod
	h.waitFor(ctx, podReadyTimeout,
		fmt.Sprintf("pod %s to become Ready", h.podKey().Name),
		func(ctx context.Context) (bool, error) {
			pod, err := h.podOrNil(ctx)
			if err != nil || pod == nil {
				return false, err
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady &&
					cond.Status == corev1.ConditionTrue {
					ready = *pod
					return true, nil
				}
			}
			return false, nil
		})
	return &ready
}

// updateKeysSecret replaces the contents of the mounted keys Secret, which is
// how an externally-delivered KES key / opcert reaches the operator.
// Fails the test: see the package comment.
func (h *harness) updateKeysSecret(
	ctx context.Context,
	data map[string][]byte,
) {
	h.t.Helper()
	secret := &corev1.Secret{}
	key := types.NamespacedName{
		Name:      keysSecretRef,
		Namespace: h.namespace,
	}
	require.NoError(h.t, h.client.Get(ctx, key, secret),
		"get keys Secret %s", keysSecretRef)
	secret.Data = data
	require.NoError(h.t, h.client.Update(ctx, secret),
		"update keys Secret %s", keysSecretRef)
}

// scrapeMetrics returns the node's forge/KES status. A single attempt can lose
// a race with a rollout or with port-forward setup, so it retries for
// scrapeRetryTimeout before giving up. Like waitPodReady it snapshots by value,
// so the result is never nil even if waitFor's failure path is ever changed to
// return.
// Fails the test: see the package comment — inside a polling condition use
// tryScrapeMetrics instead.
func (h *harness) scrapeMetrics(ctx context.Context) *forgestatus.Status {
	h.t.Helper()
	var st forgestatus.Status
	h.waitFor(ctx, scrapeRetryTimeout,
		h.podKey().Name+" metrics to be readable",
		func(ctx context.Context) (bool, error) {
			got, err := h.tryScrapeMetrics(ctx)
			if err != nil {
				return false, err
			}
			if got == nil {
				return false, errors.New("metrics scrape returned no status")
			}
			st = *got
			return true, nil
		})
	return &st
}

// currentKESPeriod returns the KES period the node believes it is in, for
// minting an opcert dated to a period the node will accept. It waits for the
// node to actually be publishing KES metrics rather than failing on the first
// scrape, because callers reach for it right after a pod roll.
//
// It returns int64, the type the metrics scraper reports, so the only place
// that has to narrow a period to the uint64 the opcert issuer takes is
// deliverOpCert.
// Fails the test: see the package comment.
func (h *harness) currentKESPeriod(ctx context.Context) int64 {
	h.t.Helper()
	var period int64
	h.waitFor(ctx, scrapeRetryTimeout,
		"the node to publish KES metrics",
		func(ctx context.Context) (bool, error) {
			st, err := h.tryScrapeMetrics(ctx)
			if err != nil {
				return false, err
			}
			if !st.HasKESData {
				return false, nil
			}
			if st.CurrentKESPeriod < 0 {
				return false, fmt.Errorf(
					"node reported a negative KES period %d",
					st.CurrentKESPeriod)
			}
			period = st.CurrentKESPeriod
			return true, nil
		})
	return period
}

// waitForged blocks until the node reports at least min forged blocks. This is
// the ground truth for "this pool actually produces blocks": the metric is
// Dingo's own Forge_forged_int counter, reset on every process start.
// Fails the test: see the package comment.
func (h *harness) waitForged(ctx context.Context, min int64) {
	h.t.Helper()
	var last int64
	h.waitFor(ctx, forgeTimeout,
		fmt.Sprintf("the node to forge %d block(s)", min),
		func(ctx context.Context) (bool, error) {
			st, err := h.tryScrapeMetrics(ctx)
			if err != nil {
				// The pod may be restarting, or the port-forward may race a
				// rollout; treated as "not yet" and surfaced on timeout.
				return false, err
			}
			last = st.ForgedBlocks
			return st.ForgedBlocks >= min, nil
		})
	h.t.Logf("node forged %d block(s) (wanted >= %d)", last, min)
}

// tryScrapeMetrics port-forwards to the node's metrics port and parses the
// exposition with the operator's own parser. Non-fataling: safe inside any
// polling condition.
func (h *harness) tryScrapeMetrics(
	ctx context.Context,
) (*forgestatus.Status, error) {
	var st *forgestatus.Status
	err := h.withPortForward(ctx, metricsPort, func(local int) error {
		url := fmt.Sprintf("http://127.0.0.1:%d/metrics", local)
		req, err := http.NewRequestWithContext(
			ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build metrics request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("get %s: %w", url, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("metrics returned status %d", resp.StatusCode)
		}
		st, err = forgestatus.Parse(resp.Body)
		return err
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// withPortForward opens an ephemeral local port forwarded to remotePort on the
// node's pod, invokes fn with it, then tears the tunnel down. kubectl exec is
// not an option: the Dingo image carries no shell.
func (h *harness) withPortForward(
	ctx context.Context,
	remotePort int,
	fn func(local int) error,
) error {
	req := h.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(h.namespace).
		Name(h.podKey().Name).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(h.cfg)
	if err != nil {
		return fmt.Errorf("build spdy round tripper: %w", err)
	}
	dialer := spdy.NewDialer(
		upgrader,
		&http.Client{Transport: transport},
		http.MethodPost,
		req.URL(),
	)

	stop := make(chan struct{})
	ready := make(chan struct{})
	fw, err := portforward.New(
		dialer,
		// Local port 0 lets the kernel pick a free one, so concurrent tests
		// never collide.
		[]string{fmt.Sprintf("0:%d", remotePort)},
		stop,
		ready,
		io.Discard,
		io.Discard,
	)
	if err != nil {
		return fmt.Errorf("build port forwarder: %w", err)
	}

	forwardErr := make(chan error, 1)
	go func() { forwardErr <- fw.ForwardPorts() }()
	defer close(stop)

	select {
	case <-ready:
	case err := <-forwardErr:
		return fmt.Errorf("port-forward to %s: %w", h.podKey().Name, err)
	case <-ctx.Done():
		return ctx.Err()
	}

	ports, err := fw.GetPorts()
	if err != nil {
		return fmt.Errorf("resolve forwarded ports: %w", err)
	}
	if len(ports) == 0 {
		return errors.New("port-forward reported no forwarded ports")
	}
	return fn(int(ports[0].Local))
}

// podLogs returns the node container's logs. The read error is returned rather
// than folded into the string, so a caller asserting on log contents can never
// mistake a failed read for a log that genuinely lacks the line.
// Non-fataling: safe inside any polling condition.
func (h *harness) podLogs(ctx context.Context) (string, error) {
	stream, err := h.clientset.CoreV1().
		Pods(h.namespace).
		GetLogs(h.podKey().Name, &corev1.PodLogOptions{
			Container: nodeContainerName,
		}).
		Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("open logs for %s: %w", h.podKey().Name, err)
	}
	defer func() { _ = stream.Close() }()
	out, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("read logs for %s: %w", h.podKey().Name, err)
	}
	return string(out), nil
}

// diagnostics renders a compact snapshot for failure messages: the DingoNode
// status, the pod's phase/conditions/container states and the tail of the node
// log. Reproduce by hand with `kubectl -n <ns> describe pod bp-0`.
func (h *harness) diagnostics(ctx context.Context) string {
	// Cleanup contexts may already be cancelled; diagnostics must still work.
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	var b strings.Builder
	fmt.Fprintf(&b, "--- diagnostics (namespace %s) ---\n", h.namespace)

	if node, err := h.getNodeErr(ctx); err != nil {
		fmt.Fprintf(&b, "DingoNode: %v\n", err)
	} else {
		fmt.Fprintf(&b, "DingoNode phase=%q kes=%+v opcert=%+v\n",
			node.Status.Phase, node.Status.KES, node.Status.OpCert)
		for _, cond := range node.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s reason=%s msg=%s\n",
				cond.Type, cond.Status, cond.Reason, cond.Message)
		}
	}

	pod, err := h.podOrNil(ctx)
	switch {
	case err != nil:
		fmt.Fprintf(&b, "pod: %v\n", err)
	case pod == nil:
		fmt.Fprintf(&b, "pod %s: absent\n", h.podKey().Name)
	default:
		fmt.Fprintf(&b, "pod %s phase=%s\n", pod.Name, pod.Status.Phase)
		for _, cond := range pod.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s reason=%s msg=%s\n",
				cond.Type, cond.Status, cond.Reason, cond.Message)
		}
		statuses := append(
			append([]corev1.ContainerStatus{},
				pod.Status.InitContainerStatuses...),
			pod.Status.ContainerStatuses...)
		for _, cs := range statuses {
			fmt.Fprintf(&b, "  container %s ready=%t restarts=%d state=%+v\n",
				cs.Name, cs.Ready, cs.RestartCount, cs.State)
		}
		if logs, err := h.podLogs(ctx); err != nil {
			fmt.Fprintf(&b, "--- node log unavailable: %v ---\n", err)
		} else {
			fmt.Fprintf(&b, "--- node log (tail) ---\n%s\n", tail(logs, 40))
		}
	}
	return b.String()
}

// tail returns at most n trailing lines of s.
func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// Status condition the operator sets for delivered block-producer key
// material, and the two reasons it uses.
const (
	condKeysValid        = "KeysValid"
	condDegraded         = "Degraded"
	reasonOpCertAccepted = "OpCertAccepted"
	reasonOpCertRejected = "OpCertRejected"
)

// The on-chain opcert counter condition and the reasons this suite names.
// condOnChainCounter mirrors the controller's own constant of the same name.
//
// Only reasonOnChainObserved means the operator got an answer. Every other
// reason the controller can set — QueryFailed, NodeNotReady, PoolNotOnChain,
// PoolIDUnset, InvalidPoolID, UnknownNetworkMagic, Disabled — is a flavour of
// "no counter", so asserting the condition's *reason* rather than its mere
// presence is what keeps TestOnChainOpCertCounterObserved from passing while
// the node-to-client protocol is broken.
const (
	condOnChainCounter    = "OnChainCounterAvailable"
	reasonOnChainObserved = "Observed"
	reasonOnChainDisabled = "Disabled"
)

// waitEvent blocks until the operator has recorded an events.k8s.io/v1 Event
// with the given type and reason against the DingoNode, and returns its note.
//
// envtest covers the reject path with a FakeRecorder, which proves the call is
// made but nothing about delivery. This is the only place that exercises the
// real chain — the manager's EventBroadcaster, the events.k8s.io API, and the
// operator's RBAC grant on it. A dropped grant would make the operator silent
// about refusals in exactly the situation an SRE goes looking for one, and no
// unit test can see it.
// Fails the test: see the package comment.
func (h *harness) waitEvent(
	ctx context.Context,
	eventType, reason string,
) string {
	h.t.Helper()
	var note string
	h.waitFor(ctx, keysDeliveryTimeout,
		fmt.Sprintf("a %s/%s Event on the DingoNode", eventType, reason),
		func(ctx context.Context) (bool, error) {
			list, err := h.clientset.EventsV1().
				Events(h.namespace).
				List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for _, ev := range list.Items {
				if ev.Type != eventType || ev.Reason != reason {
					continue
				}
				if ev.Regarding.Kind != "DingoNode" ||
					ev.Regarding.Name != nodeName {
					continue
				}
				note = ev.Note
				return true, nil
			}
			return false, nil
		})
	return note
}

// stsKeysChecksum returns the keys-checksum annotation on the node's
// StatefulSet pod template — the field whose value decides whether delivered
// key material rolls the pod. A missing StatefulSet or annotation yields "".
// Non-fataling: safe inside any polling condition.
func (h *harness) stsKeysChecksum(ctx context.Context) (string, error) {
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: nodeName, Namespace: h.namespace}
	if err := h.client.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get statefulset %s: %w", nodeName, err)
	}
	return sts.Spec.Template.Annotations[resources.KeysChecksumAnnotation], nil
}

// conditionIs reports whether the named status condition has both the given
// status and reason.
func conditionIs(
	dn *dingov1alpha1.DingoNode,
	condType string,
	status metav1.ConditionStatus,
	reason string,
) bool {
	cond := meta.FindStatusCondition(dn.Status.Conditions, condType)
	return cond != nil && cond.Status == status && cond.Reason == reason
}
