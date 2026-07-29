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

package e2e

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// finishNamespace applies the namespace-retention matrix at the end of a test.
// It returns the reason the namespace was kept, or "" when it was deleted.
//
//	keepUp  failed  outcome
//	false   false   deleted
//	false   true    retained for post-mortem diagnostics
//	true    any     retained (E2E_KEEP_UP=1)
//
// A failing test's namespace is left behind because that is the only thing
// hack/e2e/collect-diagnostics.sh can work with: the pod logs, events and
// describe output it captures exist only while the namespace does. Passing
// tests clean up immediately so a multi-test run doesn't accumulate namespaces
// across the suite.
//
// This lives in an untagged file, unlike the rest of the suite, purely so the
// matrix stays cheap to guard: namespace_test.go exercises it against a fake
// client in milliseconds under `make test`, rather than only under the
// ~15-minute, k3d-and-Docker-requiring `make e2e`. Getting the matrix backwards
// is silent in both directions — a leaked namespace per test, or destroyed
// post-mortem evidence for exactly the tests that were already failing.
//
// Both callers — harness_test.go's createNamespace and namespace_test.go — are
// test files, and .golangci.yml sets run.tests=false, so the unused linter sees
// neither and reports this as dead code. Hence the suppression below.
//
//nolint:unused // called only from _test.go files; see above
func finishNamespace(
	ctx context.Context,
	c client.Client,
	ns *corev1.Namespace,
	keepUp, failed bool,
) (string, error) {
	switch {
	case keepUp:
		return "E2E_KEEP_UP=1", nil
	case failed:
		return "test failed", nil
	}
	if err := c.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}
	return "", nil
}
