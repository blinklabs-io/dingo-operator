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

package e2e

import (
	"context"
	"fmt"
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestAPIListenersThroughPrivateService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	h := newHarness(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(h.diagnostics(ctx))
		}
	})
	dev := h.applyDevNet(ctx)
	h.applyDingoNode(
		ctx,
		dev,
		withNodeToClient(),
		func(spec *dingov1alpha1.DingoNodeSpec) error {
			spec.Image.Tag = "0.83.0"
			spec.StorageMode = dingov1alpha1.StorageModeAPI
			spec.APIs = &dingov1alpha1.APISpec{
				UTxORPC:    9090,
				Blockfrost: 3000,
				Mesh:       8080,
				Kupo:       1442,
				Bark:       3003,
				Midnight:   50051,
			}
			return nil
		},
	)
	h.waitPodReady(ctx)
	h.waitForged(ctx, 1)
	for _, allowed := range []bool{true, false} {
		name := fmt.Sprintf("api-client-%t", allowed)
		labels := map[string]string{}
		if allowed {
			labels[resources.APIAccessLabel] = "allowed"
		}
		script := fmt.Sprintf(`import socket, time
host = socket.gethostbyname(%q)
allowed = %s
ports = [9090, 3000, 8080, 1442, 3003, 50051]
deadline = time.monotonic() + 60
for port in ports:
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                connected = True
        except OSError:
            connected = False
        if connected == allowed:
            break
        if not allowed or time.monotonic() >= deadline:
            raise AssertionError((port, connected, allowed))
        time.sleep(1)
try:
    with socket.create_connection((host, 3002), timeout=3):
        raise AssertionError("API grant exposed node-to-client")
except OSError:
    pass
`, nodeName+"-private", map[bool]string{true: "True", false: "False"}[allowed])
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: h.namespace,
				Labels:    labels,
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{
					{
						Name:    "client",
						Image:   "python:3.13-alpine",
						Command: []string{"python3", "-c", script},
					},
				},
			},
		}
		require.NoError(t, h.client.Create(ctx, pod))
		h.waitFor(
			ctx,
			podReadyTimeout,
			"API access policy",
			func(ctx context.Context) (bool, error) {
				var actual corev1.Pod
				if err := h.client.Get(ctx, client.ObjectKeyFromObject(pod), &actual); err != nil {
					return false, err
				}
				if actual.Status.Phase == corev1.PodFailed {
					return false, fmt.Errorf("API client %s failed", name)
				}
				return actual.Status.Phase == corev1.PodSucceeded, nil
			},
		)
	}
}
