//go:build e2e

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// agentDingoImage must read CARDANO_SHELLEY_KES_AGENT_ADDRESS, which the
	// suite's default image does not (resources.MinSigningAgentMinor).
	// hack/e2e/k3d-up.sh side-loads it with the two images below.
	agentDingoImage = "ghcr.io/blinklabs-io/dingo:0.94.2"
	// bursaImage is a Bursa release whose kes-agent has the per-slot guard
	// (one header body signed per slot, a different one refused) and admits a
	// remote signer only by a pinned client certificate.
	bursaImage = "ghcr.io/blinklabs-io/bursa:0.27.0"
	// agentSidecarImage supplies the shell Bursa's image lacks, to write the
	// operational certificate where the agent can read it.
	agentSidecarImage = "docker.io/library/alpine:3.20"

	agentName        = "kes-agent"
	agentTLSSecret   = "kes-agent-tls"
	agentTLSPath     = "/etc/bursa-tls"
	agentClientCert  = "kes-agent-client"
	agentContainer   = "bursa"
	agentSidecar     = "files"
	agentPort        = 3001
	agentMetricsPort = 8082
	agentConfigPath  = "/etc/bursa/config.yaml"
	agentOpCertPath  = "/shared/node.cert"
	relayName        = "relay"

	// agentLeadTime covers what this test does before the nodes exist
	// (agent start, key install) on top of genesisLeadTime's node startup.
	agentLeadTime = 150 * time.Second

	// forgedBlocksWanted is how many blocks the relay must hold before the
	// chain is judged.
	forgedBlocksWanted = 8
	agentTestTimeout   = 25 * time.Minute
)

// TestAgentSignedProducerRunsTwoPods runs an agent-signed block producer with
// two pods and a relay on a devnet, signing through a real Bursa kes-agent,
// and checks that the pods forge one chain together and that forging survives
// the loss of a pod.
func TestAgentSignedProducerRunsTwoPods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), agentTestTimeout)
	defer cancel()

	h := newHarness(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(h.diagnostics(ctx))
			t.Log(h.namespaceLogs(ctx))
		}
	})

	dn := h.applyDevNetConfig(ctx, agentLeadTime)
	pki := h.applyAgentPKI(ctx)
	h.startKESAgent(ctx, dn, pki)
	opcert := h.installAgentKey(ctx, dn)

	vrf, err := dn.Keys.VRFSKeyEnvelope()
	require.NoError(t, err, "render vrf.skey")
	require.NoError(t, h.client.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keysSecretRef,
			Namespace: h.namespace,
		},
		// No kes.skey: the agent holds the only KES key.
		Data: map[string][]byte{"vrf.skey": vrf, "opcert.cert": opcert},
	}), "create keys Secret")

	h.applyRelay(ctx)
	h.applyDingoNode(ctx, dn,
		withAgentImage(),
		withReplicas(2),
		withSigningAgent(fmt.Sprintf("%s:%d", agentName, agentPort), pki),
		withRelayRefs(relayName),
	)

	// 1. Both pods and the relay come up, and the operator sees both pods.
	pods := []string{"bp-0", "bp-1", relayName + "-0"}
	for _, pod := range pods {
		h.waitNamedPodReady(ctx, pod)
		t.Logf("pod %s ready", pod)
	}
	h.waitFor(ctx, kesStatusTimeout,
		"status.replicas to report both producer pods as scraped",
		func(ctx context.Context) (bool, error) {
			node, err := h.getNodeErr(ctx)
			if err != nil {
				return false, err
			}
			if len(node.Status.Replicas) != 2 {
				return false, fmt.Errorf("status.replicas has %d entries",
					len(node.Status.Replicas))
			}
			for _, r := range node.Status.Replicas {
				if !r.Scraped {
					return false, fmt.Errorf("%s not scraped", r.Name)
				}
			}
			return true, nil
		})

	// Each side lists every pod of the other.
	assert.ElementsMatch(t, []string{
		"bp-0.bp-headless." + h.namespace + ".svc.cluster.local",
		"bp-1.bp-headless." + h.namespace + ".svc.cluster.local",
	}, h.topologyAddresses(ctx, relayName), "relay peers")
	assert.ElementsMatch(t, []string{
		"relay-0.relay-headless." + h.namespace + ".svc.cluster.local",
	}, h.topologyAddresses(ctx, nodeName), "producer peers")

	// 2. The chain advances and both pods forge.
	h.waitFor(ctx, forgeTimeout+kesStatusTimeout,
		fmt.Sprintf("the relay to hold %d blocks", forgedBlocksWanted),
		func(ctx context.Context) (bool, error) {
			n, err := h.blockNumber(ctx, relayName+"-0")
			return n >= forgedBlocksWanted, err
		})
	for _, pod := range []string{"bp-0", "bp-1"} {
		h.waitFor(ctx, forgeTimeout, pod+" to forge a block",
			func(ctx context.Context) (bool, error) {
				st, err := h.tryScrapeMetricsOf(ctx, pod)
				if err != nil {
					return false, err
				}
				t.Logf("%s forged %d", pod, st.ForgedBlocks)
				return st.ForgedBlocks > 0, nil
			})
	}
	t.Log(h.agentSummary(ctx))

	// 3. One chain.
	h.requireOneChain(ctx, pods)

	// The relay must be following both pods before one is taken away, or the
	// survivor's blocks have no way in and the failover proves nothing.
	h.waitRelayFollows(ctx, "bp-0", "bp-1")

	// 4. Losing a pod does not stop forging, and it rejoins the chain. bp-0 is
	// held down by cordoning the node, so the replacement stays Pending for as
	// long as the survivor is observed instead of racing a ten second restart.
	survivorBefore := h.forgedBy(ctx, "bp-1")
	relayBefore, err := h.blockNumber(ctx, relayName+"-0")
	require.NoError(t, err)
	old := h.namedPod(ctx, "bp-0")
	h.cordon(ctx)
	require.NoError(t, h.client.Delete(ctx, old,
		client.GracePeriodSeconds(0)), "delete bp-0")
	h.waitFor(ctx, podReadyTimeout, "bp-0 to be gone",
		func(ctx context.Context) (bool, error) {
			pod, err := h.namedPodOrNil(ctx, "bp-0")
			return pod == nil || pod.UID != old.UID, err
		})
	h.waitFor(ctx, forgeTimeout,
		"blocks to keep arriving at the relay with bp-0 down",
		func(ctx context.Context) (bool, error) {
			pod, err := h.namedPodOrNil(ctx, "bp-0")
			if err != nil {
				return false, err
			}
			if pod != nil && podIsReady(pod) {
				return false, errors.New("bp-0 is Ready")
			}
			n, err := h.blockNumber(ctx, relayName+"-0")
			if err != nil {
				return false, err
			}
			st, err := h.tryScrapeMetricsOf(ctx, "bp-1")
			if err != nil {
				return false, err
			}
			t.Logf("bp-0 down: relay at block %d (was %d), bp-1 forged %d (was %d)",
				n, relayBefore, st.ForgedBlocks, survivorBefore)
			return n >= relayBefore+4 && st.ForgedBlocks >= survivorBefore+4, nil
		})
	pod, err := h.namedPodOrNil(ctx, "bp-0")
	require.NoError(t, err)
	require.True(t, pod == nil || !podIsReady(pod),
		"bp-0 was Ready again, so the survivor alone was not shown")
	t.Log("blocks kept arriving while bp-0 was down")
	h.uncordon(ctx)

	h.waitNamedPodReady(ctx, "bp-0")
	h.requireOneChain(ctx, pods)
	h.requireNoForkedSlots(ctx, "bp-1", "bp-0")
	t.Log(h.agentSummary(ctx))

	// Every header the pods raced to sign for a slot was the same one: the
	// agent signs an identical body again as a retry, and it refuses a
	// different one as an equivocation. A pod forging on a parent of its own
	// would have drawn a refusal.
	signed, ok := h.agentCounter(ctx, "bursa_kesagent_sign_total", `result="ok"`)
	require.True(t, ok)
	assert.Positive(t, signed, "the agent signed nothing")
	equivocations, ok := h.agentCounter(ctx,
		"bursa_kesagent_slot_guard_refusals_total", `reason="equivocation"`)
	require.True(t, ok, "the agent does not export the slot guard counter")
	assert.Zero(t, equivocations,
		"the agent refused a different header for a slot already signed")
}

// agentCounter reads a series of the agent's metrics.
func (h *harness) agentCounter(
	ctx context.Context,
	name string,
	labels ...string,
) (float64, bool) {
	h.t.Helper()
	body, err := h.metricsBody(ctx, agentName, agentMetricsPort)
	require.NoError(h.t, err, "scrape the kes-agent")
	return metricSum(body, name, labels...)
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// withAgentImage runs the node on agentDingoImage.
func withAgentImage() nodeOption {
	return func(spec *dingov1alpha1.DingoNodeSpec) error {
		spec.Image.Repository, spec.Image.Tag = splitImage(agentDingoImage)
		return nil
	}
}

func splitImage(image string) (repository, tag string) {
	sep := strings.LastIndex(image, ":")
	return image[:sep], image[sep+1:]
}

// withReplicas sets spec.replicas.
func withReplicas(n int32) nodeOption {
	return func(spec *dingov1alpha1.DingoNodeSpec) error {
		spec.Replicas = &n
		return nil
	}
}

// withSigningAgent makes the producer sign through the agent at address,
// pinning its server key and presenting the client certificate in
// agentClientCert.
func withSigningAgent(address string, pki agentPKI) nodeOption {
	return func(spec *dingov1alpha1.DingoNodeSpec) error {
		if spec.BlockProducer == nil {
			return errors.New("withSigningAgent needs spec.blockProducer")
		}
		spec.BlockProducer.SigningAgent = &dingov1alpha1.SigningAgentSpec{
			Address:             address,
			ServerSPKISHA256:    pki.serverSPKI,
			ClientCertSecretRef: agentClientCert,
		}
		return nil
	}
}

// withRelayRefs peers the node with the named relays.
func withRelayRefs(refs ...string) nodeOption {
	return func(spec *dingov1alpha1.DingoNodeSpec) error {
		spec.Topology.RelayRefs = refs
		return nil
	}
}

// applyRelay creates the relay the producer peers with, which peers back.
func (h *harness) applyRelay(ctx context.Context) {
	h.t.Helper()
	repo, tag := splitImage(agentDingoImage)
	relay := &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{Name: relayName, Namespace: h.namespace},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:         dingov1alpha1.RoleRelay,
			Network:      "custom",
			NetworkMagic: new(int64(42)),
			ConfigRef:    configMapName,
			Mithril:      dingov1alpha1.MithrilSpec{Enabled: new(false)},
			Image: dingov1alpha1.ImageSpec{
				Repository: repo,
				Tag:        tag,
				PullPolicy: corev1.PullIfNotPresent,
			},
			Topology: dingov1alpha1.TopologySpec{
				UseLedgerAfterSlot: new(int64(-1)),
				RelayRefs:          []string{nodeName},
			},
		},
	}
	require.NoError(h.t, h.client.Create(ctx, relay), "create relay")
}

// agentPKI is the throwaway PKI of the agent link. Bursa's kes-agent admits a
// remote signing session only for a client certificate it has verified and
// pinned: over plain TCP it answers "client is not permitted the requested
// role" (seen against v0.26.0 in this suite), so the link is mutual TLS.
type agentPKI struct {
	caPEM, serverCertPEM, serverKeyPEM, clientCertPEM, clientKeyPEM []byte
	// serverSPKI and clientSPKI are SHA-256 digests of the certificates'
	// public keys, in hex.
	serverSPKI, clientSPKI string
}

func (h *harness) newAgentPKI() agentPKI {
	h.t.Helper()
	newKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(h.t, err)
		return k
	}
	serial := int64(1)
	issue := func(
		tmpl *x509.Certificate,
		parent *x509.Certificate,
		pub *ecdsa.PublicKey,
		signer *ecdsa.PrivateKey,
	) (*x509.Certificate, []byte) {
		tmpl.SerialNumber = big.NewInt(serial)
		serial++
		tmpl.NotBefore = time.Now().Add(-time.Hour)
		tmpl.NotAfter = time.Now().Add(24 * time.Hour)
		if parent == nil {
			parent = tmpl
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
		require.NoError(h.t, err)
		cert, err := x509.ParseCertificate(der)
		require.NoError(h.t, err)
		return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	keyPEM := func(k *ecdsa.PrivateKey) []byte {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		require.NoError(h.t, err)
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}
	spki := func(c *x509.Certificate) string {
		sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
		return hex.EncodeToString(sum[:])
	}

	caKey, serverKey, clientKey := newKey(), newKey(), newKey()
	ca, caPEM := issue(&x509.Certificate{
		Subject:               pkix.Name{CommonName: "e2e kes-agent CA"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}, nil, &caKey.PublicKey, caKey)
	svc := agentName + "." + h.namespace
	server, serverPEM := issue(&x509.Certificate{
		Subject:     pkix.Name{CommonName: agentName},
		DNSNames:    []string{agentName, svc, svc + ".svc", svc + ".svc.cluster.local"},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, ca, &serverKey.PublicKey, caKey)
	client, clientPEM := issue(&x509.Certificate{
		Subject:     pkix.Name{CommonName: "e2e dingo producer"},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, ca, &clientKey.PublicKey, caKey)
	return agentPKI{
		caPEM: caPEM, serverCertPEM: serverPEM, serverKeyPEM: keyPEM(serverKey),
		clientCertPEM: clientPEM, clientKeyPEM: keyPEM(clientKey),
		serverSPKI: spki(server), clientSPKI: spki(client),
	}
}

// applyAgentPKI generates the agent link's PKI and stores the halves the
// agent pod and the producer each mount.
func (h *harness) applyAgentPKI(ctx context.Context) agentPKI {
	h.t.Helper()
	pki := h.newAgentPKI()
	for name, data := range map[string]map[string][]byte{
		agentTLSSecret: {
			"tls.crt": pki.serverCertPEM, "tls.key": pki.serverKeyPEM,
			"ca.crt": pki.caPEM,
		},
		agentClientCert: {
			"tls.crt": pki.clientCertPEM, "tls.key": pki.clientKeyPEM,
		},
	} {
		require.NoError(h.t, h.client.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
			Data:       data,
		}), "create Secret %s", name)
	}
	return pki
}

// startKESAgent runs Bursa's kes-agent in sign mode for the devnet pool, with
// its remote sign listener on mutual TLS admitting only pki's client, and
// waits for its control socket.
func (h *harness) startKESAgent(
	ctx context.Context,
	dn *devnet.DevNet,
	pki agentPKI,
) {
	h.t.Helper()
	config := fmt.Sprintf(`kes_agent:
  mode: sign
  service_socket: /run/bursa/service.sock
  control_socket: /run/bursa/control.sock
  attested_listen_address: ":%[1]d"
  attested_tls_cert_file: %[7]s/tls.crt
  attested_tls_key_file: %[7]s/tls.key
  attested_client_ca_file: %[7]s/ca.crt
  attestation_provider: none
  profiles:
    - name: devnet
      sign_client_spki_sha256: ["%[8]s"]
      cold_vkey_hex: %[2]s
      system_start: %[3]q
      slot_length: 1
      slots_per_kes_period: %[4]d
      max_kes_evolutions: %[5]d
      guard_file: /var/lib/bursa/guard.json
metrics:
  address: 0.0.0.0
  port: %[6]d
`,
		agentPort,
		hex.EncodeToString(dn.Keys.ColdVKey),
		dn.Params.SystemStart.UTC().Format(time.RFC3339),
		slotsPerKESPeriod, maxKESEvolutions, agentMetricsPort,
		agentTLSPath, pki.clientSPKI)
	require.NoError(h.t, h.client.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: agentName + "-config", Namespace: h.namespace,
		},
		Data: map[string]string{"config.yaml": config},
	}), "create kes-agent config")

	empty := func(name string) corev1.Volume {
		return corev1.Volume{
			Name:         name,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}
	}
	mount := func(name, path string) corev1.VolumeMount {
		return corev1.VolumeMount{Name: name, MountPath: path}
	}
	labels := map[string]string{"app": agentName}
	require.NoError(h.t, h.client.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: agentName, Namespace: h.namespace, Labels: labels,
		},
		Spec: corev1.PodSpec{
			// Bursa refuses a key file reached through a symlink, which is
			// how a Secret volume lays out its files, and one it does not
			// own: copy them out to a volume it can open.
			InitContainers: []corev1.Container{{
				Name:            "tls",
				Image:           agentSidecarImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"sh", "-c",
					"cp /src/tls.crt /src/tls.key /src/ca.crt " + agentTLSPath +
						" && chown 65532:65532 " + agentTLSPath + "/* && chmod 600 " +
						agentTLSPath + "/*"},
				VolumeMounts: []corev1.VolumeMount{
					mount("tls-src", "/src"), mount("tls", agentTLSPath),
				},
			}},
			Containers: []corev1.Container{{
				Name:            agentContainer,
				Image:           bursaImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Args:            []string{"kes-agent", "--config", agentConfigPath},
				VolumeMounts: []corev1.VolumeMount{
					mount("config", "/etc/bursa"),
					mount("run", "/run/bursa"),
					mount("state", "/var/lib/bursa"),
					mount("shared", "/shared"),
					mount("tls", agentTLSPath),
				},
			}, {
				Name:            agentSidecar,
				Image:           agentSidecarImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sleep", "86400"},
				VolumeMounts:    []corev1.VolumeMount{mount("shared", "/shared")},
			}},
			Volumes: []corev1.Volume{{
				Name: "config",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: agentName + "-config",
					},
				}},
			}, {
				Name: "tls-src",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: agentTLSSecret,
				}},
			}, empty("tls"), empty("run"), empty("state"), empty("shared")},
		},
	}), "create kes-agent pod")
	require.NoError(h.t, h.client.Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: agentName, Namespace: h.namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "sign", Port: agentPort,
				TargetPort: intstr.FromInt32(agentPort),
			}},
		},
	}), "create kes-agent service")

	h.waitNamedPodReady(ctx, agentName)
	h.waitFor(ctx, podReadyTimeout, "the kes-agent control socket",
		func(ctx context.Context) (bool, error) {
			_, err := h.agentControl(ctx, "info")
			return err == nil, err
		})
}

// agentControl runs one `bursa kes-agent control` command in the agent pod and
// returns its JSON reply.
func (h *harness) agentControl(
	ctx context.Context,
	args ...string,
) (string, error) {
	cmd := append([]string{
		"/bin/bursa", "kes-agent", "control", "--config", agentConfigPath,
	}, args...)
	out, errOut, err := h.execIn(ctx, agentName, agentContainer, cmd, nil)
	if err != nil {
		return out, fmt.Errorf("%w: %s", err, strings.TrimSpace(errOut+out))
	}
	return out, nil
}

// installAgentKey has the agent generate its KES key, certifies it with the
// devnet's cold key and installs the certificate, returning it. The agent
// never sees the cold signing key.
func (h *harness) installAgentKey(
	ctx context.Context,
	dn *devnet.DevNet,
) []byte {
	h.t.Helper()
	out, err := h.agentControl(ctx, "gen-staged-key")
	require.NoError(h.t, err, "gen-staged-key")
	var reply struct {
		KESVKey string `json:"kes_vkey"`
	}
	require.NoError(h.t, json.Unmarshal([]byte(out), &reply), "parse %q", out)
	vkey, err := hex.DecodeString(reply.KESVKey)
	require.NoError(h.t, err)
	require.Len(h.t, vkey, 32, "KES verification key")

	opcert, err := dn.Keys.IssueOpCertFor(vkey, 0, 0)
	require.NoError(h.t, err, "issue opcert for the agent's KES key")
	_, errOut, err := h.execIn(ctx, agentName, agentSidecar,
		[]string{"sh", "-c", "cat > " + agentOpCertPath + " && chmod 644 " + agentOpCertPath},
		bytes.NewReader(opcert))
	require.NoError(h.t, err, "write opcert: %s", errOut)

	out, err = h.agentControl(ctx, "install-key", "--opcert", agentOpCertPath)
	require.NoError(h.t, err, "install-key")
	h.t.Logf("kes-agent installed key %s: %s", reply.KESVKey, compact(out))
	return opcert
}

func compact(s string) string { return strings.Join(strings.Fields(s), " ") }

// execIn runs cmd in a container of pod and returns what it wrote.
func (h *harness) execIn(
	ctx context.Context,
	pod, container string,
	cmd []string,
	stdin io.Reader,
) (stdout, stderr string, err error) {
	req := h.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(h.namespace).Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(h.cfg, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("build exec for %s: %w", pod, err)
	}
	var out, errOut bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: stdin, Stdout: &out, Stderr: &errOut,
	})
	return out.String(), errOut.String(), err
}

// metricSum returns the sum of the samples of name (with every label fragment
// present) in a Prometheus exposition, and whether any matched.
func metricSum(body, name string, labels ...string) (float64, bool) {
	var sum float64
	found := false
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, name)
		if !ok || rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		match := true
		for _, l := range labels {
			if !strings.Contains(rest, l) {
				match = false
			}
		}
		fields := strings.Fields(rest[strings.LastIndex(rest, "}")+1:])
		if !match || len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		sum += v
		found = true
	}
	return sum, found
}

// nodeMetric reads one gauge of the Dingo pod called pod.
func (h *harness) nodeMetric(
	ctx context.Context,
	pod, name string,
) (int64, error) {
	body, err := h.metricsBody(ctx, pod, metricsPort)
	if err != nil {
		return 0, err
	}
	v, ok := metricSum(body, name)
	if !ok {
		return 0, fmt.Errorf("%s does not report %s", pod, name)
	}
	return int64(v), nil
}

func (h *harness) blockNumber(ctx context.Context, pod string) (int64, error) {
	return h.nodeMetric(ctx, pod, "cardano_node_metrics_blockNum_int")
}

func (h *harness) forgedBy(ctx context.Context, pod string) int64 {
	h.t.Helper()
	st, err := h.tryScrapeMetricsOf(ctx, pod)
	require.NoError(h.t, err, "scrape %s", pod)
	return st.ForgedBlocks
}

func (h *harness) namedPod(ctx context.Context, name string) *corev1.Pod {
	h.t.Helper()
	pod, err := h.namedPodOrNil(ctx, name)
	require.NoError(h.t, err)
	require.NotNil(h.t, pod, "pod %s", name)
	return pod
}

// topologyAddresses returns the access points the operator rendered for the
// DingoNode called node.
func (h *harness) topologyAddresses(ctx context.Context, node string) []string {
	h.t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(h.t, h.client.Get(ctx, types.NamespacedName{Name: node + "-topology", Namespace: h.namespace}, cm))
	var doc struct {
		LocalRoots []struct {
			AccessPoints []struct {
				Address string `json:"address"`
			} `json:"accessPoints"`
		} `json:"localRoots"`
	}
	require.NoError(h.t, json.Unmarshal([]byte(cm.Data["topology.json"]), &doc))
	var out []string
	for _, root := range doc.LocalRoots {
		for _, ap := range root.AccessPoints {
			out = append(out, ap.Address)
		}
	}
	return out
}

// agentSummary reports the agent's slot guard counters.
func (h *harness) agentSummary(ctx context.Context) string {
	body, err := h.metricsBody(ctx, agentName, agentMetricsPort)
	if err != nil {
		return "agent metrics: " + err.Error()
	}
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "bursa_kesagent_") &&
			!strings.HasPrefix(line, "#") {
			b.WriteString("\n  " + line)
		}
	}
	return "kes-agent metrics:" + b.String()
}

var (
	tipLine      = regexp.MustCompile(`new tip: ([0-9a-f]{64}) at slot (\d+)`)
	acceptedLine = regexp.MustCompile(
		`block proposal accepted by chain" slot=(\d+) hash=([0-9a-f]{64})`)
)

// lastTip returns the newest "new tip" a Dingo pod logged, as "hash@slot".
func (h *harness) lastTip(ctx context.Context, pod string) (string, error) {
	logs, err := h.logsOf(ctx, pod, nodeContainerName)
	if err != nil {
		return "", err
	}
	all := tipLine.FindAllStringSubmatch(logs, -1)
	if len(all) == 0 {
		return "", fmt.Errorf("%s has logged no tip", pod)
	}
	last := all[len(all)-1]
	return last[1] + "@" + last[2], nil
}

// requireOneChain waits until the last tip hash each pod logged is the same
// one, so the pods and the relay agree on the chain by block hash, not only
// by height.
func (h *harness) requireOneChain(ctx context.Context, pods []string) {
	h.t.Helper()
	h.waitFor(ctx, podReadyTimeout, "every pod to log the same tip hash",
		func(ctx context.Context) (bool, error) {
			tips := make([]string, 0, len(pods))
			for _, pod := range pods {
				tip, err := h.lastTip(ctx, pod)
				if err != nil {
					return false, err
				}
				tips = append(tips, tip)
			}
			for _, tip := range tips[1:] {
				if tip != tips[0] {
					return false, fmt.Errorf("tips differ: %v", tips)
				}
			}
			h.t.Logf("same tip hash on %v: %s", pods, tips[0])
			return true, nil
		})
}

// requireNoForkedSlots fails if two producer pods forged different blocks for
// one slot, and if the relay never saw a block a producer forged.
func (h *harness) requireNoForkedSlots(ctx context.Context, pods ...string) {
	h.t.Helper()
	seen := map[string]string{}
	var hashes []string
	for _, pod := range pods {
		logs, err := h.logsOf(ctx, pod, nodeContainerName)
		require.NoError(h.t, err, "logs of %s", pod)
		for _, m := range acceptedLine.FindAllStringSubmatch(logs, -1) {
			if prev, ok := seen[m[1]]; ok {
				assert.Equal(h.t, prev, m[2], "slot %s forged twice, differently", m[1])
				continue
			}
			seen[m[1]] = m[2]
			hashes = append(hashes, m[2])
		}
	}
	require.NotEmpty(h.t, hashes, "the producers logged no forged block")
	relayLogs, err := h.logsOf(ctx, relayName+"-0", nodeContainerName)
	require.NoError(h.t, err)
	missing := 0
	for _, hash := range hashes {
		if !strings.Contains(relayLogs, hash) {
			missing++
		}
	}
	h.t.Logf("%d distinct forged slots across %v, %d never logged by the relay",
		len(hashes), pods, missing)
	assert.Zero(h.t, missing, "the relay never saw some forged blocks")
}

// waitRelayFollows waits until the relay's chain selection has used a
// connection to each of pods, which is how it receives a pod's blocks.
func (h *harness) waitRelayFollows(ctx context.Context, pods ...string) {
	h.t.Helper()
	h.waitFor(ctx, podReadyTimeout,
		fmt.Sprintf("the relay to follow %v", pods),
		func(ctx context.Context) (bool, error) {
			logs, err := h.logsOf(ctx, relayName+"-0", nodeContainerName)
			if err != nil {
				return false, err
			}
			for _, pod := range pods {
				ip := h.namedPod(ctx, pod).Status.PodIP
				followed := false
				for _, line := range strings.Split(logs, "\n") {
					if strings.Contains(line, "<->"+ip+":") &&
						(strings.Contains(line, "selected new best peer") ||
							strings.Contains(line, "chain switch")) {
						followed = true
						break
					}
				}
				if !followed {
					return false, fmt.Errorf("relay has not followed %s (%s)", pod, ip)
				}
			}
			return true, nil
		})
}

// cordon makes the cluster's nodes unschedulable and restores them when the
// test ends, however it ends.
func (h *harness) cordon(ctx context.Context) {
	h.t.Helper()
	h.setUnschedulable(ctx, true)
	h.t.Cleanup(func() {
		h.setUnschedulable(context.WithoutCancel(ctx), false)
	})
}

func (h *harness) uncordon(ctx context.Context) {
	h.t.Helper()
	h.setUnschedulable(ctx, false)
}

func (h *harness) setUnschedulable(ctx context.Context, v bool) {
	h.t.Helper()
	nodes := &corev1.NodeList{}
	require.NoError(h.t, h.client.List(ctx, nodes))
	for i := range nodes.Items {
		n := &nodes.Items[i]
		n.Spec.Unschedulable = v
		require.NoError(h.t, h.client.Update(ctx, n), "node %s", n.Name)
	}
}

// namespaceLogs returns the tail of every container log in the namespace, for
// a failure.
func (h *harness) namespaceLogs(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	var b strings.Builder
	pods := &corev1.PodList{}
	if err := h.client.List(ctx, pods, client.InNamespace(h.namespace)); err != nil {
		return "list pods: " + err.Error()
	}
	for _, pod := range pods.Items {
		for _, c := range pod.Spec.Containers {
			logs, err := h.logsOf(ctx, pod.Name, c.Name)
			if err != nil {
				fmt.Fprintf(&b, "--- %s/%s: %v\n", pod.Name, c.Name, err)
				continue
			}
			fmt.Fprintf(&b, "--- %s/%s\n%s\n", pod.Name, c.Name, tail(logs, 60))
		}
	}
	return b.String()
}
