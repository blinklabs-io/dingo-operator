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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/test/devnet"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// poolImageTag is the first Dingo release that reads blockProducerPools.
var poolImageTag = fmt.Sprintf("0.%d.0", resources.MinPoolListMinor)

// testPool is one pool of a test pool list: real keys, so bundles validate.
type testPool struct {
	keys    *devnet.PoolKeys
	id      string
	secret  string
	profile string
}

func newTestPools(t *testing.T, names ...string) []testPool {
	t.Helper()
	pools := make([]testPool, 0, len(names))
	for _, name := range names {
		keys, err := devnet.GeneratePoolKeys(rand.Reader)
		require.NoError(t, err)
		pools = append(pools, testPool{
			keys:    keys,
			id:      hex.EncodeToString(keys.PoolID),
			secret:  "keys-" + name,
			profile: "profile-" + name,
		})
	}
	return pools
}

// poolSecret is the keys Secret of a pool that signs through an agent: no
// kes.skey.
func poolSecret(
	t *testing.T,
	ns string,
	p testPool,
	counter uint64,
) *corev1.Secret {
	t.Helper()
	data, err := p.keys.SecretData(counter, 0)
	require.NoError(t, err)
	delete(data, keyFileKES)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: p.secret, Namespace: ns},
		Data:       data,
	}
}

// poolListNode is a block producer that forges for pools through one agent.
func poolListNode(
	name, ns string,
	age time.Duration,
	pools ...testPool,
) *dingov1alpha1.DingoNode {
	dn := forgerNode(name, ns, "", "", age)
	dn.Spec.Image.Tag = poolImageTag
	bp := dn.Spec.BlockProducer
	bp.Keys = dingov1alpha1.KeysSpec{}
	bp.SlotsPerKESPeriod = 129600
	bp.MaxKESEvolutions = 62
	bp.Rotation.Mode = dingov1alpha1.RotationModeAssisted
	bp.SigningAgent = &dingov1alpha1.SigningAgentSpec{
		Address:          "kes-agent.signing.svc:7000",
		ServerSPKISHA256: testSPKIPin,
	}
	for _, p := range pools {
		bp.Pools = append(bp.Pools, dingov1alpha1.BlockProducerPool{
			PoolID:       p.id,
			AgentProfile: p.profile,
			Keys:         dingov1alpha1.KeysSpec{SecretRef: p.secret},
		})
	}
	return dn
}

func TestValidateSpecPoolList(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	bech32 := func(p testPool) string {
		return common.PoolId(common.NewBlake2b224(p.keys.PoolID)).String()
	}
	tests := []struct {
		name    string
		mutate  func(*dingov1alpha1.DingoNode)
		wantErr string
	}{
		{name: "two pools"},
		{
			name: "bech32 and hex spell pools alike",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[0].PoolID = bech32(pools[0])
			},
		},
		{
			name: "monitor only",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Rotation.Mode =
					dingov1alpha1.RotationModeMonitorOnly
			},
		},
		{
			name: "poolId beside pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.PoolID = pools[0].id
			},
			wantErr: "cannot be combined with spec.blockProducer.poolId",
		},
		{
			name: "keys beside pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Keys.SecretRef = "other"
			},
			wantErr: "cannot be combined with spec.blockProducer.poolId or keys",
		},
		{
			name: "no signing agent",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.SigningAgent = nil
			},
			wantErr: "pools requires spec.blockProducer.signingAgent",
		},
		{
			name: "duplicate pool, in another spelling",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[1].PoolID = bech32(pools[0])
			},
			wantErr: "already listed by spec.blockProducer.pools[0]",
		},
		{
			name: "duplicate agent profile",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[1].AgentProfile =
					dn.Spec.BlockProducer.Pools[0].AgentProfile
			},
			wantErr: "need distinct profiles",
		},
		{
			name: "auto rotation",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Rotation.Mode =
					dingov1alpha1.RotationModeAuto
			},
			wantErr: "Auto is not supported with spec.blockProducer.pools",
		},
		{
			name: "pool without a keys Secret",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[1].Keys.SecretRef = ""
			},
			wantErr: "pools[1] requires keys.secretRef",
		},
		{
			name: "pool without a profile",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[1].AgentProfile = ""
			},
			wantErr: "pools[1] requires agentProfile",
		},
		{
			name: "unparseable pool",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools[1].PoolID = "not-a-pool"
			},
			wantErr: "pools[1]",
		},
		{
			name: "image that predates pool lists",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Image.Tag = "0.93.1"
			},
			wantErr: "requires a Dingo image of v0.94.0",
		},
		{
			name: "leios run mode with two pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Environment = map[string]string{
					"DINGO_RUN_MODE": "leios",
				}
			},
			wantErr: "Leios",
		},
		{
			name: "leios run mode in the CARDANO_ spelling with two pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Environment = map[string]string{
					"CARDANO_DINGO_RUN_MODE": "leios",
				}
			},
			wantErr: "Leios",
		},
		{
			// envconfig reads the CARDANO_ spelling first, so Dingo runs
			// serve.
			name: "CARDANO_ spelling of serve wins over leios",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Environment = map[string]string{
					"CARDANO_DINGO_RUN_MODE": "serve",
					"DINGO_RUN_MODE":         "leios",
				}
			},
		},
		{
			name: "musashi network with two pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Network = "musashi"
			},
			wantErr: "Leios",
		},
		{
			name: "musashi magic with two pools",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.Environment = map[string]string{
					"CARDANO_NETWORK_MAGIC": "164",
				}
			},
			wantErr: "Leios",
		},
		{
			name: "leios run mode with one pool",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools = dn.Spec.BlockProducer.Pools[:1]
				dn.Spec.Environment = map[string]string{
					"DINGO_RUN_MODE": "leios",
				}
			},
		},
		{
			name: "one pool needs the signing agent too",
			mutate: func(dn *dingov1alpha1.DingoNode) {
				dn.Spec.BlockProducer.Pools = dn.Spec.BlockProducer.Pools[:1]
				dn.Spec.BlockProducer.SigningAgent = nil
			},
			wantErr: "requires spec.blockProducer.signingAgent",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dn := poolListNode("bp", "ns", 0, pools...)
			if tt.mutate != nil {
				tt.mutate(dn)
			}
			err := validateSpec(dn)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// validateSpec must refuse every setting Dingo refuses beside
// blockProducerPools, spelled with or without the CARDANO_ prefix.
func TestValidateSpecPoolListRefusesSinglePoolEnv(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a")
	for _, name := range []string{
		"SHELLEY_VRF_KEY", "SHELLEY_KES_KEY", "SHELLEY_OPERATIONAL_CERTIFICATE",
		"SHELLEY_KES_AGENT_SOCKET", "SHELLEY_KES_AGENT_MODE",
		"SHELLEY_KES_AGENT_ADDRESS", "SHELLEY_KES_AGENT_ADDRESSES",
		"SHELLEY_KES_AGENT_CLIENT_CERT_FILE",
		"SHELLEY_KES_AGENT_CLIENT_KEY_FILE",
		"SHELLEY_KES_AGENT_SERVER_SPKI_SHA256",
		"SHELLEY_KES_AGENT_PLAINTEXT", "SHELLEY_KES_AGENT_PROFILE",
	} {
		for _, variant := range []string{"CARDANO_" + name, name} {
			t.Run(variant, func(t *testing.T) {
				t.Parallel()
				dn := poolListNode("bp", "ns", 0, pools...)
				dn.Spec.Environment = map[string]string{variant: "x"}
				err := validateSpec(dn)
				require.Error(t, err)
				assert.Contains(t, err.Error(), variant)
			})
		}
	}
}

// The vote key, which Dingo refuses beside more than one pool, is refused in
// either spelling envconfig reads.
func TestValidateSpecPoolListRefusesVoteKeyEnv(t *testing.T) {
	t.Parallel()
	pools := newTestPools(t, "a", "b")
	for _, name := range []string{
		resources.VoteKeyEnv, "CARDANO_" + resources.VoteKeyEnv,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dn := poolListNode("bp", "ns", 0, pools...)
			dn.Spec.Environment = map[string]string{name: "/keys/vote.skey"}
			err := validateSpec(dn)
			require.Error(t, err)
			assert.Contains(t, err.Error(), name)
		})
	}
}

func poolReconciler(c client.Client) *DingoNodeReconciler {
	return &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestReconcilePoolKeysRendersEveryAcceptedPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	dn := poolListNode("bp", "ns", 0, pools...)
	secrets := []client.Object{dn}
	for i, p := range pools {
		secrets = append(secrets, poolSecret(t, "ns", p, uint64(i+3)))
	}
	c := conflictClient(t, secrets...)
	r := poolReconciler(c)

	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &opts)
	require.NoError(t, err)
	assert.False(t, keep)

	ids := make([]string, 0, len(pools))
	for _, p := range pools {
		ids = append(ids, p.id)
	}
	slices.Sort(ids)
	assert.Equal(t, ids, resources.SplitKeysPools(opts.KeysPool))
	require.Len(t, opts.Pools, 3)
	for i, p := range opts.Pools {
		assert.Equal(t, ids[i], p.PoolID)
	}
	assert.NotEmpty(t, opts.KeysChecksum)
	assert.NotEmpty(t, opts.PoolsConfigChecksum)

	// Each pool has its own verdict, counter and profile.
	require.Len(t, dn.Status.Pools, 3)
	for i, p := range pools {
		ps := poolStatus(dn, p.id)
		assert.True(t, ps.KeysValid, p.id)
		assert.Equal(t, int64(i+3), ps.OnDiskCounter, p.id)
		for _, rp := range opts.Pools {
			if rp.PoolID == p.id {
				assert.Equal(t, p.profile, rp.AgentProfile)
				assert.Equal(t, p.secret, rp.SecretName)
			}
		}
	}
	kv := meta.FindStatusCondition(dn.Status.Conditions, condKeysValid)
	require.NotNil(t, kv)
	assert.Equal(t, metav1.ConditionTrue, kv.Status)

	// The pool list the pod reads, and the digests the staging container
	// matches against each Secret's opcert.
	var pc corev1.ConfigMap
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: "bp-pools", Namespace: "ns",
	}, &pc))
	assert.Equal(
		t,
		resources.PoolsConfig(dn, opts.Pools),
		pc.Data["dingo.yaml"],
	)
	var approval corev1.ConfigMap
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: resources.KeysApprovalConfigMapName(dn), Namespace: "ns",
	}, &approval))
	require.Len(t, approval.Data, 3)
	for i, p := range pools {
		cert := poolSecret(t, "ns", p, uint64(i+3)).Data[keyFileOpCert]
		assert.Equal(t, sha256Hex(cert), approval.Data[p.id])
	}
}

func TestReconcilePoolKeysMonitorOnlyKeepsLiveBundles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.Mode =
		dingov1alpha1.RotationModeMonitorOnly
	objects := []client.Object{dn}
	for _, p := range pools {
		objects = append(objects, poolSecret(t, "ns", p, 0))
	}
	c := conflictClient(t, objects...)
	r := poolReconciler(c)

	initial := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &initial)
	require.NoError(t, err)
	require.False(t, keep)
	require.NoError(t, c.Create(ctx, resources.BuildStatefulSet(dn, initial)))
	approvalKey := types.NamespacedName{
		Name: resources.KeysApprovalConfigMapName(dn), Namespace: dn.Namespace,
	}
	approval := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, approvalKey, approval))

	secretKey := types.NamespacedName{
		Name: pools[0].secret, Namespace: dn.Namespace,
	}
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, secretKey, secret))
	secret.Data = poolSecret(t, dn.Namespace, pools[0], 1).Data
	secret.Data["extra"] = []byte("changed static material")
	require.NoError(t, c.Update(ctx, secret))
	approval.Data[pools[0].id] = approvedKeysDigest(secret.Data, true)
	require.NoError(t, c.Update(ctx, approval))

	observed := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, message, err := r.reconcilePoolKeys(ctx, dn, &observed)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, monitorOnlyHeldMessage, message)
	assert.Equal(t, initial.KeysChecksum, observed.KeysChecksum)
	require.NoError(t, c.Get(ctx, approvalKey, approval))
	assert.Empty(t, approval.Data,
		"MonitorOnly must withdraw pending pool approvals")
	assert.Equal(t, int64(1), poolStatus(dn, pools[0].id).OnDiskCounter)
}

func TestReconcilePoolKeysMonitorOnlyClearsPendingOpCert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	objects := []client.Object{dn}
	for _, p := range pools {
		objects = append(objects, poolSecret(t, dn.Namespace, p, 0))
	}
	c := conflictClient(t, objects...)
	r := poolReconciler(c)

	initial := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &initial)
	require.NoError(t, err)
	require.False(t, keep)
	require.NoError(t, c.Create(ctx, resources.BuildStatefulSet(dn, initial)))

	secretKey := types.NamespacedName{
		Name: pools[0].secret, Namespace: dn.Namespace,
	}
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, secretKey, secret))
	secret.Data = poolSecret(t, dn.Namespace, pools[0], 1).Data
	require.NoError(t, c.Update(ctx, secret))

	pending := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err = r.reconcilePoolKeys(ctx, dn, &pending)
	require.NoError(t, err)
	require.False(t, keep)
	assert.Equal(t, initial.KeysChecksum, pending.KeysChecksum,
		"an opcert-only replacement does not change the rollout checksum")

	approvalKey := types.NamespacedName{
		Name: resources.KeysApprovalConfigMapName(dn), Namespace: dn.Namespace,
	}
	approval := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, approvalKey, approval))
	require.NotEmpty(t, approval.Data[pools[0].id])

	dn.Spec.BlockProducer.Rotation.Mode =
		dingov1alpha1.RotationModeMonitorOnly
	observed := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err = r.reconcilePoolKeys(ctx, dn, &observed)
	require.NoError(t, err)
	assert.False(t, keep, "an opcert-only observation does not roll the pod")
	require.NoError(t, c.Get(ctx, approvalKey, approval))
	assert.Empty(t, approval.Data,
		"switching to MonitorOnly must withdraw an Assisted approval")
}

func TestReconcilePoolKeysMonitorOnlyInitializesKeylessTemplate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.Mode =
		dingov1alpha1.RotationModeMonitorOnly
	c := conflictClient(t, dn)
	r := poolReconciler(c)

	empty := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &empty)
	require.NoError(t, err)
	require.True(t, keep)
	require.NoError(t, c.Create(ctx, resources.BuildStatefulSet(dn, empty)))
	for _, p := range pools {
		require.NoError(t, c.Create(ctx, poolSecret(t, dn.Namespace, p, 0)))
	}

	initialized := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err = r.reconcilePoolKeys(ctx, dn, &initialized)
	require.NoError(t, err)
	assert.False(t, keep)
	assert.NotEmpty(t, initialized.KeysChecksum)
	approval := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Name: resources.KeysApprovalConfigMapName(dn), Namespace: dn.Namespace,
	}, approval))
	assert.Len(t, approval.Data, len(pools))
}

// Each pool's opcert is bound to that pool's own poolId, not to the node's: a
// bundle whose cold key hashes to another listed pool is refused.
func TestReconcilePoolKeysBindsEachOpCertToItsPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	swapped := poolSecret(t, "ns", pools[1], 1)
	swapped.Name = pools[0].secret
	c := conflictClient(t, dn, swapped, poolSecret(t, "ns", pools[1], 1))
	r := poolReconciler(c)

	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, _, err := r.reconcilePoolKeys(ctx, dn, &opts)
	require.NoError(t, err)
	assert.False(t, keep)
	require.Len(t, opts.Pools, 1)
	assert.Equal(t, pools[1].id, opts.Pools[0].PoolID)
	a := poolStatus(dn, pools[0].id)
	assert.False(t, a.KeysValid)
	assert.Contains(t, a.KeysMessage, "hashes to pool id "+pools[1].id)
	assert.Contains(t, a.KeysMessage, "is "+pools[0].id)
	assert.True(t, poolStatus(dn, pools[1].id).KeysValid)
}

func TestReconcilePoolKeysDropsABadPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	for name, damage := range map[string]func(*corev1.Secret) *corev1.Secret{
		"corrupt vrf key": func(s *corev1.Secret) *corev1.Secret {
			s.Data[keyFileVRF] = []byte("not an envelope")
			return s
		},
		"missing opcert": func(s *corev1.Secret) *corev1.Secret {
			delete(s.Data, keyFileOpCert)
			return s
		},
		"absent secret": nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dn := poolListNode("bp", "ns", 0, pools...)
			objs := []client.Object{dn}
			for i, p := range pools {
				s := poolSecret(t, "ns", p, uint64(i))
				if p.id == pools[1].id {
					if damage == nil {
						continue
					}
					s = damage(s)
				}
				objs = append(objs, s)
			}
			c := conflictClient(t, objs...)
			r := poolReconciler(c)
			opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
			keep, _, err := r.reconcilePoolKeys(ctx, dn, &opts)
			require.NoError(t, err)
			assert.False(t, keep, "the other pools keep forging")

			// Left out of the pod, the pool list and the approvals.
			require.Len(t, opts.Pools, 2)
			for _, p := range opts.Pools {
				assert.NotEqual(t, pools[1].id, p.PoolID)
			}
			assert.NotContains(t, opts.KeysPool, pools[1].id)
			var pc corev1.ConfigMap
			require.NoError(t, c.Get(ctx, types.NamespacedName{
				Name: "bp-pools", Namespace: "ns",
			}, &pc))
			assert.NotContains(t, pc.Data["dingo.yaml"], pools[1].id)
			assert.Contains(t, pc.Data["dingo.yaml"], pools[0].id)
			assert.Contains(t, pc.Data["dingo.yaml"], pools[2].id)
			var approval corev1.ConfigMap
			require.NoError(t, c.Get(ctx, types.NamespacedName{
				Name: resources.KeysApprovalConfigMapName(dn), Namespace: "ns",
			}, &approval))
			assert.NotContains(t, approval.Data, pools[1].id)
			assert.Len(t, approval.Data, 2)

			// Reported for that pool alone.
			bad := poolStatus(dn, pools[1].id)
			assert.False(t, bad.KeysValid)
			assert.NotEmpty(t, bad.KeysReason)
			assert.NotEmpty(t, bad.KeysMessage)
			if damage == nil {
				assert.Equal(t, "KeysSecretMissing", bad.KeysReason)
			} else {
				assert.Equal(t, "PoolKeysRejected", bad.KeysReason)
			}
			assert.True(t, poolStatus(dn, pools[0].id).KeysValid)
			assert.True(t, poolStatus(dn, pools[2].id).KeysValid)

			// The node summary names the pool and is Degraded, not refused.
			kv := meta.FindStatusCondition(dn.Status.Conditions, condKeysValid)
			require.NotNil(t, kv)
			assert.Equal(t, metav1.ConditionFalse, kv.Status)
			assert.Contains(t, kv.Message, "pool "+pools[1].id)
			assert.True(t, meta.IsStatusConditionTrue(
				dn.Status.Conditions, condDegraded,
			))
		})
	}
}

func TestReconcilePoolKeysRefusesTheNodeWhenEveryPoolIsBad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	bad := poolSecret(t, "ns", pools[0], 0)
	bad.Data[keyFileVRF] = []byte("junk")
	c := conflictClient(t, dn, bad) // pool b's Secret is absent

	r := poolReconciler(c)
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	keep, msg, err := r.reconcilePoolKeys(ctx, dn, &opts)
	require.NoError(t, err)
	assert.True(t, keep, "the live template is kept")
	assert.Equal(t, templateHeldMessage, msg)
	assert.Empty(t, opts.Pools)
	assert.False(t, opts.MountKeys)
	kv := meta.FindStatusCondition(dn.Status.Conditions, condKeysValid)
	require.NotNil(t, kv)
	assert.Equal(t, metav1.ConditionFalse, kv.Status)
	assert.Contains(t, kv.Message, "no pool's keys are usable")
	assert.Contains(t, kv.Message, "pool "+pools[0].id)
	assert.Contains(t, kv.Message, "pool "+pools[1].id)
	assert.True(
		t,
		meta.IsStatusConditionTrue(dn.Status.Conditions, condDegraded),
	)
}

func TestPoolsKeysChecksum(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")

	render := func(
		t *testing.T, mutate func(*dingov1alpha1.DingoNode, map[string]*corev1.Secret),
	) string {
		t.Helper()
		dn := poolListNode("bp", "ns", 0, pools...)
		byName := map[string]*corev1.Secret{}
		objs := []client.Object{dn}
		for _, p := range pools {
			s := poolSecret(t, "ns", p, 1)
			byName[p.secret] = s
		}
		if mutate != nil {
			mutate(dn, byName)
		}
		for _, s := range byName {
			objs = append(objs, s)
		}
		c := conflictClient(t, objs...)
		opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
		keep, _, err := poolReconciler(c).reconcilePoolKeys(ctx, dn, &opts)
		require.NoError(t, err)
		require.False(t, keep)
		return opts.KeysChecksum
	}

	base := render(t, nil)
	require.NotEmpty(t, base)
	assert.Equal(t, base, render(t, nil), "the checksum is deterministic")

	t.Run("a new opcert does not change it", func(t *testing.T) {
		t.Parallel()
		got := render(
			t,
			func(_ *dingov1alpha1.DingoNode, s map[string]*corev1.Secret) {
				s[pools[1].secret] = poolSecret(t, "ns", pools[1], 2)
			},
		)
		assert.Equal(t, base, got)
	})
	for _, p := range pools {
		t.Run(
			"a changed vrf key of "+p.secret+" changes it",
			func(t *testing.T) {
				t.Parallel()
				got := render(
					t,
					func(_ *dingov1alpha1.DingoNode, s map[string]*corev1.Secret) {
						other := newTestPools(t, "x")[0]
						vrf, err := other.keys.VRFSKeyEnvelope()
						require.NoError(t, err)
						s[p.secret].Data[keyFileVRF] = vrf
					},
				)
				assert.NotEqual(t, base, got)
			},
		)
		t.Run("an extra entry in "+p.secret+" changes it", func(t *testing.T) {
			t.Parallel()
			got := render(
				t,
				func(_ *dingov1alpha1.DingoNode, s map[string]*corev1.Secret) {
					s[p.secret].Data["extra"] = []byte("x")
				},
			)
			assert.NotEqual(t, base, got)
		})
	}
	t.Run("the order of the spec does not change it", func(t *testing.T) {
		t.Parallel()
		got := render(
			t,
			func(dn *dingov1alpha1.DingoNode, _ map[string]*corev1.Secret) {
				slices.Reverse(dn.Spec.BlockProducer.Pools)
			},
		)
		assert.Equal(t, base, got)
	})
	t.Run("a pool dropped changes it", func(t *testing.T) {
		t.Parallel()
		got := render(
			t,
			func(_ *dingov1alpha1.DingoNode, s map[string]*corev1.Secret) {
				delete(s[pools[2].secret].Data, keyFileVRF)
			},
		)
		assert.NotEqual(t, base, got)
	})
}

// poolMetricsFetcher returns a fixed Status for every scrape.
type poolMetricsFetcher struct{ st *forgestatus.Status }

func (f poolMetricsFetcher) Fetch(
	context.Context,
	string,
) (*forgestatus.Status, error) {
	return f.st, nil
}

func poolLabelOf(id string) string {
	raw, err := hex.DecodeString(id)
	if err != nil {
		panic(err)
	}
	return common.PoolId(common.NewBlake2b224(raw)).String()
}

func TestRefreshForgeStatusReadsEachPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
	for _, p := range pools {
		poolStatus(dn, p.id).KeysValid = true
	}
	// Dingo labels the series with the bech32 pool ID.
	metrics := func(remaining, counter int64) *forgestatus.PoolMetrics {
		return &forgestatus.PoolMetrics{
			CurrentKESPeriod:      900,
			RemainingKESPeriods:   remaining,
			OpCertStartKESPeriod:  838,
			OpCertExpiryKESPeriod: 900 + remaining,
			HasKESData:            true,
			OpCertCounter:         counter,
			HasOpCertCounter:      true,
		}
	}
	st := &forgestatus.Status{
		HasTipSlot: true, TipSlot: 10,
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		Pools: map[string]*forgestatus.PoolMetrics{
			poolLabelOf(pools[0].id): metrics(55, 4),
			poolLabelOf(pools[1].id): metrics(7, 9),
			poolLabelOf(pools[2].id): metrics(31, 2),
		},
	}
	r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
	r.refreshForgeStatus(ctx, dn)

	for _, tc := range []struct {
		pool      testPool
		remaining int64
		counter   int64
	}{{pools[0], 55, 4}, {pools[1], 7, 9}, {pools[2], 31, 2}} {
		ps := poolStatus(dn, tc.pool.id)
		assert.Equal(t, tc.remaining, ps.KES.RemainingPeriods, tc.pool.id)
		assert.Equal(t, 900+tc.remaining, ps.KES.ExpiryPeriod, tc.pool.id)
		require.NotNil(t, ps.LoadedCounter, tc.pool.id)
		assert.Equal(t, tc.counter, *ps.LoadedCounter, tc.pool.id)
	}
	// The node summary is the pool with the fewest periods left, and any
	// pool due makes rotation due.
	assert.Equal(t, int64(7), dn.Status.KES.RemainingPeriods)
	due := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, due)
	assert.Equal(t, metav1.ConditionTrue, due.Status)
	assert.Contains(t, due.Message, "pool "+pools[1].id)
	assert.NotContains(t, due.Message, pools[0].id)
	assert.NotContains(t, due.Message, pools[2].id)
}

func TestRefreshForgeStatusPoolListNotDueAndUnobserved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.Rotation.RenewBeforePeriods = 8
	for _, p := range pools {
		poolStatus(dn, p.id).KeysValid = true
	}
	healthy := &forgestatus.PoolMetrics{
		CurrentKESPeriod: 900, RemainingKESPeriods: 40, HasKESData: true,
	}
	st := &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		Pools: map[string]*forgestatus.PoolMetrics{
			poolLabelOf(pools[0].id): healthy,
			poolLabelOf(pools[1].id): healthy,
		},
	}
	r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
	r.refreshForgeStatus(ctx, dn)
	cond := meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)

	// A pool the node reports nothing for is unknown, not healthy.
	delete(st.Pools, poolLabelOf(pools[1].id))
	r.refreshForgeStatus(ctx, dn)
	cond = meta.FindStatusCondition(dn.Status.Conditions, condRotationDue)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionUnknown, cond.Status)
	assert.Equal(t, "KESMetricsUnavailable", cond.Reason)
}

// A node left with one pool exposes its series unlabelled.
func TestRefreshForgeStatusOnePoolUnlabelled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	poolStatus(dn, pools[0].id).KeysValid = true // pool b was dropped
	st := &forgestatus.Status{
		HasPipelineStuckMetric: true, HasPipelineHaltedMetric: true,
		HasKESData: true, CurrentKESPeriod: 900, RemainingKESPeriods: 20,
		HasOpCertCounter: true, OpCertCounter: 6,
	}
	r := &DingoNodeReconciler{ForgeStatus: poolMetricsFetcher{st: st}}
	r.refreshForgeStatus(ctx, dn)

	ps := poolStatus(dn, pools[0].id)
	assert.Equal(t, int64(20), ps.KES.RemainingPeriods)
	require.NotNil(t, ps.LoadedCounter)
	assert.Equal(t, int64(6), *ps.LoadedCounter)
	dropped := poolStatus(dn, pools[1].id)
	assert.Zero(t, dropped.KES)
	assert.Nil(t, dropped.LoadedCounter)
}

// stubPoolsOnChain answers the counters of several pools from one query.
type stubPoolsOnChain struct {
	counters map[common.Blake2b224]onchain.Counter
	err      error
	queries  [][]common.Blake2b224
}

func (s *stubPoolsOnChain) Fetch(
	context.Context,
	onchain.Query,
) (onchain.Counter, error) {
	return onchain.Counter{}, fmt.Errorf("single-pool query used for a list")
}

func (s *stubPoolsOnChain) FetchPools(
	_ context.Context,
	_ onchain.Query,
	pools []common.Blake2b224,
) (map[common.Blake2b224]onchain.Counter, error) {
	s.queries = append(s.queries, slices.Clone(pools))
	return s.counters, s.err
}

func TestRefreshOnChainCountersOfEveryPoolWithOneQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b", "c")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	hash := func(p testPool) common.Blake2b224 {
		return common.NewBlake2b224(p.keys.PoolID)
	}
	stub := &stubPoolsOnChain{counters: map[common.Blake2b224]onchain.Counter{
		hash(pools[0]): {Value: 4, Found: true},
		hash(pools[1]): {Value: 0, Found: true},
		hash(pools[2]): {Value: 11, Found: true},
	}}
	c := conflictClient(t)
	r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
	r.refreshOnChainCounter(ctx, dn)

	require.Len(t, stub.queries, 1, "one query serves every pool")
	assert.ElementsMatch(t, []common.Blake2b224{
		hash(pools[0]), hash(pools[1]), hash(pools[2]),
	}, stub.queries[0])
	for i, want := range []int64{4, 0, 11} {
		ps := poolStatus(dn, pools[i].id)
		assert.Equal(t, want, ps.OnChainCounter, pools[i].id)
		require.NotNil(t, ps.OnChainCounterAt, pools[i].id)
	}
	cond := meta.FindStatusCondition(dn.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Observed", cond.Reason)
	assert.Zero(t, dn.Status.OpCert.OnChainCounter,
		"the node-level counter is a single-pool field")

	// Within the refresh interval nothing is asked again, and the
	// observation is restored into a status that lost it.
	dn.Status.Pools = nil
	r.refreshOnChainCounter(ctx, dn)
	assert.Len(t, stub.queries, 1)
	assert.Equal(t, int64(11), poolStatus(dn, pools[2].id).OnChainCounter)

	// A pool the chain has no counter for keeps none; the others stand.
	stub.counters = map[common.Blake2b224]onchain.Counter{
		hash(pools[0]): {Value: 5, Found: true},
	}
	ageOnChainAttempt(r, client.ObjectKeyFromObject(dn), time.Hour)
	r.refreshOnChainCounter(ctx, dn)
	require.Len(t, stub.queries, 2)
	assert.Equal(t, int64(5), poolStatus(dn, pools[0].id).OnChainCounter)
	assert.Equal(t, int64(11), poolStatus(dn, pools[2].id).OnChainCounter,
		"a counter not re-observed is kept as a floor")
}

func TestRefreshOnChainCountersFailureKeepsWhatWasRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	dn.Spec.BlockProducer.NodeToClient.Enabled = true
	hash := common.NewBlake2b224(pools[0].keys.PoolID)
	stub := &stubPoolsOnChain{counters: map[common.Blake2b224]onchain.Counter{
		hash: {Value: 4, Found: true},
	}}
	c := conflictClient(t)
	r := &DingoNodeReconciler{Client: c, Scheme: c.Scheme(), OnChain: stub}
	r.refreshOnChainCounter(ctx, dn)
	require.Equal(t, int64(4), poolStatus(dn, pools[0].id).OnChainCounter)

	stub.counters, stub.err = nil, fmt.Errorf("dial refused")
	ageOnChainAttempt(r, client.ObjectKeyFromObject(dn), time.Hour)
	r.refreshOnChainCounter(ctx, dn)
	assert.Equal(t, int64(4), poolStatus(dn, pools[0].id).OnChainCounter)
	cond := meta.FindStatusCondition(dn.Status.Conditions, condOnChainCounter)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "dial refused")
}

// The on-chain counter of a pool is the floor for that pool's bundle alone.
func TestReconcilePoolKeysEnforcesEachPoolsOwnFloor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pools := newTestPools(t, "a", "b")
	dn := poolListNode("bp", "ns", 0, pools...)
	now := metav1.NewTime(time.Now())
	for i, p := range pools {
		ps := poolStatus(dn, p.id)
		ps.OnChainCounter, ps.OnChainCounterAt = int64(7+i*10), &now
	}
	c := conflictClient(t, dn,
		// Above pool a's floor of 7, below pool b's floor of 17: accepted.
		poolSecret(t, "ns", pools[0], 9),
		// Below pool b's own floor.
		poolSecret(t, "ns", pools[1], 12),
	)
	r := poolReconciler(c)
	opts := resources.RenderOptions{MountKeys: true, Replicas: 1}
	_, _, err := r.reconcilePoolKeys(ctx, dn, &opts)
	require.NoError(t, err)
	assert.True(t, poolStatus(dn, pools[0].id).KeysValid)
	b := poolStatus(dn, pools[1].id)
	assert.False(t, b.KeysValid)
	assert.Contains(t, b.KeysMessage, "below the on-chain counter 17")
}
