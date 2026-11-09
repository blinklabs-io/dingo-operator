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
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
)

func TestStaticKeysChecksum(t *testing.T) {
	base := map[string][]byte{
		"vrf.skey":    []byte("vrf"),
		"kes.skey":    []byte("kes"),
		"opcert.cert": []byte("cert"),
	}
	sum := staticKeysChecksum(base)

	rotated := map[string][]byte{
		"vrf.skey":    []byte("vrf"),
		"kes.skey":    []byte("kes-2"),
		"opcert.cert": []byte("cert-2"),
	}
	assert.Equal(t, sum, staticKeysChecksum(rotated),
		"a new KES key and opcert leave the static checksum unchanged")

	newVRF := map[string][]byte{
		"vrf.skey":    []byte("vrf-2"),
		"kes.skey":    []byte("kes"),
		"opcert.cert": []byte("cert"),
	}
	assert.NotEqual(t, sum, staticKeysChecksum(newVRF))

	withVote := map[string][]byte{
		"vrf.skey":    []byte("vrf"),
		"kes.skey":    []byte("kes"),
		"opcert.cert": []byte("cert"),
		"vote.skey":   []byte("vote"),
	}
	assert.NotEqual(t, sum, staticKeysChecksum(withVote))
	chartVote := map[string][]byte{
		"vrf.skey":    []byte("vrf"),
		"kes.skey":    []byte("kes"),
		"opcert.cert": []byte("cert"),
		"bls.skey":    []byte("vote"),
	}
	assert.Equal(t, staticKeysChecksum(withVote), staticKeysChecksum(chartVote),
		"the vote key hashes the same under either file name")

	withExtra := map[string][]byte{
		"vrf.skey":    []byte("vrf"),
		"kes.skey":    []byte("kes-2"),
		"opcert.cert": []byte("cert-2"),
		"extra":       []byte("x"),
	}
	assert.NotEqual(t, sum, staticKeysChecksum(withExtra),
		"any entry but the KES key and opcert is static")
	nodeCert := map[string][]byte{
		"vrf.skey":  []byte("vrf"),
		"kes.skey":  []byte("kes-2"),
		"node.cert": []byte("cert-2"),
	}
	assert.Equal(t, sum, staticKeysChecksum(nodeCert),
		"the opcert is not static under either file name")
}

// The digest must be what the staging container computes with
// `cat kes.skey <cert> | sha256sum`.
func TestApprovedKeysDigest(t *testing.T) {
	want := sha256.Sum256([]byte("kes" + "cert"))
	for _, name := range []string{"opcert.cert", "node.cert"} {
		data := map[string][]byte{
			"vrf.skey": []byte("vrf"),
			"kes.skey": []byte("kes"),
			name:       []byte("cert"),
		}
		assert.Equal(
			t,
			hex.EncodeToString(want[:]),
			approvedKeysDigest(data),
			name,
		)
	}
}

func TestDecideKeysRollout(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	live := liveKeys{
		exists:   true,
		checksum: "old",
		static:   "static",
		pool:     "pool",
	}
	pendingAt := func(at time.Time, applied bool) *dingov1alpha1.InPlaceReloadStatus {
		return &dingov1alpha1.InPlaceReloadStatus{
			Checksum:    "new",
			Counter:     4,
			RequestedAt: metav1.NewTime(at),
			Applied:     applied,
		}
	}
	tests := []struct {
		name         string
		enabled      bool
		live         liveKeys
		static, pool string
		// loaded is the counter the node reports; nil defaults to 3,
		// unless unobserved is set.
		loaded       *int64
		unobserved   bool
		prev         *dingov1alpha1.InPlaceReloadStatus
		wantChecksum string
		wantStatic   string
		wantApprove  bool
		wantPending  bool
		wantApplied  bool
		wantTimedOut bool
	}{
		{
			name:         "disabled rolls as before",
			enabled:      false,
			live:         live,
			static:       "static",
			pool:         "pool",
			wantChecksum: "new",
		},
		{
			name:         "no statefulset yet",
			enabled:      true,
			live:         liveKeys{},
			static:       "static",
			pool:         "pool",
			wantChecksum: "new",
			wantStatic:   "static",
			wantApprove:  true,
		},
		{
			name:    "bundle already staged",
			enabled: true,
			live: liveKeys{
				exists:   true,
				checksum: "new",
				static:   "static",
				pool:     "pool",
			},
			static:       "static",
			pool:         "pool",
			prev:         pendingAt(now.Add(-time.Minute), true),
			wantChecksum: "new",
			wantStatic:   "static",
			wantApprove:  true,
		},
		{
			name:         "KES rotation loads in place",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
		},
		{
			name:         "pending reload keeps waiting",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			prev:         pendingAt(now.Add(-time.Minute), false),
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
		},
		{
			name:         "confirmed reload keeps the pod",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			prev:         pendingAt(now.Add(-time.Hour), true),
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
			wantApplied:  true,
		},
		{
			name:    "unconfirmed reload rolls after the timeout",
			enabled: true,
			live:    live,
			static:  "static",
			pool:    "pool",
			prev: pendingAt(
				now.Add(-keysReloadTimeout-time.Second),
				false,
			),
			wantChecksum: "new",
			wantStatic:   "static",
			wantApprove:  true,
			wantTimedOut: true,
		},
		{
			name:         "a new VRF or vote key rolls",
			enabled:      true,
			live:         live,
			static:       "static-2",
			pool:         "pool",
			wantChecksum: "new",
			wantStatic:   "static-2",
		},
		{
			name:         "a new pool rolls",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool-2",
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			name:    "a template rendered without reload in place rolls once",
			enabled: true,
			live: liveKeys{
				exists:   true,
				checksum: "old",
				pool:     "pool",
			},
			static:       "static",
			pool:         "pool",
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			name:         "a bundle reusing the loaded counter rolls",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			loaded:       new(int64(4)),
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			name:         "an unobserved loaded counter rolls",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			unobserved:   true,
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			name:         "a loaded counter of zero is observed",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			loaded:       new(int64(0)),
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
		},
		{
			name:         "a pending reload is kept once the counter loads",
			enabled:      true,
			live:         live,
			static:       "static",
			pool:         "pool",
			loaded:       new(int64(4)),
			prev:         pendingAt(now.Add(-time.Minute), true),
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
			wantApplied:  true,
		},
		{
			name:    "a newer bundle replaces the pending one",
			enabled: true,
			live:    live,
			static:  "static",
			pool:    "pool",
			prev: &dingov1alpha1.InPlaceReloadStatus{
				Checksum:    "older",
				Counter:     3,
				RequestedAt: metav1.NewTime(now.Add(-time.Hour)),
			},
			wantChecksum: "old",
			wantStatic:   "static",
			wantApprove:  true,
			wantPending:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loaded := tc.loaded
			if loaded == nil && !tc.unobserved {
				loaded = new(int64(3))
			}
			got := decideKeysRollout(
				tc.enabled, tc.live, "new", tc.static, tc.pool, 4, loaded,
				tc.prev, now,
			)
			assert.Equal(t, tc.wantChecksum, got.checksum)
			assert.Equal(t, tc.wantStatic, got.static)
			assert.Equal(t, tc.wantApprove, got.approve)
			assert.Equal(t, tc.wantTimedOut, got.timedOut)
			if !tc.wantPending {
				assert.Nil(t, got.pending)
				return
			}
			require.NotNil(t, got.pending)
			assert.Equal(t, "new", got.pending.Checksum)
			assert.Equal(t, int64(4), got.pending.Counter)
			assert.Equal(t, tc.wantApplied, got.pending.Applied)
		})
	}
}

func TestObserveLoadedOpCert(t *testing.T) {
	pending := func() *dingov1alpha1.InPlaceReloadStatus {
		return &dingov1alpha1.InPlaceReloadStatus{Checksum: "new", Counter: 4}
	}
	tests := []struct {
		name        string
		pending     *dingov1alpha1.InPlaceReloadStatus
		st          forgestatus.Status
		wantLoaded  int64
		wantApplied bool
		wantEvent   bool
	}{
		{
			name:    "gauge absent",
			pending: pending(),
			st:      forgestatus.Status{},
		},
		{
			name:    "old counter still loaded",
			pending: pending(),
			st: forgestatus.Status{
				OpCertCounter:    3,
				HasOpCertCounter: true,
			},
			wantLoaded: 3,
		},
		{
			name:    "approved counter loaded",
			pending: pending(),
			st: forgestatus.Status{
				OpCertCounter:    4,
				HasOpCertCounter: true,
			},
			wantLoaded:  4,
			wantApplied: true,
			wantEvent:   true,
		},
		{
			name: "nothing pending",
			st: forgestatus.Status{
				OpCertCounter:    4,
				HasOpCertCounter: true,
			},
			wantLoaded: 4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := events.NewFakeRecorder(1)
			r := &DingoNodeReconciler{Recorder: recorder}
			dn := &dingov1alpha1.DingoNode{}
			dn.Status.OpCert.InPlaceReload = tc.pending
			r.observeLoadedOpCert(dn, &tc.st)
			assert.Equal(t, tc.wantLoaded, ptr.Deref(dn.Status.OpCert.LoadedCounter, 0))
			if tc.pending != nil {
				assert.Equal(
					t,
					tc.wantApplied,
					dn.Status.OpCert.InPlaceReload.Applied,
				)
			}
			assert.Equal(t, tc.wantEvent, len(recorder.Events) == 1)
		})
	}
}
