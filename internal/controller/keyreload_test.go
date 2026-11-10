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
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
		{
			// A (counter 4) was approved and may have loaded while scrapes
			// failed, so the loaded counter still reads 3. B reuses counter
			// 4: a later report of 4 could come from A, so B rolls.
			name:    "a bundle reusing an unconfirmed pending counter rolls",
			enabled: true,
			live:    live,
			static:  "static",
			pool:    "pool",
			prev: &dingov1alpha1.InPlaceReloadStatus{
				Checksum:    "older",
				Counter:     4,
				RequestedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			// A (4) and then B (5) were approved unconfirmed; C reuses A's
			// counter, which the node may already be forging with.
			name:    "a bundle below an unconfirmed pending counter rolls",
			enabled: true,
			live:    live,
			static:  "static",
			pool:    "pool",
			prev: &dingov1alpha1.InPlaceReloadStatus{
				Checksum:    "older",
				Counter:     5,
				RequestedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
			wantChecksum: "new",
			wantStatic:   "static",
		},
		{
			name:    "a bundle replacing a confirmed reload loads in place",
			enabled: true,
			live:    live,
			static:  "static",
			pool:    "pool",
			loaded:  new(int64(3)),
			prev: &dingov1alpha1.InPlaceReloadStatus{
				Checksum:    "older",
				Counter:     3,
				RequestedAt: metav1.NewTime(now.Add(-time.Hour)),
				Applied:     true,
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
				nil, tc.prev, now,
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

// TestKeysReloadStaleBaseline walks the sequence where an approved bundle loads
// while metrics scrapes fail, then a second bundle with the same counter
// arrives. The second bundle must not be approved in place, so the first
// successful scrape cannot confirm it before it loads.
func TestKeysReloadStaleBaseline(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	live := liveKeys{
		exists:   true,
		checksum: "live",
		static:   "static",
		pool:     "pool",
	}
	dn := &dingov1alpha1.DingoNode{}
	dn.Status.OpCert.LoadedCounter = new(int64(3))

	// Bundle A, counter 4, is approved for an in-place reload.
	a := decideKeysRollout(
		true, live, "bundle-a", "static", "pool", 4,
		dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
		dn.Status.OpCert.InPlaceReload, now,
	)
	require.NotNil(t, a.pending)
	require.True(t, a.approve)
	dn.Status.OpCert.InPlaceReload = a.pending

	// A loads, but every scrape fails: LoadedCounter stays 3. Bundle B, also
	// counter 4, arrives.
	b := decideKeysRollout(
		true, live, "bundle-b", "static", "pool", 4,
		dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
		dn.Status.OpCert.InPlaceReload, now.Add(time.Minute),
	)
	assert.Equal(t, "bundle-b", b.checksum, "B rolls the pod")
	assert.False(t, b.approve, "B is not approved for staging in place")
	assert.Nil(t, b.pending)
	dn.Status.OpCert.InPlaceReload = b.pending

	// The first successful scrape reports A's counter. Nothing may record B
	// as loaded.
	r := &DingoNodeReconciler{Recorder: events.NewFakeRecorder(1)}
	r.observeLoadedOpCert(dn, &forgestatus.Status{
		OpCertCounter:    4,
		HasOpCertCounter: true,
	})
	assert.Nil(t, dn.Status.OpCert.InPlaceReload)
}

// TestKeysReloadAfterRoll walks the sequence where the pod rolls onto a bundle
// and the scrapes of the new pod fail, so the loaded counter still holds the
// outgoing pod's. A next bundle reusing the counter rolled onto must not be
// approved in place: the new pod reports that counter from the bundle it
// started with, which would confirm a reload that never happened.
func TestKeysReloadAfterRoll(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	dn := &dingov1alpha1.DingoNode{}
	dn.Status.OpCert.LoadedCounter = new(int64(3))

	// Bundle 1, counter 4, changes the VRF key, so the pod rolls onto it.
	live := liveKeys{
		exists:   true,
		checksum: "bundle-0",
		static:   "static",
		pool:     "pool",
	}
	b1 := decideKeysRollout(
		true, live, "bundle-1", "static-2", "pool", 4,
		dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
		dn.Status.OpCert.InPlaceReload, now,
	)
	require.Equal(t, "bundle-1", b1.checksum, "bundle 1 rolls the pod")
	dn.Status.OpCert.InPlaceReload = b1.pending
	dn.Status.OpCert.RolledCounter = b1.rolled

	// The new pod staged bundle 1 at start, but every scrape fails: the
	// loaded counter stays 3. Bundle 2, also counter 4, arrives.
	live = liveKeys{
		exists:   true,
		checksum: "bundle-1",
		static:   "static-2",
		pool:     "pool",
	}
	b2 := decideKeysRollout(
		true, live, "bundle-2", "static-2", "pool", 4,
		dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
		dn.Status.OpCert.InPlaceReload, now.Add(time.Minute),
	)
	assert.Equal(t, "bundle-2", b2.checksum, "bundle 2 rolls the pod")
	assert.False(t, b2.approve, "bundle 2 is not approved for staging in place")
	assert.Nil(t, b2.pending)
	dn.Status.OpCert.InPlaceReload = b2.pending

	// The first successful scrape reports bundle 1's counter. Nothing may
	// record bundle 2 as loaded.
	r := &DingoNodeReconciler{Recorder: events.NewFakeRecorder(1)}
	r.observeLoadedOpCert(dn, &forgestatus.Status{
		OpCertCounter:    4,
		HasOpCertCounter: true,
	})
	assert.Nil(t, dn.Status.OpCert.InPlaceReload)
}

// TestKeysReloadAfterRollLostStatus walks a roll whose status write is lost:
// the StatefulSet rolls onto bundle A, counter 4, but rolledCounter and the
// cleared reload are never persisted, so the status still holds the outgoing
// pod's counter 3. Bundle B, also counter 4, then arrives. The rolled counter
// is recovered from the live template, so B rolls the pod, and the new pod
// reporting A's counter cannot mark B applied.
func TestKeysReloadAfterRollLostStatus(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	// The template the StatefulSet rolled onto: bundle A with its counter.
	live := liveKeys{
		exists:   true,
		checksum: "bundle-a",
		static:   "static",
		pool:     "pool",
		counter:  new(int64(4)),
	}
	r := &DingoNodeReconciler{Recorder: events.NewFakeRecorder(1)}

	t.Run("a reused counter rolls", func(t *testing.T) {
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		b := decideKeysRollout(
			true, live, "bundle-b", "static", "pool", 4,
			dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
			dn.Status.OpCert.InPlaceReload, now,
		)
		assert.Equal(t, "bundle-b", b.checksum, "B rolls the pod")
		assert.False(t, b.approve, "B is not approved for staging in place")
		assert.Nil(t, b.pending)
		dn.Status.OpCert.InPlaceReload = b.pending

		// The new pod reports A's counter. Nothing may record B as loaded.
		r.observeLoadedOpCert(dn, &forgestatus.Status{
			OpCertCounter:    4,
			HasOpCertCounter: true,
		})
		assert.Nil(t, dn.Status.OpCert.InPlaceReload)
	})

	t.Run("a higher counter loads in place", func(t *testing.T) {
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		b := decideKeysRollout(
			true, live, "bundle-b", "static", "pool", 5,
			dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
			dn.Status.OpCert.InPlaceReload, now,
		)
		assert.Equal(t, "bundle-a", b.checksum, "the pod keeps running")
		assert.True(t, b.approve)
		require.NotNil(t, b.pending)
		assert.Equal(t, int64(5), b.pending.Counter)
	})

	t.Run("the template counter outranks a stale status value", func(t *testing.T) {
		// rolledCounter still holds the counter of an earlier roll.
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		dn.Status.OpCert.RolledCounter = new(int64(2))
		b := decideKeysRollout(
			true, live, "bundle-b", "static", "pool", 4,
			dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
			dn.Status.OpCert.InPlaceReload, now,
		)
		assert.Equal(t, "bundle-b", b.checksum, "B rolls the pod")
		assert.Nil(t, b.pending)
	})
}

// TestLiveKeysTemplateCounter covers reading the rolled counter from the live
// pod template.
func TestLiveKeysTemplateCounter(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, dingov1alpha1.AddToScheme(scheme))
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        *int64
	}{
		{
			name: "recorded",
			annotations: map[string]string{
				resources.KeysCounterAnnotation: "4",
			},
			want: new(int64(4)),
		},
		{
			name: "zero is a real counter",
			annotations: map[string]string{
				resources.KeysCounterAnnotation: "0",
			},
			want: new(int64(0)),
		},
		{name: "absent"},
		{
			name: "unparsable",
			annotations: map[string]string{
				resources.KeysCounterAnnotation: "four",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "ns"},
			}
			sts.Spec.Template.Annotations = tc.annotations
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(sts).Build()
			r := &DingoNodeReconciler{Client: c}
			dn := &dingov1alpha1.DingoNode{
				ObjectMeta: metav1.ObjectMeta{Name: "bp", Namespace: "ns"},
			}
			live, err := r.liveKeysTemplate(context.Background(), dn)
			require.NoError(t, err)
			assert.Equal(t, tc.want, live.counter)
		})
	}
}

// TestKeysReloadAfterRollOutgoingScrape covers scrapes that still reach the
// outgoing pod after a roll. A counter other than the rolled one leaves the
// guard in place; a bundle above it loads in place, and its confirmation ends
// the guard.
func TestKeysReloadAfterRollOutgoingScrape(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	r := &DingoNodeReconciler{Recorder: events.NewFakeRecorder(2)}
	report := func(dn *dingov1alpha1.DingoNode, counter int64) {
		r.observeLoadedOpCert(dn, &forgestatus.Status{
			OpCertCounter:    counter,
			HasOpCertCounter: true,
		})
	}
	live := liveKeys{
		exists:   true,
		checksum: "bundle-1",
		static:   "static",
		pool:     "pool",
	}
	decide := func(dn *dingov1alpha1.DingoNode, full string, counter int64) keysRollout {
		out := decideKeysRollout(
			true, live, full, "static", "pool", counter,
			dn.Status.OpCert.LoadedCounter, dn.Status.OpCert.RolledCounter,
			dn.Status.OpCert.InPlaceReload, now,
		)
		dn.Status.OpCert.InPlaceReload = out.pending
		dn.Status.OpCert.RolledCounter = out.rolled
		return out
	}

	t.Run("a different counter keeps the guard", func(t *testing.T) {
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		dn.Status.OpCert.RolledCounter = new(int64(4))
		report(dn, 3)
		require.NotNil(t, dn.Status.OpCert.RolledCounter)
		out := decide(dn, "bundle-2", 4)
		assert.Equal(t, "bundle-2", out.checksum, "a reused counter rolls")
	})

	t.Run("a higher counter loads in place and clears", func(t *testing.T) {
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		dn.Status.OpCert.RolledCounter = new(int64(4))
		out := decide(dn, "bundle-2", 5)
		assert.Equal(t, "bundle-1", out.checksum, "the pod keeps running")
		require.NotNil(t, out.pending)
		require.NotNil(t, dn.Status.OpCert.RolledCounter)
		assert.Equal(t, int64(4), *dn.Status.OpCert.RolledCounter)
		report(dn, 4)
		assert.Nil(t, dn.Status.OpCert.RolledCounter)
		assert.False(t, dn.Status.OpCert.InPlaceReload.Applied)
		report(dn, 5)
		assert.True(t, dn.Status.OpCert.InPlaceReload.Applied)
	})

	t.Run("confirming a reload clears the guard", func(t *testing.T) {
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(3))
		dn.Status.OpCert.RolledCounter = new(int64(4))
		decide(dn, "bundle-2", 5)
		report(dn, 5)
		assert.True(t, dn.Status.OpCert.InPlaceReload.Applied)
		assert.Nil(t, dn.Status.OpCert.RolledCounter)
	})

	t.Run("an equal outgoing counter still rolls a reuse", func(t *testing.T) {
		// The pod rolled onto a bundle with the counter it already had.
		// The outgoing pod's report clears the guard, and the loaded
		// counter check refuses a bundle reusing it.
		dn := &dingov1alpha1.DingoNode{}
		dn.Status.OpCert.LoadedCounter = new(int64(4))
		dn.Status.OpCert.RolledCounter = new(int64(4))
		report(dn, 4)
		assert.Nil(t, dn.Status.OpCert.RolledCounter)
		out := decide(dn, "bundle-2", 4)
		assert.Equal(t, "bundle-2", out.checksum)
		assert.Nil(t, out.pending)
	})
}

// TestDecideKeysRolloutRolledCounter covers when the rolled counter is set
// and when it is carried forward.
func TestDecideKeysRolloutRolledCounter(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	live := liveKeys{
		exists:   true,
		checksum: "old",
		static:   "static",
		pool:     "pool",
	}
	tests := []struct {
		name    string
		enabled bool
		live    liveKeys
		static  string
		prev    *dingov1alpha1.InPlaceReloadStatus
		rolled  *int64
		// loaded defaults to 1.
		loaded *int64
		want   *int64
	}{
		{
			name:    "a first render starts the pod on the bundle",
			enabled: true,
			live:    liveKeys{},
			static:  "static",
			want:    new(int64(4)),
		},
		{
			name:    "a roll records the bundle counter",
			enabled: true,
			live:    live,
			static:  "static-2",
			rolled:  new(int64(2)),
			want:    new(int64(4)),
		},
		{
			name:    "reload in place disabled records nothing",
			enabled: false,
			live:    live,
			static:  "static",
			rolled:  new(int64(2)),
		},
		{
			name:    "a timed out reload rolls and records it",
			enabled: true,
			live:    live,
			static:  "static",
			prev: &dingov1alpha1.InPlaceReloadStatus{
				Checksum:    "new",
				Counter:     4,
				RequestedAt: metav1.NewTime(now.Add(-keysReloadTimeout - time.Second)),
			},
			want: new(int64(4)),
		},
		{
			name:    "an in-place load keeps the previous value",
			enabled: true,
			live:    live,
			static:  "static",
			rolled:  new(int64(2)),
			want:    new(int64(2)),
		},
		{
			name:    "a staged bundle keeps the previous value",
			enabled: true,
			live: liveKeys{
				exists:   true,
				checksum: "new",
				static:   "static",
				pool:     "pool",
			},
			static: "static",
			rolled: new(int64(4)),
			want:   new(int64(4)),
		},
		{
			// The status write after the roll was lost: the template
			// staged this bundle and the node reports another counter.
			name:    "a staged bundle restores a lost value",
			enabled: true,
			live: liveKeys{
				exists:   true,
				checksum: "new",
				static:   "static",
				pool:     "pool",
			},
			static: "static",
			want:   new(int64(4)),
		},
		{
			name:    "a staged bundle the node reports records nothing",
			enabled: true,
			live: liveKeys{
				exists:   true,
				checksum: "new",
				static:   "static",
				pool:     "pool",
			},
			static: "static",
			loaded: new(int64(4)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loaded := tc.loaded
			if loaded == nil {
				loaded = new(int64(1))
			}
			got := decideKeysRollout(
				tc.enabled, tc.live, "new", tc.static, "pool", 4,
				loaded, tc.rolled, tc.prev, now,
			)
			assert.Equal(t, tc.want, got.rolled)
		})
	}
}
