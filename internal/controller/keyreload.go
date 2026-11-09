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
	"fmt"
	"maps"
	"time"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// keysReloadTimeout bounds how long the node may take to report an approved
// opcert loaded in place before the pod is rolled onto it instead. It covers
// the kubelet refreshing the Secret volume, the staging container's poll,
// Dingo's credential file watch and the operator's own scrape interval.
const keysReloadTimeout = 10 * time.Minute

// staticKeysChecksum returns the checksum of everything in a keys Secret that
// an in-place reload cannot change: every entry except kes.skey and the opcert.
// That covers the VRF key and the optional vote key, and any other entry, so a
// change to anything but the KES key and opcert rolls the pod. The vote key is
// hashed under one name, whichever file it came from.
func staticKeysChecksum(data map[string][]byte) string {
	static := maps.Clone(data)
	delete(static, keyFileKES)
	delete(static, keyFileOpCert)
	delete(static, keyFileNodeCert)
	delete(static, keyFileVote)
	delete(static, keyFileBLS)
	if static == nil {
		static = map[string][]byte{}
	}
	static[keyFileVRF] = data[keyFileVRF]
	if vote, _, _ := voteKeyFile(data); vote != nil {
		static[resources.VoteKeyFile] = vote
	}
	return checksumData(static)
}

// approvedKeysDigest returns the hex SHA-256 of a bundle's kes.skey followed by
// its opcert, the digest the staging container compares the Secret volume with
// before it stages them.
func approvedKeysDigest(data map[string][]byte) string {
	cert, _, _ := opCertFile(data)
	h := sha256.New()
	h.Write(data[keyFileKES])
	h.Write(cert)
	return hex.EncodeToString(h.Sum(nil))
}

// liveKeys is what the live pod template records about the keys it staged.
type liveKeys struct {
	exists   bool
	checksum string
	static   string
	pool     string
}

// keysRollout is how a validated key bundle reaches the node.
type keysRollout struct {
	// checksum and static are the keys annotations to render. A checksum equal
	// to the live one leaves the pod running.
	checksum string
	static   string
	// approve writes the bundle's digest to the approval ConfigMap, so the
	// staging container may stage it into the running pod.
	approve bool
	// pending is the status to record for the in-place reload; nil clears it.
	pending *dingov1alpha1.InPlaceReloadStatus
	// timedOut reports that an approved reload was not confirmed in time and
	// the pod is rolled instead.
	timedOut bool
}

// decideKeysRollout chooses between rolling the pod onto a validated bundle and
// loading it in place. A bundle loads in place only when the node reloads keys
// in place and the live template staged the same VRF key, vote key and pool, so
// only the KES key and opcert differ. Until the node reports the new opcert
// counter the live checksum is kept; after keysReloadTimeout the pod rolls.
//
// The reload is confirmed by the node reporting the bundle's counter, so it
// starts only when the node is observed (loaded is non-nil) forging with a
// different counter. A bundle that reuses the loaded counter, or one approved
// while the loaded counter is unknown, could never be told apart from the keys
// already running, so it rolls the pod instead.
func decideKeysRollout(
	enabled bool,
	live liveKeys,
	full, static, pool string,
	counter int64,
	loaded *int64,
	prev *dingov1alpha1.InPlaceReloadStatus,
	now time.Time,
) keysRollout {
	if !enabled {
		return keysRollout{checksum: full}
	}
	roll := keysRollout{checksum: full, static: static}
	if !live.exists || live.checksum == "" || live.checksum == full {
		// Nothing to load: the pod stages this bundle when it starts.
		roll.approve = true
		return roll
	}
	inPlace := live.static != "" && live.static == static &&
		live.pool != "" && live.pool == pool
	if !inPlace {
		return roll
	}
	pending := prev
	if pending == nil || pending.Checksum != full {
		if loaded == nil || *loaded == counter {
			return roll
		}
		pending = &dingov1alpha1.InPlaceReloadStatus{
			Checksum:    full,
			Counter:     counter,
			RequestedAt: metav1.NewTime(now),
		}
	}
	if !pending.Applied &&
		now.Sub(pending.RequestedAt.Time) > keysReloadTimeout {
		roll.approve = true
		roll.timedOut = true
		return roll
	}
	return keysRollout{
		checksum: live.checksum,
		static:   live.static,
		approve:  true,
		pending:  pending,
	}
}

// liveKeysTemplate reads what the live pod template records about its keys, so
// a render after a rejected or missing key bundle, or during an in-place
// reload, carries the live checksum rather than one of the new material or
// none.
//
// It reads through the cached client, separately from the read
// controllerutil.CreateOrUpdate does in upsertStatefulSet. On an existing
// StatefulSet the rejection path keeps the template that CreateOrUpdate itself
// read (keepTemplate), so the two reads need not agree for the template to stay
// unchanged.
//
// Only a missing StatefulSet yields an empty result (nothing to carry forward,
// and an absent annotation is correct for a first render). Every other read
// failure is returned: an empty checksum is the one value that removes the
// annotation, so an unclassified error must not be indistinguishable from
// absence.
func (r *DingoNodeReconciler) liveKeysTemplate(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
) (liveKeys, error) {
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: dn.Name, Namespace: dn.Namespace}
	if err := r.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return liveKeys{}, nil
		}
		return liveKeys{}, fmt.Errorf("read statefulset for keys: %w", err)
	}
	a := sts.Spec.Template.Annotations
	return liveKeys{
		exists:   true,
		checksum: a[resources.KeysChecksumAnnotation],
		static:   a[resources.KeysStaticChecksumAnnotation],
		pool:     normalizedPoolID(a[resources.KeysPoolAnnotation]),
	}, nil
}

// upsertKeysApprovalConfigMap records the digest of the bundle the staging
// container may stage.
func (r *DingoNodeReconciler) upsertKeysApprovalConfigMap(
	ctx context.Context,
	dn *dingov1alpha1.DingoNode,
	digest string,
) error {
	desired := resources.BuildKeysApprovalConfigMap(dn, digest)
	cm := &corev1.ConfigMap{
		ObjectMeta: objectMeta(desired.Name, desired.Namespace),
	}
	return createOrUpdateWithRetry(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return ctrl.SetControllerReference(dn, cm, r.Scheme)
	})
}

// observeLoadedOpCert records the opcert counter the node reports and confirms
// a pending in-place reload once that counter is the approved one.
func (r *DingoNodeReconciler) observeLoadedOpCert(
	dn *dingov1alpha1.DingoNode,
	st *forgestatus.Status,
) {
	if !st.HasOpCertCounter {
		return
	}
	dn.Status.OpCert.LoadedCounter = new(st.OpCertCounter)
	pending := dn.Status.OpCert.InPlaceReload
	if pending == nil || pending.Applied ||
		st.OpCertCounter != pending.Counter {
		return
	}
	pending.Applied = true
	if r.Recorder != nil {
		r.Recorder.Eventf(
			dn, nil, corev1.EventTypeNormal, "KeysReloadedInPlace",
			"ReloadKeys", "node loaded opcert counter %d without a restart",
			pending.Counter,
		)
	}
}
