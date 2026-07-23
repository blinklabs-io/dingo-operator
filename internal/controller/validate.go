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
	"errors"
	"fmt"
	"sort"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// validateSpec enforces cross-field invariants the CRD OpenAPI schema cannot
// express on its own.
func validateSpec(dn *dingov1alpha1.DingoNode) error {
	// Guard against an object with no (or an incomplete) spec. The CRD requires
	// role/network only when spec is present, so a bare `kind: DingoNode` would
	// otherwise reconcile with empty values and be treated as a relay.
	switch dn.Spec.Role {
	case dingov1alpha1.RoleRelay, dingov1alpha1.RoleBlockProducer:
	default:
		return fmt.Errorf(
			"spec.role must be %q or %q",
			dingov1alpha1.RoleRelay,
			dingov1alpha1.RoleBlockProducer,
		)
	}
	if dn.Spec.Network == "" {
		return errors.New("spec.network is required")
	}

	if dn.Spec.Network == "custom" && dn.Spec.NetworkMagic == nil {
		return errors.New("network \"custom\" requires spec.networkMagic")
	}

	// A "custom" network has no genesis built into Dingo, so it can only start
	// when a config bundle (config.json + genesis files) is supplied. Reject it
	// at admission rather than letting the pod crash-loop on a missing genesis.
	if dn.Spec.Network == "custom" && dn.Spec.ConfigRef == "" {
		return errors.New(
			"network \"custom\" requires spec.configRef (a ConfigMap holding " +
				"config.json and its genesis files)",
		)
	}

	// A config bundle implies a custom genesis, which has no auto-detectable
	// Mithril aggregator. The Mithril bootstrap init container would then
	// crash-loop resolving a non-existent aggregator, blocking pod start. Require
	// the user to either point Mithril at an aggregator or disable it explicitly.
	if dn.Spec.ConfigRef != "" &&
		(dn.Spec.Mithril.Enabled == nil || *dn.Spec.Mithril.Enabled) &&
		dn.Spec.Mithril.AggregatorURL == "" {
		return errors.New(
			"spec.configRef requires spec.mithril.aggregatorUrl to be set or " +
				"spec.mithril.enabled=false; a custom network has no " +
				"auto-detected Mithril aggregator",
		)
	}

	if dn.Spec.Role == dingov1alpha1.RoleBlockProducer {
		bp := dn.Spec.BlockProducer
		if bp == nil {
			return errors.New("role blockProducer requires spec.blockProducer")
		}
		if bp.Keys.SecretRef == "" {
			return errors.New(
				"role blockProducer requires spec.blockProducer.keys.secretRef",
			)
		}
		// Only the native Secret backend is implemented. Reject the reserved
		// ExternalSecret/CSI values rather than accepting them and silently
		// mounting a plain Secret (or failing the pod when the referent is
		// absent).
		if st := bp.Keys.SourceType; st != "" &&
			st != dingov1alpha1.KeySourceSecret {
			return fmt.Errorf(
				"keys.sourceType %q is not supported in this operator "+
					"version; only Secret is implemented",
				st,
			)
		}
		// Reject configuration for features that are not implemented yet, rather
		// than accepting them and silently doing nothing. A block producer set
		// to Auto rotation or ActiveStandby would otherwise appear healthy while
		// its KES key drifts toward expiry with no rotation, or while the
		// operator provides no failover at all. These branches are relaxed when
		// the P2/P3 rotation and HA pipelines land (cold-signer validation
		// returns here for Auto at that point).
		if bp.Rotation.Mode == dingov1alpha1.RotationModeAuto {
			return errors.New(
				"rotation mode Auto is not supported in this operator version; " +
					"use MonitorOnly or Assisted",
			)
		}
		if bp.HA.Strategy == dingov1alpha1.HAActiveStandby {
			return errors.New(
				"ha strategy ActiveStandby is not supported in this operator " +
					"version; use SingleActive",
			)
		}
	}
	return nil
}

// checksum returns a short, stable hex digest used to trigger rollouts when
// derived config (topology, keys) changes.
func checksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// keysChecksum returns a stable digest of a keys Secret's Data. Populating the
// pod-template annotation with this value rolls the StatefulSet when key
// material changes.
func keysChecksum(secret *corev1.Secret) string {
	return checksumData(secret.Data)
}

// configChecksum returns a stable digest of a config-bundle ConfigMap's
// contents (both Data and BinaryData). Populating the pod-template annotation
// with this value rolls the StatefulSet when the mounted config.json or genesis
// files change.
func configChecksum(cm *corev1.ConfigMap) string {
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for k, v := range cm.Data {
		data[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		data[k] = v
	}
	return checksumData(data)
}

// checksumData returns a stable, order-independent digest of a set of named
// byte values. Keys are sorted, and each key and value is emitted with an
// explicit byte-length prefix ("<len>:<bytes>", netstring style), so arbitrary
// bytes — including embedded NUL, ":" or digits — cannot be re-partitioned into
// a different set that hashes the same (which a plain NUL separator would
// allow).
func checksumData(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	writeField := func(p []byte) {
		fmt.Fprintf(&b, "%d:", len(p))
		b.Write(p)
	}
	for _, k := range keys {
		writeField([]byte(k))
		writeField(data[k])
	}
	return checksum(b.String())
}
