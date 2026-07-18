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

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
)

// validateSpec enforces cross-field invariants the CRD OpenAPI schema cannot
// express on its own.
func validateSpec(dn *dingov1alpha1.DingoNode) error {
	if dn.Spec.Network == "custom" && dn.Spec.NetworkMagic == nil {
		return errors.New("network \"custom\" requires spec.networkMagic")
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
		if bp.Rotation.Mode == dingov1alpha1.RotationModeAuto {
			cs := bp.Rotation.ColdSigner
			switch cs.Type {
			case dingov1alpha1.ColdSignerBursa:
				if cs.Endpoint == "" {
					return errors.New(
						"rotation mode Auto with cold signer bursa requires coldSigner.endpoint",
					)
				}
			case dingov1alpha1.ColdSignerSecret:
				if cs.SecretRef == "" {
					return errors.New(
						"rotation mode Auto with cold signer secret requires coldSigner.secretRef",
					)
				}
			case dingov1alpha1.ColdSignerNone, "":
				return errors.New(
					"rotation mode Auto requires a cold signer (bursa or secret)",
				)
			}
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

var _ = fmt.Sprintf // reserved for future validation messages
