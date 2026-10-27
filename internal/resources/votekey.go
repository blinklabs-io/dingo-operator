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

package resources

import (
	"fmt"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
)

const (
	// VoteKeyFile is the key in the block-producer keys Secret holding the
	// optional Leios BLS vote signing key.
	VoteKeyFile = "vote.skey"
	// VoteKeyEnv is the Dingo setting naming the Leios vote signing key file.
	VoteKeyEnv = "DINGO_LEIOS_VOTE_SIGNING_KEY_FILE"
	// VoteKeyPath is where the staged vote key is mounted in the node.
	VoteKeyPath = keysMountPath + "/" + VoteKeyFile
)

// ValidateKeyEnv rejects a spec.environment override of the vote key path on a
// block producer. The operator stages the key at VoteKeyPath only; a different
// path would silently leave the staged key unused, or point Dingo at a file
// that does not exist.
func ValidateKeyEnv(dn *dingov1alpha1.DingoNode) error {
	if !IsBlockProducer(dn) {
		return nil
	}
	if v, ok := dn.Spec.Environment[VoteKeyEnv]; ok && v != VoteKeyPath {
		return fmt.Errorf(
			"%s is managed by the operator and must be %s or unset; "+
				"deliver the key as %s in the keys Secret",
			VoteKeyEnv, VoteKeyPath, VoteKeyFile,
		)
	}
	return nil
}
