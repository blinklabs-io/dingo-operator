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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestKeysChecksum(t *testing.T) {
	base := &corev1.Secret{
		Data: map[string][]byte{
			"kes.skey":    []byte("kes-material"),
			"opcert.cert": []byte("opcert-material"),
			"vrf.skey":    []byte("vrf-material"),
		},
	}

	// Non-empty for a populated Secret.
	sum := keysChecksum(base)
	assert.NotEmpty(t, sum)

	// Order-independent: a Secret with the same data assembled in a different
	// insertion order produces the same checksum (Go map ordering is random,
	// but keysChecksum sorts keys before hashing).
	reordered := &corev1.Secret{
		Data: map[string][]byte{
			"vrf.skey":    []byte("vrf-material"),
			"opcert.cert": []byte("opcert-material"),
			"kes.skey":    []byte("kes-material"),
		},
	}
	assert.Equal(t, sum, keysChecksum(reordered))

	// Changing any value changes the checksum.
	changed := &corev1.Secret{
		Data: map[string][]byte{
			"kes.skey":    []byte("kes-material"),
			"opcert.cert": []byte("opcert-material-ROTATED"),
			"vrf.skey":    []byte("vrf-material"),
		},
	}
	assert.NotEqual(t, sum, keysChecksum(changed))

	// Adding a key changes the checksum (guards against value/key ambiguity).
	added := &corev1.Secret{
		Data: map[string][]byte{
			"kes.skey":    []byte("kes-material"),
			"opcert.cert": []byte("opcert-material"),
			"vrf.skey":    []byte("vrf-material"),
			"extra":       []byte(""),
		},
	}
	assert.NotEqual(t, sum, keysChecksum(added))

	// Length-prefixing must keep NUL-ambiguous inputs distinct. Under a naive
	// "<key>\0<value>\0" scheme these two Secrets both serialize to the bytes
	// m\0\0n\0\0 and would collide (a value containing NUL is indistinguishable
	// from a value boundary). {"m":"","n":""} has two empty-valued keys;
	// {"m":"\0n\0"} has one key whose value is the three bytes NUL 'n' NUL.
	twoEmptyKeys := &corev1.Secret{Data: map[string][]byte{
		"m": {}, "n": {},
	}}
	oneNULValue := &corev1.Secret{Data: map[string][]byte{
		"m": {0, 'n', 0},
	}}
	assert.NotEqual(
		t,
		keysChecksum(twoEmptyKeys),
		keysChecksum(oneNULValue),
		"NUL-ambiguous Secrets must not collide",
	)

	// A NUL embedded in a value vs. the same bytes split across a key boundary.
	embeddedNUL := &corev1.Secret{Data: map[string][]byte{
		"a": {'x', 0, 'b'},
	}}
	splitKeys := &corev1.Secret{Data: map[string][]byte{
		"a": []byte("x"), "b": {},
	}}
	assert.NotEqual(
		t,
		keysChecksum(embeddedNUL),
		keysChecksum(splitKeys),
		"embedded-NUL value must not collide with a split key set",
	)
}

func TestConfigChecksum(t *testing.T) {
	base := &corev1.ConfigMap{
		Data: map[string]string{
			"config.json":          `{"Protocol":"Cardano"}`,
			"shelley-genesis.json": `{"networkMagic":42}`,
		},
	}
	sum := configChecksum(base)
	assert.NotEmpty(t, sum)

	// Stable regardless of insertion order.
	reordered := &corev1.ConfigMap{
		Data: map[string]string{
			"shelley-genesis.json": `{"networkMagic":42}`,
			"config.json":          `{"Protocol":"Cardano"}`,
		},
	}
	assert.Equal(t, sum, configChecksum(reordered))

	// Changing any file changes the checksum (so the pod rolls).
	changed := &corev1.ConfigMap{
		Data: map[string]string{
			"config.json":          `{"Protocol":"Cardano"}`,
			"shelley-genesis.json": `{"networkMagic":99}`,
		},
	}
	assert.NotEqual(t, sum, configChecksum(changed))

	// BinaryData contributes too.
	withBinary := &corev1.ConfigMap{
		Data:       map[string]string{"config.json": `{"Protocol":"Cardano"}`},
		BinaryData: map[string][]byte{"extra.bin": {0x00, 0x01}},
	}
	assert.NotEqual(t, configChecksum(base), configChecksum(withBinary))
}
