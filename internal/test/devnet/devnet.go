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

package devnet

import (
	"fmt"
	"io"
	"time"
)

// DevNet is one throwaway single-pool Cardano network: the pool's key
// material and the configuration bundle a Dingo node starts against.
type DevNet struct {
	Keys        *PoolKeys
	Params      Params
	ConfigFiles map[string]string
}

// Generate mints a fresh pool key set from r and renders a genesis bundle
// whose chain begins at systemStart. Leave enough headroom for the node to
// come up before that instant, or it starts mid-chain with no blocks to
// build on.
func Generate(r io.Reader, systemStart time.Time) (*DevNet, error) {
	keys, err := GeneratePoolKeys(r)
	if err != nil {
		return nil, fmt.Errorf("generate pool keys: %w", err)
	}
	params := DefaultParams()
	// Genesis records whole seconds, so pin Params to the instant the
	// rendered files actually encode.
	params.SystemStart = systemStart.UTC().Truncate(time.Second)
	files, err := RenderGenesis(params, keys)
	if err != nil {
		return nil, fmt.Errorf("render genesis: %w", err)
	}
	return &DevNet{
		Keys:        keys,
		Params:      params,
		ConfigFiles: files,
	}, nil
}
