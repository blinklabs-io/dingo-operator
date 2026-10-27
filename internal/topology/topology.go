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

// Package topology renders Dingo/Cardano P2P topology.json documents from a
// DingoNode spec, merging auto-wired in-cluster peers with static external
// relays and passthrough bootstrap/public roots.
package topology

import (
	"encoding/json"
	"fmt"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
)

// relayNtNPort is the node-to-node port used for in-cluster peer access points.
const relayNtNPort = 3001

// accessPoint is a single peer address.
type accessPoint struct {
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

// localRoot is a group of trusted/advertised peers. Dingo counts each group as
// one witness for Genesis corroboration.
type localRoot struct {
	AccessPoints []accessPoint `json:"accessPoints"`
	Advertise    bool          `json:"advertise"`
	Trustable    bool          `json:"trustable"`
	Valency      int           `json:"valency"`
	WarmValency  int           `json:"warmValency,omitempty"`
}

// publicRoot is a group of public (non-trusted) peers.
type publicRoot struct {
	AccessPoints []accessPoint `json:"accessPoints"`
	Advertise    bool          `json:"advertise"`
}

// document is the rendered topology.json shape.
type document struct {
	BootstrapPeers     []accessPoint `json:"bootstrapPeers"`
	LocalRoots         []localRoot   `json:"localRoots"`
	PublicRoots        []publicRoot  `json:"publicRoots"`
	UseLedgerAfterSlot *int64        `json:"useLedgerAfterSlot,omitempty"`
}

// InClusterPeerAddress returns the headless-service DNS name for a sibling
// DingoNode relay in the given namespace.
func InClusterPeerAddress(name, namespace string) string {
	return fmt.Sprintf("%s-headless.%s.svc.cluster.local", name, namespace)
}

// Render builds a topology.json document for the node. It returns the rendered
// JSON and whether any custom topology content exists. When false, the caller
// should omit the topology so Dingo falls back to its built-in bootstrap peers
// (relays with no explicit topology). Block producers should always configure
// topology (their relays) so they never dial public bootstrap peers.
func Render(
	dn *dingov1alpha1.DingoNode,
	namespace string,
) (string, bool, error) {
	spec := dn.Spec.Topology

	doc := document{
		BootstrapPeers:     []accessPoint{},
		LocalRoots:         []localRoot{},
		PublicRoots:        []publicRoot{},
		UseLedgerAfterSlot: spec.UseLedgerAfterSlot,
	}

	// Each in-cluster relay becomes its own trusted local root group with
	// valency 1. Dingo's Genesis corroboration counts one group as one
	// witness, so folding all relays into one group would leave a single
	// witness regardless of how many relays are listed.
	autoPeer := spec.AutoPeerRelays == nil || *spec.AutoPeerRelays
	if autoPeer {
		for _, ref := range spec.RelayRefs {
			doc.LocalRoots = append(doc.LocalRoots, localRoot{
				AccessPoints: []accessPoint{{
					Address: InClusterPeerAddress(ref, namespace),
					Port:    relayNtNPort,
				}},
				Advertise:   false,
				Trustable:   true,
				Valency:     1,
				WarmValency: 1,
			})
		}
	}

	// Each external relay becomes its own local root so per-peer valency and
	// trust/advertise flags are honored.
	for _, r := range spec.ExternalRelays {
		valency := int(r.Valency)
		if valency == 0 {
			valency = 1
		}
		doc.LocalRoots = append(doc.LocalRoots, localRoot{
			AccessPoints: []accessPoint{{Address: r.Address, Port: r.Port}},
			Advertise:    r.Advertise,
			Trustable:    r.Trustable,
			Valency:      valency,
			WarmValency:  valency,
		})
	}

	for _, p := range spec.BootstrapPeers {
		doc.BootstrapPeers = append(
			doc.BootstrapPeers,
			accessPoint{Address: p.Address, Port: p.Port},
		)
	}

	if len(spec.PublicRoots) > 0 {
		pr := publicRoot{Advertise: false}
		for _, p := range spec.PublicRoots {
			pr.AccessPoints = append(
				pr.AccessPoints,
				accessPoint{Address: p.Address, Port: p.Port},
			)
		}
		doc.PublicRoots = append(doc.PublicRoots, pr)
	}

	hasContent := len(doc.LocalRoots) > 0 ||
		len(doc.BootstrapPeers) > 0 ||
		len(doc.PublicRoots) > 0 ||
		doc.UseLedgerAfterSlot != nil

	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", false, fmt.Errorf("marshal topology: %w", err)
	}
	return string(out), hasContent, nil
}
