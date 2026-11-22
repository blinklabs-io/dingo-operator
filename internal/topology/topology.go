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

// defaultRelayPort is the node-to-node port of a peer whose own port is not
// known (a relayRefs entry with no DingoNode of that name).
const defaultRelayPort = 3001

// PeerSnapshotFileName is the fixed key and mounted filename of a configured
// peer snapshot.
const PeerSnapshotFileName = "peer-snapshot.json"

// accessPoint is a single peer address.
type accessPoint struct {
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

// localRoot is a group of trusted/advertised peers.
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
	PeerSnapshotFile   string        `json:"peerSnapshotFile,omitempty"`
	PublicRoots        []publicRoot  `json:"publicRoots"`
	UseLedgerAfterSlot *int64        `json:"useLedgerAfterSlot,omitempty"`
}

// Peer is what Render knows about a relayRefs entry that has a DingoNode:
// the node-to-node port its pods serve and how many pods it runs.
type Peer struct {
	Port     int32
	Replicas int32
}

// InClusterPeerAddress returns the headless-service DNS name for a sibling
// DingoNode in the given namespace. It resolves to every ready or not-ready
// pod, so it names a peer whose pods are not known.
func InClusterPeerAddress(name, namespace string) string {
	return fmt.Sprintf("%s-headless.%s.svc.cluster.local", name, namespace)
}

// InClusterPodAddress returns the DNS name of one pod of a sibling DingoNode.
func InClusterPodAddress(name string, ordinal int32, namespace string) string {
	return fmt.Sprintf(
		"%s-%d.%s-headless.%s.svc.cluster.local",
		name, ordinal, name, namespace,
	)
}

// Render builds a topology.json document for the node. peers maps a relayRefs
// name to its DingoNode's relay port and pod count. A peer with pods is listed
// once per pod, so every pod of it is dialed rather than the one address its
// headless name happens to resolve to. A peer scaled to zero keeps its
// headless name, so the topology never loses its only peers and falls back to
// Dingo's built-in bootstrap peers. A name without an entry is dialed on 3001
// through its headless name. It returns the rendered
// JSON and whether any custom topology content exists. When false, the caller
// should omit the topology so Dingo falls back to its built-in bootstrap peers
// (relays with no explicit topology). Block producers should always configure
// topology (their relays) so they never dial public bootstrap peers.
func Render(
	dn *dingov1alpha1.DingoNode,
	namespace string,
	peers map[string]Peer,
) (string, bool, error) {
	spec := dn.Spec.Topology

	var roots []accessPoint
	// Auto-wire in-cluster relay peers.
	autoPeer := spec.AutoPeerRelays == nil || *spec.AutoPeerRelays
	if autoPeer {
		for _, ref := range spec.RelayRefs {
			peer, ok := peers[ref]
			if !ok {
				peer.Port = defaultRelayPort
			}
			if peer.Replicas < 1 {
				roots = append(roots, accessPoint{
					Address: InClusterPeerAddress(ref, namespace),
					Port:    peer.Port,
				})
				continue
			}
			for i := range peer.Replicas {
				roots = append(roots, accessPoint{
					Address: InClusterPodAddress(ref, i, namespace),
					Port:    peer.Port,
				})
			}
		}
	}

	doc := document{
		BootstrapPeers:     []accessPoint{},
		LocalRoots:         []localRoot{},
		PublicRoots:        []publicRoot{},
		UseLedgerAfterSlot: spec.UseLedgerAfterSlot,
	}
	if spec.PeerSnapshotRef != "" {
		doc.PeerSnapshotFile = PeerSnapshotFileName
	}

	// In-cluster peers form a single trusted local root group.
	if len(roots) > 0 {
		doc.LocalRoots = append(doc.LocalRoots, localRoot{
			AccessPoints: roots,
			Advertise:    false,
			Trustable:    true,
			Valency:      len(roots),
			WarmValency:  len(roots),
		})
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
		doc.PeerSnapshotFile != "" ||
		doc.UseLedgerAfterSlot != nil

	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", false, fmt.Errorf("marshal topology: %w", err)
	}
	return string(out), hasContent, nil
}
