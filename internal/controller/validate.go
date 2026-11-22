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
	"maps"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/resources"
	"github.com/blinklabs-io/dingo-operator/internal/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	corev1 "k8s.io/api/core/v1"
)

// validateSpec enforces cross-field invariants the CRD OpenAPI schema cannot
// express on its own.
func validateSpec(dn *dingov1alpha1.DingoNode) error {
	for _, suffix := range []string{"-private", "-relay", "-metrics", "-headless"} {
		if strings.HasSuffix(dn.Name, suffix) {
			return fmt.Errorf(
				"metadata.name must not end in %q; it collides with a managed Service name",
				suffix,
			)
		}
	}
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
	if dn.Spec.Image.CapabilityVersion != "" && dn.Spec.Image.Digest == "" {
		return errors.New(
			"spec.image.capabilityVersion requires spec.image.digest",
		)
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
	if dn.Spec.Topology.PeerSnapshotRef != "" &&
		dn.Spec.Topology.PeerSnapshotRef == resources.TopologyConfigMapName(dn) {
		return errors.New(
			"spec.topology.peerSnapshotRef must not name the operator-managed " +
				"topology ConfigMap",
		)
	}

	// An explicitly enabled bootstrap needs either a network with a built-in
	// aggregator or an explicit URL. Unset bootstrap is disabled automatically
	// on networks without a built-in aggregator.
	if resources.MithrilEnabled(dn) &&
		dn.Spec.Mithril.AggregatorURL == "" &&
		!resources.HasDefaultMithrilAggregator(dn.Spec.Network) {
		return fmt.Errorf(
			"spec.mithril.enabled=true on network %q requires "+
				"spec.mithril.aggregatorUrl; the network has no built-in "+
				"Mithril aggregator",
			dn.Spec.Network,
		)
	}

	// A config bundle can replace a named network's genesis, so it cannot rely
	// on that network's built-in aggregator.
	if dn.Spec.ConfigRef != "" && resources.MithrilEnabled(dn) &&
		dn.Spec.Mithril.AggregatorURL == "" {
		return errors.New(
			"spec.configRef requires spec.mithril.aggregatorUrl to be set or " +
				"Mithril bootstrap to be disabled; a custom genesis has no " +
				"auto-detected Mithril aggregator",
		)
	}

	// An image that predates the tracing options would ignore them, leaving a
	// node that reports tracing as configured while exporting nothing.
	if dn.Spec.Tracing.Enabled && !resources.SupportsTracing(dn) {
		return fmt.Errorf(
			"spec.tracing requires a Dingo image of v0.82.0 or later; set %s",
			imageVersionField(dn),
		)
	}

	if resources.UsesObservabilityExport(dn) &&
		!resources.SupportsObservabilityExport(dn) {
		return fmt.Errorf(
			"spec.tracing.alwaysSamplePools and spec.otlp require a Dingo "+
				"image of v0.91.0 or later; set %s",
			imageVersionField(dn),
		)
	}
	if err := resources.ValidateObservability(dn); err != nil {
		return err
	}

	if dn.Spec.Role == dingov1alpha1.RoleBlockProducer {
		bp := dn.Spec.BlockProducer
		if bp == nil {
			return errors.New("role blockProducer requires spec.blockProducer")
		}
		if len(bp.Pools) > 0 {
			if err := validatePools(dn, bp); err != nil {
				return err
			}
		} else {
			if bp.Keys.SecretRef == "" {
				return errors.New(
					"role blockProducer requires " +
						"spec.blockProducer.keys.secretRef",
				)
			}
			// The pool ID binds the delivered opcert to its pool and keys the
			// on-chain counter lookup; both refuse an empty one.
			if bp.PoolID == "" {
				return errors.New(
					"role blockProducer requires spec.blockProducer.poolId",
				)
			}
			// Only the native Secret backend is implemented. Reject the
			// reserved ExternalSecret/CSI values rather than accepting them
			// and silently mounting a plain Secret (or failing the pod when
			// the referent is absent).
			if st := bp.Keys.SourceType; st != "" &&
				st != dingov1alpha1.KeySourceSecret {
				return fmt.Errorf(
					"keys.sourceType %q is not supported in this operator "+
						"version; only Secret is implemented",
					st,
				)
			}
		}
		if cs := bp.Rotation.ColdSigner; cs.Profile != "" &&
			cs.Type != dingov1alpha1.ColdSignerBursa && cs.Type != "" {
			return fmt.Errorf(
				"coldSigner.profile is only valid with the bursa signer, "+
					"not type %q",
				cs.Type,
			)
		}
		if cs := bp.Rotation.ColdSigner; cs.AllowMainnet &&
			cs.Type != dingov1alpha1.ColdSignerSecret {
			return fmt.Errorf(
				"coldSigner.allowMainnet is only valid with the secret "+
					"signer, not type %q",
				cs.Type,
			)
		}
		// Before the Auto checks, so Auto with a signing agent but without
		// its control settings is refused for that reason rather than for an
		// incomplete cold signer.
		if err := validateSigningAgent(bp); err != nil {
			return err
		}
		// Two pods holding the KES key each forge a block for the same slot.
		// Only the agent's one-header-per-slot guard makes several pods safe.
		if bp.SigningAgent == nil && dn.Spec.Replicas != nil &&
			*dn.Spec.Replicas > 1 {
			return fmt.Errorf(
				"spec.replicas %d: a block producer runs one pod unless it "+
					"signs through spec.blockProducer.signingAgent",
				*dn.Spec.Replicas,
			)
		}
		// An image that predates the remote agent ignores its address and,
		// with no local kes.skey, cannot start, so refuse it before the
		// producer template changes.
		if bp.SigningAgent != nil && !resources.SupportsSigningAgent(dn) {
			return fmt.Errorf(
				"spec.blockProducer.signingAgent requires a Dingo image of "+
					"v0.%d.0 or later; set %s",
				resources.MinSigningAgentMinor,
				imageVersionField(dn),
			)
		}
		if bp.Rotation.Mode == dingov1alpha1.RotationModeAuto {
			if err := validateAutoRotation(&dn.Spec, bp); err != nil {
				return err
			}
		}
		// Reject configuration for features that are not implemented yet, rather
		// than accepting them and silently doing nothing. A block producer set
		// to ActiveStandby would otherwise appear healthy while the operator
		// provides no failover at all. This branch is relaxed when the P3 HA
		// pipeline lands.
		if bp.HA.Strategy == dingov1alpha1.HAActiveStandby {
			return errors.New(
				"ha strategy ActiveStandby is not supported in this operator " +
					"version; use SingleActive",
			)
		}
		if err := resources.ValidateKeyEnv(dn); err != nil {
			return err
		}
		// With no rendered topology the operator creates no topology ConfigMap
		// and Dingo dials its built-in public bootstrap peers.
		if _, hasTopo, err := topology.Render(
			dn, dn.Namespace, nil,
		); err != nil {
			return err
		} else if !hasTopo {
			return errors.New(
				"role blockProducer requires spec.topology (relayRefs with " +
					"autoPeerRelays, externalRelays, bootstrapPeers, " +
					"publicRoots, peerSnapshotRef or useLedgerAfterSlot); without it the " +
					"node dials public bootstrap peers",
			)
		}
	}
	if relay := dn.Spec.RelayService; relay.Type == "" ||
		relay.Type == corev1.ServiceTypeClusterIP {
		if relay.NodePort != 0 || relay.ExternalTrafficPolicy != "" ||
			len(relay.LoadBalancerSourceRanges) > 0 {
			return errors.New(
				"relayService external options require NodePort or LoadBalancer",
			)
		}
	} else if relay.Type != corev1.ServiceTypeNodePort && relay.Type != corev1.ServiceTypeLoadBalancer {
		return errors.New("relayService.type must be ClusterIP, NodePort, or LoadBalancer")
	}
	if len(dn.Spec.RelayService.LoadBalancerSourceRanges) > 0 &&
		dn.Spec.RelayService.Type != corev1.ServiceTypeLoadBalancer {
		return errors.New(
			"relayService.loadBalancerSourceRanges requires type LoadBalancer",
		)
	}
	for _, cidr := range dn.Spec.RelayService.LoadBalancerSourceRanges {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf(
				"relayService.loadBalancerSourceRanges contains invalid CIDR %q: %w",
				cidr,
				err,
			)
		}
	}
	if err := resources.ValidatePodLabels(dn); err != nil {
		return err
	}
	return resources.ValidateAPIs(dn)
}

// validatePools checks spec.blockProducer.pools: what Dingo refuses for a pool
// list (an agent address shared by pools with one profile, more than one pool
// with Leios) and what the operator does not do for one (Auto rotation).
func validatePools(
	dn *dingov1alpha1.DingoNode,
	bp *dingov1alpha1.BlockProducerSpec,
) error {
	if bp.PoolID != "" || bp.Keys.SecretRef != "" {
		return errors.New(
			"spec.blockProducer.pools cannot be combined with " +
				"spec.blockProducer.poolId or keys: list every pool, " +
				"the single one included, in pools",
		)
	}
	if bp.SigningAgent == nil {
		return errors.New(
			"spec.blockProducer.pools requires " +
				"spec.blockProducer.signingAgent: a pool list holds no " +
				"local KES keys",
		)
	}
	if !resources.SupportsPoolList(dn) {
		return fmt.Errorf(
			"spec.blockProducer.pools requires a Dingo image of v0.%d.0 or "+
				"later; set spec.image.tag",
			resources.MinPoolListMinor,
		)
	}
	if bp.Rotation.Mode == dingov1alpha1.RotationModeAuto {
		return errors.New(
			"rotation mode Auto is not supported with " +
				"spec.blockProducer.pools: rotating through the signing " +
				"agent per pool comes later; use Assisted or MonitorOnly",
		)
	}
	pools := make(map[string]string, len(bp.Pools))
	profiles := make(map[string]string, len(bp.Pools))
	for i, p := range bp.Pools {
		field := fmt.Sprintf("spec.blockProducer.pools[%d]", i)
		if p.Keys.SecretRef == "" {
			return fmt.Errorf("%s requires keys.secretRef", field)
		}
		if p.AgentProfile == "" {
			return fmt.Errorf("%s requires agentProfile", field)
		}
		if st := p.Keys.SourceType; st != "" &&
			st != dingov1alpha1.KeySourceSecret {
			return fmt.Errorf(
				"%s: keys.sourceType %q is not supported in this operator "+
					"version; only Secret is implemented",
				field, st,
			)
		}
		id, err := parsePoolID(p.PoolID)
		if err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
		if prev, dup := pools[id.String()]; dup {
			return fmt.Errorf(
				"%s lists pool %s, already listed by %s",
				field, id.String(), prev,
			)
		}
		pools[id.String()] = field
		if prev, dup := profiles[p.AgentProfile]; dup {
			return fmt.Errorf(
				"%s uses agentProfile %q, already used by %s; pools that "+
					"share the signing agent need distinct profiles",
				field, p.AgentProfile, prev,
			)
		}
		profiles[p.AgentProfile] = field
	}
	if len(bp.Pools) > 1 && leiosForging(dn) {
		return errors.New(
			"spec.blockProducer.pools lists more than one pool, which " +
				"Leios (runMode leios or the Musashi network) does not " +
				"support in Dingo",
		)
	}
	return nil
}

// leiosForging reports whether the node's effective environment selects the
// Leios run mode or the Musashi prototype network, with which Dingo refuses
// more than one pool. Dingo's envconfig.Process("cardano", ...) reads
// CARDANO_DINGO_RUN_MODE first and falls back to DINGO_RUN_MODE only when the
// former is unset.
func leiosForging(dn *dingov1alpha1.DingoNode) bool {
	env := map[string]string{}
	for _, e := range resources.BuildEnv(dn, resources.RenderOptions{}) {
		env[e.Name] = e.Value
	}
	runMode, ok := env["CARDANO_DINGO_RUN_MODE"]
	if !ok {
		runMode = env["DINGO_RUN_MODE"]
	}
	if strings.EqualFold(runMode, "leios") {
		return true
	}
	if env["CARDANO_NETWORK"] == ouroboros.NetworkCardanoMusashi.Name {
		return true
	}
	musashi := ouroboros.NetworkCardanoMusashi.NetworkMagic
	return env["CARDANO_NETWORK_MAGIC"] ==
		strconv.FormatUint(uint64(musashi), 10)
}

// reasonDuplicateForger is the condition reason for a block producer refused
// because another one already stages the same pool or keys.
const reasonDuplicateForger = "DuplicateForger"

// forgerConflict refuses dn when an older block producer in dn's namespace
// would forge with the same keys: the same pool ID on the same network (see
// networkKey), or the same keys Secret on any network. A pool ID claimed from
// another namespace is public, so forgerHeld decides it against that node's
// keys. The older node (creation time, then namespace/name) keeps running
// so exactly one of any conflicting set is admitted, whatever the reconcile
// order. A node being deleted no longer holds the keys.
func forgerConflict(
	dn *dingov1alpha1.DingoNode,
	all []dingov1alpha1.DingoNode,
) error {
	if !resources.IsBlockProducer(dn) || dn.Spec.BlockProducer == nil {
		return nil
	}
	pools := specPools(dn.Spec.BlockProducer)
	secrets := specSecrets(dn.Spec.BlockProducer)
	network := networkKey(dn)
	for i := range all {
		o := &all[i]
		// Other objects are unvalidated, so BlockProducer may be nil.
		if o.Namespace != dn.Namespace || o.Name == dn.Name ||
			!resources.IsBlockProducer(o) ||
			o.Spec.BlockProducer == nil ||
			!o.DeletionTimestamp.IsZero() ||
			!forgerOutranks(o, dn) ||
			// An invalid spec is not reconciled; forgerHeld holds the pool
			// for it only while its workload runs.
			validateSpec(o) != nil {
			continue
		}
		theirPools := specPools(o.Spec.BlockProducer)
		if sameNetwork(network, networkKey(o)) {
			for _, pool := range pools {
				if slices.Contains(theirPools, pool) {
					return heldf(
						"block producer %s/%s already forges for pool %s; "+
							"only one DingoNode may stage a pool's keys",
						o.Namespace, o.Name, pool,
					)
				}
			}
		}
		theirSecrets := specSecrets(o.Spec.BlockProducer)
		for _, secret := range secrets {
			if slices.Contains(theirSecrets, secret) {
				return heldf(
					"block producer %s/%s already stages keys Secret %q; "+
						"only one DingoNode may use a keys Secret",
					o.Namespace, o.Name, secret,
				)
			}
		}
	}
	return nil
}

// specPools returns the normalized pool IDs a block producer's spec names: its
// poolId, or every pool of its pool list.
func specPools(bp *dingov1alpha1.BlockProducerSpec) []string {
	if len(bp.Pools) > 0 {
		return poolIDs(bp)
	}
	if p := normalizedPoolID(bp.PoolID); p != "" {
		return []string{p}
	}
	return nil
}

// specSecrets returns the keys Secrets a block producer's spec names.
func specSecrets(bp *dingov1alpha1.BlockProducerSpec) []string {
	if len(bp.Pools) > 0 {
		secrets := make([]string, 0, len(bp.Pools))
		for _, p := range bp.Pools {
			secrets = append(secrets, p.Keys.SecretRef)
		}
		return secrets
	}
	return []string{bp.Keys.SecretRef}
}

// forgerOutranks reports whether a keeps the keys over b: the older object, or
// the lower namespace/name when created in the same instant.
func forgerOutranks(a, b *dingov1alpha1.DingoNode) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
}

// normalizedPoolID returns the canonical hex of a pool ID so bech32 and hex
// spellings of one pool compare equal. An unparseable ID is compared
// lower-cased; validateAutoRotation rejects it where it matters.
func normalizedPoolID(id string) string {
	if id == "" {
		return ""
	}
	if pool, err := parsePoolID(id); err == nil {
		return pool.String()
	}
	return strings.ToLower(id)
}

// checksum returns a short, stable hex digest used to trigger rollouts when
// derived config (topology, keys) changes.
func checksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// mainnetMagic is the Cardano mainnet network magic.
const mainnetMagic = 764824073

// validateAutoRotation checks what Auto rotation needs before it can issue
// an opcert: the pool ID and the node-to-client listener that together give
// the on-chain counter it issues from, and a cold signer that can sign for
// that pool. The secret signer holds the cold key in the cluster, so it is
// refused on mainnet unless coldSigner.allowMainnet opts in.
func validateAutoRotation(
	spec *dingov1alpha1.DingoNodeSpec,
	bp *dingov1alpha1.BlockProducerSpec,
) error {
	pool, err := parsePoolID(bp.PoolID)
	if err != nil {
		return err
	}
	if !bp.NodeToClient.Enabled {
		return errors.New(
			"rotation mode Auto requires " +
				"spec.blockProducer.nodeToClient.enabled to read the " +
				"on-chain opcert counter",
		)
	}
	cs := bp.Rotation.ColdSigner
	switch cs.Type {
	case dingov1alpha1.ColdSignerBursa, "":
		if cs.Endpoint == "" {
			return errors.New(
				"rotation mode Auto with the bursa signer requires " +
					"coldSigner.endpoint",
			)
		}
		u, err := url.Parse(cs.Endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf(
				"coldSigner.endpoint must be an https URL: %q", cs.Endpoint,
			)
		}
		if cs.MTLSSecretRef == "" {
			return errors.New(
				"rotation mode Auto with the bursa signer requires " +
					"coldSigner.mtlsSecretRef",
			)
		}
		if !strings.EqualFold(cs.ColdKeyHash, pool.String()) {
			return errors.New(
				"coldSigner.coldKeyHash must equal the pool ID " +
					"(blake2b-224 of the cold verification key)",
			)
		}
	case dingov1alpha1.ColdSignerSecret:
		mainnet := spec.Network == "mainnet" ||
			(spec.NetworkMagic != nil && *spec.NetworkMagic == mainnetMagic)
		if mainnet && !cs.AllowMainnet {
			return errors.New(
				"rotation mode Auto cannot use the secret cold signer on " +
					"mainnet; it keeps the cold key in the cluster",
			)
		}
		if cs.SecretRef == "" {
			return errors.New(
				"rotation mode Auto with the secret signer requires " +
					"coldSigner.secretRef",
			)
		}
	case dingov1alpha1.ColdSignerNone:
		return errors.New(
			"rotation mode Auto cannot issue with coldSigner type none",
		)
	default:
		return fmt.Errorf(
			"rotation mode Auto cannot issue with coldSigner type %q",
			cs.Type,
		)
	}
	return nil
}

func imageVersionField(dn *dingov1alpha1.DingoNode) string {
	if dn.Spec.Image.Digest != "" {
		return "spec.image.capabilityVersion"
	}
	return "spec.image.tag"
}

// keysChecksum returns a stable digest of a keys Secret's Data. Populating the
// pod-template annotation with this value rolls the StatefulSet when key
// material changes.
//
// The certificate is hashed under one name whichever of opcert.cert and
// node.cert holds it, so renaming the file in the Secret does not roll the pod.
// The retained kes.pending.skey is left out: the node never loads it, so a
// refused issuance that stores it must not roll the pod.
func keysChecksum(secret *corev1.Secret) string {
	data := maps.Clone(secret.Data)
	delete(data, keyFilePendingKES)
	if cert, _, err := opCertFile(data); err == nil && cert != nil {
		delete(data, keyFileNodeCert)
		data[keyFileOpCert] = cert
	}
	return checksumData(data)
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
