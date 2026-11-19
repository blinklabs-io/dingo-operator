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

// Package forgestatus reads Dingo's cardano-node-compatible Prometheus metrics
// to observe KES/operational-certificate lifecycle state. Once Dingo exposes an
// authoritative on-chain opcert counter over the Bark API (tracked upstream),
// the same Status can be populated from that richer source.
package forgestatus

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Metric names emitted by Dingo (cardano-node compatible).
const (
	metricCurrentKESPeriod      = "cardano_node_metrics_currentKESPeriod_int"
	metricRemainingKESPeriods   = "cardano_node_metrics_remainingKESPeriods_int"
	metricOpCertStartKESPeriod  = "cardano_node_metrics_operationalCertificateStartKESPeriod_int"
	metricOpCertExpiryKESPeriod = "cardano_node_metrics_operationalCertificateExpiryKESPeriod_int"
	metricForgedBlocks          = "cardano_node_metrics_Forge_forged_int"
	// Dingo's own wedge indicators. The ledger pipeline can stop following the
	// chain while the process stays live and the probes keep answering, so
	// neither kubelet probes nor pod state reveal it. Both gauges mean
	// operator intervention is required; halted is terminal.
	metricSlotNum        = "cardano_node_metrics_slotNum_int"
	metricPipelineStuck  = "dingo_ledger_pipeline_stuck"
	metricPipelineHalted = "dingo_ledger_pipeline_halted"
	// The issue counter of the opcert the forger has loaded, which changes
	// when Dingo reloads its credentials in place. It is NaN while no opcert
	// is loaded. Dingo labels it with pool_id, per pool, when the node forges
	// for several. Dingo also exports dingo_forge_opcert_counter_onchain, the
	// ledger's counter, which is a different value and is not read here.
	metricOpCertCounter = "dingo_forge_opcert_counter_loaded"
)

// Status is the observed forge/KES state of a node.
type Status struct {
	CurrentKESPeriod      int64
	RemainingKESPeriods   int64
	OpCertStartKESPeriod  int64
	OpCertExpiryKESPeriod int64
	ForgedBlocks          int64
	// HasKESData is true when KES metrics were present (i.e. the node is a
	// forging block producer with credentials loaded).
	HasKESData bool
	// PipelineStuck is set when the ledger pipeline has restarted without tip
	// progress often enough to be treated as stuck on a deterministic failure.
	PipelineStuck bool
	// PipelineHalted is set when the ledger pipeline has stopped retrying on an
	// unrepairable validation failure. Terminal: the node no longer follows the
	// chain, and a block producer in this state forges on a dead chain.
	PipelineHalted bool
	// These flags record whether each Dingo pipeline gauge was present. Both
	// gauges must be observed before the pipeline can be reported as following.
	HasPipelineStuckMetric  bool
	HasPipelineHaltedMetric bool
	// TipSlot is the node's tip slot; HasTipSlot reports whether the gauge was
	// present.
	TipSlot    int64
	HasTipSlot bool
	// OpCertCounter is the issue counter of the opcert the node forges with;
	// HasOpCertCounter reports whether the gauge was present.
	OpCertCounter    int64
	HasOpCertCounter bool
	// OpCertUnloaded is set when dingo_forge_opcert_counter_loaded is NaN: the
	// node has no opcert loaded, and HasOpCertCounter stays false.
	OpCertUnloaded bool
	// Pools holds the series Dingo labels with pool_id, keyed by the label
	// value (a bech32 pool ID). Dingo labels them only while it forges for
	// more than one pool; it is nil for a node that forges for one.
	Pools map[string]*PoolMetrics
}

// PoolMetrics is the forge/KES state of one pool of a node that forges for
// several.
type PoolMetrics struct {
	CurrentKESPeriod      int64
	RemainingKESPeriods   int64
	OpCertStartKESPeriod  int64
	OpCertExpiryKESPeriod int64
	// HasKESData is true when the pool's KES period gauges were present.
	HasKESData bool
	// OpCertCounter is the issue counter of the opcert loaded for the pool;
	// HasOpCertCounter reports whether it was a number.
	OpCertCounter    int64
	HasOpCertCounter bool
	// OpCertUnloaded is set when the loaded counter is NaN: the node has no
	// opcert loaded for the pool.
	OpCertUnloaded bool
}

// Fetcher retrieves forge status from a node's metrics endpoint.
type Fetcher interface {
	Fetch(ctx context.Context, metricsURL string) (*Status, error)
}

// HTTPFetcher is the default Fetcher implementation.
type HTTPFetcher struct {
	Client *http.Client
}

// NewHTTPFetcher returns a Fetcher with a bounded HTTP timeout.
func NewHTTPFetcher() *HTTPFetcher {
	return &HTTPFetcher{Client: &http.Client{Timeout: 10 * time.Second}}
}

// Fetch scrapes and parses the node's metrics endpoint.
func (f *HTTPFetcher) Fetch(
	ctx context.Context,
	metricsURL string,
) (*Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build metrics request: %w", err)
	}
	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape metrics: %w", err)
	}
	if resp == nil {
		return nil, errors.New("scrape metrics: nil response")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf(
			"metrics endpoint returned status %d",
			resp.StatusCode,
		)
	}
	return Parse(resp.Body)
}

// Parse extracts the forge/KES metrics from a Prometheus text exposition. It is
// deliberately lenient: unknown or malformed lines are ignored so a partial or
// evolving metrics surface never fails a reconcile.
func Parse(r io.Reader) (*Status, error) {
	s := &Status{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		if pool, ok := poolLabel(line); ok {
			s.addPoolSample(pool, name, v)
			continue
		}
		switch name {
		case metricCurrentKESPeriod:
			s.CurrentKESPeriod = int64(v)
			s.HasKESData = true
		case metricRemainingKESPeriods:
			s.RemainingKESPeriods = int64(v)
			s.HasKESData = true
		case metricOpCertStartKESPeriod:
			s.OpCertStartKESPeriod = int64(v)
		case metricOpCertExpiryKESPeriod:
			s.OpCertExpiryKESPeriod = int64(v)
		case metricForgedBlocks:
			s.ForgedBlocks = int64(v)
		case metricSlotNum:
			s.TipSlot = int64(v)
			s.HasTipSlot = true
		case metricPipelineStuck:
			s.HasPipelineStuckMetric = true
			s.PipelineStuck = v != 0
		case metricPipelineHalted:
			s.HasPipelineHaltedMetric = true
			s.PipelineHalted = v != 0
		case metricOpCertCounter:
			// NaN: no opcert is loaded.
			if math.IsNaN(v) {
				s.OpCertUnloaded = true
			} else {
				s.OpCertCounter = int64(v)
				s.HasOpCertCounter = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan metrics: %w", err)
	}
	return s, nil
}

// addPoolSample records one pool_id-labelled sample. Series the operator does
// not read per pool are ignored.
func (s *Status) addPoolSample(pool, name string, v float64) {
	var pm *PoolMetrics
	get := func() *PoolMetrics {
		if pm == nil {
			if s.Pools == nil {
				s.Pools = make(map[string]*PoolMetrics)
			}
			if pm = s.Pools[pool]; pm == nil {
				pm = &PoolMetrics{}
				s.Pools[pool] = pm
			}
		}
		return pm
	}
	switch name {
	case metricCurrentKESPeriod:
		get().CurrentKESPeriod = int64(v)
		get().HasKESData = true
	case metricRemainingKESPeriods:
		get().RemainingKESPeriods = int64(v)
		get().HasKESData = true
	case metricOpCertStartKESPeriod:
		get().OpCertStartKESPeriod = int64(v)
	case metricOpCertExpiryKESPeriod:
		get().OpCertExpiryKESPeriod = int64(v)
	case metricOpCertCounter:
		// NaN: the node has no opcert loaded for the pool.
		if math.IsNaN(v) {
			get().OpCertUnloaded = true
		} else {
			get().OpCertCounter = int64(v)
			get().HasOpCertCounter = true
		}
	}
}

// poolLabel returns the pool_id label of a sample line.
func poolLabel(line string) (string, bool) {
	open := strings.IndexByte(line, '{')
	closeIdx := strings.IndexByte(line, '}')
	if open < 0 || closeIdx < open {
		return "", false
	}
	labels := line[open+1 : closeIdx]
	const key = `pool_id="`
	for rest := labels; ; {
		i := strings.Index(rest, key)
		if i < 0 {
			return "", false
		}
		// The label name must start the block or follow a comma.
		if i == 0 || rest[i-1] == ',' || rest[i-1] == ' ' {
			value, _, found := strings.Cut(rest[i+len(key):], `"`)
			return value, found && value != ""
		}
		rest = rest[i+len(key):]
	}
}

// splitMetricLine splits a Prometheus sample line into its metric name (without
// labels) and its value token.
func splitMetricLine(line string) (name, value string, ok bool) {
	var rest string
	switch idx := strings.IndexAny(line, "{ "); {
	case idx < 0:
		return "", "", false
	case line[idx] == '{':
		closeIdx := strings.IndexByte(line, '}')
		if closeIdx < 0 || closeIdx < idx {
			return "", "", false
		}
		name = line[:idx]
		rest = strings.TrimSpace(line[closeIdx+1:])
	default:
		name = line[:idx]
		rest = strings.TrimSpace(line[idx+1:])
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", "", false
	}
	return name, fields[0], true
}
