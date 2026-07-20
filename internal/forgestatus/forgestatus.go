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
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan metrics: %w", err)
	}
	return s, nil
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
