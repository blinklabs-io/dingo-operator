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

package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	k8sversion "k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSplitNamespaces(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{in: "", want: nil},
		{in: " , ,", want: nil},
		{in: "keys", want: []string{"keys"}},
		{in: "a, b,a,,c ", want: []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, splitNamespaces(tt.in))
		})
	}
}

func TestSleepActionSupported(t *testing.T) {
	for gitVersion, want := range map[string]bool{
		"v1.28.3":             false,
		"v1.29.5":             false,
		"v1.29.15-eks-a1b2c3": false,
		"v1.30.0":             true,
		"v1.31.5+k3s1":        true,
		"v1.34.1-gke.100":     true,
		"":                    false,
		"not-a-version":       false,
	} {
		t.Run(gitVersion, func(t *testing.T) {
			assert.Equal(
				t,
				want,
				sleepActionSupported(&k8sversion.Info{GitVersion: gitVersion}),
			)
		})
	}
}

type failingServerVersion struct{}

func (failingServerVersion) ServerVersion() (*k8sversion.Info, error) {
	return nil, errors.New("connection refused")
}

// A failed version read is an error, not "unsupported": reading it as
// unsupported would render every relay without its hook until the next
// restart, rolling each relay twice.
func TestPreStopSleepAvailable(t *testing.T) {
	_, err := preStopSleepAvailable(failingServerVersion{})
	assert.Error(t, err)

	for gitVersion, want := range map[string]bool{
		"v1.29.5": false,
		"v1.30.0": true,
	} {
		dc := &fakediscovery.FakeDiscovery{
			Fake:               &clienttesting.Fake{},
			FakedServerVersion: &k8sversion.Info{GitVersion: gitVersion},
		}
		got, err := preStopSleepAvailable(dc)
		assert.NoError(t, err, gitVersion)
		assert.Equal(t, want, got, gitVersion)
	}
}
