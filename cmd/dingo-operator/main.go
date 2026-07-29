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
	"crypto/tls"
	"flag"
	"os"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/blinklabs-io/dingo-operator/internal/controller"
	"github.com/blinklabs-io/dingo-operator/internal/forgestatus"
	"github.com/blinklabs-io/dingo-operator/internal/onchain"
	"github.com/blinklabs-io/dingo-operator/internal/version"
	_ "go.uber.org/automaxprocs"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilRuntimeMust(clientgoscheme.AddToScheme(scheme))
	utilRuntimeMust(dingov1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		secureMetrics        bool
	)
	flag.StringVar(
		&metricsAddr,
		"metrics-bind-address",
		":8080",
		"Address the metric endpoint binds to.",
	)
	flag.StringVar(
		&probeAddr,
		"health-probe-bind-address",
		":8081",
		"Address the probe endpoint binds to.",
	)
	flag.BoolVar(
		&enableLeaderElection,
		"leader-elect",
		false,
		"Enable leader election for controller manager.",
	)
	flag.BoolVar(
		&secureMetrics,
		"metrics-secure",
		true,
		"Serve metrics over HTTPS.",
	)
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info(
		"starting dingo-operator",
		"version",
		version.GetVersionString(),
	)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
			SecureServing:  secureMetrics,
			TLSOpts: []func(*tls.Config){
				func(c *tls.Config) { c.MinVersion = tls.VersionTLS12 },
			},
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "dingo-operator.blinklabs.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	reconciler := &controller.DingoNodeReconciler{
		Client:        mgr.GetClient(),
		APIReader:     mgr.GetAPIReader(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorder("dingonode-controller"),
		ForgeStatus:   forgestatus.NewHTTPFetcher(),
		OnChain:       onchain.NewNtCFetcher(),
		PodMonitorCRD: podMonitorInstalled(mgr),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(
			err,
			"unable to create controller",
			"controller",
			"DingoNode",
		)
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// podMonitorInstalled reports whether the Prometheus Operator PodMonitor CRD is
// present, so the operator can create PodMonitors only when they are supported.
func podMonitorInstalled(mgr ctrl.Manager) bool {
	gk := schema.GroupKind{Group: "monitoring.coreos.com", Kind: "PodMonitor"}
	_, err := mgr.GetRESTMapper().RESTMapping(gk, "v1")
	if err != nil {
		setupLog.Info(
			"PodMonitor CRD not detected; PodMonitor creation disabled",
		)
		return false
	}
	return true
}

func utilRuntimeMust(err error) {
	if err != nil {
		setupLog.Error(err, "scheme setup failed")
		os.Exit(1)
	}
}
