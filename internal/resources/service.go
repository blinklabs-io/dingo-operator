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
	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func servicePorts(dn *dingov1alpha1.DingoNode) []corev1.ServicePort {
	ports := []corev1.ServicePort{
		{
			Name:       "relay",
			Port:       portRelay,
			TargetPort: intstr.FromString("relay"),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "private",
			Port:       PortNodeToClient,
			TargetPort: intstr.FromString("private"),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "metrics",
			Port:       portMetrics,
			TargetPort: intstr.FromString("metrics"),
			Protocol:   corev1.ProtocolTCP,
		},
	}
	for _, listener := range apiListeners(dn) {
		if listener.port > 0 {
			ports = append(
				ports,
				corev1.ServicePort{
					Name:       listener.name,
					Port:       listener.port,
					TargetPort: intstr.FromString(listener.name),
					Protocol:   corev1.ProtocolTCP,
				},
			)
		}
	}
	return ports
}

// BuildHeadlessService constructs the headless Service backing the StatefulSet
// (stable per-pod DNS for peer addressing).
func BuildHeadlessService(dn *dingov1alpha1.DingoNode) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HeadlessServiceName(dn),
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 SelectorLabels(dn),
			Ports:                    servicePorts(dn),
		},
	}
}

// BuildClientService constructs the stable client Service for the node.
func BuildClientService(dn *dingov1alpha1.DingoNode) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dn.Name,
			Namespace: dn.Namespace,
			Labels:    Labels(dn),
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeClusterIP,
			SessionAffinity: corev1.ServiceAffinityClientIP,
			Selector:        SelectorLabels(dn),
			Ports:           servicePorts(dn),
		},
	}
}

// BuildRelayService exposes only P2P, even when external publication is enabled.
func BuildRelayService(dn *dingov1alpha1.DingoNode) *corev1.Service {
	svc := BuildClientService(dn)
	svc.Name = dn.Name + "-relay"
	svc.Annotations = dn.Spec.RelayService.Annotations
	svc.Spec.Ports = svc.Spec.Ports[:1]
	svc.Spec.Type = dn.Spec.RelayService.Type
	if svc.Spec.Type == "" {
		svc.Spec.Type = corev1.ServiceTypeClusterIP
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		svc.Spec.ExternalTrafficPolicy = dn.Spec.RelayService.ExternalTrafficPolicy
		svc.Spec.Ports[0].NodePort = dn.Spec.RelayService.NodePort
	}
	if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		svc.Spec.LoadBalancerSourceRanges = dn.Spec.RelayService.LoadBalancerSourceRanges
	}
	return svc
}

// BuildPrivateService exposes NtC and enabled APIs on a ClusterIP Service.
func BuildPrivateService(dn *dingov1alpha1.DingoNode) *corev1.Service {
	svc := BuildClientService(dn)
	svc.Name = dn.Name + "-private"
	ports := make([]corev1.ServicePort, 0, len(svc.Spec.Ports)-2)
	ports = append(ports, svc.Spec.Ports[1])
	svc.Spec.Ports = append(ports, svc.Spec.Ports[3:]...)
	return svc
}

// BuildMetricsService exposes only metrics on a ClusterIP Service.
func BuildMetricsService(dn *dingov1alpha1.DingoNode) *corev1.Service {
	svc := BuildClientService(dn)
	svc.Name = dn.Name + "-metrics"
	svc.Spec.Ports = svc.Spec.Ports[2:3]
	return svc
}
