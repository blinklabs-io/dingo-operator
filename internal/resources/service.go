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

func servicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:       "relay",
			Port:       portRelay,
			TargetPort: intstr.FromString("relay"),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "private",
			Port:       portPrivate,
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
			Ports:                    servicePorts(),
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
			Ports:           servicePorts(),
		},
	}
}
