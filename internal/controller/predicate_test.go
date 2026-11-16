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
	"testing"

	dingov1alpha1 "github.com/blinklabs-io/dingo-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func predicateNode() *dingov1alpha1.DingoNode {
	return &dingov1alpha1.DingoNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "relay",
			Namespace:  "ns",
			Generation: 3,
		},
		Spec: dingov1alpha1.DingoNodeSpec{
			Role:    dingov1alpha1.RoleRelay,
			Network: "preview",
		},
	}
}

// TestDingoNodePredicateIgnoresStatusOnlyUpdates pins the For watch filter:
// the reconciler writes status on most passes, so a status-only update must
// not enqueue the node again (issue #175). Spec, annotation, create and delete
// events still do.
func TestDingoNodePredicateIgnoresStatusOnlyUpdates(t *testing.T) {
	p := dingoNodePredicate()

	statusOnly := predicateNode()
	statusOnly.Status.Tip.Slot = 1000
	assert.False(t, p.Update(event.UpdateEvent{
		ObjectOld: predicateNode(),
		ObjectNew: statusOnly,
	}), "status-only update must not enqueue")

	specChange := predicateNode()
	specChange.Generation = 4
	assert.True(t, p.Update(event.UpdateEvent{
		ObjectOld: predicateNode(),
		ObjectNew: specChange,
	}), "spec change must enqueue")

	annotated := predicateNode()
	annotated.Annotations = map[string]string{"example.com/poke": "1"}
	assert.True(t, p.Update(event.UpdateEvent{
		ObjectOld: predicateNode(),
		ObjectNew: annotated,
	}), "annotation change must enqueue")

	assert.True(t, p.Create(event.CreateEvent{Object: predicateNode()}))
	assert.True(t, p.Delete(event.DeleteEvent{Object: predicateNode()}))
}
