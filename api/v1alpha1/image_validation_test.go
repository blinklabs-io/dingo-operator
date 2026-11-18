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

package v1alpha1

import (
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	celvalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

func TestImageDigestCapabilityVersionAdmission(t *testing.T) {
	data, err := os.ReadFile(
		"../../config/crd/bases/dingo.blinklabs.io_dingonodes.yaml",
	)
	require.NoError(t, err)

	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(data, &crd))
	require.NotEmpty(t, crd.Spec.Versions)
	root := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := root.Properties["spec"]
	image := spec.Properties["image"]

	internal := &apiextensionsinternal.JSONSchemaProps{}
	require.NoError(
		t,
		apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
			&image,
			internal,
			nil,
		),
	)
	structural, err := structuralschema.NewStructural(internal)
	require.NoError(t, err)
	validator := celvalidation.NewValidator(
		structural,
		false,
		celconfig.PerCallLimit,
	)
	require.NotNil(t, validator)

	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestOnly := map[string]any{"digest": digest}
	legacy := map[string]any{"digest": digest, "tag": "0.85.0"}
	explicit := map[string]any{
		"digest": digest, "capabilityVersion": "0.85.0",
	}
	changedLegacy := map[string]any{"digest": digest, "tag": "0.84.0"}
	changedDigest := map[string]any{
		"digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" +
			"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"tag": "0.85.0",
	}
	changedExplicit := map[string]any{
		"digest":            changedDigest["digest"],
		"capabilityVersion": "0.85.0",
	}

	validate := func(current, old map[string]any) field.ErrorList {
		errs, _ := validator.Validate(
			t.Context(),
			field.NewPath("image"),
			structural,
			current,
			old,
			math.MaxInt,
		)
		return errs
	}

	require.NotEmpty(t, validate(digestOnly, nil))
	require.NotEmpty(t, validate(legacy, nil))
	require.Empty(t, validate(explicit, nil))
	require.Empty(t, validate(legacy, legacy))
	require.NotEmpty(t, validate(changedLegacy, legacy))
	require.NotEmpty(t, validate(changedDigest, legacy))
	require.Empty(t, validate(explicit, legacy))
	require.Empty(t, validate(changedExplicit, legacy))
	require.NotEmpty(t, validate(
		map[string]any{"capabilityVersion": "0.85.0"},
		nil,
	))
}
