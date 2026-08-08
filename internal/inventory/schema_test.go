package inventory

import (
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// crdWith builds a CRD serving the given spec and status property names, which
// is the only part of the schema this check reads.
func crdWith(version string, specProps, statusProps []string) *unstructured.Unstructured {
	props := func(names []string) map[string]any {
		out := map[string]any{}
		for _, n := range names {
			out[n] = map[string]any{"type": "string"}
		}
		return out
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": crdName},
		"spec": map[string]any{
			"versions": []any{map[string]any{
				"name": version,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{
					"properties": map[string]any{
						"spec":   map[string]any{"properties": props(specProps)},
						"status": map[string]any{"properties": props(statusProps)},
					},
				}},
			}},
		},
	}}
}

func fakeDynamic(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[k8sschema.GroupVersionResource]string{
			crdResource:          "CustomResourceDefinitionList",
			GroupVersionResource: "ClusterConnectionList",
		},
		objects...,
	)
}

// The case the whole check exists for. A schema that predates the binary makes
// the API server discard fields the operator set — and from inside, a pruned
// field is indistinguishable from one nobody set, so a connection asking to be
// scoped below cluster-admin resolves to the default and reports itself
// healthy. Naming the discarded fields is what turns that from a mystery into
// an instruction.
func TestASchemaOlderThanTheBinaryNamesWhatItWouldDiscard(t *testing.T) {
	t.Parallel()

	// A v0.13.0 schema: everything except the three fields v0.14.0 added.
	old := []string{
		"endpoint", "displayName", "secretName", "project", "serviceAccount",
		"selfServiceAccountName", "tokenTTL", "selfTokenTTL", "expiryWarnThreshold",
		"labels", "annotations",
	}
	client := fakeDynamic(crdWith(GroupVersionResource.Version, old, jsonFieldNames(v1alpha1.ClusterConnectionStatus{})))

	got := CheckSchema(t.Context(), client)
	if got.Unverifiable != nil {
		t.Fatalf("Unverifiable = %v, want nil: the CRD was readable", got.Unverifiable)
	}
	if !got.Stale() {
		t.Fatal("a schema missing three spec fields was not reported as stale")
	}
	for _, want := range []string{"clusterRole", "namespaces", "clusterResources"} {
		if !contains(got.MissingSpecFields, want) {
			t.Errorf("MissingSpecFields = %v, does not name %q", got.MissingSpecFields, want)
		}
	}
	if msg := got.Missing(); !strings.Contains(msg, "spec.clusterRole") {
		t.Errorf("Missing() = %q, does not qualify the field with its section", msg)
	}
}

// The ordinary case, and the one a false positive would ruin: a current schema
// must report nothing at all, or the warning becomes noise on every healthy
// install and stops being read.
func TestACurrentSchemaReportsNothing(t *testing.T) {
	t.Parallel()

	client := fakeDynamic(crdWith(GroupVersionResource.Version,
		jsonFieldNames(v1alpha1.ClusterConnectionSpec{}),
		jsonFieldNames(v1alpha1.ClusterConnectionStatus{})))

	got := CheckSchema(t.Context(), client)
	if got.Stale() {
		t.Errorf("a current schema was reported stale: %s", got.Missing())
	}
	if got.Unverifiable != nil {
		t.Errorf("Unverifiable = %v, want nil", got.Unverifiable)
	}
}

// A schema carrying more than the binary knows is a downgrade — an older image
// against a newer CRD. Nothing is discarded, because the binary simply ignores
// what it does not understand, so there is nothing to say. Reporting it would
// mean a rollback started alarming about a problem it does not have.
func TestASchemaNewerThanTheBinaryIsSilent(t *testing.T) {
	t.Parallel()

	future := append(jsonFieldNames(v1alpha1.ClusterConnectionSpec{}), "somethingAddedLater")
	client := fakeDynamic(crdWith(GroupVersionResource.Version, future,
		jsonFieldNames(v1alpha1.ClusterConnectionStatus{})))

	got := CheckSchema(t.Context(), client)
	if got.Stale() {
		t.Errorf("a newer schema was reported stale: %s", got.Missing())
	}
}

// Being unable to read the CRD is its own answer, not staleness. An operator who
// upgraded the image before the chart has no ClusterRole yet and lands here
// exactly — reporting them as stale would fire on every partial upgrade and
// point at the wrong remedy.
func TestBeingUnableToReadIsNotStaleness(t *testing.T) {
	t.Parallel()

	client := fakeDynamic()
	client.PrependReactor("get", "customresourcedefinitions",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				k8sschema.GroupResource{Resource: "customresourcedefinitions"}, crdName,
				errors.New("no permission"))
		})

	got := CheckSchema(t.Context(), client)
	if got.Stale() {
		t.Error("a CRD that could not be read was reported as stale")
	}
	if got.Unverifiable == nil {
		t.Fatal("a forbidden read was reported as a clean check")
	}
}

// A CRD that is not there at all is the scheduler's business: it already names
// the remedy and recovers on its own. Reporting it here too, in different words,
// would only compete with the message that already works.
func TestAnAbsentCRDIsLeftToTheSchedulerToReport(t *testing.T) {
	t.Parallel()

	got := CheckSchema(t.Context(), fakeDynamic())
	if got.Stale() || got.Unverifiable != nil {
		t.Errorf("an absent CRD produced a report: stale=%v unverifiable=%v", got.Stale(), got.Unverifiable)
	}
}

// The field list comes from reflection precisely so that it cannot fall behind
// the Go types. If this ever returns fewer names than the struct has JSON
// fields, every check above quietly narrows and the feature stops covering the
// thing it exists for.
func TestFieldNamesComeFromTheTypeItself(t *testing.T) {
	t.Parallel()

	got := jsonFieldNames(v1alpha1.ClusterConnectionSpec{})
	for _, want := range []string{"endpoint", "clusterRole", "namespaces", "clusterResources"} {
		if !contains(got, want) {
			t.Errorf("jsonFieldNames does not report %q; the check would not cover it", want)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
