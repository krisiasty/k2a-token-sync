package config

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// TestShippedExampleIsValid decodes examples/cluster-connection.yaml with the
// real spec type and resolves it with the real loader.
//
// That file is documentation people copy, so it must never drift out of step with
// the API — a stale example is worse than no example. It has already caught one:
// the loader rejected a per-cluster key that had been removed from the schema.
//
// UnmarshalStrict is what makes this worth running. The API server prunes unknown
// fields rather than rejecting them for some clients, so a field the example sets
// and the type no longer has would otherwise vanish silently.
func TestShippedExampleIsValid(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "examples", "cluster-connection.yaml")
	raw, err := os.ReadFile(path) //nolint:gosec // G304: a fixed path to a file in this repository
	if err != nil {
		t.Fatalf("the example is missing: %v", err)
	}

	var obj v1alpha1.ClusterConnection
	if err := yaml.UnmarshalStrict(raw, &obj); err != nil {
		t.Fatalf("the example in %s does not match the API types: %v", path, err)
	}

	if obj.APIVersion != "k2a-token-sync.io/v1alpha1" {
		t.Errorf("apiVersion = %q, want k2a-token-sync.io/v1alpha1", obj.APIVersion)
	}
	if obj.Kind != "ClusterConnection" {
		t.Errorf("kind = %q, want ClusterConnection", obj.Kind)
	}

	cluster, err := FromSpec(obj.Name, obj.Spec)
	if err != nil {
		t.Fatalf("the example spec does not resolve: %v", err)
	}
	if cluster.Endpoint == "" || cluster.SecretName == "" {
		t.Errorf("the example resolved to an incomplete cluster: %+v", cluster)
	}
}
