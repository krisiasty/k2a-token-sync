package config

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestShippedExampleConfigIsValid parses the inventory embedded in
// deploy/configmap.yaml with the real loader.
//
// The example is documentation people copy, so it must never drift out of step
// with the parser — a stale example is worse than no example.
func TestShippedExampleConfigIsValid(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "configmap.yaml")
	raw, err := os.ReadFile(path) //nolint:gosec // G304: a fixed path inside the repository
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	var manifest struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}

	inventory, ok := manifest.Data["config.yaml"]
	if !ok {
		t.Fatalf("%s has no data[\"config.yaml\"] entry", path)
	}

	dir := t.TempDir()
	inventoryPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(inventoryPath, []byte(inventory), 0o600); err != nil {
		t.Fatalf("writing extracted inventory: %v", err)
	}

	t.Setenv("CONFIG_PATH", inventoryPath)
	t.Setenv("POD_NAMESPACE", "r2a-cert-sync")

	cfg, err := Load(discardLogger())
	if err != nil {
		t.Fatalf("the example inventory in %s does not load: %v", path, err)
	}

	// The example is meant to demonstrate both providers.
	var sawRancher, sawDirect bool
	for _, c := range cfg.Clusters {
		switch c.Provider {
		case ProviderRancher:
			sawRancher = true
		case ProviderDirect:
			sawDirect = true
		}
	}
	if !sawRancher || !sawDirect {
		t.Errorf("example covers rancher=%v direct=%v, want both providers demonstrated", sawRancher, sawDirect)
	}
}
