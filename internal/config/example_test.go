package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestShippedExampleConfigIsValid loads examples/config.yaml with the real loader.
//
// That file is documentation people copy, so it must never drift out of step
// with the parser — a stale example is worse than no example. It has already
// caught one: the loader rejects unknown fields, so a per-cluster key that was
// removed from the schema failed here rather than in someone's cluster.
func TestShippedExampleConfigIsValid(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "config.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the example inventory is missing: %v", err)
	}

	t.Setenv("CONFIG_PATH", path)
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
