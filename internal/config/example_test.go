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
	t.Setenv("POD_NAMESPACE", "k2a-token-sync")

	cfg, err := Load(discardLogger())
	if err != nil {
		t.Fatalf("the example inventory in %s does not load: %v", path, err)
	}

	if len(cfg.Clusters) == 0 {
		t.Error("the example inventory declares no clusters, so it demonstrates nothing")
	}
}
