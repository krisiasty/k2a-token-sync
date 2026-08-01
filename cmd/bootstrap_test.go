package main

import (
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
)

// The rendered manifest is a user-facing artifact: --print writes it to stdout for
// committing to a repository. So its exact shape is pinned here — a stray
// "status: {}" or a defaulted field frozen into the file would be a regression a
// reader would have to notice by eye.
func TestRenderedConnectionIsMinimalAndComplete(t *testing.T) {
	t.Parallel()

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:     "standalone-1",
		Endpoint: "10.1.0.10",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	raw, err := renderConnection(cluster, "k2a-token-sync")
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	got := string(raw)

	want := `apiVersion: k2a-token-sync.io/v1alpha1
kind: ClusterConnection
metadata:
  name: standalone-1
  namespace: k2a-token-sync
spec:
  endpoint: 10.1.0.10:6443
  secretName: cluster-standalone-1
`
	if got != want {
		t.Errorf("rendered manifest differs.\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}

	// Status belongs to k2a-token-sync; a stanza for it in a committed file is an
	// invitation to edit something the tool owns.
	if strings.Contains(got, "status") {
		t.Error("the manifest carries a status stanza")
	}
	// Defaults come from the CRD schema at admission. Emitting them here would
	// freeze today's values into a file that outlives them.
	for _, field := range []string{"tokenTTL", "selfTokenTTL", "expiryWarnThreshold", "serviceAccount"} {
		if strings.Contains(got, field) {
			t.Errorf("the manifest sets %q, which should be left to the schema's default", field)
		}
	}
}

// What is printed must be what would be applied, or --print and the default mode
// would diverge and only one of them would be tested by anything.
func TestPrintedManifestMatchesWhatWouldBeApplied(t *testing.T) {
	t.Parallel()

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:     "standalone-1",
		Endpoint: "cluster.example.com:8443",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	applied := connectionFor(cluster, "k2a-token-sync")

	raw, err := renderConnection(cluster, "k2a-token-sync")
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	var printed v1alpha1.ClusterConnection
	if err := yaml.UnmarshalStrict(raw, &printed); err != nil {
		t.Fatalf("the rendered manifest does not decode into the API type: %v", err)
	}

	if printed.Name != applied.Name || printed.Namespace != applied.Namespace {
		t.Errorf("identity differs: printed %s/%s, applied %s/%s",
			printed.Namespace, printed.Name, applied.Namespace, applied.Name)
	}
	if !reflect.DeepEqual(printed.Spec, applied.Spec) {
		t.Errorf("spec differs:\nprinted %+v\napplied %+v", printed.Spec, applied.Spec)
	}
}
