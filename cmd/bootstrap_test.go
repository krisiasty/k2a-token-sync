package main

import (
	"io"
	"reflect"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
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

	raw, err := renderConnection(cluster, "k2a-token-sync", false)
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

	applied := connectionFor(cluster, "k2a-token-sync", false)

	raw, err := renderConnection(cluster, "k2a-token-sync", false)
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

// Delegated bootstrap hands this artifact to a different team, so it must be a
// complete, directly applicable contract rather than prose approximating what
// the ordinary bootstrap path would create.
func TestRenderedDownstreamManifestIsCompleteAndManaged(t *testing.T) {
	t.Parallel()

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:                    "delegated-1",
		Endpoint:                "delegated-1.example.com",
		ServiceAccountName:      "argocd-delegated",
		ServiceAccountNamespace: "cluster-access",
		SelfServiceAccountName:  "token-sync-delegated",
		ClusterRole:             "argocd-restricted",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	raw, err := renderDownstreamManifest(cluster)
	if err != nil {
		t.Fatalf("renderDownstreamManifest returned unexpected error: %v", err)
	}
	documents := strings.Split(strings.TrimSpace(string(raw)), "\n---\n")
	want := []struct {
		apiVersion string
		kind       string
		name       string
		namespace  string
	}{
		{"v1", "ServiceAccount", "argocd-delegated", "cluster-access"},
		{"rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "argocd-delegated-role-binding", ""},
		{"v1", "ServiceAccount", "token-sync-delegated", "cluster-access"},
		{"rbac.authorization.k8s.io/v1", "ClusterRole", "token-sync-delegated", ""},
		{"rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "token-sync-delegated", ""},
	}
	if len(documents) != len(want) {
		t.Fatalf("manifest has %d YAML documents, want %d:\n%s", len(documents), len(want), raw)
	}

	for i, document := range documents {
		var object unstructured.Unstructured
		if err := yaml.UnmarshalStrict([]byte(document), &object); err != nil {
			t.Fatalf("document %d is not valid Kubernetes YAML: %v\n%s", i+1, err, document)
		}
		if object.GetAPIVersion() != want[i].apiVersion || object.GetKind() != want[i].kind ||
			object.GetName() != want[i].name || object.GetNamespace() != want[i].namespace {
			t.Errorf("document %d identity = %s %s %s/%s, want %s %s %s/%s",
				i+1, object.GetAPIVersion(), object.GetKind(), object.GetNamespace(), object.GetName(),
				want[i].apiVersion, want[i].kind, want[i].namespace, want[i].name)
		}
		if object.GetLabels()["app.kubernetes.io/managed-by"] != "k2a-token-sync" {
			t.Errorf("document %d (%s) lacks the managed-by label", i+1, object.GetName())
		}
	}

	// The chosen ArgoCD role has to reach both the ArgoCD binding and the self
	// role's narrowly-scoped bind grant. Dropping either would apply cleanly but
	// leave completion or the first repair unable to work.
	for _, needle := range []string{
		"name: argocd-restricted",
		"resourceNames:\n  - argocd-restricted",
		"resources:\n  - serviceaccounts/token",
	} {
		if !strings.Contains(string(raw), needle) {
			t.Errorf("downstream manifest does not contain %q:\n%s", needle, raw)
		}
	}
}

// A scoped registration's manifest has to carry its scoping. --print exists so
// the object can live in git, and one that silently dropped the fields would
// apply as an unscoped, cluster-admin registration — the exact thing the
// operator was avoiding, arriving as a surprise the first time the manifest is
// re-applied rather than at the moment it was written.
func TestARenderedManifestCarriesItsScoping(t *testing.T) {
	t.Parallel()

	yes := true
	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:             "standalone-1",
		Endpoint:         "10.1.0.10",
		ClusterRole:      "argocd-restricted",
		Namespaces:       []string{"team-a", "team-b"},
		ClusterResources: &yes,
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	raw, err := renderConnection(cluster, "k2a-token-sync", false)
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	got := string(raw)

	for _, want := range []string{"clusterRole: argocd-restricted", "team-a", "team-b", "clusterResources: true"} {
		if !strings.Contains(got, want) {
			t.Errorf("the manifest does not carry %q:\n%s", want, got)
		}
	}

	// What is printed must still be what would be applied.
	var printed v1alpha1.ClusterConnection
	if err := yaml.UnmarshalStrict(raw, &printed); err != nil {
		t.Fatalf("the rendered manifest does not decode into the API type: %v", err)
	}
	applied := connectionFor(cluster, "k2a-token-sync", false)
	if !reflect.DeepEqual(printed.Spec, applied.Spec) {
		t.Errorf("spec differs:\nprinted %+v\napplied %+v", printed.Spec, applied.Spec)
	}
}

// The counterpart, and the reason clusterRole is emitted conditionally: an
// unscoped bootstrap must produce exactly the manifest it always produced.
// Naming cluster-admin explicitly would freeze today's default into a file that
// outlives it, which is why tokenTTL and serviceAccount are left out too.
func TestAnUnscopedManifestStatesNoScoping(t *testing.T) {
	t.Parallel()

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name: "standalone-1", Endpoint: "10.1.0.10",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}
	raw, err := renderConnection(cluster, "k2a-token-sync", false)
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	for _, field := range []string{"clusterRole", "namespaces", "clusterResources"} {
		if strings.Contains(string(raw), field) {
			t.Errorf("the manifest states %q, which should be left to the schema's default:\n%s", field, raw)
		}
	}
}

// connectionAt builds a stored ClusterConnection for a named endpoint. Distinct
// from remove_test.go's fixture, which pins every connection to one endpoint
// because the endpoint is not what those tests vary.
func connectionAt(name, endpoint string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "k2a-token-sync.io/v1alpha1",
		"kind":       "ClusterConnection",
		"metadata":   map[string]any{"name": name, "namespace": "k2a-token-sync"},
		"spec":       map[string]any{"endpoint": endpoint},
	}}
}

func inventoryOf(objects ...runtime.Object) dynamic.Interface {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		objects...,
	)
}

func bootstrapClusterFor(t *testing.T, name, endpoint string) config.Cluster {
	t.Helper()
	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{Name: name, Endpoint: endpoint})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}
	return cluster
}

// Bootstrap is where a duplicate can still be prevented. Afterwards there is
// nothing to prevent: both objects exist, k2a-token-sync stops both of them, and
// the cluster that was working goes down with the one that was added by mistake.
func TestBootstrapRefusesToRegisterAClusterTwice(t *testing.T) {
	t.Parallel()

	err := refuseDuplicateEndpoint(t.Context(),
		inventoryOf(connectionAt("prod", "10.1.0.10:6443")),
		"k2a-token-sync",
		bootstrapClusterFor(t, "prod-copy", "10.1.0.10"),
		&steps{w: io.Discard})
	if err == nil {
		t.Fatal("bootstrap accepted a second ClusterConnection for a cluster already registered")
	}
	// Naming the existing object is the whole value of refusing here: without it
	// the operator knows only that something is in the way.
	if !strings.Contains(err.Error(), "prod") {
		t.Errorf("the refusal does not name the existing connection: %v", err)
	}
	if !strings.Contains(err.Error(), "Nothing has been changed") {
		t.Errorf("the refusal does not say the cluster was left alone: %v", err)
	}
}

// Re-running bootstrap for a cluster already in the inventory is the documented
// way to change one — to rotate a credential or re-scope a registration. It is
// the same object, not a second claim, and treating it as a duplicate would make
// every connection unmaintainable the moment it existed.
func TestBootstrapStaysIdempotentForAClusterItAlreadyRegisters(t *testing.T) {
	t.Parallel()

	if err := refuseDuplicateEndpoint(t.Context(),
		inventoryOf(connectionAt("prod", "10.1.0.10:6443")),
		"k2a-token-sync",
		bootstrapClusterFor(t, "prod", "10.1.0.10"),
		&steps{w: io.Discard}); err != nil {
		t.Errorf("re-running bootstrap for an existing cluster was refused: %v", err)
	}
}

// An inventory that cannot be read is not evidence of a duplicate. The very first
// bootstrap on an installation lands here whenever the CRD is not applied yet, and
// refusing would make the tool impossible to start using. It warns and proceeds;
// the daemon still catches what this misses.
func TestBootstrapProceedsWhenTheInventoryCannotBeRead(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
	)
	dyn.PrependReactor("list", "clusterconnections",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(
				schema.GroupResource{Resource: "clusterconnections"}, "")
		})

	var out strings.Builder
	if err := refuseDuplicateEndpoint(t.Context(), dyn, "k2a-token-sync",
		bootstrapClusterFor(t, "first", "10.1.0.10"), &steps{w: &out}); err != nil {
		t.Fatalf("bootstrap refused because it could not read the inventory: %v", err)
	}
	// Proceeding silently would be worse than refusing: the one check that could
	// have caught a duplicate did not run, and nobody would know.
	if !strings.Contains(out.String(), "unknown") {
		t.Errorf("nothing was said about the check that could not run: %q", out.String())
	}
}
