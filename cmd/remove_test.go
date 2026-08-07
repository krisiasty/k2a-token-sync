package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/argocd"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/downstream"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

const removeNamespace = "k2a-token-sync"

// testEndpoint is the only downstream address these fixtures need: every
// test either does not care what it is, or specifically wants two
// connections to share it.
const testEndpoint = "10.1.0.10:6443"

// connectionObject builds a ClusterConnection as the API server would store
// one, in the shape inventory.Client.Get/List resolve. Mirrors
// internal/inventory/inventory_test.go's fixture, duplicated here rather than
// exported across packages for a handful of fields.
func connectionObject(name, secretName string, annotations map[string]string) *unstructured.Unstructured {
	spec := map[string]any{"endpoint": testEndpoint}
	if secretName != "" {
		spec["secretName"] = secretName
	}
	meta := map[string]any{
		"name":       name,
		"namespace":  removeNamespace,
		"generation": int64(1),
	}
	if len(annotations) > 0 {
		anns := make(map[string]any, len(annotations))
		for k, v := range annotations {
			anns[k] = v
		}
		meta["annotations"] = anns
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "k2a-token-sync.io/v1alpha1",
		"kind":       "ClusterConnection",
		"metadata":   meta,
		"spec":       spec,
	}}
}

// newTestInventory builds an inventory.Client over a fake dynamic client, the
// same way internal/inventory's own tests do.
func newTestInventory(objects ...runtime.Object) *inventory.Client {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		objects...,
	)
	return inventory.NewClient(dyn, removeNamespace)
}

// argocdSecret builds an ArgoCD cluster Secret as bootstrap or a reconcile
// pass would leave it.
func argocdSecret(name string, labels, annotations map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "argocd", Labels: labels, Annotations: annotations},
		Data:       map[string][]byte{"config": []byte(`{"bearerToken":"a-token"}`)},
	}
}

// credentialSecret builds this tool's own credential Secret as
// WriteCredentials leaves it.
func credentialSecret(name string, labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: removeNamespace, Labels: labels},
		Data:       map[string][]byte{"token": []byte("a-token")},
	}
}

// A downstream identity another tool created, without k2a-token-sync's
// managed-by label, must be left alone rather than deleted out from under
// whoever put it there — the #37 reasoning applies just as much on the way
// out as it did on the way in. The rest of the teardown still has to run: one
// refused object must never abort everything after it.
func TestADownstreamServiceAccountWithoutTheLabelIsSkipped(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system"},
	})

	outcome, err := downstream.RemoveServiceAccount(t.Context(), client, "kube-system", "argocd-manager", false)
	if err != nil {
		t.Fatalf("RemoveServiceAccount returned unexpected error: %v", err)
	}
	if outcome != downstream.NotOwnedOutcome {
		t.Fatalf("outcome = %v, want NotOwnedOutcome for a ServiceAccount without the managed-by label", outcome)
	}

	item := remainingFromNamed(namedOutcome{
		name: "kube-system/argocd-manager", outcome: outcome, kubectl: "kubectl -n kube-system delete serviceaccount argocd-manager",
	})
	if item == nil {
		t.Fatal("an unowned ServiceAccount produced no remaining item; the run would exit 0 having left it in place")
	}
	if !strings.Contains(item.why, "not managed by k2a-token-sync") {
		t.Errorf("why = %q, does not name the reason it was skipped", item.why)
	}

	// A labelled sibling must be unaffected: the guard is per-object.
	managed := fake.NewClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Namespace: "kube-system", Labels: downstream.ManagedByLabel},
	})
	siblingOutcome, err := downstream.RemoveServiceAccount(t.Context(), managed, "kube-system", "k2a-token-sync", false)
	if err != nil {
		t.Fatalf("RemoveServiceAccount returned unexpected error for the managed sibling: %v", err)
	}
	if siblingOutcome != downstream.RemovedOutcome {
		t.Errorf("sibling outcome = %v, want RemovedOutcome — one skip must not stop the rest of the teardown", siblingOutcome)
	}
}

// The guard "not ours" and the guard "belongs to a different connection" are
// different questions: a Secret this tool manages can still have been
// published for another cluster, if a secretName was ever reused. Only the
// second is at stake here, and it must be caught even though the managed-by
// label is present.
func TestAnArgoCDSecretAnnotatedForAnotherClusterIsRefused(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(argocdSecret("cluster-standalone-1",
		map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue},
		map[string]string{argocd.ClusterNameAnnotation: "some-other-cluster"},
	))

	guard := inspectArgoCDSecretForRemoval(t.Context(), client, "argocd", "cluster-standalone-1", "standalone-1")
	if guard.target != targetOurs {
		t.Fatalf("target = %v, want targetOurs — the managed-by label is present", guard.target)
	}

	reason := guard.refusalReason()
	if reason == "" {
		t.Fatal("a Secret annotated for a different cluster was not refused")
	}
	if !strings.Contains(reason, "some-other-cluster") {
		t.Errorf("refusal %q does not name the cluster the Secret actually belongs to", reason)
	}
}

// The downstream identities are cluster-scoped and named identically for
// every ClusterConnection, so removing them for one connection registered
// twice against the same downstream cluster would silently break the other.
// This can only be told from another entry's resolved endpoint, so the whole
// downstream half is refused and the colliding connection is named.
func TestASecondConnectionSharingTheEndpointBlocksTheDownstreamHalf(t *testing.T) {
	t.Parallel()

	inv := newTestInventory(
		connectionObject("standalone-1", "", nil),
		connectionObject("standalone-1-duplicate", "", nil),
	)

	other, err := endpointCollision(t.Context(), inv, "standalone-1", testEndpoint)
	if err != nil {
		t.Fatalf("endpointCollision returned unexpected error: %v", err)
	}
	if other != "standalone-1-duplicate" {
		t.Fatalf("other = %q, want the colliding connection named", other)
	}

	// An unrelated endpoint must not collide with anything.
	inv2 := newTestInventory(connectionObject("standalone-1", "", nil))
	if other, err := endpointCollision(t.Context(), inv2, "standalone-1", testEndpoint); err != nil || other != "" {
		t.Errorf("endpointCollision = %q, %v, want no collision when nothing else shares the endpoint", other, err)
	}
}

// --dry-run has to run every read and every guard while performing zero
// writes: a plan that lies about what it would do is worse than none.
func TestDryRunPerformsNoDeleteActions(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		connectionObject("standalone-1", "", nil),
	)
	outcome, err := removeClusterConnection(t.Context(), dyn, removeNamespace, "standalone-1", true)
	if err != nil {
		t.Fatalf("removeClusterConnection returned unexpected error: %v", err)
	}
	if outcome != downstream.RemovedOutcome {
		t.Fatalf("outcome = %v, want RemovedOutcome (it would have been deleted)", outcome)
	}
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("--dry-run issued a delete action against the dynamic client: %+v", action)
		}
	}

	client := fake.NewClientset(
		argocdSecret("cluster-standalone-1", map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil),
		credentialSecret("standalone-1-credentials", map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)
	if _, err := removeArgoCDSecret(t.Context(), client, "argocd", "cluster-standalone-1", true); err != nil {
		t.Fatalf("removeArgoCDSecret returned unexpected error: %v", err)
	}
	if _, _, err := removeCredentialSecret(t.Context(), client, removeNamespace, "standalone-1-credentials", "standalone-1", true); err != nil {
		t.Fatalf("removeCredentialSecret returned unexpected error: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("--dry-run issued a delete action against the kubernetes client: %+v", action)
		}
	}
}

// A run with neither flag set must refuse before touching any cluster, and
// the message has to name both flags: the reader has to know both ways out.
func TestNeitherConfirmNorDryRunIsRefused(t *testing.T) {
	t.Parallel()

	err := validateRemoveFlags("standalone-1", "", "ctx", false, false, false)
	if err == nil {
		t.Fatal("a run with neither --confirm nor --dry-run was not refused")
	}
	for _, want := range []string{"--confirm", "--dry-run"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q:\n%v", want, err)
		}
	}
}

// --skip-downstream promises to leave the downstream cluster alone, and a
// downstream flag alongside it is a contradiction that must be rejected
// rather than silently resolved one way or the other.
func TestSkipDownstreamWithFromKubeconfigIsRejected(t *testing.T) {
	t.Parallel()

	err := validateRemoveFlags("standalone-1", "/path/to/kubeconfig", "", true, true, false)
	if err == nil {
		t.Fatal("--skip-downstream together with --from-kubeconfig was not rejected")
	}
	if !strings.Contains(err.Error(), "--skip-downstream") {
		t.Errorf("refusal does not mention --skip-downstream: %v", err)
	}
}

// Today's README tells people to 'kubectl delete ccon' first, so the object
// being gone already is the common case, not the exception. remove has to
// recover the names it needs from defaults and the override flags, and say
// so, rather than silently guessing.
func TestAMissingClusterConnectionFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	inv := newTestInventory()

	cluster, usedFallback, err := resolveRemovalCluster(t.Context(), inv, "standalone-1", config.RemovalClusterInput{
		Name: "standalone-1",
	})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if !usedFallback {
		t.Fatal("usedFallback = false for a ClusterConnection that does not exist")
	}

	want, err := config.RemovalCluster(config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("config.RemovalCluster returned unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cluster, want) {
		t.Errorf("cluster = %+v, want the same defaults config.RemovalCluster produces: %+v", cluster, want)
	}
}

// An adopted registration's ArgoCD Secret carries the managed-by label like
// any other, since ApplyRegistration writes it unconditionally on the first
// pass after adoption — so the ownership guard does not, and must not, refuse
// deleting it. What has to happen instead is a warning: that credential
// originated from 'argocd cluster add' and cannot be recovered except by
// re-running that command.
func TestAnAdoptedConnectionWarnsButStillDeletesTheSecret(t *testing.T) {
	t.Parallel()

	object := connectionObject("standalone-1", "cluster-standalone-1",
		map[string]string{v1alpha1.AnnotationAdopted: "true"})
	inv := newTestInventory(object)

	cluster, usedFallback, err := resolveRemovalCluster(t.Context(), inv, "standalone-1", config.RemovalClusterInput{})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if usedFallback {
		t.Fatal("usedFallback = true although the ClusterConnection exists")
	}
	if !cluster.AdoptedRegistration {
		t.Fatal("AdoptedRegistration was not carried over from the object's annotation")
	}

	warning := adoptionWarning(cluster, "argocd")
	if warning == "" {
		t.Fatal("an adopted connection produced no warning")
	}
	if !strings.Contains(warning, "argocd cluster add") {
		t.Errorf("warning = %q, does not name the command that can restore the credential", warning)
	}

	// The Secret carries the managed-by label the first pass after adoption
	// always writes, so the guard must clear it for deletion.
	client := fake.NewClientset(argocdSecret(cluster.SecretName,
		map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil))
	guard := inspectArgoCDSecretForRemoval(t.Context(), client, "argocd", cluster.SecretName, cluster.Name)
	if guard.target != targetOurs {
		t.Fatalf("target = %v, want targetOurs for an adopted Secret past its first pass", guard.target)
	}
	if reason := guard.refusalReason(); reason != "" {
		t.Errorf("an adopted registration's Secret was refused: %q", reason)
	}

	outcome, err := removeArgoCDSecret(t.Context(), client, "argocd", cluster.SecretName, false)
	if err != nil {
		t.Fatalf("removeArgoCDSecret returned unexpected error: %v", err)
	}
	if outcome != downstream.RemovedOutcome {
		t.Fatalf("outcome = %v, want RemovedOutcome — an adopted Secret is still ours to delete", outcome)
	}
}

// Sanity check on the reactor-based failure path used elsewhere in this
// package's idiom (cmd/adopt_test.go): a Forbidden Get must not be mistaken
// for absence, and must surface as an error for that object without being
// silently swallowed.
func TestAForbiddenSecretSurfacesAsAnError(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "secrets"}, "cluster-standalone-1", errors.New("no access to ArgoCD's namespace"))
	})

	_, err := removeArgoCDSecret(t.Context(), client, "argocd", "cluster-standalone-1", false)
	if err == nil {
		t.Fatal("a Forbidden Get was not surfaced as an error")
	}
}
