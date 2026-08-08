package main

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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

// unresolvableConnectionObject builds a ClusterConnection whose spec
// config.FromSpec refuses: an endpoint carrying a URL path, which its
// normalisation rejects. A tokenTTL the API's OpenAPI schema would reject is
// out of reach for a fixture built directly as unstructured data (no
// admission runs against a fake dynamic client), so this is the way to make
// decode's FromSpec call fail without going through the schema at all.
func unresolvableConnectionObject(name string) *unstructured.Unstructured {
	object := connectionObject(name, "", nil)
	spec, ok := object.Object["spec"].(map[string]any)
	if !ok {
		panic("connectionObject did not build a map[string]any spec")
	}
	spec["endpoint"] = "10.1.0.10:6443/some/path"
	return object
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

// credentialSecretName is the name every fixture in this file uses for
// standalone-1's own credential Secret, matching what
// config.Cluster.CredentialsSecretName derives for that cluster.
const credentialSecretName = "standalone-1-credentials" //nolint:gosec // a Secret name, not a credential

// credentialSecret builds this tool's own credential Secret as
// WriteCredentials leaves it.
func credentialSecret(labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: removeNamespace, Labels: labels},
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

// Every pair this guard exists to catch is marked InvalidReason by
// inventory.Client.List, which blocks both halves of a shared endpoint
// outright — so filtering entries on InvalidReason rather than on a missing
// endpoint would leave the guard permanently blind, and tear the downstream
// half down for a connection another spec still depends on. That is the exact
// case removing one of two duplicates is meant to fix, so it has to hold on a
// pair contesting a secretName as well, where the verdict names the endpoint
// and the Secret collision is never reported at all.
func TestEndpointCollisionCatchesADuplicatePairEvenThoughBothAreInvalid(t *testing.T) {
	t.Parallel()

	inv := newTestInventory(
		connectionObject("standalone-1", "shared-secret", nil),
		connectionObject("standalone-1-duplicate", "shared-secret", nil),
	)

	// Sanity check: both entries really are marked invalid, so this test is
	// exercising the case it claims to.
	entries, err := inv.List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	for _, e := range entries {
		if e.InvalidReason == "" {
			t.Fatalf("entry %q was not marked invalid by the shared secretName; fixture does not exercise the contested case", e.Cluster.Name)
		}
	}

	other, err := endpointCollision(t.Context(), inv, "standalone-1", testEndpoint)
	if err != nil {
		t.Fatalf("endpointCollision returned unexpected error: %v", err)
	}
	if other != "standalone-1-duplicate" {
		t.Fatalf("other = %q, want the colliding connection named even though blockContestedSecrets marked both invalid", other)
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
		credentialSecret(map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)
	if _, err := removeOwnedSecret(t.Context(), client, "argocd", "cluster-standalone-1", true); err != nil {
		t.Fatalf("removeArgoCDSecret returned unexpected error: %v", err)
	}
	credGuard := inspectCredentialSecretForRemoval(t.Context(), client, removeNamespace, "standalone-1-credentials", "standalone-1")
	if reason := credGuard.refusalReason(); reason != "" {
		t.Fatalf("inspectCredentialSecretForRemoval refused an owned credential Secret: %q", reason)
	}
	if _, err := removeOwnedSecret(t.Context(), client, removeNamespace, "standalone-1-credentials", true); err != nil {
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

	cluster, usedFallback, fallbackReason, err := resolveRemovalCluster(t.Context(), inv, "standalone-1", config.RemovalClusterInput{
		Name: "standalone-1",
	})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if !usedFallback {
		t.Fatal("usedFallback = false for a ClusterConnection that does not exist")
	}
	if fallbackReason != "" {
		t.Errorf("fallbackReason = %q, want empty for a simply-absent ClusterConnection", fallbackReason)
	}

	want, err := config.RemovalCluster(config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("config.RemovalCluster returned unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cluster, want) {
		t.Errorf("cluster = %+v, want the same defaults config.RemovalCluster produces: %+v", cluster, want)
	}
}

// An unresolvable ClusterConnection — a malformed tokenTTL, an endpoint with
// a path, a name over the length limit — must fall back the same way an
// absent one does, rather than being acted on with every name zeroed. This
// is a different inventory.Client error path than the missing-object case
// above: the object exists, but decode's FromSpec call inside it fails, so
// Get returns an Entry with InvalidReason set rather than apierrors.NotFound.
func TestAnUnresolvableClusterConnectionFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	inv := newTestInventory(unresolvableConnectionObject("standalone-1"))

	cluster, usedFallback, fallbackReason, err := resolveRemovalCluster(t.Context(), inv, "standalone-1", config.RemovalClusterInput{
		Name: "standalone-1",
	})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if !usedFallback {
		t.Fatal("usedFallback = false for a ClusterConnection whose spec could not be resolved")
	}
	if fallbackReason == "" {
		t.Fatal("fallbackReason = \"\", want the InvalidReason explaining why the spec could not be resolved")
	}

	want, err := config.RemovalCluster(config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("config.RemovalCluster returned unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cluster, want) {
		t.Errorf("cluster = %+v, want the same defaults config.RemovalCluster produces "+
			"rather than a Cluster built from a partially-resolved spec: %+v", cluster, want)
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

	cluster, usedFallback, fallbackReason, err := resolveRemovalCluster(t.Context(), inv, "standalone-1", config.RemovalClusterInput{})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if usedFallback {
		t.Fatal("usedFallback = true although the ClusterConnection exists")
	}
	if fallbackReason != "" {
		t.Errorf("fallbackReason = %q, want empty when the ClusterConnection resolved cleanly", fallbackReason)
	}
	if !cluster.AdoptedRegistration {
		t.Fatal("AdoptedRegistration was not carried over from the object's annotation")
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

	warning := adoptionWarning(cluster, guard, usedFallback, "argocd")
	if warning == "" {
		t.Fatal("an adopted connection produced no warning")
	}
	if !strings.Contains(warning, "argocd cluster add") {
		t.Errorf("warning = %q, does not name the command that can restore the credential", warning)
	}

	outcome, err := removeOwnedSecret(t.Context(), client, "argocd", cluster.SecretName, false)
	if err != nil {
		t.Fatalf("removeOwnedSecret returned unexpected error: %v", err)
	}
	if outcome != downstream.RemovedOutcome {
		t.Fatalf("outcome = %v, want RemovedOutcome — an adopted Secret is still ours to delete", outcome)
	}
}

// The adoption annotation lives on the ClusterConnection, so once that object
// is gone the definitive record of an inherited registration is gone with it —
// and that is precisely the run the fallback path exists for. Saying nothing
// would silently throw away a credential only 'argocd cluster add' can
// re-mint. The Secret's own field managers are the surviving evidence, and
// this checks they are turned into a warning that names the command.
func TestAnInheritedRegistrationIsStillFlaggedWhenTheConnectionIsGone(t *testing.T) {
	t.Parallel()

	// No ClusterConnection at all: resolveRemovalCluster must fall back.
	cluster, usedFallback, _, err := resolveRemovalCluster(t.Context(), newTestInventory(), "standalone-1",
		config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}
	if !usedFallback {
		t.Fatal("usedFallback = false although no ClusterConnection exists")
	}
	if cluster.AdoptedRegistration {
		t.Fatal("AdoptedRegistration = true with no object to have recorded it")
	}

	// A Secret this tool manages that still carries the manager which created
	// it — what taking over an 'argocd cluster add' registration leaves behind.
	secret := argocdSecret(cluster.SecretName, map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil)
	secret.ManagedFields = []metav1.ManagedFieldsEntry{
		{Manager: argocd.FieldManagerRegistration},
		{Manager: "argocd"},
	}
	client := fake.NewClientset(secret)

	guard := inspectArgoCDSecretForRemoval(t.Context(), client, "argocd", cluster.SecretName, cluster.Name)
	if reason := guard.refusalReason(); reason != "" {
		t.Fatalf("the Secret was refused (%q); a co-owner is a reason to warn, not to refuse", reason)
	}

	warning := adoptionWarning(cluster, guard, usedFallback, "argocd")
	if warning == "" {
		t.Fatal("a co-owned registration produced no warning on the fallback path")
	}
	for _, want := range []string{"argocd cluster add", "argocd", "may have been adopted"} {
		if !strings.Contains(warning, want) {
			t.Errorf("warning = %q, does not mention %q", warning, want)
		}
	}
}

// The counterpart: a fallback run against a Secret this tool alone manages has
// nothing to warn about, and inventing a hedge for every such run would train
// people to scroll past the one that matters.
func TestAFallbackRunWarnsNothingWhenTheRegistrationIsSolelyOurs(t *testing.T) {
	t.Parallel()

	cluster, usedFallback, _, err := resolveRemovalCluster(t.Context(), newTestInventory(), "standalone-1",
		config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("resolveRemovalCluster returned unexpected error: %v", err)
	}

	secret := argocdSecret(cluster.SecretName, map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil)
	secret.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: argocd.FieldManagerRegistration}}
	client := fake.NewClientset(secret)

	guard := inspectArgoCDSecretForRemoval(t.Context(), client, "argocd", cluster.SecretName, cluster.Name)
	if warning := adoptionWarning(cluster, guard, usedFallback, "argocd"); warning != "" {
		t.Errorf("warning = %q, want none: nothing else manages this Secret", warning)
	}
}

// A retirement very often runs against a cluster that is already gone, and
// five dial failures are one problem, not five. Reporting them individually
// buries the only useful next step under five kubectl invocations that would
// fail exactly the same way, so they collapse into a single item that names
// --skip-downstream.
func TestAnUnreachableDownstreamCollapsesIntoOneActionableItem(t *testing.T) {
	t.Parallel()

	cluster, err := config.RemovalCluster(config.RemovalClusterInput{Name: "standalone-1"})
	if err != nil {
		t.Fatalf("RemovalCluster returned unexpected error: %v", err)
	}
	cluster.Endpoint = testEndpoint

	dial := errors.New("dial tcp 10.1.0.10:6443: i/o timeout")
	all := []namedOutcome{
		{name: "argocd-manager-role-binding", err: dial},
		{name: "k2a-token-sync", err: dial},
		{name: "kube-system/argocd-manager", err: dial},
		{name: "kube-system/k2a-token-sync", err: dial},
		{name: "k2a-token-sync", err: dial},
	}

	item := unreachableDownstream(all, cluster)
	if item == nil {
		t.Fatal("five failed downstream objects did not collapse into one item")
	}
	for _, want := range []string{"--skip-downstream", testEndpoint, "unreachable"} {
		if !strings.Contains(item.why+" "+item.kubectl, want) {
			t.Errorf("item does not mention %q: why=%q kubectl=%q", want, item.why, item.kubectl)
		}
	}

	// One success means the cluster answered, so the failures are about
	// individual objects and each deserves its own line again.
	all[2].err = nil
	if item := unreachableDownstream(all, cluster); item != nil {
		t.Errorf("collapsed to %q although one object succeeded, so the cluster was reachable", item.why)
	}
}

// A Secret that cannot be read at all must be refused, not deleted. The check
// that would prove it belongs to this cluster is the very thing that failed,
// so proceeding would mean deleting on the strength of a question never
// answered — and a transient Forbidden or API error is exactly when that
// happens.
func TestAnUnreadableArgoCDSecretIsRefusedRatherThanDeleted(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "secrets"}, "cluster-standalone-1", errors.New("no access to ArgoCD's namespace"))
	})

	guard := inspectArgoCDSecretForRemoval(t.Context(), client, "argocd", "cluster-standalone-1", "standalone-1")
	if guard.target != targetUnreadable {
		t.Fatalf("target = %v, want targetUnreadable", guard.target)
	}
	if guard.refusalReason() == "" {
		t.Fatal("an unreadable Secret was cleared for deletion")
	}
	if !guard.unverifiable() {
		t.Error("unverifiable() = false for a Secret that could not be read")
	}
	// And the hand-fix must not tell anyone to delete something whose owner
	// was never established.
	fix := refusedSecretHandFix("argocd", "cluster-standalone-1", guard.belongsToDifferentCluster(), guard.unverifiable())
	if strings.Contains(fix, "delete") {
		t.Errorf("hand-fix suggests a delete for an unverifiable Secret: %q", fix)
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

	_, err := removeOwnedSecret(t.Context(), client, "argocd", "cluster-standalone-1", false)
	if err == nil {
		t.Fatal("a Forbidden Get was not surfaced as an error")
	}
}

// actionRecorder records a label for each action a reactor observes, in the
// order the fake clientsets invoked them, so a test can assert on the actual
// sequence of Get/Delete calls across three separate fake clients rather than
// only on what each step decided in isolation.
type actionRecorder struct {
	mu  sync.Mutex
	log []string
}

func (r *actionRecorder) add(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, label)
}

func (r *actionRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.log))
	copy(out, r.log)
	return out
}

// deletesOnly returns the phase name (the part before ":") of every recorded
// delete action, in order — the sequence the five-step order promises.
func (r *actionRecorder) deletesOnly() []string {
	var out []string
	for _, entry := range r.snapshot() {
		phase, verb, ok := strings.Cut(entry, ":")
		if ok && verb == "delete" {
			out = append(out, phase)
		}
	}
	return out
}

// lastIndex returns the last position of an exact "phase:verb" label, or -1.
func (r *actionRecorder) lastIndex(label string) int {
	log := r.snapshot()
	for i := len(log) - 1; i >= 0; i-- {
		if log[i] == label {
			return i
		}
	}
	return -1
}

// lastIndexWithPrefix returns the last position of any label starting with
// prefix, or -1.
func (r *actionRecorder) lastIndexWithPrefix(prefix string) int {
	log := r.snapshot()
	for i := len(log) - 1; i >= 0; i-- {
		if strings.HasPrefix(log[i], prefix+":") {
			return i
		}
	}
	return -1
}

// named is the common surface of k8stesting's GetAction and DeleteAction:
// both carry the object's name, which is what tells one Secret apart from
// another on the same fake clientset.
type named interface {
	GetName() string
}

// byName returns a reactor that records "<names[action's name]>:<verb>" for
// any action whose name is a key in names, and otherwise records nothing. It
// never claims to have handled the action, so the fake clientset's normal
// object-tracker behaviour still runs.
func (r *actionRecorder) byName(names map[string]string) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		if n, ok := action.(named); ok {
			if phase, ok := names[n.GetName()]; ok {
				r.add(phase + ":" + action.GetVerb())
			}
		}
		return false, nil, nil
	}
}

// byPhase returns a reactor that records "<phase>:<verb>" for every action it
// sees, regardless of name — used for the downstream clientset, where every
// object in play belongs to the same "downstream" phase of the teardown.
func (r *actionRecorder) byPhase(phase string) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		r.add(phase + ":" + action.GetVerb())
		return false, nil, nil
	}
}

// downstreamFixture builds the five objects a clean teardown expects to find
// on the downstream cluster, all carrying downstream.ManagedByLabel: the two
// ServiceAccounts (ArgoCD's and k2a-token-sync's own), their ClusterRoleBindings,
// and k2a-token-sync's own ClusterRole.
func downstreamFixture() []runtime.Object {
	return []runtime.Object{
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system", Labels: downstream.ManagedByLabel},
		},
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Namespace: "kube-system", Labels: downstream.ManagedByLabel},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding", Labels: downstream.ManagedByLabel},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Labels: downstream.ManagedByLabel},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Labels: downstream.ManagedByLabel},
		},
	}
}

// executeRemoval is what runRemove delegates the guarded, ordered teardown to
// once its clients are built — see runRemove's own doc comment. Testing it
// directly, against three independent fake clientsets, is what lets these
// tests drive the ordering and exit-signal contract that talking to runRemove
// only through flags never could.
//
// This is the plan-mandated split the task-4 brief pointed at: bootstrap's
// own tests (cmd/bootstrap_test.go) only reach runBootstrap's helpers, for
// the same reason runRemove's flag-parsing wrapper is not tested here either
// — there is nothing left in it to test once executeRemoval is.

// The five-step order the design promises — the ClusterConnection first,
// then the ArgoCD Secret, then the credential, then downstream, and a final
// ArgoCD Secret recheck last — has to actually happen in that sequence
// against real clients, not merely be asserted by runRemove's doc comment.
// This drives executeRemoval end to end and inspects the order the three
// fake clientsets recorded their actions in, rather than only checking what
// each guard decided.
func TestExecuteRemovalRunsTheFiveStepsInOrder(t *testing.T) {
	t.Parallel()

	rec := &actionRecorder{}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		connectionObject("standalone-1", "", nil),
	)
	dyn.PrependReactor("*", "clusterconnections", rec.byPhase("connection"))

	localClient := fake.NewClientset(
		argocdSecret("cluster-standalone-1", map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil),
		credentialSecret(map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)
	localClient.PrependReactor("*", "secrets", rec.byName(map[string]string{
		"cluster-standalone-1":     "registration",
		"standalone-1-credentials": "credential",
	}))

	downstreamClient := fake.NewClientset(downstreamFixture()...)
	downstreamClient.PrependReactor("delete", "*", rec.byPhase("downstream"))

	out := &steps{w: io.Discard}
	remaining, err := executeRemoval(t.Context(), out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining = %+v, want none: every object here is owned and resolvable", remaining)
	}

	deletes := rec.deletesOnly()
	want := []string{"connection", "registration", "credential", "downstream", "downstream", "downstream", "downstream", "downstream"}
	if !reflect.DeepEqual(deletes, want) {
		t.Fatalf("delete order = %v, want %v", deletes, want)
	}

	// Step 5 re-checks the ArgoCD Secret after the downstream teardown, not
	// before it or interleaved with it.
	lastRegGet := rec.lastIndex("registration:get")
	lastDownstream := rec.lastIndexWithPrefix("downstream")
	if lastRegGet < lastDownstream {
		t.Errorf("the ArgoCD Secret recheck (registration:get at index %d) did not run after the downstream "+
			"teardown (last downstream action at index %d): %v", lastRegGet, lastDownstream, rec.snapshot())
	}
}

// Finding 2's fix: --dry-run being safe in each of the three local delete
// helpers individually (TestDryRunPerformsNoDeleteActions) does not prove
// executeRemoval's own wiring passes dryRun through correctly — a mistake
// made only in the orchestrating function itself, such as dropping the flag
// on one call, would slip past that test entirely. This clones
// TestExecuteRemovalRunsTheFiveStepsInOrder's fixture with dryRun: true and
// checks both halves of the promise directly against executeRemoval: not one
// delete action reaches any of the three fake clientsets, and the output
// still reports the same five-step plan a real run would have executed, just
// phrased as "would delete".
func TestExecuteRemovalDryRunRecordsNoDeletesButReportsTheFullPlan(t *testing.T) {
	t.Parallel()

	rec := &actionRecorder{}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		connectionObject("standalone-1", "", nil),
	)
	dyn.PrependReactor("*", "clusterconnections", rec.byPhase("connection"))

	localClient := fake.NewClientset(
		argocdSecret("cluster-standalone-1", map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil),
		credentialSecret(map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)
	localClient.PrependReactor("*", "secrets", rec.byName(map[string]string{
		"cluster-standalone-1":     "registration",
		"standalone-1-credentials": "credential",
	}))

	downstreamClient := fake.NewClientset(downstreamFixture()...)
	downstreamClient.PrependReactor("*", "*", rec.byPhase("downstream"))

	var buf bytes.Buffer
	out := &steps{w: &buf}
	remaining, err := executeRemoval(t.Context(), out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
		dryRun:          true,
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining = %+v, want none: every object here is owned and resolvable", remaining)
	}

	// Not one delete action reached any of the three fake clientsets.
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("--dry-run issued a delete action against the dynamic client: %+v", action)
		}
	}
	for _, action := range localClient.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("--dry-run issued a delete action against the local kubernetes client: %+v", action)
		}
	}
	for _, action := range downstreamClient.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("--dry-run issued a delete action against the downstream kubernetes client: %+v", action)
		}
	}
	if deletes := rec.deletesOnly(); len(deletes) != 0 {
		t.Errorf("delete-verb actions recorded despite --dry-run: %v", deletes)
	}

	// Every read still ran: the guards and the plan are still built from real
	// data, only the writes are suppressed.
	var reads int
	for _, entry := range rec.snapshot() {
		if strings.HasSuffix(entry, ":get") || strings.HasSuffix(entry, ":list") {
			reads++
		}
	}
	if reads == 0 {
		t.Fatal("no read actions were recorded; the guards did not run under --dry-run")
	}

	// The full five-step plan is still reported, just phrased as "would
	// delete" rather than "deleted".
	got := buf.String()
	for _, want := range []string{
		"would delete " + removeNamespace + "/standalone-1",
		"would delete argocd/cluster-standalone-1",
		"would delete " + removeNamespace + "/standalone-1-credentials",
		"would delete argocd-manager-role-binding",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output does not report the plan for %q:\n%s", want, got)
		}
	}
}

// A detected endpoint collision has to skip the downstream half entirely —
// not merely report a name while still touching the downstream cluster. This
// asserts that directly: the downstream fake clientset fails the test if it
// receives any call at all once a collision is found, which a test that only
// checked endpointCollision's return value could never catch.
func TestExecuteRemovalSkipsTheDownstreamHalfEntirelyOnCollision(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		connectionObject("standalone-1", "", nil),
		connectionObject("standalone-1-duplicate", "", nil),
	)
	localClient := fake.NewClientset(
		argocdSecret("cluster-standalone-1", map[string]string{argocd.ManagedByLabel: argocd.ManagedByValue}, nil),
		credentialSecret(map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)

	downstreamClient := fake.NewClientset()
	downstreamClient.PrependReactor("*", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		t.Errorf("downstream client was called (%s %s) despite a detected endpoint collision",
			action.GetVerb(), action.GetResource().Resource)
		return false, nil, nil
	})

	out := &steps{w: io.Discard}
	remaining, err := executeRemoval(t.Context(), out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}

	var found bool
	for _, item := range remaining {
		if strings.Contains(item.what, "downstream identities and RBAC") {
			found = true
			if !strings.Contains(item.why, "standalone-1-duplicate") {
				t.Errorf("remaining item's why = %q, does not name the colliding connection", item.why)
			}
		}
	}
	if !found {
		t.Fatalf("no remaining item recorded the skipped downstream half: %+v", remaining)
	}
}

// The exit-signal contract has two halves, and both have to actually hold
// against executeRemoval itself rather than against the smaller guard
// functions it calls: a run that leaves something behind reports it in
// remaining, and a clean run reports none. TestExecuteRemovalRunsTheFiveStepsInOrder
// above already covers the clean half; this covers the other one.
func TestExecuteRemovalSignalsRemainingItemsForAForeignSecret(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		connectionObject("standalone-1", "", nil),
	)
	// The ArgoCD Secret carries no managed-by label: not this tool's, so it
	// must be left alone and reported, and the run must still signal non-zero
	// even though --skip-downstream leaves nothing else to fail.
	localClient := fake.NewClientset(
		argocdSecret("cluster-standalone-1", nil, nil),
		credentialSecret(map[string]string{
			argocd.ManagedByLabel:  argocd.ManagedByValue,
			credentialClusterLabel: "standalone-1",
		}),
	)

	out := &steps{w: io.Discard}
	remaining, err := executeRemoval(t.Context(), out, localClient, dyn, nil, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
		skipDownstream:  true,
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}
	if len(remaining) == 0 {
		t.Fatal("remaining is empty for a foreign ArgoCD Secret; the caller would exit 0 having left it in place")
	}
}

// Finding 2's fix: when resolveRemovalCluster falls back — for either reason,
// here because the ClusterConnection is simply gone — the output has to say
// the endpoint-collision guard could not run, not just that the
// ClusterConnection itself is missing. Silence here means the downstream half
// is torn down with that protection off and nothing in the output says so.
func TestExecuteRemovalExplainsTheSkippedCollisionGuardOnFallback(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
	)
	localClient := fake.NewClientset()
	downstreamClient := fake.NewClientset()

	var buf bytes.Buffer
	out := &steps{w: &buf}
	_, err := executeRemoval(t.Context(), out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "endpoint-collision guard") {
		t.Errorf("output does not explain that the endpoint-collision guard could not run:\n%s", buf.String())
	}
}

// Finding 1's fix: a ClusterConnection whose spec cannot be resolved must be
// treated as a fallback case, the same as an absent one, and the output has
// to name the reason rather than silently acting with every name zeroed.
func TestExecuteRemovalFallsBackAndExplainsAnUnresolvableConnection(t *testing.T) {
	t.Parallel()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		unresolvableConnectionObject("standalone-1"),
	)
	localClient := fake.NewClientset()
	downstreamClient := fake.NewClientset()

	var buf bytes.Buffer
	out := &steps{w: &buf}
	remaining, err := executeRemoval(t.Context(), out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     "standalone-1",
		namespace:       removeNamespace,
		argocdNamespace: "argocd",
		fallback:        config.RemovalClusterInput{Name: "standalone-1"},
	})
	if err != nil {
		t.Fatalf("executeRemoval returned unexpected error: %v", err)
	}
	// The fallback recovers the conventional names, so the ClusterConnection
	// delete below and the (absent) Secrets are all still handled cleanly —
	// nothing here should be left as a remaining item on account of the
	// fallback itself.
	for _, item := range remaining {
		if strings.Contains(item.why, "could not be resolved") {
			t.Errorf("the resolution fallback itself was reported as a remaining item: %+v", item)
		}
	}

	got := buf.String()
	if !strings.Contains(got, "could not be resolved") {
		t.Errorf("output does not say the ClusterConnection could not be resolved:\n%s", got)
	}
	if !strings.Contains(got, "URL path") {
		t.Errorf("output does not name the reason (the endpoint's URL path) the spec could not be resolved:\n%s", got)
	}
	if !strings.Contains(got, "endpoint-collision guard") {
		t.Errorf("output does not also explain that the endpoint-collision guard could not run:\n%s", got)
	}
}
