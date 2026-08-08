package inventory

import (
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
)

const testNamespace = "k2a-token-sync"

// connection builds a ClusterConnection as the API server would store one, with
// the schema's defaults already applied — secretName in particular, since that is
// what claims are made on.
func connection(name, endpoint, secretName string) *unstructured.Unstructured {
	spec := map[string]any{"endpoint": endpoint}
	if secretName != "" {
		spec["secretName"] = secretName
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "k2a-token-sync.io/v1alpha1",
		"kind":       "ClusterConnection",
		"metadata": map[string]any{
			"name":       name,
			"namespace":  testNamespace,
			"generation": int64(1),
		},
		"spec": spec,
	}}
}

// newTestClient builds a Client over a fake dynamic client. The scheme stays
// empty on purpose: the API package carries no generated deepcopy or scheme
// registration, because generating the CRD needs neither, and the dynamic client
// works in unstructured objects that carry their own kind.
func newTestClient(objects ...runtime.Object) *Client {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{GroupVersionResource: "ClusterConnectionList"},
		objects...,
	)
	return NewClient(dyn, testNamespace)
}

func byName(t *testing.T, entries []Entry) map[string]Entry {
	t.Helper()
	out := make(map[string]Entry, len(entries))
	for _, entry := range entries {
		out[entry.Cluster.Name] = entry
	}
	return out
}

// Two connections claiming one Secret must both stand down.
//
// Letting the first one seen keep it reads as though it protects the incumbent
// and does the reverse: the list is alphabetical, so a newcomer sorting earlier
// takes the Secret and republishes it against its own endpoint, while the cluster
// that was there is dropped from reconciliation with its status frozen mid-sentence.
func TestBothClaimantsAreBlockedWhenTwoConnectionsWantOneSecret(t *testing.T) {
	t.Parallel()

	client := newTestClient(
		connection("alpha", "alpha.example.com:6443", "cluster-shared"),
		connection("omega", "omega.example.com:6443", "cluster-shared"),
	)

	entries, err := client.List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List returned %d entries, want 2 — a conflict must not drop anyone", len(entries))
	}

	for _, entry := range entries {
		if entry.InvalidReason == "" {
			t.Errorf("%s was left free to write the contested Secret", entry.Cluster.Name)
		}
	}

	// Each has to be told who it is arguing with, or the operator has one half of a
	// conflict and no way to find the other.
	got := byName(t, entries)
	if !strings.Contains(got["alpha"].InvalidReason, `"omega"`) {
		t.Errorf("alpha is not told that omega claims the same Secret: %q", got["alpha"].InvalidReason)
	}
	if !strings.Contains(got["omega"].InvalidReason, `"alpha"`) {
		t.Errorf("omega is not told that alpha claims the same Secret: %q", got["omega"].InvalidReason)
	}
	if strings.Contains(got["alpha"].InvalidReason, `"alpha"`) {
		t.Errorf("alpha is listed as its own rival: %q", got["alpha"].InvalidReason)
	}
}

// The case the old behaviour got exactly backwards: a new connection whose name
// sorts before the established one. First-seen-wins handed the Secret straight to
// the newcomer.
func TestALexicallyEarlierClaimantCannotTakeOverAManagedSecret(t *testing.T) {
	t.Parallel()

	established := connection("zulu", "zulu.example.com:6443", "cluster-shared")
	newcomer := connection("alpha", "alpha.example.com:6443", "cluster-shared")

	entries, err := newTestClient(established, newcomer).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	got := byName(t, entries)
	if got["alpha"].InvalidReason == "" {
		t.Error("the newcomer took ownership of a Secret another connection was already registered under")
	}
	if got["zulu"].InvalidReason == "" {
		t.Error("the established connection kept writing a Secret whose ownership is disputed")
	}
}

// Order must not decide anything, so the same entries in either order must reach
// the same verdict.
//
// This exercises blockContestedSecrets directly rather than going through List:
// the fake dynamic client returns objects sorted by name, so a test at that level
// cannot vary the order it is supposed to be varying and would pass either way.
func TestTheVerdictDoesNotDependOnOrder(t *testing.T) {
	t.Parallel()

	claim := func(name string) Entry {
		return Entry{Cluster: config.Cluster{Name: name, SecretName: "cluster-shared"}}
	}

	forwards := []Entry{claim("alpha"), claim("omega")}
	backwards := []Entry{claim("omega"), claim("alpha")}
	blockContestedSecrets(forwards)
	blockContestedSecrets(backwards)

	a, b := byName(t, forwards), byName(t, backwards)
	for _, name := range []string{"alpha", "omega"} {
		if a[name].InvalidReason == "" {
			t.Errorf("%s was not blocked", name)
		}
		if a[name].InvalidReason != b[name].InvalidReason {
			t.Errorf("%s got a different verdict depending on order:\n  %q\n  %q",
				name, a[name].InvalidReason, b[name].InvalidReason)
		}
	}
}

// Two connections naming one downstream cluster are a duplicate, not two
// configurations, and both must stand down.
//
// They contend over everything downstream: one ServiceAccount, one
// ClusterRoleBinding whose roleRef cannot be changed, and — since ArgoCD keys a
// cluster by its server URL — two registrations it cannot tell apart. Letting
// both run means each undoes the other on a schedule, and neither object says so.
func TestBothClaimantsAreBlockedWhenTwoConnectionsNameOneCluster(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("prod", "10.1.0.10:6443", "cluster-prod"),
		connection("prod-copy", "10.1.0.10:6443", "cluster-prod-copy"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	got := byName(t, entries)
	for _, name := range []string{"prod", "prod-copy"} {
		if got[name].InvalidReason == "" {
			t.Errorf("%s was left free to reconcile a cluster another connection also claims", name)
		}
		if got[name].InvalidCause != v1alpha1.ReasonEndpointConflict {
			t.Errorf("%s was blocked with cause %q, want %q",
				name, got[name].InvalidCause, v1alpha1.ReasonEndpointConflict)
		}
	}

	// Each has to name the other, or the operator holds one half of a conflict with
	// nothing to search for.
	if !strings.Contains(got["prod"].InvalidReason, `"prod-copy"`) {
		t.Errorf("prod is not told which connection duplicates it: %q", got["prod"].InvalidReason)
	}
	if !strings.Contains(got["prod-copy"].InvalidReason, `"prod"`) {
		t.Errorf("prod-copy is not told which connection duplicates it: %q", got["prod-copy"].InvalidReason)
	}
}

// The variant that a check on the written value would miss entirely. These three
// spellings are one API server, and a duplicate is far more likely to be written
// differently than identically — it is usually a second person adding a cluster
// they did not know was there.
func TestEndpointsThatDifferOnlyInSpellingAreOneCluster(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("bare", "10.1.0.10", ""),
		connection("ported", "10.1.0.10:6443", ""),
		connection("url", "https://10.1.0.10:6443/", ""),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	for _, entry := range entries {
		if entry.InvalidCause != v1alpha1.ReasonEndpointConflict {
			t.Errorf("%s (endpoint %q) was not recognised as the same cluster as the others",
				entry.Cluster.Name, entry.Cluster.Endpoint)
		}
	}
}

// A duplicate collides on both rules at once, since two connections for one
// cluster usually also want one Secret. The endpoint is the one to report: told
// about the Secret, an operator renames one of two objects that should never both
// have existed, and ends up with two registrations for one cluster — which is the
// bug, spelled differently.
func TestADuplicateIsReportedAsTheDuplicateItIs(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("prod", "10.1.0.10:6443", "cluster-prod"),
		connection("prod-again", "10.1.0.10:6443", "cluster-prod"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	for _, entry := range entries {
		if entry.InvalidCause != v1alpha1.ReasonEndpointConflict {
			t.Errorf("%s was reported as %q, want %q — the Secret collision is a symptom of the duplicate",
				entry.Cluster.Name, entry.InvalidCause, v1alpha1.ReasonEndpointConflict)
		}
	}
}

// Order must not decide anything here either, and this rule has an ordering trap
// the Secret rule does not: it runs first, and an entry it blocks is then invisible
// to the Secret rule. Both directions must reach the same verdict.
func TestTheEndpointVerdictDoesNotDependOnOrder(t *testing.T) {
	t.Parallel()

	claim := func(name string) Entry {
		return Entry{Cluster: config.Cluster{Name: name, Endpoint: "10.1.0.10:6443"}}
	}

	forwards := []Entry{claim("alpha"), claim("omega")}
	backwards := []Entry{claim("omega"), claim("alpha")}
	blockContestedEndpoints(forwards)
	blockContestedEndpoints(backwards)

	a, b := byName(t, forwards), byName(t, backwards)
	for _, name := range []string{"alpha", "omega"} {
		if a[name].InvalidReason == "" {
			t.Errorf("%s was not blocked", name)
		}
		if a[name].InvalidReason != b[name].InvalidReason {
			t.Errorf("%s got a different verdict depending on order:\n  %q\n  %q",
				name, a[name].InvalidReason, b[name].InvalidReason)
		}
	}
}

// The ordinary inventory, which this rule must leave completely alone: every
// connection names its own cluster, and several of them accept the same defaults
// for everything else.
func TestDistinctClustersAreNotTreatedAsDuplicates(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("one", "10.1.0.10:6443", ""),
		connection("two", "10.1.0.11:6443", ""),
		connection("three", "10.1.0.12", ""),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	for _, entry := range entries {
		if entry.InvalidReason != "" {
			t.Errorf("%s was blocked in an inventory with no duplicates: %q",
				entry.Cluster.Name, entry.InvalidReason)
		}
	}
}

// A conflict between two clusters must not sideline a third that shares nothing
// with them.
func TestAnUncontestedConnectionIsUnaffectedByOthersFighting(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("alpha", "alpha.example.com:6443", "cluster-shared"),
		connection("omega", "omega.example.com:6443", "cluster-shared"),
		connection("solo", "solo.example.com:6443", "cluster-solo"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	got := byName(t, entries)
	if got["solo"].InvalidReason != "" {
		t.Errorf("an unrelated connection was blocked: %q", got["solo"].InvalidReason)
	}
	if got["solo"].Cluster.SecretName != "cluster-solo" {
		t.Errorf("solo resolved to %q", got["solo"].Cluster.SecretName)
	}
}

// Three or more is the same rule, and the message has to name all the others.
func TestEveryClaimantIsNamedWhenThreeConnectionsCollide(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("alpha", "alpha.example.com:6443", "cluster-shared"),
		connection("bravo", "bravo.example.com:6443", "cluster-shared"),
		connection("delta", "delta.example.com:6443", "cluster-shared"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	reason := byName(t, entries)["bravo"].InvalidReason
	for _, other := range []string{`"alpha"`, `"delta"`} {
		if !strings.Contains(reason, other) {
			t.Errorf("bravo is not told about %s: %q", other, reason)
		}
	}
}

// An entry that never resolved cannot claim anything, so it must not make a
// perfectly good connection look contested.
func TestAnUnresolvableConnectionDoesNotContestASecret(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("good", "good.example.com:6443", "cluster-shared"),
		connection("broken", "", "cluster-shared"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	got := byName(t, entries)
	if got["broken"].InvalidReason == "" {
		t.Error("a connection with no endpoint was accepted")
	}
	if got["good"].InvalidReason != "" {
		t.Errorf("a valid connection was blocked by an unresolvable rival: %q", got["good"].InvalidReason)
	}
}

// The ordinary case, kept honest: one connection per Secret reconciles normally.
func TestASingleClaimantIsLeftAlone(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("only", "only.example.com:6443", ""),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
	if entries[0].InvalidReason != "" {
		t.Errorf("an uncontested connection was blocked: %q", entries[0].InvalidReason)
	}
}

func TestObjectsThatDoNotDecodeAreReportedAgainstTheirOwnName(t *testing.T) {
	t.Parallel()

	malformed := connection("malformed", "malformed.example.com:6443", "cluster-malformed")
	// tokenTTL is a string in the schema; a number cannot be converted into one.
	spec, ok := malformed.Object["spec"].(map[string]any)
	if !ok {
		t.Fatal("the fixture no longer has a spec map")
	}
	spec["tokenTTL"] = int64(720)

	entries, err := newTestClient(malformed).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want the malformed object reported rather than dropped", len(entries))
	}
	if entries[0].Cluster.Name != "malformed" {
		t.Errorf("reported against %q, want the object's own name", entries[0].Cluster.Name)
	}
	if entries[0].InvalidReason == "" {
		t.Error("a malformed object was accepted")
	}
}

func TestUpdateStatusWritesToTheSubresource(t *testing.T) {
	t.Parallel()

	client := newTestClient(connection("only", "only.example.com:6443", ""))

	err := client.UpdateStatus(t.Context(), "only", v1alpha1.ClusterConnectionStatus{
		ObservedGeneration: 1,
		LastAction:         "up-to-date",
	})
	if err != nil {
		t.Fatalf("UpdateStatus returned unexpected error: %v", err)
	}

	entries, err := client.List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if entries[0].Status.LastAction != "up-to-date" {
		t.Errorf("lastAction = %q, want it read back", entries[0].Status.LastAction)
	}
	if entries[0].Edited() {
		t.Error("the entry reads as edited although status caught up with the spec")
	}
}

// A malformed object — one the Go types cannot parse — keeps the two things that
// matter: the generation being rejected, and the status recording what was last
// published. Discarding the status would make fixing a typo cost a needless
// reissue of a credential that is working.
func TestAMalformedObjectKeepsItsGenerationAndPublishedState(t *testing.T) {
	t.Parallel()

	malformed := connection("malformed", "malformed.example.com:6443", "cluster-malformed")
	malformed.SetGeneration(7)
	// tokenTTL is a string in the schema; a number cannot be converted into one.
	spec, ok := malformed.Object["spec"].(map[string]any)
	if !ok {
		t.Fatal("the fixture no longer has a spec map")
	}
	spec["tokenTTL"] = int64(720)
	malformed.Object["status"] = map[string]any{
		"observedGeneration":    int64(6),
		"appliedCredentialHash": "sha256:abc",
	}

	entries, err := newTestClient(malformed).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want the malformed object reported rather than dropped", len(entries))
	}

	entry := entries[0]
	if entry.InvalidReason == "" {
		t.Error("a malformed object was accepted")
	}
	if entry.InvalidCause != v1alpha1.ReasonInvalidSpec {
		t.Errorf("InvalidCause is %q, want %q", entry.InvalidCause, v1alpha1.ReasonInvalidSpec)
	}
	if entry.Generation != 7 {
		t.Errorf("Generation is %d, want 7 — the generation being rejected", entry.Generation)
	}
	if entry.Status.AppliedCredentialHash != "sha256:abc" {
		t.Errorf("the published fingerprint was discarded: %+v", entry.Status)
	}
	if entry.ObservedGeneration != 6 {
		t.Errorf("ObservedGeneration is %d, want 6 from the status beside the broken spec", entry.ObservedGeneration)
	}
}

// The cause travels with the reason, since it is what decides which conditions
// get written.
func TestTheInventorySaysWhichKindOfProblemItFound(t *testing.T) {
	t.Parallel()

	entries, err := newTestClient(
		connection("alpha", "alpha.example.com:6443", "cluster-shared"),
		connection("omega", "omega.example.com:6443", "cluster-shared"),
		connection("broken", "", "cluster-broken"),
		connection("fine", "fine.example.com:6443", "cluster-fine"),
	).List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}

	want := map[string]string{
		"alpha":  v1alpha1.ReasonSecretNameConflict,
		"omega":  v1alpha1.ReasonSecretNameConflict,
		"broken": v1alpha1.ReasonInvalidSpec,
		"fine":   "",
	}
	for _, entry := range entries {
		if got := entry.InvalidCause; got != want[entry.Cluster.Name] {
			t.Errorf("%s: InvalidCause is %q, want %q", entry.Cluster.Name, got, want[entry.Cluster.Name])
		}
		if (entry.InvalidCause == "") != (entry.InvalidReason == "") {
			t.Errorf("%s: cause %q and reason %q disagree about whether it is usable",
				entry.Cluster.Name, entry.InvalidCause, entry.InvalidReason)
		}
	}
}

// An Event carrying only a name and a namespace is accepted by the API server and
// then never shown by 'kubectl describe', which field-selects on
// involvedObject.uid alongside the kind. That failure is invisible from this side
// — the write succeeds — so the reference is worth asserting field by field.
func TestReferenceCarriesWhatKubectlSelectsOn(t *testing.T) {
	t.Parallel()

	object := connection("standalone-1", "10.1.0.10", "cluster-standalone-1")
	object.SetUID("1b4e28ba-2fa1-11d2-883f-0016d3cca427")
	client := newTestClient(object)

	ref, err := client.Reference(t.Context(), "standalone-1")
	if err != nil {
		t.Fatalf("Reference returned an error: %v", err)
	}

	if ref.UID != "1b4e28ba-2fa1-11d2-883f-0016d3cca427" {
		t.Errorf("uid = %q, want the object's; without it the event is written and never shown", ref.UID)
	}
	if ref.Kind != "ClusterConnection" {
		t.Errorf("kind = %q, want %q", ref.Kind, "ClusterConnection")
	}
	if ref.APIVersion != "k2a-token-sync.io/v1alpha1" {
		t.Errorf("apiVersion = %q, want %q", ref.APIVersion, "k2a-token-sync.io/v1alpha1")
	}
	if ref.Namespace != testNamespace || ref.Name != "standalone-1" {
		t.Errorf("reference names %s/%s, want %s/standalone-1", ref.Namespace, ref.Name, testNamespace)
	}
}

// A reference that cannot be read must say so rather than producing a reference
// with an empty UID, which would write an event nothing can find.
func TestReferenceToAMissingObjectFails(t *testing.T) {
	t.Parallel()

	if _, err := newTestClient().Reference(t.Context(), "absent"); err == nil {
		t.Fatal("Reference succeeded for an object that does not exist")
	}
}

// Adoption is an annotation rather than a spec field, so FromSpec cannot see it and
// this is the only place it can reach the reconciler. Getting it wrong in the
// permissive direction would have a collided cluster name reported as an intended
// migration, which is the whole thing the record exists to distinguish.
func TestAdoptionIsReadFromTheObjectsAnnotation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
		set   bool
		want  bool
	}{
		{name: "absent", want: false},
		{name: "true", value: "true", set: true, want: true},
		// Anything that is not exactly "true" reads as no adoption. A half-written or
		// misspelt value must report the takeover rather than excuse it.
		{name: "misspelt", value: "yes", set: true, want: false},
		{name: "empty", value: "", set: true, want: false},
		{name: "capitalised", value: "True", set: true, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			object := connection("standalone-1", "10.1.0.10", "cluster-standalone-1")
			if tc.set {
				object.SetAnnotations(map[string]string{v1alpha1.AnnotationAdopted: tc.value})
			}

			entry := decode(object)
			if entry.InvalidReason != "" {
				t.Fatalf("the object did not resolve: %s", entry.InvalidReason)
			}
			if entry.Cluster.AdoptedRegistration != tc.want {
				t.Errorf("AdoptedRegistration = %v, want %v for annotation %q",
					entry.Cluster.AdoptedRegistration, tc.want, tc.value)
			}
		})
	}
}

// Get resolves the same Cluster fields as List would, including the adoption
// annotation, so that removal acts on the same names and annotations the daemon
// would.
func TestGetResolvesTheSameFieldsAsListDoes(t *testing.T) {
	t.Parallel()

	object := connection("my-cluster", "10.1.0.10", "cluster-my-cluster")
	object.SetAnnotations(map[string]string{v1alpha1.AnnotationAdopted: "true"})
	object.SetGeneration(2)

	client := newTestClient(object)

	// Get the entry via Get
	got, err := client.Get(t.Context(), "my-cluster")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}

	// Get the entry via List
	entries, err := client.List(t.Context())
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
	listed := entries[0]

	// Verify that Get and List agree on the Cluster fields
	if got.Cluster.Name != listed.Cluster.Name {
		t.Errorf("Get and List disagree on name: %q vs %q", got.Cluster.Name, listed.Cluster.Name)
	}
	if got.Cluster.Endpoint != listed.Cluster.Endpoint {
		t.Errorf("Get and List disagree on endpoint: %q vs %q", got.Cluster.Endpoint, listed.Cluster.Endpoint)
	}
	if got.Cluster.SecretName != listed.Cluster.SecretName {
		t.Errorf("Get and List disagree on secret name: %q vs %q", got.Cluster.SecretName, listed.Cluster.SecretName)
	}
	if got.Cluster.AdoptedRegistration != listed.Cluster.AdoptedRegistration {
		t.Errorf("Get and List disagree on AdoptedRegistration: %v vs %v", got.Cluster.AdoptedRegistration, listed.Cluster.AdoptedRegistration)
	}
	if got.Generation != listed.Generation {
		t.Errorf("Get and List disagree on Generation: %d vs %d", got.Generation, listed.Generation)
	}
}

// Get on a missing object returns an error where apierrors.IsNotFound is true.
func TestGetOnAMissingObjectReturnsNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient()

	_, err := client.Get(t.Context(), "absent")
	if err == nil {
		t.Fatal("Get succeeded for an object that does not exist")
	}
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get returned an error that is not a not-found error: %v", err)
	}
}
