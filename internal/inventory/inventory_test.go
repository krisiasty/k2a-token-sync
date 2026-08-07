package inventory

import (
	"strings"
	"testing"

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
