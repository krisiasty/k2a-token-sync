package events

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
)

const testNamespace = "k2a-token-sync"

// fixedClock keeps the generated event name predictable.
var fixedClock = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

// staticReferencer answers with one reference, or refuses.
type staticReferencer struct {
	ref corev1.ObjectReference
	err error
}

func (s staticReferencer) Reference(context.Context, string) (corev1.ObjectReference, error) {
	return s.ref, s.err
}

func clusterConnectionRef() corev1.ObjectReference {
	return corev1.ObjectReference{
		APIVersion: "k2a-token-sync.io/v1alpha1",
		Kind:       "ClusterConnection",
		Namespace:  testNamespace,
		Name:       "standalone-1",
		UID:        types.UID("1b4e28ba-2fa1-11d2-883f-0016d3cca427"),
	}
}

func newTestRecorder(refs ObjectReferencer) (*Recorder, *fake.Clientset) {
	client := fake.NewClientset()
	r := New(client, testNamespace, refs, slog.New(slog.DiscardHandler))
	r.now = func() time.Time { return fixedClock }
	return r, client
}

// The UID is the whole reason the reference is a lookup rather than a value built
// from a name. kubectl's Events section field-selects on involvedObject.uid, so an
// event without one is accepted by the API server and then never shown by the one
// command it exists for — which would leave this feature looking like it worked.
func TestARecordedEventCarriesTheReferenceKubectlSelectsOn(t *testing.T) {
	t.Parallel()

	ref := clusterConnectionRef()
	r, client := newTestRecorder(staticReferencer{ref: ref})

	r.Warning(t.Context(), "standalone-1", "IdentityRestored", "recreated ServiceAccount kube-system/argocd-manager")

	list, err := client.CoreV1().Events(testNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing events: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("%d events were written, want 1", len(list.Items))
	}
	event := list.Items[0]

	if event.InvolvedObject != ref {
		t.Errorf("involvedObject = %+v, want %+v", event.InvolvedObject, ref)
	}
	// The event must live in the object's namespace: the API server rejects one
	// where the two disagree.
	if event.Namespace != ref.Namespace {
		t.Errorf("the event is in namespace %q and the object in %q; the API server would reject that",
			event.Namespace, ref.Namespace)
	}
	if event.Type != corev1.EventTypeWarning {
		t.Errorf("type = %q, want %q", event.Type, corev1.EventTypeWarning)
	}
	if event.Reason != "IdentityRestored" {
		t.Errorf("reason = %q, want %q", event.Reason, "IdentityRestored")
	}
	if event.Source.Component != component {
		t.Errorf("source component = %q, want %q — it is the From column", event.Source.Component, component)
	}
	// Without these, 'kubectl describe' prints <unknown> for when it happened, which
	// is most of what an event is for.
	if event.FirstTimestamp.IsZero() || event.LastTimestamp.IsZero() {
		t.Errorf("the event has no timestamps: first=%v last=%v", event.FirstTimestamp, event.LastTimestamp)
	}
	if event.Count != 1 {
		t.Errorf("count = %d, want 1", event.Count)
	}
}

// A name the API server will not accept loses the event, and every event is
// generated rather than supplied, so this is worth asserting rather than reading.
func TestEventNamesAreAcceptableToTheAPIServer(t *testing.T) {
	t.Parallel()

	for _, reason := range []string{
		"CredentialReissued", "IdentityRestored", "RenewalMintFailed",
		"RenewalRecovered", "ReconciliationResumed", "SecretNameConflict",
	} {
		name := eventName("standalone-1", reason, fixedClock)
		if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
			t.Errorf("the name generated for %s is not a valid object name: %q (%v)", reason, name, problems)
		}
	}
}

// One pass can restore an identity and reissue a credential, and on a platform with
// a coarse wall clock both land in the same nanosecond. Colliding names would mean
// the API server rejecting the second of the two.
func TestTwoEventsInTheSameInstantGetDifferentNames(t *testing.T) {
	t.Parallel()

	restored := eventName("standalone-1", "IdentityRestored", fixedClock)
	reissued := eventName("standalone-1", "CredentialReissued", fixedClock)
	if restored == reissued {
		t.Errorf("both events would be called %q, so only one of them could be written", restored)
	}
}

// Recording is best-effort by design: an event describes work that has already
// happened, so failing to write one must not be able to fail the work. There is
// nothing for a caller to handle, which means nothing here may panic either.
func TestAnUnresolvableReferenceDropsTheEvent(t *testing.T) {
	t.Parallel()

	r, client := newTestRecorder(staticReferencer{err: errors.New("clusterconnections.k2a-token-sync.io not found")})

	r.Normal(t.Context(), "standalone-1", "CredentialReissued", "reissued ArgoCD's credential")

	list, err := client.CoreV1().Events(testNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing events: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("%d events were written for an object that could not be referenced", len(list.Items))
	}
}
