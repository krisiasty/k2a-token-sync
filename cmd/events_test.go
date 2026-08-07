package main

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// recordedEvent is one Event the poll asked for.
type recordedEvent struct {
	Type    string
	Cluster string
	Reason  string
	Message string
}

// fakeRecorder collects what was recorded, so a test can assert what an Event said
// and — the half these tests are mostly about — that the polls after a transition
// recorded nothing.
type fakeRecorder struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (f *fakeRecorder) Normal(_ context.Context, cluster, reason, message string) {
	f.add(corev1.EventTypeNormal, cluster, reason, message)
}

func (f *fakeRecorder) Warning(_ context.Context, cluster, reason, message string) {
	f.add(corev1.EventTypeWarning, cluster, reason, message)
}

func (f *fakeRecorder) add(eventType, cluster, reason, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, recordedEvent{eventType, cluster, reason, message})
}

func (f *fakeRecorder) recorded() []recordedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// recorderFor reaches the fake that testScheduler installs.
func recorderFor(t *testing.T, s *scheduler) *fakeRecorder {
	t.Helper()
	rec, ok := s.events.(*fakeRecorder)
	if !ok {
		t.Fatalf("the scheduler's recorder is %T, want the test fake", s.events)
	}
	return rec
}

// A verdict is written once and then read back on every poll, which is what makes
// a blocked cluster cost nothing. The Event has to follow the write rather than the
// verdict, or an object nobody has fixed would collect one every thirty seconds for
// as long as it stayed broken.
func TestARejectedSpecIsRecordedOnceRatherThanEveryPoll(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("broken")
	inv.invalid["broken"] = `endpoint "nonsense" is not host:port`
	s := testScheduler(t, inv, newFakeReconciler())
	recorder := recorderFor(t, s)

	s.tick(t.Context())

	written := only(t, recorder.recorded())
	if written.Type != corev1.EventTypeWarning {
		t.Errorf("the event is %s, want %s", written.Type, corev1.EventTypeWarning)
	}
	// The condition reason, not a name of its own, so the Events section and the
	// Ready condition agree about what is wrong.
	if written.Reason != v1alpha1.ReasonInvalidSpec {
		t.Errorf("the event reason is %q, want the condition's %q", written.Reason, v1alpha1.ReasonInvalidSpec)
	}
	if written.Message != inv.invalid["broken"] {
		t.Errorf("the message is %q, want the reason the spec was rejected", written.Message)
	}

	// The object now says it, so the polls that follow write nothing and must
	// record nothing.
	s.tick(t.Context())
	s.tick(t.Context())

	if events := recorder.recorded(); len(events) != 1 {
		t.Errorf("%d events were recorded across three polls, want 1: %+v", len(events), events)
	}
}

// A contested Secret reports under the reason that names the situation across
// objects, which is the one neither object can fix alone.
func TestAContestedSecretIsRecordedAsAConflict(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"; ` +
		"none of them will be reconciled until one claim remains"

	inv := newFakeInventory("contested")
	inv.invalid["contested"] = reason
	inv.cause["contested"] = v1alpha1.ReasonSecretNameConflict
	s := testScheduler(t, inv, newFakeReconciler())
	recorder := recorderFor(t, s)

	s.tick(t.Context())

	written := only(t, recorder.recorded())
	if written.Reason != v1alpha1.ReasonSecretNameConflict {
		t.Errorf("the event reason is %q, want %q", written.Reason, v1alpha1.ReasonSecretNameConflict)
	}
	if !strings.Contains(written.Message, "cluster-shared") {
		t.Errorf("the message does not name the contested Secret: %q", written.Message)
	}
}

// The withdrawal of a verdict is worth recording precisely because nothing else
// marks it: resolving a conflict is done by deleting the *other* object, so this
// one's generation never changes and observedGeneration cannot show the difference.
func TestAWithdrawnVerdictIsRecorded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	inv := newFakeInventory("contested")
	inv.invalid["contested"] = `secretName "cluster-shared" is also claimed by "other"`
	inv.cause["contested"] = v1alpha1.ReasonSecretNameConflict
	s := testScheduler(t, inv, newFakeReconciler())
	recorder := recorderFor(t, s)

	s.tick(ctx)
	if len(recorder.recorded()) != 1 {
		t.Fatalf("the verdict itself was not recorded: %+v", recorder.recorded())
	}

	// The other claimant is deleted, so this one is free. Its own spec never changed.
	delete(inv.invalid, "contested")
	delete(inv.cause, "contested")

	s.tick(ctx)

	events := recorder.recorded()
	if len(events) != 2 {
		t.Fatalf("%d events were recorded, want 2 — the verdict and its withdrawal: %+v", len(events), events)
	}
	resumed := events[1]
	if resumed.Type != corev1.EventTypeNormal {
		t.Errorf("the event is %s, want %s", resumed.Type, corev1.EventTypeNormal)
	}
	if resumed.Reason != v1alpha1.ReasonReconciliationResumed {
		t.Errorf("the event reason is %q, want %q", resumed.Reason, v1alpha1.ReasonReconciliationResumed)
	}

	// And not again on the polls that follow, since there is no longer a transition.
	s.tick(ctx)
	if events := recorder.recorded(); len(events) != 2 {
		t.Errorf("%d events were recorded after the object recovered, want 2: %+v", len(events), events)
	}
}

// only asserts that exactly one Event was recorded and returns it.
func only(t *testing.T, events []recordedEvent) recordedEvent {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("%d events were recorded, want exactly 1: %+v", len(events), events)
	}
	return events[0]
}
