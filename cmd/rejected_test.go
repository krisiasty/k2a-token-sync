package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// condition returns one condition from a written status, failing if it is absent.
func condition(t *testing.T, status v1alpha1.ClusterConnectionStatus, conditionType string) metav1.Condition {
	t.Helper()
	cond := meta.FindStatusCondition(status.Conditions, conditionType)
	if cond == nil {
		t.Fatalf("no %s condition was written; the object says nothing about why it is being ignored", conditionType)
	}
	return *cond
}

// An object whose spec cannot be resolved is never reconciled, and used to be the
// only kind of object that said nothing about it: excluded before reconciliation,
// so nothing ever wrote its status. `kubectl get ccon` showed an empty Ready
// column and the reason lived only in this process's /status.
func TestAnUnresolvableSpecIsWrittenToTheObject(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("broken")
	inv.invalid["broken"] = `endpoint "nonsense" is not host:port`
	s := testScheduler(t, inv, newFakeReconciler())

	s.tick(t.Context())

	written, ok := inv.written["broken"]
	if !ok {
		t.Fatal("no status was written for an object that cannot be reconciled")
	}

	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready is %s, want False", ready.Status)
	}
	if ready.Reason != v1alpha1.ReasonInvalidSpec {
		t.Errorf("Ready reason is %q, want %q", ready.Reason, v1alpha1.ReasonInvalidSpec)
	}
	if ready.Message != inv.invalid["broken"] {
		t.Errorf("Ready message is %q, want the reason the spec was rejected", ready.Message)
	}

	// The generation that was rejected, so that fixing the object is visibly
	// different from this verdict still standing.
	if written.ObservedGeneration != 1 {
		t.Errorf("observedGeneration is %d, want the rejected generation 1", written.ObservedGeneration)
	}
	if ready.ObservedGeneration != 1 {
		t.Errorf("the condition's observedGeneration is %d, want 1", ready.ObservedGeneration)
	}

	// A spec this object can fix on its own must not blame a neighbour.
	if cond := meta.FindStatusCondition(written.Conditions, v1alpha1.ConditionConflict); cond != nil {
		t.Errorf("a Conflict condition was written for a spec problem: %+v", cond)
	}
}

// Two connections claiming one Secret get a condition that names the situation
// across objects, which admission cannot see and neither object can fix alone.
func TestAContestedSecretIsWrittenAsAConflict(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"; ` +
		"none of them will be reconciled until one claim remains"

	inv := newFakeInventory("contested")
	inv.invalid["contested"] = reason
	inv.cause["contested"] = v1alpha1.ReasonSecretNameConflict
	s := testScheduler(t, inv, newFakeReconciler())

	s.tick(t.Context())

	written, ok := inv.written["contested"]
	if !ok {
		t.Fatal("no status was written for a contested object")
	}

	conflict := condition(t, written, v1alpha1.ConditionConflict)
	if conflict.Status != metav1.ConditionTrue {
		t.Errorf("Conflict is %s, want True", conflict.Status)
	}
	if conflict.Reason != v1alpha1.ReasonSecretNameConflict {
		t.Errorf("Conflict reason is %q, want %q", conflict.Reason, v1alpha1.ReasonSecretNameConflict)
	}
	if conflict.Message != reason {
		t.Errorf("Conflict message is %q, want the message naming the other claimant", conflict.Message)
	}

	// Ready has to say so too, or a cluster that is not being reconciled reads as
	// though it were.
	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready is %s, want False", ready.Status)
	}
	if ready.Reason != v1alpha1.ReasonSecretNameConflict {
		t.Errorf("Ready reason is %q, want %q", ready.Reason, v1alpha1.ReasonSecretNameConflict)
	}
}

// The verdict is written once, not every poll. A rejected object is a steady
// state, and one that generated a write every thirty seconds would produce an
// endless stream of resourceVersion churn and events for a cluster where nothing
// is happening at all.
func TestTheVerdictIsWrittenOnceAndThenLeftAlone(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("broken")
	inv.invalid["broken"] = "endpoint is missing"
	s := testScheduler(t, inv, newFakeReconciler())

	for range 5 {
		s.tick(t.Context())
	}

	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.writes != 1 {
		t.Errorf("%d status writes over five polls, want 1 — the verdict is unchanged after the first", inv.writes)
	}
}

// Fixing the object has to clear what was written about it. For a conflict the
// fix is deleting the *other* object, so this one's spec never changes: nothing
// in the reconciler would ever remove the condition, and it would sit there
// accusing a neighbour that no longer exists.
func TestFixingAConflictClearsTheConflictCondition(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("contested")
	inv.invalid["contested"] = `secretName "cluster-shared" is also claimed by "other"`
	inv.cause["contested"] = v1alpha1.ReasonSecretNameConflict
	rec := newFakeReconciler()
	s := testScheduler(t, inv, rec)

	s.tick(t.Context())
	if _, ok := inv.written["contested"]; !ok {
		t.Fatal("the conflict was not written in the first place")
	}

	// The other claimant goes away.
	inv.mu.Lock()
	delete(inv.invalid, "contested")
	inv.mu.Unlock()

	s.tick(t.Context())
	s.wg.Wait()

	if rec.passes("contested") != 1 {
		t.Fatalf("the cluster reconciled %d times after the conflict cleared, want 1", rec.passes("contested"))
	}
	written := inv.written["contested"]
	if cond := meta.FindStatusCondition(written.Conditions, v1alpha1.ConditionConflict); cond != nil {
		t.Errorf("the Conflict condition survived the conflict: %+v", cond)
	}
}

// A fixed spec is reconciled and the rejection is replaced, rather than lingering
// beside a working registration.
func TestFixingASpecReplacesTheRejection(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("broken")
	inv.invalid["broken"] = "endpoint is missing"
	rec := newFakeReconciler()
	s := testScheduler(t, inv, rec)

	s.tick(t.Context())

	// An edited spec is a new generation, which is what the operator's fix looks
	// like from here.
	inv.mu.Lock()
	delete(inv.invalid, "broken")
	inv.generation = 2
	inv.mu.Unlock()

	s.tick(t.Context())
	s.wg.Wait()

	written := inv.written["broken"]
	if written.LastAction == "" || strings.HasPrefix(written.LastAction, "not reconciled") {
		t.Errorf("lastAction is still %q after the spec was fixed", written.LastAction)
	}
	if written.ObservedGeneration != 2 {
		t.Errorf("observedGeneration is %d, want the generation that was reconciled", written.ObservedGeneration)
	}

	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionTrue || ready.Reason == v1alpha1.ReasonInvalidSpec {
		t.Errorf("Ready is still %s/%s after the spec was fixed and reconciled", ready.Status, ready.Reason)
	}
}

// One object the API server will not accept must not stop the others from being
// told why they are stuck.
func TestAFailedVerdictWriteDoesNotHideTheOtherObjects(t *testing.T) {
	t.Parallel()

	inv := &refusingInventory{
		fakeInventory: newFakeInventory("a-refused", "b-broken"),
		refuse:        "a-refused",
	}
	inv.invalid["a-refused"] = "endpoint is missing"
	inv.invalid["b-broken"] = "endpoint is missing"

	s := testScheduler(t, inv, newFakeReconciler())
	s.tick(t.Context())

	if _, ok := inv.written["b-broken"]; !ok {
		t.Error("the second object was left without a verdict because the first could not be written")
	}
	if _, ok := inv.written["a-refused"]; ok {
		t.Error("the refused write was recorded anyway; the fake is not testing what it claims to")
	}
}

// refusingInventory rejects the status write for one named object, as an API
// server would for an object whose schema validation fails.
type refusingInventory struct {
	*fakeInventory
	refuse string
}

func (r *refusingInventory) UpdateStatus(
	ctx context.Context,
	name string,
	status v1alpha1.ClusterConnectionStatus,
) error {
	if name == r.refuse {
		return errors.New("ClusterConnection.k2a-token-sync.io \"" + name + "\" is invalid")
	}
	return r.fakeInventory.UpdateStatus(ctx, name, status)
}

var _ clusterInventory = (*refusingInventory)(nil)
