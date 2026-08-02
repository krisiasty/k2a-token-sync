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

// A verdict can be formed while a pass is running, and the pass must not undo it.
//
// The pass computed its result before the conflict existed, so writing that result
// puts Ready=True back on an object this tool has just stopped reconciling. In the
// conflict case the fix is deleting the other object, so this one's generation
// never changes and nothing about that Ready=True looks stale — it simply reads as
// a healthy cluster that is quietly no longer being maintained.
func TestAPassFinishingAfterTheVerdictDoesNotUndoIt(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"`

	inv := newFakeInventory("late")
	rec := newFakeReconciler("late")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the pass to start", func() bool { return rec.passes("late") == 1 })

	// The other claimant appears while the pass is in flight, too late to stop it.
	inv.mu.Lock()
	inv.invalid["late"] = reason
	inv.cause["late"] = v1alpha1.ReasonSecretNameConflict
	inv.mu.Unlock()

	s.tick(ctx)
	if _, ok := inv.written["late"]; !ok {
		t.Fatal("the poll did not write the verdict while the pass was running")
	}

	rec.releaseAll()
	s.wg.Wait()

	written := inv.written["late"]
	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse || ready.Reason != v1alpha1.ReasonSecretNameConflict {
		t.Errorf("the finishing pass left Ready=%s/%s over the verdict", ready.Status, ready.Reason)
	}
	if conflict := condition(t, written, v1alpha1.ConditionConflict); conflict.Status != metav1.ConditionTrue {
		t.Errorf("the finishing pass cleared the Conflict condition, which it does not get to decide")
	}

	// The pass did publish, and what it published is worth keeping: without it the
	// next pass to run would reissue a credential that is perfectly current.
	if written.AppliedCredentialHash != "sha256:published-late" {
		t.Errorf("the pass's own record of what it published was dropped: %q", written.AppliedCredentialHash)
	}

	// And the two writers must agree, or every poll would rewrite the object.
	before := inv.writes
	s.tick(ctx)
	if inv.writes != before {
		t.Errorf("%d further writes; the pass and the poll disagree about the verdict", inv.writes-before)
	}
}

// The narrow ordering: a verdict formed while the pass's write was already in
// flight. The pass cannot have seen it, so it writes Ready=True over a decision
// this tool has already taken — and with the generation unmoved, that reads as a
// healthy cluster rather than as a stale one.
//
// The hook puts the verdict in exactly that gap, which is the one place a test can
// reach it: between the writer deciding what to say and the API receiving it.
func TestAVerdictFormedDuringAPassesWriteStillLands(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"`

	inv := newFakeInventory("racing")
	rec := newFakeReconciler()
	s := testScheduler(t, inv, rec)

	inv.onFirstWrite = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.state["racing"]
		state.invalidReason = reason
		state.invalidCause = v1alpha1.ReasonSecretNameConflict
	}

	s.tick(t.Context())
	s.wg.Wait()

	written := inv.written["racing"]
	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse || ready.Reason != v1alpha1.ReasonSecretNameConflict {
		t.Errorf("the object was left saying Ready=%s/%s after being blocked mid-write",
			ready.Status, ready.Reason)
	}
	if conflict := condition(t, written, v1alpha1.ConditionConflict); conflict.Status != metav1.ConditionTrue {
		t.Error("no Conflict condition after being blocked mid-write")
	}

	// The second write is laid over the first, so what the pass published is still
	// recorded. Losing it would cost a reissue of a credential that is current.
	if written.AppliedCredentialHash != "sha256:published-racing" {
		t.Errorf("the pass's record of what it published was dropped: %q", written.AppliedCredentialHash)
	}

	// And it settles: the poll that follows finds the object already saying it.
	before := inv.writes
	s.tick(t.Context())
	if inv.writes != before {
		t.Errorf("%d further writes after the verdict settled", inv.writes-before)
	}
}

// The inverse ordering: the pass saw a conflict and overlaid its verdict, but the
// conflict cleared while that write was in flight. The successful pass result is
// now the current truth and must win in the same way a newly formed verdict does.
func TestAVerdictClearedDuringAPassesWriteDoesNotOutrankThePass(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"`

	inv := newFakeInventory("recovering")
	rec := newFakeReconciler("recovering")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the pass to start", func() bool { return rec.passes("recovering") == 1 })

	inv.mu.Lock()
	inv.invalid["recovering"] = reason
	inv.cause["recovering"] = v1alpha1.ReasonSecretNameConflict
	inv.mu.Unlock()
	s.tick(ctx)

	// The pass has already returned its successful result when UpdateStatus runs.
	// Clear the verdict there, after the pass decided to overlay it but before its
	// post-write check.
	inv.onFirstWrite = func() {
		inv.mu.Lock()
		delete(inv.invalid, "recovering")
		delete(inv.cause, "recovering")
		inv.mu.Unlock()

		s.mu.Lock()
		state := s.state["recovering"]
		state.invalidReason = ""
		state.invalidCause = ""
		s.mu.Unlock()
	}

	rec.releaseAll()
	s.wg.Wait()

	written := inv.written["recovering"]
	ready := condition(t, written, v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionTrue || ready.Reason != v1alpha1.ReasonReady {
		t.Errorf("the cleared verdict left Ready=%s/%s over the successful pass", ready.Status, ready.Reason)
	}
	if conflict := meta.FindStatusCondition(written.Conditions, v1alpha1.ConditionConflict); conflict != nil {
		t.Errorf("the cleared verdict left its Conflict condition behind: %+v", conflict)
	}
	if written.AppliedCredentialHash != "sha256:published-recovering" {
		t.Errorf("the pass's record of what it published was dropped: %q", written.AppliedCredentialHash)
	}
}

// If a verdict clears after an old pass starts, resolving a same-generation
// Secret conflict must remain immediately due after that pass finishes. Otherwise
// the pass's ordinary cadence postpones repair for five minutes.
func TestClearingAVerdictDuringAPassQueuesAFreshPass(t *testing.T) {
	t.Parallel()

	const reason = `secretName "cluster-shared" is also claimed by "other"`

	inv := newFakeInventory("retry-clear")
	rec := newFakeReconciler("retry-clear")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the first pass to start", func() bool { return rec.passes("retry-clear") == 1 })

	inv.mu.Lock()
	inv.invalid["retry-clear"] = reason
	inv.cause["retry-clear"] = v1alpha1.ReasonSecretNameConflict
	inv.mu.Unlock()
	s.tick(ctx)

	inv.mu.Lock()
	delete(inv.invalid, "retry-clear")
	delete(inv.cause, "retry-clear")
	inv.mu.Unlock()
	s.tick(ctx)

	rec.releaseAll()
	s.wg.Wait()

	// The due time set by the clearing poll survives the old pass's scheduling,
	// so the next poll starts a pass against the now-current verdict.
	s.tick(ctx)
	s.wg.Wait()
	if got := rec.passes("retry-clear"); got != 2 {
		t.Errorf("%d passes after the verdict cleared, want a fresh second pass", got)
	}
}
