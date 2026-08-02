package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

// fakeInventory answers from a fixed set of clusters and can be told to stop
// answering, which is the failure the real client cannot be asked to reproduce.
type fakeInventory struct {
	mu       sync.Mutex
	names    []string
	calls    int
	writes   int
	deadline time.Time
	hadNo    bool
	written  map[string]v1alpha1.ClusterConnectionStatus

	// invalid, generation and prior stand for what the API would hold: a Secret
	// another connection claims, an edited spec, and a status left behind by an
	// earlier process.
	invalid    map[string]string
	cause      map[string]string
	generation int64
	prior      map[string]v1alpha1.ClusterConnectionStatus

	// hang holds List until it is closed or the call's context ends.
	hang chan struct{}

	// onFirstWrite runs once, during the first status write, and is how a test puts
	// something in the gap between a writer deciding what to say and the API
	// receiving it.
	onFirstWrite func()
}

func newFakeInventory(names ...string) *fakeInventory {
	return &fakeInventory{
		names:      names,
		written:    map[string]v1alpha1.ClusterConnectionStatus{},
		invalid:    map[string]string{},
		cause:      map[string]string{},
		prior:      map[string]v1alpha1.ClusterConnectionStatus{},
		generation: 1,
	}
}

// readyStatus is a status of the kind a previous process would have left in the
// API: the cluster reconciled, and said so. It describes generation 1, so it goes
// stale the moment a test edits the spec.
func readyStatus() v1alpha1.ClusterConnectionStatus {
	const generation = int64(1)

	return v1alpha1.ClusterConnectionStatus{
		ObservedGeneration: generation,
		LastAction:         "up-to-date",
		Conditions: []metav1.Condition{{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             v1alpha1.ReasonReady,
			Message:            "ArgoCD holds a current credential for this cluster",
			ObservedGeneration: generation,
			LastTransitionTime: metav1.Now(),
		}},
	}
}

func (f *fakeInventory) List(ctx context.Context) ([]inventory.Entry, error) {
	f.mu.Lock()
	f.calls++
	if deadline, ok := ctx.Deadline(); ok {
		f.deadline = deadline
	} else {
		f.hadNo = true
	}
	hang := f.hang
	entries := make([]inventory.Entry, 0, len(f.names))
	for _, name := range f.names {
		status := f.prior[name]
		entries = append(entries, inventory.Entry{
			Cluster:            config.Cluster{Name: name, Endpoint: name + ".example.com:6443"},
			Generation:         f.generation,
			ObservedGeneration: status.ObservedGeneration,
			Status:             status,
			InvalidReason:      f.invalid[name],
			InvalidCause:       f.invalidCause(name),
		})
	}
	f.mu.Unlock()

	if hang != nil {
		select {
		case <-hang:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return entries, nil
}

// invalidCause mirrors the real inventory, which never reports a reason without
// saying which kind it is.
func (f *fakeInventory) invalidCause(name string) string {
	if f.invalid[name] == "" {
		return ""
	}
	if cause := f.cause[name]; cause != "" {
		return cause
	}
	return v1alpha1.ReasonInvalidSpec
}

func (f *fakeInventory) UpdateStatus(_ context.Context, name string, status v1alpha1.ClusterConnectionStatus) error {
	f.mu.Lock()
	f.written[name] = status
	f.writes++
	// Read back by the next List, as the API server's copy would be. Without this a
	// test cannot tell a write that happens once from one that repeats every poll.
	f.prior[name] = status
	hook := f.onFirstWrite
	f.onFirstWrite = nil
	f.mu.Unlock()

	// Run outside the fake's own lock: hooks reach into the scheduler, and nothing
	// that holds the scheduler's lock ever calls in here.
	if hook != nil {
		hook()
	}
	return nil
}

func (f *fakeInventory) listDeadline() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadline, !f.hadNo && !f.deadline.IsZero()
}

// fakeReconciler records every pass and can hold the ones it is told to.
type fakeReconciler struct {
	mu       sync.Mutex
	calls    map[string]int
	inFlight int
	peak     int

	// held names the clusters whose pass blocks on release.
	held    map[string]bool
	release chan struct{}
}

func newFakeReconciler(held ...string) *fakeReconciler {
	f := &fakeReconciler{
		calls:   map[string]int{},
		held:    map[string]bool{},
		release: make(chan struct{}),
	}
	for _, name := range held {
		f.held[name] = true
	}
	return f
}

func (f *fakeReconciler) Cluster(
	ctx context.Context,
	cluster config.Cluster,
	prior v1alpha1.ClusterConnectionStatus,
	generation int64,
) (v1alpha1.ClusterConnectionStatus, error) {
	f.mu.Lock()
	f.calls[cluster.Name]++
	f.inFlight++
	f.peak = max(f.peak, f.inFlight)
	hold := f.held[cluster.Name]
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if hold {
		select {
		case <-f.release:
		case <-ctx.Done():
			return prior, ctx.Err()
		}
	}

	status := prior
	status.ObservedGeneration = generation
	status.LastAction = "up-to-date"
	// Stands for the pass's own work: the record of what it published, which a
	// later writer must not drop.
	status.AppliedCredentialHash = "sha256:published-" + cluster.Name
	// The real reconciler sets Ready on every path it returns from, and tests about
	// stale conditions depend on that, so the fake does too.
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1alpha1.ReasonReady,
		Message:            "ArgoCD holds a current credential for this cluster",
		ObservedGeneration: generation,
	})
	return status, nil
}

func (f *fakeReconciler) passes(cluster string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[cluster]
}

func (f *fakeReconciler) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inFlight
}

func (f *fakeReconciler) highWaterMark() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

// holdAll blocks every cluster the fake is asked about.
func (f *fakeReconciler) holdAll(names ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, name := range names {
		f.held[name] = true
	}
}

func (f *fakeReconciler) releaseAll() { close(f.release) }

func testScheduler(t *testing.T, inv clusterInventory, rec clusterReconciler) *scheduler {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	return newScheduler(inv, rec, logger, newHealthState())
}

// waitFor blocks until cond holds, and fails the test if it never does. The
// polling is what keeps these tests free of sleeps calibrated to a machine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// fillPool takes every slot, so a pass dispatched afterwards is guaranteed to
// queue rather than run. The returned function frees one slot, letting exactly
// one queued pass proceed — which is what makes the wait deterministic instead of
// a race against the scheduler.
func fillPool(t *testing.T, s *scheduler) func() {
	t.Helper()
	for range maxConcurrentPasses {
		s.slots <- struct{}{}
	}
	return func() { <-s.slots }
}

// A pass claims its cluster when it is dispatched and may then wait for a slot.
// What was true at the claim need not still be true when the slot arrives, and
// two of the things that can change in between are decided by refusing to write.
func TestAQueuedPassRechecksItsClusterBeforeWriting(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// change happens while the pass is queued.
		change func(inv *fakeInventory)
	}{
		{
			// The fail-closed rule from the Secret-conflict work: when two
			// ClusterConnections claim one Secret, both stand down. A queued pass that
			// acted on its claim would walk straight through it and publish the
			// contested Secret against its own endpoint.
			name: "another connection claims the same Secret",
			change: func(inv *fakeInventory) {
				inv.invalid["queued"] = `"other" claims the same Secret`
			},
		},
		{
			// A cluster deleted while its pass was queued should not have a fresh
			// credential published for it.
			name:   "the cluster leaves the inventory",
			change: func(inv *fakeInventory) { inv.names = nil },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inv := newFakeInventory("queued")
			rec := newFakeReconciler()
			s := testScheduler(t, inv, rec)
			free := fillPool(t, s)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			// Dispatched, claimed, and now waiting for a slot that nothing will free.
			s.tick(ctx)
			waitFor(t, "the pass to be claimed", func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.state["queued"].running
			})
			if rec.passes("queued") != 0 {
				t.Fatal("the pass ran although every slot was taken")
			}

			inv.mu.Lock()
			tc.change(inv)
			inv.mu.Unlock()
			s.tick(ctx)

			free()
			s.wg.Wait()

			if got := rec.passes("queued"); got != 0 {
				t.Errorf("%d passes ran; the cluster was no longer one this tool may write", got)
			}
			for name, status := range inv.written {
				if !strings.HasPrefix(status.LastAction, "not reconciled") {
					t.Errorf("%s had a reconciliation result written (lastAction %q); the pass must not have run",
						name, status.LastAction)
				}
			}
		})
	}
}

// Readiness means every cluster in the inventory has reconciled — and a status
// this process merely read does not establish that.
//
// A restarted pod finds Ready=True on every object, from the process before it.
// Reporting ready off the back of that would have /readyz pass while every pass
// was still queued, which is exactly the window a rollout uses to decide the new
// pod is serving.
func TestReadinessWaitsForAPassInThisProcess(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("restarted")
	inv.prior["restarted"] = readyStatus() // what the previous process left behind
	rec := newFakeReconciler()
	s := testScheduler(t, inv, rec)
	free := fillPool(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	if s.health.isReady() {
		t.Error("ready before this process reconciled anything, on the strength of a status it only read")
	}
	if published := s.health.report().Clusters; published[0].Synced {
		t.Error("a cluster reads as synced although its first pass has not run")
	}

	free()
	s.wg.Wait()

	if !s.health.isReady() {
		t.Error("not ready after every cluster reconciled")
	}
}

// The same reasoning after an edit: the Ready condition on the object describes
// the spec as it was before the edit.
func TestReadinessDropsUntilAnEditedSpecHasReconciled(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("edited")
	inv.prior["edited"] = readyStatus()
	rec := newFakeReconciler()
	s := testScheduler(t, inv, rec)

	s.tick(t.Context())
	s.wg.Wait()
	if !s.health.isReady() {
		t.Fatal("not ready after a successful pass")
	}

	inv.mu.Lock()
	inv.generation = 2
	inv.prior["edited"] = readyStatus() // still describes generation 1
	inv.mu.Unlock()

	rec.holdAll("edited")
	s.tick(t.Context())
	if s.health.isReady() {
		t.Error("ready while the edited spec has not been reconciled by anyone")
	}

	rec.releaseAll()
	s.wg.Wait()
	if !s.health.isReady() {
		t.Error("not ready after the edited spec reconciled")
	}
}

// The snapshot must be built and published without letting go of the lock in
// between, which is the only thing that orders two completions publishing at once.
//
// This asserts the mechanism rather than the symptom, because the symptom is a
// narrow interleaving that a test cannot reliably provoke — the check below it
// caught the old structure roughly once in ten runs. healthState's clock is the
// way in: record reads it, so a probe installed there runs at exactly the moment
// the snapshot is being published, and can ask whether the scheduler's lock is
// still held. TryLock is not how production code should ever ask that; here the
// question is the point of the test.
func TestASnapshotIsBuiltAndPublishedWithoutReleasingTheLock(t *testing.T) {
	t.Parallel()

	s := testScheduler(t, newFakeInventory("c"), newFakeReconciler())

	probed := make(chan bool, 4)
	s.health.now = func() time.Time {
		if s.mu.TryLock() {
			s.mu.Unlock()
			probed <- false
		} else {
			probed <- true
		}
		return time.Now()
	}

	s.publish()

	select {
	case held := <-probed:
		if !held {
			t.Error("the snapshot was published with the lock released; a completion in that gap " +
				"can publish a newer snapshot that this one then overwrites")
		}
	default:
		t.Fatal("nothing was published")
	}
}

// What several passes finishing at once must leave behind is the newest view of
// the fleet, not whichever view happened to be published last.
//
// Building a snapshot and publishing it used to be separate synchronised steps,
// so two completions could build in one order and publish in the other. The
// older snapshot then stayed on display until something else published, which
// for readiness means reporting a fleet as ready moments after a cluster in it
// failed.
//
// This cannot force the interleaving, and against the old structure it caught it
// in about one run in ten even repeated twenty times over. It stands as the
// end-state check — once every pass has finished, what is published has to agree
// with what happened — with the test above it pinning the mechanism outright.
func TestConcurrentCompletionsLeaveTheNewestSnapshotPublished(t *testing.T) {
	t.Parallel()

	names := []string{"a-fails", "b-ok", "c-ok", "d-ok", "e-ok", "f-ok"}
	inv := newFakeInventory(names...)
	for _, name := range names {
		inv.prior[name] = readyStatus()
	}
	s := testScheduler(t, inv, &failingReconciler{fail: map[string]bool{"a-fails": true}})

	// Repeated, because a single round catches an ordering slip only when the pass
	// holding the stale snapshot happens to be the last one to publish, and the
	// window it needs is a lock handoff wide.
	for round := range 20 {
		s.mu.Lock()
		for _, state := range s.state {
			state.dueAt = time.Now().Add(-time.Minute)
		}
		s.mu.Unlock()

		s.tick(t.Context())
		s.wg.Wait()

		published := s.health.report()
		byName := make(map[string]clusterReport, len(published.Clusters))
		for _, report := range published.Clusters {
			byName[report.Name] = report
		}
		for _, name := range names {
			report, ok := byName[name]
			if !ok {
				t.Fatalf("round %d: %s is missing from the published snapshot", round, name)
			}
			if name == "a-fails" {
				if report.Synced || report.Error == "" {
					t.Fatalf("round %d: the failed cluster reads as synced=%v error=%q",
						round, report.Synced, report.Error)
				}
				continue
			}
			if !report.Synced {
				t.Fatalf("round %d: %s reconciled, but the published snapshot predates its pass", round, name)
			}
		}
		if published.Ready {
			t.Fatalf("round %d: ready although one cluster's pass failed", round)
		}
	}
}

// A failed pass has to reach /status when it fails, not at the next poll.
func TestAFailedPassPublishesItsResultImmediately(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("failing")
	s := testScheduler(t, inv, &failingReconciler{fail: map[string]bool{"failing": true}})
	free := fillPool(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The poll publishes a snapshot from before the pass, and no further poll
	// happens in this test: whatever appears afterwards came from the pass itself.
	s.tick(ctx)
	free()
	s.wg.Wait()

	report := s.health.report()
	if len(report.Clusters) != 1 {
		t.Fatalf("the health snapshot holds %d clusters, want 1", len(report.Clusters))
	}
	if report.Clusters[0].Error == "" {
		t.Error("a failed pass left no error in /status until the next poll")
	}
	if report.Ready {
		t.Error("ready although the only cluster's pass failed")
	}
}

// The point of the whole change: a cluster stuck in its five-minute timeout must
// not keep every other cluster from being reconciled.
//
// Serially, "slow" and "stuck" were the same thing — a pass that hangs holds the
// poll loop, so nothing else becomes due, an edited spec is not noticed, and a
// token elsewhere in the fleet can expire while the loop is technically working.
func TestAStuckClusterDoesNotHoldUpTheRest(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("stuck-a", "stuck-b", "healthy")
	rec := newFakeReconciler("stuck-a", "stuck-b")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)

	// The poll itself must be back long before the passes it started are, and the
	// healthy cluster must reconcile to completion while the other two hang.
	waitFor(t, "every pass to start", func() bool {
		return rec.passes("healthy") == 1 && rec.passes("stuck-a") == 1 && rec.passes("stuck-b") == 1
	})
	waitFor(t, "the healthy cluster to finish while the others hang", func() bool {
		return rec.running() == 2
	})

	// And the loop keeps polling while they are stuck, which is what lets a new or
	// edited cluster be picked up at all.
	s.tick(ctx)
	inv.mu.Lock()
	calls := inv.calls
	inv.mu.Unlock()
	if calls != 2 {
		t.Errorf("the inventory was listed %d times, want 2 — the second poll waited on a stuck pass", calls)
	}

	rec.releaseAll()
	s.wg.Wait()
}

// A cluster whose pass is still running must not be started again by the polls
// that happen while it runs, however overdue it looks.
func TestAClusterIsNotStartedTwiceWhileItsPassRuns(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("slow")
	rec := newFakeReconciler("slow")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the first pass to start", func() bool { return rec.passes("slow") == 1 })

	for range 3 {
		s.tick(ctx)
	}

	// Counting only after every dispatched pass has finished. Reading the counter
	// straight after the polls would pass whether or not a second pass was
	// dispatched, since a goroutine that has been started has not necessarily run.
	rec.releaseAll()
	s.wg.Wait()

	if got := rec.passes("slow"); got != 1 {
		t.Errorf("%d passes for one cluster, want 1 — a running pass was dispatched again", got)
	}

	// Once it finishes it is schedulable again, on its own interval.
	s.mu.Lock()
	running, dueAt := s.state["slow"].running, s.state["slow"].dueAt
	s.mu.Unlock()
	if running {
		t.Error("the cluster is still marked as running after its pass returned")
	}
	if !dueAt.After(time.Now()) {
		t.Error("a cluster that has just reconciled is due again immediately")
	}
}

// Concurrency is bounded, so an unreachable fleet cannot turn into a burst of
// requests against a control plane that is already having a bad day.
func TestNoMoreThanTheBoundReconcileAtOnce(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, maxConcurrentPasses+2)
	for i := range maxConcurrentPasses + 2 {
		names = append(names, string(rune('a'+i))+"-cluster")
	}

	inv := newFakeInventory(names...)
	rec := newFakeReconciler()
	rec.holdAll(names...)
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the pool to fill", func() bool { return rec.running() == maxConcurrentPasses })

	// Give any excess a chance to appear before concluding it will not.
	time.Sleep(50 * time.Millisecond)
	if got := rec.highWaterMark(); got > maxConcurrentPasses {
		t.Errorf("%d passes ran at once, want at most %d", got, maxConcurrentPasses)
	}

	// The ones over the bound are queued, not dropped.
	rec.releaseAll()
	s.wg.Wait()
	for _, name := range names {
		if rec.passes(name) != 1 {
			t.Errorf("%s reconciled %d times, want 1 — a queued pass was lost", name, rec.passes(name))
		}
	}
}

// An inventory call that is accepted and then never answered used to hang the
// poll loop for the life of the process, silently: nothing else in the loop has
// a deadline of its own, and the process context is cancelled only at shutdown.
func TestAStalledInventoryCallIsBoundedByADeadline(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("c")
	inv.hang = make(chan struct{}) // never closed
	s := testScheduler(t, inv, newFakeReconciler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- s.refresh(ctx) }()

	waitFor(t, "the list to be in flight", func() bool {
		_, ok := inv.listDeadline()
		return ok
	})

	deadline, ok := inv.listDeadline()
	if !ok {
		t.Fatal("the inventory was listed with no deadline; a call that is never answered would hang the loop forever")
	}
	// The second of slack is for the gap between reading the clock here and the
	// goroutine reaching the call; the assertion that matters is the order of
	// magnitude — a bound, rather than the process lifetime.
	if left := deadline.Sub(started); left > inventoryTimeout+time.Second {
		t.Errorf("the list may run for %v, want no more than %v", left, inventoryTimeout)
	}

	select {
	case err := <-done:
		t.Fatalf("refresh returned %v while the list was still hanging", err)
	default:
	}

	// Shutdown must still reach it, deadline or not.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("refresh returned %v after cancellation, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh ignored cancellation")
	}
}

// A poll that fails is not progress: liveness is judged on polls that completed,
// so a permanently stalled inventory eventually shows up as a stalled loop
// rather than as a pass that is forever in progress.
func TestAFailedPollIsNotRecordedAsProgress(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("c")
	inv.hang = make(chan struct{})
	s := testScheduler(t, inv, newFakeReconciler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := s.health.polledAt
	go func() { s.tick(ctx) }()
	waitFor(t, "the list to be in flight", func() bool {
		_, ok := inv.listDeadline()
		return ok
	})
	cancel()

	waitFor(t, "the tick to return", func() bool {
		s.health.mu.RLock()
		defer s.health.mu.RUnlock()
		return s.health.polledAt.Equal(before)
	})

	s.health.mu.RLock()
	defer s.health.mu.RUnlock()
	if !s.health.polledAt.Equal(before) {
		t.Error("a poll that never read the inventory counted as progress")
	}
}

// Failure is per cluster: a cluster that fails backs off, and the one next to it
// keeps its own schedule.
func TestBackoffStaysWithTheClusterThatFailed(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("bad", "good")
	rec := &failingReconciler{fail: map[string]bool{"bad": true}}
	s := testScheduler(t, inv, rec)

	s.tick(t.Context())
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	bad, good := s.state["bad"], s.state["good"]
	if bad.backoff <= retryInterval {
		t.Errorf("the failed cluster's backoff is %v, want it grown past %v", bad.backoff, retryInterval)
	}
	if good.backoff != retryInterval {
		t.Errorf("a healthy cluster's backoff moved to %v because another cluster failed", good.backoff)
	}
	if !good.dueAt.After(bad.dueAt) {
		t.Error("the failed cluster is due before the healthy one; backoff was not applied")
	}
}

type failingReconciler struct {
	fail map[string]bool
}

func (f *failingReconciler) Cluster(
	_ context.Context,
	cluster config.Cluster,
	prior v1alpha1.ClusterConnectionStatus,
	generation int64,
) (v1alpha1.ClusterConnectionStatus, error) {
	if !f.fail[cluster.Name] {
		return prior, nil
	}
	// A failing pass records why on the object, as the real reconciler does; that
	// message is what /status and 'kubectl describe' show.
	err := errors.New("downstream is unreachable")
	status := prior
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1alpha1.ReasonEndpointUnreachable,
		Message:            err.Error(),
		ObservedGeneration: generation,
	})
	status.LastAction = "failed"
	return status, err
}

// A cluster that disappears from the inventory mid-pass is dropped once its pass
// is over, not while a goroutine is still writing to its state.
func TestAClusterRemovedMidPassIsDroppedOnceItsPassEnds(t *testing.T) {
	t.Parallel()

	inv := newFakeInventory("going")
	rec := newFakeReconciler("going")
	s := testScheduler(t, inv, rec)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s.tick(ctx)
	waitFor(t, "the pass to start", func() bool { return rec.passes("going") == 1 })

	inv.mu.Lock()
	inv.names = slices.Delete(inv.names, 0, 1)
	inv.mu.Unlock()

	s.tick(ctx)
	s.mu.Lock()
	_, stillTracked := s.state["going"]
	s.mu.Unlock()
	if !stillTracked {
		t.Error("a cluster was forgotten while its own pass was still writing to it")
	}

	rec.releaseAll()
	s.wg.Wait()

	s.tick(ctx)
	s.mu.Lock()
	_, tracked := s.state["going"]
	s.mu.Unlock()
	if tracked {
		t.Error("a cluster that left the inventory is still scheduled after its pass finished")
	}
}
