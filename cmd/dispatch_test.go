package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

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
	deadline time.Time
	hadNo    bool
	written  map[string]v1alpha1.ClusterConnectionStatus

	// hang holds List until it is closed or the call's context ends.
	hang chan struct{}
}

func newFakeInventory(names ...string) *fakeInventory {
	return &fakeInventory{names: names, written: map[string]v1alpha1.ClusterConnectionStatus{}}
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
		entries = append(entries, inventory.Entry{
			Cluster:    config.Cluster{Name: name, Endpoint: name + ".example.com:6443"},
			Generation: 1,
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

func (f *fakeInventory) UpdateStatus(_ context.Context, name string, status v1alpha1.ClusterConnectionStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written[name] = status
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
	_ int64,
) (v1alpha1.ClusterConnectionStatus, error) {
	if f.fail[cluster.Name] {
		return prior, errors.New("downstream is unreachable")
	}
	return prior, nil
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
