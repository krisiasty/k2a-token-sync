package main

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
	"github.com/krisiasty/k2a-token-sync/internal/reconcile"
)

const (
	// pollInterval is how often the inventory is listed. It also sets the
	// granularity of scheduling, which is fine against a one-minute floor.
	//
	// A watch would make this sub-second, at the cost of a cache with a
	// lifecycle, reconnect handling and a queue to deduplicate events. The
	// inventory changes when a human adds a cluster, so that trade is the wrong
	// way round — and a list doubles as the recovery path: whatever was believed
	// before, the next poll replaces it.
	pollInterval = 30 * time.Second

	// maxPassInterval caps how long a healthy cluster goes unreconciled.
	//
	// It is also the worst case for noticing anything k2a-token-sync cannot watch.
	// Holding no read permission on the Secrets it writes, it learns what ArgoCD
	// actually has only from what its own apply returns — so a Secret deleted or
	// emptied out of band stays that way until the next pass, however healthy the
	// token behind it is.
	//
	// Minutes rather than a day because a pass on an unchanged cluster is now free:
	// the registration apply is a genuine no-op, since nothing written in it comes
	// from the clock, and the credential is reissued on its own schedule. What used
	// to make frequent passes expensive was a last-sync annotation that changed
	// every time; see registrationConfig in internal/argocd.
	maxPassInterval = 5 * time.Minute

	// minPassInterval floors the derived interval, so an aggressively capped
	// token lifetime cannot turn the loop into a busy wait.
	minPassInterval = 1 * time.Minute

	// retryInterval is how soon a failed cluster is retried, before backoff.
	retryInterval = 1 * time.Minute

	// maxRetryInterval caps the per-cluster exponential backoff.
	maxRetryInterval = 30 * time.Minute

	// clusterTimeout bounds one cluster's reconciliation.
	clusterTimeout = 5 * time.Minute

	// maxConcurrentPasses bounds how many clusters reconcile at the same time.
	//
	// A pass is almost entirely waiting on two API servers, so the bound is about
	// what those API servers see rather than about this process: four passes are a
	// handful of requests, and a fleet that has all gone unreachable at once cannot
	// turn into a thundering herd against a control plane that is already
	// struggling. It is also what keeps one stuck cluster to one slot instead of
	// the whole loop.
	//
	// Passes beyond the bound queue rather than being dropped, so the worst case
	// for a cluster at the back of the queue is still proportional to how many
	// clusters ahead of it are timing out. Raising this is the answer if that ever
	// becomes real; four suits a fleet of tens.
	maxConcurrentPasses = 4

	// inventoryTimeout bounds a single call to the API server about
	// ClusterConnection objects — the list, and each status write.
	//
	// Without it these inherit the process context, which is cancelled only at
	// shutdown: a connection that is accepted and then never answered hangs the
	// poll loop for as long as the process lives, and does it silently.
	inventoryTimeout = 30 * time.Second

	// dueSlack lets a cluster due within half a tick count as due now.
	//
	// Scheduling is quantised to the poll, so an interval can only ever land on a
	// tick. The question is which one, and without slack the answer was always the
	// tick *after* the interval elapsed: dueAt is computed from a clock read that
	// happens a little after a tick — the inventory list comes first — so it sits a
	// few milliseconds beyond the tick that should have caught it, and every
	// interval silently became one tick longer than the one configured. Five and a
	// half minutes for a configured five, measured live.
	//
	// Reading the clock earlier only moves that hazard around; anything between the
	// tick and the comparison reintroduces it. Rounding to the nearest tick instead
	// of the next one is insensitive to all of it.
	dueSlack = pollInterval / 2
)

// clusterInventory and clusterReconciler are the scheduler's view of its two
// collaborators. They are declared here, next to the code that uses them, so a
// test can stand in a cluster that never finishes or an inventory that never
// answers — neither of which the real implementations can be asked to do. The
// concrete types wired up in main satisfy them as they are.
type clusterInventory interface {
	List(ctx context.Context) ([]inventory.Entry, error)
	UpdateStatus(ctx context.Context, name string, status v1alpha1.ClusterConnectionStatus) error
}

type clusterReconciler interface {
	Cluster(
		ctx context.Context,
		cluster config.Cluster,
		prior v1alpha1.ClusterConnectionStatus,
		generation int64,
	) (v1alpha1.ClusterConnectionStatus, error)
}

// clusterState is what the scheduler remembers between passes. The durable half
// lives in the object's status; this is only the timing.
type clusterState struct {
	cluster    config.Cluster
	generation int64
	status     v1alpha1.ClusterConnectionStatus

	dueAt   time.Time
	backoff time.Duration

	// running is set while a pass for this cluster is dispatched but not yet
	// finished, including the time it spends queued for a slot. It is what stops
	// the polls that happen during a long pass from starting a second one.
	running bool

	// departed is set when a poll no longer finds this cluster in the inventory
	// but a pass for it is still in flight, so the state cannot be dropped yet.
	departed bool

	// passedGeneration is the spec generation this process last reconciled
	// successfully, and it is what readiness is judged on. Generations start at
	// one, so a zero here means this process has not yet finished a pass for this
	// cluster — which the persisted status cannot tell us, since it describes what
	// some earlier process did.
	passedGeneration int64

	// invalidReason is set when the object's spec cannot be resolved, or when
	// another ClusterConnection claims the same Secret. Such a cluster is never
	// reconciled, but it is still reported — on the object as well as in /status,
	// since the object is where an operator looks. invalidCause decides which
	// conditions say so.
	invalidReason string
	invalidCause  string
}

// scheduler polls the inventory and reconciles each cluster on its own cadence.
//
// Clusters are independent in timing and, up to maxConcurrentPasses, in
// parallelism: a failing cluster backs off alone, and a slow one occupies one
// slot rather than the loop. The poll never waits for a pass, so edits, new
// clusters and everything else the poll is for keep being noticed while a
// cluster sits in its five-minute timeout.
//
// mu guards the state map and everything reachable from it. A pass reads what it
// needs under the lock, runs without it, and writes its result back under it, so
// what is serialised is the bookkeeping rather than the work.
type scheduler struct {
	inv    clusterInventory
	rec    clusterReconciler
	logger *slog.Logger
	health *healthState

	mu    sync.Mutex
	state map[string]*clusterState

	slots chan struct{}
	wg    sync.WaitGroup

	now func() time.Time
}

func newScheduler(inv clusterInventory, rec clusterReconciler, logger *slog.Logger, health *healthState) *scheduler {
	return &scheduler{
		inv:    inv,
		rec:    rec,
		logger: logger,
		health: health,
		state:  make(map[string]*clusterState),
		slots:  make(chan struct{}, maxConcurrentPasses),
		now:    time.Now,
	}
}

func (s *scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Passes outlive the tick that dispatched them, so shutdown waits for them
	// here. Their contexts are derived from ctx, so this is a matter of moments:
	// what it buys is that a pass part-way through publishing a credential is not
	// abandoned between the two applies.
	defer s.wg.Wait()

	for {
		s.tick(ctx)

		select {
		case <-ctx.Done():
			s.logger.Info("shutting down")
			return
		case <-ticker.C:
		}
	}
}

// tick refreshes the inventory and starts whatever is due.
func (s *scheduler) tick(ctx context.Context) {
	if err := s.refresh(ctx); err != nil {
		if inventory.IsCRDMissing(err) {
			s.logger.Error("the ClusterConnection CRD is not installed; apply the chart's crds/ directory",
				"error", err)
		} else {
			s.logger.Error("listing the inventory failed", "error", err)
		}
		return
	}

	s.dispatch(ctx)
	s.reportRejected(ctx)

	s.health.recordPoll()
	s.publish()
}

// reportRejected writes the verdict onto every object this tool is declining to
// reconcile.
//
// Those objects are the ones an operator goes looking at, and until now they were
// the only ones that said nothing: excluded before reconciliation, so nothing ever
// wrote their status. 'kubectl get ccon' showed an empty Ready column, or worse a
// Ready=True left over from before the spec was broken, and the reason lived only
// in this process's /status endpoint.
//
// The whole phase shares one deadline rather than one per write. A poll that
// stretches is a poll that stops counting as progress, and a fleet of objects the
// API server is refusing to accept must not be able to push it there.
func (s *scheduler) reportRejected(ctx context.Context) {
	rejected := s.rejectedNeedingStatus()
	if len(rejected) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()

	for _, r := range rejected {
		if err := s.inv.UpdateStatus(ctx, r.name, r.status); err != nil {
			// Logged and stepped over: one object the API server will not accept must
			// not stop the others from being told why they are stuck.
			s.logger.Warn("writing the reason a cluster is not being reconciled failed",
				"cluster", r.name, "error", err)
			continue
		}
		s.logger.Info("cluster is not being reconciled", "cluster", r.name, "reason", r.reason)
	}
}

type rejection struct {
	name   string
	reason string
	status v1alpha1.ClusterConnectionStatus
}

// rejectedNeedingStatus lists the rejected clusters whose objects do not already
// say so.
//
// The comparison is against the status read in this poll, which is what makes a
// steady state cost nothing: the write happens once, the next poll reads it back,
// and from then on the desired status and the actual one are the same value. There
// is no bookkeeping of what was written, so a status edited or rolled back out of
// band is simply written again.
func (s *scheduler) rejectedNeedingStatus() []rejection {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []rejection
	for name, state := range s.state {
		if state.invalidReason == "" {
			continue
		}
		desired := rejectedStatus(state.status, state)
		// Compared exactly. Both sides come from the same read, so everything this
		// does not touch is identical by construction, and the cost of being wrong is
		// one redundant write rather than a missing one.
		if reflect.DeepEqual(desired, state.status) {
			continue
		}
		out = append(out, rejection{name: name, reason: state.invalidReason, status: desired})
	}
	slices.SortFunc(out, func(a, b rejection) int { return strings.Compare(a.name, b.name) })
	return out
}

// rejectedStatus lays the verdict over a status: what an object should say while
// this tool is declining to reconcile it. The caller holds the lock.
//
// It takes the status to work from rather than reading state.status, because it
// has two callers with two different starting points — the poll, which has only
// what the object already says, and a pass that finished after the verdict was
// formed, which has its own result and a fingerprint worth keeping. Applying it
// twice changes nothing, so the two agreeing matters more than which writes last.
func rejectedStatus(status v1alpha1.ClusterConnectionStatus, state *clusterState) v1alpha1.ClusterConnectionStatus {
	status.Conditions = slices.Clone(status.Conditions)

	// The generation that was rejected, so that fixing the object is visibly
	// different from this verdict still being current.
	status.ObservedGeneration = state.generation

	reason := state.invalidCause
	if reason == "" {
		// Not reachable from the inventory, which always says which kind it is. Here
		// because a condition with no reason is one the API server rejects outright,
		// which would lose the message as well as the reason.
		reason = v1alpha1.ReasonInvalidSpec
	}

	status.LastAction = "not reconciled: " + state.invalidReason
	setRejectedCondition(&status, v1alpha1.ConditionReady, metav1.ConditionFalse, reason, state)

	// Conflict is the one condition that names something outside this object, so it
	// is set only for the conflict itself and removed as soon as the cause is
	// something else. Leaving it behind would have an object blaming a neighbour for
	// a problem in its own spec.
	if reason == v1alpha1.ReasonSecretNameConflict {
		setRejectedCondition(&status, v1alpha1.ConditionConflict, metav1.ConditionTrue, reason, state)
	} else {
		meta.RemoveStatusCondition(&status.Conditions, v1alpha1.ConditionConflict)
	}
	return status
}

func setRejectedCondition(
	status *v1alpha1.ClusterConnectionStatus,
	conditionType string,
	value metav1.ConditionStatus,
	reason string,
	state *clusterState,
) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             value,
		Reason:             reason,
		Message:            state.invalidReason,
		ObservedGeneration: state.generation,
	})
}

// refresh reconciles the in-memory schedule with the objects in the API.
func (s *scheduler) refresh(ctx context.Context) error {
	listCtx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()

	entries, err := s.inv.List(listCtx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]struct{}, len(entries))
	now := s.now()

	for _, entry := range entries {
		name := entry.Cluster.Name
		seen[name] = struct{}{}

		state, known := s.state[name]
		if !known {
			state = &clusterState{dueAt: now, backoff: retryInterval}
			s.state[name] = state
			s.logger.Info("cluster added to the inventory", "cluster", name)
		}

		// An edited spec is due immediately, so 'kubectl edit' takes effect
		// within a poll rather than at the next scheduled pass. The comparison is
		// against the generation recorded in status, so it survives a restart:
		// an edit made while k2a-token-sync was down is still noticed.
		//
		// An edit that lands mid-pass is not lost either: the pass writes back the
		// generation it read, so the object still reads as edited afterwards and
		// the next poll dispatches it again.
		if entry.Edited() {
			state.dueAt = now
		}

		state.cluster = entry.Cluster
		state.generation = entry.Generation
		state.status = entry.Status
		state.invalidReason = entry.InvalidReason
		state.invalidCause = entry.InvalidCause
		state.departed = false
	}

	for name, state := range s.state {
		if _, still := seen[name]; still {
			continue
		}
		// A pass still holds this state and will write its result to it. Dropping
		// it now would let the same cluster be dispatched twice if it reappeared
		// before the pass finished, so it goes at the next poll instead — marked,
		// so that a pass still waiting for a slot abandons rather than publishing a
		// credential for a cluster that has just been decommissioned.
		if state.running {
			state.departed = true
			continue
		}
		delete(s.state, name)
		// The generated Secret is deliberately left behind: k2a-token-sync holds
		// no delete permission in ArgoCD's namespace, and removing a
		// registration is an operator's decision.
		s.logger.Info("cluster removed from the inventory; its ArgoCD Secret is left in place",
			"cluster", name)
	}
	return nil
}

// dispatch starts a pass for every cluster whose time has come and returns
// without waiting for any of them.
//
// Waiting was the old shape, and it made every cluster hostage to the slowest
// one: the poll could not run, so nothing else became due, an edit sat unseen,
// and the liveness probe — which excused any tick still in progress — reported a
// healthy loop the entire time. Each pass now runs on its own goroutine against
// a bounded pool, and keeps its own schedule and backoff as before.
func (s *scheduler) dispatch(ctx context.Context) {
	for _, state := range s.claimDue() {
		s.wg.Go(func() {
			defer s.releaseCluster(state)

			// Queuing for a slot happens before the pass is marked as started, so a
			// cluster waiting its turn is not mistaken for one that has wedged.
			select {
			case s.slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-s.slots }()

			s.reconcileOne(ctx, state)
		})
	}
}

// claimDue lists the clusters whose time has come, in a stable order so logs read
// consistently, and marks each as running.
//
// Selecting and claiming are one step on purpose: the claim is what a later poll
// consults to see that a pass is already under way, and anything between the two
// would be a window for dispatching the same cluster twice.
func (s *scheduler) claimDue() []*clusterState {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := s.now().Add(dueSlack)
	var due []*clusterState
	for _, state := range s.state {
		if state.running || blockedReason(state) != "" || state.dueAt.After(cutoff) {
			continue
		}
		state.running = true
		due = append(due, state)
	}
	slices.SortFunc(due, func(a, b *clusterState) int {
		return strings.Compare(a.cluster.Name, b.cluster.Name)
	})
	return due
}

// blockedReason says why a cluster must not be reconciled, or returns empty if it
// may be. The caller holds the lock.
func blockedReason(state *clusterState) string {
	switch {
	case state.invalidReason != "":
		return state.invalidReason
	case state.departed:
		return "it is no longer in the inventory"
	default:
		return ""
	}
}

func (s *scheduler) releaseCluster(state *clusterState) {
	s.mu.Lock()
	state.running = false
	s.mu.Unlock()
}

func (s *scheduler) reconcileOne(ctx context.Context, state *clusterState) {
	// Passes no longer finish inside the poll that started them, so each one
	// publishes its own result — on the way out of either branch, since a pass that
	// failed is the one worth seeing. Without this /status and readiness would lag
	// a whole poll behind the work they describe.
	defer s.publish()

	s.mu.Lock()
	// Claimed at dispatch, checked again here, because the two can be a slot's
	// wait apart. In between, a poll may have found another ClusterConnection
	// claiming this cluster's Secret, or the object may have been deleted
	// altogether. Both are decided by refusing to write, so a pass that acts on the
	// claim it was given rather than on what is true now would walk straight
	// through a check that exists to fail closed.
	if reason := blockedReason(state); reason != "" {
		name := state.cluster.Name
		s.mu.Unlock()
		s.logger.Info("abandoning a pass that was waiting for a slot", "cluster", name, "reason", reason)
		return
	}
	cluster, generation := state.cluster, state.generation
	prior := state.status
	// The reconciler edits the conditions it is given, and /status reads them from
	// here while it does, so the pass gets a slice of its own.
	prior.Conditions = slices.Clone(prior.Conditions)
	// This pass running is itself the proof that the conflict is over — a contested
	// cluster never gets one. The reconciler knows nothing about Conflict and would
	// carry it forward untouched, leaving an object that reconciles perfectly well
	// still accusing a neighbour that may not even exist any more.
	meta.RemoveStatusCondition(&prior.Conditions, v1alpha1.ConditionConflict)
	s.mu.Unlock()

	// When the pass started, not when it finished. Scheduling from the end would add
	// the pass's own duration to every interval, and since that duration always
	// carries dueAt past the poll tick that would have caught it, each interval
	// silently became one whole tick longer than the one configured.
	startedAt := s.now()
	s.health.passStarted(cluster.Name, startedAt)
	defer s.health.passFinished(cluster.Name)

	passCtx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	status, err := s.rec.Cluster(passCtx, cluster, prior, generation)

	// A verdict can be formed while a pass is running: any poll may find the object
	// contested or its spec broken, and this pass started before that. Writing its
	// own result now would put Ready=True back on an object this tool has just
	// stopped reconciling — and in the conflict case the generation has not changed,
	// so nothing about that Ready=True would look stale.
	//
	// So the verdict goes over the pass's result rather than the pass's result over
	// the verdict. Both writers apply the same one, which is what makes it safe for
	// either to write last; what the pass adds is its own fingerprint, since it did
	// publish a credential and dropping that would cost a needless reissue later.
	//
	// Departure is deliberately not a verdict here, unlike at the start of a pass:
	// there may no longer be an object to write to, and "no longer in the inventory"
	// is not something to say about a spec.
	//
	// What this does not close is a verdict formed in the moment between this read
	// and the write below: the object then carries the pass's result until the next
	// poll writes the verdict over it. Closing that needs a per-object lock held
	// across two API calls, which is a lot of machinery for one poll of staleness
	// that repairs itself.
	s.mu.Lock()
	blocked := state.invalidReason
	if blocked != "" {
		status = rejectedStatus(status, state)
	}
	s.mu.Unlock()

	if blocked != "" {
		s.logger.Info("a pass finished for a cluster that has since been blocked",
			"cluster", cluster.Name, "reason", blocked)
	}

	if writeErr := s.writeStatus(ctx, cluster.Name, status); writeErr != nil {
		// Losing the status write is not fatal, but it does mean the next pass
		// reissues: the fingerprint it would have compared against is gone.
		s.logger.Warn("writing status failed; the next pass will reissue",
			"cluster", cluster.Name, "error", writeErr)
	}

	s.mu.Lock()
	state.status = status
	if err != nil {
		// Backoff is measured from the end of a failed pass on purpose: a pass that
		// fails slowly, on a timeout say, should not be retried the instant it
		// returns.
		state.dueAt = s.now().Add(state.backoff)
		retryIn := state.backoff
		state.backoff = min(state.backoff*2, maxRetryInterval)
		s.mu.Unlock()

		s.logger.Info("retrying later", "cluster", cluster.Name,
			"retry_in", retryIn.Round(time.Second).String())
		return
	}
	state.backoff = retryInterval
	state.dueAt = dueAfterPass(startedAt, status)
	// What readiness is judged on: this process has now reconciled this spec, as
	// opposed to having read a status left behind by a process that did.
	state.passedGeneration = generation
	s.mu.Unlock()
}

// writeStatus records a pass's result against the object it belongs to.
//
// It takes the loop's context rather than the pass's: a pass that ran out of time
// still has something worth writing down, and that is precisely the pass whose
// deadline has already gone.
func (s *scheduler) writeStatus(ctx context.Context, name string, status v1alpha1.ClusterConnectionStatus) error {
	writeCtx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()
	return s.inv.UpdateStatus(writeCtx, name, status)
}

// dueAfterPass is when a cluster whose pass just succeeded is next due.
//
// It takes only the moment the pass began, so the pass's own duration cannot leak
// into the interval — which is the whole point, and why this is a function rather
// than two lines inline reading the clock again.
func dueAfterPass(startedAt time.Time, status v1alpha1.ClusterConnectionStatus) time.Time {
	return startedAt.Add(nextInterval(status, startedAt))
}

// nextInterval decides when a healthy cluster is next due.
//
// The cap governs any ordinary cluster, since half of even a short token's life is
// longer than a few minutes. What the remaining lifetime still decides is the
// pathological case: an API server may cap token lifetime via
// --service-account-max-token-expiration, and against a token measured in minutes
// half the remaining life is the interval that keeps a margin of one whole
// refresh. The floor stops that from becoming a busy wait.
func nextInterval(status v1alpha1.ClusterConnectionStatus, now time.Time) time.Duration {
	if status.TokenExpiresAt == nil || status.TokenExpiresAt.IsZero() {
		return maxPassInterval
	}
	half := status.TokenExpiresAt.Sub(now) / 2
	return max(min(maxPassInterval, half), minPassInterval)
}

// publish hands /status and /readyz a fresh snapshot of the schedule.
//
// Building the snapshot and publishing it happen under one hold of the lock, and
// that is the whole point of the method existing. Two passes finishing at once
// could otherwise build their snapshots in one order and publish them in the
// other, leaving the older of the two on display until something else published —
// which for readiness means reporting a fleet as ready after a cluster in it has
// just failed.
//
// The lock is held across the call into healthState, which has a lock of its own.
// That ordering is safe because nothing in healthState calls back into the
// scheduler; it only stores what it is handed.
func (s *scheduler) publish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health.record(s.reportLocked(), s.nextDueInLocked())
}

// nextDueInLocked is how long until the earliest scheduled reconciliation. The
// caller holds the lock.
//
// A cluster whose pass is running is skipped: its dueAt belongs to the pass that
// has already started and would otherwise read as permanently overdue.
func (s *scheduler) nextDueInLocked() time.Duration {
	now := s.now()
	next := maxPassInterval
	for _, state := range s.state {
		if state.invalidReason != "" || state.running {
			continue
		}
		if d := state.dueAt.Sub(now); d < next {
			next = d
		}
	}
	return max(next, 0)
}

// reportLocked renders the current state for /status. The caller holds the lock.
func (s *scheduler) reportLocked() []clusterReport {
	out := make([]clusterReport, 0, len(s.state))
	for _, state := range s.state {
		report := clusterReport{
			Name:                     state.cluster.Name,
			Endpoint:                 state.cluster.Endpoint,
			Secret:                   state.status.Secret,
			LastAction:               state.status.LastAction,
			ServingCertDaysRemaining: state.status.ServingCertDaysRemaining,
			DueAt:                    state.dueAt,
		}
		// Synced means this process has reconciled the spec that is in the API now.
		// A persisted Ready condition is not enough on its own: after a restart it
		// describes what some earlier process managed, and after an edit it describes
		// the spec before the edit — and taking either at face value would let
		// readiness pass while every pass was still queued. The condition still has
		// to agree, since a pass can succeed and report a cluster as not ready.
		reconciledNow := state.passedGeneration != 0 && state.passedGeneration == state.generation

		if state.invalidReason != "" {
			report.Error = state.invalidReason
		} else if cond := meta.FindStatusCondition(state.status.Conditions, v1alpha1.ConditionReady); cond != nil {
			report.Synced = reconciledNow && cond.Status == metav1.ConditionTrue
			if cond.Status != metav1.ConditionTrue {
				report.Error = cond.Message
			}
		}
		if state.status.TokenExpiresAt != nil {
			report.TokenExpiresAt = state.status.TokenExpiresAt.Time
		}
		if state.status.ServingCertExpiresAt != nil {
			report.ServingCertExpiresAt = state.status.ServingCertExpiresAt.Time
		}
		if state.status.LastSyncTime != nil {
			report.SyncedAt = state.status.LastSyncTime.Time
		}
		out = append(out, report)
	}
	slices.SortFunc(out, func(a, b clusterReport) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// compile-time proof that the types main wires in still fit what the scheduler
// asks for, since nothing else in the package refers to them by concrete type.
var (
	_ clusterInventory  = (*inventory.Client)(nil)
	_ clusterReconciler = (*reconcile.Reconciler)(nil)
)
