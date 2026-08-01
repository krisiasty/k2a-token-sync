package main

import (
	"context"
	"log/slog"
	"slices"
	"strings"
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

// clusterState is what the scheduler remembers between passes. The durable half
// lives in the object's status; this is only the timing.
type clusterState struct {
	cluster    config.Cluster
	generation int64
	status     v1alpha1.ClusterConnectionStatus

	dueAt   time.Time
	backoff time.Duration

	// invalidReason is set when the object's spec cannot be resolved. Such a
	// cluster is never reconciled, but it is still reported.
	invalidReason string
}

// scheduler polls the inventory and reconciles each cluster on its own cadence.
//
// One goroutine does the work, serially. Clusters are independent in *timing*
// rather than in parallelism: a failing cluster backs off alone, and the others
// keep their own schedules, which is the point. Running them concurrently would
// add locking around shared state for no benefit at this scale.
type scheduler struct {
	inv    *inventory.Client
	rec    *reconcile.Reconciler
	logger *slog.Logger
	health *healthState

	state map[string]*clusterState
	now   func() time.Time
}

func newScheduler(inv *inventory.Client, rec *reconcile.Reconciler, logger *slog.Logger, health *healthState) *scheduler {
	return &scheduler{
		inv:    inv,
		rec:    rec,
		logger: logger,
		health: health,
		state:  make(map[string]*clusterState),
		now:    time.Now,
	}
}

func (s *scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

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

// tick refreshes the inventory and reconciles whatever is due.
func (s *scheduler) tick(ctx context.Context) {
	s.health.recordAttempt()

	if err := s.refresh(ctx); err != nil {
		if inventory.IsCRDMissing(err) {
			s.logger.Error("the ClusterConnection CRD is not installed; apply the chart's crds/ directory",
				"error", err)
		} else {
			s.logger.Error("listing the inventory failed", "error", err)
		}
		return
	}

	for _, name := range s.dueClusters() {
		if ctx.Err() != nil {
			return
		}
		s.reconcileOne(ctx, s.state[name])
	}

	s.health.record(s.report(), s.nextDueIn())
}

// refresh reconciles the in-memory schedule with the objects in the API.
func (s *scheduler) refresh(ctx context.Context) error {
	entries, err := s.inv.List(ctx)
	if err != nil {
		return err
	}

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
		if entry.Edited() {
			state.dueAt = now
		}

		state.cluster = entry.Cluster
		state.generation = entry.Generation
		state.status = entry.Status
		state.invalidReason = entry.InvalidReason
	}

	for name := range s.state {
		if _, still := seen[name]; !still {
			delete(s.state, name)
			// The generated Secret is deliberately left behind: k2a-token-sync holds
			// no delete permission in ArgoCD's namespace, and removing a
			// registration is an operator's decision.
			s.logger.Info("cluster removed from the inventory; its ArgoCD Secret is left in place",
				"cluster", name)
		}
	}
	return nil
}

// dueClusters lists the clusters whose time has come, in a stable order so logs
// read consistently.
func (s *scheduler) dueClusters() []string {
	cutoff := s.now().Add(dueSlack)
	var due []string
	for name, state := range s.state {
		if state.invalidReason == "" && !state.dueAt.After(cutoff) {
			due = append(due, name)
		}
	}
	slices.Sort(due)
	return due
}

func (s *scheduler) reconcileOne(ctx context.Context, state *clusterState) {
	passCtx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	// When the pass started, not when it finished. Scheduling from the end would add
	// the pass's own duration to every interval, and since that duration always
	// carries dueAt past the poll tick that would have caught it, each interval
	// silently became one whole tick longer than the one configured.
	startedAt := s.now()

	status, err := s.rec.Cluster(passCtx, state.cluster, state.status, state.generation)
	state.status = status

	if writeErr := s.inv.UpdateStatus(ctx, state.cluster.Name, status); writeErr != nil {
		// Losing the status write is not fatal, but it does mean the next pass
		// reissues: the fingerprint it would have compared against is gone.
		s.logger.Warn("writing status failed; the next pass will reissue",
			"cluster", state.cluster.Name, "error", writeErr)
	}

	if err != nil {
		// Backoff is measured from the end of a failed pass on purpose: a pass that
		// fails slowly, on a timeout say, should not be retried the instant it
		// returns.
		state.dueAt = s.now().Add(state.backoff)
		s.logger.Info("retrying later", "cluster", state.cluster.Name,
			"retry_in", state.backoff.Round(time.Second).String())
		state.backoff = min(state.backoff*2, maxRetryInterval)
		return
	}

	state.backoff = retryInterval
	state.dueAt = dueAfterPass(startedAt, status)
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

// nextDueIn is how long until the earliest scheduled reconciliation, for the
// liveness probe.
func (s *scheduler) nextDueIn() time.Duration {
	now := s.now()
	next := maxPassInterval
	for _, state := range s.state {
		if state.invalidReason != "" {
			continue
		}
		if d := state.dueAt.Sub(now); d < next {
			next = d
		}
	}
	return max(next, 0)
}

// report renders the current state for /status.
func (s *scheduler) report() []clusterReport {
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
		if state.invalidReason != "" {
			report.Error = state.invalidReason
		} else if cond := meta.FindStatusCondition(state.status.Conditions, v1alpha1.ConditionReady); cond != nil {
			report.Synced = cond.Status == metav1.ConditionTrue
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
