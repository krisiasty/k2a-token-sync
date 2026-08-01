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
	// way round — and a list doubles as the recovery path, since whatever the
	// daemon believed, the next poll replaces it.
	pollInterval = 30 * time.Second

	// maxPassInterval caps how long a healthy cluster goes unreconciled, so a
	// long-lived token still gets its serving certificate checked daily.
	maxPassInterval = 24 * time.Hour

	// minPassInterval floors the derived interval, so an aggressively capped
	// token lifetime cannot turn the loop into a busy wait.
	minPassInterval = 1 * time.Minute

	// retryInterval is how soon a failed cluster is retried, before backoff.
	retryInterval = 1 * time.Minute

	// maxRetryInterval caps the per-cluster exponential backoff.
	maxRetryInterval = 30 * time.Minute

	// clusterTimeout bounds one cluster's reconciliation.
	clusterTimeout = 5 * time.Minute
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
		// an edit made while the daemon was down is still noticed.
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
			// The generated Secret is deliberately left behind: the daemon holds
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
	now := s.now()
	var due []string
	for name, state := range s.state {
		if state.invalidReason == "" && !state.dueAt.After(now) {
			due = append(due, name)
		}
	}
	slices.Sort(due)
	return due
}

func (s *scheduler) reconcileOne(ctx context.Context, state *clusterState) {
	passCtx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	status, err := s.rec.Cluster(passCtx, state.cluster, state.status, state.generation)
	state.status = status

	if writeErr := s.inv.UpdateStatus(ctx, state.cluster.Name, status); writeErr != nil {
		// Losing the status write is not fatal, but it does mean the next pass
		// reissues: the fingerprint it would have compared against is gone.
		s.logger.Warn("writing status failed; the next pass will reissue",
			"cluster", state.cluster.Name, "error", writeErr)
	}

	now := s.now()
	if err != nil {
		state.dueAt = now.Add(state.backoff)
		s.logger.Info("retrying later", "cluster", state.cluster.Name,
			"retry_in", state.backoff.Round(time.Second).String())
		state.backoff = min(state.backoff*2, maxRetryInterval)
		return
	}

	state.backoff = retryInterval
	state.dueAt = now.Add(nextInterval(status, now))
}

// nextInterval decides when a healthy cluster is next due.
//
// The configured lifetime is an upper bound, not the whole story: an API server
// may cap token lifetime via --service-account-max-token-expiration, so a
// credential can be far shorter lived than requested. Waking at half the
// remaining lifetime keeps a margin of one whole refresh; the daily cap means a
// long-lived token still gets its certificate checked.
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
