package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

// clusterReport is one cluster's line in /status. It carries no credential
// material — only what was observed and when.
type clusterReport struct {
	Name       string `json:"name"`
	Endpoint   string `json:"endpoint"`
	Secret     string `json:"secret,omitempty"`
	Synced     bool   `json:"synced"`
	Error      string `json:"error,omitempty"`
	LastAction string `json:"lastAction,omitempty"`

	SyncedAt                 time.Time `json:"syncedAt,omitzero"`
	DueAt                    time.Time `json:"dueAt,omitzero"`
	TokenExpiresAt           time.Time `json:"tokenExpiresAt,omitzero"`
	SelfCredentialExpiresAt  time.Time `json:"selfCredentialExpiresAt,omitzero"`
	ServingCertExpiresAt     time.Time `json:"servingCertExpiresAt,omitzero"`
	ServingCertDaysRemaining int32     `json:"servingCertDaysRemaining,omitempty"`
}

const (
	// maxProgressAge is how long the loop may go without completing a poll before
	// liveness fails.
	//
	// Ten polls is generous on purpose. The probe exists to catch a process that
	// has stopped making progress, and restarting is a remedy for exactly one
	// cause: a loop that has wedged. An API server that is briefly unreachable —
	// during a control plane upgrade, say — is not that, and answering it by
	// killing a pod that is behaving correctly would trade a recoverable outage for
	// a restart loop. The window is wide enough to sit through one.
	maxProgressAge = 10 * pollInterval

	// passGrace is how far a pass may overrun clusterTimeout before it counts as
	// wedged rather than slow.
	//
	// Every pass runs under that timeout, so overrunning it means the context was
	// not honoured — a goroutine blocked somewhere that does not take one. Such a
	// pass never ends and never gives its slot back, and enough of them stop the
	// fleet reconciling while the polls carry on completing, which is a stall the
	// poll clock alone cannot see.
	passGrace = 1 * time.Minute
)

// healthState tracks the scheduler for the probe endpoints.
type healthState struct {
	mu            sync.RWMutex
	nextAttemptAt time.Time
	clusters      []clusterReport
	lastSuccessAt time.Time
	ready         bool

	// polledAt is when a poll last completed, and passes holds the start time of
	// every pass currently in flight. Between them they are what liveness is
	// judged on.
	polledAt time.Time
	passes   map[string]time.Time

	// schema is the last verdict on whether the CRD matches this binary. It is
	// process-wide rather than per-cluster: one schema serves every connection,
	// and a stale one discards the same fields on all of them.
	//
	// schemaChecked distinguishes "checked, and current" from "not checked yet",
	// which the zero value cannot. Without it the metric would read 1 from the
	// moment the process started, and a healthy-looking gauge that only means
	// "nothing has run" is worse than no gauge at all.
	schema        inventory.SchemaCheck
	schemaChecked bool

	now func() time.Time
}

// recordSchema stores the latest schema verdict for /status and /metrics.
func (s *healthState) recordSchema(check inventory.SchemaCheck) {
	s.mu.Lock()
	s.schema = check
	s.schemaChecked = true
	s.mu.Unlock()
}

// schemaCheck returns the last verdict, and whether one has been reached at all.
func (s *healthState) schemaCheck() (inventory.SchemaCheck, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schema, s.schemaChecked
}

func newHealthState() *healthState {
	return &healthState{
		polledAt: time.Now(), // the process has just started; that counts as progress
		passes:   make(map[string]time.Time),
		now:      time.Now,
	}
}

// recordPoll marks a completed poll. Only a poll that got as far as reading the
// inventory counts: one that could not is the case liveness is meant to notice.
func (s *healthState) recordPoll() {
	s.mu.Lock()
	s.polledAt = s.now()
	s.mu.Unlock()
}

func (s *healthState) passStarted(cluster string, startedAt time.Time) {
	s.mu.Lock()
	s.passes[cluster] = startedAt
	s.mu.Unlock()
}

func (s *healthState) passFinished(cluster string) {
	s.mu.Lock()
	delete(s.passes, cluster)
	s.mu.Unlock()
}

func (s *healthState) record(clusters []clusterReport, nextDueIn time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusters = clusters
	s.nextAttemptAt = s.now().Add(min(nextDueIn, pollInterval))

	// Readiness means every cluster in the inventory is registered. An unready
	// pod makes a partial failure visible without stopping the loop: the
	// remaining clusters keep their own backoff.
	ready := true
	for _, c := range clusters {
		if !c.Synced {
			ready = false
			break
		}
	}
	s.ready = ready
	if ready {
		s.lastSuccessAt = s.now()
	}
}

// isLive reports whether the loop is still making progress.
//
// Two things can stop it, and neither used to be visible here. A poll that never
// returns — an inventory call left hanging by a connection that is accepted and
// then ignored — used to be read as a pass in progress, which excused liveness
// indefinitely: the probe reported health for exactly as long as the loop did
// nothing at all. And a pass that ignores its deadline holds its slot for good,
// so enough of them stop the fleet reconciling while the polls carry on
// completing on time.
func (s *healthState) isLive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	if now.Sub(s.polledAt) > maxProgressAge {
		return false
	}
	for _, startedAt := range s.passes {
		if now.Sub(startedAt) > clusterTimeout+passGrace {
			return false
		}
	}
	return true
}

func (s *healthState) isReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

// statusReport is the payload of /status.
type statusReport struct {
	Ready         bool            `json:"ready"`
	LastSuccessAt time.Time       `json:"lastSuccessAt,omitzero"`
	NextAttemptAt time.Time       `json:"nextAttemptAt,omitzero"`
	Schema        schemaReport    `json:"schema"`
	Clusters      []clusterReport `json:"clusters"`
}

// schemaReport says whether the CRD matches this binary.
//
// Always present rather than omitted when healthy: "the schema is current" is
// an answer somebody may be looking for, and a field that appears only when
// something is wrong cannot be used to confirm that nothing is.
type schemaReport struct {
	Current bool `json:"current"`

	// MissingFields names what the API server would discard, so /status says
	// which settings are being ignored rather than only that some are.
	MissingFields []string `json:"missingFields,omitempty"`

	// Unverified is set when the CRD could not be read — distinct from stale,
	// and usually meaning the chart's ClusterRole has not been applied yet.
	Unverified string `json:"unverified,omitempty"`
}

func (s *healthState) report() statusReport {
	s.mu.RLock()
	defer s.mu.RUnlock()

	schema := schemaReport{Current: s.schemaChecked && !s.schema.Stale()}
	if s.schema.Stale() {
		schema.MissingFields = append(append([]string{}, s.schema.MissingSpecFields...), s.schema.MissingStatusFields...)
	}
	if s.schema.Unverifiable != nil {
		schema.Unverified = s.schema.Unverifiable.Error()
	}

	return statusReport{
		Ready:         s.ready,
		LastSuccessAt: s.lastSuccessAt,
		NextAttemptAt: s.nextAttemptAt,
		Schema:        schema,
		Clusters:      s.clusters,
	}
}

func newHealthHandler(logger *slog.Logger, state *healthState, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		if state.isLive() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "reconciliation loop stalled", http.StatusServiceUnavailable)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if state.isReady() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "waiting for every cluster to reconcile", http.StatusServiceUnavailable)
	})

	// /status exposes per-cluster detail, including observed certificate expiry.
	// It carries no credential material.
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(state.report()); err != nil {
			logger.Warn("writing status response failed", "error", err)
		}
	})

	mux.Handle("/metrics", metricsHandler)
	return mux
}

func runHealthServer(
	ctx context.Context,
	logger *slog.Logger,
	port string,
	state *healthState,
	metricsHandler http.Handler,
	cancel context.CancelFunc,
) {
	mux := newHealthHandler(logger, state, metricsHandler)

	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// The shutdown context is deliberately not derived from ctx: ctx is already
	// cancelled by the time this runs, so an inherited context would abort the
	// graceful shutdown immediately instead of letting in-flight probes finish.
	go func() { //nolint:gosec // detached shutdown context is intended, see above
		<-ctx.Done()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx) //nolint:contextcheck // detached by design, see above
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("health server error", "error", err)
		cancel() // exit the main loop so the pod restarts
	}
}
