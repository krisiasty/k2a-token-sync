package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"
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
	ServingCertExpiresAt     time.Time `json:"servingCertExpiresAt,omitzero"`
	ServingCertDaysRemaining int32     `json:"servingCertDaysRemaining,omitempty"`
}

// healthState tracks the scheduler for the probe endpoints.
type healthState struct {
	mu            sync.RWMutex
	inProgress    bool
	nextAttemptAt time.Time
	clusters      []clusterReport
	lastSuccessAt time.Time
	ready         bool
}

func newHealthState() *healthState {
	return &healthState{}
}

func (s *healthState) recordAttempt() {
	s.mu.Lock()
	s.inProgress = true
	s.mu.Unlock()
}

func (s *healthState) record(clusters []clusterReport, nextDueIn time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusters = clusters
	s.inProgress = false
	s.nextAttemptAt = time.Now().Add(min(nextDueIn, pollInterval))

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
		s.lastSuccessAt = time.Now()
	}
}

// isLive reports whether the scheduler is still ticking. The grace window
// absorbs one slow cluster without tripping the liveness probe.
func (s *healthState) isLive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.inProgress || s.nextAttemptAt.IsZero() {
		return true // startup, or a pass is in progress
	}
	return time.Now().Before(s.nextAttemptAt.Add(clusterTimeout + pollInterval))
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
	Clusters      []clusterReport `json:"clusters"`
}

func (s *healthState) report() statusReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return statusReport{
		Ready:         s.ready,
		LastSuccessAt: s.lastSuccessAt,
		NextAttemptAt: s.nextAttemptAt,
		Clusters:      s.clusters,
	}
}

func runHealthServer(ctx context.Context, logger *slog.Logger, port string, state *healthState, cancel context.CancelFunc) {
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
