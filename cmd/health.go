package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/krisiasty/r2a-cert-sync/internal/reconcile"
)

// healthState tracks the reconciliation loop for the probe endpoints.
type healthState struct {
	mu            sync.RWMutex
	clusterCount  int
	nextAttemptAt time.Time
	lastResult    reconcile.Result
	lastSuccessAt time.Time
	ready         bool
}

func newHealthState(clusterCount int) *healthState {
	return &healthState{clusterCount: clusterCount}
}

func (s *healthState) recordAttempt() {
	s.mu.Lock()
	s.nextAttemptAt = time.Time{} // clear while a pass is in progress
	s.mu.Unlock()
}

func (s *healthState) record(result reconcile.Result, next time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastResult = result
	s.nextAttemptAt = time.Now().Add(next)

	// Readiness means every configured cluster is registered. A partial pass
	// leaves the pod unready so the condition is visible, but the loop keeps
	// running — the remaining clusters are retried on backoff.
	if result.AllSynced() && len(result.Clusters) == s.clusterCount {
		s.ready = true
		s.lastSuccessAt = time.Now()
	}
}

// isLive reports whether the loop is running on schedule. The grace window
// absorbs a long Rancher rotation without tripping the liveness probe.
func (s *healthState) isLive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nextAttemptAt.IsZero() {
		return true // startup, or a pass is in progress
	}
	return time.Now().Before(s.nextAttemptAt.Add(passTimeout + maxRetryInterval))
}

func (s *healthState) isReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

// statusReport is the payload of /status.
type statusReport struct {
	Ready         bool                      `json:"ready"`
	LastSuccessAt time.Time                 `json:"lastSuccessAt,omitzero"`
	NextAttemptAt time.Time                 `json:"nextAttemptAt,omitzero"`
	Clusters      []reconcile.ClusterStatus `json:"clusters"`
}

func (s *healthState) report() statusReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return statusReport{
		Ready:         s.ready,
		LastSuccessAt: s.lastSuccessAt,
		NextAttemptAt: s.nextAttemptAt,
		Clusters:      s.lastResult.Clusters,
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

	go func() { //nolint:gosec // a fresh context is required once ctx is cancelled
		<-ctx.Done()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("health server error", "error", err)
		cancel() // exit the main loop so the pod restarts
	}
}
