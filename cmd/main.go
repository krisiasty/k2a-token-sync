// Command k2a-token-sync keeps ArgoCD's registrations for downstream Kubernetes
// clusters valid, using short-lived tokens it mints and rotates itself.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/krisiasty/k2a-token-sync/internal/config"
	kubeclient "github.com/krisiasty/k2a-token-sync/internal/k8s"
	"github.com/krisiasty/k2a-token-sync/internal/reconcile"
)

const (
	// retryInterval is how soon a failed pass is retried, before backoff.
	retryInterval = 1 * time.Minute

	// maxRetryInterval caps the exponential backoff.
	maxRetryInterval = 30 * time.Minute

	// minPassInterval floors the derived pass interval, so an aggressively
	// capped token lifetime cannot turn the loop into a busy wait.
	minPassInterval = 1 * time.Minute

	// passTimeout bounds one reconciliation pass over all clusters. Generous,
	// because a pass over many clusters legitimately takes several minutes.
	passTimeout = 45 * time.Minute
)

func main() {
	logger := newLogger()

	if len(os.Args) > 1 {
		if err := runSubcommand(logger, os.Args[1], os.Args[2:]); err != nil {
			logger.Error("command failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := runDaemon(logger); err != nil {
		logger.Error("daemon failed", "error", err)
		os.Exit(1)
	}
}

// nextPassInterval decides how long to sleep after a clean pass.
//
// The configured refreshInterval is an upper bound, not the whole story: a
// downstream API server may cap token lifetime via
// --service-account-max-token-expiration, so a credential can be far shorter
// lived than requested. Sleeping the full interval would then leave ArgoCD
// holding an expired token for most of the gap. Waking at half the shortest
// remaining lifetime keeps a margin of one whole refresh, and the floor stops a
// pathologically short cap from turning into a busy loop.
func nextPassInterval(refreshInterval time.Duration, soonestExpiry, now time.Time) time.Duration {
	if soonestExpiry.IsZero() {
		return refreshInterval
	}
	derived := soonestExpiry.Sub(now) / 2
	return max(min(refreshInterval, derived), minPassInterval)
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey && attr.Value.Kind() == slog.KindTime {
				return slog.String(attr.Key, attr.Value.Time().UTC().Format("2006-01-02T15:04:05.000Z07:00"))
			}
			return attr
		},
	}))
}

func runSubcommand(logger *slog.Logger, name string, args []string) error {
	switch name {
	case "bootstrap":
		return runBootstrap(logger, args)
	case "version":
		fmt.Println(versionString())
		return nil
	case "-h", "--help", "help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown command %q", name)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `k2a-token-sync — keeps ArgoCD's downstream cluster registrations valid.

Usage:
  k2a-token-sync                 run the reconciliation daemon (default)
  k2a-token-sync bootstrap ...   provision a standalone cluster for the daemon
  k2a-token-sync version         print version information

Run 'k2a-token-sync bootstrap --help' for bootstrap options.
`)
}

func runDaemon(logger *slog.Logger) error {
	cfg, err := config.Load(logger)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	local, err := kubeclient.NewClient()
	if err != nil {
		return fmt.Errorf("building kubernetes client: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	reconciler := reconcile.New(cfg, local, logger)

	state := newHealthState(len(cfg.Clusters))

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		runHealthServer(ctx, logger, cfg.HealthPort, state, stop)
	})

	logger.Info("starting k2a-token-sync",
		"version", versionString(),
		"namespace", cfg.Namespace,
		"clusters", len(cfg.Clusters),
		"refresh_interval", cfg.RefreshInterval.String(),
	)

	backoff := retryInterval

	for {
		state.recordAttempt()

		passCtx, cancel := context.WithTimeout(ctx, passTimeout)
		result := reconciler.Run(passCtx)
		cancel()

		if ctx.Err() != nil {
			logger.Info("shutting down")
			return nil
		}

		var next time.Duration
		if failures := result.Failures(); failures > 0 {
			next = backoff
			backoff = min(backoff*2, maxRetryInterval)
			state.record(result, next)
			logger.Error("reconciliation pass had failures",
				"failed", failures,
				"total", len(result.Clusters),
				"retry_in", next.Round(time.Second).String(),
			)
		} else {
			backoff = retryInterval
			next = nextPassInterval(cfg.RefreshInterval, result.SoonestTokenExpiry(), time.Now())
			state.record(result, next)
			logger.Info("reconciliation pass complete",
				"clusters", len(result.Clusters),
				"next_pass_in", next.Round(time.Second).String(),
			)
		}

		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			logger.Info("shutting down")
			return nil
		case <-timer.C:
		}
	}
}
