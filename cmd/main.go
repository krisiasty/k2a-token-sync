// Command r2a-cert-sync keeps ArgoCD's registrations for downstream RKE2
// clusters valid, bypassing the Rancher proxy on the GitOps request path.
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

	"github.com/krisiasty/r2a-cert-sync/internal/config"
	kubeclient "github.com/krisiasty/r2a-cert-sync/internal/k8s"
	"github.com/krisiasty/r2a-cert-sync/internal/reconcile"
)

const (
	// retryInterval is how soon a failed pass is retried, before backoff.
	retryInterval = 1 * time.Minute

	// maxRetryInterval caps the exponential backoff.
	maxRetryInterval = 30 * time.Minute

	// passTimeout bounds one reconciliation pass over all clusters. Generous,
	// because a Rancher-orchestrated rotation legitimately takes many minutes.
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
	fmt.Fprint(os.Stderr, `r2a-cert-sync — keeps ArgoCD's downstream RKE2 cluster registrations valid.

Usage:
  r2a-cert-sync                 run the reconciliation daemon (default)
  r2a-cert-sync bootstrap ...   provision a standalone cluster for the daemon
  r2a-cert-sync version         print version information

Run 'r2a-cert-sync bootstrap --help' for bootstrap options.
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

	reconciler, err := reconcile.New(ctx, cfg, local, logger)
	if err != nil {
		return err
	}

	state := newHealthState(len(cfg.Clusters))

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		runHealthServer(ctx, logger, cfg.HealthPort, state, stop)
	})

	logger.Info("starting r2a-cert-sync",
		"version", versionString(),
		"namespace", cfg.Namespace,
		"clusters", len(cfg.Clusters),
		"refresh_interval", cfg.RefreshInterval.String(),
		"rancher", cfg.Rancher != nil,
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
			next = cfg.RefreshInterval
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
