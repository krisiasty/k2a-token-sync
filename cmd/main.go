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

	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/events"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
	kubeclient "github.com/krisiasty/k2a-token-sync/internal/k8s"
	"github.com/krisiasty/k2a-token-sync/internal/reconcile"
)

func main() {
	// Subcommands are run by a person and report in plain text on stderr, keeping
	// stdout for their output — bootstrap writes a manifest there, and log lines
	// mixed into it would break a redirect or a pipe into kubectl.
	if len(os.Args) > 1 {
		if err := runSubcommand(os.Args[1], os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// The reconciliation loop has no such output, and its logs are read by a
	// collector rather than a person, so they stay structured and on stdout.
	logger := newLogger()
	if err := runSync(logger); err != nil {
		logger.Error("k2a-token-sync failed", "error", err)
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

func runSubcommand(name string, args []string) error {
	switch name {
	case "bootstrap":
		return runBootstrap(args)
	case "restrict-rbac":
		return runRestrictRBAC(args)
	case "remove":
		return runRemove(args)
	case "licenses":
		return writeThirdPartyNotices(os.Stdout)
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
  k2a-token-sync                 run the reconciliation loop (default)
  k2a-token-sync bootstrap ...   provision a standalone cluster for k2a-token-sync
  k2a-token-sync restrict-rbac   restrict patch access to configured ArgoCD cluster Secrets
  k2a-token-sync remove ...      retire a cluster: delete its registration, credential and identities
  k2a-token-sync licenses        print third-party license notices
  k2a-token-sync version         print version information

Run 'k2a-token-sync <command> --help' for command options.
`)
}

func runSync(logger *slog.Logger) error {
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

	dyn, err := kubeclient.NewDynamicClient()
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	state := newHealthState()
	telemetry := newRuntimeTelemetry(logger)
	metricsHandler := newMetricsHandler(state)

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		telemetry.run(ctx)
	})
	wg.Go(func() {
		runHealthServer(ctx, logger, cfg.HealthPort, state, metricsHandler, stop)
	})

	logger.Info("starting k2a-token-sync",
		"version", versionString(),
		"namespace", cfg.Namespace,
		"argocd_namespace", cfg.ArgoCDNamespace,
		"poll_interval", pollInterval.String(),
		"telemetry_sample_interval", telemetrySampleInterval.String(),
		"telemetry_report_interval", telemetryReportInterval.String(),
	)

	// One recorder for both writers. The poll records what it decided about an
	// object and a pass records what it did with one, and they are the same handful
	// of Events on the same objects.
	inv := inventory.NewClient(dyn, cfg.Namespace)
	recorder := events.New(local, cfg.Namespace, inv, logger)

	scheduler := newScheduler(
		inv,
		reconcile.New(cfg, local, recorder, logger),
		recorder,
		logger,
		state,
	)
	// Supplied here rather than taken as a constructor argument so that every
	// existing test building a scheduler keeps working unchanged, and so a test
	// that wants to drive the check can substitute one.
	scheduler.schema = func(ctx context.Context) inventory.SchemaCheck {
		return inventory.CheckSchema(ctx, dyn)
	}
	scheduler.run(ctx)

	return nil
}
