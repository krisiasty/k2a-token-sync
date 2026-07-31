package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/krisiasty/r2a-cert-sync/internal/config"
	kubeclient "github.com/krisiasty/r2a-cert-sync/internal/k8s"
	"github.com/krisiasty/r2a-cert-sync/internal/reconcile"
)

const bootstrapTimeout = 2 * time.Minute

// runBootstrap provisions a standalone RKE2 cluster for the daemon.
//
// It runs on an operator's workstation, where a working kubeconfig for both the
// downstream cluster and the ArgoCD cluster already exists. That existing access
// is used once to install the daemon's own narrowly-scoped identity downstream
// and store a durable credential for it next to the daemon — so no
// administrative credential has to be transferred or kept anywhere.
//
// This exists because standalone RKE2 offers no equivalent of Rancher's
// pre-privileged cluster agent: something has to establish the first foothold.
func runBootstrap(logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		clusterName    = fs.String("cluster", "", "name for the cluster; must match the 'name' of an entry in the daemon's config")
		endpoint       = fs.String("endpoint", "", "direct API endpoint ArgoCD will connect to, as host or host:port")
		downstreamCtx  = fs.String("context", "", "kubeconfig context of the downstream RKE2 cluster (required)")
		downstreamKube = fs.String("kubeconfig", "", "path to the kubeconfig holding --context (default: normal kubeconfig resolution)")
		localCtx       = fs.String("argocd-context", "", "kubeconfig context of the cluster running ArgoCD and this daemon (default: current context)")
		localKube      = fs.String("argocd-kubeconfig", "", "path to the kubeconfig holding --argocd-context")
		namespace      = fs.String("namespace", "r2a-cert-sync", "namespace the daemon runs in; the credential secret is written here")
		saName         = fs.String("serviceaccount", "argocd-manager", "downstream ServiceAccount ArgoCD authenticates as")
		saNamespace    = fs.String("serviceaccount-namespace", "kube-system", "namespace for the downstream ServiceAccounts")
		agentSAName    = fs.String("agent-serviceaccount", "r2a-cert-sync", "downstream ServiceAccount the daemon authenticates as")
		dryRun         = fs.Bool("dry-run", false, "report what would be done without changing anything")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: r2a-cert-sync bootstrap --cluster NAME --endpoint HOST[:PORT] --context CTX [flags]

Provisions a standalone RKE2 cluster so the daemon can maintain its ArgoCD
registration without Rancher. Installs two downstream identities — one for
ArgoCD (cluster-admin) and one narrowly-scoped for the daemon — then writes a
durable credential for the latter into the daemon's namespace.

Clusters managed by Rancher do not need this: the daemon bootstraps them through
the Rancher API proxy.

Flags:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	switch {
	case *clusterName == "":
		fs.Usage()
		return errors.New("--cluster is required")
	case *endpoint == "":
		fs.Usage()
		return errors.New("--endpoint is required")
	case *downstreamCtx == "":
		fs.Usage()
		return errors.New("--context is required")
	}

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:                    *clusterName,
		Endpoint:                *endpoint,
		ServiceAccountName:      *saName,
		ServiceAccountNamespace: *saNamespace,
		AgentServiceAccountName: *agentSAName,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), bootstrapTimeout)
	defer cancel()

	downstreamClient, downstreamCfg, err := kubeclient.ClientForContext(*downstreamKube, *downstreamCtx)
	if err != nil {
		return err
	}

	logger = logger.With("cluster", cluster.Name)
	logger.Info("connected to downstream cluster",
		"context", *downstreamCtx,
		"server", downstreamCfg.Host,
		"argocd_endpoint", cluster.ServerURL(),
	)

	if *dryRun {
		logger.Info("dry run; no changes made",
			"would_create_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			"would_create_agent_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.AgentServiceAccountName,
			"would_write_secret", *namespace+"/"+cluster.CredentialsSecretName(),
		)
		return nil
	}

	creds, err := reconcile.Provision(ctx, downstreamClient, cluster)
	if err != nil {
		return fmt.Errorf("provisioning downstream cluster: %w", err)
	}
	logger.Info("provisioned downstream identities",
		"argocd_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
		"agent_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.AgentServiceAccountName,
	)

	localClient, err := localClientFor(*localKube, *localCtx)
	if err != nil {
		return err
	}

	if err := kubeclient.WriteCredentials(ctx, localClient, *namespace, cluster.CredentialsSecretName(), creds,
		map[string]string{
			"app.kubernetes.io/managed-by": "r2a-cert-sync",
			"r2a-cert-sync.io/cluster":     cluster.Name,
		}); err != nil {
		return fmt.Errorf("writing credential secret: %w", err)
	}

	logger.Info("bootstrap complete",
		"credentials_secret", *namespace+"/"+cluster.CredentialsSecretName(),
	)

	fmt.Fprintf(os.Stderr, `
Cluster %q is ready. Append it to the 'clusters' list in your Helm values and
run 'helm upgrade':

  - name: %s
    provider: direct
    endpoint: %s
    secretName: %s

Do not edit the ConfigMap directly. The chart renders both the ConfigMap and
the Role in the ArgoCD namespace from that one list, and the Role scopes access
to the cluster Secrets it names. A ConfigMap-only edit leaves the Secret above
out of the Role, so reconciliation fails with "secrets ... is forbidden", and
the edit is reverted by the next upgrade.

No bootstrapSecret is needed — the durable credential is already stored in
%s/%s.
`, cluster.Name, cluster.Name, cluster.Endpoint, cluster.SecretName,
		*namespace, cluster.CredentialsSecretName())

	return nil
}

// localClientFor builds a client for the cluster running ArgoCD and the daemon.
// With no explicit context it falls back to normal kubeconfig resolution, which
// also covers running this inside that cluster.
func localClientFor(kubeconfig, contextName string) (kubernetes.Interface, error) {
	if kubeconfig == "" && contextName == "" {
		return kubeclient.NewClient()
	}
	client, _, err := kubeclient.ClientForContext(kubeconfig, contextName)
	return client, err
}
