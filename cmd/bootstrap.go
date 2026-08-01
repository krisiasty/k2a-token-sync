package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/downstream"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
	kubeclient "github.com/krisiasty/k2a-token-sync/internal/k8s"
	"github.com/krisiasty/k2a-token-sync/internal/reconcile"
)

const bootstrapTimeout = 2 * time.Minute

// runBootstrap provisions a downstream cluster for the daemon.
//
// It exists because something has to establish the first foothold: the daemon has
// no way into a cluster until an identity exists for it there, and it deliberately
// never holds administrative material of its own.
//
// The split of outputs is the point. The credential goes straight from the
// downstream cluster into the daemon's namespace, never through the terminal; the
// ClusterConnection goes to stdout, where it carries nothing secret and can be
// committed. So the operator's shell sees only what is safe to keep.
func runBootstrap(logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		clusterName = fs.String("cluster", "", "name for the cluster, and for the ClusterConnection object (required)")
		endpoint    = fs.String("endpoint", "", "the address ArgoCD will connect to, as host or host:port (required)")

		fromKubeconfig = fs.String("from-kubeconfig", "",
			"path to a kubeconfig granting admin access to the downstream cluster")
		downstreamCtx = fs.String("context", "",
			"context to use within --from-kubeconfig, or within the ambient kubeconfig when that is unset")

		homeKube = fs.String("home-kubeconfig", "",
			"path to the kubeconfig for the cluster running ArgoCD and the daemon (default: normal resolution)")
		homeCtx = fs.String("home-context", "",
			"context of the cluster running ArgoCD and the daemon (default: current context)")

		namespace   = fs.String("namespace", "k2a-token-sync", "namespace the daemon runs in; the credential is written here")
		saName      = fs.String("serviceaccount", "argocd-manager", "downstream ServiceAccount ArgoCD authenticates as")
		saNamespace = fs.String("serviceaccount-namespace", "kube-system", "namespace for the downstream ServiceAccounts")
		agentSAName = fs.String("agent-serviceaccount", "k2a-token-sync", "downstream ServiceAccount the daemon authenticates as")

		create = fs.Bool("create", false, "apply the ClusterConnection instead of printing it")
		dryRun = fs.Bool("dry-run", false, "report what would be done without changing anything")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: k2a-token-sync bootstrap --cluster NAME --endpoint HOST[:PORT] [flags]

Provisions a downstream cluster so the daemon can maintain its ArgoCD
registration. Installs two downstream identities — one for ArgoCD (cluster-admin)
and one narrowly-scoped for the daemon — stores a credential for the latter in the
daemon's namespace, and prints the ClusterConnection to apply.

Run this once per cluster. Afterwards the daemon needs no administrative access.

Downstream access comes from --from-kubeconfig, a --context within the ambient
kubeconfig, or both. Two files are supported deliberately: merging kubeconfigs is
unsafe when they define the same context name for different clusters.

The credential never passes through your shell. Only the ClusterConnection is
printed, and it contains nothing secret:

  k2a-token-sync bootstrap --cluster prod-1 --endpoint prod-1.example.com:6443 \
    --from-kubeconfig ./prod-1.kubeconfig > clusters/prod-1.yaml

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
	case *fromKubeconfig == "" && *downstreamCtx == "":
		fs.Usage()
		return errors.New("one of --from-kubeconfig or --context is required, to reach the downstream cluster")
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

	// Both connections are resolved before anything is created. Provisioning a
	// cluster and then discovering there is nowhere to put the result would leave
	// identities behind with no credential stored for them.
	downstreamClient, downstreamCfg, err := kubeclient.ClientForContext(*fromKubeconfig, *downstreamCtx)
	if err != nil {
		return err
	}
	localClient, err := localClientFor(*homeKube, *homeCtx)
	if err != nil {
		return err
	}

	logger = logger.With("cluster", cluster.Name)
	logger.Info("connected to the downstream cluster",
		"server", downstreamCfg.Host,
		"argocd_endpoint", cluster.ServerURL(),
	)

	if *dryRun {
		logger.Info("dry run; no changes made",
			"would_create_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			"would_create_agent_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.AgentServiceAccountName,
			"would_write_secret", *namespace+"/"+cluster.CredentialsSecretName(),
		)
		return printConnection(cluster, *namespace)
	}

	// Pre-flight before provisioning. A certificate that does not cover the
	// endpoint is the most common reason direct access fails, and it is far
	// cheaper to learn now than after two identities exist downstream.
	if err := preflight(ctx, downstreamClient, cluster, logger); err != nil {
		return err
	}

	provisioned, err := reconcile.Provision(ctx, downstreamClient, cluster)
	if err != nil {
		return fmt.Errorf("provisioning the downstream cluster: %w", err)
	}
	logger.Info("provisioned downstream identities",
		"argocd_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
		"agent_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.AgentServiceAccountName,
		"agent_credential_expires_at", provisioned.ExpiresAt.UTC().Format(time.RFC3339),
	)

	if err := kubeclient.WriteCredentials(ctx, localClient, *namespace, cluster.CredentialsSecretName(), provisioned,
		map[string]string{
			"app.kubernetes.io/managed-by": "k2a-token-sync",
			"k2a-token-sync.io/cluster":    cluster.Name,
		}); err != nil {
		return fmt.Errorf("writing the credential: %w", err)
	}
	logger.Info("stored the credential", "secret", *namespace+"/"+cluster.CredentialsSecretName())

	// Prove the whole path ArgoCD will use, with the credential just minted. This
	// is a warning rather than a failure: the endpoint may be reachable from the
	// daemon's cluster but not from here.
	verifyCredential(ctx, cluster, provisioned, logger)

	if *create {
		return createConnection(ctx, cluster, *namespace, *homeKube, *homeCtx, logger)
	}

	fmt.Fprintf(os.Stderr, "\nApply the ClusterConnection below to register %q. "+
		"The daemon picks it up within one poll.\n\n", cluster.Name)
	return printConnection(cluster, *namespace)
}

// preflight checks that the endpoint ArgoCD will use presents a certificate that
// covers it and verifies against the cluster's own CA.
func preflight(ctx context.Context, client kubernetes.Interface, cluster config.Cluster, logger *slog.Logger) error {
	ca, err := downstream.ClusterCA(ctx, client, cluster.ServiceAccount.Namespace)
	if err != nil {
		return fmt.Errorf("reading the cluster CA: %w", err)
	}

	cert, err := downstream.ProbeServingCert(ctx, cluster.Endpoint, ca)
	if err != nil {
		return fmt.Errorf("probing %s: %w", cluster.Endpoint, err)
	}
	if cert.HostnameError != nil {
		return fmt.Errorf("the endpoint's certificate cannot be used: %w", cert.HostnameError)
	}
	if !cert.TrustedByCA {
		return fmt.Errorf("the certificate presented at %s does not verify against the cluster CA; "+
			"ArgoCD would reject this endpoint", cluster.Endpoint)
	}

	logger.Info("endpoint certificate checked",
		"expires_at", cert.NotAfter.UTC().Format(time.RFC3339),
		"days_remaining", cert.DaysRemaining(),
	)
	return nil
}

// verifyCredential uses the stored credential exactly as the daemon will, to
// prove endpoint, certificate, token and downstream RBAC work together.
func verifyCredential(ctx context.Context, cluster config.Cluster, creds *kubeclient.Credentials, logger *slog.Logger) {
	client, err := reconcile.ClientFromCredentials(cluster.ServerURL(), creds)
	if err != nil {
		logger.Warn("could not build a client for the stored credential", "error", err)
		return
	}
	if _, err := downstream.ClusterCA(ctx, client, cluster.ServiceAccount.Namespace); err != nil {
		logger.Warn("could not verify the credential against the endpoint from here; "+
			"the daemon may still succeed if it can reach the endpoint",
			"endpoint", cluster.Endpoint, "error", err)
		return
	}
	logger.Info("verified the credential against the endpoint ArgoCD will use", "endpoint", cluster.Endpoint)
}

// connectionFor builds the object that declares this cluster to the daemon.
//
// Only the fields the operator chose are set. Everything else is left to the
// schema's defaults, so the printed manifest stays short and does not freeze
// today's defaults into a file that outlives them.
func connectionFor(cluster config.Cluster, namespace string) *v1alpha1.ClusterConnection {
	return &v1alpha1.ClusterConnection{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "k2a-token-sync.io/v1alpha1",
			Kind:       "ClusterConnection",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: namespace,
		},
		Spec: v1alpha1.ClusterConnectionSpec{
			Endpoint:   cluster.Endpoint,
			SecretName: cluster.SecretName,
		},
	}
}

// printConnection writes the object to stdout. Logs go to stderr, so the output
// can be redirected into a file or piped into kubectl.
//
// Only apiVersion, kind, metadata and spec are printed: status belongs to the
// daemon, and a "status: {}" stanza in a committed file is noise at best and an
// invitation to edit it at worst.
func printConnection(cluster config.Cluster, namespace string) error {
	conn := connectionFor(cluster, namespace)
	raw, err := yaml.Marshal(struct {
		metav1.TypeMeta `json:",inline"`
		Metadata        metav1.ObjectMeta              `json:"metadata"`
		Spec            v1alpha1.ClusterConnectionSpec `json:"spec"`
	}{TypeMeta: conn.TypeMeta, Metadata: conn.ObjectMeta, Spec: conn.Spec})
	if err != nil {
		return fmt.Errorf("encoding the ClusterConnection: %w", err)
	}
	if _, err := os.Stdout.Write(raw); err != nil {
		return fmt.Errorf("writing the ClusterConnection: %w", err)
	}
	return nil
}

// createConnection applies the object directly, for people who would rather not
// pipe YAML around. It uses the dynamic client for the same reason the daemon
// does: no generated clientset is needed to write one object.
func createConnection(
	ctx context.Context,
	cluster config.Cluster,
	namespace, homeKube, homeCtx string,
	logger *slog.Logger,
) error {
	dyn, err := kubeclient.DynamicClientForContext(homeKube, homeCtx)
	if err != nil {
		return err
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(connectionFor(cluster, namespace))
	if err != nil {
		return fmt.Errorf("encoding the ClusterConnection: %w", err)
	}

	if _, err := dyn.Resource(inventory.GroupVersionResource).Namespace(namespace).
		Apply(ctx, cluster.Name, &unstructured.Unstructured{Object: obj}, metav1.ApplyOptions{
			FieldManager: "k2a-token-sync-bootstrap",
		}); err != nil {
		return fmt.Errorf("applying the ClusterConnection: %w", err)
	}

	logger.Info("applied the ClusterConnection; the daemon picks it up within one poll",
		"object", namespace+"/"+cluster.Name)
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
