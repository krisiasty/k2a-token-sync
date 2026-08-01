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

// runBootstrap provisions a downstream cluster for k2a-token-sync.
//
// It exists because something has to establish the first foothold: k2a-token-sync
// has no way into a cluster until an identity exists for it there, and it
// deliberately never holds administrative material of its own.
//
// The split of outputs is the point. The credential goes straight from the
// downstream cluster into its own namespace, never through the terminal; the
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
		fromContext = fs.String("from-context", "",
			"context to use within --from-kubeconfig, or within the ambient kubeconfig when that is unset")

		// The unprefixed pair means what it means in any kubectl-like tool: the
		// cluster this command works against. For bootstrap that is where the
		// output lands — the credential and the ClusterConnection.
		kubeconfig = fs.String("kubeconfig", "",
			"path to the kubeconfig for the cluster running ArgoCD and k2a-token-sync (default: normal resolution)")
		kubeContext = fs.String("context", "",
			"context of the cluster running ArgoCD and k2a-token-sync (default: current context)")

		namespace   = fs.String("namespace", "k2a-token-sync", "namespace k2a-token-sync runs in; the credential is written here")
		saName      = fs.String("serviceaccount", "argocd-manager", "downstream ServiceAccount ArgoCD authenticates as")
		saNamespace = fs.String("serviceaccount-namespace", "kube-system", "namespace for the downstream ServiceAccounts")
		selfSAName  = fs.String("self-serviceaccount", "k2a-token-sync", "downstream ServiceAccount k2a-token-sync authenticates as")

		create = fs.Bool("create", false, "apply the ClusterConnection instead of printing it")
		dryRun = fs.Bool("dry-run", false, "report what would be done without changing anything")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: k2a-token-sync bootstrap --cluster NAME --endpoint HOST[:PORT] [flags]

Provisions a downstream cluster so k2a-token-sync can maintain its ArgoCD
registration. Installs two downstream identities — one for ArgoCD (cluster-admin)
and one narrowly-scoped for k2a-token-sync — stores a credential for the latter in
its namespace, and prints the ClusterConnection to apply.

Run this once per cluster. Afterwards k2a-token-sync needs no administrative
access.

Two clusters are involved. --kubeconfig and --context select the one running
ArgoCD and k2a-token-sync, where the credential is stored and the object created;
both default to your usual kubeconfig and current context. --from-kubeconfig and
--from-context select the downstream cluster being onboarded, and one of them is
required. Separate files are supported deliberately: merging kubeconfigs is unsafe
when they define the same context name for different clusters.

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
	case *fromKubeconfig == "" && *fromContext == "":
		fs.Usage()
		return errors.New("one of --from-kubeconfig or --from-context is required, to reach the downstream cluster")
	}

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:                    *clusterName,
		Endpoint:                *endpoint,
		ServiceAccountName:      *saName,
		ServiceAccountNamespace: *saNamespace,
		SelfServiceAccountName:  *selfSAName,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), bootstrapTimeout)
	defer cancel()

	// Both connections are resolved before anything is created. Provisioning a
	// cluster and then discovering there is nowhere to put the result would leave
	// identities behind with no credential stored for them.
	downstreamClient, downstreamCfg, err := kubeclient.ClientForContext(*fromKubeconfig, *fromContext)
	if err != nil {
		return err
	}
	localClient, err := localClientFor(*kubeconfig, *kubeContext)
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
			"would_create_self_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName,
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
		"self_serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName,
		"self_credential_expires_at", provisioned.ExpiresAt.UTC().Format(time.RFC3339),
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
	// cluster k2a-token-sync runs on but not from here.
	verifyCredential(ctx, cluster, provisioned, logger)

	if *create {
		return createConnection(ctx, cluster, *namespace, *kubeconfig, *kubeContext, logger)
	}

	fmt.Fprintf(os.Stderr, "\nApply the ClusterConnection below to register %q. "+
		"k2a-token-sync picks it up within one poll.\n\n", cluster.Name)
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

// verifyCredential uses the stored credential exactly as k2a-token-sync will, to
// prove endpoint, certificate, token and downstream RBAC work together.
func verifyCredential(ctx context.Context, cluster config.Cluster, creds *kubeclient.Credentials, logger *slog.Logger) {
	client, err := reconcile.ClientFromCredentials(cluster.ServerURL(), creds)
	if err != nil {
		logger.Warn("could not build a client for the stored credential", "error", err)
		return
	}
	if _, err := downstream.ClusterCA(ctx, client, cluster.ServiceAccount.Namespace); err != nil {
		logger.Warn("could not verify the credential against the endpoint from here; "+
			"k2a-token-sync may still succeed if it can reach the endpoint",
			"endpoint", cluster.Endpoint, "error", err)
		return
	}
	logger.Info("verified the credential against the endpoint ArgoCD will use", "endpoint", cluster.Endpoint)
}

// connectionFor builds the object that declares this cluster to k2a-token-sync.
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
// Only apiVersion, kind, metadata and spec are printed: status belongs to
// k2a-token-sync, and a "status: {}" stanza in a committed file is noise at best
// and an invitation to edit it at worst.
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
// pipe YAML around. It uses the dynamic client for the same reason k2a-token-sync
// does: no generated clientset is needed to write one object.
func createConnection(
	ctx context.Context,
	cluster config.Cluster,
	namespace, kubeconfig, kubeContext string,
	logger *slog.Logger,
) error {
	dyn, err := kubeclient.DynamicClientForContext(kubeconfig, kubeContext)
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

	logger.Info("applied the ClusterConnection; k2a-token-sync picks it up within one poll",
		"object", namespace+"/"+cluster.Name)
	return nil
}

// localClientFor builds a client for the cluster running ArgoCD and k2a-token-sync.
// With no explicit context it falls back to normal kubeconfig resolution, which
// also covers running this inside that cluster.
func localClientFor(kubeconfig, contextName string) (kubernetes.Interface, error) {
	if kubeconfig == "" && contextName == "" {
		return kubeclient.NewClient()
	}
	client, _, err := kubeclient.ClientForContext(kubeconfig, contextName)
	return client, err
}
