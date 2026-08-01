package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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

// runBootstrap provisions a downstream cluster for k2a-token-sync and registers
// it.
//
// It exists because something has to establish the first foothold: k2a-token-sync
// has no way into a cluster until an identity exists for it there, and it
// deliberately never holds administrative material of its own. That makes this an
// imperative step by nature — it needs a credential no repository should hold —
// and the same is true of `argocd cluster add`.
//
// By default it finishes the job: provisions the identities, stores the
// credential, and applies the ClusterConnection. --print stops short of applying
// and writes the manifest to stdout instead, for anyone keeping those objects in
// git. Progress goes to stderr either way, so a redirect captures only YAML.
func runBootstrap(args []string) error {
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
		// credential is stored and the ClusterConnection created.
		kubeconfig = fs.String("kubeconfig", "",
			"path to the kubeconfig for the cluster running ArgoCD and k2a-token-sync (default: normal resolution)")
		kubeContext = fs.String("context", "",
			"context of the cluster running ArgoCD and k2a-token-sync (default: current context)")

		namespace   = fs.String("namespace", "k2a-token-sync", "namespace k2a-token-sync runs in; the credential is written here")
		saName      = fs.String("serviceaccount", "argocd-manager", "downstream ServiceAccount ArgoCD authenticates as")
		saNamespace = fs.String("serviceaccount-namespace", "kube-system", "namespace for the downstream ServiceAccounts")
		selfSAName  = fs.String("self-serviceaccount", "k2a-token-sync", "downstream ServiceAccount k2a-token-sync authenticates as")

		printOnly = fs.Bool("print", false, "write the ClusterConnection to stdout instead of applying it")
		dryRun    = fs.Bool("dry-run", false, "report what would be done and change nothing")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: k2a-token-sync bootstrap --cluster NAME --endpoint HOST[:PORT] [flags]

Prepares a downstream cluster and registers it, so ArgoCD can reach it directly.
Installs two identities there — one for ArgoCD (cluster-admin) and one
narrowly-scoped for k2a-token-sync — stores a credential for the latter, and
applies the ClusterConnection that puts the cluster into service.

Run this once per cluster. Afterwards k2a-token-sync needs no administrative
access, and renews its own credential unattended.

Two clusters are involved. --kubeconfig and --context select the one running
ArgoCD and k2a-token-sync, where the credential is stored and the object created;
both default to your usual kubeconfig and current context. --from-kubeconfig and
--from-context select the downstream cluster being prepared, and one of them is
required.

Modes:
  (default)    provision, store the credential, and apply the ClusterConnection
  --print      provision and store the credential, but write the manifest to
               stdout instead of applying it — for keeping those objects in git
  --dry-run    change nothing; report the plan and show the manifest

The credential never passes through your terminal, and the manifest contains
nothing secret:

  k2a-token-sync bootstrap --cluster prod-1 --endpoint prod-1.example.com:6443 \
    --from-kubeconfig ./prod-1.kubeconfig

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

	out := &steps{w: os.Stderr}

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

	out.headingf("Bootstrapping %s", cluster.Name)
	out.stepf("downstream cluster", "%s", downstreamCfg.Host)
	out.stepf("ArgoCD endpoint", "%s", cluster.ServerURL())
	out.blank()

	if *dryRun {
		out.stepf("identities", "would create %s and %s",
			cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName)
		out.stepf("credential", "would write %s", *namespace+"/"+cluster.CredentialsSecretName())
		out.stepf("registration", "would apply %s", *namespace+"/"+cluster.Name)
		out.blank()
		out.notef("Nothing was changed. The manifest below is what would be applied.")
		out.blank()
		return printConnection(cluster, *namespace)
	}

	// Pre-flight before provisioning. A certificate that does not cover the
	// endpoint is the most common reason direct access fails, and it is far
	// cheaper to learn now than after two identities exist downstream.
	cert, err := preflight(ctx, downstreamClient, cluster)
	if err != nil {
		return err
	}
	out.stepf("endpoint certificate", "valid until %s (%d days left)",
		cert.NotAfter.UTC().Format(time.DateOnly), cert.DaysRemaining())

	provisioned, err := reconcile.Provision(ctx, downstreamClient, cluster)
	if err != nil {
		return fmt.Errorf("provisioning the downstream cluster: %w", err)
	}
	out.stepf("identities", "%s, %s",
		cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
		cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName)

	if err := kubeclient.WriteCredentials(ctx, localClient, *namespace, cluster.CredentialsSecretName(), provisioned,
		map[string]string{
			"app.kubernetes.io/managed-by": "k2a-token-sync",
			"k2a-token-sync.io/cluster":    cluster.Name,
		}); err != nil {
		return fmt.Errorf("writing the credential: %w", err)
	}
	out.stepf("credential", "%s, expires %s",
		*namespace+"/"+cluster.CredentialsSecretName(),
		provisioned.ExpiresAt.UTC().Format(time.DateOnly))

	// Prove the whole path ArgoCD will use, with the credential just stored. A
	// failure here is a warning: the endpoint may be reachable from the cluster
	// k2a-token-sync runs on but not from wherever this is being run.
	if err := verifyCredential(ctx, cluster, provisioned); err != nil {
		out.warnf("could not reach %s with the new credential from here: %v", cluster.Endpoint, err)
		out.warnf("k2a-token-sync may still succeed if it can reach the endpoint")
	} else {
		out.stepf("verified", "the credential works against the ArgoCD endpoint")
	}

	if *printOnly {
		out.blank()
		out.notef("Apply or commit the manifest below to put %s into service.", cluster.Name)
		out.blank()
		return printConnection(cluster, *namespace)
	}

	if err := applyConnection(ctx, cluster, *namespace, *kubeconfig, *kubeContext); err != nil {
		return err
	}
	out.stepf("registration", "%s", *namespace+"/"+cluster.Name)
	out.blank()
	out.notef("Done. k2a-token-sync publishes ArgoCD's credential within 30 seconds:")
	out.notef("  kubectl -n %s get ccon %s", *namespace, cluster.Name)
	return nil
}

// steps writes the progress a person reads. Structured logging is right for the
// reconciliation loop, which a collector consumes, and wrong here: every value is
// short, the labels are self-evident, and JSON would spend most of its characters
// on quoting and timestamps.
//
// Everything goes to stderr, so --print can send YAML to stdout and a redirect
// captures only the manifest.
type steps struct {
	w io.Writer
}

const stepLabelWidth = 21

// The writes are deliberately unchecked: progress output that cannot reach the
// terminal has no useful recovery, and failing the bootstrap because a pipe closed
// would be worse than finishing quietly.

func (s *steps) headingf(format string, args ...any) {
	_, _ = fmt.Fprintf(s.w, format+"\n", args...)
}

func (s *steps) stepf(label, format string, args ...any) {
	_, _ = fmt.Fprintf(s.w, "  %-*s %s\n", stepLabelWidth, label, fmt.Sprintf(format, args...))
}

func (s *steps) warnf(format string, args ...any) {
	_, _ = fmt.Fprintf(s.w, "  %-*s %s\n", stepLabelWidth, "warning", fmt.Sprintf(format, args...))
}

func (s *steps) notef(format string, args ...any) {
	_, _ = fmt.Fprintf(s.w, format+"\n", args...)
}

func (s *steps) blank() {
	_, _ = fmt.Fprintln(s.w)
}

// preflight checks that the endpoint ArgoCD will use presents a certificate that
// covers it and verifies against the cluster's own CA.
func preflight(ctx context.Context, client kubernetes.Interface, cluster config.Cluster) (*downstream.ServingCert, error) {
	ca, err := downstream.ClusterCA(ctx, client, cluster.ServiceAccount.Namespace)
	if err != nil {
		return nil, fmt.Errorf("reading the cluster CA: %w", err)
	}

	cert, err := downstream.ProbeServingCert(ctx, cluster.Endpoint, ca)
	if err != nil {
		return nil, fmt.Errorf("probing %s: %w", cluster.Endpoint, err)
	}
	if cert.HostnameError != nil {
		return nil, fmt.Errorf("the endpoint's certificate cannot be used: %w", cert.HostnameError)
	}
	if !cert.TrustedByCA {
		return nil, fmt.Errorf("the certificate presented at %s does not verify against the cluster CA; "+
			"ArgoCD would reject this endpoint", cluster.Endpoint)
	}
	return cert, nil
}

// verifyCredential uses the stored credential exactly as k2a-token-sync will, to
// prove endpoint, certificate, token and downstream RBAC work together.
func verifyCredential(ctx context.Context, cluster config.Cluster, creds *kubeclient.Credentials) error {
	client, err := reconcile.ClientFromCredentials(cluster.ServerURL(), creds)
	if err != nil {
		return err
	}
	_, err = downstream.ClusterCA(ctx, client, cluster.ServiceAccount.Namespace)
	return err
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

// renderConnection encodes the object as the YAML a person would commit.
//
// Only apiVersion, kind, metadata and spec are included: status belongs to
// k2a-token-sync, and a "status: {}" stanza in a committed file is noise at best
// and an invitation to edit it at worst.
func renderConnection(cluster config.Cluster, namespace string) ([]byte, error) {
	conn := connectionFor(cluster, namespace)
	raw, err := yaml.Marshal(struct {
		metav1.TypeMeta `json:",inline"`
		Metadata        metav1.ObjectMeta              `json:"metadata"`
		Spec            v1alpha1.ClusterConnectionSpec `json:"spec"`
	}{TypeMeta: conn.TypeMeta, Metadata: conn.ObjectMeta, Spec: conn.Spec})
	if err != nil {
		return nil, fmt.Errorf("encoding the ClusterConnection: %w", err)
	}
	return raw, nil
}

// printConnection writes the manifest to stdout, where a redirect or a pipe into
// kubectl can take it.
func printConnection(cluster config.Cluster, namespace string) error {
	raw, err := renderConnection(cluster, namespace)
	if err != nil {
		return err
	}
	if _, err := os.Stdout.Write(raw); err != nil {
		return fmt.Errorf("writing the ClusterConnection: %w", err)
	}
	return nil
}

// applyConnection puts the object into the cluster. Server-side apply means this
// both creates and updates, so re-running bootstrap for a cluster is safe.
//
// It uses the dynamic client for the same reason k2a-token-sync does: no
// generated clientset is needed to write one object.
func applyConnection(ctx context.Context, cluster config.Cluster, namespace, kubeconfig, kubeContext string) error {
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
