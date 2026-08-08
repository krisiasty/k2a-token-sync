package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/argocd"
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

		namespace = fs.String("namespace", "k2a-token-sync", "namespace k2a-token-sync runs in; the credential is written here")
		// Needed only to look at the registration this cluster would claim. The
		// default matches the chart's, and the daemon's ARGOCD_NAMESPACE.
		argocdNamespace = fs.String("argocd-namespace", "argocd",
			"namespace holding ArgoCD's cluster Secrets, checked for a registration this would take over")
		adopt = fs.Bool("adopt", false,
			"take over an existing cluster Secret that k2a-token-sync did not create, e.g. migrating from 'argocd cluster add'")
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

Before provisioning anything, this checks whether ArgoCD already holds a cluster
Secret under the name this cluster would claim, and refuses if one is there that
k2a-token-sync did not create. That Secret is what ArgoCD authenticates with, and
the name 'argocd cluster add' uses is the same one — so a mistyped cluster name
would silently repoint an existing registration at a different cluster. Migrating
from 'argocd cluster add' is the case where you do want that, and --adopt is how
you say so.

The credential never passes through your terminal, and the manifest contains
nothing secret:

  k2a-token-sync bootstrap --cluster prod-1 --endpoint prod-1.example.com:6443 --from-kubeconfig ./prod-1.kubeconfig

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
	localClient, localCfg, err := localClientFor(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}

	// Output is grouped by cluster, because "where did that happen" is the first
	// question a reader has when a command touches two of them. Each heading names
	// its cluster once and the steps beneath it inherit the location.
	out.headingf("Bootstrapping %s", cluster.Name)
	out.blank()
	out.headingf("Downstream cluster — %s", cluster.Name)
	out.stepf("reached via", "%s", downstreamCfg.Host)
	out.stepf("ArgoCD endpoint", "%s", cluster.ServerURL())

	// Pre-flight before provisioning. A certificate that does not cover the
	// endpoint is the most common reason direct access fails, and it is far cheaper
	// to learn now than after two identities exist downstream. It runs in a dry run
	// too — it reads a ConfigMap and opens a TLS connection, changing nothing, and
	// a plan that omits the precondition most likely to fail is worth little.
	cert, err := preflight(ctx, downstreamClient, cluster)
	if err != nil {
		return err
	}
	out.stepf("endpoint certificate", "valid until %s (%d days left)",
		cert.NotAfter.UTC().Format(time.DateOnly), cert.DaysRemaining())

	// Before provisioning, and before the dry-run branch, because this is the only
	// point at which taking over somebody else's registration can be *prevented*
	// rather than reported after the fact: there are administrative credentials here
	// and a person to answer to, where the daemon holds no read permission on those
	// Secrets by design. Refusing after provisioning would also leave two identities
	// behind downstream with no credential stored for them.
	//
	// Only the refusal happens here. What was found is reported further down, under
	// the heading for the cluster it was found on.
	target := inspectRegistrationTarget(ctx, localClient, *argocdNamespace, cluster.SecretName)
	if err := target.refusal(*adopt, *argocdNamespace, cluster.SecretName); err != nil {
		return err
	}
	adopted := target.recordsAdoption(*adopt)

	if *dryRun {
		out.stepf("identities", "would create %s and %s",
			cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName)
		out.blank()
		out.headingf("Cluster running ArgoCD — %s", describeCluster(*kubeContext, localCfg.Host))
		out.sameClusterNote(downstreamCfg.Host, localCfg.Host)
		target.report(out, *argocdNamespace, cluster.SecretName, adopted)
		out.stepf("credential", "would write %s", *namespace+"/"+cluster.CredentialsSecretName())
		// "connection", not "registration": the line above from target.report
		// already used that label for ArgoCD's cluster Secret, and two different
		// objects in two different namespaces sharing one label is what made this
		// output ambiguous. 'remove' has always drawn the distinction this way.
		out.stepf("connection", "would apply %s", *namespace+"/"+cluster.Name)
		out.blank()
		out.notef("Nothing was changed. The manifest below is what would be applied.")
		out.blank()
		return printConnection(cluster, *namespace, adopted)
	}

	provisioned, err := reconcile.Provision(ctx, downstreamClient, cluster)
	if err != nil {
		return fmt.Errorf("provisioning the downstream cluster: %w", err)
	}
	out.stepf("identities", "%s, %s",
		cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
		cluster.ServiceAccount.Namespace+"/"+cluster.SelfServiceAccountName)

	// Prove the path ArgoCD will use, with the credential just minted, before
	// storing it. Reported here because it is the downstream endpoint being tested;
	// a failure is a warning, since the endpoint may be reachable from the cluster
	// k2a-token-sync runs on but not from wherever this is being run.
	if err := verifyCredential(ctx, cluster, provisioned); err != nil {
		out.warnf("could not reach the endpoint with the new credential from here: %v", err)
		out.warnf("k2a-token-sync may still succeed if it can reach the endpoint")
	} else {
		out.stepf("verified", "the new credential works against the endpoint")
	}

	out.blank()
	out.headingf("Cluster running ArgoCD — %s", describeCluster(*kubeContext, localCfg.Host))
	out.sameClusterNote(downstreamCfg.Host, localCfg.Host)
	target.report(out, *argocdNamespace, cluster.SecretName, adopted)

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

	if *printOnly {
		out.blank()
		out.notef("Apply or commit the manifest below, to %s, to put %s into service.",
			describeCluster(*kubeContext, localCfg.Host), cluster.Name)
		out.blank()
		return printConnection(cluster, *namespace, adopted)
	}

	if err := applyConnection(ctx, cluster, *namespace, *kubeconfig, *kubeContext, adopted); err != nil {
		return err
	}
	out.stepf("connection", "%s", *namespace+"/"+cluster.Name)
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

// sameClusterNote calls out the case where both clusters are one — registering the
// cluster ArgoCD itself runs on. Without it the reader sees the same address twice
// and has to work out whether that is a mistake.
func (s *steps) sameClusterNote(downstreamHost, localHost string) {
	if downstreamHost == localHost {
		s.stepf("note", "the same cluster as above")
	}
}

// registrationTarget is what was found where this cluster's ArgoCD registration
// will be written.
type registrationTarget int

const (
	// targetAbsent means nothing is there: an ordinary create, and the common case.
	targetAbsent registrationTarget = iota

	// targetOurs means a previous k2a-token-sync pass created it. Re-running
	// bootstrap for a cluster already in service lands here, and must not be
	// mistaken for a takeover.
	targetOurs

	// targetForeign means something else created it. This is the case worth
	// stopping for.
	targetForeign

	// targetUnreadable means the check could not run.
	targetUnreadable
)

// inspectRegistrationTarget looks at the cluster Secret this bootstrap would put
// into service, and reports who owns it.
//
// A Secret created by 'argocd cluster add' is indistinguishable by name from one
// k2a-token-sync would create: the same cluster- prefix, and cluster-<name> is the
// default on both sides. The managed-by label is the difference, and it is only
// ever written by a k2a-token-sync pass — so its absence means "not this tool's",
// which is exactly the question being asked.
//
// Being unable to read is not a failure. Bootstrap runs with whatever kubeconfig
// the operator has, which need not cover ArgoCD's namespace, and refusing on that
// basis would break bootstrap for everyone with narrower rights in order to guard
// a case where a person is present anyway.
// It returns no error for the same reason: every outcome, including not being
// allowed to look, is one of the states above.
func inspectRegistrationTarget(
	ctx context.Context,
	client kubernetes.Interface,
	argocdNamespace, secretName string,
) registrationTarget {
	secret, err := client.CoreV1().Secrets(argocdNamespace).Get(ctx, secretName, metav1.GetOptions{})
	return classifyRegistrationSecret(secret, err)
}

// classifyRegistrationSecret turns the result of reading a cluster Secret into
// the verdict above.
//
// Split out from the read so a caller that already holds the Secret can reach
// the same verdict without a second Get. 'remove' is that caller: it needs the
// cluster annotation and the field managers off the very same object, and
// reading it twice would both cost a round trip and open a window in which the
// Secret it classified is not the Secret it went on to inspect.
func classifyRegistrationSecret(secret *corev1.Secret, err error) registrationTarget {
	switch {
	case apierrors.IsNotFound(err):
		return targetAbsent
	case err != nil:
		return targetUnreadable
	case secret.Labels[argocd.ManagedByLabel] == argocd.ManagedByValue:
		return targetOurs
	default:
		return targetForeign
	}
}

// refusal is the error for a takeover that was not asked for, or nil when there is
// nothing to stop.
//
// It names the Secret and what would happen to it, because the reader has to be
// able to tell which of two very different situations they are in — a migration
// they meant, or a cluster name that collided with a registration other
// Applications depend on — and only they can.
func (t registrationTarget) refusal(adopt bool, argocdNamespace, secretName string) error {
	if t != targetForeign || adopt {
		return nil
	}
	return fmt.Errorf("%s/%s already exists and was not created by k2a-token-sync, so putting this cluster "+
		"into service would replace its credential with one for this cluster\n"+
		"  If that is the migration you intend, from 'argocd cluster add', re-run with --adopt.\n"+
		"  If it is not, this cluster's name has collided with an existing registration: choose another "+
		"--cluster name, or set spec.secretName on the ClusterConnection to something unclaimed.\n"+
		"  Nothing has been changed",
		argocdNamespace, secretName)
}

// recordsAdoption reports whether the ClusterConnection should carry the adoption
// annotation.
//
// Only where adoption actually applies. Marking a Secret this tool created as
// adopted would leave a permanent claim that something was inherited when nothing
// was, and the annotation's whole job is to tell those two apart later. The
// unreadable case defers to the operator: they asked for adoption and nothing here
// can contradict them.
func (t registrationTarget) recordsAdoption(adopt bool) bool {
	return adopt && (t == targetForeign || t == targetUnreadable)
}

// report says what was found, under the heading for the cluster it was found on.
func (t registrationTarget) report(out *steps, argocdNamespace, secretName string, adopted bool) {
	secret := argocdNamespace + "/" + secretName
	switch t {
	case targetAbsent:
		out.stepf("registration", "%s does not exist yet", secret)
	case targetOurs:
		out.stepf("registration", "%s already managed by k2a-token-sync", secret)
	case targetForeign:
		out.stepf("registration", "adopting %s, which k2a-token-sync did not create", secret)
		out.warnf("its existing credential will be replaced and cannot be recovered; " +
			"'argocd cluster add' is what restores it")
	case targetUnreadable:
		out.warnf("could not read %s, so whether this would take over an existing registration is unknown", secret)
		if adopted {
			out.stepf("registration", "adopting %s on request", secret)
		}
	}
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
//
// adopted adds the annotation that tells later passes a co-owner on the cluster
// Secret was expected. It goes on the object rather than being remembered here
// because the pass that notices is a different process, days later: bootstrap is
// the only place the intent exists, and the object is the only place it survives.
func connectionFor(cluster config.Cluster, namespace string, adopted bool) *v1alpha1.ClusterConnection {
	var annotations map[string]string
	if adopted {
		annotations = map[string]string{v1alpha1.AnnotationAdopted: "true"}
	}
	return &v1alpha1.ClusterConnection{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "k2a-token-sync.io/v1alpha1",
			Kind:       "ClusterConnection",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   namespace,
			Annotations: annotations,
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
func renderConnection(cluster config.Cluster, namespace string, adopted bool) ([]byte, error) {
	conn := connectionFor(cluster, namespace, adopted)
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
func printConnection(cluster config.Cluster, namespace string, adopted bool) error {
	raw, err := renderConnection(cluster, namespace, adopted)
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
func applyConnection(
	ctx context.Context,
	cluster config.Cluster,
	namespace, kubeconfig, kubeContext string,
	adopted bool,
) error {
	dyn, err := kubeclient.DynamicClientForContext(kubeconfig, kubeContext)
	if err != nil {
		return err
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(connectionFor(cluster, namespace, adopted))
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
//
// The rest.Config comes back so the cluster can be named in the output: a command
// that writes to two clusters should say which one each step touched.
func localClientFor(kubeconfig, contextName string) (kubernetes.Interface, *rest.Config, error) {
	if kubeconfig == "" && contextName == "" {
		cfg, err := kubeclient.LocalRESTConfig()
		if err != nil {
			return nil, nil, err
		}
		client, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("creating kubernetes client: %w", err)
		}
		return client, cfg, nil
	}
	return kubeclient.ClientForContext(kubeconfig, contextName)
}

// describeCluster names a cluster the way its operator would recognise it: by
// context when one was given, since that is what they typed, with the server
// address for confirmation. In-cluster there is no context, so the address stands
// alone.
func describeCluster(contextName, host string) string {
	if contextName == "" {
		return host
	}
	return fmt.Sprintf("context %s (%s)", contextName, host)
}
