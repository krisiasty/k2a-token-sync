package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/krisiasty/k2a-token-sync/internal/argocd"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/downstream"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
	kubeclient "github.com/krisiasty/k2a-token-sync/internal/k8s"
)

const removeTimeout = 2 * time.Minute

// credentialClusterLabel names the cluster that owns this tool's own
// credential Secret. The literal is the same string as
// argocd.ClusterNameAnnotation, but that constant is exported for a
// different field on a different object: an annotation on the ArgoCD cluster
// Secret, written by argocd.ApplyRegistration. reconcile.go and bootstrap.go
// write this same string as a label — not an annotation — on the credential
// Secret instead, which is a distinct piece of the API with its own selector
// semantics, so it is repeated here as its own local literal rather than
// reusing argocd's constant for what is only a coincidentally identical
// string.
const credentialClusterLabel = "k2a-token-sync.io/cluster" //nolint:gosec // a label key, not a credential

// runRemove tears down everything bootstrap or the API put in place for one
// cluster: the ClusterConnection, the ArgoCD registration, this tool's own
// credential, and — unless told to leave them — the downstream identities and
// RBAC.
//
// It exists because until now the only documented way to retire a cluster was
// 'kubectl delete ccon', which leaves the ArgoCD Secret, the credential and
// the downstream identities all behind: an escalatable ServiceAccount with a
// live token nobody is watching. This is the deliberate counterpart.
//
// The local guards — resolving the ClusterConnection, and checking ownership
// of the ArgoCD Secret and the credential Secret — and the endpoint-collision
// guard are all evaluated up front, before anything is deleted, so a run
// that is going to refuse one of those says so before it has half-torn-down
// anything. The five downstream objects are different: their ownership guard
// is evaluated per-object, at delete time, during step 4, after steps 1-3
// have already run, via internal/downstream/remove.go's primitives — this is
// what lets a run "skip that object and carry on" rather than needing every
// downstream identity read twice. Guards on individual objects never stop the
// rest of the teardown — only the flag-validation checks below stop
// everything before it starts.
//
// runRemove itself only parses and validates flags and builds the two
// clients; executeRemoval does everything from there, so a test can drive
// the ordering, the guards and the exit-signal contract with fake clientsets
// instead of real ones.
func runRemove(args []string) error {
	fs := flag.NewFlagSet("remove", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		clusterName = fs.String("cluster", "", "the ClusterConnection's name (required)")

		// The unprefixed pair means what it means in bootstrap: the cluster this
		// command works against, running ArgoCD and k2a-token-sync.
		kubeconfig = fs.String("kubeconfig", "",
			"path to the kubeconfig for the cluster running ArgoCD and k2a-token-sync (default: normal resolution)")
		kubeContext = fs.String("context", "",
			"context of the cluster running ArgoCD and k2a-token-sync (default: current context)")

		namespace       = fs.String("namespace", "k2a-token-sync", "namespace holding the ClusterConnection and the credential Secret")
		argocdNamespace = fs.String("argocd-namespace", "argocd", "namespace holding ArgoCD's cluster Secret")

		fromKubeconfig = fs.String("from-kubeconfig", "",
			"path to a kubeconfig for the downstream cluster being cleaned up")
		fromContext = fs.String("from-context", "",
			"context to use within --from-kubeconfig, or within the ambient kubeconfig when that is unset")
		skipDownstream = fs.Bool("skip-downstream", false,
			"remove only the local objects, leaving the downstream identities and RBAC in place; "+
				"mutually exclusive with --from-kubeconfig/--from-context")

		// Fallback overrides. They matter only when the ClusterConnection is
		// already gone, which today's README makes the common case: it tells
		// people to delete the object first, and this is what recovers the names
		// that were on it.
		saName = fs.String("serviceaccount", "argocd-manager",
			"fallback: downstream ServiceAccount ArgoCD authenticates as, used only when the ClusterConnection no longer exists")
		saNamespace = fs.String("serviceaccount-namespace", "kube-system",
			"fallback: namespace for the downstream ServiceAccounts, used only when the ClusterConnection no longer exists")
		selfSAName = fs.String("self-serviceaccount", "k2a-token-sync",
			"fallback: downstream ServiceAccount k2a-token-sync authenticates as, used only when the ClusterConnection no longer exists")
		secretName = fs.String("secret-name", "",
			"fallback: ArgoCD cluster Secret name (default cluster-<name>), used only when the ClusterConnection no longer exists")

		confirm = fs.Bool("confirm", false, "required before anything is deleted")
		dryRun  = fs.Bool("dry-run", false, "report the plan and change nothing; does not need --confirm")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: k2a-token-sync remove --cluster NAME (--from-kubeconfig PATH | --from-context CTX | --skip-downstream) --confirm

Retires a cluster: deletes its ClusterConnection, the ArgoCD cluster Secret,
k2a-token-sync's own credential for it, and — unless --skip-downstream is
given — the downstream identities and RBAC ArgoCD and k2a-token-sync
authenticated as. 'kubectl delete ccon' alone leaves all of that behind,
including a live, escalatable ServiceAccount nobody is watching any more.

Two clusters are involved, as in bootstrap. --kubeconfig and --context select
the one running ArgoCD and k2a-token-sync, where the ClusterConnection, the
ArgoCD Secret and the credential all live; both default to your usual
kubeconfig and current context. --from-kubeconfig and --from-context select
the downstream cluster being cleaned up, and one of them is required unless
--skip-downstream says to leave the downstream side alone.

Nothing is deleted without --confirm. --dry-run reports the same plan and
changes nothing, without needing --confirm.

Removing something that is not this tool's is refused object by object: a
Secret without k2a-token-sync's managed-by label, or one whose recorded owner
names a different cluster, is left alone and named in the output, and the
rest of the teardown still runs. If another ClusterConnection in this
namespace resolves to the same downstream endpoint, the whole downstream half
is refused, since the two identities are cluster-scoped and shared: removing
them for one would break the other. That check only sees ClusterConnections
in --namespace on this cluster — a second ArgoCD instance elsewhere sharing
the same downstream cluster is invisible to it.

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

	if err := validateRemoveFlags(*clusterName, *fromKubeconfig, *fromContext, *skipDownstream, *confirm, *dryRun); err != nil {
		fs.Usage()
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), removeTimeout)
	defer cancel()

	out := &steps{w: os.Stderr}

	localClient, localCfg, err := localClientFor(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}
	dyn, err := kubeclient.DynamicClientForContext(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}

	var (
		downstreamClient kubernetes.Interface
		downstreamHost   string
	)
	if !*skipDownstream {
		client, cfg, dcErr := kubeclient.ClientForContext(*fromKubeconfig, *fromContext)
		if dcErr != nil {
			return dcErr
		}
		downstreamClient = client
		downstreamHost = cfg.Host
	}

	remaining, err := executeRemoval(ctx, out, localClient, dyn, downstreamClient, removeParams{
		clusterName:     *clusterName,
		namespace:       *namespace,
		argocdNamespace: *argocdNamespace,
		fallback: config.RemovalClusterInput{
			Name:                    *clusterName,
			ServiceAccountName:      *saName,
			ServiceAccountNamespace: *saNamespace,
			SelfServiceAccountName:  *selfSAName,
			SecretName:              *secretName,
		},
		kubeContext:    *kubeContext,
		localHost:      localCfg.Host,
		downstreamHost: downstreamHost,
		skipDownstream: *skipDownstream,
		dryRun:         *dryRun,
	})
	if err != nil {
		return err
	}

	if len(remaining) > 0 {
		out.blank()
		out.notef("The following could not be removed. Nothing else was affected by these:")
		for _, item := range remaining {
			out.blank()
			out.notef("  %s", item.what)
			out.notef("    %s", item.why)
			out.notef("    %s", item.kubectl)
		}
		return fmt.Errorf("%d object(s) could not be removed; see above for how to remove them by hand", len(remaining))
	}

	if *dryRun {
		out.notef("Nothing was changed. This is what removing %s would do.", *clusterName)
		return nil
	}
	out.notef("Done. ArgoCD no longer holds a registration for %s.", *clusterName)
	return nil
}

// removeParams bundles the flag-derived settings executeRemoval needs, once
// runRemove has parsed and validated the flags and built the two clients from
// them. Kept as one struct rather than a growing positional argument list,
// and so a test can construct one directly instead of parsing flags at all.
type removeParams struct {
	clusterName     string
	namespace       string
	argocdNamespace string

	// fallback is what resolveRemovalCluster falls back to when the
	// ClusterConnection is gone or unresolvable; see resolveRemovalCluster.
	fallback config.RemovalClusterInput

	// kubeContext and localHost are display-only, for describeCluster's
	// "Cluster running ArgoCD — ..." heading.
	kubeContext string
	localHost   string

	// downstreamHost is display-only too; empty when skipDownstream is true,
	// since nothing then talks to a downstream cluster at all.
	downstreamHost string

	skipDownstream bool
	dryRun         bool
}

// executeRemoval runs the five-step teardown against already-built clients:
// resolve the ClusterConnection, run every guard, then delete in order. It is
// everything runRemove does once flag-parsing and client construction are out
// of the way, split out so a test can drive the ordering, the collision
// guard and the exit-signal contract with fake clientsets — runBootstrap has
// no equivalent split because its tests only reach its helpers; this task's
// brief required more than that.
//
// A non-nil error means a read that has to succeed before anything else can
// be attempted (reading or listing ClusterConnections) failed; the caller
// aborts without deleting anything. A non-empty remaining, error-free
// otherwise, means the guarded, step-by-step teardown ran to completion but
// left one or more objects behind — the caller turns that into a non-zero
// exit. Both are empty and nil only once every object was deleted, already
// absent, or (in a dry run) would have been.
func executeRemoval(
	ctx context.Context,
	out *steps,
	localClient kubernetes.Interface,
	dyn dynamic.Interface,
	downstreamClient kubernetes.Interface,
	params removeParams,
) ([]remainingItem, error) {
	inv := inventory.NewClient(dyn, params.namespace)

	// Every guard below is a read, run before this touches anything. The
	// ClusterConnection is read here rather than re-derived, so a non-default
	// spec.secretName or serviceAccount is never missed.
	cluster, usedFallback, fallbackReason, err := resolveRemovalCluster(ctx, inv, params.clusterName, params.fallback)
	if err != nil {
		return nil, fmt.Errorf("reading the ClusterConnection: %w", err)
	}

	// The collision guard can only compare against a real, resolved endpoint,
	// which a fallback cluster never has — whether the fallback happened
	// because the object is gone or because its spec could not be resolved.
	// Skipping it here, rather than only failing to find a match, means the
	// downstream half is torn down with that protection silently off; the
	// output below says so.
	var collidesWith string
	skippedCollisionGuard := usedFallback && !params.skipDownstream
	if !usedFallback && !params.skipDownstream {
		collidesWith, err = endpointCollision(ctx, inv, cluster.Name, cluster.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("listing ClusterConnections: %w", err)
		}
	}

	secretGuard := inspectArgoCDSecretForRemoval(ctx, localClient, params.argocdNamespace, cluster.SecretName, cluster.Name)
	credName := cluster.CredentialsSecretName()
	credGuard := inspectCredentialSecretForRemoval(ctx, localClient, params.namespace, credName, cluster.Name)

	out.headingf("Removing %s", cluster.Name)
	out.blank()
	if usedFallback {
		if fallbackReason != "" {
			out.notef("The ClusterConnection %s/%s could not be resolved (%s); using default names and the "+
				"override flags below.", params.namespace, params.clusterName, fallbackReason)
		} else {
			out.notef("The ClusterConnection %s/%s no longer exists; using default names and the override flags below.",
				params.namespace, params.clusterName)
		}
		out.blank()
	}
	if skippedCollisionGuard {
		out.notef("Skipping the endpoint-collision guard: there is no resolved endpoint for %s to compare "+
			"against, so another ClusterConnection sharing its downstream cluster cannot be detected.", params.clusterName)
		out.blank()
	}

	var remaining []remainingItem

	// --- Cluster running ArgoCD and k2a-token-sync ---
	out.headingf("Cluster running ArgoCD — %s", describeCluster(params.kubeContext, params.localHost))

	// Step 1: the ClusterConnection, first, so the daemon stops republishing
	// the registration while the rest of this is torn down.
	ccOutcome, ccErr := removeClusterConnection(ctx, dyn, params.namespace, cluster.Name, params.dryRun)
	out.stepf("connection", "%s", describeOutcome(params.namespace+"/"+cluster.Name, ccOutcome, "", ccErr, params.dryRun))
	if item := remainingFromOutcome(params.namespace+"/"+cluster.Name, ccOutcome, ccErr,
		fmt.Sprintf("kubectl -n %s delete clusterconnection %s", params.namespace, cluster.Name)); item != nil {
		remaining = append(remaining, *item)
	}

	// Step 2: the ArgoCD cluster Secret — the live credential. The guard was
	// evaluated above, before any delete ran.
	if adopted := adoptionWarning(cluster, params.argocdNamespace); adopted != "" {
		out.warnf("%s", adopted)
	}
	if reason := secretGuard.refusalReason(); reason != "" {
		verb := "left alone"
		if params.dryRun {
			verb = "would leave alone"
		}
		out.stepf("registration", "%s: %s/%s (%s)", verb, params.argocdNamespace, cluster.SecretName, reason)
		remaining = append(remaining, remainingItem{
			what: params.argocdNamespace + "/" + cluster.SecretName,
			why:  reason,
			kubectl: refusedSecretHandFix(params.argocdNamespace, cluster.SecretName,
				secretGuard.belongsToDifferentCluster(), secretGuard.unverifiable()),
		})
	} else {
		regOutcome, regErr := removeArgoCDSecret(ctx, localClient, params.argocdNamespace, cluster.SecretName, params.dryRun)
		out.stepf("registration", "%s",
			describeOutcome(params.argocdNamespace+"/"+cluster.SecretName, regOutcome, "", regErr, params.dryRun))
		if item := remainingFromOutcome(params.argocdNamespace+"/"+cluster.SecretName, regOutcome, regErr,
			fmt.Sprintf("kubectl -n %s delete secret %s", params.argocdNamespace, cluster.SecretName)); item != nil {
			remaining = append(remaining, *item)
		}
	}

	// Step 3: this tool's own credential for the cluster. The guard was
	// evaluated above too, alongside the ArgoCD Secret's, before any delete ran.
	if reason := credGuard.refusalReason(); reason != "" {
		verb := "left alone"
		if params.dryRun {
			verb = "would leave alone"
		}
		out.stepf("credential", "%s: %s/%s (%s)", verb, params.namespace, credName, reason)
		remaining = append(remaining, remainingItem{
			what: params.namespace + "/" + credName,
			why:  reason,
			kubectl: refusedSecretHandFix(params.namespace, credName,
				credGuard.belongsToDifferentCluster(), credGuard.unverifiable()),
		})
	} else {
		credOutcome, credErr := removeCredentialSecret(ctx, localClient, params.namespace, credName, params.dryRun)
		out.stepf("credential", "%s", describeOutcome(params.namespace+"/"+credName, credOutcome, "", credErr, params.dryRun))
		if item := remainingFromOutcome(params.namespace+"/"+credName, credOutcome, credErr,
			fmt.Sprintf("kubectl -n %s delete secret %s", params.namespace, credName)); item != nil {
			remaining = append(remaining, *item)
		}
	}
	out.blank()

	// Step 4: downstream, unless asked to leave it. Bindings before their
	// subjects, so privilege is dropped before the identity is.
	if !params.skipDownstream {
		out.headingf("Downstream cluster — %s", cluster.Name)
		out.stepf("reached via", "%s", params.downstreamHost)

		if collidesWith != "" {
			out.warnf("%s also resolves to %s; leaving the downstream identities and RBAC in place "+
				"so removing them here does not break it too", collidesWith, cluster.Endpoint)
			remaining = append(remaining, remainingItem{
				what: "downstream identities and RBAC for " + cluster.Name,
				why: fmt.Sprintf("endpoint is shared with ClusterConnection %q; the two identities are "+
					"cluster-scoped and shared between every connection to this cluster", collidesWith),
				kubectl: "resolve the collision between the two ClusterConnections first, then re-run " +
					"remove once only one of them still claims this endpoint",
			})
		} else {
			saRoleBinding := cluster.ServiceAccount.Name + "-role-binding"

			crbA, errA := downstream.RemoveClusterRoleBinding(ctx, downstreamClient, saRoleBinding, params.dryRun)
			saB, errB := downstream.RemoveServiceAccount(ctx, downstreamClient,
				cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name, params.dryRun)
			crbC, errC := downstream.RemoveClusterRoleBinding(ctx, downstreamClient, cluster.SelfServiceAccountName, params.dryRun)
			roleD, errD := downstream.RemoveClusterRole(ctx, downstreamClient, cluster.SelfServiceAccountName, params.dryRun)
			saE, errE := downstream.RemoveServiceAccount(ctx, downstreamClient,
				cluster.ServiceAccount.Namespace, cluster.SelfServiceAccountName, params.dryRun)

			bindings := []namedOutcome{
				{name: saRoleBinding, outcome: crbA, err: errA,
					kubectl: "kubectl delete clusterrolebinding " + saRoleBinding},
				{name: cluster.SelfServiceAccountName, outcome: crbC, err: errC,
					kubectl: "kubectl delete clusterrolebinding " + cluster.SelfServiceAccountName},
			}
			identities := []namedOutcome{
				{name: cluster.ServiceAccount.Namespace + "/" + cluster.ServiceAccount.Name, outcome: saB, err: errB,
					kubectl: fmt.Sprintf("kubectl -n %s delete serviceaccount %s",
						cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name)},
				{name: cluster.ServiceAccount.Namespace + "/" + cluster.SelfServiceAccountName, outcome: saE, err: errE,
					kubectl: fmt.Sprintf("kubectl -n %s delete serviceaccount %s",
						cluster.ServiceAccount.Namespace, cluster.SelfServiceAccountName)},
			}
			clusterrole := namedOutcome{name: cluster.SelfServiceAccountName, outcome: roleD, err: errD,
				kubectl: "kubectl delete clusterrole " + cluster.SelfServiceAccountName}

			out.stepf("bindings", "%s", summarise(bindings, params.dryRun))
			out.stepf("identities", "%s", summarise(identities, params.dryRun))
			out.stepf("clusterrole", "%s", summarise([]namedOutcome{clusterrole}, params.dryRun))

			all := make([]namedOutcome, 0, len(bindings)+len(identities)+1)
			all = append(all, bindings...)
			all = append(all, identities...)
			all = append(all, clusterrole)
			for _, it := range all {
				if item := remainingFromNamed(it); item != nil {
					remaining = append(remaining, *item)
				}
			}
		}
		out.blank()
	}

	// Step 5: re-check the ArgoCD Secret. A daemon pass already in flight when
	// step 1 landed will re-apply the registration and, finding no bearer
	// token, mint and publish a fresh credential. This runs last on purpose:
	// by now the downstream identity is gone, so anything republished here is
	// already dead. Skipped in a dry run, since nothing was deleted to recheck.
	if !params.dryRun && secretGuard.refusalReason() == "" {
		recheckOutcome, recheckErr := removeArgoCDSecret(ctx, localClient, params.argocdNamespace, cluster.SecretName, false)
		switch {
		case recheckErr != nil:
			out.warnf("re-checking %s/%s after teardown: %v", params.argocdNamespace, cluster.SecretName, recheckErr)
			remaining = append(remaining, remainingItem{
				what:    params.argocdNamespace + "/" + cluster.SecretName,
				why:     recheckErr.Error(),
				kubectl: fmt.Sprintf("kubectl -n %s delete secret %s", params.argocdNamespace, cluster.SecretName),
			})
		case recheckOutcome == downstream.RemovedOutcome:
			out.warnf("%s/%s reappeared after teardown began — a daemon pass republished it — and was deleted again",
				params.argocdNamespace, cluster.SecretName)
		}
	}

	return remaining, nil
}

// validateRemoveFlags checks the combinations runRemove refuses before
// touching any cluster: a missing --cluster, an ambiguous or missing
// downstream selection, and a run that asked for neither --confirm nor
// --dry-run.
func validateRemoveFlags(clusterName, fromKubeconfig, fromContext string, skipDownstream, confirm, dryRun bool) error {
	downstreamSelected := fromKubeconfig != "" || fromContext != ""

	switch {
	case clusterName == "":
		return errors.New("--cluster is required")
	case skipDownstream && downstreamSelected:
		return errors.New("--skip-downstream and --from-kubeconfig/--from-context are mutually exclusive; " +
			"choose one way to handle the downstream cluster")
	case !skipDownstream && !downstreamSelected:
		return errors.New("one of --from-kubeconfig or --from-context is required, to reach the downstream " +
			"cluster being cleaned up, or pass --skip-downstream to remove only the local objects")
	case !confirm && !dryRun:
		return errors.New("either --confirm or --dry-run is required: --confirm before anything is deleted, " +
			"or --dry-run to preview the plan and change nothing")
	}
	return nil
}

// resolveRemovalCluster reads the ClusterConnection remove is asked to tear
// down and resolves the names to delete from it — secretName, serviceAccount,
// selfServiceAccountName and the adoption annotation — exactly as the daemon
// would, rather than re-deriving defaults that a non-default spec would miss.
//
// It falls back to config.RemovalCluster with the override flags, and
// usedFallback says so, in two situations: the object is already gone
// (likely, since deleting it has been the only documented step until now),
// or it exists but its spec cannot be resolved — a malformed tokenTTL, an
// endpoint with a path, an over-length name — the same condition
// inventory.Client.List reports via Entry.InvalidReason. Acting on such an
// entry's Cluster directly would mean acting with every name zeroed, since
// decode never partially resolves a spec; falling back the same way as for
// an absent object at least recovers the conventional names. fallbackReason
// is empty for the absent case and holds inv's InvalidReason for the other,
// so the caller can say in its output which one happened.
func resolveRemovalCluster(
	ctx context.Context,
	inv *inventory.Client,
	name string,
	fallback config.RemovalClusterInput,
) (cluster config.Cluster, usedFallback bool, fallbackReason string, err error) {
	entry, err := inv.Get(ctx, name)
	switch {
	case apierrors.IsNotFound(err):
		cluster, err = config.RemovalCluster(fallback)
		return cluster, true, "", err
	case err != nil:
		return config.Cluster{}, false, "", err
	}
	if entry.InvalidReason != "" {
		cluster, err = config.RemovalCluster(fallback)
		return cluster, true, entry.InvalidReason, err
	}
	return entry.Cluster, false, "", nil
}

// endpointCollision reports the name of another ClusterConnection in the
// namespace that resolves to the same downstream endpoint as cluster, if any.
//
// The downstream identities are cluster-scoped and named identically for
// every connection, so removing them for one cluster registered twice would
// break the other. This can only compare against a ClusterConnection that was
// actually read: a fallback cluster has no recorded endpoint, so callers skip
// this guard entirely when resolveRemovalCluster fell back to defaults.
//
// Entries are filtered on an empty Cluster.Endpoint here, not on
// InvalidReason: inventory.Client.List's blockContestedSecrets marks both
// halves of a secretName conflict invalid, including a pair that also shares
// an endpoint — exactly the collision this guard exists to catch. Dropping
// those on InvalidReason instead of on a missing endpoint would let the
// downstream half be torn down for a connection that another, still-valid
// spec depends on. A FromSpec failure zeroes Cluster entirely, so filtering
// on the endpoint alone still excludes it without needing InvalidReason too.
func endpointCollision(ctx context.Context, inv *inventory.Client, clusterName, endpoint string) (string, error) {
	entries, err := inv.List(ctx)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.Cluster.Endpoint == "" || e.Cluster.Name == clusterName {
			continue
		}
		if e.Cluster.Endpoint == endpoint {
			return e.Cluster.Name, nil
		}
	}
	return "", nil
}

// argocdSecretRemovalGuard is what inspecting the ArgoCD cluster Secret found,
// for deciding whether remove may delete it.
type argocdSecretRemovalGuard struct {
	// target is inspectRegistrationTarget's verdict on ownership: bootstrap's
	// exact question, "did k2a-token-sync create this", asked again here.
	target registrationTarget

	// belongsTo is set when the Secret is ours by label but its
	// ClusterNameAnnotation names a different cluster than the one being
	// removed — the "belongs to a different connection" guard, which "not
	// ours" alone cannot catch.
	belongsTo string

	// verifyErr is set when target is targetOurs but the second read needed
	// to check ClusterNameAnnotation failed for a reason other than the
	// Secret being gone. Absence is not this: removeArgoCDSecret re-reads and
	// reports that case on its own. A real read failure — Forbidden, a
	// transient API error — must lean toward caution: refusing to act on a
	// Secret this could not finish inspecting, rather than proceeding as if
	// no conflicting owner annotation had been found.
	verifyErr error
}

// inspectArgoCDSecretForRemoval combines inspectRegistrationTarget's ownership
// check with the narrower one remove also needs: even a Secret this tool
// manages can have been published for a different cluster than the one named
// on the command line, if a secretName was reused. That can only be told from
// the annotation, so it is checked only once ownership itself is established
// — a foreign Secret is already refused by "not ours" and never reaches this
// check.
func inspectArgoCDSecretForRemoval(
	ctx context.Context,
	client kubernetes.Interface,
	argocdNamespace, secretName, clusterName string,
) argocdSecretRemovalGuard {
	guard := argocdSecretRemovalGuard{target: inspectRegistrationTarget(ctx, client, argocdNamespace, secretName)}
	if guard.target != targetOurs {
		return guard
	}

	secret, err := client.CoreV1().Secrets(argocdNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Gone between the ownership check above and here; removeArgoCDSecret
			// discovers and reports that on its own.
			return guard
		}
		guard.verifyErr = err
		return guard
	}
	if owner := secret.Annotations[argocd.ClusterNameAnnotation]; owner != "" && owner != clusterName {
		guard.belongsTo = owner
	}
	return guard
}

// refusalReason names why remove must leave the ArgoCD Secret alone, or ""
// when it is clear to act — including when the Secret is simply absent or
// could not be read, both of which removeArgoCDSecret discovers and reports
// on its own.
func (g argocdSecretRemovalGuard) refusalReason() string {
	switch {
	case g.target == targetForeign:
		return "not managed by k2a-token-sync"
	case g.verifyErr != nil:
		return fmt.Sprintf("could not verify which cluster it belongs to: %v", g.verifyErr)
	case g.belongsTo != "":
		return fmt.Sprintf("belongs to cluster %q, not this one", g.belongsTo)
	default:
		return ""
	}
}

// belongsToDifferentCluster reports whether the Secret is confirmed to
// belong to a different cluster's registration — refusedSecretHandFix's cue
// to hand back an inspection instead of a delete, since a delete hint there
// would risk tearing down another, live cluster's ArgoCD registration.
func (g argocdSecretRemovalGuard) belongsToDifferentCluster() bool {
	return g.belongsTo != ""
}

// unverifiable reports whether the check that would have confirmed or ruled
// out belongsToDifferentCluster could not complete — the same reason to
// steer refusedSecretHandFix away from a delete hint, since a Secret this
// could not finish inspecting might just as easily belong to another
// cluster.
func (g argocdSecretRemovalGuard) unverifiable() bool {
	return g.verifyErr != nil
}

// adoptionWarning explains, when relevant, that this cluster's ArgoCD
// registration was inherited from 'argocd cluster add' rather than created by
// this tool — so deleting it here throws away a credential that command
// would have to re-mint, and nothing but re-running it restores one.
func adoptionWarning(cluster config.Cluster, argocdNamespace string) string {
	if !cluster.AdoptedRegistration {
		return ""
	}
	return fmt.Sprintf("%s/%s was adopted from 'argocd cluster add'; its credential cannot be "+
		"recovered except by re-running that command", argocdNamespace, cluster.SecretName)
}

// removeClusterConnection deletes the ClusterConnection, Getting first so a
// concurrent delete — or one that already happened — is reported as already
// gone rather than surfaced as an error, the same Get-before-Delete pattern
// Task 1's downstream removal functions use.
func removeClusterConnection(
	ctx context.Context,
	dyn dynamic.Interface,
	namespace, name string,
	dryRun bool,
) (downstream.RemovalOutcome, error) {
	res := dyn.Resource(inventory.GroupVersionResource).Namespace(namespace)

	if _, err := res.Get(ctx, name, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("getting clusterconnection %s/%s: %w", namespace, name, err)
	}
	if dryRun {
		return downstream.RemovedOutcome, nil
	}
	if err := res.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("deleting clusterconnection %s/%s: %w", namespace, name, err)
	}
	return downstream.RemovedOutcome, nil
}

// removeArgoCDSecret deletes the ArgoCD cluster Secret. The full ownership
// guard — including the "belongs to a different cluster" check — is the
// caller's job, evaluated up front via inspectArgoCDSecretForRemoval before
// the first delete runs; this function does not repeat that annotation
// check. It does independently re-check the managed-by label against the
// same Get it already does before deleting, rather than trusting the
// caller's earlier verdict unconditionally: a transient failure in the
// guard's read, or a change to the Secret between the two reads, must not be
// able to reach a delete with no check of its own left standing. This also
// means step 5's post-teardown recheck of this same Secret, which calls this
// function again, is not unguarded either.
func removeArgoCDSecret(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	dryRun bool,
) (downstream.RemovalOutcome, error) {
	existing, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("getting secret %s/%s: %w", namespace, name, err)
	}
	if existing.Labels[argocd.ManagedByLabel] != argocd.ManagedByValue {
		return downstream.NotOwnedOutcome, nil
	}
	if dryRun {
		return downstream.RemovedOutcome, nil
	}
	if err := client.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("deleting secret %s/%s: %w", namespace, name, err)
	}
	return downstream.RemovedOutcome, nil
}

// credentialSecretRemovalGuard is what inspecting this tool's own credential
// Secret found, for deciding whether remove may delete it. Mirrors
// argocdSecretRemovalGuard's role for the ArgoCD Secret: gathered by a read
// up front, before any delete runs. removeCredentialSecret independently
// re-checks the managed-by label at delete time too — see its own doc
// comment — so this verdict is the first check, not the only one.
type credentialSecretRemovalGuard struct {
	// notOwned is set when the Secret exists but lacks the managed-by label.
	notOwned bool

	// belongsTo is set when the Secret is ours by label but its cluster label
	// names a different cluster than the one being removed — the credential
	// Secret's half of the "belongs to a different connection" guard: an
	// annotation on the ArgoCD Secret, a label here, both written by the same
	// WriteCredentials call.
	belongsTo string

	// verifyErr is set when the Get here failed for a reason other than the
	// Secret being gone. Absence is not this: removeCredentialSecret re-reads
	// and reports that case on its own, and there is nothing to guard on a
	// Secret that was never seen. A real read failure — Forbidden, a
	// transient API error — must lean toward caution: refusing to act on a
	// Secret this could not inspect, rather than treating it as clear to
	// delete.
	verifyErr error
}

// inspectCredentialSecretForRemoval checks this tool's own credential Secret
// for the two ownership guards removeCredentialSecret used to run inline,
// interleaved with its own delete: the managed-by label, and the cluster
// label naming this cluster.
func inspectCredentialSecretForRemoval(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, clusterName string,
) credentialSecretRemovalGuard {
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Absence is not this guard's concern: removeCredentialSecret
			// re-reads and reports it on its own, the same way
			// inspectArgoCDSecretForRemoval leaves it to removeArgoCDSecret.
			return credentialSecretRemovalGuard{}
		}
		return credentialSecretRemovalGuard{verifyErr: err}
	}

	switch {
	case secret.Labels[argocd.ManagedByLabel] != argocd.ManagedByValue:
		return credentialSecretRemovalGuard{notOwned: true}
	case secret.Labels[credentialClusterLabel] != "" && secret.Labels[credentialClusterLabel] != clusterName:
		return credentialSecretRemovalGuard{belongsTo: secret.Labels[credentialClusterLabel]}
	default:
		return credentialSecretRemovalGuard{}
	}
}

// refusalReason names why remove must leave the credential Secret alone, or
// "" when it is clear to act.
func (g credentialSecretRemovalGuard) refusalReason() string {
	switch {
	case g.notOwned:
		return "not managed by k2a-token-sync"
	case g.verifyErr != nil:
		return fmt.Sprintf("could not verify which cluster it belongs to: %v", g.verifyErr)
	case g.belongsTo != "":
		return fmt.Sprintf("belongs to cluster %q, not this one", g.belongsTo)
	default:
		return ""
	}
}

// belongsToDifferentCluster is argocdSecretRemovalGuard.belongsToDifferentCluster's
// counterpart for the credential Secret.
func (g credentialSecretRemovalGuard) belongsToDifferentCluster() bool {
	return g.belongsTo != ""
}

// unverifiable is argocdSecretRemovalGuard.unverifiable's counterpart for the
// credential Secret.
func (g credentialSecretRemovalGuard) unverifiable() bool {
	return g.verifyErr != nil
}

// removeCredentialSecret deletes this tool's own credential Secret. The full
// ownership guard — including the "belongs to a different cluster" check —
// is the caller's job, evaluated up front via inspectCredentialSecretForRemoval
// before the first delete runs; this function does not repeat that cluster-label
// check. It does independently re-check the managed-by label against the
// same Get it already does before deleting, rather than trusting the
// caller's earlier verdict unconditionally — the same reasoning
// removeArgoCDSecret's doc comment explains.
func removeCredentialSecret(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	dryRun bool,
) (downstream.RemovalOutcome, error) {
	existing, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("getting secret %s/%s: %w", namespace, name, err)
	}
	if existing.Labels[argocd.ManagedByLabel] != argocd.ManagedByValue {
		return downstream.NotOwnedOutcome, nil
	}
	if dryRun {
		return downstream.RemovedOutcome, nil
	}
	if err := client.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return downstream.AbsentOutcome, nil
		}
		return downstream.AbsentOutcome, fmt.Errorf("deleting secret %s/%s: %w", namespace, name, err)
	}
	return downstream.RemovedOutcome, nil
}

// namedOutcome pairs one downstream object's display name with what happened
// to it and the kubectl invocation that finishes the job by hand, so a report
// line covering several objects (the bindings, the identities) can name a
// mixed result instead of flattening it into a claim that everything
// succeeded.
type namedOutcome struct {
	name    string
	outcome downstream.RemovalOutcome
	err     error
	kubectl string
}

// describeOutcome renders one object's outcome for the progress output,
// wording it as an action taken or a plan: --dry-run says "would delete" and
// "would leave alone" against a real run's "deleted" and "left alone".
func describeOutcome(name string, outcome downstream.RemovalOutcome, reason string, err error, dryRun bool) string {
	if err != nil {
		return fmt.Sprintf("failed: %s: %v", name, err)
	}
	switch outcome {
	case downstream.RemovedOutcome:
		if dryRun {
			return "would delete " + name
		}
		return "deleted " + name
	case downstream.AbsentOutcome:
		return name + " already gone"
	case downstream.NotOwnedOutcome:
		if reason == "" {
			reason = "not managed by k2a-token-sync"
		}
		verb := "left alone"
		if dryRun {
			verb = "would leave alone"
		}
		return fmt.Sprintf("%s: %s (%s)", verb, name, reason)
	default:
		return name
	}
}

// summarise renders a set of namedOutcomes sharing one report line, such as
// the downstream bindings or identities: everything deleted (or that would
// be) together in one clause, and anything left alone or failed named on its
// own, so a mixed result is never reported as if it fully succeeded.
func summarise(items []namedOutcome, dryRun bool) string {
	var deleted, other []string
	verb := "deleted"
	if dryRun {
		verb = "would delete"
	}
	for _, it := range items {
		switch {
		case it.err != nil:
			other = append(other, fmt.Sprintf("failed: %s: %v", it.name, it.err))
		case it.outcome == downstream.NotOwnedOutcome:
			skipVerb := "left alone"
			if dryRun {
				skipVerb = "would leave alone"
			}
			other = append(other, fmt.Sprintf("%s: %s (not managed by k2a-token-sync)", skipVerb, it.name))
		case it.outcome == downstream.AbsentOutcome:
			other = append(other, it.name+" (already gone)")
		default: // RemovedOutcome
			deleted = append(deleted, it.name)
		}
	}

	var parts []string
	if len(deleted) > 0 {
		parts = append(parts, verb+" "+strings.Join(deleted, ", "))
	}
	parts = append(parts, other...)
	if len(parts) == 0 {
		return "nothing to do"
	}
	return strings.Join(parts, "; ")
}

// refusedSecretHandFix builds the "how to finish this by hand" hint for an
// ArgoCD Secret or credential Secret remove refused to touch. A Secret that
// is simply not managed by k2a-token-sync at all gets a delete hint — the
// operator may reasonably want it gone. A Secret confirmed to belong to a
// different cluster, or one whose ownership could not be verified at all,
// gets a read-only inspection instead: following a delete hint there would
// risk tearing down another, live cluster's ArgoCD registration or
// credential, exactly what the guard just refused to let remove itself do.
func refusedSecretHandFix(namespace, name string, belongsToDifferentCluster, unverifiable bool) string {
	switch {
	case belongsToDifferentCluster:
		return fmt.Sprintf("belongs to a different cluster; leave it alone unless that is a mistake — "+
			"inspect first with: kubectl -n %s get secret %s -o yaml", namespace, name)
	case unverifiable:
		return fmt.Sprintf("its cluster ownership could not be verified; leave it alone unless that is a "+
			"mistake — inspect first with: kubectl -n %s get secret %s -o yaml", namespace, name)
	default:
		return fmt.Sprintf("kubectl -n %s delete secret %s", namespace, name)
	}
}

// remainingItem is one object a teardown could not remove: what it is, why,
// and the kubectl invocation that finishes the job by hand.
type remainingItem struct {
	what    string
	why     string
	kubectl string
}

// remainingFromOutcome turns one local object's outcome into a remainingItem
// when it was not fully handled — skipped or failed — or nil when nothing
// remains to report about it. Absent counts as handled: it is the goal state,
// not a failure.
//
// Every one of this package's guards now runs and reports its own reason
// before the first delete (see runRemove's doc comment), so by the time an
// outcome reaches here a NotOwnedOutcome has only one possible explanation —
// the caller never even had a specific reason to pass, unlike when this
// still took one.
func remainingFromOutcome(name string, outcome downstream.RemovalOutcome, err error, kubectl string) *remainingItem {
	switch {
	case err != nil:
		return &remainingItem{what: name, why: err.Error(), kubectl: kubectl}
	case outcome == downstream.NotOwnedOutcome:
		return &remainingItem{what: name, why: "not managed by k2a-token-sync", kubectl: kubectl}
	default:
		return nil
	}
}

// remainingFromNamed is remainingFromOutcome for a namedOutcome, which
// already carries its own kubectl invocation.
func remainingFromNamed(it namedOutcome) *remainingItem {
	return remainingFromOutcome(it.name, it.outcome, it.err, it.kubectl)
}
