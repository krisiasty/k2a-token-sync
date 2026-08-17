package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"github.com/krisiasty/k2a-token-sync/internal/hardening"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
	kubeclient "github.com/krisiasty/k2a-token-sync/internal/k8s"
)

const restrictRBACTimeout = 2 * time.Minute

type restrictRBACParams struct {
	names       hardening.Names
	cluster     string
	confirm     bool
	dryRun      bool
	printOnly   bool
	input       io.Reader
	progress    io.Writer
	manifestOut io.Writer
}

// runRestrictRBAC narrows patch permission to the exact Secrets resolved from
// the live ClusterConnection inventory. It always uses the operator's
// kubeconfig; the controller is never granted permission to alter its own RBAC.
func runRestrictRBAC(args []string) error {
	fs := flag.NewFlagSet("restrict-rbac", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		kubeconfig        = fs.String("kubeconfig", "", "path to the kubeconfig for the cluster running ArgoCD and k2a-token-sync (default: normal resolution)")
		kubeContext       = fs.String("context", "", "context of the cluster running ArgoCD and k2a-token-sync (default: current context)")
		namespace         = fs.String("namespace", "k2a-token-sync", "namespace holding the ClusterConnections and controller ServiceAccount")
		argocdNamespace   = fs.String("argocd-namespace", "argocd", "namespace holding ArgoCD's cluster Secrets")
		serviceAccount    = fs.String("serviceaccount", "k2a-token-sync", "controller ServiceAccount to bind")
		baselineRole      = fs.String("baseline-role", "k2a-token-sync", "Helm-managed ArgoCD-namespace Role whose broad patch permission is removed")
		baselineBinding   = fs.String("baseline-rolebinding", "k2a-token-sync", "Helm-managed RoleBinding used to validate the controller identity")
		restrictedRole    = fs.String("role", "k2a-token-sync-restricted", "generated Role containing the Secret-name patch allowlist")
		restrictedBinding = fs.String("rolebinding", "k2a-token-sync-restricted", "generated RoleBinding for the restricted Role")
		confirm           = fs.Bool("confirm", false, "apply without an interactive confirmation prompt")
		dryRun            = fs.Bool("dry-run", false, "discover and validate the inventory and show the change without writing it")
		printOnly         = fs.Bool("print", false, "write deterministic restricted Role and RoleBinding YAML to stdout without changing RBAC")
	)

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: k2a-token-sync restrict-rbac [flags]

Restricts k2a-token-sync's patch permission in the ArgoCD namespace to the
exact Secret names resolved from every ClusterConnection. The generated patch
Role and RoleBinding are installed first; only then is namespace-wide patch
removed from the Helm baseline Role. Namespace-wide create remains because
Kubernetes RBAC cannot restrict create by resourceNames and deleted managed
Secrets must remain self-healing.

The command refuses malformed or conflicting inventory, unexpected live RBAC,
and deployments where ArgoCD and k2a-token-sync share a namespace. It reads no
ArgoCD Secret. Run it again after adding or removing a ClusterConnection or
changing spec.secretName.

Confirmation is interactive by default. --confirm is for non-interactive use,
--dry-run validates and previews without writes, and --print emits the generated
Role and RoleBinding for a GitOps repository. Set restrictedRBAC.enabled=true in
the Helm release as part of activation so upgrades cannot restore broad patch.

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
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *dryRun && *printOnly {
		return errors.New("--dry-run and --print are separate preview modes and cannot be combined")
	}
	if *confirm && (*dryRun || *printOnly) {
		return errors.New("--confirm only applies to a write; it cannot be combined with --dry-run or --print")
	}

	names := hardening.Names{
		Namespace:           *namespace,
		ArgoCDNamespace:     *argocdNamespace,
		ServiceAccount:      *serviceAccount,
		BaselineRole:        *baselineRole,
		BaselineRoleBinding: *baselineBinding,
		RestrictedRole:      *restrictedRole,
		RestrictedBinding:   *restrictedBinding,
	}
	if err := validateRBACNames(names); err != nil {
		return err
	}
	if names.Namespace == names.ArgoCDNamespace {
		return errors.New("--namespace and --argocd-namespace must differ: the same-namespace Role already has broad Secret access, so restricted mode would not reduce the controller's permissions")
	}

	ctx, cancel := context.WithTimeout(context.Background(), restrictRBACTimeout)
	defer cancel()
	client, cfg, err := localClientFor(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}
	dyn, err := kubeclient.DynamicClientForContext(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}
	return executeRestriction(ctx, client, dyn, restrictRBACParams{
		names:       names,
		cluster:     describeCluster(*kubeContext, cfg.Host),
		confirm:     *confirm,
		dryRun:      *dryRun,
		printOnly:   *printOnly,
		input:       os.Stdin,
		progress:    os.Stderr,
		manifestOut: os.Stdout,
	})
}

func validateRBACNames(names hardening.Names) error {
	values := []struct{ field, value string }{
		{"--namespace", names.Namespace},
		{"--argocd-namespace", names.ArgoCDNamespace},
		{"--serviceaccount", names.ServiceAccount},
		{"--baseline-role", names.BaselineRole},
		{"--baseline-rolebinding", names.BaselineRoleBinding},
		{"--role", names.RestrictedRole},
		{"--rolebinding", names.RestrictedBinding},
	}
	for _, value := range values {
		if value.value == "" {
			return fmt.Errorf("%s must not be empty", value.field)
		}
		if problems := validation.IsDNS1123Subdomain(value.value); len(problems) > 0 {
			return fmt.Errorf("%s %q is invalid: %s", value.field, value.value, strings.Join(problems, ", "))
		}
	}
	if names.BaselineRole == names.RestrictedRole {
		return errors.New("--baseline-role and --role must name different Roles")
	}
	if names.BaselineRoleBinding == names.RestrictedBinding {
		return errors.New("--baseline-rolebinding and --rolebinding must name different RoleBindings")
	}
	return nil
}

func executeRestriction(
	ctx context.Context,
	client kubernetes.Interface,
	dyn dynamic.Interface,
	params restrictRBACParams,
) error {
	out := &steps{w: params.progress}
	entries, err := inventory.NewClient(dyn, params.names.Namespace).List(ctx)
	if err != nil {
		return err
	}
	plan, err := hardening.BuildPlan(entries)
	if err != nil {
		return err
	}
	if err := hardening.Inspect(ctx, client, params.names); err != nil {
		return err
	}

	out.headingf("Restricting ArgoCD Secret RBAC — %s", params.cluster)
	out.stepf("k2a-token-sync", "%s (ServiceAccount %s)", params.names.Namespace, params.names.ServiceAccount)
	out.stepf("ArgoCD namespace", "%s", params.names.ArgoCDNamespace)
	out.stepf("baseline RBAC", "%s, %s", params.names.BaselineRole, params.names.BaselineRoleBinding)
	out.stepf("restricted RBAC", "%s, %s", params.names.RestrictedRole, params.names.RestrictedBinding)
	out.stepf("configured clusters", "%d", len(plan.Connections))
	for _, connection := range plan.Connections {
		out.stepf("  "+connection.Name, "%s", connection.SecretName)
	}
	out.stepf("patch allowlist", "%d Secret(s)", len(plan.SecretNames))
	for _, name := range plan.SecretNames {
		out.stepf("  Secret", "%s", name)
	}
	if len(plan.SecretNames) == 0 {
		out.warnf("the inventory is empty; the restricted Role will contain no patch rule")
	}
	out.warnf("namespace-wide Secret create remains enabled so deleted managed Secrets can be recreated")
	out.warnf("rerun this command after adding, renaming, or removing a ClusterConnection")
	out.warnf("enable restrictedRBAC.enabled in Helm or GitOps so a later reconciliation cannot restore broad patch")

	if params.printOnly {
		raw, renderErr := renderRestrictedManifest(params.names, plan.SecretNames)
		if renderErr != nil {
			return renderErr
		}
		out.blank()
		out.notef("No RBAC was changed. Commit the manifest on stdout together with restrictedRBAC.enabled=true.")
		_, err = params.manifestOut.Write(raw)
		if err != nil {
			return fmt.Errorf("writing restricted RBAC manifest: %w", err)
		}
		return nil
	}
	if params.dryRun {
		out.blank()
		out.notef("Nothing was changed. The restricted Role would be installed before broad patch is removed.")
		return nil
	}
	if !params.confirm {
		ok, confirmErr := confirmRestriction(params.input, params.progress)
		if confirmErr != nil {
			return confirmErr
		}
		if !ok {
			return errors.New("confirmation not received; nothing was changed")
		}
	}

	if err := hardening.Apply(ctx, client, params.names, plan.SecretNames); err != nil {
		return err
	}
	out.blank()
	out.stepf("restricted Role", "applied before removing broad patch")
	out.stepf("baseline Role", "namespace-wide patch removed; namespace-wide create retained")

	verification, err := hardening.VerifyAuthorization(ctx, client, params.names, plan.SecretNames)
	if err != nil {
		return fmt.Errorf("the RBAC objects were updated, but effective authorization verification failed: %w", err)
	}
	if verification.Skipped != "" {
		out.warnf("authorization verification skipped: %s", verification.Skipped)
	} else {
		out.stepf("authorization", "%d SubjectAccessReview checks passed", verification.Checks)
	}
	out.notef("Done. Patch access is restricted to the listed Secret names.")
	return nil
}

func confirmRestriction(input io.Reader, output io.Writer) (bool, error) {
	if _, err := fmt.Fprint(output, "\nType 'yes' to apply this RBAC restriction: "); err != nil {
		return false, fmt.Errorf("writing confirmation prompt: %w", err)
	}
	line, err := bufio.NewReader(input).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("reading confirmation: %w", err)
	}
	return strings.TrimSpace(line) == "yes", nil
}

func renderRestrictedManifest(names hardening.Names, secretNames []string) ([]byte, error) {
	objects := []any{
		hardening.RestrictedRole(names, secretNames),
		hardening.RestrictedRoleBinding(names),
	}
	var out bytes.Buffer
	for i, object := range objects {
		if i > 0 {
			out.WriteString("---\n")
		}
		raw, err := yaml.Marshal(object)
		if err != nil {
			return nil, fmt.Errorf("encoding restricted RBAC object %d: %w", i+1, err)
		}
		out.Write(raw)
	}
	return out.Bytes(), nil
}
