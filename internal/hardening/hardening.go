// Package hardening builds and applies the optional name-restricted RBAC policy
// for Argo CD cluster Secrets.
package hardening

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

const (
	// managedByLabel and the companion component label mark restricted RBAC
	// objects this command may safely update on a later run.
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "k2a-token-sync"
	appNameLabel   = "app.kubernetes.io/name"
	componentLabel = "app.kubernetes.io/component"
	componentValue = "restricted-secret-rbac"
	fieldManager   = "k2a-token-sync-restrict-rbac"
	syncWave       = "argocd.argoproj.io/sync-wave"
)

// Names identifies the Helm baseline objects and the separate restricted
// objects maintained by the operator command.
type Names struct {
	Namespace           string
	ArgoCDNamespace     string
	ServiceAccount      string
	BaselineRole        string
	BaselineRoleBinding string
	RestrictedRole      string
	RestrictedBinding   string
}

// Connection is the reviewable part of one inventory entry.
type Connection struct {
	Name       string
	SecretName string
}

// Plan is a validated, deterministic allowlist.
type Plan struct {
	Connections []Connection
	SecretNames []string
}

// BuildPlan refuses any invalid or conflicting inventory entry. It resolves
// names through inventory.Client before this function is called, so defaults
// and runtime validation are exactly the same as for reconciliation.
func BuildPlan(entries []inventory.Entry) (Plan, error) {
	var invalid []string
	for _, entry := range entries {
		if entry.InvalidReason != "" {
			invalid = append(invalid, fmt.Sprintf("%s: %s", entry.Cluster.Name, entry.InvalidReason))
		}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return Plan{}, fmt.Errorf("the ClusterConnection inventory is not safe to restrict:\n  %s",
			strings.Join(invalid, "\n  "))
	}

	connections := make([]Connection, 0, len(entries))
	claimed := make(map[string]string, len(entries))
	for _, entry := range entries {
		name := entry.Cluster.SecretName
		if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
			return Plan{}, fmt.Errorf("ClusterConnection %q resolves to invalid Secret name %q: %s",
				entry.Cluster.Name, name, strings.Join(problems, ", "))
		}
		if other, exists := claimed[name]; exists {
			return Plan{}, fmt.Errorf("ClusterConnections %q and %q both resolve to Secret %q",
				other, entry.Cluster.Name, name)
		}
		claimed[name] = entry.Cluster.Name
		connections = append(connections, Connection{Name: entry.Cluster.Name, SecretName: name})
	}

	sort.Slice(connections, func(i, j int) bool { return connections[i].Name < connections[j].Name })
	secretNames := make([]string, 0, len(claimed))
	for name := range claimed {
		secretNames = append(secretNames, name)
	}
	sort.Strings(secretNames)
	return Plan{Connections: connections, SecretNames: secretNames}, nil
}

// RestrictedRole returns only patch permission for the exact desired names.
// An empty inventory deliberately produces no rules: an empty resourceNames
// field would not be a safe representation of deny-all.
func RestrictedRole(names Names, secretNames []string) *rbacv1.Role {
	rules := []rbacv1.PolicyRule{}
	if len(secretNames) > 0 {
		rules = []rbacv1.PolicyRule{{
			APIGroups:     []string{""},
			Resources:     []string{"secrets"},
			ResourceNames: append([]string(nil), secretNames...),
			Verbs:         []string{"patch"},
		}}
	}
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.RestrictedRole,
			Namespace: names.ArgoCDNamespace,
			Labels: map[string]string{
				managedByLabel: managedByValue,
				appNameLabel:   names.ServiceAccount,
				componentLabel: componentValue,
			},
			Annotations: map[string]string{syncWave: "-2"},
		},
		Rules: rules,
	}
}

// RestrictedRoleBinding binds the generated role to the controller identity.
func RestrictedRoleBinding(names Names) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.RestrictedBinding,
			Namespace: names.ArgoCDNamespace,
			Labels: map[string]string{
				managedByLabel: managedByValue,
				appNameLabel:   names.ServiceAccount,
				componentLabel: componentValue,
			},
			Annotations: map[string]string{syncWave: "-1"},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: names.RestrictedRole},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: names.ServiceAccount, Namespace: names.Namespace,
		}},
	}
}

// Inspect validates every live object whose identity the command relies on.
// The baseline Role may be in the chart's ordinary broad state or its durable
// restricted state, but no other rules are silently replaced.
func Inspect(ctx context.Context, client kubernetes.Interface, names Names) error {
	if names.Namespace == names.ArgoCDNamespace {
		return errors.New("the Argo CD namespace and k2a-token-sync namespace are the same; " +
			"the controller's same-namespace Role already has broad Secret access, so restriction would provide no isolation")
	}
	serviceAccount, err := client.CoreV1().ServiceAccounts(names.Namespace).
		Get(ctx, names.ServiceAccount, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading controller ServiceAccount %s/%s: %w",
			names.Namespace, names.ServiceAccount, err)
	}
	if !chartIdentity(serviceAccount.Labels, names.ServiceAccount) {
		return fmt.Errorf("ServiceAccount %s/%s does not have the expected Helm identity labels; refusing to bind an unrecognised subject",
			names.Namespace, names.ServiceAccount)
	}

	roles := client.RbacV1().Roles(names.ArgoCDNamespace)
	baseline, err := roles.Get(ctx, names.BaselineRole, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading baseline Role %s/%s: %w", names.ArgoCDNamespace, names.BaselineRole, err)
	}
	if !chartIdentity(baseline.Labels, names.ServiceAccount) || !baselineRulesExpected(baseline.Rules) {
		return fmt.Errorf("baseline Role %s/%s has unexpected identity labels or rules; refusing to replace RBAC not recognised as the chart's create/patch policy",
			names.ArgoCDNamespace, names.BaselineRole)
	}

	baselineBinding, err := client.RbacV1().RoleBindings(names.ArgoCDNamespace).
		Get(ctx, names.BaselineRoleBinding, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading baseline RoleBinding %s/%s: %w",
			names.ArgoCDNamespace, names.BaselineRoleBinding, err)
	}
	if !chartIdentity(baselineBinding.Labels, names.ServiceAccount) ||
		!bindingMatches(baselineBinding, names.BaselineRole, names) {
		return fmt.Errorf("baseline RoleBinding %s/%s has unexpected identity labels, roleRef, or subject; refusing to overwrite foreign RBAC",
			names.ArgoCDNamespace, names.BaselineRoleBinding)
	}

	if existing, getErr := roles.Get(ctx, names.RestrictedRole, metav1.GetOptions{}); getErr == nil {
		if !owned(existing.Labels) {
			return fmt.Errorf("restricted Role %s/%s already exists without k2a-token-sync ownership labels; refusing to overwrite it",
				names.ArgoCDNamespace, names.RestrictedRole)
		}
	} else if !apierrors.IsNotFound(getErr) {
		return fmt.Errorf("reading restricted Role %s/%s: %w", names.ArgoCDNamespace, names.RestrictedRole, getErr)
	}

	if existing, getErr := client.RbacV1().RoleBindings(names.ArgoCDNamespace).
		Get(ctx, names.RestrictedBinding, metav1.GetOptions{}); getErr == nil {
		if !owned(existing.Labels) || !bindingMatches(existing, names.RestrictedRole, names) {
			return fmt.Errorf("restricted RoleBinding %s/%s has unexpected ownership, roleRef, or subjects; refusing to overwrite it",
				names.ArgoCDNamespace, names.RestrictedBinding)
		}
	} else if !apierrors.IsNotFound(getErr) {
		return fmt.Errorf("reading restricted RoleBinding %s/%s: %w",
			names.ArgoCDNamespace, names.RestrictedBinding, getErr)
	}
	return nil
}

func owned(labels map[string]string) bool {
	return labels[managedByLabel] == managedByValue && labels[componentLabel] == componentValue
}

func chartIdentity(labels map[string]string, name string) bool {
	return labels[managedByLabel] == "Helm" && labels[appNameLabel] == name
}

func baselineRulesExpected(rules []rbacv1.PolicyRule) bool {
	return reflect.DeepEqual(rules, baselineRules(true)) || reflect.DeepEqual(rules, baselineRules(false))
}

func baselineRules(broad bool) []rbacv1.PolicyRule {
	verbs := []string{"create"}
	if broad {
		verbs = append(verbs, "patch")
	}
	return []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: verbs}}
}

func bindingMatches(binding *rbacv1.RoleBinding, role string, names Names) bool {
	return binding.RoleRef == (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role}) &&
		reflect.DeepEqual(binding.Subjects, []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: names.ServiceAccount, Namespace: names.Namespace,
		}})
}

// Apply installs the restricted permission before removing broad patch from
// the baseline Role. Every update preserves metadata fields this command does
// not own and uses a dedicated field manager.
func Apply(ctx context.Context, client kubernetes.Interface, names Names, secretNames []string) error {
	if err := Inspect(ctx, client, names); err != nil {
		return err
	}

	desiredRole := RestrictedRole(names, secretNames)
	if err := upsertRole(ctx, client, desiredRole); err != nil {
		return err
	}
	desiredBinding := RestrictedRoleBinding(names)
	if err := upsertBinding(ctx, client, desiredBinding); err != nil {
		return err
	}

	roles := client.RbacV1().Roles(names.ArgoCDNamespace)
	baseline, err := roles.Get(ctx, names.BaselineRole, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("re-reading baseline Role before restricting it: %w", err)
	}
	baseline.Rules = baselineRules(false)
	if _, err := roles.Update(ctx, baseline, metav1.UpdateOptions{FieldManager: fieldManager}); err != nil {
		return fmt.Errorf("removing namespace-wide patch from baseline Role %s/%s: %w",
			names.ArgoCDNamespace, names.BaselineRole, err)
	}

	return verifyObjects(ctx, client, names, desiredRole)
}

func upsertRole(ctx context.Context, client kubernetes.Interface, desired *rbacv1.Role) error {
	roles := client.RbacV1().Roles(desired.Namespace)
	existing, err := roles.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := roles.Create(ctx, desired, metav1.CreateOptions{FieldManager: fieldManager}); err != nil {
			return fmt.Errorf("creating restricted Role %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading restricted Role %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	existing.Labels = merge(existing.Labels, desired.Labels)
	existing.Annotations = merge(existing.Annotations, desired.Annotations)
	existing.Rules = desired.Rules
	if _, err := roles.Update(ctx, existing, metav1.UpdateOptions{FieldManager: fieldManager}); err != nil {
		return fmt.Errorf("updating restricted Role %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func upsertBinding(ctx context.Context, client kubernetes.Interface, desired *rbacv1.RoleBinding) error {
	bindings := client.RbacV1().RoleBindings(desired.Namespace)
	existing, err := bindings.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{FieldManager: fieldManager}); err != nil {
			return fmt.Errorf("creating restricted RoleBinding %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading restricted RoleBinding %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	existing.Labels = merge(existing.Labels, desired.Labels)
	existing.Annotations = merge(existing.Annotations, desired.Annotations)
	existing.RoleRef = desired.RoleRef
	existing.Subjects = desired.Subjects
	if _, err := bindings.Update(ctx, existing, metav1.UpdateOptions{FieldManager: fieldManager}); err != nil {
		return fmt.Errorf("updating restricted RoleBinding %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func merge(into, from map[string]string) map[string]string {
	if into == nil {
		into = make(map[string]string, len(from))
	}
	for key, value := range from {
		into[key] = value
	}
	return into
}

func verifyObjects(ctx context.Context, client kubernetes.Interface, names Names, role *rbacv1.Role) error {
	actualRole, err := client.RbacV1().Roles(names.ArgoCDNamespace).Get(ctx, names.RestrictedRole, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading back restricted Role: %w", err)
	}
	if !owned(actualRole.Labels) || !reflect.DeepEqual(actualRole.Rules, role.Rules) {
		return errors.New("restricted Role read-back differs from the intended policy")
	}
	actualBinding, err := client.RbacV1().RoleBindings(names.ArgoCDNamespace).
		Get(ctx, names.RestrictedBinding, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading back restricted RoleBinding: %w", err)
	}
	if !owned(actualBinding.Labels) || !bindingMatches(actualBinding, names.RestrictedRole, names) {
		return errors.New("restricted RoleBinding read-back differs from the intended policy")
	}
	baseline, err := client.RbacV1().Roles(names.ArgoCDNamespace).Get(ctx, names.BaselineRole, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading back baseline Role: %w", err)
	}
	if !reflect.DeepEqual(baseline.Rules, baselineRules(false)) {
		return errors.New("baseline Role read-back still contains unexpected permission")
	}
	return nil
}

// AuthorizationResult reports whether effective authorization was verified or
// skipped because the CLI caller may not create SubjectAccessReviews.
type AuthorizationResult struct {
	Skipped string
	Checks  int
}

// VerifyAuthorization checks every allowed name, one excluded name, and
// namespace-wide create as the controller ServiceAccount rather than the CLI
// caller.
func VerifyAuthorization(ctx context.Context, client kubernetes.Interface, names Names, secretNames []string) (AuthorizationResult, error) {
	user := fmt.Sprintf("system:serviceaccount:%s:%s", names.Namespace, names.ServiceAccount)
	type accessCheck struct {
		verb, name string
		want       bool
	}
	checks := make([]accessCheck, 0, len(secretNames)+2)
	for _, name := range secretNames {
		checks = append(checks, accessCheck{verb: "patch", name: name, want: true})
	}
	checks = append(checks,
		accessCheck{verb: "patch", name: excludedName(secretNames), want: false},
		accessCheck{verb: "create", want: true},
	)

	result := AuthorizationResult{}
	for _, check := range checks {
		review, err := client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User: user,
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: names.ArgoCDNamespace,
					Verb:      check.verb,
					Resource:  "secrets",
					Name:      check.name,
				},
			},
		}, metav1.CreateOptions{FieldManager: fieldManager})
		if apierrors.IsForbidden(err) {
			result.Skipped = "the CLI caller is not allowed to create SubjectAccessReviews"
			return result, nil
		}
		if err != nil {
			return result, fmt.Errorf("checking %s access to Secret %q: %w", check.verb, check.name, err)
		}
		result.Checks++
		if review.Status.EvaluationError != "" {
			return result, fmt.Errorf("authorizer could not evaluate %s access to Secret %q: %s",
				check.verb, check.name, review.Status.EvaluationError)
		}
		if review.Status.Allowed != check.want {
			want := "denied"
			if check.want {
				want = "allowed"
			}
			actual := "denied"
			if review.Status.Allowed {
				actual = "allowed"
			}
			return result, fmt.Errorf("ServiceAccount %s/%s authorization to %s Secret %q is %s, expected %s: %s",
				names.Namespace, names.ServiceAccount, check.verb,
				check.name, actual, want, review.Status.Reason)
		}
	}
	return result, nil
}

func excludedName(names []string) string {
	claimed := make(map[string]struct{}, len(names))
	for _, name := range names {
		claimed[name] = struct{}{}
	}
	base := "cluster-k2a-token-sync-rbac-excluded"
	for i := 0; ; i++ {
		candidate := base
		if i > 0 {
			candidate = fmt.Sprintf("%s-%d", base, i)
		}
		if _, exists := claimed[candidate]; !exists {
			return candidate
		}
	}
}
