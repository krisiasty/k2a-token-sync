package downstream

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestEnsureArgoCDIdentityIsIdempotent(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ctx := context.Background()

	repairs, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager", clusterAdminRole)
	if err != nil {
		t.Fatalf("first call returned unexpected error: %v", err)
	}
	if !repairs.ServiceAccount || !repairs.Binding {
		t.Errorf("first call reported %+v, want both created", repairs)
	}

	sa, err := client.CoreV1().ServiceAccounts("kube-system").Get(ctx, "argocd-manager", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("serviceaccount was not created: %v", err)
	}
	if sa.Labels["app.kubernetes.io/managed-by"] != "k2a-token-sync" {
		t.Error("serviceaccount is not labelled as managed by this tool")
	}

	binding, err := client.RbacV1().ClusterRoleBindings().Get(ctx, "argocd-manager-role-binding", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("clusterrolebinding was not created: %v", err)
	}
	if binding.RoleRef.Name != clusterAdminRole {
		t.Errorf("bound to %q, want %q", binding.RoleRef.Name, clusterAdminRole)
	}

	// A second pass must be a no-op, and this now runs on every single pass rather
	// than only when a credential is due — so a spurious "repaired" here would
	// reissue ArgoCD's credential every five minutes, forever.
	repairs, err = EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager", clusterAdminRole)
	if err != nil {
		t.Fatalf("second call returned unexpected error: %v", err)
	}
	if repairs.Any() {
		t.Errorf("second call reported %+v, want a no-op", repairs)
	}
}

// The two halves are reported separately because they mean different things to
// the caller: a recreated ServiceAccount invalidates ArgoCD's token and forces a
// reissue, a recreated binding does not.
func TestEnsureArgoCDIdentityDistinguishesWhatItRepaired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := fake.NewSimpleClientset()

	if _, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager", clusterAdminRole); err != nil {
		t.Fatalf("setup returned unexpected error: %v", err)
	}

	// Someone deletes the binding but leaves the account: the token ArgoCD holds
	// still authenticates, it has simply lost its permissions.
	if err := client.RbacV1().ClusterRoleBindings().
		Delete(ctx, "argocd-manager-role-binding", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the binding: %v", err)
	}

	repairs, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager", clusterAdminRole)
	if err != nil {
		t.Fatalf("EnsureArgoCDIdentity returned unexpected error: %v", err)
	}
	if repairs.ServiceAccount {
		t.Error("reported the serviceaccount as recreated when only the binding was missing")
	}
	if !repairs.Binding {
		t.Error("did not report the binding as recreated")
	}

	// And the other way round: the account goes, the binding stays. Every token
	// ever issued for the old account is now dead.
	if err := client.CoreV1().ServiceAccounts("kube-system").
		Delete(ctx, "argocd-manager", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the serviceaccount: %v", err)
	}

	repairs, err = EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager", clusterAdminRole)
	if err != nil {
		t.Fatalf("EnsureArgoCDIdentity returned unexpected error: %v", err)
	}
	if !repairs.ServiceAccount {
		t.Error("did not report the serviceaccount as recreated")
	}
	if repairs.Binding {
		t.Error("reported the binding as recreated when it was still there")
	}
}

func TestEnsureClusterRoleBindingRefusesToRewriteForeignBinding(t *testing.T) {
	t.Parallel()

	// Silently rewriting an existing binding would be an unannounced privilege
	// change, so k2a-token-sync must refuse and surface it.
	client := fake.NewSimpleClientset(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "view"},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: "argocd-manager", Namespace: "kube-system",
		}},
	})

	_, err := EnsureClusterRoleBinding(context.Background(), client,
		"argocd-manager-role-binding", clusterAdminRole, "kube-system", "argocd-manager")
	if err == nil {
		t.Fatal("EnsureClusterRoleBinding overwrote a binding pointing at another role")
	}
	// The message is what an operator acts on, so it has to name both roles and
	// the remedy. "Resolve manually" was true and useless: nothing in it said
	// that a roleRef cannot be edited, or that k2a-token-sync holds bind on the
	// old role only, which together are why bootstrap is the one thing that can
	// finish the job.
	if !errors.Is(err, ErrRoleRefImmutable) {
		t.Errorf("error = %v, want it to wrap ErrRoleRefImmutable so the caller can name a condition reason", err)
	}
	for _, want := range []string{"view", clusterAdminRole, "immutable", "--replace-binding"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestEnsureClusterRoleBindingRefusesBindingWithoutOurSubject(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: clusterAdminRole},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: "someone-else", Namespace: "kube-system",
		}},
	})

	_, err := EnsureClusterRoleBinding(context.Background(), client,
		"argocd-manager-role-binding", clusterAdminRole, "kube-system", "argocd-manager")
	if err == nil {
		t.Fatal("EnsureClusterRoleBinding accepted a binding that omits our subject")
	}
	if !strings.Contains(err.Error(), "does not include serviceaccount") {
		t.Errorf("error = %q, want it to name the missing subject", err)
	}
}

func TestMintTokenUsesServerReportedExpiry(t *testing.T) {
	t.Parallel()

	// The API server caps token lifetime via --service-account-max-token-expiration,
	// so the granted expiry — not the requested TTL — must drive scheduling.
	granted := time.Now().Add(1 * time.Hour).UTC().Truncate(time.Second)

	var requested int64
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok || create.GetSubresource() != "token" {
			return false, nil, nil
		}
		// The request itself is inspected, not just answered. A reactor that only
		// returns a canned response accepts any request, which is how a token asking
		// for expirationSeconds: 0 reached a real API server.
		if req, isRequest := create.GetObject().(*authenticationv1.TokenRequest); isRequest {
			if req.Spec.ExpirationSeconds != nil {
				requested = *req.Spec.ExpirationSeconds
			}
		}
		return true, &authenticationv1.TokenRequest{
			Status: authenticationv1.TokenRequestStatus{
				Token:               "minted-token",
				ExpirationTimestamp: metav1.NewTime(granted),
			},
		}, nil
	})

	token, err := MintToken(context.Background(), client, "kube-system", "argocd-manager", 720*time.Hour)
	if err != nil {
		t.Fatalf("MintToken returned unexpected error: %v", err)
	}
	if token.Value != "minted-token" {
		t.Errorf("token value = %q", token.Value)
	}
	if !token.ExpiresAt.Equal(granted) {
		t.Errorf("ExpiresAt = %v, want the server-reported %v", token.ExpiresAt, granted)
	}
	if want := int64((720 * time.Hour).Seconds()); requested != want {
		t.Errorf("requested expirationSeconds = %d, want %d", requested, want)
	}
}

func TestMintTokenRefusesAnUnsetLifetime(t *testing.T) {
	t.Parallel()

	// A zero TTL is a caller that forgot to set one. The API server's own complaint
	// — "may not specify a duration less than 10 minutes" — reads like a cluster
	// misconfiguration, so it is caught here and named for what it is.
	_, err := MintToken(context.Background(), fake.NewSimpleClientset(), "kube-system", "argocd-manager", 0)
	if err == nil {
		t.Fatal("MintToken accepted a zero lifetime")
	}
	if !strings.Contains(err.Error(), "never set") {
		t.Errorf("the error does not say the lifetime was unset: %v", err)
	}
}

func TestClusterCA(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: rootCAConfigMap, Namespace: "kube-system"},
		Data:       map[string]string{rootCAKey: "pem-bundle"},
	})

	ca, err := ClusterCA(context.Background(), client, "kube-system")
	if err != nil {
		t.Fatalf("ClusterCA returned unexpected error: %v", err)
	}
	if string(ca) != "pem-bundle" {
		t.Errorf("ClusterCA = %q", ca)
	}

	if _, err := ClusterCA(context.Background(), client, "other"); err == nil {
		t.Error("ClusterCA succeeded for a namespace without the configmap")
	}
}

func TestEnsureSelfIdentityGrantsOnlyWhatIsNeeded(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ctx := context.Background()

	if err := EnsureSelfIdentity(ctx, client, "kube-system", "k2a-token-sync", clusterAdminRole); err != nil {
		t.Fatalf("EnsureSelfIdentity returned unexpected error: %v", err)
	}

	role, err := client.RbacV1().ClusterRoles().Get(ctx, "k2a-token-sync", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("clusterrole was not created: %v", err)
	}

	// k2a-token-sync must never hold blanket secret access in a downstream cluster.
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource == "secrets" {
				t.Error("the self clusterrole grants access to secrets")
			}
			if resource == "*" {
				t.Error("the self clusterrole uses a wildcard resource")
			}
		}
		for _, verb := range rule.Verbs {
			if verb == "*" {
				t.Error("the self clusterrole uses a wildcard verb")
			}
		}
	}

	// Idempotent: this runs on the bootstrap path and must tolerate re-runs.
	if err := EnsureSelfIdentity(ctx, client, "kube-system", "k2a-token-sync", clusterAdminRole); err != nil {
		t.Fatalf("second EnsureSelfIdentity returned unexpected error: %v", err)
	}
}

// A freshly minted token is proof of nothing until it is used. This is the one
// call that exercises endpoint, CA, token and RBAC together, so what it reports
// has to be exact: an unauthenticated token must surface as an error, and an
// authenticated but unauthorised one must not.
func TestCanActAsClusterAdmin(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		allowed     bool
		apiErr      error
		want        bool
		wantErr     bool
		errContains string
	}{
		{
			name:    "cluster-admin",
			allowed: true,
			want:    true,
		},
		{
			// The dangling-binding case: the token is real and the API server
			// accepted it, but the role behind it grants nothing.
			name:    "authenticated but not authorised",
			allowed: false,
			want:    false,
		},
		{
			// A rejected token is not a false answer, it is no answer.
			name:        "token refused",
			apiErr:      apierrors.NewUnauthorized("Unauthorized"),
			wantErr:     true,
			errContains: "what this credential may do",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := fake.NewSimpleClientset()
			client.PrependReactor("create", "selfsubjectaccessreviews",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					if tc.apiErr != nil {
						return true, nil, tc.apiErr
					}
					return true, &authorizationv1.SelfSubjectAccessReview{
						Status: authorizationv1.SubjectAccessReviewStatus{Allowed: tc.allowed},
					}, nil
				})

			got, err := CanActAsClusterAdmin(context.Background(), client)
			if tc.wantErr {
				if err == nil {
					t.Fatal("CanActAsClusterAdmin accepted a refused token")
				}
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error does not say what was being attempted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanActAsClusterAdmin returned unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("allowed = %v, want %v", got, tc.want)
			}
		})
	}
}

// The review must ask about everything, since that is the permission ArgoCD's
// identity is supposed to hold and the one a dangling binding no longer grants.
func TestCanActAsClusterAdminAsksTheBroadestQuestion(t *testing.T) {
	t.Parallel()

	var asked *authorizationv1.ResourceAttributes
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(k8stesting.CreateAction)
			if !ok {
				return false, nil, nil
			}
			if review, isReview := create.GetObject().(*authorizationv1.SelfSubjectAccessReview); isReview {
				asked = review.Spec.ResourceAttributes
			}
			return true, &authorizationv1.SelfSubjectAccessReview{
				Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
			}, nil
		})

	if _, err := CanActAsClusterAdmin(context.Background(), client); err != nil {
		t.Fatalf("CanActAsClusterAdmin returned unexpected error: %v", err)
	}
	if asked == nil {
		t.Fatal("no resource attributes were sent")
	}
	if asked.Verb != "*" || asked.Group != "*" || asked.Resource != "*" {
		t.Errorf("asked about %+v, want every verb on every resource in every group", asked)
	}
}

// Creating ArgoCD's binding needs 'bind' on the role it references, not just
// permission to create bindings: Kubernetes refuses any binding granting
// permissions the creator does not hold. Without this rule, restoring a deleted
// argocd-manager binding fails on every pass with "attempting to grant RBAC
// permissions not currently held" — which is what it did, in every released
// version, because a fake clientset does not run the RBAC admission plugin and
// so every test of that path passed against a create a real API server refuses.
//
// This cannot reproduce that: it asserts the rule is present and correctly
// scoped, and the behaviour itself is verified against a live cluster.
func TestSelfRulesCanBindTheRoleItHasToRestore(t *testing.T) {
	t.Parallel()

	var bind *rbacv1.PolicyRule
	rules := selfRules(clusterAdminRole)
	for i, rule := range rules {
		if slices.Contains(rule.Resources, "clusterroles") && slices.Contains(rule.Verbs, "bind") {
			bind = &rules[i]
			break
		}
	}
	if bind == nil {
		t.Fatal("selfRules grants no 'bind' on clusterroles, so restoring ArgoCD's binding " +
			"is refused by the API server on every pass")
	}

	// Not decoration. 'bind' without resourceNames would let this identity grant
	// any role in the cluster to any subject — a materially larger power than
	// re-pointing the one role it already manages, and the difference between a
	// repair and an escalation primitive.
	if len(bind.ResourceNames) == 0 {
		t.Error("the bind rule has no resourceNames, so it permits binding every role in the cluster")
	}
	if !slices.Contains(bind.ResourceNames, clusterAdminRole) {
		t.Errorf("bind rule resourceNames = %v, which does not include %q — the role "+
			"EnsureArgoCDIdentity actually binds", bind.ResourceNames, clusterAdminRole)
	}
	if !slices.Contains(bind.APIGroups, rbacv1.GroupName) {
		t.Errorf("bind rule apiGroups = %v, want %q", bind.APIGroups, rbacv1.GroupName)
	}
}

// A connection may name a role narrower than cluster-admin, and the bind grant
// has to follow it. Pinned to cluster-admin, a scoped registration would lose
// the self-heal #57 restored: the identity could create no binding at all for
// the role it was actually asked to bind, and would fail on every pass exactly
// as it did before that fix.
func TestSelfRulesBindFollowsTheConfiguredRole(t *testing.T) {
	t.Parallel()

	for _, role := range []string{clusterAdminRole, "argocd-restricted", "some.group:reader"} {
		var found bool
		for _, rule := range selfRules(role) {
			if slices.Contains(rule.Verbs, "bind") {
				found = true
				if !slices.Contains(rule.ResourceNames, role) {
					t.Errorf("selfRules(%q): bind resourceNames = %v, which does not name it",
						role, rule.ResourceNames)
				}
				if len(rule.ResourceNames) != 1 {
					t.Errorf("selfRules(%q): bind names %d roles, want exactly the one it binds",
						role, len(rule.ResourceNames))
				}
			}
		}
		if !found {
			t.Errorf("selfRules(%q) has no bind rule", role)
		}
	}
}

// Every configuration path defaults the role, so an empty one means a caller
// left the field unset. Binding to "" would create a binding granting nothing,
// and the failure would surface later as ArgoCD getting 403s against a
// registration that still looks healthy — so it is refused where the bug is,
// the way MintToken refuses a zero lifetime.
func TestAnUnsetClusterRoleIsRefusedRatherThanBoundToNothing(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	if _, err := EnsureArgoCDIdentity(t.Context(), client, "kube-system", "argocd-manager", ""); err == nil {
		t.Error("EnsureArgoCDIdentity bound ArgoCD's identity to an empty ClusterRole")
	}
	if err := EnsureSelfIdentity(t.Context(), client, "kube-system", "k2a-token-sync", ""); err == nil {
		t.Error("EnsureSelfIdentity provisioned a bind rule naming no role")
	}
}
