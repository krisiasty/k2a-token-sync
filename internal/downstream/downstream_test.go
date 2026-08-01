package downstream

import (
	"context"
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

	repairs, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
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
	repairs, err = EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
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

	if _, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager"); err != nil {
		t.Fatalf("setup returned unexpected error: %v", err)
	}

	// Someone deletes the binding but leaves the account: the token ArgoCD holds
	// still authenticates, it has simply lost its permissions.
	if err := client.RbacV1().ClusterRoleBindings().
		Delete(ctx, "argocd-manager-role-binding", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the binding: %v", err)
	}

	repairs, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
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

	repairs, err = EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
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
	if !strings.Contains(err.Error(), "already binds") {
		t.Errorf("error = %q, want it to explain the conflicting role", err)
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

	if err := EnsureSelfIdentity(ctx, client, "kube-system", "k2a-token-sync"); err != nil {
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
	if err := EnsureSelfIdentity(ctx, client, "kube-system", "k2a-token-sync"); err != nil {
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
