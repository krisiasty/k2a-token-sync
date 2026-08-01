package downstream

import (
	"context"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestEnsureArgoCDIdentityIsIdempotent(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ctx := context.Background()

	changed, err := EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
	if err != nil {
		t.Fatalf("first call returned unexpected error: %v", err)
	}
	if !changed {
		t.Error("first call reported no change, want the identity to be created")
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

	// A second pass must be a no-op; this runs on every reconciliation.
	changed, err = EnsureArgoCDIdentity(ctx, client, "kube-system", "argocd-manager")
	if err != nil {
		t.Fatalf("second call returned unexpected error: %v", err)
	}
	if changed {
		t.Error("second call reported a change, want a no-op")
	}
}

func TestEnsureClusterRoleBindingRefusesToRewriteForeignBinding(t *testing.T) {
	t.Parallel()

	// Silently rewriting an existing binding would be an unannounced privilege
	// change, so the daemon must refuse and surface it.
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

	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok || create.GetSubresource() != "token" {
			return false, nil, nil
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

func TestEnsureAgentIdentityGrantsOnlyWhatTheDaemonNeeds(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ctx := context.Background()

	if err := EnsureAgentIdentity(ctx, client, "kube-system", "k2a-token-sync"); err != nil {
		t.Fatalf("EnsureAgentIdentity returned unexpected error: %v", err)
	}

	role, err := client.RbacV1().ClusterRoles().Get(ctx, "k2a-token-sync", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("clusterrole was not created: %v", err)
	}

	// The daemon must never hold blanket secret access in a downstream cluster.
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource == "secrets" {
				t.Error("agent clusterrole grants access to secrets")
			}
			if resource == "*" {
				t.Error("agent clusterrole uses a wildcard resource")
			}
		}
		for _, verb := range rule.Verbs {
			if verb == "*" {
				t.Error("agent clusterrole uses a wildcard verb")
			}
		}
	}

	// Idempotent: this runs on the bootstrap path and must tolerate re-runs.
	if err := EnsureAgentIdentity(ctx, client, "kube-system", "k2a-token-sync"); err != nil {
		t.Fatalf("second EnsureAgentIdentity returned unexpected error: %v", err)
	}
}
