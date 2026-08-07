package downstream

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// RemoveClusterRoleBinding is the first delete in the removal path, and the
// riskiest: a wrong guard here either leaves ArgoCD bound to a ServiceAccount
// this tool no longer manages, or deletes a binding it never created. Every
// outcome the function can report — removed, already gone, not ours, or a
// preview of removal — is exercised here.
func TestRemoveClusterRoleBinding(t *testing.T) {
	t.Parallel()

	t.Run("deletes a managed binding", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset(&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding", Labels: ManagedByLabel},
		})

		outcome, err := RemoveClusterRoleBinding(t.Context(), client, "argocd-manager-role-binding", false)
		if err != nil {
			t.Fatalf("RemoveClusterRoleBinding returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		if _, err := client.RbacV1().ClusterRoleBindings().
			Get(t.Context(), "argocd-manager-role-binding", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("binding still exists after removal: %v", err)
		}
	})

	t.Run("reports an absent binding without error", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset()

		outcome, err := RemoveClusterRoleBinding(t.Context(), client, "argocd-manager-role-binding", false)
		if err != nil {
			t.Fatalf("RemoveClusterRoleBinding returned unexpected error: %v", err)
		}
		if outcome != AbsentOutcome {
			t.Errorf("outcome = %v, want AbsentOutcome", outcome)
		}
	})

	t.Run("leaves an unlabelled binding untouched", func(t *testing.T) {
		t.Parallel()

		// A binding that lacks ManagedByLabel was not created by this tool, so
		// removing it would delete something an operator or another tool owns.
		client := fake.NewClientset(&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding"},
		})

		outcome, err := RemoveClusterRoleBinding(t.Context(), client, "argocd-manager-role-binding", false)
		if err != nil {
			t.Fatalf("RemoveClusterRoleBinding returned unexpected error: %v", err)
		}
		if outcome != NotOwnedOutcome {
			t.Errorf("outcome = %v, want NotOwnedOutcome", outcome)
		}
		if _, err := client.RbacV1().ClusterRoleBindings().
			Get(t.Context(), "argocd-manager-role-binding", metav1.GetOptions{}); err != nil {
			t.Errorf("unowned binding was removed: %v", err)
		}
	})

	t.Run("dry-run reports removal without deleting", func(t *testing.T) {
		t.Parallel()

		// A dry-run has to report the same RemovedOutcome a real run would, so a
		// preview and an actual removal read identically to the caller — the
		// only difference is that no delete action reaches the API, which is
		// what this asserts via the fake clientset's recorded actions.
		client := fake.NewClientset(&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding", Labels: ManagedByLabel},
		})

		outcome, err := RemoveClusterRoleBinding(t.Context(), client, "argocd-manager-role-binding", true)
		if err != nil {
			t.Fatalf("RemoveClusterRoleBinding returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		for _, action := range client.Actions() {
			if action.Matches("delete", "clusterrolebindings") {
				t.Error("dry-run recorded a delete action against the fake clientset")
			}
		}
		if _, err := client.RbacV1().ClusterRoleBindings().
			Get(t.Context(), "argocd-manager-role-binding", metav1.GetOptions{}); err != nil {
			t.Errorf("binding no longer exists after a dry-run: %v", err)
		}
	})
}

// RemoveServiceAccount removes the identity ArgoCD authenticates as. The same
// guard and outcome set as the binding case applies, and matters just as much
// here: this is the object whose token grants cluster-admin, so deleting one
// this tool doesn't own would silently break something it never provisioned.
func TestRemoveServiceAccount(t *testing.T) {
	t.Parallel()

	t.Run("deletes a managed serviceaccount", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset(&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system", Labels: ManagedByLabel},
		})

		outcome, err := RemoveServiceAccount(t.Context(), client, "kube-system", "argocd-manager", false)
		if err != nil {
			t.Fatalf("RemoveServiceAccount returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		if _, err := client.CoreV1().ServiceAccounts("kube-system").
			Get(t.Context(), "argocd-manager", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("serviceaccount still exists after removal: %v", err)
		}
	})

	t.Run("reports an absent serviceaccount without error", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset()

		outcome, err := RemoveServiceAccount(t.Context(), client, "kube-system", "argocd-manager", false)
		if err != nil {
			t.Fatalf("RemoveServiceAccount returned unexpected error: %v", err)
		}
		if outcome != AbsentOutcome {
			t.Errorf("outcome = %v, want AbsentOutcome", outcome)
		}
	})

	t.Run("leaves an unlabelled serviceaccount untouched", func(t *testing.T) {
		t.Parallel()

		// A serviceaccount that lacks ManagedByLabel was not created by this
		// tool, so removing it would delete something an operator or another
		// tool owns.
		client := fake.NewClientset(&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system"},
		})

		outcome, err := RemoveServiceAccount(t.Context(), client, "kube-system", "argocd-manager", false)
		if err != nil {
			t.Fatalf("RemoveServiceAccount returned unexpected error: %v", err)
		}
		if outcome != NotOwnedOutcome {
			t.Errorf("outcome = %v, want NotOwnedOutcome", outcome)
		}
		if _, err := client.CoreV1().ServiceAccounts("kube-system").
			Get(t.Context(), "argocd-manager", metav1.GetOptions{}); err != nil {
			t.Errorf("unowned serviceaccount was removed: %v", err)
		}
	})

	t.Run("dry-run reports removal without deleting", func(t *testing.T) {
		t.Parallel()

		// A dry-run has to report the same RemovedOutcome a real run would, so a
		// preview and an actual removal read identically to the caller — the
		// only difference is that no delete action reaches the API, which is
		// what this asserts via the fake clientset's recorded actions.
		client := fake.NewClientset(&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system", Labels: ManagedByLabel},
		})

		outcome, err := RemoveServiceAccount(t.Context(), client, "kube-system", "argocd-manager", true)
		if err != nil {
			t.Fatalf("RemoveServiceAccount returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		for _, action := range client.Actions() {
			if action.Matches("delete", "serviceaccounts") {
				t.Error("dry-run recorded a delete action against the fake clientset")
			}
		}
		if _, err := client.CoreV1().ServiceAccounts("kube-system").
			Get(t.Context(), "argocd-manager", metav1.GetOptions{}); err != nil {
			t.Errorf("serviceaccount no longer exists after a dry-run: %v", err)
		}
	})
}

// RemoveClusterRole removes the permission grant itself. Cluster-scoped
// objects with a shared, predictable name are the ones most likely to collide
// with something outside this tool's ownership, so the not-owned guard
// matters here as much as it does for the binding and the identity it binds.
func TestRemoveClusterRole(t *testing.T) {
	t.Parallel()

	t.Run("deletes a managed clusterrole", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset(&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Labels: ManagedByLabel},
		})

		outcome, err := RemoveClusterRole(t.Context(), client, "k2a-token-sync", false)
		if err != nil {
			t.Fatalf("RemoveClusterRole returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		if _, err := client.RbacV1().ClusterRoles().
			Get(t.Context(), "k2a-token-sync", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("clusterrole still exists after removal: %v", err)
		}
	})

	t.Run("reports an absent clusterrole without error", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset()

		outcome, err := RemoveClusterRole(t.Context(), client, "k2a-token-sync", false)
		if err != nil {
			t.Fatalf("RemoveClusterRole returned unexpected error: %v", err)
		}
		if outcome != AbsentOutcome {
			t.Errorf("outcome = %v, want AbsentOutcome", outcome)
		}
	})

	t.Run("leaves an unlabelled clusterrole untouched", func(t *testing.T) {
		t.Parallel()

		// A clusterrole that lacks ManagedByLabel was not created by this tool,
		// so removing it would delete something an operator or another tool
		// owns.
		client := fake.NewClientset(&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync"},
		})

		outcome, err := RemoveClusterRole(t.Context(), client, "k2a-token-sync", false)
		if err != nil {
			t.Fatalf("RemoveClusterRole returned unexpected error: %v", err)
		}
		if outcome != NotOwnedOutcome {
			t.Errorf("outcome = %v, want NotOwnedOutcome", outcome)
		}
		if _, err := client.RbacV1().ClusterRoles().
			Get(t.Context(), "k2a-token-sync", metav1.GetOptions{}); err != nil {
			t.Errorf("unowned clusterrole was removed: %v", err)
		}
	})

	t.Run("dry-run reports removal without deleting", func(t *testing.T) {
		t.Parallel()

		// A dry-run has to report the same RemovedOutcome a real run would, so a
		// preview and an actual removal read identically to the caller — the
		// only difference is that no delete action reaches the API, which is
		// what this asserts via the fake clientset's recorded actions.
		client := fake.NewClientset(&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "k2a-token-sync", Labels: ManagedByLabel},
		})

		outcome, err := RemoveClusterRole(t.Context(), client, "k2a-token-sync", true)
		if err != nil {
			t.Fatalf("RemoveClusterRole returned unexpected error: %v", err)
		}
		if outcome != RemovedOutcome {
			t.Errorf("outcome = %v, want RemovedOutcome", outcome)
		}
		for _, action := range client.Actions() {
			if action.Matches("delete", "clusterroles") {
				t.Error("dry-run recorded a delete action against the fake clientset")
			}
		}
		if _, err := client.RbacV1().ClusterRoles().
			Get(t.Context(), "k2a-token-sync", metav1.GetOptions{}); err != nil {
			t.Errorf("clusterrole no longer exists after a dry-run: %v", err)
		}
	})
}

// A concurrent delete between the guarding Get and the Delete call must not
// surface as an error: the end state — the object is gone — is exactly what
// the caller asked for.
func TestRemoveClusterRoleBindingTreatsAConcurrentDeleteAsAbsent(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding", Labels: ManagedByLabel},
	})
	client.PrependReactor("delete", "clusterrolebindings", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(
			schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"},
			"argocd-manager-role-binding")
	})

	outcome, err := RemoveClusterRoleBinding(t.Context(), client, "argocd-manager-role-binding", false)
	if err != nil {
		t.Fatalf("RemoveClusterRoleBinding returned unexpected error: %v", err)
	}
	if outcome != AbsentOutcome {
		t.Errorf("outcome = %v, want AbsentOutcome", outcome)
	}
}
