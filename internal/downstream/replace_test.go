package downstream

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func existingBinding(name, role string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: ManagedByLabel},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: "argocd-manager", Namespace: "kube-system",
		}},
	}
}

// Bootstrap has to know the current role before it provisions anything: finding
// out afterwards would mean stopping with two identities already created, and
// the refusal has to be able to name both roles to be worth reading.
func TestBindingRoleReportsWhatIsThereAndDistinguishesAbsence(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(existingBinding("argocd-manager-role-binding", "cluster-admin"))
	role, err := BindingRole(t.Context(), client, "argocd-manager-role-binding")
	if err != nil {
		t.Fatalf("BindingRole returned unexpected error: %v", err)
	}
	if role != "cluster-admin" {
		t.Errorf("role = %q, want %q", role, "cluster-admin")
	}

	// Absence is not an error: it is the ordinary case on a cluster nobody has
	// bootstrapped, and the caller distinguishes it by the empty string.
	role, err = BindingRole(t.Context(), fake.NewClientset(), "argocd-manager-role-binding")
	if err != nil {
		t.Fatalf("BindingRole on a missing binding returned an error: %v", err)
	}
	if role != "" {
		t.Errorf("role = %q, want empty for a binding that does not exist", role)
	}
}

// The whole point of --replace-binding: a roleRef cannot be edited, so the
// binding is deleted and recreated. The result has to be indistinguishable from
// one bootstrap created fresh, label included, or 'remove' would later refuse to
// clean up the very binding this wrote.
func TestReplacingABindingRepointsItAndKeepsItOurs(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(existingBinding("argocd-manager-role-binding", "cluster-admin"))
	if err := ReplaceClusterRoleBinding(t.Context(), client,
		"argocd-manager-role-binding", "argocd-restricted", "kube-system", "argocd-manager"); err != nil {
		t.Fatalf("ReplaceClusterRoleBinding returned unexpected error: %v", err)
	}

	got, err := client.RbacV1().ClusterRoleBindings().Get(t.Context(), "argocd-manager-role-binding", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the binding is gone after a replace: %v", err)
	}
	if got.RoleRef.Name != "argocd-restricted" {
		t.Errorf("roleRef = %q, want %q", got.RoleRef.Name, "argocd-restricted")
	}
	for k, v := range ManagedByLabel {
		if got.Labels[k] != v {
			t.Errorf("labels = %v, want %s=%s — the label 'remove' keys off", got.Labels, k, v)
		}
	}
	if len(got.Subjects) != 1 || got.Subjects[0].Name != "argocd-manager" {
		t.Errorf("subjects = %v, want the ServiceAccount it was replacing for", got.Subjects)
	}
}

// Replacing a binding that is not there is how a repair after a partial failure
// looks, and it has to succeed rather than insisting on deleting something
// first.
func TestReplacingAnAbsentBindingJustCreatesIt(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	if err := ReplaceClusterRoleBinding(t.Context(), client,
		"argocd-manager-role-binding", "argocd-restricted", "kube-system", "argocd-manager"); err != nil {
		t.Fatalf("ReplaceClusterRoleBinding returned unexpected error: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(
		t.Context(), "argocd-manager-role-binding", metav1.GetOptions{}); err != nil {
		t.Fatalf("the binding was not created: %v", err)
	}
}
