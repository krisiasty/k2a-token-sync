package downstream

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// BindingRole reports the ClusterRole a binding currently references, or "" when
// the binding does not exist.
//
// Separate from EnsureClusterRoleBinding because bootstrap needs the answer
// before it provisions anything: discovering a role change after two identities
// have been created is a worse place to stop than before, and knowing the
// current role is what lets the refusal name both sides of the change.
func BindingRole(ctx context.Context, client kubernetes.Interface, bindingName string) (string, error) {
	existing, err := client.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("getting clusterrolebinding %s: %w", bindingName, err)
	}
	return existing.RoleRef.Name, nil
}

// ReplaceClusterRoleBinding deletes a ClusterRoleBinding and recreates it
// against a different ClusterRole.
//
// This is the only way to repoint a binding — a roleRef is immutable — and it is
// deliberately not available to the reconciliation loop. Between the delete and
// the create, ArgoCD's token authenticates and authorises nothing, so every
// request it makes fails; doing that unattended, on a timer, to a cluster that
// was working is not a decision this tool should take on its own. Bootstrap runs
// with administrative credentials and a person present, which is what makes it
// the right place, on the same reasoning as --adopt.
//
// The gap is as short as two sequential API calls. It is not zero, and the
// caller is expected to have said so before getting here.
func ReplaceClusterRoleBinding(
	ctx context.Context,
	client kubernetes.Interface,
	bindingName, clusterRole, saNamespace, saName string,
) error {
	if clusterRole == "" {
		return fmt.Errorf("refusing to replace %s with a binding to an empty ClusterRole: "+
			"the role to bind was never set", bindingName)
	}

	err := client.RbacV1().ClusterRoleBindings().Delete(ctx, bindingName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting clusterrolebinding %s: %w", bindingName, err)
	}

	// Recreated through the same path that would have created it in the first
	// place, so a replaced binding is indistinguishable from a fresh one —
	// including its managed-by label, which is what 'remove' keys off later.
	if _, err := EnsureClusterRoleBinding(ctx, client, bindingName, clusterRole, saNamespace, saName); err != nil {
		return fmt.Errorf("recreating clusterrolebinding %s against %q: %w", bindingName, clusterRole, err)
	}
	return nil
}
