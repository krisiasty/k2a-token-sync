package downstream

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// RemovalOutcome is what happened, or would happen, to one object during removal.
type RemovalOutcome int

// The possible RemovalOutcome values.
const (
	RemovedOutcome  RemovalOutcome = iota // deleted (or, in dry-run, would be)
	AbsentOutcome                         // already gone; not an error
	NotOwnedOutcome                       // exists but lacks ManagedByLabel; left alone
)

// isManaged reports whether an object carries the label this tool stamps on
// everything it creates, checked against the same ManagedByLabel pair used to
// set it rather than a re-declared literal.
func isManaged(labels map[string]string) bool {
	for k, v := range ManagedByLabel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// RemoveClusterRoleBinding deletes a ClusterRoleBinding if k2a-token-sync's
// ManagedByLabel is present, and reports what it found.
//
// The object is fetched before it is deleted, rather than issuing a blind
// Delete, so the label can be checked and dryRun can be honoured without a
// write ever reaching the API server.
func RemoveClusterRoleBinding(ctx context.Context, client kubernetes.Interface, name string, dryRun bool) (RemovalOutcome, error) {
	existing, err := client.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return AbsentOutcome, nil
	}
	if err != nil {
		return AbsentOutcome, fmt.Errorf("getting clusterrolebinding %s: %w", name, err)
	}
	if !isManaged(existing.Labels) {
		return NotOwnedOutcome, nil
	}
	if dryRun {
		return RemovedOutcome, nil
	}

	if err := client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		// A concurrent delete between the Get above and here still leaves the
		// cluster in the state this call was asked to reach.
		if apierrors.IsNotFound(err) {
			return AbsentOutcome, nil
		}
		return AbsentOutcome, fmt.Errorf("deleting clusterrolebinding %s: %w", name, err)
	}
	return RemovedOutcome, nil
}

// RemoveServiceAccount deletes a ServiceAccount under the same guard.
func RemoveServiceAccount(ctx context.Context, client kubernetes.Interface, namespace, name string, dryRun bool) (RemovalOutcome, error) {
	existing, err := client.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return AbsentOutcome, nil
	}
	if err != nil {
		return AbsentOutcome, fmt.Errorf("getting serviceaccount %s/%s: %w", namespace, name, err)
	}
	if !isManaged(existing.Labels) {
		return NotOwnedOutcome, nil
	}
	if dryRun {
		return RemovedOutcome, nil
	}

	if err := client.CoreV1().ServiceAccounts(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return AbsentOutcome, nil
		}
		return AbsentOutcome, fmt.Errorf("deleting serviceaccount %s/%s: %w", namespace, name, err)
	}
	return RemovedOutcome, nil
}

// RemoveClusterRole deletes a ClusterRole under the same guard.
func RemoveClusterRole(ctx context.Context, client kubernetes.Interface, name string, dryRun bool) (RemovalOutcome, error) {
	existing, err := client.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return AbsentOutcome, nil
	}
	if err != nil {
		return AbsentOutcome, fmt.Errorf("getting clusterrole %s: %w", name, err)
	}
	if !isManaged(existing.Labels) {
		return NotOwnedOutcome, nil
	}
	if dryRun {
		return RemovedOutcome, nil
	}

	if err := client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return AbsentOutcome, nil
		}
		return AbsentOutcome, fmt.Errorf("deleting clusterrole %s: %w", name, err)
	}
	return RemovedOutcome, nil
}
