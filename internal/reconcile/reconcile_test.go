package reconcile

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krisiasty/k2a-token-sync/internal/config"
)

func TestArgoCDSecretError(t *testing.T) {
	t.Parallel()

	r := &Reconciler{cfg: &config.Config{ArgoCDNamespace: "argocd"}}
	cluster := config.Cluster{Name: "standalone-1", SecretName: "cluster-standalone-1"}

	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "secrets"}, cluster.SecretName, errors.New("no access"))

	t.Run("nil passes through", func(t *testing.T) {
		t.Parallel()
		if err := r.argocdSecretError(cluster, nil); err != nil {
			t.Fatalf("argocdSecretError(nil) = %v, want nil", err)
		}
	})

	t.Run("other errors are untouched", func(t *testing.T) {
		t.Parallel()
		original := errors.New("connection refused")
		got := r.argocdSecretError(cluster, original)
		if !errors.Is(got, original) || got.Error() != original.Error() {
			t.Fatalf("argocdSecretError() = %q, want it returned verbatim", got)
		}
	})

	// The hint must survive the wrapping the apply helpers add, which is how the
	// error actually arrives here.
	t.Run("wrapped forbidden gains the remedy", func(t *testing.T) {
		t.Parallel()
		wrapped := fmt.Errorf("applying cluster secret argocd/%s: %w", cluster.SecretName, forbidden)
		got := r.argocdSecretError(cluster, wrapped)

		if !errors.Is(got, forbidden) {
			t.Error("the original API error is no longer unwrappable")
		}
		for _, want := range []string{"create and patch", "argocd", cluster.SecretName} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("error %q does not mention %q", got, want)
			}
		}
	})
}
