package reconcile

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/argocd"
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

// Reporting Ready=True while knowing ArgoCD holds a credential this tool did not
// publish would be a lie about the one thing the object exists to report. The
// failure has to reach status, and the recorded digest has to survive it — that
// digest is what the next pass compares against, so losing it would hide the
// replacement permanently.
func TestReplacedCredentialIsAFailureWithTheDigestStillRecorded(t *testing.T) {
	t.Parallel()

	var status v1alpha1.ClusterConnectionStatus
	recordFingerprint(&status, argocd.Fingerprint{
		Server:         "https://10.1.0.10:6443",
		CredentialHash: "the-digest-this-tool-published",
	})

	err := fmt.Errorf("%w: overwritten", errCredentialReplaced)

	if got := reasonFor(err); got != v1alpha1.ReasonCredentialReplaced {
		t.Errorf("reasonFor = %q, want %q — EndpointUnreachable would blame the wrong thing",
			got, v1alpha1.ReasonCredentialReplaced)
	}
	if status.AppliedCredentialHash != "the-digest-this-tool-published" {
		t.Errorf("recorded digest = %q, want what was published", status.AppliedCredentialHash)
	}
}
