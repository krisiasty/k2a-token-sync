package reconcile

import (
	"errors"
	"fmt"
	"net/url"
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

// The reason is the first thing an operator reads and the thing they act on;
// the message beside it is only seen by someone who already ran 'describe'. So
// a confidently wrong reason costs a whole round of debugging in the wrong
// place, which is exactly what the old default did: every failure nobody had
// classified claimed the endpoint was unreachable, including an RBAC refusal
// from an API server that had answered twice already in the same pass.
func TestReasonForNamesACauseOnlyWhenItKnowsOne(t *testing.T) {
	t.Parallel()

	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "clusterrolebindings"}, "argocd-manager-role-binding",
		errors.New("attempting to grant RBAC permissions not currently held"))

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"never bootstrapped", errNoCredential, v1alpha1.ReasonAwaitingCredential},
		{"locked out", errCredentialExpired, v1alpha1.ReasonCredentialExpired},
		{"certificate unusable", errCertificateInvalid, v1alpha1.ReasonCertificateInvalid},
		{"minted credential refused", errCredentialRejected, v1alpha1.ReasonCredentialRejected},
		{"published credential overwritten", errCredentialReplaced, v1alpha1.ReasonCredentialReplaced},

		// The case that motivated this: the API server answered and said no.
		// Reported as unreachable, it sends the reader to the network.
		{"rbac refusal", forbidden, v1alpha1.ReasonPermissionDenied},
		{"rbac refusal, wrapped", fmt.Errorf("creating clusterrolebinding: %w", forbidden),
			v1alpha1.ReasonPermissionDenied},

		// The only shape of error that actually establishes the endpoint could
		// not be reached. client-go wraps dial failures in *url.Error.
		{"dial failure", &url.Error{Op: "Get", URL: "https://10.1.0.10:6443/api",
			Err: errors.New("dial tcp 10.1.0.10:6443: connect: connection refused")},
			v1alpha1.ReasonEndpointUnreachable},

		// Anything unrecognised says only that the pass failed. A future error
		// nobody classifies inherits an honest reason rather than a wrong one,
		// which is the whole point of the default.
		{"unclassified", errors.New("reading the cluster CA: configmap has no ca.crt entry"),
			v1alpha1.ReasonReconcileFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := reasonFor(tc.err); got != tc.want {
				t.Errorf("reasonFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// Nothing but a genuine transport failure may claim the endpoint is
// unreachable. Stated as its own test because the regression it guards is
// silent: a new default, or a new case ordered above the url.Error one, would
// keep every other test passing while quietly restoring the wrong answer.
func TestOnlyATransportFailureClaimsTheEndpointIsUnreachable(t *testing.T) {
	t.Parallel()

	notEndpointFailures := []error{
		errors.New("some new failure nobody has classified yet"),
		fmt.Errorf("minting token: %w", apierrors.NewForbidden(
			schema.GroupResource{Resource: "serviceaccounts/token"}, "argocd-manager", errors.New("denied"))),
		apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "kube-root-ca.crt"),
		apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, "cluster-x", errors.New("conflict")),
	}
	for _, err := range notEndpointFailures {
		if got := reasonFor(err); got == v1alpha1.ReasonEndpointUnreachable {
			t.Errorf("reasonFor(%v) claimed the endpoint was unreachable", err)
		}
	}
}
