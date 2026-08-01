package reconcile

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/k8s"
)

const (
	testNamespace = "k2a-token-sync"

	oldToken = "old-token-that-works"
	newToken = "freshly-minted-token" //nolint:gosec // a test fixture, not a credential
)

var testCA = []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")

func testCluster() config.Cluster {
	return config.Cluster{
		Name:                    "standalone-1",
		Endpoint:                "10.1.0.10:6443",
		SecretName:              "cluster-standalone-1",
		AgentServiceAccountName: "k2a-token-sync",
		AgentTokenTTL:           2160 * time.Hour,
		ServiceAccount:          config.ServiceAccountRef{Name: "argocd-manager", Namespace: "kube-system"},
	}
}

// mintsTokens makes a fake downstream cluster answer TokenRequest, which it does
// not do by default.
func mintsTokens(client *fake.Clientset, token string, expiresAt time.Time) {
	client.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authenticationv1.TokenRequest{
			Status: authenticationv1.TokenRequestStatus{
				Token:               token,
				ExpirationTimestamp: metav1.NewTime(expiresAt),
			},
		}, nil
	})
}

// rootCA is the one ConfigMap the agent identity may read, and therefore what
// verifying a renewed credential actually calls.
func rootCA() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "kube-system"},
		Data:       map[string]string{"ca.crt": string(testCA)},
	}
}

func storedCredential(expiresAt time.Time) *corev1.Secret {
	data := map[string][]byte{
		"token":  []byte(oldToken),
		"ca.crt": testCA,
	}
	if !expiresAt.IsZero() {
		data["expires-at"] = []byte(expiresAt.UTC().Format(time.RFC3339))
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-1-credentials", Namespace: testNamespace},
		Data:       data,
	}
}

func newReconciler(local kubernetes.Interface, verify func() (kubernetes.Interface, error)) *Reconciler {
	return &Reconciler{
		cfg:    &config.Config{Namespace: testNamespace, ArgoCDNamespace: "argocd"},
		local:  local,
		logger: slog.New(slog.DiscardHandler),
		now:    func() time.Time { return time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC) },
		clientForToken: func(_, _ string, _ []byte) (kubernetes.Interface, error) {
			return verify()
		},
	}
}

// The property this guards: overwriting a working credential with a broken one
// would lock the daemon out of the cluster, recoverable only by a human
// re-running bootstrap. So a renewal that cannot be verified must be discarded.
func TestRenewalThatFailsVerificationKeepsTheWorkingCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()
	expires := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	local := fake.NewClientset(storedCredential(expires))
	downstreamClient := fake.NewClientset(rootCA())
	mintsTokens(downstreamClient, newToken, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))

	// The verification client cannot read the CA, standing in for a token the API
	// server will not accept.
	r := newReconciler(local, func() (kubernetes.Interface, error) {
		return fake.NewClientset(), nil
	})

	status := v1alpha1.ClusterConnectionStatus{
		AgentCredentialExpiresAt: &metav1.Time{Time: expires},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

	r.renewAgentCredential(ctx, cluster, access, &status, r.logger)

	creds, err := k8s.ReadCredentials(ctx, local, testNamespace, cluster.CredentialsSecretName())
	if err != nil {
		t.Fatalf("reading the stored credential back: %v", err)
	}
	if creds.Token != oldToken {
		t.Errorf("stored token = %q, want the working credential %q left in place", creds.Token, oldToken)
	}
	if !creds.ExpiresAt.Equal(expires) {
		t.Errorf("stored expiry = %v, want %v unchanged", creds.ExpiresAt, expires)
	}
	if !status.AgentCredentialExpiresAt.Time.Equal(expires) {
		t.Errorf("status expiry = %v, want %v unchanged", status.AgentCredentialExpiresAt, expires)
	}
}

func TestRenewalStoresAVerifiedCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()
	oldExpiry := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	newExpiry := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	local := fake.NewClientset(storedCredential(oldExpiry))
	downstreamClient := fake.NewClientset(rootCA())
	mintsTokens(downstreamClient, newToken, newExpiry)

	r := newReconciler(local, func() (kubernetes.Interface, error) {
		return fake.NewClientset(rootCA()), nil
	})

	status := v1alpha1.ClusterConnectionStatus{
		AgentCredentialExpiresAt: &metav1.Time{Time: oldExpiry},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: oldExpiry}

	r.renewAgentCredential(ctx, cluster, access, &status, r.logger)

	creds, err := k8s.ReadCredentials(ctx, local, testNamespace, cluster.CredentialsSecretName())
	if err != nil {
		t.Fatalf("reading the stored credential back: %v", err)
	}
	if creds.Token != newToken {
		t.Errorf("stored token = %q, want the renewed %q", creds.Token, newToken)
	}
	if !creds.ExpiresAt.Equal(newExpiry) {
		t.Errorf("stored expiry = %v, want %v", creds.ExpiresAt, newExpiry)
	}
	// ReadCredentials trims, so compare trimmed: the PEM's trailing newline is
	// not part of what is stored.
	if !bytes.Equal(creds.CA, bytes.TrimSpace(testCA)) {
		t.Errorf("the CA bundle was not carried over to the renewed credential: got %q", creds.CA)
	}
	if status.AgentCredentialExpiresAt == nil || !status.AgentCredentialExpiresAt.Time.Equal(newExpiry) {
		t.Errorf("status expiry = %v, want %v so the lockout clock is visible",
			status.AgentCredentialExpiresAt, newExpiry)
	}
}

// An expired credential cannot be renewed with itself, so the daemon must say so
// rather than reporting an opaque 401 from the API server.
func TestAccessReportsAnExpiredCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()
	expired := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	r := newReconciler(fake.NewClientset(storedCredential(expired)), func() (kubernetes.Interface, error) {
		return fake.NewClientset(), nil
	})

	_, err := r.access(ctx, cluster)
	if err == nil {
		t.Fatal("access succeeded with an expired credential")
	}
	if !errors.Is(err, errCredentialExpired) {
		t.Fatalf("error = %v, want it to wrap errCredentialExpired", err)
	}
	if got := reasonFor(err); got != v1alpha1.ReasonCredentialExpired {
		t.Errorf("reasonFor() = %q, want %q", got, v1alpha1.ReasonCredentialExpired)
	}
}

// A credential written before expiries were recorded, or by external automation
// that omits the key, must keep working rather than being treated as expired.
func TestAccessAcceptsACredentialWithNoRecordedExpiry(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()

	r := newReconciler(fake.NewClientset(storedCredential(time.Time{})), func() (kubernetes.Interface, error) {
		return fake.NewClientset(rootCA()), nil
	})

	access, err := r.access(ctx, cluster)
	if err != nil {
		t.Fatalf("access rejected a credential with no recorded expiry: %v", err)
	}
	if !access.expiresAt.IsZero() {
		t.Errorf("expiresAt = %v, want zero", access.expiresAt)
	}
}
