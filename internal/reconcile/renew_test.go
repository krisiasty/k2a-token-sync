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
		Name:                   "standalone-1",
		Endpoint:               "10.1.0.10:6443",
		SecretName:             "cluster-standalone-1",
		SelfServiceAccountName: "k2a-token-sync",
		SelfTokenTTL:           2160 * time.Hour,
		ServiceAccount:         config.ServiceAccountRef{Name: "argocd-manager", Namespace: "kube-system"},
	}
}

// Renewal is gated on age because a pass is now minutes rather than a day. Get
// this wrong in one direction and the credential is reminted every few minutes;
// wrong in the other and it silently ages out, locking k2a-token-sync out of the
// cluster with no way back but a human re-running bootstrap.
func TestSelfCredentialDue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	cluster := testCluster() // selfTokenTTL is 2160h, so 90 days

	cases := []struct {
		name      string
		issuedAgo time.Duration // zero means no issue time recorded
		expiresIn time.Duration
		noExpiry  bool
		want      bool
	}{
		{
			// Nothing recorded: mint one, so there is an expiry to reason from.
			name:     "no recorded expiry",
			noExpiry: true,
			want:     true,
		},
		{
			// Just minted. The old behaviour renewed here too, on every pass.
			name:      "freshly minted",
			issuedAgo: time.Minute,
			expiresIn: 2160 * time.Hour,
			want:      false,
		},
		{
			name:      "not quite a day old",
			issuedAgo: 23 * time.Hour,
			expiresIn: 2160*time.Hour - 23*time.Hour,
			want:      false,
		},
		{
			name:      "a day old",
			issuedAgo: selfRenewInterval,
			expiresIn: 2160*time.Hour - selfRenewInterval,
			want:      true,
		},
		{
			// The case that makes the second test necessary. A credential granted an
			// hour is nowhere near a day old, so age alone would leave it alone until
			// long after it expired — and an expired self credential is a lock-out
			// that only a human re-running bootstrap can undo. Half of what was
			// granted is what applies.
			name:      "granted an hour, half of it gone",
			issuedAgo: 31 * time.Minute,
			expiresIn: 29 * time.Minute,
			want:      true,
		},
		{
			name:      "granted an hour, not yet half gone",
			issuedAgo: 20 * time.Minute,
			expiresIn: 40 * time.Minute,
			want:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var issuedAt, expiresAt time.Time
			if tc.issuedAgo != 0 {
				issuedAt = now.Add(-tc.issuedAgo)
			}
			if !tc.noExpiry {
				expiresAt = now.Add(tc.expiresIn)
			}
			if got := selfCredentialDue(cluster, issuedAt, expiresAt, now); got != tc.want {
				t.Errorf("selfCredentialDue(issued %v ago, expires in %v) = %v, want %v",
					tc.issuedAgo, tc.expiresIn, got, tc.want)
			}
		})
	}
}

// A credential written before the issue time was recorded — by an older release, or
// by bootstrap — must still be renewed. Age is then inferred from what is left of
// the requested lifetime, which is exactly what the previous version did.
func TestSelfCredentialDueWithoutARecordedIssueTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	cluster := testCluster() // selfTokenTTL is 2160h

	if selfCredentialDue(cluster, time.Time{}, now.Add(2160*time.Hour), now) {
		t.Error("a credential with the full lifetime remaining was renewed")
	}
	if !selfCredentialDue(cluster, time.Time{}, now.Add(2160*time.Hour-selfRenewInterval), now) {
		t.Error("a day-old credential was not renewed")
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

// rootCA is the one ConfigMap its own identity may read, and therefore what
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
// would lock k2a-token-sync out of the cluster, recoverable only by a human
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
		SelfCredentialExpiresAt: &metav1.Time{Time: expires},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

	r.renewSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

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
	if !status.SelfCredentialExpiresAt.Time.Equal(expires) {
		t.Errorf("status expiry = %v, want %v unchanged", status.SelfCredentialExpiresAt, expires)
	}
	// An issue time here would date the credential that is actually in use to a
	// renewal that never happened, ageing out a working one a day later.
	if status.SelfCredentialIssuedAt != nil {
		t.Errorf("a discarded renewal recorded an issue time: %v", status.SelfCredentialIssuedAt)
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
		SelfCredentialExpiresAt: &metav1.Time{Time: oldExpiry},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: oldExpiry}

	r.renewSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

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
	if status.SelfCredentialExpiresAt == nil || !status.SelfCredentialExpiresAt.Time.Equal(newExpiry) {
		t.Errorf("status expiry = %v, want %v so the lockout clock is visible",
			status.SelfCredentialExpiresAt, newExpiry)
	}
	// Without this, the next pass has no age to judge and would renew again
	// immediately — every five minutes, indefinitely.
	if status.SelfCredentialIssuedAt == nil || !status.SelfCredentialIssuedAt.Time.Equal(r.now()) {
		t.Errorf("status issue time = %v, want %v", status.SelfCredentialIssuedAt, r.now())
	}
}

// An expired credential cannot be renewed with itself, so k2a-token-sync must say so
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
