package reconcile

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
func mintsTokens(client *fake.Clientset, expiresAt time.Time) {
	client.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authenticationv1.TokenRequest{
			Status: authenticationv1.TokenRequestStatus{
				Token:               newToken,
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
		events: &fakeRecorder{},
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
	mintsTokens(downstreamClient, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))

	// The verification client cannot read the CA, standing in for a token the API
	// server will not accept.
	r := newReconciler(local, func() (kubernetes.Interface, error) {
		return fake.NewClientset(), nil
	})

	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: expires},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

	if _, err := r.renewSelfCredential(ctx, cluster, access, &status, r.logger, r.now()); err != nil {
		t.Logf("renewal reported: %v", err)
	}

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
	mintsTokens(downstreamClient, newExpiry)

	r := newReconciler(local, func() (kubernetes.Interface, error) {
		return fake.NewClientset(rootCA()), nil
	})

	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: oldExpiry},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: oldExpiry}

	if _, err := r.renewSelfCredential(ctx, cluster, access, &status, r.logger, r.now()); err != nil {
		t.Logf("renewal reported: %v", err)
	}

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

// failsTo makes one verb against one resource return an error, standing in for
// the RBAC and API-server failures that renewal actually meets.
func failsTo(client *fake.Clientset, verb, resource string, message string) {
	client.PrependReactor(verb, resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New(message)
	})
}

// Every way renewal can fail has to reach the object, not just the log. A failure
// that only warns spends the credential's remaining lifetime in silence, and the
// first visible symptom is a cluster that can no longer be reached at all.
func TestRenewalFailuresAreRecordedOnTheObject(t *testing.T) {
	t.Parallel()

	// Two hours left of a lifetime that was granted as four: past the quarter that
	// counts as comfortable, so every one of these is the urgent flavour.
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	issued := now.Add(-2 * time.Hour)
	expires := now.Add(2 * time.Hour)

	cases := []struct {
		name       string
		breaks     func(local *fake.Clientset, downstreamClient *fake.Clientset)
		verifyWith func() (kubernetes.Interface, error)
		wantReason string
	}{
		{
			// No permission to mint, or the API server refusing: the usual shape of
			// a downstream RBAC problem.
			name: "cannot mint a replacement",
			breaks: func(_ *fake.Clientset, downstreamClient *fake.Clientset) {
				failsTo(downstreamClient, "create", "serviceaccounts", "serviceaccounts/token is forbidden")
			},
			verifyWith: func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil },
			wantReason: v1alpha1.ReasonRenewalMintFailed,
		},
		{
			// Minted, but it does not work — the case that must never overwrite a
			// credential that does.
			name:       "the replacement does not work",
			breaks:     func(_ *fake.Clientset, _ *fake.Clientset) {},
			verifyWith: func() (kubernetes.Interface, error) { return fake.NewClientset(), nil },
			wantReason: v1alpha1.ReasonRenewalUnverified,
		},
		{
			// Minted and verified, but this tool cannot write its own namespace.
			name: "cannot store the replacement",
			breaks: func(local *fake.Clientset, _ *fake.Clientset) {
				failsTo(local, "update", "secrets", "secrets is forbidden")
				failsTo(local, "create", "secrets", "secrets is forbidden")
			},
			verifyWith: func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil },
			wantReason: v1alpha1.ReasonRenewalNotStored,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cluster := testCluster()
			cluster.SelfTokenTTL = 4 * time.Hour

			local := fake.NewClientset(storedCredential(expires))
			downstreamClient := fake.NewClientset(rootCA())
			mintsTokens(downstreamClient, now.Add(4*time.Hour))
			tc.breaks(local, downstreamClient)

			r := newReconciler(local, tc.verifyWith)
			r.now = func() time.Time { return now }

			status := v1alpha1.ClusterConnectionStatus{
				SelfCredentialExpiresAt: &metav1.Time{Time: expires},
				SelfCredentialIssuedAt:  &metav1.Time{Time: issued},
			}
			access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

			r.maintainSelfCredential(t.Context(), cluster, access, &status, r.logger, now)

			condition := meta.FindStatusCondition(status.Conditions, v1alpha1.ConditionSelfCredentialValid)
			if condition == nil {
				t.Fatal("renewal failed without saying so on the object")
			}
			if condition.Status != metav1.ConditionFalse {
				t.Errorf("SelfCredentialValid = %s, want False", condition.Status)
			}
			// The reason has to name the step, since the three point at different
			// places: downstream RBAC, the credential just issued, and this tool's
			// own namespace.
			if condition.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", condition.Reason, tc.wantReason)
			}
			if !strings.Contains(condition.Message, "bootstrapped again") {
				t.Errorf("the message does not say what the end of this is: %q", condition.Message)
			}

			// The working credential must survive every one of these.
			creds, err := k8s.ReadCredentials(t.Context(), local, testNamespace, cluster.CredentialsSecretName())
			if err != nil {
				t.Fatalf("reading the stored credential back: %v", err)
			}
			if creds.Token != oldToken {
				t.Errorf("stored token = %q, want the working credential %q left alone", creds.Token, oldToken)
			}
			if status.SelfCredentialIssuedAt.Time != issued {
				t.Error("a failed renewal moved the issue time, which would age out a credential that still works")
			}

			// Two hours left of four: Ready must stop claiming everything is fine.
			_, _, threatened := selfCredentialThreatensAccess(cluster, status, now)
			if !threatened {
				t.Error("Ready would still report healthy with the credential two hours from expiry")
			}
		})
	}
}

// The same failure is unremarkable with most of the lifetime left. Ready stays
// true, because ArgoCD genuinely is still being served, while the dedicated
// condition carries the bad news.
func TestAFailingRenewalWithHeadroomDoesNotUnsettleReady(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cluster := testCluster() // selfTokenTTL is 2160h
	issued := now.Add(-25 * time.Hour)
	expires := issued.Add(2160 * time.Hour) // 89 days still to run

	local := fake.NewClientset(storedCredential(expires))
	downstreamClient := fake.NewClientset(rootCA())
	failsTo(downstreamClient, "create", "serviceaccounts", "serviceaccounts/token is forbidden")

	r := newReconciler(local, func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil })
	r.now = func() time.Time { return now }

	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: expires},
		SelfCredentialIssuedAt:  &metav1.Time{Time: issued},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

	r.maintainSelfCredential(t.Context(), cluster, access, &status, r.logger, now)

	if !meta.IsStatusConditionFalse(status.Conditions, v1alpha1.ConditionSelfCredentialValid) {
		t.Error("a failing renewal was not reported at all")
	}
	if _, _, threatened := selfCredentialThreatensAccess(cluster, status, now); threatened {
		t.Error("Ready was pulled down with 89 days of credential left, which is not yet a threat to access")
	}
}

// A credential the API server capped short has no comfortable margin to wait for,
// so proportional headroom is not enough on its own.
func TestACappedLifetimeIsUrgentImmediately(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cluster := testCluster() // asks for 2160h

	// Granted two hours, and only just issued: three quarters of it still to run.
	issued := now.Add(-15 * time.Minute)
	expires := now.Add(105 * time.Minute)

	if !selfCredentialCritical(cluster, issued, expires, now) {
		t.Error("a two-hour credential was treated as having comfortable headroom")
	}

	// The same proportion of a ninety-day credential is not urgent at all.
	longIssued := now.Add(-22 * 24 * time.Hour)
	longExpires := now.Add(68 * 24 * time.Hour)
	if selfCredentialCritical(cluster, longIssued, longExpires, now) {
		t.Error("68 days of remaining credential was treated as urgent")
	}
}

// Renewal that succeeds must clear the condition, or one bad afternoon would
// leave the object complaining forever.
func TestASuccessfulRenewalClearsTheCondition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cluster := testCluster()
	issued := now.Add(-25 * time.Hour)
	expires := now.Add(2000 * time.Hour)

	local := fake.NewClientset(storedCredential(expires))
	downstreamClient := fake.NewClientset(rootCA())
	mintsTokens(downstreamClient, now.Add(2160*time.Hour))

	r := newReconciler(local, func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil })
	r.now = func() time.Time { return now }

	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: expires},
		SelfCredentialIssuedAt:  &metav1.Time{Time: issued},
	}
	// Left over from an earlier failure.
	setCondition(&status, v1alpha1.ConditionSelfCredentialValid, metav1.ConditionFalse,
		v1alpha1.ReasonRenewalMintFailed, "an earlier failure", 1)

	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}
	r.maintainSelfCredential(t.Context(), cluster, access, &status, r.logger, now)

	if !meta.IsStatusConditionTrue(status.Conditions, v1alpha1.ConditionSelfCredentialValid) {
		t.Error("a successful renewal left the object still reporting a failure")
	}
}

// A credential with no recorded expiry is explicitly tolerated when read, on the
// understanding that the next renewal replaces it with one whose deadline is
// known. When that renewal is what fails, the tool is blind: it holds a working
// credential and cannot say whether it has ninety days or ninety seconds.
func TestAFailingRenewalWithNoRecordedExpiryIsTreatedAsUrgent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cluster := testCluster()

	local := fake.NewClientset(storedCredential(time.Time{})) // no expires-at
	downstreamClient := fake.NewClientset(rootCA())
	failsTo(downstreamClient, "create", "serviceaccounts", "serviceaccounts/token is forbidden")

	r := newReconciler(local, func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil })
	r.now = func() time.Time { return now }

	var status v1alpha1.ClusterConnectionStatus
	access := &clusterAccess{client: downstreamClient, ca: testCA}

	r.maintainSelfCredential(t.Context(), cluster, access, &status, r.logger, now)

	condition := meta.FindStatusCondition(status.Conditions, v1alpha1.ConditionSelfCredentialValid)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatal("a failing renewal was not reported")
	}

	// The zero time subtracted from now is -2562047h, which in a status message
	// reads as a bug in this tool rather than a fact about the Secret.
	if strings.Contains(condition.Message, "0001-01-01") || strings.Contains(condition.Message, "-2562047") {
		t.Errorf("the message renders the zero time as though it were a deadline: %q", condition.Message)
	}
	if !strings.Contains(condition.Message, "unknown") {
		t.Errorf("the message does not say the expiry is unknown: %q", condition.Message)
	}

	// Policy: nothing rules out this credential expiring within the hour.
	if _, _, threatened := selfCredentialThreatensAccess(cluster, status, now); !threatened {
		t.Error("an unknown deadline with renewal failing was treated as comfortable")
	}
}

// Status has to describe the credential actually in use. A previous expiry left
// behind would have Ready judged against one deadline while the condition
// explaining the failure quoted another.
func TestAnUnknownExpiryClearsWhatStatusRemembered(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cluster := testCluster()
	stale := now.Add(2000 * time.Hour)

	local := fake.NewClientset(storedCredential(time.Time{})) // the Secret lost its expires-at
	downstreamClient := fake.NewClientset(rootCA())
	failsTo(downstreamClient, "create", "serviceaccounts", "serviceaccounts/token is forbidden")

	r := newReconciler(local, func() (kubernetes.Interface, error) { return fake.NewClientset(rootCA()), nil })
	r.now = func() time.Time { return now }

	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: stale},
		SelfCredentialIssuedAt:  &metav1.Time{Time: now.Add(-160 * time.Hour)},
	}

	if err := r.reconcile(t.Context(), cluster, &status, r.logger, now); err == nil {
		t.Log("reconcile completed; only the recorded expiry matters here")
	}

	if status.SelfCredentialExpiresAt != nil {
		t.Errorf("status still claims the credential expires %v, which belongs to a credential that is gone",
			status.SelfCredentialExpiresAt)
	}
	if status.SelfCredentialIssuedAt != nil {
		t.Errorf("status still claims an issue time of %v for a credential this tool did not write",
			status.SelfCredentialIssuedAt)
	}
}
