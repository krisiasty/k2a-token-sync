package argocd

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const testCA = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"

func testSecret() ClusterSecret {
	return ClusterSecret{
		Name:        "cluster-downstream-1",
		Namespace:   "argocd",
		DisplayName: "downstream-1",
		Server:      "https://10.0.0.10:6443",
		BearerToken: "token-value",
		CAData:      []byte(testCA),
		ClusterName: "downstream-1",
	}
}

// newClient returns a fake with server-side apply support. NewSimpleClientset
// tracks no managed fields, so it cannot model what these tests are about.
func newClient() kubernetes.Interface {
	return fake.NewClientset()
}

func TestApplyProducesArgoCDClusterSecret(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newClient()
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	expires := now.Add(720 * time.Hour)

	desired := testSecret()
	desired.TokenExpiresAt = expires
	desired.ServingCertExpiresAt = now.Add(200 * 24 * time.Hour)

	if err := ApplyCredential(ctx, client, desired); err != nil {
		t.Fatalf("ApplyCredential returned unexpected error: %v", err)
	}
	if _, err := ApplyRegistration(ctx, client, desired, now); err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}

	secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the applied secret cannot be read back: %v", err)
	}

	// Without this label ArgoCD ignores the Secret entirely.
	if got := secret.Labels[SecretTypeLabel]; got != "cluster" {
		t.Errorf("%s = %q, want cluster", SecretTypeLabel, got)
	}
	if got := string(secret.Data["name"]); got != "downstream-1" {
		t.Errorf("data.name = %q", got)
	}
	if got := string(secret.Data["server"]); got != "https://10.0.0.10:6443" {
		t.Errorf("data.server = %q, want the direct endpoint", got)
	}
	if _, ok := secret.Data["project"]; ok {
		t.Error("data.project was set although no project was configured")
	}

	var parsed struct {
		BearerToken     string `json:"bearerToken"`
		TLSClientConfig struct {
			CAData   string `json:"caData"`
			Insecure bool   `json:"insecure"`
		} `json:"tlsClientConfig"`
	}
	if err := json.Unmarshal(secret.Data["config"], &parsed); err != nil {
		t.Fatalf("data.config is not valid JSON: %v", err)
	}
	if parsed.BearerToken != "token-value" {
		t.Errorf("config.bearerToken = %q", parsed.BearerToken)
	}
	if parsed.TLSClientConfig.Insecure {
		t.Error("config.tlsClientConfig.insecure is true; verification must stay on")
	}

	// ArgoCD models caData as []byte, so it must be base64 in the JSON.
	decoded, err := base64.StdEncoding.DecodeString(parsed.TLSClientConfig.CAData)
	if err != nil {
		t.Fatalf("config.tlsClientConfig.caData is not base64: %v", err)
	}
	if string(decoded) != testCA {
		t.Errorf("caData did not round-trip: got %q", decoded)
	}

	if got := secret.Annotations[TokenExpiryAnnotation]; got != expires.Format(time.RFC3339) {
		t.Errorf("%s = %q, want %q", TokenExpiryAnnotation, got, expires.Format(time.RFC3339))
	}
	if secret.Annotations[ServingCertExpiryAnnotation] == "" {
		t.Errorf("%s was not recorded", ServingCertExpiryAnnotation)
	}
	if got := secret.Annotations[ClusterNameAnnotation]; got != "downstream-1" {
		t.Errorf("%s = %q", ClusterNameAnnotation, got)
	}
}

// TestApplyRegistrationKeepsTheCredential is the reason there are two field
// managers. Server-side apply removes fields a manager owns and then omits, so a
// single manager re-applying the registration with no token in hand would strip
// the credential and silently break the cluster.
func TestApplyRegistrationKeepsTheCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newClient()
	now := time.Now()

	desired := testSecret()
	if err := ApplyCredential(ctx, client, desired); err != nil {
		t.Fatalf("ApplyCredential returned unexpected error: %v", err)
	}

	// A later pass has no token to write — only the registration.
	registrationOnly := testSecret()
	registrationOnly.BearerToken = ""

	hasCredential, err := ApplyRegistration(ctx, client, registrationOnly, now)
	if err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}
	if !hasCredential {
		t.Error("ApplyRegistration reported no credential, but one was applied before it")
	}

	secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the secret back: %v", err)
	}
	if !hasBearerToken(secret) {
		t.Fatal("re-applying the registration stripped the credential")
	}
}

// The apply response is k2a-token-sync's only view of what it published, since it
// holds no read permission in ArgoCD's namespace. A Secret that lost its
// credential must therefore be reported as such.
func TestApplyRegistrationReportsAMissingCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newClient()
	now := time.Now()

	desired := testSecret()
	desired.BearerToken = ""

	hasCredential, err := ApplyRegistration(ctx, client, desired, now)
	if err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}
	if hasCredential {
		t.Error("ApplyRegistration reported a credential on a Secret that has none")
	}
}

func TestApplyMergesExtraLabelsAndAnnotations(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newClient()

	desired := testSecret()
	desired.ExtraLabels = map[string]string{"env": "prod"}
	desired.ExtraAnnotations = map[string]string{"owner": "platform"}

	if _, err := ApplyRegistration(ctx, client, desired, time.Now()); err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}

	secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the secret back: %v", err)
	}
	if secret.Labels["env"] != "prod" {
		t.Error("extra label was not merged")
	}
	if secret.Annotations["owner"] != "platform" {
		t.Error("extra annotation was not merged")
	}
	if secret.Labels[SecretTypeLabel] != "cluster" {
		t.Error("extra labels overwrote the ArgoCD secret-type label")
	}
}

func TestNeedsRefresh(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	ttl := 720 * time.Hour
	desired := testSecret()

	current := func() Fingerprint {
		return Fingerprint{
			Server:         desired.Server,
			DisplayName:    desired.DisplayName,
			Project:        desired.Project,
			CAHash:         HashCA(desired.CAData),
			TokenExpiresAt: now.Add(ttl),
		}
	}

	cases := []struct {
		name          string
		mutate        func(*Fingerprint)
		hasCredential bool
		want          RefreshReason
		wantSkip      bool
	}{
		{name: "fresh registration needs nothing", mutate: func(*Fingerprint) {}, hasCredential: true, wantSkip: true},
		{
			// Also the status-was-lost case: nothing recorded reads as reissue.
			name:          "nothing published yet",
			mutate:        func(f *Fingerprint) { *f = Fingerprint{} },
			hasCredential: true,
			want:          ReasonMissing,
		},
		{
			// The self-healing path: someone deleted or emptied the Secret.
			name:          "credential gone from the secret",
			mutate:        func(*Fingerprint) {},
			hasCredential: false,
			want:          ReasonNoToken,
		},
		{
			name:          "server drift",
			mutate:        func(f *Fingerprint) { f.Server = "https://10.9.9.9:6443" },
			hasCredential: true,
			want:          ReasonServerDrift,
		},
		{
			name:          "name drift",
			mutate:        func(f *Fingerprint) { f.DisplayName = "renamed" },
			hasCredential: true,
			want:          ReasonNameDrift,
		},
		{
			name:          "project drift",
			mutate:        func(f *Fingerprint) { f.Project = "other" },
			hasCredential: true,
			want:          ReasonProjectDrift,
		},
		{
			name:          "ca drift",
			mutate:        func(f *Fingerprint) { f.CAHash = HashCA([]byte("different")) },
			hasCredential: true,
			want:          ReasonCADrift,
		},
		{
			name:          "unrecorded expiry",
			mutate:        func(f *Fingerprint) { f.TokenExpiresAt = time.Time{} },
			hasCredential: true,
			want:          ReasonUnknownExpiry,
		},
		{
			// Half the lifetime is the refresh point, so a long outage still
			// leaves a wide margin before ArgoCD's credential dies.
			name:          "past half its lifetime",
			mutate:        func(f *Fingerprint) { f.TokenExpiresAt = now.Add(ttl/2 - time.Minute) },
			hasCredential: true,
			want:          ReasonExpiring,
		},
		{
			name:          "just inside half its lifetime",
			mutate:        func(f *Fingerprint) { f.TokenExpiresAt = now.Add(ttl/2 + time.Minute) },
			hasCredential: true,
			wantSkip:      true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			applied := current()
			tc.mutate(&applied)

			got := NeedsRefresh(applied, tc.hasCredential, desired, ttl, now)
			if tc.wantSkip {
				if got != "" {
					t.Fatalf("NeedsRefresh = %q, want no refresh", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("NeedsRefresh = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFingerprintRoundTrip(t *testing.T) {
	t.Parallel()

	// A fingerprint taken from what was applied must compare equal against the
	// same desired state, or every pass would reissue.
	desired := testSecret()
	desired.TokenExpiresAt = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	applied := desired.Fingerprint()
	if got := NeedsRefresh(applied, true, desired, 720*time.Hour, desired.TokenExpiresAt.Add(-700*time.Hour)); got != "" {
		t.Fatalf("NeedsRefresh = %q immediately after applying, want no refresh", got)
	}
}
