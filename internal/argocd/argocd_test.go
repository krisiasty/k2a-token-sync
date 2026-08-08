package argocd

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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

	if _, err := ApplyCredential(ctx, client, desired); err != nil {
		t.Fatalf("ApplyCredential returned unexpected error: %v", err)
	}
	if _, err := ApplyRegistration(ctx, client, desired); err != nil {
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

	desired := testSecret()
	if _, err := ApplyCredential(ctx, client, desired); err != nil {
		t.Fatalf("ApplyCredential returned unexpected error: %v", err)
	}

	// A later pass has no token to write — only the registration.
	registrationOnly := testSecret()
	registrationOnly.BearerToken = ""

	published, err := ApplyRegistration(ctx, client, registrationOnly)
	if err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}
	if published.CredentialHash == "" {
		t.Error("ApplyRegistration reported no credential, but one was applied before it")
	}

	secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the secret back: %v", err)
	}
	if hashCredential(secret) == "" {
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

	desired := testSecret()
	desired.BearerToken = ""

	published, err := ApplyRegistration(ctx, client, desired)
	if err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}
	if published.CredentialHash != "" {
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

	if _, err := ApplyRegistration(ctx, client, desired); err != nil {
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

// Controller-owned values are assigned after custom metadata, so direct callers
// cannot turn an ArgoCD cluster Secret into something else or falsify its
// bookkeeping.
func TestRegistrationIgnoresControllerOwnedExtraMetadata(t *testing.T) {
	t.Parallel()

	desired := testSecret()
	desired.TokenExpiresAt = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	desired.ServingCertExpiresAt = time.Date(2027, 7, 31, 12, 0, 0, 0, time.UTC)
	desired.ExtraLabels = map[string]string{
		SecretTypeLabel: "repository",
		ManagedByLabel:  "somebody-else",
	}
	desired.ExtraAnnotations = map[string]string{
		ClusterNameAnnotation:       "another-cluster",
		TokenExpiryAnnotation:       "never",
		ServingCertExpiryAnnotation: "never",
	}

	registration := desired.registrationConfig()
	wantLabels := map[string]string{
		SecretTypeLabel: "cluster",
		ManagedByLabel:  ManagedByValue,
	}
	wantAnnotations := map[string]string{
		ClusterNameAnnotation:       desired.ClusterName,
		TokenExpiryAnnotation:       desired.TokenExpiresAt.UTC().Format(time.RFC3339),
		ServingCertExpiryAnnotation: desired.ServingCertExpiresAt.UTC().Format(time.RFC3339),
	}
	if !reflect.DeepEqual(registration.Labels, wantLabels) {
		t.Errorf("labels = %v, want controller-owned values %v", registration.Labels, wantLabels)
	}
	if !reflect.DeepEqual(registration.Annotations, wantAnnotations) {
		t.Errorf("annotations = %v, want controller-owned values %v", registration.Annotations, wantAnnotations)
	}
}

func TestExpiryAnnotationsCannotBeFakedWhenNothingWasPublished(t *testing.T) {
	t.Parallel()

	desired := testSecret()
	fakeExpiry := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	desired.ExtraAnnotations = map[string]string{
		TokenExpiryAnnotation:       fakeExpiry,
		ServingCertExpiryAnnotation: fakeExpiry,
	}

	got := desired.registrationConfig().Annotations
	for _, key := range []string{TokenExpiryAnnotation, ServingCertExpiryAnnotation} {
		if value, found := got[key]; found {
			t.Errorf("%s = %q, want it absent: nothing was published, so there is no expiry to record", key, value)
		}
	}
}

// Re-applying the registration on an unchanged cluster has to be a genuine
// no-op, or the reconciliation interval cannot be short: a write ArgoCD sees
// every few minutes would churn its cluster cache continuously. That holds only
// while nothing written here is derived from the clock, which a single innocuous
// annotation — a last-sync stamp, say — would quietly undo.
//
// So the annotation keys are pinned. Adding one that changes on its own is then a
// test failure rather than a slow rediscovery of why passes used to be daily.
func TestRegistrationWritesNothingDerivedFromTheClock(t *testing.T) {
	t.Parallel()

	desired := testSecret()
	desired.TokenExpiresAt = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	desired.ServingCertExpiresAt = time.Date(2027, 7, 31, 12, 0, 0, 0, time.UTC)

	config := desired.registrationConfig()

	want := map[string]bool{
		ClusterNameAnnotation:       true,
		TokenExpiryAnnotation:       true,
		ServingCertExpiryAnnotation: true,
	}
	for key := range config.Annotations {
		if !want[key] {
			t.Errorf("unexpected annotation %q: if its value can change on its own, "+
				"every pass becomes a write and an event for ArgoCD", key)
		}
		delete(want, key)
	}
	for key := range want {
		t.Errorf("annotation %q is no longer written", key)
	}

	// Both remaining timestamps describe something observed, so they hold still
	// between passes while the thing they describe does.
	if got := config.Annotations[TokenExpiryAnnotation]; got != "2026-08-30T12:00:00Z" {
		t.Errorf("token expiry annotation = %q", got)
	}
	if got := config.Annotations[ServingCertExpiryAnnotation]; got != "2027-07-31T12:00:00Z" {
		t.Errorf("serving cert expiry annotation = %q", got)
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

	const publishedHash = "the-credential-this-tool-published"

	cases := []struct {
		name      string
		mutate    func(*Fingerprint)
		published string
		want      RefreshReason
		wantSkip  bool
	}{
		{name: "fresh registration needs nothing", mutate: func(*Fingerprint) {}, published: publishedHash, wantSkip: true},
		{
			// Also the status-was-lost case: nothing recorded reads as reissue.
			name:      "nothing published yet",
			mutate:    func(f *Fingerprint) { *f = Fingerprint{} },
			published: publishedHash,
			want:      ReasonUnrecorded,
		},
		{
			// The self-healing path: someone deleted or emptied the Secret.
			name:      "credential gone from the secret",
			mutate:    func(*Fingerprint) {},
			published: "",
			want:      ReasonNoToken,
		},
		{
			name:      "server drift",
			mutate:    func(f *Fingerprint) { f.Server = "https://10.9.9.9:6443" },
			published: publishedHash,
			want:      ReasonServerDrift,
		},
		{
			name:      "name drift",
			mutate:    func(f *Fingerprint) { f.DisplayName = "renamed" },
			published: publishedHash,
			want:      ReasonNameDrift,
		},
		{
			name:      "project drift",
			mutate:    func(f *Fingerprint) { f.Project = "other" },
			published: publishedHash,
			want:      ReasonProjectDrift,
		},
		{
			name:      "ca drift",
			mutate:    func(f *Fingerprint) { f.CAHash = HashCA([]byte("different")) },
			published: publishedHash,
			want:      ReasonCADrift,
		},
		{
			name:      "unrecorded expiry",
			mutate:    func(f *Fingerprint) { f.TokenExpiresAt = time.Time{} },
			published: publishedHash,
			want:      ReasonUnknownExpiry,
		},
		{
			// Half the lifetime is the refresh point, so a long outage still
			// leaves a wide margin before ArgoCD's credential dies.
			//
			// No issue time here, nor in any case above: they exercise the fallback
			// for a status written before it was recorded, which must keep behaving
			// exactly as it used to.
			name:      "past half its lifetime",
			mutate:    func(f *Fingerprint) { f.TokenExpiresAt = now.Add(ttl/2 - time.Minute) },
			published: publishedHash,
			want:      ReasonExpiring,
		},
		{
			name:      "just inside half its lifetime",
			mutate:    func(f *Fingerprint) { f.TokenExpiresAt = now.Add(ttl/2 + time.Minute) },
			published: publishedHash,
			wantSkip:  true,
		},
		{
			// Somebody else wrote over the credential. Nothing else about the Secret
			// has to change for this to be true, which is why no other comparison
			// here can see it.
			name: "credential replaced by another writer",
			mutate: func(f *Fingerprint) {
				f.CredentialHash = "the-digest-this-tool-recorded"
			},
			published: "a-completely-different-credential",
			want:      ReasonCredentialReplaced,
		},
		{
			// The same digest is the ordinary case, and must not reissue.
			name: "credential is the one that was published",
			mutate: func(f *Fingerprint) {
				f.CredentialHash = publishedHash
			},
			published: publishedHash,
			wantSkip:  true,
		},
		{
			// A status written before digests were recorded has none. Comparing
			// against nothing would reissue every cluster at once on upgrade, so the
			// check waits for the next reissue to record one.
			name:      "no digest recorded yet",
			mutate:    func(f *Fingerprint) { f.CredentialHash = "" },
			published: "anything at all",
			wantSkip:  true,
		},
		{
			// The case the issue time exists for. Against half the *requested*
			// lifetime, a token the API server capped at an hour read as expiring
			// from the instant it was minted — so a conservatively configured
			// cluster reissued on every pass, forever, churning ArgoCD's cache.
			name: "granted an hour, ten minutes gone",
			mutate: func(f *Fingerprint) {
				f.TokenIssuedAt = now.Add(-10 * time.Minute)
				f.TokenExpiresAt = now.Add(50 * time.Minute)
			},
			published: publishedHash,
			wantSkip:  true,
		},
		{
			// And it must still be reissued in time. Half of an hour, not half of
			// the 720 hours nobody granted.
			name: "granted an hour, past half of it",
			mutate: func(f *Fingerprint) {
				f.TokenIssuedAt = now.Add(-31 * time.Minute)
				f.TokenExpiresAt = now.Add(29 * time.Minute)
			},
			published: publishedHash,
			want:      ReasonExpiring,
		},
		{
			// Granted what was asked for: the same answer either way, which is why
			// recording the issue time changes nothing for an ordinary cluster.
			name: "issue time recorded, full lifetime granted",
			mutate: func(f *Fingerprint) {
				f.TokenIssuedAt = now.Add(-ttl/2 - time.Minute)
				f.TokenExpiresAt = now.Add(ttl/2 - time.Minute)
			},
			published: publishedHash,
			want:      ReasonExpiring,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			applied := current()
			tc.mutate(&applied)

			got := NeedsRefresh(applied, tc.published, desired, ttl, now)
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
	issued := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	desired := testSecret()
	desired.TokenIssuedAt = issued
	desired.TokenExpiresAt = issued.Add(720 * time.Hour)

	const published = "digest-of-what-was-published"

	applied := desired.Fingerprint()
	applied.CredentialHash = published
	if applied.TokenIssuedAt != issued {
		t.Errorf("the fingerprint dropped the issue time: %v", applied.TokenIssuedAt)
	}
	if got := NeedsRefresh(applied, published, desired, 720*time.Hour, issued.Add(time.Minute)); got != "" {
		t.Fatalf("NeedsRefresh = %q immediately after applying, want no refresh", got)
	}
}

// The digest is the whole mechanism, so what it does and does not distinguish
// matters. Two credentials that differ in any way must hash differently, and
// anything that is not a usable credential must hash to nothing at all —
// otherwise "no credential" and "somebody else's credential" become the same
// answer, and they call for opposite reactions.
func TestHashCredential(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	published := func(t *testing.T, mutate func(*ClusterSecret)) string {
		t.Helper()
		client := newClient()
		desired := testSecret()
		mutate(&desired)
		if _, err := ApplyCredential(ctx, client, desired); err != nil {
			t.Fatalf("ApplyCredential returned unexpected error: %v", err)
		}
		secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading the secret back: %v", err)
		}
		return hashCredential(secret)
	}

	base := published(t, func(*ClusterSecret) {})
	if base == "" {
		t.Fatal("a published credential hashed to nothing")
	}

	same := published(t, func(*ClusterSecret) {})
	if same != base {
		t.Error("the same credential hashed differently twice; every pass would reissue")
	}

	otherToken := published(t, func(c *ClusterSecret) { c.BearerToken = "a-different-token" })
	if otherToken == base {
		t.Error("a different bearer token hashed the same; a replaced credential would go unnoticed")
	}

	// The CA travels in the same payload, and a hand-edited one breaks ArgoCD's
	// connection just as thoroughly as a bad token.
	otherCA := published(t, func(c *ClusterSecret) {
		c.CAData = []byte("-----BEGIN CERTIFICATE-----\nDIFFERENT\n-----END CERTIFICATE-----\n")
	})
	if otherCA == base {
		t.Error("a different CA bundle hashed the same")
	}

	// Absent, and present-but-empty, both mean there is no credential.
	for _, tc := range []struct {
		name   string
		secret *corev1.Secret
	}{
		{"no config key", &corev1.Secret{}},
		{"config is not json", &corev1.Secret{Data: map[string][]byte{"config": []byte("{")}}},
		{"config carries no token", &corev1.Secret{Data: map[string][]byte{"config": []byte(`{"tlsClientConfig":{}}`)}}},
	} {
		if got := hashCredential(tc.secret); got != "" {
			t.Errorf("%s hashed to %q, want the empty string that means no credential", tc.name, got)
		}
	}
}

// Publishing a credential takes two calls, and something can write between them.
//
// If the digest recorded as "mine" came from the second call's response, that
// writer's credential would be adopted as this tool's own and the comparison
// would be satisfied by it from then on — the detection defeated precisely when
// it is needed. So ApplyCredential reports what it sent, and only the observation
// comes from the response.
func TestTheRecordedDigestIsWhatWasSentNotWhatCameBack(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newClient()
	desired := testSecret()

	written, err := ApplyCredential(ctx, client, desired)
	if err != nil {
		t.Fatalf("ApplyCredential returned unexpected error: %v", err)
	}
	if written == "" {
		t.Fatal("ApplyCredential reported no digest for a credential it published")
	}

	// Another writer replaces the credential before the registration apply runs.
	intruder := `{"bearerToken":"not-the-token-this-tool-minted","tlsClientConfig":{"insecure":false}}`
	secret, err := client.CoreV1().Secrets("argocd").Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the secret back: %v", err)
	}
	secret.Data["config"] = []byte(intruder)
	if _, err := client.CoreV1().Secrets("argocd").Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("simulating another writer: %v", err)
	}

	observed, err := ApplyRegistration(ctx, client, desired)
	if err != nil {
		t.Fatalf("ApplyRegistration returned unexpected error: %v", err)
	}

	if observed.CredentialHash == written {
		t.Fatal("the two calls reported the same digest; this test is no longer simulating interference")
	}
	if observed.CredentialHash != hashConfig([]byte(intruder)) {
		t.Errorf("the observation is %q, want the intruder's credential — the response reports what is there",
			observed.CredentialHash)
	}

	// The recorded value must still identify this tool's own credential, so the
	// next pass sees the mismatch rather than blessing the replacement.
	if reason := NeedsRefresh(fingerprintWith(written), observed.CredentialHash, desired,
		720*time.Hour, time.Now()); reason != ReasonCredentialReplaced {
		t.Errorf("NeedsRefresh = %q, want %q", reason, ReasonCredentialReplaced)
	}
	if reason := NeedsRefresh(fingerprintWith(observed.CredentialHash), observed.CredentialHash, desired,
		720*time.Hour, time.Now()); reason == ReasonCredentialReplaced {
		t.Error("recording the response's digest hides the replacement entirely, which is the bug this guards")
	}
}

// fingerprintWith builds a fingerprint matching testSecret, carrying the given
// credential digest, so only that field decides the outcome.
func fingerprintWith(credential string) Fingerprint {
	desired := testSecret()
	return Fingerprint{
		Server:         desired.Server,
		DisplayName:    desired.DisplayName,
		Project:        desired.Project,
		CAHash:         HashCA(desired.CAData),
		TokenExpiresAt: time.Now().Add(700 * time.Hour),
		CredentialHash: credential,
	}
}

// ForeignManagers is the only signal available to something that cannot read the
// Secret it writes. Force takes the fields this tool manages but does not evict the
// previous manager, so an entry survives for anything it set that this tool does
// not manage — and that residue is what distinguishes a Secret this tool created
// from one it took over.
func TestForeignManagersNamesOnlyOtherManagers(t *testing.T) {
	t.Parallel()

	managed := func(names ...string) *corev1.Secret {
		entries := make([]metav1.ManagedFieldsEntry, 0, len(names))
		for _, name := range names {
			entries = append(entries, metav1.ManagedFieldsEntry{Manager: name, Operation: metav1.ManagedFieldsOperationApply})
		}
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-standalone-1", Namespace: "argocd", ManagedFields: entries,
		}}
	}

	cases := []struct {
		name     string
		managers []string
		want     []string
	}{
		{
			// A Secret this tool created. Both of its own managers are present, because
			// the registration and the credential are deliberately applied separately.
			name:     "only this tool",
			managers: []string{FieldManagerRegistration, FieldManagerCredential},
			want:     nil,
		},
		{
			// The migration from 'argocd cluster add', and also what a collided cluster
			// name looks like. Nothing here can tell them apart.
			name:     "taken over from argocd",
			managers: []string{FieldManagerRegistration, "argocd", FieldManagerCredential},
			want:     []string{"argocd"},
		},
		{
			// Sorted and deduplicated, because the result goes into a status message and
			// a value that reshuffles between passes reads as something having changed.
			name:     "several, repeated and out of order",
			managers: []string{"kubectl-client-side-apply", "argocd", "argocd", FieldManagerRegistration},
			want:     []string{"argocd", "kubectl-client-side-apply"},
		},
		{
			// An entry with no manager names nobody, so it cannot be reported as a party
			// to anything.
			name:     "an unnamed entry",
			managers: []string{FieldManagerRegistration, ""},
			want:     nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ForeignManagers(managed(tc.managers...))
			if !slices.Equal(got, tc.want) {
				t.Errorf("ForeignManagers = %v, want %v", got, tc.want)
			}
		})
	}
}

// The constraint the whole scoping feature turns on. A connection that never
// mentioned namespaces or clusterResources must produce exactly the Secret it
// produced before those fields existed — not one with empty keys added. An
// extra key would be written on the next pass for every registration in
// service, which is a write and an ArgoCD cache churn per cluster for a change
// nobody asked for.
//
// ArgoCD also reads an absent namespaces key as "every namespace", so an empty
// one would not merely be noise: it would change what the Secret means.
func TestAnUnscopedRegistrationWritesNoScopingKeys(t *testing.T) {
	t.Parallel()

	desired := ClusterSecret{
		Name: "cluster-downstream-1", Namespace: "argocd",
		DisplayName: "downstream-1", Server: "https://10.0.0.10:6443",
	}
	config := desired.registrationConfig()

	got := make([]string, 0, len(config.Data))
	for k := range config.Data {
		got = append(got, k)
	}
	sort.Strings(got)

	// Pinned exactly rather than checked for absence: a future key added
	// unconditionally would pass an absence check and still churn every Secret.
	if want := []string{"name", "server"}; !reflect.DeepEqual(got, want) {
		t.Errorf("data keys = %v, want exactly %v", got, want)
	}
}

// The format is ArgoCD's, not ours: it reads namespaces as a comma-separated
// list and clusterResources as a quoted boolean. Getting either shape wrong
// produces a Secret ArgoCD accepts and silently misreads.
func TestAScopedRegistrationWritesArgoCDsFormat(t *testing.T) {
	t.Parallel()

	yes := true
	desired := ClusterSecret{
		Name: "cluster-downstream-1", Namespace: "argocd",
		DisplayName: "downstream-1", Server: "https://10.0.0.10:6443",
		Namespaces: []string{"team-a", "team-b"}, ClusterResources: &yes,
	}
	config := desired.registrationConfig()

	if got := string(config.Data["namespaces"]); got != "team-a,team-b" {
		t.Errorf("namespaces = %q, want %q", got, "team-a,team-b")
	}
	if got := string(config.Data["clusterResources"]); got != "true" {
		t.Errorf("clusterResources = %q, want %q", got, "true")
	}
}

// false is a value the operator chose and must be written; only nil means they
// never mentioned it. Collapsing the two would make it impossible to say
// "these namespaces, and no cluster-scoped resources" — the tightest scoping
// the feature can express, and the one most likely to be wanted.
func TestClusterResourcesFalseIsWrittenRatherThanOmitted(t *testing.T) {
	t.Parallel()

	no := false
	desired := ClusterSecret{
		Name: "cluster-downstream-1", Namespace: "argocd",
		DisplayName: "downstream-1", Server: "https://10.0.0.10:6443",
		Namespaces: []string{"team-a"}, ClusterResources: &no,
	}
	config := desired.registrationConfig()

	raw, ok := config.Data["clusterResources"]
	if !ok {
		t.Fatal("clusterResources was omitted, so an explicit false cannot be expressed")
	}
	if string(raw) != "false" {
		t.Errorf("clusterResources = %q, want %q", raw, "false")
	}
}
