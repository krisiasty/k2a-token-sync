package argocd

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
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

func TestRenderProducesArgoCDClusterSecret(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	expires := now.Add(720 * time.Hour)

	desired := testSecret()
	desired.TokenExpiresAt = expires
	desired.ServingCertExpiresAt = now.Add(200 * 24 * time.Hour)

	secret, err := desired.Render(now)
	if err != nil {
		t.Fatalf("Render returned unexpected error: %v", err)
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

func TestRenderMergesExtraLabelsAndAnnotations(t *testing.T) {
	t.Parallel()

	desired := testSecret()
	desired.ExtraLabels = map[string]string{"env": "prod"}
	desired.ExtraAnnotations = map[string]string{"owner": "platform"}

	secret, err := desired.Render(time.Now())
	if err != nil {
		t.Fatalf("Render returned unexpected error: %v", err)
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

	current := func() *Observed {
		return &Observed{
			Exists:         true,
			Server:         desired.Server,
			DisplayName:    desired.DisplayName,
			Project:        desired.Project,
			CAData:         desired.CAData,
			HasBearerToken: true,
			TokenExpiresAt: now.Add(ttl),
		}
	}

	cases := []struct {
		name     string
		mutate   func(*Observed)
		want     RefreshReason
		wantSkip bool
	}{
		{name: "fresh registration needs nothing", mutate: func(*Observed) {}, wantSkip: true},
		{name: "missing secret", mutate: func(o *Observed) { o.Exists = false }, want: ReasonMissing},
		{name: "no token", mutate: func(o *Observed) { o.HasBearerToken = false }, want: ReasonNoToken},
		{name: "server drift", mutate: func(o *Observed) { o.Server = "https://10.9.9.9:6443" }, want: ReasonServerDrift},
		{name: "name drift", mutate: func(o *Observed) { o.DisplayName = "renamed" }, want: ReasonNameDrift},
		{name: "project drift", mutate: func(o *Observed) { o.Project = "other" }, want: ReasonProjectDrift},
		{name: "ca drift", mutate: func(o *Observed) { o.CAData = []byte("different") }, want: ReasonCADrift},
		{name: "unrecorded expiry", mutate: func(o *Observed) { o.TokenExpiresAt = time.Time{} }, want: ReasonUnknownExpiry},
		{
			// Half the lifetime is the refresh point, so a long outage still
			// leaves a wide margin before ArgoCD's credential dies.
			name:   "past half its lifetime",
			mutate: func(o *Observed) { o.TokenExpiresAt = now.Add(ttl/2 - time.Minute) },
			want:   ReasonExpiring,
		},
		{
			name:     "just inside half its lifetime",
			mutate:   func(o *Observed) { o.TokenExpiresAt = now.Add(ttl/2 + time.Minute) },
			wantSkip: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observed := current()
			tc.mutate(observed)

			got := NeedsRefresh(observed, desired, ttl, now)
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
