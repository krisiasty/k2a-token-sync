package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestNormaliseEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "10.0.0.10", want: "10.0.0.10:6443"},
		{in: "10.0.0.10:6443", want: "10.0.0.10:6443"},
		{in: "10.0.0.10:16443", want: "10.0.0.10:16443"},
		{in: "https://10.0.0.10:6443", want: "10.0.0.10:6443"},
		{in: "https://rke2.example.com", want: "rke2.example.com:6443"},
		{in: "  rke2.example.com  ", want: "rke2.example.com:6443"},
		{in: "[2001:db8::1]:6443", want: "[2001:db8::1]:6443"},
		{in: "", wantErr: true},
		{in: "https://host/path", wantErr: true},
		{in: "host:0", wantErr: true},
		{in: "host:not-a-port", wantErr: true},
		{in: "2001:db8::1", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := normaliseEndpoint(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normaliseEndpoint(%q) = %q, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("normaliseEndpoint(%q) returned unexpected error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("normaliseEndpoint(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func loadFrom(t *testing.T, body string) (*Config, error) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing test config: %v", err)
	}
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("POD_NAMESPACE", "r2a-cert-sync")

	return Load(discardLogger())
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := loadFrom(t, `
rancher:
  url: https://rancher.example.com/
  tokenSecret:
    name: rancher-credentials
clusters:
  - name: downstream-1
    endpoint: 10.0.0.10
`)
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	if cfg.RefreshInterval != defaultRefreshInterval {
		t.Errorf("RefreshInterval = %v, want %v", cfg.RefreshInterval, defaultRefreshInterval)
	}
	if cfg.Rancher == nil {
		t.Fatal("Rancher section was not resolved")
	}
	// The trailing slash must be trimmed or every proxy URL gains a double slash.
	if cfg.Rancher.URL != "https://rancher.example.com" {
		t.Errorf("Rancher.URL = %q, want the trailing slash trimmed", cfg.Rancher.URL)
	}
	if cfg.Rancher.Token.Key != defaultRancherTokenKey {
		t.Errorf("Rancher.Token.Key = %q, want %q", cfg.Rancher.Token.Key, defaultRancherTokenKey)
	}

	if len(cfg.Clusters) != 1 {
		t.Fatalf("got %d clusters, want 1", len(cfg.Clusters))
	}
	c := cfg.Clusters[0]

	if c.Provider != ProviderRancher {
		t.Errorf("Provider = %q, want %q (the default)", c.Provider, ProviderRancher)
	}
	if c.Endpoint != "10.0.0.10:6443" {
		t.Errorf("Endpoint = %q, want the default port applied", c.Endpoint)
	}
	if c.ServerURL() != "https://10.0.0.10:6443" {
		t.Errorf("ServerURL() = %q", c.ServerURL())
	}
	if c.Host() != "10.0.0.10" {
		t.Errorf("Host() = %q, want the port stripped", c.Host())
	}
	if c.SecretName != "cluster-downstream-1" {
		t.Errorf("SecretName = %q, want cluster-downstream-1", c.SecretName)
	}
	if cfg.ArgoCDNamespace != defaultArgoCDNamespace {
		t.Errorf("ArgoCDNamespace = %q, want %q", cfg.ArgoCDNamespace, defaultArgoCDNamespace)
	}
	if c.DisplayName != "downstream-1" || c.RancherClusterName != "downstream-1" {
		t.Errorf("DisplayName/RancherClusterName not defaulted to name: %q/%q", c.DisplayName, c.RancherClusterName)
	}
	if c.ServiceAccount.Name != defaultServiceAccountName || c.ServiceAccount.Namespace != defaultServiceAccountNS {
		t.Errorf("ServiceAccount = %+v, want the argocd-manager default", c.ServiceAccount)
	}
	if c.TokenTTL != defaultTokenTTL {
		t.Errorf("TokenTTL = %v, want %v", c.TokenTTL, defaultTokenTTL)
	}
	if c.CredentialsSecretName() != "downstream-1-credentials" {
		t.Errorf("CredentialsSecretName() = %q", c.CredentialsSecretName())
	}
}

func TestLoadDefaultsSectionAppliesToClusters(t *testing.T) {
	cfg, err := loadFrom(t, `
argocdNamespace: gitops
defaults:
  tokenTTL: 48h
  refreshInterval: 6h
  serviceAccount:
    name: argo-sa
    namespace: argo-system
clusters:
  - name: a
    provider: direct
    endpoint: 10.0.0.1
    bootstrapSecret:
      name: a-bootstrap
  - name: b
    provider: direct
    endpoint: 10.0.0.2
    bootstrapSecret:
      name: b-bootstrap
    tokenTTL: 12h
`)
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	if cfg.RefreshInterval != 6*time.Hour {
		t.Errorf("RefreshInterval = %v, want 6h", cfg.RefreshInterval)
	}
	if got := cfg.Clusters[0].TokenTTL; got != 48*time.Hour {
		t.Errorf("clusters[0].TokenTTL = %v, want the default of 48h", got)
	}
	if got := cfg.Clusters[1].TokenTTL; got != 12*time.Hour {
		t.Errorf("clusters[1].TokenTTL = %v, want its own override of 12h", got)
	}
	if cfg.ArgoCDNamespace != "gitops" {
		t.Errorf("ArgoCDNamespace = %q, want gitops", cfg.ArgoCDNamespace)
	}
	for i, c := range cfg.Clusters {
		if c.ServiceAccount.Name != "argo-sa" || c.ServiceAccount.Namespace != "argo-system" {
			t.Errorf("clusters[%d].ServiceAccount = %+v, want the defaults section applied", i, c.ServiceAccount)
		}
	}
}

func TestLoadRejects(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "no clusters",
			body: "clusters: []\n",
			want: "no clusters configured",
		},
		{
			name: "duplicate cluster name",
			body: `
clusters:
  - {name: a, provider: direct, endpoint: 10.0.0.1, bootstrapSecret: {name: s}}
  - {name: a, provider: direct, endpoint: 10.0.0.2, bootstrapSecret: {name: s}}
`,
			want: "duplicate cluster name",
		},
		{
			// Two clusters writing one Secret would silently overwrite each other.
			name: "colliding target secrets",
			body: `
clusters:
  - {name: a, provider: direct, endpoint: 10.0.0.1, secretName: shared, bootstrapSecret: {name: s}}
  - {name: b, provider: direct, endpoint: 10.0.0.2, secretName: shared, bootstrapSecret: {name: s}}
`,
			want: "already generated by cluster",
		},
		{
			// secretNamespace was removed: one daemon serves one ArgoCD instance.
			name: "per-cluster secretNamespace is rejected",
			body: `
clusters:
  - {name: a, provider: direct, endpoint: 10.0.0.1, secretNamespace: gitops}
`,
			want: "parsing config file",
		},
		{
			name: "rancher provider without rancher section",
			body: "clusters:\n  - {name: a, endpoint: 10.0.0.1}\n",
			want: "no rancher section is configured",
		},
		{
			name: "empty bootstrap secret name",
			body: "clusters:\n  - {name: a, provider: direct, endpoint: 10.0.0.1, bootstrapSecret: {name: \"\"}}\n",
			want: "bootstrapSecret.name is empty",
		},
		{
			// Standalone RKE2 has no remote rotation API, so this can never work.
			name: "autoRotate on a direct cluster",
			body: `
clusters:
  - {name: a, provider: direct, endpoint: 10.0.0.1, autoRotate: true, bootstrapSecret: {name: s}}
`,
			want: "autoRotate requires provider",
		},
		{
			name: "unknown provider",
			body: "clusters:\n  - {name: a, provider: magic, endpoint: 10.0.0.1}\n",
			want: "unknown provider",
		},
		{
			name: "unknown field",
			body: "clusters:\n  - {name: a, endpoint: 10.0.0.1, wat: true}\n",
			want: "parsing config file",
		},
		{
			name: "token ttl below the floor",
			body: `
rancher: {url: https://r.example.com, tokenSecret: {name: t}}
clusters:
  - {name: a, endpoint: 10.0.0.1, tokenTTL: 30s}
`,
			want: "must be at least",
		},
		{
			name: "rancher url without scheme",
			body: `
rancher: {url: rancher.example.com, tokenSecret: {name: t}}
clusters:
  - {name: a, endpoint: 10.0.0.1}
`,
			want: "must include a scheme",
		},
		{
			name: "bootstrap secret on a rancher cluster",
			body: `
rancher: {url: https://r.example.com, tokenSecret: {name: t}}
clusters:
  - {name: a, endpoint: 10.0.0.1, bootstrapSecret: {name: s}}
`,
			want: "only valid for provider",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadFrom(t, tc.body)
			if err == nil {
				t.Fatalf("Load accepted invalid config, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadRequiresPodNamespace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("clusters: []\n"), 0o600); err != nil {
		t.Fatalf("writing test config: %v", err)
	}
	t.Setenv("CONFIG_PATH", path)
	// Set it through t.Setenv first so the original value is restored, then
	// clear it for the duration of this test.
	t.Setenv("POD_NAMESPACE", "placeholder")
	if err := os.Unsetenv("POD_NAMESPACE"); err != nil {
		t.Fatalf("unsetting POD_NAMESPACE: %v", err)
	}

	if _, err := Load(discardLogger()); err == nil {
		t.Fatal("Load succeeded without POD_NAMESPACE")
	}
}

func TestBootstrapClusterMatchesConfigDefaults(t *testing.T) {
	t.Parallel()

	// The bootstrap subcommand and the config file must agree on names and
	// locations, or a bootstrapped cluster will not match its config entry.
	fromFlags, err := BootstrapCluster(BootstrapClusterInput{Name: "standalone-1", Endpoint: "10.1.0.10"})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	if fromFlags.Provider != ProviderDirect {
		t.Errorf("Provider = %q, want %q", fromFlags.Provider, ProviderDirect)
	}
	if fromFlags.Endpoint != "10.1.0.10:6443" {
		t.Errorf("Endpoint = %q", fromFlags.Endpoint)
	}
	if fromFlags.SecretName != "cluster-standalone-1" {
		t.Errorf("SecretName = %q, want cluster-standalone-1", fromFlags.SecretName)
	}
	if fromFlags.CredentialsSecretName() != "standalone-1-credentials" {
		t.Errorf("CredentialsSecretName() = %q", fromFlags.CredentialsSecretName())
	}
	if fromFlags.ServiceAccount.Name != defaultServiceAccountName {
		t.Errorf("ServiceAccount.Name = %q", fromFlags.ServiceAccount.Name)
	}
	if fromFlags.AgentServiceAccountName != defaultAgentServiceAccount {
		t.Errorf("AgentServiceAccountName = %q", fromFlags.AgentServiceAccountName)
	}
}
