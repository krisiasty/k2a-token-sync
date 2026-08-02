package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// minimalSpec is what the API server hands over once schema defaults are
// applied, so the defaults appear here rather than being inferred by FromSpec.
func minimalSpec() v1alpha1.ClusterConnectionSpec {
	return v1alpha1.ClusterConnectionSpec{
		Endpoint: "10.0.0.10",
		ServiceAccount: &v1alpha1.ServiceAccountRef{
			Name:      defaultServiceAccountName,
			Namespace: defaultServiceAccountNS,
		},
		SelfServiceAccountName: defaultSelfServiceAccount,
		TokenTTL:               "720h",
		SelfTokenTTL:           "2160h",
		ExpiryWarnThreshold:    "2160h",
	}
}

func TestFromSpecDerivesNamesAndEndpoint(t *testing.T) {
	t.Parallel()

	cluster, err := FromSpec("downstream-1", minimalSpec())
	if err != nil {
		t.Fatalf("FromSpec returned unexpected error: %v", err)
	}

	if cluster.Endpoint != "10.0.0.10:6443" {
		t.Errorf("Endpoint = %q, want the default port applied", cluster.Endpoint)
	}
	if cluster.ServerURL() != "https://10.0.0.10:6443" {
		t.Errorf("ServerURL() = %q", cluster.ServerURL())
	}
	if cluster.Host() != "10.0.0.10" {
		t.Errorf("Host() = %q, want the port stripped", cluster.Host())
	}
	if cluster.SecretName != "cluster-downstream-1" {
		t.Errorf("SecretName = %q, want cluster-downstream-1", cluster.SecretName)
	}
	if cluster.CredentialsSecretName() != "downstream-1-credentials" {
		t.Errorf("CredentialsSecretName() = %q", cluster.CredentialsSecretName())
	}
	if cluster.DisplayName != "downstream-1" {
		t.Errorf("DisplayName = %q, want it defaulted to the object name", cluster.DisplayName)
	}
	if cluster.TokenTTL != 720*time.Hour {
		t.Errorf("TokenTTL = %v", cluster.TokenTTL)
	}
	if cluster.SelfTokenTTL != 2160*time.Hour {
		t.Errorf("SelfTokenTTL = %v", cluster.SelfTokenTTL)
	}
	if cluster.ServiceAccount.Name != defaultServiceAccountName {
		t.Errorf("ServiceAccount.Name = %q", cluster.ServiceAccount.Name)
	}
}

// An empty spec is what arrives if the CRD's defaults were ever dropped, so
// FromSpec applies the same values rather than producing a cluster with a zero
// token lifetime.
func TestFromSpecToleratesUndefaultedSpec(t *testing.T) {
	t.Parallel()

	cluster, err := FromSpec("downstream-1", v1alpha1.ClusterConnectionSpec{Endpoint: "10.0.0.10"})
	if err != nil {
		t.Fatalf("FromSpec returned unexpected error: %v", err)
	}
	if cluster.TokenTTL != defaultTokenTTL {
		t.Errorf("TokenTTL = %v, want the package default", cluster.TokenTTL)
	}
	if cluster.SelfTokenTTL != defaultSelfTokenTTL {
		t.Errorf("SelfTokenTTL = %v, want the package default", cluster.SelfTokenTTL)
	}
	if cluster.ServiceAccount.Name != defaultServiceAccountName ||
		cluster.ServiceAccount.Namespace != defaultServiceAccountNS {
		t.Errorf("ServiceAccount = %+v, want the argocd-manager default", cluster.ServiceAccount)
	}
	if cluster.SelfServiceAccountName != defaultSelfServiceAccount {
		t.Errorf("SelfServiceAccountName = %q", cluster.SelfServiceAccountName)
	}
}

func TestFromSpecOverrides(t *testing.T) {
	t.Parallel()

	spec := minimalSpec()
	spec.Endpoint = "https://cluster.example.com:8443/"
	spec.DisplayName = "shown-in-argocd"
	spec.SecretName = "cluster-custom"
	spec.Project = "platform"
	spec.TokenTTL = "12h"
	spec.ServiceAccount = &v1alpha1.ServiceAccountRef{Name: "argo-sa", Namespace: "argo-system"}

	cluster, err := FromSpec("downstream-1", spec)
	if err != nil {
		t.Fatalf("FromSpec returned unexpected error: %v", err)
	}

	if cluster.Endpoint != "cluster.example.com:8443" {
		t.Errorf("Endpoint = %q, want the scheme and trailing slash stripped", cluster.Endpoint)
	}
	if cluster.DisplayName != "shown-in-argocd" {
		t.Errorf("DisplayName = %q", cluster.DisplayName)
	}
	if cluster.SecretName != "cluster-custom" {
		t.Errorf("SecretName = %q", cluster.SecretName)
	}
	if cluster.Project != "platform" {
		t.Errorf("Project = %q", cluster.Project)
	}
	if cluster.TokenTTL != 12*time.Hour {
		t.Errorf("TokenTTL = %v", cluster.TokenTTL)
	}
	if cluster.ServiceAccount.Name != "argo-sa" || cluster.ServiceAccount.Namespace != "argo-system" {
		t.Errorf("ServiceAccount = %+v", cluster.ServiceAccount)
	}
}

func TestFromSpecRejects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		objName string
		mutate  func(*v1alpha1.ClusterConnectionSpec)
		want    string
	}{
		{
			name:    "empty name",
			objName: "",
			mutate:  func(*v1alpha1.ClusterConnectionSpec) {},
			want:    "name must not be empty",
		},
		{
			// Secret names are derived from it, so an over-long name would
			// produce objects the API server rejects.
			name:    "name too long for derived secret names",
			objName: strings.Repeat("a", maxClusterNameLength+1),
			mutate:  func(*v1alpha1.ClusterConnectionSpec) {},
			want:    "exceeds 63 characters",
		},
		{
			name:    "empty endpoint",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.Endpoint = "" },
			want:    "endpoint must not be empty",
		},
		{
			name:    "endpoint with a path",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.Endpoint = "example.com/k8s/clusters/local" },
			want:    "not a URL path",
		},
		{
			name:    "non-numeric port",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.Endpoint = "example.com:https" },
			want:    "non-numeric port",
		},
		{
			name:    "token ttl below the floor",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.TokenTTL = "30s" },
			want:    "must be at least",
		},
		{
			name:    "self token ttl below the floor",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.SelfTokenTTL = "1s" },
			want:    "must be at least",
		},
		{
			name:    "unparsable duration",
			objName: "a",
			mutate:  func(s *v1alpha1.ClusterConnectionSpec) { s.TokenTTL = "soon" },
			want:    "not a valid duration",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := minimalSpec()
			tc.mutate(&spec)

			_, err := FromSpec(tc.objName, spec)
			if err == nil {
				t.Fatalf("FromSpec accepted an invalid spec, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("FromSpec error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestFromSpecRejectsControllerOwnedSecretMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		field string
		key   string
	}{
		{field: "labels", key: SecretTypeLabel},
		{field: "labels", key: ManagedByLabel},
		{field: "annotations", key: ClusterNameAnnotation},
		{field: "annotations", key: TokenExpiryAnnotation},
		{field: "annotations", key: ServingCertExpiryAnnotation},
	}

	for _, tc := range cases {
		t.Run(tc.field+"/"+tc.key, func(t *testing.T) {
			t.Parallel()

			spec := minimalSpec()
			if tc.field == "labels" {
				spec.Labels = map[string]string{tc.key: "user-controlled"}
			} else {
				spec.Annotations = map[string]string{tc.key: "user-controlled"}
			}

			_, err := FromSpec("downstream-1", spec)
			if err == nil {
				t.Fatalf("FromSpec accepted controller-owned %s key %q", tc.field, tc.key)
			}
			for _, want := range []string{"spec." + tc.field, tc.key, "reserved", "remove it"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("FromSpec error = %q, want it to mention %q", err, want)
				}
			}
		})
	}
}

func TestFromSpecAcceptsOrdinarySecretMetadata(t *testing.T) {
	t.Parallel()

	spec := minimalSpec()
	spec.Labels = map[string]string{"environment": "production"}
	spec.Annotations = map[string]string{"owner": "platform"}

	cluster, err := FromSpec("downstream-1", spec)
	if err != nil {
		t.Fatalf("FromSpec rejected ordinary custom metadata: %v", err)
	}
	if cluster.Labels["environment"] != "production" || cluster.Annotations["owner"] != "platform" {
		t.Errorf("custom metadata was not preserved: labels=%v annotations=%v", cluster.Labels, cluster.Annotations)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "k2a-token-sync")

	cfg, err := Load(discardLogger())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Namespace != "k2a-token-sync" {
		t.Errorf("Namespace = %q", cfg.Namespace)
	}
	if cfg.ArgoCDNamespace != defaultArgoCDNamespace {
		t.Errorf("ArgoCDNamespace = %q, want the default", cfg.ArgoCDNamespace)
	}
	if cfg.HealthPort != "8080" {
		t.Errorf("HealthPort = %q, want the default", cfg.HealthPort)
	}
}

func TestLoadRejectsBadEnvironment(t *testing.T) {
	t.Run("namespace is required", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		if _, err := Load(discardLogger()); err == nil {
			t.Fatal("Load succeeded without POD_NAMESPACE")
		}
	})

	t.Run("health port must be a port", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "k2a-token-sync")
		t.Setenv("HEALTH_PORT", "not-a-port")
		if _, err := Load(discardLogger()); err == nil {
			t.Fatal("Load accepted a non-numeric HEALTH_PORT")
		}
	})
}

func TestBootstrapClusterMatchesFromSpec(t *testing.T) {
	t.Parallel()

	// The bootstrap subcommand and an object resolved from the API must agree on
	// every field, or a bootstrapped cluster will not match the ClusterConnection
	// that describes it.
	//
	// Compared whole, deliberately. An earlier version listed the fields it cared
	// about, so when SelfTokenTTL was added to Cluster and left unset on the
	// bootstrap path this test stayed green and bootstrap asked the API server for
	// a token with expirationSeconds: 0. Comparing the struct means the next field
	// cannot be forgotten the same way.
	fromFlags, err := BootstrapCluster(BootstrapClusterInput{Name: "standalone-1", Endpoint: "10.1.0.10"})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	spec := minimalSpec()
	spec.Endpoint = "10.1.0.10"
	fromAPI, err := FromSpec("standalone-1", spec)
	if err != nil {
		t.Fatalf("FromSpec returned unexpected error: %v", err)
	}

	if !reflect.DeepEqual(fromFlags, fromAPI) {
		t.Errorf("the two paths disagree:\nbootstrap %+v\nAPI       %+v", fromFlags, fromAPI)
	}

	// Nothing may be left at its zero value: every duration reaches an API server,
	// where zero is not "unset" but a rejected request.
	if fromFlags.TokenTTL <= 0 || fromFlags.SelfTokenTTL <= 0 || fromFlags.ExpiryWarnThreshold <= 0 {
		t.Errorf("a duration was left unset: tokenTTL=%s selfTokenTTL=%s expiryWarnThreshold=%s",
			fromFlags.TokenTTL, fromFlags.SelfTokenTTL, fromFlags.ExpiryWarnThreshold)
	}
}

// The flags exist to override the defaults, so check they still arrive.
func TestBootstrapClusterHonoursItsFlags(t *testing.T) {
	t.Parallel()

	cluster, err := BootstrapCluster(BootstrapClusterInput{
		Name:                    "standalone-1",
		Endpoint:                "10.1.0.10",
		ServiceAccountName:      "argo",
		ServiceAccountNamespace: "platform",
		SelfServiceAccountName:  "sync",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	if cluster.ServiceAccount.Name != "argo" || cluster.ServiceAccount.Namespace != "platform" {
		t.Errorf("ServiceAccount is %+v, want argo/platform", cluster.ServiceAccount)
	}
	if cluster.SelfServiceAccountName != "sync" {
		t.Errorf("SelfServiceAccountName is %q, want %q", cluster.SelfServiceAccountName, "sync")
	}

	// A partially-specified reference must still default the other half, exactly as
	// the schema's per-field defaults do.
	cluster, err = BootstrapCluster(BootstrapClusterInput{
		Name:               "standalone-1",
		Endpoint:           "10.1.0.10",
		ServiceAccountName: "argo",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}
	if cluster.ServiceAccount.Namespace != defaultServiceAccountNS {
		t.Errorf("Namespace is %q, want the default %q", cluster.ServiceAccount.Namespace, defaultServiceAccountNS)
	}
}
