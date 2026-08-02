// Package config resolves k2a-token-sync's runtime configuration.
//
// There are two sources and they are deliberately different in kind. Process
// settings — which namespace k2a-token-sync runs in, which ArgoCD instance it serves,
// which port serves health — come from the environment, because they are fixed
// for the lifetime of a deployment. The cluster inventory comes from
// ClusterConnection objects, because it changes while k2a-token-sync runs.
//
// This package owns the runtime Cluster type and the normalisation applied to it,
// so an object resolved from the API and a cluster prepared by the bootstrap
// subcommand agree on endpoints, names and Secret locations.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

const (
	defaultAPIPort = "6443"

	// SecretTypeLabel makes ArgoCD recognize a generated cluster Secret.
	SecretTypeLabel = "argocd.argoproj.io/secret-type" //nolint:gosec // a label key, not a credential

	// ManagedByLabel records which controller owns the generated Secret.
	ManagedByLabel = "app.kubernetes.io/managed-by"

	// TokenExpiryAnnotation records the expiry of the published credential.
	TokenExpiryAnnotation = "k2a-token-sync.io/token-expires-at" //nolint:gosec // an annotation key, not a credential

	// ServingCertExpiryAnnotation records the downstream serving certificate expiry.
	ServingCertExpiryAnnotation = "k2a-token-sync.io/serving-cert-expires-at"

	// ClusterNameAnnotation records which ClusterConnection owns the Secret.
	ClusterNameAnnotation = "k2a-token-sync.io/cluster"

	defaultTokenTTL     = 720 * time.Hour  // 30 days
	defaultSelfTokenTTL = 2160 * time.Hour // 90 days

	// defaultExpiryWarnThreshold gives three months' notice, which is enough to
	// schedule a certificate rotation or a control-plane restart properly rather
	// than as an emergency.
	defaultExpiryWarnThreshold = 2160 * time.Hour // 90 days

	defaultArgoCDNamespace    = "argocd"
	defaultServiceAccountName = "argocd-manager"
	defaultServiceAccountNS   = "kube-system"
	defaultSelfServiceAccount = "k2a-token-sync"

	minTokenTTL = 1 * time.Hour

	// maxClusterNameLength keeps every name derived from a cluster's name a
	// valid object name, since "cluster-<name>" and "<name>-credentials" are
	// both Secret names.
	maxClusterNameLength = 63
)

// Config is the resolved process configuration.
type Config struct {
	// Namespace k2a-token-sync runs in. Its ClusterConnection objects and every
	// credential Secret it owns live here.
	Namespace string

	// ArgoCDNamespace is where every generated cluster Secret is written. One
	// instance serves one ArgoCD, so this is a single value rather than a
	// per-cluster setting.
	ArgoCDNamespace string

	// HealthPort serves /livez, /readyz and /status.
	HealthPort string
}

// Cluster is one downstream cluster to keep registered with ArgoCD, resolved
// from a ClusterConnection spec.
type Cluster struct {
	// Name is the ClusterConnection's name. It identifies the cluster in logs
	// and, unless DisplayName overrides it, in the ArgoCD cluster list.
	Name string

	// Endpoint is the direct, proxy-bypassing address of the downstream API
	// server, always normalised to host:port. This is what ArgoCD connects to.
	Endpoint string

	// DisplayName is the cluster name ArgoCD shows. Defaults to Name.
	DisplayName string

	// ServiceAccount is the downstream identity ArgoCD authenticates as.
	ServiceAccount ServiceAccountRef

	// SelfServiceAccountName is the downstream identity k2a-token-sync itself
	// authenticates as after bootstrap.
	SelfServiceAccountName string

	// SecretName is the ArgoCD cluster Secret k2a-token-sync generates and
	// maintains, in Config.ArgoCDNamespace.
	SecretName string

	// TokenTTL is the requested lifetime of the credential written into the
	// ArgoCD Secret. The API server may shorten it.
	TokenTTL time.Duration

	// SelfTokenTTL is the requested lifetime of k2a-token-sync's own credential,
	// renewed on every successful pass, and therefore also the length of an
	// outage k2a-token-sync can recover from unaided.
	SelfTokenTTL time.Duration

	// ExpiryWarnThreshold controls when k2a-token-sync starts warning about the
	// downstream API server's serving certificate.
	ExpiryWarnThreshold time.Duration

	// Labels and Annotations are merged into the generated ArgoCD Secret.
	Labels      map[string]string
	Annotations map[string]string

	// Project optionally scopes the cluster to a single ArgoCD project.
	Project string
}

// ServiceAccountRef identifies a downstream ServiceAccount.
type ServiceAccountRef struct {
	Name      string
	Namespace string
}

// CredentialsSecretName is the Secret in k2a-token-sync's namespace holding the
// durable credential provisioned for this cluster.
func (c Cluster) CredentialsSecretName() string {
	return c.Name + "-credentials"
}

// ServerURL is the API server address as ArgoCD records it.
func (c Cluster) ServerURL() string {
	return "https://" + c.Endpoint
}

// Host is the endpoint with the port stripped, for certificate SAN checks.
func (c Cluster) Host() string {
	host, _, err := net.SplitHostPort(c.Endpoint)
	if err != nil {
		return c.Endpoint
	}
	return host
}

// Load reads process configuration from the environment.
func Load(logger *slog.Logger) (*Config, error) {
	namespace, err := requireEnv("POD_NAMESPACE")
	if err != nil {
		return nil, err
	}

	healthPort := envOrDefault(logger, "HEALTH_PORT", "8080")
	if p, convErr := strconv.Atoi(healthPort); convErr != nil || p < 1 || p > 65535 {
		return nil, fmt.Errorf("HEALTH_PORT %q is not a valid port number", healthPort)
	}

	return &Config{
		Namespace:       namespace,
		ArgoCDNamespace: envOrDefault(logger, "ARGOCD_NAMESPACE", defaultArgoCDNamespace),
		HealthPort:      healthPort,
	}, nil
}

// FromSpec resolves a ClusterConnection spec into a runtime Cluster.
//
// The schema has already applied its defaults and rejected malformed values, so
// what remains here is what a schema cannot express: normalising the endpoint,
// deriving names, parsing durations into Go types, and the length limit on the
// object's own name, which OpenAPI cannot constrain.
func FromSpec(name string, spec v1alpha1.ClusterConnectionSpec) (Cluster, error) {
	var out Cluster

	if name == "" {
		return out, errors.New("name must not be empty")
	}
	if len(name) > maxClusterNameLength {
		return out, fmt.Errorf("name %q exceeds %d characters, so the Secret names derived from it would be invalid",
			name, maxClusterNameLength)
	}
	out.Name = name

	endpoint, err := normaliseEndpoint(spec.Endpoint)
	if err != nil {
		return out, err
	}
	out.Endpoint = endpoint

	out.DisplayName = orDefault(spec.DisplayName, name)
	out.SecretName = orDefault(spec.SecretName, "cluster-"+name)
	out.SelfServiceAccountName = orDefault(spec.SelfServiceAccountName, defaultSelfServiceAccount)
	if err := validateSecretMetadata(spec.Labels, spec.Annotations); err != nil {
		return out, err
	}
	out.Labels = spec.Labels
	out.Annotations = spec.Annotations
	out.Project = spec.Project

	out.ServiceAccount = ServiceAccountRef{Name: defaultServiceAccountName, Namespace: defaultServiceAccountNS}
	if spec.ServiceAccount != nil {
		out.ServiceAccount.Name = orDefault(spec.ServiceAccount.Name, defaultServiceAccountName)
		out.ServiceAccount.Namespace = orDefault(spec.ServiceAccount.Namespace, defaultServiceAccountNS)
	}

	if out.TokenTTL, err = parseDuration("tokenTTL", spec.TokenTTL, defaultTokenTTL, minTokenTTL); err != nil {
		return out, err
	}
	if out.SelfTokenTTL, err = parseDuration("selfTokenTTL", spec.SelfTokenTTL, defaultSelfTokenTTL, minTokenTTL); err != nil {
		return out, err
	}
	if out.ExpiryWarnThreshold, err = parseDuration("expiryWarnThreshold", spec.ExpiryWarnThreshold, defaultExpiryWarnThreshold, 0); err != nil {
		return out, err
	}

	return out, nil
}

// IsReservedSecretLabel reports whether a label is owned by k2a-token-sync on
// the generated ArgoCD Secret.
func IsReservedSecretLabel(key string) bool {
	return key == SecretTypeLabel || key == ManagedByLabel
}

// IsReservedSecretAnnotation reports whether an annotation is owned by
// k2a-token-sync on the generated ArgoCD Secret.
func IsReservedSecretAnnotation(key string) bool {
	switch key {
	case ClusterNameAnnotation, TokenExpiryAnnotation, ServingCertExpiryAnnotation:
		return true
	default:
		return false
	}
}

func validateSecretMetadata(labels, annotations map[string]string) error {
	// Iterate the fixed keys rather than the user maps so an object containing more
	// than one reserved key receives the same first error on every reconciliation.
	for _, key := range []string{SecretTypeLabel, ManagedByLabel} {
		if _, found := labels[key]; found {
			return fmt.Errorf("spec.labels key %q is reserved for k2a-token-sync; remove it", key)
		}
	}
	for _, key := range []string{ClusterNameAnnotation, TokenExpiryAnnotation, ServingCertExpiryAnnotation} {
		if _, found := annotations[key]; found {
			return fmt.Errorf("spec.annotations key %q is reserved for k2a-token-sync; remove it", key)
		}
	}
	return nil
}

// normaliseEndpoint accepts "host", "host:port" or a full "https://host:port"
// URL and returns a bare host:port, defaulting to the standard API server port.
func normaliseEndpoint(in string) (string, error) {
	endpoint := strings.TrimSpace(in)
	if endpoint == "" {
		return "", errors.New("endpoint must not be empty")
	}
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimSuffix(endpoint, "/")

	if strings.Contains(endpoint, "/") {
		return "", fmt.Errorf("endpoint %q must be a host or host:port, not a URL path", in)
	}

	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		// No port at all is the common case, and the only one worth accepting:
		// anything else is malformed rather than abbreviated.
		if !strings.Contains(endpoint, ":") {
			return net.JoinHostPort(endpoint, defaultAPIPort), nil
		}
		return "", fmt.Errorf("endpoint %q is not a valid host:port: %w", in, err)
	}
	if host == "" {
		return "", fmt.Errorf("endpoint %q has no host", in)
	}
	if port == "" {
		return net.JoinHostPort(host, defaultAPIPort), nil
	}
	if _, convErr := strconv.Atoi(port); convErr != nil {
		return "", fmt.Errorf("endpoint %q has a non-numeric port", in)
	}
	return net.JoinHostPort(host, port), nil
}

func parseDuration(field, raw string, def, lowerBound time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a valid duration: %w", field, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s %q must be positive", field, raw)
	}
	if lowerBound > 0 && d < lowerBound {
		return 0, fmt.Errorf("%s %q must be at least %s", field, raw, lowerBound)
	}
	return d, nil
}

func orDefault(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func requireEnv(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return "", fmt.Errorf("%s must be set", key)
	}
	return value, nil
}

func envOrDefault(logger *slog.Logger, key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	logger.Debug("environment variable not set, using default", "variable", key, "default", fallback)
	return fallback
}
