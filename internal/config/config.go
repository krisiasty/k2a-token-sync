// Package config loads and validates the daemon's runtime configuration.
//
// Process-level settings (where the config file lives, which namespace the
// daemon runs in, the health port) come from environment variables. The cluster
// inventory comes from a YAML file, normally projected from a ConfigMap, so
// clusters can be added or removed without a chart upgrade.
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

	"sigs.k8s.io/yaml"
)

const (
	defaultAPIPort = "6443"

	// defaultTokenTTL is the lifetime requested for ArgoCD's credential. Long
	// enough that a prolonged control-path outage is harmless, since a refresh
	// happens at half life.
	defaultTokenTTL = 720 * time.Hour // 30 days

	defaultRefreshInterval = 24 * time.Hour

	// defaultExpiryWarnThreshold gives three months' notice, which is enough to
	// schedule a control-plane restart or certificate rotation properly rather
	// than as an emergency.
	defaultExpiryWarnThreshold = 2160 * time.Hour // 90 days

	defaultArgoCDNamespace     = "argocd"
	defaultServiceAccountName  = "argocd-manager"
	defaultServiceAccountNS    = "kube-system"
	defaultAgentServiceAccount = "k2a-token-sync"
	defaultBootstrapSecretKey  = "kubeconfig"
	minTokenTTL                = 1 * time.Hour
	minRefreshInterval         = 1 * time.Minute
	maxClusterNameLength       = 63
)

// Config is the fully resolved, validated configuration.
type Config struct {
	// Namespace the daemon runs in. All referenced secrets — bootstrap
	// credentials and the daemon's own durable credentials — live here.
	Namespace string

	// HealthPort serves /livez, /readyz and /status.
	HealthPort string

	// ArgoCDNamespace is where every generated cluster Secret is written. One
	// daemon serves one ArgoCD instance, so this is a single value rather than a
	// per-cluster setting.
	ArgoCDNamespace string

	// RefreshInterval is the reconciliation period.
	RefreshInterval time.Duration

	Clusters []Cluster
}

// Cluster is one downstream cluster to keep registered with ArgoCD.
type Cluster struct {
	// Name identifies the cluster in logs and, unless overridden, in the
	// ArgoCD cluster list.
	Name string

	// Endpoint is the direct, proxy-bypassing address of the downstream API
	// server, always normalised to host:port. This is what ArgoCD connects to.
	Endpoint string

	// DisplayName is the cluster name ArgoCD shows. Defaults to Name.
	DisplayName string

	// BootstrapSecret holds a kubeconfig or bearer token granting temporary
	// administrative access, used once to provision the daemon's own
	// credential.
	BootstrapSecret SecretRef

	// ServiceAccount is the downstream identity ArgoCD authenticates as.
	ServiceAccount ServiceAccountRef

	// AgentServiceAccountName is the downstream identity the daemon itself
	// authenticates as after bootstrap.
	AgentServiceAccountName string

	// SecretName is the ArgoCD cluster Secret this daemon generates and
	// maintains, in Config.ArgoCDNamespace.
	SecretName string

	// TokenTTL is the requested lifetime of the credential written into the
	// ArgoCD Secret. The API server may shorten it.
	TokenTTL time.Duration

	// ExpiryWarnThreshold controls when the daemon starts warning about the
	// downstream API server's serving certificate.
	ExpiryWarnThreshold time.Duration

	// Labels and Annotations are merged into the generated ArgoCD Secret.
	Labels      map[string]string
	Annotations map[string]string

	// Project optionally scopes the cluster to a single ArgoCD project.
	Project string
}

// SecretRef points at a key within a Secret in the daemon's namespace.
type SecretRef struct {
	Name string
	Key  string
}

// ServiceAccountRef identifies a downstream ServiceAccount.
type ServiceAccountRef struct {
	Name      string
	Namespace string
}

// CredentialsSecretName is the Secret in the daemon's namespace holding the
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

// file mirrors the on-disk YAML. It is deliberately separate from the resolved
// types so that "unset" can be distinguished from "set to the zero value".
type file struct {
	ArgoCDNamespace string        `json:"argocdNamespace,omitempty"`
	Defaults        *defaults     `json:"defaults,omitempty"`
	Clusters        []clusterFile `json:"clusters"`
}

type defaults struct {
	TokenTTL            string             `json:"tokenTTL,omitempty"`
	RefreshInterval     string             `json:"refreshInterval,omitempty"`
	ExpiryWarnThreshold string             `json:"expiryWarnThreshold,omitempty"`
	ServiceAccount      *serviceAccountRef `json:"serviceAccount,omitempty"`
}

type clusterFile struct {
	Name                    string             `json:"name"`
	Endpoint                string             `json:"endpoint"`
	DisplayName             string             `json:"displayName,omitempty"`
	BootstrapSecret         *secretRef         `json:"bootstrapSecret,omitempty"`
	ServiceAccount          *serviceAccountRef `json:"serviceAccount,omitempty"`
	AgentServiceAccountName string             `json:"agentServiceAccountName,omitempty"`
	SecretName              string             `json:"secretName,omitempty"`
	TokenTTL                string             `json:"tokenTTL,omitempty"`
	ExpiryWarnThreshold     string             `json:"expiryWarnThreshold,omitempty"`
	Labels                  map[string]string  `json:"labels,omitempty"`
	Annotations             map[string]string  `json:"annotations,omitempty"`
	Project                 string             `json:"project,omitempty"`
}

type secretRef struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

type serviceAccountRef struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// Load reads process configuration from the environment and the cluster
// inventory from the referenced YAML file.
func Load(logger *slog.Logger) (*Config, error) {
	path := envOrDefault(logger, "CONFIG_PATH", "/etc/k2a-token-sync/config.yaml")

	namespace, err := requireEnv("POD_NAMESPACE")
	if err != nil {
		return nil, err
	}

	healthPort := envOrDefault(logger, "HEALTH_PORT", "8080")
	if p, convErr := strconv.Atoi(healthPort); convErr != nil || p < 1 || p > 65535 {
		return nil, fmt.Errorf("HEALTH_PORT %q is not a valid port number", healthPort)
	}

	// The path is operator-supplied by design: the inventory is projected from a
	// ConfigMap whose mount point is deployment-specific.
	raw, err := os.ReadFile(path) //nolint:gosec // G304: configurable config path is intended
	if err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	var f file
	if err := yaml.UnmarshalStrict(raw, &f); err != nil {
		return nil, fmt.Errorf("parsing config file %s: %w", path, err)
	}

	cfg := &Config{Namespace: namespace, HealthPort: healthPort}
	if err := cfg.applyFile(&f, logger); err != nil {
		return nil, fmt.Errorf("invalid config file %s: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) applyFile(f *file, logger *slog.Logger) error {
	def := f.Defaults
	if def == nil {
		def = &defaults{}
	}

	var err error
	if c.RefreshInterval, err = parseDuration("defaults.refreshInterval", def.RefreshInterval, defaultRefreshInterval, minRefreshInterval); err != nil {
		return err
	}
	defTokenTTL, err := parseDuration("defaults.tokenTTL", def.TokenTTL, defaultTokenTTL, minTokenTTL)
	if err != nil {
		return err
	}
	defWarn, err := parseDuration("defaults.expiryWarnThreshold", def.ExpiryWarnThreshold, defaultExpiryWarnThreshold, 0)
	if err != nil {
		return err
	}
	c.ArgoCDNamespace = orDefault(f.ArgoCDNamespace, defaultArgoCDNamespace)

	if len(f.Clusters) == 0 {
		return errors.New("no clusters configured")
	}

	seenNames := make(map[string]struct{}, len(f.Clusters))
	seenSecrets := make(map[string]string, len(f.Clusters))

	for i, cf := range f.Clusters {
		cluster, err := resolveCluster(cf, def, defTokenTTL, defWarn)
		if err != nil {
			return fmt.Errorf("clusters[%d]: %w", i, err)
		}

		if _, dup := seenNames[cluster.Name]; dup {
			return fmt.Errorf("clusters[%d]: duplicate cluster name %q", i, cluster.Name)
		}
		seenNames[cluster.Name] = struct{}{}

		// Two clusters writing one Secret would silently overwrite each other.
		if owner, dup := seenSecrets[cluster.SecretName]; dup {
			return fmt.Errorf("clusters[%d]: secretName %q already generated by cluster %q",
				i, cluster.SecretName, owner)
		}
		seenSecrets[cluster.SecretName] = cluster.Name

		// The runtime scheduler clamps to half the granted token lifetime, so
		// this is not fatal — but it means refreshInterval is not what governs
		// the cadence, which is worth saying out loud.
		if c.RefreshInterval > cluster.TokenTTL/2 {
			logger.Warn("refreshInterval exceeds half the token lifetime; the effective cadence will be shorter",
				"cluster", cluster.Name,
				"refresh_interval", c.RefreshInterval.String(),
				"token_ttl", cluster.TokenTTL.String(),
			)
		}
		c.Clusters = append(c.Clusters, cluster)
	}

	return nil
}

func resolveCluster(cf clusterFile, def *defaults, defTokenTTL, defWarn time.Duration) (Cluster, error) {
	var out Cluster

	if cf.Name == "" {
		return out, errors.New("name must not be empty")
	}
	if len(cf.Name) > maxClusterNameLength {
		return out, fmt.Errorf("name %q exceeds %d characters", cf.Name, maxClusterNameLength)
	}
	out.Name = cf.Name

	endpoint, err := normaliseEndpoint(cf.Endpoint)
	if err != nil {
		return out, err
	}
	out.Endpoint = endpoint

	out.DisplayName = orDefault(cf.DisplayName, cf.Name)
	out.SecretName = orDefault(cf.SecretName, "cluster-"+cf.Name)
	out.AgentServiceAccountName = orDefault(cf.AgentServiceAccountName, defaultAgentServiceAccount)
	out.Labels = cf.Labels
	out.Annotations = cf.Annotations
	out.Project = cf.Project

	sa := cf.ServiceAccount
	if sa == nil {
		sa = def.ServiceAccount
	}
	out.ServiceAccount = ServiceAccountRef{Name: defaultServiceAccountName, Namespace: defaultServiceAccountNS}
	if sa != nil {
		out.ServiceAccount.Name = orDefault(sa.Name, defaultServiceAccountName)
		out.ServiceAccount.Namespace = orDefault(sa.Namespace, defaultServiceAccountNS)
	}

	// bootstrapSecret is optional: a cluster prepared with
	// 'k2a-token-sync bootstrap' already has a durable credential stored, and
	// needs no bootstrap material at all. It is only required when the operator
	// wants the daemon to perform the bootstrap itself on first contact, which
	// is checked at reconciliation time rather than here.
	out.BootstrapSecret = resolveSecretRef(cf.BootstrapSecret, "", defaultBootstrapSecretKey)
	if cf.BootstrapSecret != nil && cf.BootstrapSecret.Name == "" {
		return out, errors.New("bootstrapSecret is present but bootstrapSecret.name is empty")
	}

	if out.TokenTTL, err = parseDuration("tokenTTL", cf.TokenTTL, defTokenTTL, minTokenTTL); err != nil {
		return out, err
	}
	if out.ExpiryWarnThreshold, err = parseDuration("expiryWarnThreshold", cf.ExpiryWarnThreshold, defWarn, 0); err != nil {
		return out, err
	}

	return out, nil
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
	if strings.ContainsAny(endpoint, "/?#") {
		return "", fmt.Errorf("endpoint %q must be a host or host:port, not a URL path", in)
	}

	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		// No port present, or an unbracketed IPv6 literal.
		if strings.Count(endpoint, ":") > 1 {
			return "", fmt.Errorf("endpoint %q: IPv6 addresses must be bracketed, e.g. [::1]:6443", in)
		}
		host, port = endpoint, defaultAPIPort
	}
	if host == "" {
		return "", fmt.Errorf("endpoint %q has no host", in)
	}
	if p, convErr := strconv.Atoi(port); convErr != nil || p < 1 || p > 65535 {
		return "", fmt.Errorf("endpoint %q has an invalid port", in)
	}
	return net.JoinHostPort(host, port), nil
}

func parseDuration(field, raw string, def, lowerBound time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative", field)
	}
	if lowerBound > 0 && d < lowerBound {
		return 0, fmt.Errorf("%s must be at least %s", field, lowerBound)
	}
	return d, nil
}

func resolveSecretRef(ref *secretRef, defName, defKey string) SecretRef {
	out := SecretRef{Name: defName, Key: defKey}
	if ref == nil {
		return out
	}
	out.Name = orDefault(ref.Name, defName)
	out.Key = orDefault(ref.Key, defKey)
	return out
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
	v, ok := os.LookupEnv(key)
	switch {
	case !ok:
		return "", fmt.Errorf("required environment variable %s is not set", key)
	case v == "":
		return "", fmt.Errorf("required environment variable %s must not be empty", key)
	}
	return v, nil
}

func envOrDefault(logger *slog.Logger, key, def string) string {
	v, ok := os.LookupEnv(key)
	if ok && v == "" {
		logger.Warn("optional env var explicitly set to empty, using default", "env", key, "default", def)
	}
	if v != "" {
		return v
	}
	return def
}
