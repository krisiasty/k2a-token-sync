// Package config loads and validates the daemon's runtime configuration.
//
// Process-level settings (where the config file lives, which namespace the
// daemon runs in, the health port) come from environment variables. The cluster
// inventory comes from a YAML file, normally projected from a ConfigMap, so
// clusters can be added or removed without a chart upgrade.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// Provider selects how the daemon obtains administrative access to a
// downstream cluster in order to mint credentials for ArgoCD.
type Provider string

const (
	// ProviderRancher reaches the downstream cluster through the Rancher API
	// proxy, authenticating with a Rancher token. Rancher's cluster agent is
	// already privileged in every cluster it manages, so no per-cluster
	// bootstrap is required.
	ProviderRancher Provider = "rancher"

	// ProviderDirect reaches the downstream cluster at its own endpoint. The
	// first reconciliation uses an operator-supplied bootstrap credential; the
	// daemon then provisions and stores a durable credential of its own and
	// the bootstrap credential can be discarded.
	ProviderDirect Provider = "direct"
)

const (
	defaultAPIPort = "6443"

	// defaultTokenTTL is the lifetime requested for ArgoCD's credential. Long
	// enough that a prolonged control-path outage is harmless, since a refresh
	// happens at half life.
	defaultTokenTTL = 720 * time.Hour // 30 days

	defaultRefreshInterval = 24 * time.Hour

	// defaultExpiryWarnThreshold matches the window in which RKE2 itself will
	// rotate certificates on a service restart. Warning from here means the
	// "restart rke2-server" remedy is available for the whole warning period,
	// and there is time to schedule a control-plane restart properly.
	defaultExpiryWarnThreshold = 2160 * time.Hour // 90 days

	// defaultRotateThreshold sits well inside the warning window, so an
	// operator sees warnings for two months before the daemon acts on its own.
	defaultRotateThreshold     = 720 * time.Hour // 30 days
	defaultArgoCDNamespace     = "argocd"
	defaultServiceAccountName  = "argocd-manager"
	defaultServiceAccountNS    = "kube-system"
	defaultAgentServiceAccount = "r2a-cert-sync"
	defaultBootstrapSecretKey  = "kubeconfig"
	defaultRancherTokenKey     = "token"
	defaultRancherCASecretKey  = "ca.crt"
	minTokenTTL                = 1 * time.Hour
	minRefreshInterval         = 1 * time.Minute
	maxClusterNameLength       = 63
)

// Config is the fully resolved, validated configuration.
type Config struct {
	// Namespace the daemon runs in. All referenced secrets — the Rancher
	// token, bootstrap credentials and the daemon's own durable credentials —
	// live here.
	Namespace string

	// HealthPort serves /livez, /readyz and /status.
	HealthPort string

	// ArgoCDNamespace is where every generated cluster Secret is written. One
	// daemon serves one ArgoCD instance, so this is a single value rather than a
	// per-cluster setting.
	ArgoCDNamespace string

	// RefreshInterval is the reconciliation period.
	RefreshInterval time.Duration

	Rancher  *RancherConfig
	Clusters []Cluster
}

// RancherConfig describes how to reach the Rancher management API. It is
// required only when at least one cluster uses ProviderRancher.
type RancherConfig struct {
	URL   string
	Token SecretRef

	// CA optionally supplies a PEM bundle used to verify the Rancher
	// endpoint. When empty the system trust store is used.
	CA SecretRef

	InsecureSkipTLSVerify bool
}

// Cluster is one downstream RKE2 cluster to keep registered with ArgoCD.
type Cluster struct {
	// Name identifies the cluster in logs and, unless overridden, in Rancher
	// and in the ArgoCD cluster list.
	Name string

	Provider Provider

	// Endpoint is the direct, proxy-bypassing address of the downstream API
	// server, always normalised to host:port. This is what ArgoCD connects to.
	Endpoint string

	// DisplayName is the cluster name ArgoCD shows. Defaults to Name.
	DisplayName string

	// RancherClusterName is the cluster's name in Rancher. Defaults to Name.
	// Only meaningful for ProviderRancher.
	RancherClusterName string

	// BootstrapSecret holds a kubeconfig or bearer token granting temporary
	// administrative access, used once to provision the daemon's own
	// credential. Only meaningful for ProviderDirect.
	BootstrapSecret SecretRef

	// ServiceAccount is the downstream identity ArgoCD authenticates as.
	ServiceAccount ServiceAccountRef

	// AgentServiceAccountName is the downstream identity the daemon itself
	// authenticates as after bootstrap. Only meaningful for ProviderDirect.
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

	// AutoRotate permits the daemon to trigger an RKE2 certificate rotation
	// through Rancher when the serving certificate nears expiry. Only
	// available for ProviderRancher — standalone RKE2 exposes no such API.
	AutoRotate bool

	// RotateThreshold is the remaining serving-certificate lifetime below
	// which rotation is triggered.
	RotateThreshold time.Duration

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
// durable credential provisioned for a ProviderDirect cluster.
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
	Rancher         *rancherFile  `json:"rancher,omitempty"`
	Defaults        *defaults     `json:"defaults,omitempty"`
	Clusters        []clusterFile `json:"clusters"`
}

type rancherFile struct {
	URL                   string     `json:"url"`
	TokenSecret           *secretRef `json:"tokenSecret,omitempty"`
	CASecret              *secretRef `json:"caSecret,omitempty"`
	InsecureSkipTLSVerify bool       `json:"insecureSkipTLSVerify,omitempty"`
}

type defaults struct {
	TokenTTL            string             `json:"tokenTTL,omitempty"`
	RefreshInterval     string             `json:"refreshInterval,omitempty"`
	ExpiryWarnThreshold string             `json:"expiryWarnThreshold,omitempty"`
	RotateThreshold     string             `json:"rotateThreshold,omitempty"`
	ServiceAccount      *serviceAccountRef `json:"serviceAccount,omitempty"`
}

type clusterFile struct {
	Name                    string             `json:"name"`
	Provider                string             `json:"provider,omitempty"`
	Endpoint                string             `json:"endpoint"`
	DisplayName             string             `json:"displayName,omitempty"`
	RancherClusterName      string             `json:"rancherClusterName,omitempty"`
	BootstrapSecret         *secretRef         `json:"bootstrapSecret,omitempty"`
	ServiceAccount          *serviceAccountRef `json:"serviceAccount,omitempty"`
	AgentServiceAccountName string             `json:"agentServiceAccountName,omitempty"`
	SecretName              string             `json:"secretName,omitempty"`
	TokenTTL                string             `json:"tokenTTL,omitempty"`
	ExpiryWarnThreshold     string             `json:"expiryWarnThreshold,omitempty"`
	AutoRotate              bool               `json:"autoRotate,omitempty"`
	RotateThreshold         string             `json:"rotateThreshold,omitempty"`
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
	path := envOrDefault(logger, "CONFIG_PATH", "/etc/r2a-cert-sync/config.yaml")

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
	defRotate, err := parseDuration("defaults.rotateThreshold", def.RotateThreshold, defaultRotateThreshold, 0)
	if err != nil {
		return err
	}

	c.ArgoCDNamespace = orDefault(f.ArgoCDNamespace, defaultArgoCDNamespace)

	if len(f.Clusters) == 0 {
		return fmt.Errorf("no clusters configured")
	}

	seenNames := make(map[string]struct{}, len(f.Clusters))
	seenSecrets := make(map[string]string, len(f.Clusters))
	needsRancher := false

	for i, cf := range f.Clusters {
		cluster, err := resolveCluster(cf, def, defTokenTTL, defWarn, defRotate)
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

		if cluster.Provider == ProviderRancher {
			needsRancher = true
		}

		// Rotating before ever warning defeats the point of the warning: the
		// operator would learn about the expiry from the rotation itself.
		if cluster.AutoRotate && cluster.RotateThreshold > cluster.ExpiryWarnThreshold {
			logger.Warn("rotateThreshold exceeds expiryWarnThreshold, so rotation will happen before any warning",
				"cluster", cluster.Name,
				"rotate_threshold", cluster.RotateThreshold.String(),
				"expiry_warn_threshold", cluster.ExpiryWarnThreshold.String(),
			)
		}

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
		if cluster.AutoRotate && cluster.Provider != ProviderRancher {
			return fmt.Errorf("clusters[%d]: autoRotate requires provider %q; standalone RKE2 exposes no rotation API", i, ProviderRancher)
		}

		c.Clusters = append(c.Clusters, cluster)
	}

	if f.Rancher != nil {
		if f.Rancher.URL == "" {
			return fmt.Errorf("rancher.url must not be empty")
		}
		url := strings.TrimRight(f.Rancher.URL, "/")
		if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
			return fmt.Errorf("rancher.url %q must include a scheme", f.Rancher.URL)
		}
		token := resolveSecretRef(f.Rancher.TokenSecret, "", defaultRancherTokenKey)
		if token.Name == "" {
			return fmt.Errorf("rancher.tokenSecret.name must be set")
		}
		if f.Rancher.InsecureSkipTLSVerify {
			logger.Warn("rancher TLS verification disabled by configuration", "url", url)
		}
		c.Rancher = &RancherConfig{
			URL:                   url,
			Token:                 token,
			CA:                    resolveSecretRef(f.Rancher.CASecret, "", defaultRancherCASecretKey),
			InsecureSkipTLSVerify: f.Rancher.InsecureSkipTLSVerify,
		}
	}

	if needsRancher && c.Rancher == nil {
		return fmt.Errorf("at least one cluster uses provider %q but no rancher section is configured", ProviderRancher)
	}
	if !needsRancher && c.Rancher != nil {
		logger.Warn("rancher section configured but no cluster uses it")
	}

	return nil
}

func resolveCluster(cf clusterFile, def *defaults, defTokenTTL, defWarn, defRotate time.Duration) (Cluster, error) {
	var out Cluster

	if cf.Name == "" {
		return out, fmt.Errorf("name must not be empty")
	}
	if len(cf.Name) > maxClusterNameLength {
		return out, fmt.Errorf("name %q exceeds %d characters", cf.Name, maxClusterNameLength)
	}
	out.Name = cf.Name

	provider := Provider(cf.Provider)
	if cf.Provider == "" {
		provider = ProviderRancher
	}
	switch provider {
	case ProviderRancher, ProviderDirect:
	default:
		return out, fmt.Errorf("unknown provider %q (want %q or %q)", cf.Provider, ProviderRancher, ProviderDirect)
	}
	out.Provider = provider

	endpoint, err := normaliseEndpoint(cf.Endpoint)
	if err != nil {
		return out, err
	}
	out.Endpoint = endpoint

	out.DisplayName = orDefault(cf.DisplayName, cf.Name)
	out.RancherClusterName = orDefault(cf.RancherClusterName, cf.Name)
	out.SecretName = orDefault(cf.SecretName, "cluster-"+cf.Name)
	out.AgentServiceAccountName = orDefault(cf.AgentServiceAccountName, defaultAgentServiceAccount)
	out.Labels = cf.Labels
	out.Annotations = cf.Annotations
	out.Project = cf.Project
	out.AutoRotate = cf.AutoRotate

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
	// 'r2a-cert-sync bootstrap' already has a durable credential stored, and
	// needs no bootstrap material at all. It is only required when the operator
	// wants the daemon to perform the bootstrap itself on first contact, which
	// is checked at reconciliation time rather than here.
	if provider == ProviderDirect {
		out.BootstrapSecret = resolveSecretRef(cf.BootstrapSecret, "", defaultBootstrapSecretKey)
		if cf.BootstrapSecret != nil && cf.BootstrapSecret.Name == "" {
			return out, fmt.Errorf("bootstrapSecret is present but bootstrapSecret.name is empty")
		}
	} else if cf.BootstrapSecret != nil {
		return out, fmt.Errorf("bootstrapSecret is only valid for provider %q", ProviderDirect)
	}

	if out.TokenTTL, err = parseDuration("tokenTTL", cf.TokenTTL, defTokenTTL, minTokenTTL); err != nil {
		return out, err
	}
	if out.ExpiryWarnThreshold, err = parseDuration("expiryWarnThreshold", cf.ExpiryWarnThreshold, defWarn, 0); err != nil {
		return out, err
	}
	if out.RotateThreshold, err = parseDuration("rotateThreshold", cf.RotateThreshold, defRotate, 0); err != nil {
		return out, err
	}

	return out, nil
}

// normaliseEndpoint accepts "host", "host:port" or a full "https://host:port"
// URL and returns a bare host:port, defaulting to the RKE2 API port.
func normaliseEndpoint(in string) (string, error) {
	endpoint := strings.TrimSpace(in)
	if endpoint == "" {
		return "", fmt.Errorf("endpoint must not be empty")
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
