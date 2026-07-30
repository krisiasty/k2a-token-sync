// Package reconcile drives one pass over every configured cluster.
//
// The reconciler is the whole daemon in outline. For each cluster it obtains
// administrative access, ensures ArgoCD's downstream identity exists, mints a
// short-lived credential, and publishes it in an ArgoCD cluster Secret pointing
// at the cluster's direct endpoint. It also probes that endpoint's serving
// certificate and, where permitted, asks Rancher to rotate it.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/krisiasty/r2a-cert-sync/internal/argocd"
	"github.com/krisiasty/r2a-cert-sync/internal/config"
	"github.com/krisiasty/r2a-cert-sync/internal/downstream"
	"github.com/krisiasty/r2a-cert-sync/internal/k8s"
	"github.com/krisiasty/r2a-cert-sync/internal/rancher"
)

// clientTimeout bounds any single downstream API call.
const clientTimeout = 30 * time.Second

// Reconciler holds the collaborators needed for a reconciliation pass.
type Reconciler struct {
	cfg    *config.Config
	local  kubernetes.Interface
	logger *slog.Logger

	// rancherClient is nil when no cluster uses the Rancher provider.
	rancherClient *rancher.Client

	// now is injectable for tests.
	now func() time.Time
}

// New builds a Reconciler. The Rancher client is constructed eagerly so that a
// bad URL or unreadable token fails at startup rather than mid-cycle.
func New(ctx context.Context, cfg *config.Config, local kubernetes.Interface, logger *slog.Logger) (*Reconciler, error) {
	r := &Reconciler{cfg: cfg, local: local, logger: logger, now: time.Now}

	if cfg.Rancher == nil {
		return r, nil
	}

	token, err := k8s.ReadSecretKey(ctx, local, cfg.Namespace, cfg.Rancher.Token.Name, cfg.Rancher.Token.Key)
	if err != nil {
		return nil, fmt.Errorf("reading rancher token: %w", err)
	}

	var ca []byte
	if cfg.Rancher.CA.Name != "" {
		ca, err = k8s.ReadSecretKey(ctx, local, cfg.Namespace, cfg.Rancher.CA.Name, cfg.Rancher.CA.Key)
		if err != nil {
			return nil, fmt.Errorf("reading rancher CA: %w", err)
		}
	}

	r.rancherClient, err = rancher.New(rancher.Options{
		BaseURL:               cfg.Rancher.URL,
		Token:                 string(token),
		CA:                    ca,
		InsecureSkipTLSVerify: cfg.Rancher.InsecureSkipTLSVerify,
		Logger:                logger,
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ClusterStatus is the outcome of reconciling one cluster.
type ClusterStatus struct {
	Name     string    `json:"name"`
	Provider string    `json:"provider"`
	Endpoint string    `json:"endpoint"`
	Secret   string    `json:"secret"`
	Synced   bool      `json:"synced"`
	Error    string    `json:"error,omitempty"`
	Action   string    `json:"action,omitempty"`
	SyncedAt time.Time `json:"syncedAt,omitzero"`

	TokenExpiresAt       time.Time `json:"tokenExpiresAt,omitzero"`
	ServingCertExpiresAt time.Time `json:"servingCertExpiresAt,omitzero"`
	ServingCertDaysLeft  int       `json:"servingCertDaysLeft,omitempty"`
	ServingCertTrusted   bool      `json:"servingCertTrusted"`
	ServingCertWarning   string    `json:"servingCertWarning,omitempty"`
	Rotated              bool      `json:"rotated,omitempty"`
}

// Result aggregates a full pass.
type Result struct {
	Clusters []ClusterStatus `json:"clusters"`
}

// AllSynced reports whether every cluster reconciled cleanly.
func (r Result) AllSynced() bool {
	for _, c := range r.Clusters {
		if !c.Synced {
			return false
		}
	}
	return true
}

// Failures counts clusters that did not reconcile.
func (r Result) Failures() int {
	var n int
	for _, c := range r.Clusters {
		if !c.Synced {
			n++
		}
	}
	return n
}

// SoonestTokenExpiry returns the earliest expiry among the credentials this pass
// published, or the zero time if none are known.
//
// The scheduler needs this because the API server may cap token lifetime far
// below what was requested. Sleeping for a fixed refreshInterval would then let
// ArgoCD's credential die between passes.
func (r Result) SoonestTokenExpiry() time.Time {
	var soonest time.Time
	for _, c := range r.Clusters {
		if c.TokenExpiresAt.IsZero() {
			continue
		}
		if soonest.IsZero() || c.TokenExpiresAt.Before(soonest) {
			soonest = c.TokenExpiresAt
		}
	}
	return soonest
}

// Run reconciles every configured cluster.
//
// Clusters are independent: a failure is recorded and the pass continues, so one
// unreachable cluster cannot stall the others.
func (r *Reconciler) Run(ctx context.Context) Result {
	result := Result{Clusters: make([]ClusterStatus, 0, len(r.cfg.Clusters))}

	for i := range r.cfg.Clusters {
		cluster := r.cfg.Clusters[i]

		if ctx.Err() != nil {
			return result
		}

		logger := r.logger.With("cluster", cluster.Name, "provider", string(cluster.Provider))
		status := ClusterStatus{
			Name:     cluster.Name,
			Provider: string(cluster.Provider),
			Endpoint: cluster.Endpoint,
			Secret:   r.cfg.ArgoCDNamespace + "/" + cluster.SecretName,
		}

		if err := r.reconcileCluster(ctx, cluster, logger, &status); err != nil {
			status.Error = err.Error()
			logger.Error("cluster reconciliation failed", "error", err)
		} else {
			status.Synced = true
			status.SyncedAt = r.now()
		}

		result.Clusters = append(result.Clusters, status)
	}

	return result
}

func (r *Reconciler) reconcileCluster(ctx context.Context, cluster config.Cluster, logger *slog.Logger, status *ClusterStatus) error {
	access, err := r.access(ctx, cluster, logger)
	if err != nil {
		return err
	}

	// Rotation is evaluated before credentials are minted: a rotation restarts
	// the downstream control plane and invalidates tokens issued beforehand.
	if cluster.AutoRotate {
		rotated, err := r.maybeRotate(ctx, cluster, access, logger, status)
		if err != nil {
			return err
		}
		if rotated {
			status.Rotated = true
			// Re-establish access; the proxy connection may have been reset.
			if access, err = r.access(ctx, cluster, logger); err != nil {
				return fmt.Errorf("re-establishing access after rotation: %w", err)
			}
		}
	}

	ca, err := downstream.ClusterCA(ctx, access.client, cluster.ServiceAccount.Namespace)
	if err != nil {
		return err
	}

	if err := r.probe(ctx, cluster, ca, logger, status); err != nil {
		return err
	}

	desired := argocd.ClusterSecret{
		Name:                 cluster.SecretName,
		Namespace:            r.cfg.ArgoCDNamespace,
		DisplayName:          cluster.DisplayName,
		Server:               cluster.ServerURL(),
		CAData:               ca,
		Project:              cluster.Project,
		ClusterName:          cluster.Name,
		ServingCertExpiresAt: status.ServingCertExpiresAt,
		ExtraLabels:          cluster.Labels,
		ExtraAnnotations:     cluster.Annotations,
	}

	observed, err := argocd.Observe(ctx, r.local, r.cfg.ArgoCDNamespace, cluster.SecretName)
	if err != nil {
		return err
	}

	now := r.now()
	reason := argocd.NeedsRefresh(observed, desired, cluster.TokenTTL, now)
	if reason == "" {
		status.Action = "up-to-date"
		status.TokenExpiresAt = observed.TokenExpiresAt
		logger.Info("registration current",
			"token_expires_at", observed.TokenExpiresAt.UTC().Format(time.RFC3339),
			"serving_cert_days_left", status.ServingCertDaysLeft,
		)
		// Refresh the observed serving-cert annotation without reissuing the
		// credential, so expiry stays visible on the object.
		desired.BearerToken = ""
		desired.TokenExpiresAt = observed.TokenExpiresAt
		return r.annotate(ctx, desired, now)
	}

	created, err := downstream.EnsureArgoCDIdentity(ctx, access.client, cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name)
	if err != nil {
		return err
	}
	if created {
		logger.Info("provisioned ArgoCD identity in downstream cluster",
			"serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name)
	}

	token, err := downstream.MintToken(ctx, access.client,
		cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name, cluster.TokenTTL)
	if err != nil {
		return err
	}

	granted := time.Until(token.ExpiresAt)
	if granted < cluster.TokenTTL*9/10 {
		logger.Warn("API server shortened the requested token lifetime",
			"requested", cluster.TokenTTL.String(),
			"granted", granted.Round(time.Minute).String(),
			"hint", "raise --service-account-max-token-expiration on the downstream API server, or lower tokenTTL")
	}

	desired.BearerToken = token.Value
	desired.TokenExpiresAt = token.ExpiresAt

	if err := argocd.Apply(ctx, r.local, desired, now); err != nil {
		return err
	}

	status.Action = string(reason)
	status.TokenExpiresAt = token.ExpiresAt
	logger.Info("credential reissued",
		"reason", string(reason),
		"token_expires_at", token.ExpiresAt.UTC().Format(time.RFC3339),
		"serving_cert_days_left", status.ServingCertDaysLeft,
	)
	return nil
}

// annotate refreshes the bookkeeping annotations on an otherwise current Secret.
func (r *Reconciler) annotate(ctx context.Context, desired argocd.ClusterSecret, now time.Time) error {
	secret, err := desired.Render(now)
	if err != nil {
		return err
	}
	// Leave credential material alone; only the annotations are being updated.
	delete(secret.Data, "config")
	return k8s.UpsertSecret(ctx, r.local, secret)
}

// probe inspects the serving certificate at the direct endpoint and records what
// it finds, warning about conditions that will break ArgoCD.
func (r *Reconciler) probe(ctx context.Context, cluster config.Cluster, ca []byte, logger *slog.Logger, status *ClusterStatus) error {
	cert, err := downstream.ProbeServingCert(ctx, cluster.Endpoint, ca)
	if err != nil {
		return fmt.Errorf("probing direct endpoint: %w", err)
	}

	status.ServingCertExpiresAt = cert.NotAfter
	status.ServingCertDaysLeft = cert.DaysRemaining()
	status.ServingCertTrusted = cert.TrustedByCA

	if cert.HostnameError != nil {
		status.ServingCertWarning = cert.HostnameError.Error()
		// Fail loudly: publishing this credential would produce a cluster
		// registration ArgoCD can never connect to, which is far harder to
		// diagnose from the ArgoCD side than an explicit error here.
		return cert.HostnameError
	}

	if !cert.TrustedByCA {
		status.ServingCertWarning = "serving certificate does not verify against the cluster CA"
		return fmt.Errorf("the certificate presented at %s does not verify against the cluster CA "+
			"published as ArgoCD's caData; ArgoCD would reject this endpoint", cluster.Endpoint)
	}

	if remaining := time.Until(cert.NotAfter); remaining < cluster.ExpiryWarnThreshold {
		logger.Warn("downstream API server certificate is nearing expiry",
			"expires_at", cert.NotAfter.UTC().Format(time.RFC3339),
			"days_left", cert.DaysRemaining(),
			"remedy", rotationRemedy(cluster),
		)
	}
	return nil
}

// rotationRemedy states what an operator should do about an expiring certificate.
func rotationRemedy(cluster config.Cluster) string {
	switch {
	case cluster.AutoRotate:
		return "rotation will be triggered automatically through Rancher"
	case cluster.Provider == config.ProviderRancher:
		return "rotate certificates for this cluster in Rancher, or set autoRotate: true"
	default:
		return "restart rke2-server on the control-plane nodes; RKE2 rotates certificates within 90 days of expiry on restart"
	}
}

// maybeRotate triggers a Rancher-orchestrated rotation when the serving
// certificate is inside the configured threshold.
func (r *Reconciler) maybeRotate(ctx context.Context, cluster config.Cluster, access *clusterAccess, logger *slog.Logger, status *ClusterStatus) (bool, error) {
	// Probe without CA verification here; only the expiry matters for the
	// rotation decision, and a cluster whose certificate has already drifted
	// out of trust is exactly the one that needs rotating.
	cert, err := downstream.ProbeServingCert(ctx, cluster.Endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("probing direct endpoint before rotation: %w", err)
	}

	status.ServingCertExpiresAt = cert.NotAfter
	status.ServingCertDaysLeft = cert.DaysRemaining()

	if time.Until(cert.NotAfter) >= cluster.RotateThreshold {
		return false, nil
	}
	if access.rancherClusterID == "" {
		return false, errors.New("autoRotate is enabled but the cluster has no Rancher ID")
	}

	logger.Warn("triggering certificate rotation through Rancher",
		"days_left", cert.DaysRemaining(),
		"threshold", cluster.RotateThreshold.String(),
		"rancher_cluster_id", access.rancherClusterID,
	)

	if err := r.rancherClient.RotateCertificates(ctx, access.rancherClusterID); err != nil {
		return false, fmt.Errorf("rotating certificates: %w", err)
	}
	return true, nil
}

// clusterAccess is an administrative connection to a downstream cluster.
type clusterAccess struct {
	client           kubernetes.Interface
	rancherClusterID string
}

// access establishes administrative access to a downstream cluster.
func (r *Reconciler) access(ctx context.Context, cluster config.Cluster, logger *slog.Logger) (*clusterAccess, error) {
	switch cluster.Provider {
	case config.ProviderRancher:
		return r.rancherAccess(ctx, cluster)
	case config.ProviderDirect:
		return r.directAccess(ctx, cluster, logger)
	default:
		return nil, fmt.Errorf("unsupported provider %q", cluster.Provider)
	}
}

// rancherAccess reaches the cluster through the Rancher API proxy. Rancher's
// agent is already privileged in every cluster it manages, so this needs no
// per-cluster bootstrap.
func (r *Reconciler) rancherAccess(ctx context.Context, cluster config.Cluster) (*clusterAccess, error) {
	if r.rancherClient == nil {
		return nil, errors.New("rancher provider requires a configured rancher section")
	}

	found, err := r.rancherClient.FindCluster(ctx, cluster.RancherClusterName)
	if err != nil {
		return nil, err
	}

	var ca []byte
	if r.cfg.Rancher.CA.Name != "" {
		ca, err = k8s.ReadSecretKey(ctx, r.local, r.cfg.Namespace, r.cfg.Rancher.CA.Name, r.cfg.Rancher.CA.Key)
		if err != nil {
			return nil, err
		}
	}

	restCfg := r.rancherClient.ProxyRESTConfig(found.ID, ca, r.cfg.Rancher.InsecureSkipTLSVerify)
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building client for rancher proxy: %w", err)
	}
	return &clusterAccess{client: client, rancherClusterID: found.ID}, nil
}

// directAccess reaches a standalone cluster at its own endpoint.
//
// It prefers the durable credential the daemon provisioned for itself. On the
// first pass that credential does not exist, so the operator-supplied bootstrap
// credential is used once to create it — after which the bootstrap Secret can be
// deleted.
func (r *Reconciler) directAccess(ctx context.Context, cluster config.Cluster, logger *slog.Logger) (*clusterAccess, error) {
	creds, err := k8s.ReadCredentials(ctx, r.local, r.cfg.Namespace, cluster.CredentialsSecretName())
	if err == nil {
		client, err := clientFromToken(cluster.ServerURL(), creds.Token, creds.CA)
		if err != nil {
			return nil, err
		}
		return &clusterAccess{client: client}, nil
	}
	if !errors.Is(err, k8s.ErrNotFound) {
		return nil, err
	}

	// No durable credential yet. Either the operator ran the bootstrap
	// subcommand (in which case the credential would exist) or they supplied
	// bootstrap material for the daemon to use once, here.
	if cluster.BootstrapSecret.Name == "" {
		return nil, fmt.Errorf("cluster %q has no credential in %s/%s and no bootstrapSecret is configured; "+
			"run 'r2a-cert-sync bootstrap --cluster %s --endpoint %s --context <kubeconfig-context>', "+
			"or set bootstrapSecret to a secret holding a kubeconfig or token for the cluster",
			cluster.Name, r.cfg.Namespace, cluster.CredentialsSecretName(), cluster.Name, cluster.Endpoint)
	}

	logger.Info("no stored credential, bootstrapping from the supplied bootstrap secret",
		"bootstrap_secret", r.cfg.Namespace+"/"+cluster.BootstrapSecret.Name)

	bootstrapClient, err := r.bootstrapClient(ctx, cluster)
	if err != nil {
		return nil, err
	}

	provisioned, err := Provision(ctx, bootstrapClient, cluster)
	if err != nil {
		return nil, err
	}

	if err := k8s.WriteCredentials(ctx, r.local, r.cfg.Namespace, cluster.CredentialsSecretName(),
		provisioned, map[string]string{
			"app.kubernetes.io/managed-by": "r2a-cert-sync",
			"r2a-cert-sync.io/cluster":     cluster.Name,
		}); err != nil {
		return nil, fmt.Errorf("storing provisioned credential: %w", err)
	}

	logger.Info("stored durable credential; the bootstrap secret is no longer needed",
		"credentials_secret", r.cfg.Namespace+"/"+cluster.CredentialsSecretName(),
		"bootstrap_secret", r.cfg.Namespace+"/"+cluster.BootstrapSecret.Name)

	client, err := clientFromToken(cluster.ServerURL(), provisioned.Token, provisioned.CA)
	if err != nil {
		return nil, err
	}
	return &clusterAccess{client: client}, nil
}

// Provision installs the daemon's own identity in a downstream cluster and
// returns a durable credential for it.
//
// A bound token cannot be used here: its lifetime is capped by the API server,
// so the daemon would eventually lock itself out with no way back in without
// another human bootstrap. The identity is narrowly scoped instead.
func Provision(ctx context.Context, admin kubernetes.Interface, cluster config.Cluster) (*k8s.Credentials, error) {
	namespace := cluster.ServiceAccount.Namespace

	if err := downstream.EnsureAgentIdentity(ctx, admin, namespace, cluster.AgentServiceAccountName); err != nil {
		return nil, err
	}
	if _, err := downstream.EnsureArgoCDIdentity(ctx, admin, namespace, cluster.ServiceAccount.Name); err != nil {
		return nil, err
	}

	token, err := downstream.CreateLegacyToken(ctx, admin, namespace,
		cluster.AgentServiceAccountName, cluster.AgentServiceAccountName+"-token")
	if err != nil {
		return nil, err
	}

	ca, err := downstream.ClusterCA(ctx, admin, namespace)
	if err != nil {
		return nil, err
	}

	return &k8s.Credentials{Token: token, CA: ca}, nil
}

// bootstrapClient builds a client from the operator-supplied bootstrap Secret,
// which may hold either a kubeconfig or a bare bearer token.
func (r *Reconciler) bootstrapClient(ctx context.Context, cluster config.Cluster) (kubernetes.Interface, error) {
	secret, err := k8s.GetSecret(ctx, r.local, r.cfg.Namespace, cluster.BootstrapSecret.Name)
	if err != nil {
		if errors.Is(err, k8s.ErrNotFound) {
			return nil, fmt.Errorf("cluster %q has no stored credential and its bootstrap secret %s/%s is missing; "+
				"run 'r2a-cert-sync bootstrap --cluster %s' or create the secret manually: %w",
				cluster.Name, r.cfg.Namespace, cluster.BootstrapSecret.Name, cluster.Name, err)
		}
		return nil, err
	}

	if raw, ok := secret.Data[cluster.BootstrapSecret.Key]; ok && len(raw) > 0 {
		return clientFromKubeconfig(raw)
	}
	if raw, ok := secret.Data["token"]; ok && len(raw) > 0 {
		ca := secret.Data["ca.crt"]
		return clientFromToken(cluster.ServerURL(), string(raw), ca)
	}
	return nil, fmt.Errorf("bootstrap secret %s/%s has neither a %q nor a \"token\" key",
		r.cfg.Namespace, cluster.BootstrapSecret.Name, cluster.BootstrapSecret.Key)
}

// clientFromKubeconfig builds a client from raw kubeconfig bytes.
//
// The server address is taken from the kubeconfig as-is. RKE2 writes
// https://127.0.0.1:6443 into /etc/rancher/rke2/rke2.yaml, so a kubeconfig
// copied straight off a node must have its server rewritten to the reachable
// endpoint before being handed over.
func clientFromKubeconfig(raw []byte) (kubernetes.Interface, error) {
	apiConfig, err := clientcmd.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing bootstrap kubeconfig: %w", err)
	}
	restCfg, err := clientcmd.NewDefaultClientConfig(*apiConfig, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building client from bootstrap kubeconfig: %w", err)
	}
	restCfg.Timeout = clientTimeout

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("creating client from bootstrap kubeconfig: %w", err)
	}
	return client, nil
}

func clientFromToken(server, token string, ca []byte) (kubernetes.Interface, error) {
	cfg := &rest.Config{
		Host:        server,
		BearerToken: token,
		Timeout:     clientTimeout,
	}
	if len(ca) > 0 {
		cfg.TLSClientConfig = rest.TLSClientConfig{CAData: ca}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building client for %s: %w", server, err)
	}
	return client, nil
}
