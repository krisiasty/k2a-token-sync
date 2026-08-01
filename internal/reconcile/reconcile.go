// Package reconcile drives one pass over every configured cluster.
//
// The reconciler is the whole daemon in outline. For each cluster it connects
// with the credential provisioned for it, ensures ArgoCD's downstream identity
// exists, mints a short-lived credential, and publishes it in an ArgoCD cluster
// Secret pointing at the cluster's direct endpoint. It also probes that
// endpoint's serving certificate and reports what it finds.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/krisiasty/k2a-token-sync/internal/argocd"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/downstream"
	"github.com/krisiasty/k2a-token-sync/internal/k8s"
)

// clientTimeout bounds any single downstream API call.
const clientTimeout = 30 * time.Second

// Reconciler holds the collaborators needed for a reconciliation pass.
type Reconciler struct {
	cfg    *config.Config
	local  kubernetes.Interface
	logger *slog.Logger

	// applied records what was last published for each cluster, keyed by
	// cluster name. The daemon holds no read permission on Secrets in ArgoCD's
	// namespace, so this is how drift is detected.
	//
	// In memory for now, which means a restart reissues once per cluster —
	// harmless, and self-correcting. It moves into ClusterConnection status,
	// where it survives restarts, when the inventory does.
	applied map[string]argocd.Fingerprint

	// now is injectable for tests.
	now func() time.Time
}

// New builds a Reconciler.
func New(cfg *config.Config, local kubernetes.Interface, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		cfg:     cfg,
		local:   local,
		logger:  logger,
		applied: make(map[string]argocd.Fingerprint, len(cfg.Clusters)),
		now:     time.Now,
	}
}

// ClusterStatus is the outcome of reconciling one cluster.
type ClusterStatus struct {
	Name     string    `json:"name"`
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

		logger := r.logger.With("cluster", cluster.Name)
		status := ClusterStatus{
			Name:     cluster.Name,
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

	now := r.now()
	applied := r.applied[cluster.Name]
	first := applied == (argocd.Fingerprint{})

	// Carry the recorded expiry into the desired state so the registration
	// re-applies with the annotation it already had, rather than dropping it.
	desired.TokenExpiresAt = applied.TokenExpiresAt

	// Applying the registration is how the daemon observes what it published: an
	// apply returns the object and needs only the patch verb. Skipped on the
	// first pass for a cluster, where the credential has to be minted anyway and
	// applying the label first would briefly show ArgoCD a cluster with no
	// credential.
	var hasCredential bool
	if !first {
		if hasCredential, err = argocd.ApplyRegistration(ctx, r.local, desired, now); err != nil {
			return r.argocdSecretError(cluster, err)
		}
	}

	reason := argocd.NeedsRefresh(applied, hasCredential, desired, cluster.TokenTTL, now)
	if reason == "" {
		status.Action = "up-to-date"
		status.TokenExpiresAt = applied.TokenExpiresAt
		logger.Info("registration current",
			"token_expires_at", applied.TokenExpiresAt.UTC().Format(time.RFC3339),
			"serving_cert_days_left", status.ServingCertDaysLeft,
		)
		return nil
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

	// The credential goes on first when the Secret is new. A Secret carrying no
	// argocd.argoproj.io/secret-type label is invisible to ArgoCD, so writing the
	// credential before the registration means ArgoCD never sees a cluster it
	// cannot authenticate to — not even for the moment between two applies.
	if err := argocd.ApplyCredential(ctx, r.local, desired); err != nil {
		return r.argocdSecretError(cluster, err)
	}
	if _, err := argocd.ApplyRegistration(ctx, r.local, desired, now); err != nil {
		return r.argocdSecretError(cluster, err)
	}
	r.applied[cluster.Name] = desired.Fingerprint()

	status.Action = string(reason)
	status.TokenExpiresAt = token.ExpiresAt
	logger.Info("credential reissued",
		"reason", string(reason),
		"token_expires_at", token.ExpiresAt.UTC().Format(time.RFC3339),
		"serving_cert_days_left", status.ServingCertDaysLeft,
	)
	return nil
}

// argocdSecretError explains a permission failure on a generated cluster Secret.
//
// The daemon needs create and patch on Secrets in ArgoCD's namespace and holds
// nothing else there. The raw API error names neither the Role nor the remedy, so
// a missing or narrowed Role otherwise looks like a bug in the daemon.
//
// Passing a nil error through keeps the call sites free of extra branching.
func (r *Reconciler) argocdSecretError(cluster config.Cluster, err error) error {
	if err == nil || !apierrors.IsForbidden(err) {
		return err
	}
	return fmt.Errorf("%w; the daemon's Role in namespace %s must allow create and patch on secrets "+
		"so it can maintain %q", err, r.cfg.ArgoCDNamespace, cluster.SecretName)
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
			"remedy", "rotate or reissue the API server's serving certificate; how depends on your distribution",
		)
	}
	return nil
}

// clusterAccess is a connection to a downstream cluster.
type clusterAccess struct {
	client kubernetes.Interface
}

// access reaches a cluster at its own endpoint.
//
// It prefers the durable credential the daemon provisioned for itself. On the
// first pass that credential does not exist, so the operator-supplied bootstrap
// credential is used once to create it — after which the bootstrap Secret can be
// deleted.
func (r *Reconciler) access(ctx context.Context, cluster config.Cluster, logger *slog.Logger) (*clusterAccess, error) {
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
			"run 'k2a-token-sync bootstrap --cluster %s --endpoint %s --context <kubeconfig-context>', "+
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
			"app.kubernetes.io/managed-by": "k2a-token-sync",
			"k2a-token-sync.io/cluster":    cluster.Name,
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
				"run 'k2a-token-sync bootstrap --cluster %s' or create the secret manually: %w",
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
// The server address is taken from the kubeconfig as-is. A kubeconfig written
// for use on the node itself usually points at https://127.0.0.1:6443, so one
// copied straight off a control-plane node must have its server rewritten to the
// reachable endpoint before being handed over.
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
