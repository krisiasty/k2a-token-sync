// Package reconcile brings one cluster's ArgoCD registration up to date.
//
// The reconciler is the whole daemon in outline. For a cluster it connects with
// the credential provisioned for it, ensures ArgoCD's downstream identity exists,
// mints a short-lived credential, and publishes it in an ArgoCD cluster Secret
// pointing at the cluster's own endpoint. It also probes that endpoint's serving
// certificate and reports what it finds.
//
// Scheduling lives with the caller. Each cluster is reconciled on its own cadence,
// so a cluster that fails cannot drag the others onto its retry interval.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/argocd"
	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/downstream"
	"github.com/krisiasty/k2a-token-sync/internal/k8s"
)

// clientTimeout bounds any single downstream API call.
const clientTimeout = 30 * time.Second

// Reconciler holds the collaborators needed to reconcile a cluster.
type Reconciler struct {
	cfg    *config.Config
	local  kubernetes.Interface
	logger *slog.Logger

	// now and clientForToken are injectable for tests. clientForToken exists so
	// credential renewal can be exercised without a live API server, which is
	// worth a seam: the property it guards is that a credential which fails
	// verification never replaces one that works.
	now            func() time.Time
	clientForToken func(server, token string, ca []byte) (kubernetes.Interface, error)
}

// New builds a Reconciler.
func New(cfg *config.Config, local kubernetes.Interface, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		cfg:            cfg,
		local:          local,
		logger:         logger,
		now:            time.Now,
		clientForToken: clientFromToken,
	}
}

// Cluster reconciles one cluster and returns the status to record on its
// ClusterConnection.
//
// prior is the status from the previous pass, and is the daemon's only memory:
// holding no read permission on the generated Secret, it cannot inspect what it
// published, so the applied fingerprint in prior is how drift is detected.
//
// A status is returned even on failure, so the object always says what went
// wrong. The error is returned alongside it for the caller's backoff.
func (r *Reconciler) Cluster(
	ctx context.Context,
	cluster config.Cluster,
	prior v1alpha1.ClusterConnectionStatus,
	generation int64,
) (v1alpha1.ClusterConnectionStatus, error) {
	logger := r.logger.With("cluster", cluster.Name)
	now := r.now()

	status := prior
	status.ObservedGeneration = generation
	status.Secret = r.cfg.ArgoCDNamespace + "/" + cluster.SecretName

	if err := r.reconcile(ctx, cluster, &status, logger, now); err != nil {
		setCondition(&status, v1alpha1.ConditionReady, metav1.ConditionFalse, reasonFor(err), err.Error(), generation)
		status.LastAction = "failed"
		logger.Error("cluster reconciliation failed", "error", err)
		return status, err
	}

	status.LastSyncTime = &metav1.Time{Time: now}
	setCondition(&status, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonReady,
		"ArgoCD holds a current credential for this cluster", generation)
	return status, nil
}

func (r *Reconciler) reconcile(
	ctx context.Context,
	cluster config.Cluster,
	status *v1alpha1.ClusterConnectionStatus,
	logger *slog.Logger,
	now time.Time,
) error {
	access, err := r.access(ctx, cluster)
	if err != nil {
		return err
	}
	if !access.expiresAt.IsZero() {
		status.AgentCredentialExpiresAt = &metav1.Time{Time: access.expiresAt}
	}

	ca, err := downstream.ClusterCA(ctx, access.client, cluster.ServiceAccount.Namespace)
	if err != nil {
		return err
	}

	// Renewing here rather than at the end means a failure further down does not
	// cost the daemon its own credential's headroom: reaching this point proves
	// the current credential works, which is the only precondition for replacing
	// it.
	r.renewAgentCredential(ctx, cluster, access, status, logger)

	if err := r.probe(ctx, cluster, ca, status, logger); err != nil {
		return err
	}

	desired := argocd.ClusterSecret{
		Name:             cluster.SecretName,
		Namespace:        r.cfg.ArgoCDNamespace,
		DisplayName:      cluster.DisplayName,
		Server:           cluster.ServerURL(),
		CAData:           ca,
		Project:          cluster.Project,
		ClusterName:      cluster.Name,
		ExtraLabels:      cluster.Labels,
		ExtraAnnotations: cluster.Annotations,
	}
	if status.ServingCertExpiresAt != nil {
		desired.ServingCertExpiresAt = status.ServingCertExpiresAt.Time
	}

	applied := fingerprintFrom(*status)
	desired.TokenExpiresAt = applied.TokenExpiresAt
	first := applied == (argocd.Fingerprint{})

	// Applying the registration is how the daemon observes what it published: an
	// apply returns the object and needs only the patch verb. Skipped on a
	// cluster's first pass, where a credential has to be minted anyway and
	// applying the label first would briefly show ArgoCD a cluster it cannot
	// authenticate to.
	var hasCredential bool
	if !first {
		if hasCredential, err = argocd.ApplyRegistration(ctx, r.local, desired, now); err != nil {
			return r.argocdSecretError(cluster, err)
		}
	}

	reason := argocd.NeedsRefresh(applied, hasCredential, desired, cluster.TokenTTL, now)
	if reason == "" {
		status.LastAction = "up-to-date"
		logger.Info("registration current",
			"token_expires_at", applied.TokenExpiresAt.UTC().Format(time.RFC3339),
			"serving_cert_days_remaining", status.ServingCertDaysRemaining,
		)
		return nil
	}

	created, err := downstream.EnsureArgoCDIdentity(ctx, access.client,
		cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name)
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

	granted := token.ExpiresAt.Sub(now)
	if granted < cluster.TokenTTL*9/10 {
		logger.Warn("API server shortened the requested token lifetime",
			"requested", cluster.TokenTTL.String(),
			"granted", granted.Round(time.Minute).String(),
			"hint", "raise --service-account-max-token-expiration on the downstream API server, or lower tokenTTL")
	}

	desired.BearerToken = token.Value
	desired.TokenExpiresAt = token.ExpiresAt

	// The credential goes on first. A Secret carrying no
	// argocd.argoproj.io/secret-type label is invisible to ArgoCD, so writing the
	// credential before the registration means ArgoCD never sees a cluster it
	// cannot authenticate to — not even between two applies.
	if err := argocd.ApplyCredential(ctx, r.local, desired); err != nil {
		return r.argocdSecretError(cluster, err)
	}
	if _, err := argocd.ApplyRegistration(ctx, r.local, desired, now); err != nil {
		return r.argocdSecretError(cluster, err)
	}

	recordFingerprint(status, desired.Fingerprint())
	status.LastAction = string(reason)
	logger.Info("credential reissued",
		"reason", string(reason),
		"token_expires_at", token.ExpiresAt.UTC().Format(time.RFC3339),
		"serving_cert_days_remaining", status.ServingCertDaysRemaining,
	)
	return nil
}

// fingerprintFrom reads back what a previous pass recorded.
func fingerprintFrom(status v1alpha1.ClusterConnectionStatus) argocd.Fingerprint {
	f := argocd.Fingerprint{
		Server:      status.AppliedServer,
		DisplayName: status.AppliedDisplayName,
		Project:     status.AppliedProject,
		CAHash:      status.AppliedCAHash,
	}
	if status.TokenExpiresAt != nil {
		f.TokenExpiresAt = status.TokenExpiresAt.Time
	}
	return f
}

func recordFingerprint(status *v1alpha1.ClusterConnectionStatus, f argocd.Fingerprint) {
	status.AppliedServer = f.Server
	status.AppliedDisplayName = f.DisplayName
	status.AppliedProject = f.Project
	status.AppliedCAHash = f.CAHash
	status.TokenExpiresAt = &metav1.Time{Time: f.TokenExpiresAt}
}

func setCondition(
	status *v1alpha1.ClusterConnectionStatus,
	condType string,
	state metav1.ConditionStatus,
	reason, message string,
	generation int64,
) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             state,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// reasonFor maps a failure onto the condition reason that best describes it, so
// 'kubectl get ccon' distinguishes "never bootstrapped" from "cannot connect".
func reasonFor(err error) string {
	switch {
	case errors.Is(err, errNoCredential):
		return v1alpha1.ReasonAwaitingCredential
	case errors.Is(err, errCredentialExpired):
		return v1alpha1.ReasonCredentialExpired
	case errors.Is(err, errCertificateInvalid):
		return v1alpha1.ReasonCertificateInvalid
	default:
		return v1alpha1.ReasonEndpointUnreachable
	}
}

var (
	// errNoCredential means the cluster has never been bootstrapped.
	errNoCredential = errors.New("no stored credential")

	// errCredentialExpired means the daemon was down for longer than its own
	// token's lifetime and has locked itself out. Only a bootstrap recovers it,
	// which is why the condition reason says so rather than reporting a 401.
	errCredentialExpired = errors.New("stored credential expired")

	// errCertificateInvalid means the endpoint's certificate cannot work for
	// ArgoCD, whatever the credential.
	errCertificateInvalid = errors.New("serving certificate unusable")
)

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

// probe inspects the serving certificate at the endpoint and records what it
// finds, failing on conditions that would break ArgoCD.
func (r *Reconciler) probe(
	ctx context.Context,
	cluster config.Cluster,
	ca []byte,
	status *v1alpha1.ClusterConnectionStatus,
	logger *slog.Logger,
) error {
	cert, err := downstream.ProbeServingCert(ctx, cluster.Endpoint, ca)
	if err != nil {
		return fmt.Errorf("probing the endpoint: %w", err)
	}

	status.ServingCertExpiresAt = &metav1.Time{Time: cert.NotAfter}
	status.ServingCertDaysRemaining = int32(cert.DaysRemaining()) //nolint:gosec // G115: a day count cannot overflow int32

	if cert.HostnameError != nil {
		// Fail loudly: publishing this credential would produce a cluster
		// registration ArgoCD can never connect to, which is far harder to
		// diagnose from the ArgoCD side than an explicit error here.
		setCondition(status, v1alpha1.ConditionServingCertificateValid, metav1.ConditionFalse,
			v1alpha1.ReasonCertificateInvalid, cert.HostnameError.Error(), status.ObservedGeneration)
		return fmt.Errorf("%w: %w", errCertificateInvalid, cert.HostnameError)
	}

	if !cert.TrustedByCA {
		message := fmt.Sprintf("the certificate presented at %s does not verify against the cluster CA "+
			"published as ArgoCD's caData; ArgoCD would reject this endpoint", cluster.Endpoint)
		setCondition(status, v1alpha1.ConditionServingCertificateValid, metav1.ConditionFalse,
			v1alpha1.ReasonCertificateInvalid, message, status.ObservedGeneration)
		return fmt.Errorf("%w: %s", errCertificateInvalid, message)
	}

	if remaining := cert.NotAfter.Sub(r.now()); remaining < cluster.ExpiryWarnThreshold {
		message := fmt.Sprintf("expires in %d days; rotate or reissue the API server's serving certificate, "+
			"which depends on your distribution", cert.DaysRemaining())
		setCondition(status, v1alpha1.ConditionServingCertificateValid, metav1.ConditionFalse,
			v1alpha1.ReasonCertificateInvalid, message, status.ObservedGeneration)
		logger.Warn("API server certificate is nearing expiry",
			"expires_at", cert.NotAfter.UTC().Format(time.RFC3339),
			"days_remaining", cert.DaysRemaining(),
			"remedy", "rotate or reissue the API server's serving certificate; how depends on your distribution",
		)
		// Not an error: the registration still works until it expires, and
		// refusing to publish would break a cluster that is merely due for
		// maintenance.
		return nil
	}

	setCondition(status, v1alpha1.ConditionServingCertificateValid, metav1.ConditionTrue,
		v1alpha1.ReasonReady, "trusted, covers the endpoint, and not near expiry", status.ObservedGeneration)
	return nil
}

// clusterAccess is a connection to a downstream cluster.
type clusterAccess struct {
	client kubernetes.Interface

	// ca is the bundle the stored credential was verified against, kept so a
	// renewed credential can be stored with the same one.
	ca []byte

	// expiresAt is when the credential in use stops working, or zero if the
	// stored credential does not record it.
	expiresAt time.Time
}

// access reaches a cluster at its own endpoint with the credential provisioned
// for it at bootstrap.
//
// There is deliberately no fallback: the daemon never holds administrative
// material for a downstream cluster, so a cluster with no credential is reported
// as awaiting bootstrap rather than bootstrapped from something lying around.
func (r *Reconciler) access(ctx context.Context, cluster config.Cluster) (*clusterAccess, error) {
	creds, err := k8s.ReadCredentials(ctx, r.local, r.cfg.Namespace, cluster.CredentialsSecretName())
	if err != nil {
		if errors.Is(err, k8s.ErrNotFound) {
			return nil, fmt.Errorf("%w in %s/%s; bootstrap this cluster with "+
				"'k2a-token-sync bootstrap --cluster %s --endpoint %s --from-kubeconfig <file>', "+
				"or provision the identities and that Secret with your own automation",
				errNoCredential, r.cfg.Namespace, cluster.CredentialsSecretName(), cluster.Name, cluster.Endpoint)
		}
		return nil, err
	}

	if !creds.ExpiresAt.IsZero() && !creds.ExpiresAt.After(r.now()) {
		return nil, fmt.Errorf("%w: the daemon's credential for this cluster expired at %s; "+
			"bootstrap it again to issue a new one",
			errCredentialExpired, creds.ExpiresAt.UTC().Format(time.RFC3339))
	}

	client, err := r.clientForToken(cluster.ServerURL(), creds.Token, creds.CA)
	if err != nil {
		return nil, err
	}
	return &clusterAccess{client: client, ca: creds.CA, expiresAt: creds.ExpiresAt}, nil
}

// Provision installs the daemon's own identity in a downstream cluster and
// returns a credential for it.
//
// The identity is narrowly scoped — see downstream.EnsureAgentIdentity — so the
// credential this returns can do little beyond minting tokens and reading the
// cluster CA. The token is bound and therefore expires: the daemon renews it on
// every successful pass, and its lifetime is the length of an outage it can
// recover from unaided.
func Provision(ctx context.Context, admin kubernetes.Interface, cluster config.Cluster) (*k8s.Credentials, error) {
	namespace := cluster.ServiceAccount.Namespace

	if err := downstream.EnsureAgentIdentity(ctx, admin, namespace, cluster.AgentServiceAccountName); err != nil {
		return nil, err
	}
	if _, err := downstream.EnsureArgoCDIdentity(ctx, admin, namespace, cluster.ServiceAccount.Name); err != nil {
		return nil, err
	}

	token, err := downstream.MintToken(ctx, admin, namespace, cluster.AgentServiceAccountName, cluster.AgentTokenTTL)
	if err != nil {
		return nil, err
	}

	ca, err := downstream.ClusterCA(ctx, admin, namespace)
	if err != nil {
		return nil, err
	}

	return &k8s.Credentials{Token: token.Value, CA: ca, ExpiresAt: token.ExpiresAt}, nil
}

// renewAgentCredential mints a replacement for the daemon's own credential using
// the credential it currently holds, and stores it once it is proven to work.
//
// Renewing on every successful pass rather than at half life is deliberate: the
// write is one API call against a Secret nothing watches, and it keeps the
// remaining lifetime near the full TTL at all times. Renewing at half life would
// halve the outage the daemon can survive, for no saving worth having.
//
// The verification is what makes self-renewal safe. Overwriting a working
// credential with a broken one would lock the daemon out of the cluster with no
// way back except a human re-running bootstrap, so the new token is used for one
// call before it replaces the old one.
func (r *Reconciler) renewAgentCredential(
	ctx context.Context,
	cluster config.Cluster,
	access *clusterAccess,
	status *v1alpha1.ClusterConnectionStatus,
	logger *slog.Logger,
) {
	namespace := cluster.ServiceAccount.Namespace

	token, err := downstream.MintToken(ctx, access.client, namespace, cluster.AgentServiceAccountName, cluster.AgentTokenTTL)
	if err != nil {
		logger.Warn("could not renew the daemon's own credential; the current one still works",
			"expires_at", formatTime(status.AgentCredentialExpiresAt), "error", err)
		return
	}

	granted := token.ExpiresAt.Sub(r.now())
	if granted < cluster.AgentTokenTTL*9/10 {
		logger.Warn("API server shortened the daemon's own token lifetime, which shortens the outage it can survive",
			"requested", cluster.AgentTokenTTL.String(),
			"granted", granted.Round(time.Minute).String(),
			"hint", "raise --service-account-max-token-expiration on the downstream API server, or lower agentTokenTTL")
	}

	probe, err := r.clientForToken(cluster.ServerURL(), token.Value, access.ca)
	if err != nil {
		logger.Warn("could not build a client for the renewed credential; keeping the current one", "error", err)
		return
	}
	if _, err := downstream.ClusterCA(ctx, probe, namespace); err != nil {
		logger.Warn("the renewed credential does not work; keeping the current one", "error", err)
		return
	}

	if err := k8s.WriteCredentials(ctx, r.local, r.cfg.Namespace, cluster.CredentialsSecretName(),
		&k8s.Credentials{Token: token.Value, CA: access.ca, ExpiresAt: token.ExpiresAt},
		map[string]string{
			"app.kubernetes.io/managed-by": "k2a-token-sync",
			"k2a-token-sync.io/cluster":    cluster.Name,
		}); err != nil {
		logger.Warn("could not store the renewed credential; keeping the current one", "error", err)
		return
	}

	status.AgentCredentialExpiresAt = &metav1.Time{Time: token.ExpiresAt}
	logger.Debug("renewed the daemon's own credential", "expires_at", token.ExpiresAt.UTC().Format(time.RFC3339))
}

func formatTime(t *metav1.Time) string {
	if t == nil {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
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
