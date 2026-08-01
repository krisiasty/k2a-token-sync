// Package reconcile brings one cluster's ArgoCD registration up to date.
//
// The reconciler is the whole of k2a-token-sync in outline. For a cluster it connects with
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

// selfRenewInterval is how often k2a-token-sync replaces its own credential.
//
// This used to happen on every pass, which was the same thing while a pass meant
// a day. Now that a healthy pass costs nothing and runs every few minutes, the
// interval has to be said out loud, or the tool would mint a token every few
// minutes per cluster for no gain at all.
//
// A day keeps the remaining lifetime within a day of the full selfTokenTTL, so
// the outage k2a-token-sync can survive stays as close to that TTL as renewing
// continuously would achieve.
const selfRenewInterval = 24 * time.Hour

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
// prior is the status from the previous pass, and is k2a-token-sync's only memory:
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
		status.SelfCredentialExpiresAt = &metav1.Time{Time: access.expiresAt}
	}

	ca, err := downstream.ClusterCA(ctx, access.client, cluster.ServiceAccount.Namespace)
	if err != nil {
		return err
	}

	// Renewing here rather than at the end means a failure further down does not
	// cost k2a-token-sync its own credential's headroom: reaching this point proves
	// the current credential works, which is the only precondition for replacing
	// it.
	var selfIssuedAt time.Time
	if status.SelfCredentialIssuedAt != nil {
		selfIssuedAt = status.SelfCredentialIssuedAt.Time
	}
	if selfCredentialDue(cluster, selfIssuedAt, access.expiresAt, now) {
		r.renewSelfCredential(ctx, cluster, access, status, logger, now)
	}

	// Every pass, not only when a credential is due. ArgoCD's identity is the one
	// thing this tool depends on that a person can delete, and until this ran on
	// every pass the damage was invisible: the published Secret still carried a
	// bearer token, the fingerprint still matched, and the log went on reporting
	// the credential as current for the fortnight until the next reissue — while
	// ArgoCD had been failing since the moment the ServiceAccount went away.
	//
	// Two reads when nothing is wrong, which is the whole cost.
	repairs, err := downstream.EnsureArgoCDIdentity(ctx, access.client,
		cluster.ServiceAccount.Namespace, cluster.ServiceAccount.Name)
	if err != nil {
		return err
	}
	if repairs.Any() {
		logger.Warn("ArgoCD's downstream identity was missing and has been restored",
			"serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			"recreated_serviceaccount", repairs.ServiceAccount,
			"recreated_binding", repairs.Binding,
		)
	}

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
	desired.TokenIssuedAt = applied.TokenIssuedAt
	first := applied == (argocd.Fingerprint{})

	// Applying the registration is how k2a-token-sync observes what it published: an
	// apply returns the object and needs only the patch verb. Skipped on a
	// cluster's first pass, where a credential has to be minted anyway and
	// applying the label first would briefly show ArgoCD a cluster it cannot
	// authenticate to.
	var published string
	if !first {
		if published, err = argocd.ApplyRegistration(ctx, r.local, desired); err != nil {
			return r.argocdSecretError(cluster, err)
		}
	}

	reason := argocd.NeedsRefresh(applied, published, desired, cluster.TokenTTL, now)
	if repairs.ServiceAccount {
		// Outranks anything the published state comparison concluded: that compares
		// what was written against what is wanted, and both can look perfect while
		// the token itself is dead. A new ServiceAccount has a new UID, so every
		// token bound to the old one — including ArgoCD's — stopped authenticating
		// when it was deleted.
		reason = argocd.ReasonIdentityRecreated
	}
	if reason == "" {
		status.LastAction = "up-to-date"

		// This is far and away the most repeated line in the log — one per cluster
		// per pass, for as long as nothing is wrong — so it says what happened
		// ("nothing") and when that will stop being true, rather than leaving a
		// reader to subtract two timestamps to find out.
		//
		// Reaching here means the token is not yet past half the lifetime it was
		// granted, so this is always positive.
		reissueDueIn := argocd.ReissueAt(applied, cluster.TokenTTL).Sub(now)
		logger.Info("credential still current, nothing written",
			"token_expires_at", applied.TokenExpiresAt.UTC().Format(time.RFC3339),
			"reissue_due_in", reissueDueIn.Round(time.Minute).String(),
			"serving_cert_days_remaining", status.ServingCertDaysRemaining,
		)
		return nil
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

	// Prove the credential before publishing it, exactly as bootstrap and
	// self-renewal do for the tool's own. Every part of this has been checked
	// separately by now — the endpoint's certificate against this CA, the identity
	// existing, the API server issuing a token for it — but never the composition,
	// and it is the composition ArgoCD depends on.
	//
	// The CA passed here is the bundle about to be published, so this connects the
	// way ArgoCD will, not the way this tool happens to be configured.
	if err := r.verifyForArgoCD(ctx, cluster, token.Value, ca, logger); err != nil {
		return err
	}

	desired.BearerToken = token.Value
	desired.TokenExpiresAt = token.ExpiresAt
	desired.TokenIssuedAt = now

	// The credential goes on first. A Secret carrying no
	// argocd.argoproj.io/secret-type label is invisible to ArgoCD, so writing the
	// credential before the registration means ArgoCD never sees a cluster it
	// cannot authenticate to — not even between two applies.
	written, err := argocd.ApplyCredential(ctx, r.local, desired)
	if err != nil {
		return r.argocdSecretError(cluster, err)
	}
	observed, err := argocd.ApplyRegistration(ctx, r.local, desired)
	if err != nil {
		return r.argocdSecretError(cluster, err)
	}

	// What gets recorded is what was written, never what came back. The two applies
	// are separate calls, so a writer landing between them would have its
	// credential in that response — and recording that would adopt somebody else's
	// token as this tool's own, leaving the comparison satisfied by it forever.
	//
	// The response is still worth reading. Differing already means exactly that
	// happened, in the width of one round trip, which the next pass would report
	// anyway; saying so now names it while the cause is still in view.
	if observed != written {
		logger.Warn("the credential just published was overwritten immediately, before this pass finished",
			"secret", desired.Namespace+"/"+desired.Name,
			"hint", "something else is writing this Secret; see the warning against provisioning it declaratively",
		)
	}

	fingerprint := desired.Fingerprint()
	fingerprint.CredentialHash = written
	recordFingerprint(status, fingerprint)
	// The reason alone would read as a state rather than an action: "cluster secret
	// does not exist" is not what the pass did, and it is untrue by the time it is
	// recorded. Naming the action keeps the field consistent with "up-to-date" and
	// "failed", and the reason stays as the cause.
	status.LastAction = "reissued the credential: " + string(reason)
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
		Server:         status.AppliedServer,
		DisplayName:    status.AppliedDisplayName,
		Project:        status.AppliedProject,
		CAHash:         status.AppliedCAHash,
		CredentialHash: status.AppliedCredentialHash,
	}
	if status.TokenExpiresAt != nil {
		f.TokenExpiresAt = status.TokenExpiresAt.Time
	}
	if status.TokenIssuedAt != nil {
		f.TokenIssuedAt = status.TokenIssuedAt.Time
	}
	return f
}

func recordFingerprint(status *v1alpha1.ClusterConnectionStatus, f argocd.Fingerprint) {
	status.AppliedServer = f.Server
	status.AppliedDisplayName = f.DisplayName
	status.AppliedProject = f.Project
	status.AppliedCAHash = f.CAHash
	status.AppliedCredentialHash = f.CredentialHash
	status.TokenExpiresAt = &metav1.Time{Time: f.TokenExpiresAt}
	status.TokenIssuedAt = &metav1.Time{Time: f.TokenIssuedAt}
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
	case errors.Is(err, errCredentialRejected):
		return v1alpha1.ReasonCredentialRejected
	default:
		return v1alpha1.ReasonEndpointUnreachable
	}
}

var (
	// errNoCredential means the cluster has never been bootstrapped.
	errNoCredential = errors.New("no stored credential")

	// errCredentialExpired means k2a-token-sync was down for longer than its own
	// token's lifetime and has locked itself out. Only a bootstrap recovers it,
	// which is why the condition reason says so rather than reporting a 401.
	errCredentialExpired = errors.New("stored credential expired")

	// errCertificateInvalid means the endpoint's certificate cannot work for
	// ArgoCD, whatever the credential.
	errCertificateInvalid = errors.New("serving certificate unusable")

	// errCredentialRejected means a credential this tool just minted was refused by
	// the API server it was minted against. It gets a reason of its own because the
	// obvious alternative, EndpointUnreachable, would be actively misleading: the
	// endpoint answered, and said no.
	errCredentialRejected = errors.New("minted credential rejected")
)

// argocdSecretError explains a permission failure on a generated cluster Secret.
//
// k2a-token-sync needs create and patch on Secrets in ArgoCD's namespace and holds
// nothing else there. The raw API error names neither the Role nor the remedy, so
// a missing or narrowed Role otherwise looks like a bug in k2a-token-sync.
//
// Passing a nil error through keeps the call sites free of extra branching.
func (r *Reconciler) argocdSecretError(cluster config.Cluster, err error) error {
	if err == nil || !apierrors.IsForbidden(err) {
		return err
	}
	return fmt.Errorf("%w; k2a-token-sync's Role in namespace %s must allow create and patch on secrets "+
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
// There is deliberately no fallback: k2a-token-sync never holds administrative
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
		return nil, fmt.Errorf("%w: k2a-token-sync's credential for this cluster expired at %s; "+
			"bootstrap it again to issue a new one",
			errCredentialExpired, creds.ExpiresAt.UTC().Format(time.RFC3339))
	}

	client, err := r.clientForToken(cluster.ServerURL(), creds.Token, creds.CA)
	if err != nil {
		return nil, err
	}
	return &clusterAccess{client: client, ca: creds.CA, expiresAt: creds.ExpiresAt}, nil
}

// Provision installs k2a-token-sync's own identity in a downstream cluster and
// returns a credential for it.
//
// The identity is narrowly scoped — see downstream.EnsureSelfIdentity — so the
// credential this returns can do little beyond minting tokens and reading the
// cluster CA. The token is bound and therefore expires: k2a-token-sync renews it on
// every successful pass, and its lifetime is the length of an outage it can
// recover from unaided.
func Provision(ctx context.Context, admin kubernetes.Interface, cluster config.Cluster) (*k8s.Credentials, error) {
	namespace := cluster.ServiceAccount.Namespace

	if err := downstream.EnsureSelfIdentity(ctx, admin, namespace, cluster.SelfServiceAccountName); err != nil {
		return nil, err
	}
	if _, err := downstream.EnsureArgoCDIdentity(ctx, admin, namespace, cluster.ServiceAccount.Name); err != nil {
		return nil, err
	}

	token, err := downstream.MintToken(ctx, admin, namespace, cluster.SelfServiceAccountName, cluster.SelfTokenTTL)
	if err != nil {
		return nil, err
	}

	ca, err := downstream.ClusterCA(ctx, admin, namespace)
	if err != nil {
		return nil, err
	}

	return &k8s.Credentials{Token: token.Value, CA: ca, ExpiresAt: token.ExpiresAt}, nil
}

// verifyForArgoCD checks a freshly minted credential the way ArgoCD will use it.
//
// The two answers this produces are treated very differently, and deliberately so.
//
// Failing to authenticate is fatal. The call reaching the API server and coming
// back rejected means the token is worthless, so publishing it would replace a
// credential that may still work with one that certainly does not. The pass fails
// instead, the previous credential stays where it is, and the object says why.
//
// Being authenticated but unauthorised is only a warning. It almost certainly
// means the cluster-admin ClusterRole has been deleted or emptied, leaving the
// binding dangling — worth shouting about, since ArgoCD will get 403s on
// everything. But refusing to publish on that basis would be worse: authorisation
// can involve webhooks and authorizers this tool knows nothing about, and a false
// negative here would withhold a perfectly good credential and break the cluster
// this is meant to protect.
func (r *Reconciler) verifyForArgoCD(
	ctx context.Context,
	cluster config.Cluster,
	token string,
	ca []byte,
	logger *slog.Logger,
) error {
	client, err := r.clientForToken(cluster.ServerURL(), token, ca)
	if err != nil {
		return fmt.Errorf("building a client for the new credential: %w", err)
	}

	allowed, err := downstream.CanActAsClusterAdmin(ctx, client)
	if err != nil {
		return fmt.Errorf("%w: the credential just minted for ArgoCD does not work against %s: %w",
			errCredentialRejected, cluster.ServerURL(), err)
	}
	if !allowed {
		logger.Warn("ArgoCD's credential authenticates but is not authorised; ArgoCD will get 403s",
			"serviceaccount", cluster.ServiceAccount.Namespace+"/"+cluster.ServiceAccount.Name,
			"hint", "check that the cluster-admin ClusterRole still exists and still grants what its name implies")
	}
	return nil
}

// selfCredentialDue reports whether k2a-token-sync's own credential is old enough
// to replace.
//
// Two tests, and the second is the one that matters most. Age answers the ordinary
// case: replace a day-old credential, keeping the remaining lifetime within a day
// of the full TTL. Half the granted lifetime answers the case an API server capping
// tokens creates — a credential granted an hour cannot wait a day to be renewed, or
// it expires first and locks k2a-token-sync out of the cluster with no way back but
// a human re-running bootstrap.
//
// issuedAt is absent for a credential written before it was recorded, including one
// bootstrap created under an older release. Age is then inferred from what is left
// of the requested lifetime, which reads a capped credential as older than it is
// and renews sooner — the safe direction, and the same case that already warns.
func selfCredentialDue(cluster config.Cluster, issuedAt, expiresAt, now time.Time) bool {
	if expiresAt.IsZero() {
		// Nothing known about it. Minting establishes an expiry to reason from.
		return true
	}
	if issuedAt.IsZero() {
		return cluster.SelfTokenTTL-expiresAt.Sub(now) >= selfRenewInterval
	}

	granted := expiresAt.Sub(issuedAt)
	return now.Sub(issuedAt) >= selfRenewInterval || !now.Before(issuedAt.Add(granted/2))
}

// renewSelfCredential mints a replacement for k2a-token-sync's own credential using
// the credential it currently holds, and stores it once it is proven to work.
//
// Renewing daily rather than at half life is deliberate: the write is one API call
// against a Secret nothing watches, and it keeps the remaining lifetime within a
// day of the full TTL. Renewing at half life would halve the outage
// k2a-token-sync can survive, for no saving worth having.
//
// The verification is what makes self-renewal safe. Overwriting a working
// credential with a broken one would lock k2a-token-sync out of the cluster with no
// way back except a human re-running bootstrap, so the new token is used for one
// call before it replaces the old one.
func (r *Reconciler) renewSelfCredential(
	ctx context.Context,
	cluster config.Cluster,
	access *clusterAccess,
	status *v1alpha1.ClusterConnectionStatus,
	logger *slog.Logger,
	now time.Time,
) {
	namespace := cluster.ServiceAccount.Namespace

	token, err := downstream.MintToken(ctx, access.client, namespace, cluster.SelfServiceAccountName, cluster.SelfTokenTTL)
	if err != nil {
		logger.Warn("could not renew k2a-token-sync's own credential; the current one still works",
			"expires_at", formatTime(status.SelfCredentialExpiresAt), "error", err)
		return
	}

	granted := token.ExpiresAt.Sub(now)
	if granted < cluster.SelfTokenTTL*9/10 {
		logger.Warn("API server shortened k2a-token-sync's own token lifetime, which shortens the outage it can survive",
			"requested", cluster.SelfTokenTTL.String(),
			"granted", granted.Round(time.Minute).String(),
			"hint", "raise --service-account-max-token-expiration on the downstream API server, or lower selfTokenTTL")
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

	// Both, and only once the credential is stored. Recording an issue time for a
	// credential that failed to store would age out one that still works.
	status.SelfCredentialExpiresAt = &metav1.Time{Time: token.ExpiresAt}
	status.SelfCredentialIssuedAt = &metav1.Time{Time: now}
	logger.Debug("renewed k2a-token-sync's own credential", "expires_at", token.ExpiresAt.UTC().Format(time.RFC3339))
}

func formatTime(t *metav1.Time) string {
	if t == nil {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
}

// ClientFromCredentials builds a client for a downstream cluster from a stored
// credential, so the bootstrap subcommand can verify one exactly as k2a-token-sync
// will use it.
func ClientFromCredentials(server string, creds *k8s.Credentials) (kubernetes.Interface, error) {
	return clientFromToken(server, creds.Token, creds.CA)
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
