// Package argocd renders and maintains the cluster Secrets ArgoCD watches.
//
// ArgoCD discovers external clusters from Secrets labelled
// argocd.argoproj.io/secret-type=cluster and re-reads them on every reconcile,
// so replacing the credential in place takes effect without restarting any
// ArgoCD component.
package argocd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// SecretTypeLabel and secretTypeCluster are how ArgoCD identifies a cluster
	// Secret.
	SecretTypeLabel   = "argocd.argoproj.io/secret-type" //nolint:gosec // a label key, not a credential
	secretTypeCluster = "cluster"

	// FieldManagerRegistration owns everything except the credential.
	//
	// The split from FieldManagerCredential is what makes a write-only design
	// possible. Server-side apply removes fields a manager owns and then omits,
	// so a single manager applying the registration without a credential in hand
	// would strip the credential. With two, the registration can be re-applied on
	// every pass — a no-op when nothing changed — while the credential is written
	// only when one is minted.
	FieldManagerRegistration = "k2a-token-sync"

	// FieldManagerCredential owns data.config and nothing else.
	FieldManagerCredential = "k2a-token-sync-credential" //nolint:gosec // a field manager name, not a credential

	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "k2a-token-sync"

	// TokenExpiryAnnotation records when the credential we wrote expires. It is
	// what lets k2a-token-sync decide whether a refresh is due without having to
	// decode the token itself.
	TokenExpiryAnnotation = "k2a-token-sync.io/token-expires-at" //nolint:gosec // an annotation key, not a credential

	// ServingCertExpiryAnnotation records the observed expiry of the downstream
	// API server's serving certificate, so it is visible with kubectl.
	ServingCertExpiryAnnotation = "k2a-token-sync.io/serving-cert-expires-at"

	// ClusterNameAnnotation records which configured cluster owns this Secret.
	ClusterNameAnnotation = "k2a-token-sync.io/cluster"

	nameKey    = "name"
	serverKey  = "server"
	configKey  = "config"
	projectKey = "project"
)

// clusterConfig is ArgoCD's cluster credential payload, stored JSON-encoded
// under the "config" key. Field names and shapes must match ArgoCD's
// v1alpha1.ClusterConfig.
type clusterConfig struct {
	BearerToken     string          `json:"bearerToken,omitempty"`
	TLSClientConfig tlsClientConfig `json:"tlsClientConfig"`
}

type tlsClientConfig struct {
	// Insecure disables verification of the API server certificate.
	Insecure bool `json:"insecure"`

	// CAData is the PEM bundle ArgoCD verifies the API server against. It is
	// []byte in ArgoCD's own struct, so it marshals as base64.
	CAData []byte `json:"caData,omitempty"`

	// ServerName overrides the name checked against the certificate SANs.
	ServerName string `json:"serverName,omitempty"`
}

// ClusterSecret is the desired state of one ArgoCD cluster registration.
type ClusterSecret struct {
	Name      string
	Namespace string

	// DisplayName is the cluster name shown in ArgoCD.
	DisplayName string

	// Server is the API server URL ArgoCD connects to — the direct endpoint,
	// never a management-plane proxy.
	Server string

	BearerToken string
	CAData      []byte

	// Project optionally scopes the cluster to one ArgoCD project.
	Project string

	// TokenExpiresAt and ServingCertExpiresAt are recorded as annotations.
	TokenExpiresAt       time.Time
	ServingCertExpiresAt time.Time

	// TokenIssuedAt is when the credential was minted. It is recorded in status
	// rather than on the Secret, being of no interest to ArgoCD.
	TokenIssuedAt time.Time

	// ClusterName is the configured name of the owning cluster entry.
	ClusterName string

	ExtraLabels      map[string]string
	ExtraAnnotations map[string]string
}

// registrationConfig is everything except the credential: the labels that make
// ArgoCD notice the Secret at all, the bookkeeping annotations, and the server,
// name and project it reads.
//
// Every value here is derived from the cluster's configuration or from something
// observed about it, never from the clock. That is what makes an unchanged pass a
// genuine no-op: the apply finds nothing to change, so there is no write, no
// resourceVersion bump and nothing for ArgoCD to react to.
//
// So there is deliberately no last-sync annotation, tempting as one is. It would
// change on every pass, making every pass a write and every write an event for
// ArgoCD — which is precisely what forced the reconciliation interval to be long,
// and with it how long a deleted Secret stayed deleted. The same fact lives in the
// ClusterConnection's status.lastSyncTime, alongside everything else this tool
// records.
func (c ClusterSecret) registrationConfig() *applycorev1.SecretApplyConfiguration {
	labels := map[string]string{
		SecretTypeLabel: secretTypeCluster,
		managedByLabel:  managedByValue,
	}
	for k, v := range c.ExtraLabels {
		labels[k] = v
	}

	annotations := map[string]string{
		ClusterNameAnnotation: c.ClusterName,
	}
	if !c.TokenExpiresAt.IsZero() {
		annotations[TokenExpiryAnnotation] = c.TokenExpiresAt.UTC().Format(time.RFC3339)
	}
	if !c.ServingCertExpiresAt.IsZero() {
		annotations[ServingCertExpiryAnnotation] = c.ServingCertExpiresAt.UTC().Format(time.RFC3339)
	}
	for k, v := range c.ExtraAnnotations {
		annotations[k] = v
	}

	data := map[string][]byte{
		nameKey:   []byte(c.DisplayName),
		serverKey: []byte(c.Server),
	}
	if c.Project != "" {
		data[projectKey] = []byte(c.Project)
	}

	return applycorev1.Secret(c.Name, c.Namespace).
		WithType(corev1.SecretTypeOpaque).
		WithLabels(labels).
		WithAnnotations(annotations).
		WithData(data)
}

// credentialConfig is the credential half, and nothing else. It returns the
// encoded payload alongside the apply configuration, because the digest of what
// is being published has to come from the bytes actually sent.
func (c ClusterSecret) credentialConfig() (*applycorev1.SecretApplyConfiguration, []byte, error) {
	// ArgoCD reads bearerToken out of the Secret's "config" key. It goes only
	// into that Secret, never a log.
	payload, err := json.Marshal(clusterConfig{ //nolint:gosec // G117: serialising the credential here is intended
		BearerToken: c.BearerToken,
		TLSClientConfig: tlsClientConfig{
			CAData: c.CAData,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("encoding cluster config: %w", err)
	}

	return applycorev1.Secret(c.Name, c.Namespace).
		WithType(corev1.SecretTypeOpaque).
		WithData(map[string][]byte{configKey: payload}), payload, nil
}

// ApplyRegistration writes everything except the credential and reports the
// credential that is on the server afterwards, digested.
//
// An apply returns the object it produced, and needs only the patch verb, so
// this doubles as k2a-token-sync's only view of what it has published: no get, list
// or watch permission in ArgoCD's namespace is required anywhere. That is what
// makes a deleted or emptied Secret self-healing — the next pass recreates the
// registration and sees that the credential is gone.
func ApplyRegistration(ctx context.Context, client kubernetes.Interface, desired ClusterSecret) (string, error) {
	applied, err := client.CoreV1().Secrets(desired.Namespace).Apply(ctx, desired.registrationConfig(), metav1.ApplyOptions{
		FieldManager: FieldManagerRegistration,
		Force:        true,
	})
	if err != nil {
		return "", fmt.Errorf("applying cluster secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return hashCredential(applied), nil
}

// ApplyCredential writes the credential half and returns a digest of exactly what
// it published.
//
// The digest comes from the bytes sent rather than from any response, and that
// distinction is the whole point. A response describes the object as it stands
// when the server answers, which is not the same claim: another writer landing in
// between would have its credential digested and recorded as this tool's own,
// leaving the comparison permanently satisfied by somebody else's token.
func ApplyCredential(ctx context.Context, client kubernetes.Interface, desired ClusterSecret) (string, error) {
	config, payload, err := desired.credentialConfig()
	if err != nil {
		return "", err
	}
	if _, err := client.CoreV1().Secrets(desired.Namespace).Apply(ctx, config, metav1.ApplyOptions{
		FieldManager: FieldManagerCredential,
		Force:        true,
	}); err != nil {
		return "", fmt.Errorf("applying credential for %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return hashConfig(payload), nil
}

// hashCredential digests the credential half of a Secret as it currently stands
// on the server, or returns "" when there is effectively none.
//
// An empty string means the same thing it always did — no usable credential — so
// a config present but carrying no bearer token still counts as absent.
//
// The digest covers the whole config payload rather than the token alone, because
// ArgoCD's TLS settings live in there too: a hand-edited caData would otherwise
// survive unnoticed until the next reissue, breaking ArgoCD's connection while
// the token itself looked perfectly fine.
func hashCredential(secret *corev1.Secret) string {
	raw, ok := secret.Data[configKey]
	if !ok {
		return ""
	}
	var parsed clusterConfig
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ""
	}
	if parsed.BearerToken == "" {
		return ""
	}
	return hashConfig(raw)
}

// hashConfig digests a credential payload. Shared so that what is recorded and
// what is observed are always measured the same way.
func hashConfig(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Fingerprint is what a previous pass wrote, as recorded in the owning
// ClusterConnection's status.
//
// k2a-token-sync cannot read the generated Secret back, so this is how it knows
// whether the published registration still matches what is wanted. A zero value
// means nothing has been published, which is why it reads as "reissue".
type Fingerprint struct {
	Server         string
	DisplayName    string
	Project        string
	CAHash         string
	TokenExpiresAt time.Time
	TokenIssuedAt  time.Time

	// CredentialHash digests the credential this tool last published, taken from
	// the payload it sent.
	//
	// Every other field here describes what was wanted, and this one is no
	// different: it is the credential this tool intended ArgoCD to hold. What the
	// server actually holds arrives separately, from an apply response, and the two
	// disagreeing is the whole signal — so this side of the comparison must never
	// be sourced from a response, or it would agree with whatever it found.
	CredentialHash string
}

// Fingerprint describes what applying this ClusterSecret would publish, for
// recording in status after a successful write.
func (c ClusterSecret) Fingerprint() Fingerprint {
	return Fingerprint{
		Server:         c.Server,
		DisplayName:    c.DisplayName,
		Project:        c.Project,
		CAHash:         HashCA(c.CAData),
		TokenExpiresAt: c.TokenExpiresAt,
		TokenIssuedAt:  c.TokenIssuedAt,
	}
}

// HashCA reduces a CA bundle to a comparable digest, so drift can be detected
// from status without storing the bundle twice.
func HashCA(ca []byte) string {
	if len(ca) == 0 {
		return ""
	}
	sum := sha256.Sum256(ca)
	return hex.EncodeToString(sum[:])
}

// RefreshReason explains why a credential is being reissued. An empty reason
// means the registration is current and no write is required.
type RefreshReason string

// The reasons a credential is reissued. They are surfaced in logs and in
// /status, so they read as explanations rather than codes.
const (
	// ReasonUnrecorded fires when status holds no fingerprint. It deliberately
	// does not claim the Secret is absent: holding no read permission on it, this
	// tool cannot know that. Either the cluster has never been published or its
	// status was lost, and the two are indistinguishable from here.
	ReasonUnrecorded RefreshReason = "no registration recorded in status"
	ReasonNoToken    RefreshReason = "cluster secret has no bearer token"

	// ReasonCredentialReplaced means the published credential is not the one this
	// tool last wrote. Something else — External Secrets, a sealed-secrets
	// controller, a person with kubectl — has written over it, and whatever is
	// there now was not minted here, is not tracked here, and cannot be renewed
	// here.
	ReasonCredentialReplaced RefreshReason = "cluster secret holds a credential this tool did not write"

	// ReasonIdentityRecreated is the one reason that does not come from comparing
	// what was published against what is wanted. Both can match exactly while the
	// token is dead: a bound token carries the ServiceAccount's UID, so deleting
	// and recreating that account invalidates every token issued for it, leaving a
	// Secret that looks perfect and authenticates to nothing.
	ReasonIdentityRecreated RefreshReason = "downstream identity was recreated, so its tokens no longer authenticate"
	ReasonExpiring          RefreshReason = "token is past half its lifetime"
	ReasonServerDrift       RefreshReason = "recorded server URL does not match configuration"
	ReasonNameDrift         RefreshReason = "recorded cluster name does not match configuration"
	ReasonProjectDrift      RefreshReason = "recorded project does not match configuration"
	ReasonCADrift           RefreshReason = "recorded CA bundle does not match the cluster CA"
	ReasonUnknownExpiry     RefreshReason = "token expiry is not recorded"
)

// NeedsRefresh decides whether to mint a new credential.
//
// applied is what the last pass recorded in status; published is the credential
// the server reported when the registration was applied this pass, digested.
// Minting on every cycle would work, but each write churns ArgoCD's cluster
// cache, so a credential is reissued only when it is past half its lifetime, when
// something it depends on has drifted, or when it has gone missing or been
// replaced.
func NeedsRefresh(applied Fingerprint, published string, desired ClusterSecret, ttl time.Duration, now time.Time) RefreshReason {
	switch {
	case applied == (Fingerprint{}):
		// Nothing recorded: either this cluster has never been published, or
		// status was lost. Reissuing is the safe reading of both.
		return ReasonUnrecorded
	case published == "":
		return ReasonNoToken
	case applied.CredentialHash != "" && published != applied.CredentialHash:
		// Only meaningful once a hash has been recorded. A status written before
		// this existed has none, so the comparison waits for the next reissue to
		// record one rather than reissuing every cluster at once on upgrade.
		return ReasonCredentialReplaced
	case applied.Server != desired.Server:
		return ReasonServerDrift
	case applied.DisplayName != desired.DisplayName:
		return ReasonNameDrift
	case applied.Project != desired.Project:
		return ReasonProjectDrift
	case len(desired.CAData) > 0 && applied.CAHash != HashCA(desired.CAData):
		return ReasonCADrift
	case applied.TokenExpiresAt.IsZero():
		return ReasonUnknownExpiry
	case !now.Before(ReissueAt(applied, ttl)):
		return ReasonExpiring
	}
	return ""
}

// ReissueAt is when the published credential reaches half the lifetime it was
// actually granted.
//
// The requested lifetime is not always the granted one: an API server capping
// tokens with --service-account-max-token-expiration issues something far shorter,
// and comparing against half the request would then be true from the moment the
// token was minted — reissuing on every pass, forever, for a cluster that is
// merely configured conservatively.
//
// requested is the fallback for a credential published before the issue time was
// recorded. Such a status is indistinguishable from one written by an older
// release, so it keeps the old comparison until the next reissue records an issue
// time and the question settles itself.
func ReissueAt(applied Fingerprint, requested time.Duration) time.Time {
	if applied.TokenIssuedAt.IsZero() {
		return applied.TokenExpiresAt.Add(-requested / 2)
	}
	granted := applied.TokenExpiresAt.Sub(applied.TokenIssuedAt)
	return applied.TokenIssuedAt.Add(granted / 2)
}
