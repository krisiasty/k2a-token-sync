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

	// LastSyncAnnotation records the last successful reconciliation.
	LastSyncAnnotation = "k2a-token-sync.io/last-sync"

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

	// ClusterName is the configured name of the owning cluster entry.
	ClusterName string

	ExtraLabels      map[string]string
	ExtraAnnotations map[string]string
}

// registrationConfig is everything except the credential: the labels that make
// ArgoCD notice the Secret at all, the bookkeeping annotations, and the server,
// name and project it reads.
func (c ClusterSecret) registrationConfig(now time.Time) *applycorev1.SecretApplyConfiguration {
	labels := map[string]string{
		SecretTypeLabel: secretTypeCluster,
		managedByLabel:  managedByValue,
	}
	for k, v := range c.ExtraLabels {
		labels[k] = v
	}

	annotations := map[string]string{
		ClusterNameAnnotation: c.ClusterName,
		LastSyncAnnotation:    now.UTC().Format(time.RFC3339),
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

// credentialConfig is the credential half, and nothing else.
func (c ClusterSecret) credentialConfig() (*applycorev1.SecretApplyConfiguration, error) {
	// ArgoCD reads bearerToken out of the Secret's "config" key. It goes only
	// into that Secret, never a log.
	payload, err := json.Marshal(clusterConfig{ //nolint:gosec // G117: serialising the credential here is intended
		BearerToken: c.BearerToken,
		TLSClientConfig: tlsClientConfig{
			CAData: c.CAData,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encoding cluster config: %w", err)
	}

	return applycorev1.Secret(c.Name, c.Namespace).
		WithType(corev1.SecretTypeOpaque).
		WithData(map[string][]byte{configKey: payload}), nil
}

// ApplyRegistration writes everything except the credential and reports whether
// the credential is present on the server afterwards.
//
// An apply returns the object it produced, and needs only the patch verb, so
// this doubles as k2a-token-sync's only view of what it has published: no get, list
// or watch permission in ArgoCD's namespace is required anywhere. That is what
// makes a deleted or emptied Secret self-healing — the next pass recreates the
// registration and sees that the credential is gone.
func ApplyRegistration(ctx context.Context, client kubernetes.Interface, desired ClusterSecret, now time.Time) (bool, error) {
	applied, err := client.CoreV1().Secrets(desired.Namespace).Apply(ctx, desired.registrationConfig(now), metav1.ApplyOptions{
		FieldManager: FieldManagerRegistration,
		Force:        true,
	})
	if err != nil {
		return false, fmt.Errorf("applying cluster secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return hasBearerToken(applied), nil
}

// ApplyCredential writes the credential half.
func ApplyCredential(ctx context.Context, client kubernetes.Interface, desired ClusterSecret) error {
	config, err := desired.credentialConfig()
	if err != nil {
		return err
	}
	if _, err := client.CoreV1().Secrets(desired.Namespace).Apply(ctx, config, metav1.ApplyOptions{
		FieldManager: FieldManagerCredential,
		Force:        true,
	}); err != nil {
		return fmt.Errorf("applying credential for %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func hasBearerToken(secret *corev1.Secret) bool {
	raw, ok := secret.Data[configKey]
	if !ok {
		return false
	}
	var parsed clusterConfig
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return false
	}
	return parsed.BearerToken != ""
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
	ReasonMissing       RefreshReason = "cluster secret does not exist"
	ReasonNoToken       RefreshReason = "cluster secret has no bearer token"
	ReasonExpiring      RefreshReason = "token is past half its lifetime"
	ReasonServerDrift   RefreshReason = "recorded server URL does not match configuration"
	ReasonNameDrift     RefreshReason = "recorded cluster name does not match configuration"
	ReasonProjectDrift  RefreshReason = "recorded project does not match configuration"
	ReasonCADrift       RefreshReason = "recorded CA bundle does not match the cluster CA"
	ReasonUnknownExpiry RefreshReason = "token expiry is not recorded"
)

// NeedsRefresh decides whether to mint a new credential.
//
// applied is what the last pass recorded in status; hasCredential is what the
// server reported when the registration was applied this pass. Minting on every
// cycle would work, but each write churns ArgoCD's cluster cache, so a
// credential is reissued only when it is past half its lifetime, when something
// it depends on has drifted, or when it has gone missing entirely.
func NeedsRefresh(applied Fingerprint, hasCredential bool, desired ClusterSecret, ttl time.Duration, now time.Time) RefreshReason {
	switch {
	case applied == (Fingerprint{}):
		// Nothing recorded: either this cluster has never been published, or
		// status was lost. Reissuing is the safe reading of both.
		return ReasonMissing
	case !hasCredential:
		return ReasonNoToken
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
	case applied.TokenExpiresAt.Sub(now) < ttl/2:
		return ReasonExpiring
	}
	return ""
}
