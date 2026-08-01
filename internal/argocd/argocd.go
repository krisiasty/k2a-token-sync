// Package argocd renders and maintains the cluster Secrets ArgoCD watches.
//
// ArgoCD discovers external clusters from Secrets labelled
// argocd.argoproj.io/secret-type=cluster and re-reads them on every reconcile,
// so replacing the credential in place takes effect without restarting any
// ArgoCD component.
package argocd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/krisiasty/k2a-token-sync/internal/k8s"
)

const (
	// SecretTypeLabel and secretTypeCluster are how ArgoCD identifies a cluster
	// Secret.
	SecretTypeLabel   = "argocd.argoproj.io/secret-type" //nolint:gosec // a label key, not a credential
	secretTypeCluster = "cluster"

	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "k2a-token-sync"

	// TokenExpiryAnnotation records when the credential we wrote expires. It is
	// what lets the daemon decide whether a refresh is due without having to
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

// Render builds the Secret ArgoCD expects.
func (c ClusterSecret) Render(now time.Time) (*corev1.Secret, error) {
	// The credential is the point of this payload: ArgoCD reads bearerToken out
	// of the Secret's "config" key. It goes only into that Secret, never a log.
	payload, err := json.Marshal(clusterConfig{ //nolint:gosec // G117: serialising the credential here is intended
		BearerToken: c.BearerToken,
		TLSClientConfig: tlsClientConfig{
			CAData: c.CAData,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encoding cluster config: %w", err)
	}

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
		configKey: payload,
	}
	if c.Project != "" {
		data[projectKey] = []byte(c.Project)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.Name,
			Namespace:   c.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}, nil
}

// Apply writes the cluster Secret, creating it when absent.
func Apply(ctx context.Context, client kubernetes.Interface, desired ClusterSecret, now time.Time) error {
	secret, err := desired.Render(now)
	if err != nil {
		return err
	}
	return k8s.UpsertSecret(ctx, client, secret)
}

// Observed is the current state of a cluster Secret, used to decide whether a
// new credential is needed.
type Observed struct {
	Exists         bool
	Server         string
	DisplayName    string
	Project        string
	CAData         []byte
	HasBearerToken bool
	TokenExpiresAt time.Time
}

// Observe reads the existing cluster Secret. A missing Secret is not an error —
// it simply means nothing has been generated yet.
func Observe(ctx context.Context, client kubernetes.Interface, namespace, name string) (*Observed, error) {
	secret, err := k8s.GetSecret(ctx, client, namespace, name)
	if err != nil {
		if isNotFound(err) {
			return &Observed{}, nil
		}
		return nil, err
	}

	out := &Observed{
		Exists:      true,
		Server:      string(secret.Data[serverKey]),
		DisplayName: string(secret.Data[nameKey]),
		Project:     string(secret.Data[projectKey]),
	}

	if raw, ok := secret.Data[configKey]; ok {
		var parsed clusterConfig
		if err := json.Unmarshal(raw, &parsed); err == nil {
			out.HasBearerToken = parsed.BearerToken != ""
			out.CAData = parsed.TLSClientConfig.CAData
		}
	}

	if value, ok := secret.Annotations[TokenExpiryAnnotation]; ok {
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			out.TokenExpiresAt = parsed
		}
	}

	return out, nil
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
// Reissuing on every cycle would work but writes to the Secret churn ArgoCD's
// cluster cache, so a refresh happens only once the token is past half its
// lifetime or something has drifted.
func NeedsRefresh(observed *Observed, desired ClusterSecret, ttl time.Duration, now time.Time) RefreshReason {
	switch {
	case !observed.Exists:
		return ReasonMissing
	case !observed.HasBearerToken:
		return ReasonNoToken
	case observed.Server != desired.Server:
		return ReasonServerDrift
	case observed.DisplayName != desired.DisplayName:
		return ReasonNameDrift
	case observed.Project != desired.Project:
		return ReasonProjectDrift
	case len(desired.CAData) > 0 && string(observed.CAData) != string(desired.CAData):
		return ReasonCADrift
	case observed.TokenExpiresAt.IsZero():
		return ReasonUnknownExpiry
	case observed.TokenExpiresAt.Sub(now) < ttl/2:
		return ReasonExpiring
	}
	return ""
}

func isNotFound(err error) bool {
	return errors.Is(err, k8s.ErrNotFound)
}
