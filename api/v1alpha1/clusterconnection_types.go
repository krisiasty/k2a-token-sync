package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types set on a ClusterConnection.
const (
	// ConditionReady reports whether ArgoCD currently holds a usable
	// registration for this cluster.
	ConditionReady = "Ready"

	// ConditionServingCertificateValid reports on the certificate the API
	// server presents at the configured endpoint: trusted by the published CA,
	// covering the endpoint name, and not near expiry.
	ConditionServingCertificateValid = "ServingCertificateValid"

	// ConditionSelfCredentialValid reports whether k2a-token-sync is still able to
	// renew its own credential for this cluster.
	//
	// Separate from Ready because the two can genuinely disagree, and for a long
	// time. Renewal failing does not stop ArgoCD working: the credential in hand
	// keeps minting ArgoCD's tokens until it expires. What it does mean is that a
	// clock is running, and at the end of it only a person re-running bootstrap can
	// restore access. That deserves to be visible from the first failure rather
	// than at the end.
	ConditionSelfCredentialValid = "SelfCredentialValid" //nolint:gosec // a condition type, not a credential

	// ConditionConflict reports that another ClusterConnection claims the same
	// secretName. Admission cannot see this, since it spans objects.
	ConditionConflict = "Conflict"
)

// Reasons accompanying those conditions. They are read by operators far more
// often than by code, so they name the situation rather than the code path.
const (
	ReasonReady               = "Ready"
	ReasonAwaitingCredential  = "AwaitingCredential"
	ReasonCredentialExpired   = "CredentialExpired" //nolint:gosec // a condition reason, not a credential
	ReasonEndpointUnreachable = "EndpointUnreachable"
	ReasonCertificateInvalid  = "CertificateInvalid"
	ReasonCredentialRejected  = "CredentialRejected" //nolint:gosec // a condition reason, not a credential
	ReasonCredentialReplaced  = "CredentialReplaced" //nolint:gosec // a condition reason, not a credential
	ReasonSecretNameConflict  = "SecretNameConflict"
	ReasonInvalidSpec         = "InvalidSpec"

	// The three ways renewing k2a-token-sync's own credential can fail. They are
	// distinct because they point at different places: the downstream cluster's
	// RBAC or API server, the credential the API server just issued, and this
	// tool's own namespace.
	ReasonRenewalMintFailed = "RenewalMintFailed"
	ReasonRenewalUnverified = "RenewalUnverified"
	ReasonRenewalNotStored  = "RenewalNotStored"

	// ReasonSelfCredentialExpiring means renewal has been failing long enough that
	// the credential in use is running out. Access to the cluster is at stake, not
	// just freshness.
	ReasonSelfCredentialExpiring = "SelfCredentialExpiring" //nolint:gosec // a condition reason, not a credential
)

// ClusterConnection declares how to reach one downstream cluster, how to
// authenticate to it, and how long the credentials it publishes should live.
//
// Deleting one stops maintenance. It deliberately does not delete the generated
// ArgoCD Secret: k2a-token-sync holds no delete permission in ArgoCD's namespace, so
// removing that Secret stays an explicit operator action.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=ccon,categories=argocd
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.endpoint`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Self Valid",type=string,JSONPath=`.status.conditions[?(@.type=="SelfCredentialValid")].status`
// A date column is rendered by kubectl as time *elapsed*, which is negative for
// anything in the future and prints as "<invalid>". Both of these are expiries,
// so they are strings and show the timestamp itself. Age below is a real date
// column, and correct as one.
// +kubebuilder:printcolumn:name="Token Expires",type=string,JSONPath=`.status.tokenExpiresAt`
// +kubebuilder:printcolumn:name="Self Expires",type=string,JSONPath=`.status.selfCredentialExpiresAt`
// +kubebuilder:printcolumn:name="Cert Days",type=integer,JSONPath=`.status.servingCertDaysRemaining`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ClusterConnection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterConnectionSpec   `json:"spec,omitempty"`
	Status ClusterConnectionStatus `json:"status,omitempty"`
}

// ClusterConnectionList is the list form returned when polling the API.
//
// +kubebuilder:object:root=true
type ClusterConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ClusterConnection `json:"items"`
}

// ClusterConnectionSpec is the desired registration.
//
// The object's name is the cluster's name: it identifies the cluster in logs, in
// the ArgoCD cluster list unless displayName overrides it, and in the names of
// the Secrets derived from it.
type ClusterConnectionSpec struct {
	// Endpoint is the address ArgoCD connects to, as "host" or "host:port".
	// Port 6443 is assumed when omitted. This must be an address that bypasses
	// any management-plane proxy, and it must appear in the API server's
	// serving-certificate SANs — k2a-token-sync refuses to publish a registration
	// whose certificate does not cover it.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?(:[0-9]{1,5})?$`
	Endpoint string `json:"endpoint"`

	// DisplayName is the name ArgoCD shows. Defaults to the object's name.
	//
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// SecretName is the ArgoCD cluster Secret this connection maintains, in the
	// namespace k2a-token-sync serves. Defaults to "cluster-<name>".
	//
	// The cluster- prefix is required rather than conventional. k2a-token-sync holds
	// namespace-wide patch on Secrets in ArgoCD's namespace, because cluster
	// names are not known when its RBAC is created and RBAC cannot scope by
	// prefix. The prefix is what keeps a connection from being pointed at
	// ArgoCD's own Secrets.
	//
	// +optional
	// +kubebuilder:validation:Pattern=`^cluster-[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SecretName string `json:"secretName,omitempty"`

	// Project optionally scopes the cluster to a single ArgoCD project.
	//
	// +optional
	Project string `json:"project,omitempty"`

	// ServiceAccount is the downstream identity ArgoCD authenticates as. The
	// default matches what `argocd cluster add` installs, so an existing
	// registration can be taken over without touching the cluster.
	//
	// +optional
	// +kubebuilder:default={name: "argocd-manager", namespace: "kube-system"}
	ServiceAccount *ServiceAccountRef `json:"serviceAccount,omitempty"`

	// SelfServiceAccountName is the downstream identity k2a-token-sync itself
	// authenticates as, in the same namespace as ServiceAccount. It is narrowly
	// scoped: it can mint tokens and read the cluster CA, nothing more.
	//
	// +optional
	// +kubebuilder:default="k2a-token-sync"
	SelfServiceAccountName string `json:"selfServiceAccountName,omitempty"`

	// TokenTTL is the lifetime requested for ArgoCD's credential, reissued at
	// half life. The API server may cap it; the granted lifetime is what is
	// scheduled against, and a shortened one is reported.
	//
	// +optional
	// +kubebuilder:default="720h"
	// +kubebuilder:validation:Pattern=`^([0-9]+h)?([0-9]+m)?([0-9]+s)?$`
	TokenTTL string `json:"tokenTTL,omitempty"`

	// SelfTokenTTL is the lifetime requested for k2a-token-sync's own credential,
	// renewed on every successful pass. It is therefore also the downtime
	// budget: if k2a-token-sync does not run for this long, it locks itself out and
	// the cluster must be bootstrapped again.
	//
	// +optional
	// +kubebuilder:default="2160h"
	// +kubebuilder:validation:Pattern=`^([0-9]+h)?([0-9]+m)?([0-9]+s)?$`
	SelfTokenTTL string `json:"selfTokenTTL,omitempty"`

	// ExpiryWarnThreshold is the remaining serving-certificate lifetime below
	// which k2a-token-sync starts warning.
	//
	// +optional
	// +kubebuilder:default="2160h"
	// +kubebuilder:validation:Pattern=`^([0-9]+h)?([0-9]+m)?([0-9]+s)?$`
	ExpiryWarnThreshold string `json:"expiryWarnThreshold,omitempty"`

	// Labels are merged into the generated ArgoCD Secret. The controller-owned
	// argocd.argoproj.io/secret-type and app.kubernetes.io/managed-by keys cannot
	// be overridden.
	//
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are merged into the generated ArgoCD Secret. The
	// controller-owned k2a-token-sync.io/cluster,
	// k2a-token-sync.io/token-expires-at and
	// k2a-token-sync.io/serving-cert-expires-at keys cannot be overridden.
	//
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ServiceAccountRef identifies a downstream ServiceAccount.
type ServiceAccountRef struct {
	// +kubebuilder:default="argocd-manager"
	// +optional
	Name string `json:"name,omitempty"`

	// +kubebuilder:default="kube-system"
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// ClusterConnectionStatus is what k2a-token-sync observed and published.
//
// It is also k2a-token-sync's memory. Holding no read permission on Secrets in
// ArgoCD's namespace, it cannot inspect what it published, so the applied
// fingerprint below is how drift is detected on the next pass.
type ClusterConnectionStatus struct {
	// ObservedGeneration is the spec generation this status describes. A
	// generation ahead of it means the object was edited and is due immediately.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Secret is the generated ArgoCD cluster Secret, as namespace/name.
	//
	// +optional
	Secret string `json:"secret,omitempty"`

	// TokenExpiresAt is when ArgoCD's credential expires.
	//
	// +optional
	TokenExpiresAt *metav1.Time `json:"tokenExpiresAt,omitempty"`

	// TokenIssuedAt is when ArgoCD's credential was minted.
	//
	// Recorded because the granted lifetime is not the requested one whenever an
	// API server caps it with --service-account-max-token-expiration, and half of
	// the granted lifetime is what a reissue is actually due at. Without this,
	// nothing here says how long the token was ever meant to live.
	//
	// +optional
	TokenIssuedAt *metav1.Time `json:"tokenIssuedAt,omitempty"`

	// SelfCredentialExpiresAt is when k2a-token-sync's own credential expires, and
	// so when it would lock itself out if it stopped running.
	//
	// +optional
	SelfCredentialExpiresAt *metav1.Time `json:"selfCredentialExpiresAt,omitempty"`

	// SelfCredentialIssuedAt is when k2a-token-sync's own credential was minted, and
	// therefore how old it is — which is what decides a renewal, rather than being
	// inferred from what is left of the requested lifetime.
	//
	// +optional
	SelfCredentialIssuedAt *metav1.Time `json:"selfCredentialIssuedAt,omitempty"`

	// +optional
	ServingCertExpiresAt *metav1.Time `json:"servingCertExpiresAt,omitempty"`

	// +optional
	ServingCertDaysRemaining int32 `json:"servingCertDaysRemaining,omitempty"`

	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// LastAction records what the last pass did — reissued a credential, found
	// the registration current, or failed.
	//
	// +optional
	LastAction string `json:"lastAction,omitempty"`

	// AppliedServer, AppliedDisplayName, AppliedProject and AppliedCAHash are
	// the fingerprint of what was last written to the ArgoCD Secret. Comparing
	// them with the desired values detects drift without reading the Secret
	// back. An empty fingerprint means "reissue", which is the safe default
	// after a status loss.
	//
	// +optional
	AppliedServer string `json:"appliedServer,omitempty"`

	// +optional
	AppliedDisplayName string `json:"appliedDisplayName,omitempty"`

	// +optional
	AppliedProject string `json:"appliedProject,omitempty"`

	// +optional
	AppliedCAHash string `json:"appliedCAHash,omitempty"`

	// AppliedCredentialHash digests the credential last published to ArgoCD.
	//
	// It is taken from the payload that was sent, never from any response. A
	// response describes the Secret as it stands when the server answers, which is
	// a different claim: anything that wrote in between would be digested and
	// remembered as this tool's own. Recording what was published keeps the
	// comparison anchored to something only this tool could have written.
	//
	// A later pass compares it against what an apply response reports, which is the
	// only thing that can be learned about a Secret this tool may not read.
	//
	// +optional
	AppliedCredentialHash string `json:"appliedCredentialHash,omitempty"`
}
