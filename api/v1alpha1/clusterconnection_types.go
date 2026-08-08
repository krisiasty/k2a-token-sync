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

	// ConditionConflict reports that another ClusterConnection claims something
	// only one of them can own: the same secretName, or the same downstream
	// cluster. Admission cannot see either, since both span objects.
	ConditionConflict = "Conflict"

	// ConditionSecretExclusivelyOwned reports whether k2a-token-sync is the only
	// thing managing the ArgoCD cluster Secret this connection publishes.
	//
	// It cannot be checked before the fact. k2a-token-sync holds no read permission
	// on those Secrets, so the first it can learn of a co-owner is the managedFields
	// an apply hands back — by which point the write has happened. What this
	// condition is for is that the takeover does not then go unrecorded: a
	// registration made by 'argocd cluster add' carries the same cluster- prefix and
	// the same default name, so a typo in a cluster name repoints an existing
	// registration at a different cluster and looks exactly like the documented
	// migration.
	ConditionSecretExclusivelyOwned = "SecretExclusivelyOwned"

	// ConditionSchemaCurrent reports whether the CRD serving this object matches
	// the k2a-token-sync build reconciling it.
	//
	// It is process-wide rather than per-connection — one schema serves them all
	// — but it is reported on every connection because that is where it is
	// visible. Somebody who set a field and saw nothing happen looks at the
	// object, not at the process, and a pruned field leaves no other trace: from
	// inside, a field the API server discarded is indistinguishable from one
	// nobody set.
	ConditionSchemaCurrent = "SchemaCurrent"
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

	// ReasonEndpointConflict means two connections resolve to the same downstream
	// cluster. Distinct from SecretNameConflict because the remedies differ: a
	// contested Secret is two objects disagreeing about where to publish, and can
	// be settled by renaming one, whereas two objects naming one cluster is a
	// duplicate that has to be deleted.
	ReasonEndpointConflict = "EndpointConflict"

	// ReasonPermissionDenied means the API server answered and refused. It is
	// distinct from EndpointUnreachable for the reason CredentialRejected is:
	// something that answered is not something that could not be reached, and the
	// two send whoever reads it to opposite ends of the problem.
	ReasonPermissionDenied = "PermissionDenied"

	// ReasonRoleRefImmutable means spec.clusterRole was changed on a connection
	// whose binding already exists. Kubernetes will not repoint a roleRef, and
	// k2a-token-sync holds bind on the previous role only, so this is one of the
	// few states it cannot reconcile its way out of — only bootstrap can, and the
	// message says so. Distinct from PermissionDenied because nothing is broken
	// and nothing was refused: the cluster is working, and the spec is simply
	// ahead of what has been applied to it.
	ReasonRoleRefImmutable = "RoleRefImmutable"

	// ReasonSchemaOutdated means the CRD predates this build, so the API server
	// is discarding fields set on connections. The message names them: which
	// settings are being ignored is the whole of what an operator needs.
	ReasonSchemaOutdated = "SchemaOutdated"

	// ReasonSchemaUnverified means the CRD could not be read, so whether it
	// matches is unknown. Distinct from Outdated: the usual cause is an upgrade
	// that reached the image before the chart, where nothing is wrong yet.
	ReasonSchemaUnverified = "SchemaUnverified"

	// ReasonReconcileFailed is the reason for a failure this tool cannot place.
	//
	// It deliberately names no cause. Every unclassified failure used to report
	// EndpointUnreachable, so a pass that reached the endpoint twice and then hit
	// an RBAC refusal told its reader to go and check the network. A reason that
	// says only "the pass failed" is worth more than a confident wrong one: the
	// message carries the detail, and nobody is sent the wrong way.
	ReasonReconcileFailed = "ReconcileFailed"

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

	// ReasonForeignFieldManager means something other than k2a-token-sync holds
	// fields on the cluster Secret, and nothing said that was intended. The likely
	// readings are a registration created by 'argocd cluster add' and taken over, or
	// a cluster name that collided with one.
	ReasonForeignFieldManager = "ForeignFieldManager"

	// ReasonAdoptedRegistration means the same co-ownership, on a connection whose
	// adoption was requested deliberately. The distinction is the whole point of
	// recording it: a migration reported identically to an accident, permanently, is
	// a warning people learn to scroll past.
	ReasonAdoptedRegistration = "AdoptedRegistration"
)

// AnnotationAdopted marks a ClusterConnection whose ArgoCD cluster Secret was
// deliberately taken over from whatever created it, rather than created by
// k2a-token-sync.
//
// Written by 'bootstrap --adopt', which is the only place the decision can be
// made: it runs with administrative credentials, so it can read the Secret before
// anything is written, and there is a person there to mean it.
//
// An annotation rather than a spec field because it records what was done once,
// not what is desired continuously — and because adding it needs no schema change,
// so an older CRD does not silently prune it.
const AnnotationAdopted = "k2a-token-sync.io/adopted"

// Reasons naming an action rather than a state. They accompany the Events
// k2a-token-sync records on a ClusterConnection, where what matters is that
// something happened; the conditions above say how the object stands now.
//
// They live here, beside those, rather than in the package that emits them.
// Several of the Events reuse the reasons above verbatim — a renewal failure
// reports the same RenewalMintFailed that goes on the condition — and two sets of
// names for one situation is precisely what would have 'kubectl describe' and
// 'kubectl get ccon' disagreeing about the same event.
const (
	// ReasonCredentialReissued accompanies a new credential published for ArgoCD.
	// The reason it was due is the one already recorded in lastAction.
	ReasonCredentialReissued = "CredentialReissued" //nolint:gosec // an event reason, not a credential

	// ReasonIdentityRestored means ArgoCD's downstream ServiceAccount or its
	// binding was missing and has been recreated. It is the rarest thing here and
	// the hardest to reconstruct afterwards, since the published Secret goes on
	// looking perfectly healthy either way.
	ReasonIdentityRestored = "IdentityRestored"

	// ReasonRenewalRecovered means k2a-token-sync can renew its own credential for
	// this cluster again. Paired with a preceding RenewalMintFailed,
	// RenewalUnverified or RenewalNotStored, which is the only case it is emitted
	// in.
	ReasonRenewalRecovered = "RenewalRecovered"

	// ReasonReconciliationResumed means the spec problem or Secret conflict that
	// stopped this object being reconciled is resolved.
	ReasonReconciliationResumed = "ReconciliationResumed"
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

	// ClusterRole is the downstream ClusterRole ArgoCD's identity is bound to.
	//
	// The default is what `argocd cluster add` installs and what every
	// registration held before this field existed. Naming a narrower role is the
	// supported way to give ArgoCD less than cluster-admin — the role itself is
	// yours to create and maintain, since only you know what your Applications
	// need.
	//
	// Changing this on a live connection cannot be applied by k2a-token-sync.
	// A ClusterRoleBinding's roleRef is immutable, so the binding has to be
	// replaced, and k2a-token-sync holds bind on the previous role only. Re-run
	// bootstrap with --replace-binding, which has the credentials to do both.
	//
	// +optional
	// +kubebuilder:default="cluster-admin"
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.:]*[a-z0-9])?$`
	ClusterRole string `json:"clusterRole,omitempty"`

	// Namespaces optionally restricts ArgoCD to these namespaces on this
	// cluster. Empty — the default — means every namespace, which is what a
	// registration meant before this field existed.
	//
	// This is ArgoCD's own scoping, written into the cluster Secret it reads. It
	// is not a substitute for ClusterRole: it governs what ArgoCD will attempt,
	// while the role governs what the API server will permit. Setting one
	// without the other is legitimate, and they answer different questions.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespaces []string `json:"namespaces,omitempty"`

	// ClusterResources permits ArgoCD to manage cluster-scoped resources on a
	// connection restricted with Namespaces.
	//
	// A pointer rather than a plain bool so that "unset" and "false" stay
	// distinguishable: the key is written into ArgoCD's Secret only when it was
	// asked for, and a registration that never mentioned it must keep producing
	// exactly the Secret it produced before.
	//
	// It means nothing without Namespaces — an unrestricted registration already
	// covers cluster-scoped resources — and is rejected in that combination
	// rather than written and silently ignored.
	//
	// +optional
	ClusterResources *bool `json:"clusterResources,omitempty"`

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

	// Labels are merged into the generated ArgoCD Secret. Values supplied for the
	// controller-owned argocd.argoproj.io/secret-type and
	// app.kubernetes.io/managed-by keys are ignored; k2a-token-sync always writes
	// its required values while continuing to reconcile the connection.
	//
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are merged into the generated ArgoCD Secret. Values supplied for
	// the controller-owned k2a-token-sync.io/cluster,
	// k2a-token-sync.io/token-expires-at and
	// k2a-token-sync.io/serving-cert-expires-at keys are ignored; k2a-token-sync
	// writes only the values it observed while continuing to reconcile the
	// connection.
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
