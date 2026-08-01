// Package downstream performs the work that happens inside a managed
// cluster: provisioning the identities ArgoCD and k2a-token-sync authenticate as,
// minting bound tokens, reading the cluster CA, and probing the API server's
// serving certificate.
package downstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// rootCAConfigMap is projected into every namespace by the kube-controller-manager
	// and is the canonical in-cluster source of the API server's CA bundle.
	rootCAConfigMap = "kube-root-ca.crt"
	rootCAKey       = "ca.crt"

	// clusterAdminRole is the built-in role bound to the identity ArgoCD uses.
	clusterAdminRole = "cluster-admin"

	handshakeTimeout = 10 * time.Second
)

// ManagedByLabel marks every object this tool creates in a downstream cluster,
// so an operator can find and remove them.
var ManagedByLabel = map[string]string{"app.kubernetes.io/managed-by": "k2a-token-sync"}

// Token is a freshly minted ServiceAccount token.
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// ServingCert describes the certificate the API server presents at its direct
// endpoint — the one ArgoCD will have to trust.
type ServingCert struct {
	NotAfter time.Time
	Subject  string
	Serial   string

	// TrustedByCA reports whether the presented chain verifies against the
	// cluster CA we are about to publish as ArgoCD's caData.
	TrustedByCA bool

	// HostnameError is set when the certificate carries no SAN matching the
	// configured endpoint. This is the single most common reason direct access
	// fails: a serving certificate typically covers the node addresses and
	// in-cluster names, but not a VIP, load balancer or FQDN unless it was
	// explicitly included when the certificate was issued.
	HostnameError error
}

// DaysRemaining is the whole days left before the serving certificate expires.
func (s ServingCert) DaysRemaining() int {
	return int(time.Until(s.NotAfter).Hours() / 24)
}

// EnsureServiceAccount creates a ServiceAccount if it is missing, and returns
// whether it had to be created.
func EnsureServiceAccount(ctx context.Context, client kubernetes.Interface, namespace, name string) (bool, error) {
	_, err := client.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("getting serviceaccount %s/%s: %w", namespace, name, err)
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: ManagedByLabel},
	}
	if _, err := client.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, fmt.Errorf("creating serviceaccount %s/%s: %w", namespace, name, err)
	}
	return true, nil
}

// EnsureClusterRoleBinding binds a ServiceAccount to a ClusterRole, creating the
// binding if absent. An existing binding that points elsewhere is reported
// rather than silently rewritten — that would be an unexpected privilege change.
func EnsureClusterRoleBinding(ctx context.Context, client kubernetes.Interface, bindingName, clusterRole, saNamespace, saName string) (bool, error) {
	subject := rbacv1.Subject{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      saName,
		Namespace: saNamespace,
	}

	existing, err := client.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})
	if err == nil {
		if existing.RoleRef.Name != clusterRole {
			return false, fmt.Errorf("clusterrolebinding %s already binds %q, not %q; resolve manually",
				bindingName, existing.RoleRef.Name, clusterRole)
		}
		for _, s := range existing.Subjects {
			if s.Kind == subject.Kind && s.Name == subject.Name && s.Namespace == subject.Namespace {
				return false, nil
			}
		}
		return false, fmt.Errorf("clusterrolebinding %s does not include serviceaccount %s/%s; resolve manually",
			bindingName, saNamespace, saName)
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("getting clusterrolebinding %s: %w", bindingName, err)
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName, Labels: ManagedByLabel},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRole,
		},
		Subjects: []rbacv1.Subject{subject},
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, fmt.Errorf("creating clusterrolebinding %s: %w", bindingName, err)
	}
	return true, nil
}

// EnsureArgoCDIdentity provisions the ServiceAccount ArgoCD authenticates as and
// grants it cluster-admin, mirroring what `argocd cluster add` installs.
func EnsureArgoCDIdentity(ctx context.Context, client kubernetes.Interface, namespace, name string) (bool, error) {
	created, err := EnsureServiceAccount(ctx, client, namespace, name)
	if err != nil {
		return false, err
	}
	bound, err := EnsureClusterRoleBinding(ctx, client, name+"-role-binding", clusterAdminRole, namespace, name)
	if err != nil {
		return false, err
	}
	return created || bound, nil
}

// MintToken requests a bound token for a ServiceAccount.
//
// The API server may cap the lifetime below what we ask for
// (--service-account-max-token-expiration), so the returned expiry is the
// authoritative one and callers must schedule against it rather than the request.
func MintToken(ctx context.Context, client kubernetes.Interface, namespace, name string, ttl time.Duration) (*Token, error) {
	// A zero TTL means a caller left the field unset, since no configuration path
	// can produce one. The API server rejects it as "may not specify a duration
	// less than 10 minutes", which sends whoever reads that towards the cluster
	// rather than towards the bug.
	if ttl <= 0 {
		return nil, fmt.Errorf("refusing to mint a token for %s/%s with a lifetime of %s: "+
			"the requested lifetime was never set", namespace, name, ttl)
	}

	request := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			// ExpirationSeconds is a *int64 so the field can be left unset;
			// Go 1.26's new(expr) covers that without a helper.
			ExpirationSeconds: new(int64(ttl.Seconds())),
		},
	}

	result, err := client.CoreV1().ServiceAccounts(namespace).
		CreateToken(ctx, name, request, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("minting token for serviceaccount %s/%s: %w", namespace, name, err)
	}
	if result.Status.Token == "" {
		return nil, fmt.Errorf("token request for %s/%s returned an empty token", namespace, name)
	}

	return &Token{
		Value:     result.Status.Token,
		ExpiresAt: result.Status.ExpirationTimestamp.Time,
	}, nil
}

// selfRules are the only permissions k2a-token-sync needs in a downstream cluster
// after bootstrap: maintain ArgoCD's identity, mint tokens for it, and read the
// cluster CA.
//
// Note that the right to mint a token for a cluster-admin ServiceAccount is
// itself equivalent to cluster-admin. This grant is narrow for auditability and
// to avoid handing out direct read access to every Secret in the cluster — not
// because it is unprivileged.
//
// These are ClusterRole rules, so they carry no namespace: the binding needs to
// cover kube-root-ca.crt in whichever namespace the ServiceAccounts live in.
func selfRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"serviceaccounts"},
			Verbs:     []string{"get", "create"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"serviceaccounts/token"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"clusterrolebindings"},
			Verbs:     []string{"get", "create"},
		},
		{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{rootCAConfigMap},
			Verbs:         []string{"get"},
		},
	}
}

// EnsureSelfIdentity provisions the identity k2a-token-sync itself uses in a
// standalone cluster, together with its ClusterRole and binding.
func EnsureSelfIdentity(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	if _, err := EnsureServiceAccount(ctx, client, namespace, name); err != nil {
		return err
	}

	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: ManagedByLabel},
		Rules:      selfRules(),
	}
	if _, err := client.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating clusterrole %s: %w", name, err)
		}
		if _, err := client.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("updating clusterrole %s: %w", name, err)
		}
	}

	if _, err := EnsureClusterRoleBinding(ctx, client, name, name, namespace, name); err != nil {
		return err
	}
	return nil
}

// ClusterCA reads the API server CA bundle from the kube-root-ca.crt ConfigMap.
func ClusterCA(ctx context.Context, client kubernetes.Interface, namespace string) ([]byte, error) {
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, rootCAConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("reading %s/%s: %w", namespace, rootCAConfigMap, err)
	}
	ca, ok := cm.Data[rootCAKey]
	if !ok || ca == "" {
		return nil, fmt.Errorf("configmap %s/%s has no %q entry", namespace, rootCAConfigMap, rootCAKey)
	}
	return []byte(ca), nil
}

// ProbeServingCert opens a TLS connection to the direct endpoint and reports on
// the certificate the API server presents there.
//
// This is deliberately independent of the credential path: it is the only way to
// observe what ArgoCD will actually encounter when it connects directly, and the
// only way to see the serving certificate's expiry.
func ProbeServingCert(ctx context.Context, endpoint string, ca []byte) (*ServingCert, error) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint %q: %w", endpoint, err)
	}

	// The cluster CA is the only trust anchor that means anything here: ArgoCD will
	// be handed exactly this bundle as caData, so the host's own trust store is
	// beside the point. When no bundle is supplied the pool stays empty, and the
	// handshake fails as untrusted — which is precisely the finding to report.
	roots := x509.NewCertPool()
	if len(ca) > 0 && !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("cluster CA bundle contains no usable certificates")
	}

	dialCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{},
		Config: &tls.Config{
			RootCAs:    roots,
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		},
	}
	conn, err := dialer.DialContext(dialCtx, "tcp", endpoint)

	// A refused certificate is a diagnosis, not a dead end. Go returns the chain it
	// rejected, so a certificate that is untrusted, misnamed or expired still
	// yields an expiry reading and a precise explanation — without this probe ever
	// having to accept one.
	var refused *tls.CertificateVerificationError
	var chain []*x509.Certificate
	switch {
	case err == nil:
		defer func() { _ = conn.Close() }()
		tlsConn, ok := conn.(*tls.Conn)
		if !ok {
			return nil, fmt.Errorf("unexpected connection type %T for %s", conn, endpoint)
		}
		chain = tlsConn.ConnectionState().PeerCertificates
	case errors.As(err, &refused):
		chain = refused.UnverifiedCertificates
	default:
		return nil, fmt.Errorf("TLS handshake with %s failed: %w", endpoint, err)
	}

	if len(chain) == 0 {
		return nil, fmt.Errorf("%s presented no certificates", endpoint)
	}
	leaf := chain[0]

	out := &ServingCert{
		NotAfter: leaf.NotAfter,
		Subject:  leaf.Subject.String(),
		Serial:   leaf.SerialNumber.String(),
	}
	if err == nil {
		out.TrustedByCA = true
		return out, nil
	}

	// Go's verifier stops at the first problem, and checks the hostname before it
	// builds a chain. Both answers matter to whoever has to fix the cluster, so
	// both are established here rather than inferred from one error.
	if hostErr := leaf.VerifyHostname(host); hostErr != nil {
		out.HostnameError = fmt.Errorf(
			"the API server certificate at %s carries no SAN for %q (SANs: %v); "+
				"add this name to the API server's serving certificate, "+
				"otherwise ArgoCD cannot verify this endpoint: %w",
			endpoint, host, sans(leaf), hostErr)
	}

	intermediates := x509.NewCertPool()
	for _, cert := range chain[1:] {
		intermediates.AddCert(cert)
	}
	_, verifyErr := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	out.TrustedByCA = verifyErr == nil

	return out, nil
}

func sans(cert *x509.Certificate) []string {
	out := make([]string, 0, len(cert.DNSNames)+len(cert.IPAddresses))
	out = append(out, cert.DNSNames...)
	for _, ip := range cert.IPAddresses {
		out = append(out, ip.String())
	}
	return out
}
