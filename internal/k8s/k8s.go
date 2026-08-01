// Package k8s wraps the Kubernetes client construction and the Secret I/O the
// daemon performs against the cluster it runs in.
package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

// ErrNotFound reports a missing Secret or key. Callers distinguish "not yet
// bootstrapped" from a genuine failure.
var ErrNotFound = errors.New("not found")

// errConcurrentCreate is wrapped into a synthetic conflict when a Secret is
// created between our Get and our Create, so the retry loop takes the update path.
var errConcurrentCreate = errors.New("secret appeared concurrently")

// Credentials is the credential the daemon holds for one downstream cluster,
// stored as a Secret in the daemon's own namespace.
type Credentials struct {
	// Token authenticates as the daemon's downstream ServiceAccount.
	Token string

	// CA is the PEM bundle for the downstream API server.
	CA []byte

	// ExpiresAt is when Token stops working. It is stored alongside the token so
	// the daemon knows its own deadline without decoding a JWT, and can report it
	// on the ClusterConnection: this is the daemon's own lockout clock, not
	// ArgoCD's.
	//
	// Zero means unknown, which is what a credential written by an older version
	// or by external automation looks like. That is treated as "renew now" rather
	// than "never expires".
	ExpiresAt time.Time
}

const (
	credentialsTokenKey     = "token"
	credentialsCAKey        = "ca.crt"
	credentialsExpiresAtKey = "expires-at"
)

// secretsResource names the Secret resource for synthesised conflict errors.
var secretsResource = schema.GroupResource{Group: "", Resource: "secrets"}

// NewClient builds a client for the cluster the daemon runs in, falling back to
// the ambient kubeconfig so the bootstrap subcommand works off-cluster.
func NewClient() (kubernetes.Interface, error) {
	cfg, err := LocalRESTConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}
	return client, nil
}

// NewDynamicClient builds a dynamic client for the cluster the daemon runs in.
// It is used for ClusterConnection objects, which need no typed client: the
// daemon converts them with runtime.DefaultUnstructuredConverter.
func NewDynamicClient() (dynamic.Interface, error) {
	cfg, err := LocalRESTConfig()
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating dynamic client: %w", err)
	}
	return client, nil
}

// DynamicClientForContext builds a dynamic client from a named kubeconfig
// context, for the bootstrap subcommand's --create.
func DynamicClientForContext(kubeconfigPath, contextName string) (dynamic.Interface, error) {
	if kubeconfigPath == "" && contextName == "" {
		return NewDynamicClient()
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules = &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules,
		&clientcmd.ConfigOverrides{CurrentContext: contextName},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig context %q: %w", contextName, err)
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating dynamic client for context %q: %w", contextName, err)
	}
	return client, nil
}

// LocalRESTConfig resolves in-cluster configuration, then the ambient kubeconfig.
func LocalRESTConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("in-cluster config failed: %w", err)
	}
	cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		nil,
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building kube client config: %w", err)
	}
	return cfg, nil
}

// ClientForContext builds a client from a named kubeconfig context. Used by the
// bootstrap subcommand, which runs against a cluster the daemon cannot yet reach.
func ClientForContext(kubeconfigPath, contextName string) (kubernetes.Interface, *rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules = &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules,
		&clientcmd.ConfigOverrides{CurrentContext: contextName},
	).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("loading kubeconfig context %q: %w", contextName, err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating kubernetes client for context %q: %w", contextName, err)
	}
	return client, cfg, nil
}

// ReadSecretKey returns one trimmed value from a Secret.
func ReadSecretKey(ctx context.Context, client kubernetes.Interface, namespace, name, key string) ([]byte, error) {
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("secret %s/%s: %w", namespace, name, ErrNotFound)
		}
		return nil, fmt.Errorf("getting secret %s/%s: %w", namespace, name, err)
	}
	value, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("secret %s/%s has no key %q: %w", namespace, name, key, ErrNotFound)
	}
	value = bytes.TrimSpace(value)
	if len(value) == 0 {
		return nil, fmt.Errorf("secret %s/%s key %q is blank: %w", namespace, name, key, ErrNotFound)
	}
	return value, nil
}

// GetSecret fetches a Secret, mapping absence to ErrNotFound.
func GetSecret(ctx context.Context, client kubernetes.Interface, namespace, name string) (*corev1.Secret, error) {
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("secret %s/%s: %w", namespace, name, ErrNotFound)
		}
		return nil, fmt.Errorf("getting secret %s/%s: %w", namespace, name, err)
	}
	return secret, nil
}

// ReadCredentials loads the downstream credential previously stored by the
// daemon, by the bootstrap subcommand, or by external automation.
func ReadCredentials(ctx context.Context, client kubernetes.Interface, namespace, name string) (*Credentials, error) {
	secret, err := GetSecret(ctx, client, namespace, name)
	if err != nil {
		return nil, err
	}
	token := bytes.TrimSpace(secret.Data[credentialsTokenKey])
	if len(token) == 0 {
		return nil, fmt.Errorf("credentials secret %s/%s key %q is blank: %w", namespace, name, credentialsTokenKey, ErrNotFound)
	}
	ca := bytes.TrimSpace(secret.Data[credentialsCAKey])
	if len(ca) == 0 {
		return nil, fmt.Errorf("credentials secret %s/%s key %q is blank: %w", namespace, name, credentialsCAKey, ErrNotFound)
	}

	creds := &Credentials{Token: string(token), CA: ca}

	// An unparsable or absent expiry is left zero rather than rejected: the
	// token may well still work, and a caller that renews on every pass will
	// replace it with one whose deadline is known.
	if raw := bytes.TrimSpace(secret.Data[credentialsExpiresAtKey]); len(raw) > 0 {
		if parsed, err := time.Parse(time.RFC3339, string(raw)); err == nil {
			creds.ExpiresAt = parsed
		}
	}
	return creds, nil
}

// WriteCredentials stores the daemon's downstream credential, creating the Secret
// if it does not exist.
func WriteCredentials(ctx context.Context, client kubernetes.Interface, namespace, name string, creds *Credentials, labels map[string]string) error {
	data := map[string][]byte{
		credentialsTokenKey: []byte(creds.Token),
		credentialsCAKey:    creds.CA,
	}
	if !creds.ExpiresAt.IsZero() {
		data[credentialsExpiresAtKey] = []byte(creds.ExpiresAt.UTC().Format(time.RFC3339))
	}
	return UpsertSecret(ctx, client, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	})
}

// UpsertSecret creates the Secret, or updates the labels, annotations and data
// of an existing one. Fields the daemon does not manage are left untouched so
// that other controllers can annotate the object freely.
func UpsertSecret(ctx context.Context, client kubernetes.Interface, desired *corev1.Secret) error {
	secrets := client.CoreV1().Secrets(desired.Namespace)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := secrets.Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("getting secret %s/%s: %w", desired.Namespace, desired.Name, err)
			}
			if _, err := secrets.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
				if apierrors.IsAlreadyExists(err) {
					// Let RetryOnConflict pick it up and fall through to update.
					return apierrors.NewConflict(secretsResource, desired.Name, errConcurrentCreate)
				}
				return fmt.Errorf("creating secret %s/%s: %w", desired.Namespace, desired.Name, err)
			}
			return nil
		}

		updated := existing.DeepCopy()
		if updated.Data == nil {
			updated.Data = make(map[string][]byte, len(desired.Data))
		}
		for k, v := range desired.Data {
			updated.Data[k] = v
		}
		updated.Labels = mergeMap(updated.Labels, desired.Labels)
		updated.Annotations = mergeMap(updated.Annotations, desired.Annotations)

		if _, err := secrets.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("updating secret %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	})
}

func mergeMap(into, from map[string]string) map[string]string {
	if len(from) == 0 {
		return into
	}
	if into == nil {
		into = make(map[string]string, len(from))
	}
	for k, v := range from {
		into[k] = v
	}
	return into
}
