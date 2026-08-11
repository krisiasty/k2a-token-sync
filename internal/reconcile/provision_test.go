package reconcile

import (
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/internal/config"
)

// The second half of delegated bootstrap must not fall back to the ordinary
// ensure-and-repair path. Its downstream credential may mint the initial token,
// but it deliberately need not hold permission to create or update RBAC objects.
func TestProvisionFromExistingOnlyMintsAndReads(t *testing.T) {
	t.Parallel()

	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name:     "delegated-1",
		Endpoint: "delegated-1.example.com",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}

	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-root-ca.crt",
			Namespace: cluster.ServiceAccount.Namespace,
		},
		Data: map[string]string{"ca.crt": "delegated-ca"},
	})
	client.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok || create.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{
			Token:               "initial-self-token",
			ExpirationTimestamp: metav1.NewTime(expires),
		}}, nil
	})

	creds, err := ProvisionFromExisting(t.Context(), client, cluster)
	if err != nil {
		t.Fatalf("ProvisionFromExisting returned unexpected error: %v", err)
	}
	if creds.Token != "initial-self-token" || string(creds.CA) != "delegated-ca" || !creds.ExpiresAt.Equal(expires) {
		t.Errorf("credentials = %+v, want the token, CA and server expiry", creds)
	}

	for _, action := range client.Actions() {
		allowed := action.Matches("get", "configmaps") ||
			(action.Matches("create", "serviceaccounts") && action.GetSubresource() == "token")
		if !allowed {
			t.Errorf("delegated completion attempted downstream mutation %s %s/%s",
				action.GetVerb(), action.GetResource().Resource, action.GetSubresource())
		}
	}
}
