package main

import (
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/argocd"
	"github.com/krisiasty/k2a-token-sync/internal/config"
)

const argocdNamespace = "argocd"

// clusterSecret is a registration as it would stand in ArgoCD's namespace. The
// managed-by label is the only thing distinguishing one k2a-token-sync created from
// one 'argocd cluster add' left behind — the names are identical by construction.
func clusterSecret(name string, labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: argocdNamespace, Labels: labels},
		Data:       map[string][]byte{"config": []byte(`{"bearerToken":"a-permanent-token"}`)},
	}
}

func bootstrapCluster(t *testing.T) config.Cluster {
	t.Helper()
	cluster, err := config.BootstrapCluster(config.BootstrapClusterInput{
		Name: "standalone-1", Endpoint: "10.1.0.10",
	})
	if err != nil {
		t.Fatalf("BootstrapCluster returned unexpected error: %v", err)
	}
	return cluster
}

// The case the whole guard exists for. `argocd cluster add` writes
// cluster-<name>, which is also this tool's default, so a mistyped --cluster
// silently repoints a registration other Applications depend on — and the
// permanent token that was there is gone and was never readable by this tool.
func TestACollidingNameIsRefusedWithoutAdopt(t *testing.T) {
	t.Parallel()

	cluster := bootstrapCluster(t)
	client := fake.NewClientset(clusterSecret(cluster.SecretName, map[string]string{
		argocd.SecretTypeLabel: "cluster",
	}))

	target := inspectRegistrationTarget(t.Context(), client, argocdNamespace, cluster.SecretName)
	if target != targetForeign {
		t.Fatalf("target = %v, want targetForeign for a Secret without the managed-by label", target)
	}

	refusal := target.refusal(false, argocdNamespace, cluster.SecretName)
	if refusal == nil {
		t.Fatal("bootstrap did not refuse a Secret it did not create")
	}
	// The reader has to be able to tell which of two very different situations they
	// are in, and only they can, so the message has to name both and the way out.
	for _, want := range []string{
		argocdNamespace + "/" + cluster.SecretName,
		"--adopt",
		"collided",
		"Nothing has been changed",
	} {
		if !strings.Contains(refusal.Error(), want) {
			t.Errorf("the refusal does not mention %q:\n%v", want, refusal)
		}
	}

	// Nothing may be adopted on the strength of a refused run.
	if target.recordsAdoption(false) {
		t.Error("a refused run recorded an adoption")
	}
}

// Migrating from 'argocd cluster add' is the supported path and has to keep
// working — the guard is about making it deliberate, not about closing it.
func TestAdoptProceedsAndIsRecordedOnTheObject(t *testing.T) {
	t.Parallel()

	cluster := bootstrapCluster(t)
	client := fake.NewClientset(clusterSecret(cluster.SecretName, map[string]string{
		argocd.SecretTypeLabel: "cluster",
	}))

	target := inspectRegistrationTarget(t.Context(), client, argocdNamespace, cluster.SecretName)
	if refusal := target.refusal(true, argocdNamespace, cluster.SecretName); refusal != nil {
		t.Fatalf("--adopt was refused: %v", refusal)
	}
	if !target.recordsAdoption(true) {
		t.Fatal("an adopted registration was not recorded")
	}

	// The pass that notices the co-owner is a different process, days later.
	// Bootstrap is the only place the intent exists and the object the only place it
	// survives, so it has to reach the manifest — including the one --print emits.
	raw, err := renderConnection(cluster, "k2a-token-sync", true)
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	if !strings.Contains(string(raw), v1alpha1.AnnotationAdopted+": \"true\"") {
		t.Errorf("the manifest does not carry the adoption annotation:\n%s", raw)
	}
}

// Re-running bootstrap for a cluster already in service is routine and must not
// look like a takeover.
func TestASecretThisToolCreatedNeedsNoAdoption(t *testing.T) {
	t.Parallel()

	cluster := bootstrapCluster(t)
	client := fake.NewClientset(clusterSecret(cluster.SecretName, map[string]string{
		argocd.SecretTypeLabel: "cluster",
		argocd.ManagedByLabel:  argocd.ManagedByValue,
	}))

	target := inspectRegistrationTarget(t.Context(), client, argocdNamespace, cluster.SecretName)
	if target != targetOurs {
		t.Fatalf("target = %v, want targetOurs", target)
	}
	if refusal := target.refusal(false, argocdNamespace, cluster.SecretName); refusal != nil {
		t.Errorf("re-running bootstrap on a managed cluster was refused: %v", refusal)
	}
	// Marking this as adopted would leave a permanent claim that something was
	// inherited when nothing was, which is the distinction the annotation exists for.
	if target.recordsAdoption(true) {
		t.Error("--adopt recorded an adoption on a Secret this tool created")
	}
}

// The ordinary case, and the overwhelmingly common one.
func TestAnOrdinaryCreateIsNotAnAdoption(t *testing.T) {
	t.Parallel()

	cluster := bootstrapCluster(t)

	target := inspectRegistrationTarget(t.Context(), fake.NewClientset(), argocdNamespace, cluster.SecretName)
	if target != targetAbsent {
		t.Fatalf("target = %v, want targetAbsent when no Secret exists", target)
	}
	if refusal := target.refusal(false, argocdNamespace, cluster.SecretName); refusal != nil {
		t.Errorf("an ordinary create was refused: %v", refusal)
	}
	if target.recordsAdoption(true) {
		t.Error("--adopt recorded an adoption where there was nothing to adopt")
	}

	raw, err := renderConnection(cluster, "k2a-token-sync", false)
	if err != nil {
		t.Fatalf("renderConnection returned unexpected error: %v", err)
	}
	if strings.Contains(string(raw), v1alpha1.AnnotationAdopted) {
		t.Errorf("an ordinary create was annotated as adopted:\n%s", raw)
	}
}

// Bootstrap runs with whatever kubeconfig the operator has, which need not cover
// ArgoCD's namespace. Refusing on that basis would break bootstrap for everyone
// with narrower rights, to guard a case where a person is present anyway.
func TestBeingUnableToLookDoesNotBlockBootstrap(t *testing.T) {
	t.Parallel()

	cluster := bootstrapCluster(t)
	client := fake.NewClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "secrets"}, cluster.SecretName, errors.New("no access to ArgoCD's namespace"))
	})

	// Not being allowed to look is one of the states, not an error: every outcome
	// here has to be something bootstrap can carry on from.
	target := inspectRegistrationTarget(t.Context(), client, argocdNamespace, cluster.SecretName)
	if target != targetUnreadable {
		t.Fatalf("target = %v, want targetUnreadable", target)
	}
	if refusal := target.refusal(false, argocdNamespace, cluster.SecretName); refusal != nil {
		t.Errorf("bootstrap was blocked by a check it could not run: %v", refusal)
	}
	// The operator asked, and nothing here can contradict them.
	if !target.recordsAdoption(true) {
		t.Error("--adopt was ignored when the target could not be read")
	}
}
