package hardening

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/internal/config"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

func testNames() Names {
	return Names{
		Namespace:           "k2a-token-sync",
		ArgoCDNamespace:     "argocd",
		ServiceAccount:      "k2a-token-sync",
		BaselineRole:        "k2a-token-sync",
		BaselineRoleBinding: "k2a-token-sync",
		RestrictedRole:      "k2a-token-sync-restricted",
		RestrictedBinding:   "k2a-token-sync-restricted",
	}
}

func baselineObjects(names Names) []runtime.Object {
	labels := map[string]string{managedByLabel: "Helm", appNameLabel: names.ServiceAccount}
	return []runtime.Object{
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: names.ServiceAccount, Namespace: names.Namespace, Labels: labels},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: names.BaselineRole, Namespace: names.ArgoCDNamespace, Labels: labels},
			Rules:      baselineRules(true),
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: names.BaselineRoleBinding, Namespace: names.ArgoCDNamespace, Labels: labels},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: names.BaselineRole},
			Subjects: []rbacv1.Subject{{
				Kind: rbacv1.ServiceAccountKind, Name: names.ServiceAccount, Namespace: names.Namespace,
			}},
		},
	}
}

func TestBuildPlanSortsResolvedDefaultAndCustomNames(t *testing.T) {
	entries := []inventory.Entry{
		{Cluster: config.Cluster{Name: "zeta", SecretName: "cluster-custom"}},
		{Cluster: config.Cluster{Name: "alpha", SecretName: "cluster-alpha"}},
	}
	plan, err := BuildPlan(entries)
	if err != nil {
		t.Fatal(err)
	}
	wantConnections := []Connection{
		{Name: "alpha", SecretName: "cluster-alpha"},
		{Name: "zeta", SecretName: "cluster-custom"},
	}
	if !reflect.DeepEqual(plan.Connections, wantConnections) {
		t.Fatalf("connections = %#v, want %#v", plan.Connections, wantConnections)
	}
	if want := []string{"cluster-alpha", "cluster-custom"}; !reflect.DeepEqual(plan.SecretNames, want) {
		t.Fatalf("secret names = %#v, want %#v", plan.SecretNames, want)
	}
}

func TestBuildPlanRefusesInvalidAndConflictingEntries(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		_, err := BuildPlan([]inventory.Entry{{
			Cluster: config.Cluster{Name: "broken"}, InvalidReason: "endpoint must not be empty",
		}})
		if err == nil || !strings.Contains(err.Error(), "broken: endpoint must not be empty") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("conflicting", func(t *testing.T) {
		_, err := BuildPlan([]inventory.Entry{
			{Cluster: config.Cluster{Name: "one", SecretName: "cluster-shared"}},
			{Cluster: config.Cluster{Name: "two", SecretName: "cluster-shared"}},
		})
		if err == nil || !strings.Contains(err.Error(), "both resolve to Secret") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestEmptyInventoryOmitsPatchRule(t *testing.T) {
	role := RestrictedRole(testNames(), nil)
	if len(role.Rules) != 0 {
		t.Fatalf("empty inventory rendered rules: %#v", role.Rules)
	}
}

func TestInspectRefusesUnexpectedIdentityAndSameNamespace(t *testing.T) {
	names := testNames()
	t.Run("same namespace", func(t *testing.T) {
		names := names
		names.ArgoCDNamespace = names.Namespace
		if err := Inspect(t.Context(), fake.NewClientset(), names); err == nil || !strings.Contains(err.Error(), "no isolation") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("foreign restricted role", func(t *testing.T) {
		objects := baselineObjects(names)
		objects = append(objects, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: names.RestrictedRole, Namespace: names.ArgoCDNamespace},
		})
		err := Inspect(t.Context(), fake.NewClientset(objects...), names)
		if err == nil || !strings.Contains(err.Error(), "without k2a-token-sync ownership") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("unexpected baseline subject", func(t *testing.T) {
		objects := baselineObjects(names)
		binding, ok := objects[2].(*rbacv1.RoleBinding)
		if !ok {
			t.Fatalf("fixture object has type %T", objects[2])
		}
		binding.Subjects[0].Name = "someone-else"
		err := Inspect(t.Context(), fake.NewClientset(objects...), names)
		if err == nil || !strings.Contains(err.Error(), "roleRef, or subject") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestApplyInstallsRestrictedObjectsBeforeRemovingBroadPatch(t *testing.T) {
	names := testNames()
	client := fake.NewClientset(baselineObjects(names)...)
	if err := Apply(t.Context(), client, names, []string{"cluster-a", "cluster-b"}); err != nil {
		t.Fatal(err)
	}

	var writes []string
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" {
			writes = append(writes, action.GetVerb()+" "+action.GetResource().Resource)
		}
	}
	wantWrites := []string{"create roles", "create rolebindings", "update roles"}
	if !reflect.DeepEqual(writes, wantWrites) {
		t.Fatalf("writes = %#v, want safe activation order %#v", writes, wantWrites)
	}

	restricted, err := client.RbacV1().Roles(names.ArgoCDNamespace).
		Get(t.Context(), names.RestrictedRole, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := restricted.Rules[0].ResourceNames; !reflect.DeepEqual(got, []string{"cluster-a", "cluster-b"}) {
		t.Fatalf("resourceNames = %#v", got)
	}
	baseline, err := client.RbacV1().Roles(names.ArgoCDNamespace).
		Get(t.Context(), names.BaselineRole, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(baseline.Rules, baselineRules(false)) {
		t.Fatalf("baseline rules = %#v", baseline.Rules)
	}
}

func TestApplyUpdatesAllowlistForInventoryChanges(t *testing.T) {
	names := testNames()
	client := fake.NewClientset(baselineObjects(names)...)
	for _, allowlist := range [][]string{
		{"cluster-a", "cluster-b"},
		{"cluster-b", "cluster-renamed"},
		{},
	} {
		if err := Apply(t.Context(), client, names, allowlist); err != nil {
			t.Fatal(err)
		}
		role, err := client.RbacV1().Roles(names.ArgoCDNamespace).
			Get(t.Context(), names.RestrictedRole, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(allowlist) == 0 && len(role.Rules) != 0 {
			t.Fatalf("removed inventory left patch rules: %#v", role.Rules)
		}
	}
}

func TestVerifyAuthorizationChecksAllowedExcludedAndCreate(t *testing.T) {
	names := testNames()
	client := fake.NewClientset()
	client.PrependReactor("create", "subjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(ktesting.CreateAction)
		if !ok {
			return true, nil, fmt.Errorf("action has type %T", action)
		}
		reviewObject, ok := create.GetObject().(*authorizationv1.SubjectAccessReview)
		if !ok {
			return true, nil, fmt.Errorf("review has type %T", create.GetObject())
		}
		review := reviewObject.DeepCopy()
		attrs := review.Spec.ResourceAttributes
		switch {
		case review.Spec.User != "system:serviceaccount:k2a-token-sync:k2a-token-sync":
			return true, nil, fmt.Errorf("wrong user %q", review.Spec.User)
		case attrs.Verb == "create" && attrs.Name == "":
			review.Status.Allowed = true
		case attrs.Verb == "patch" && attrs.Name == "cluster-a":
			review.Status.Allowed = true
		case attrs.Verb == "patch":
			review.Status.Allowed = false
		default:
			return true, nil, fmt.Errorf("unexpected review: %#v", attrs)
		}
		return true, review, nil
	})

	result, err := VerifyAuthorization(t.Context(), client, names, []string{"cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped != "" || result.Checks != 3 {
		t.Fatalf("result = %#v", result)
	}
}

func TestVerifyAuthorizationSkipsWhenCallerCannotReview(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "authorization.k8s.io", Resource: "subjectaccessreviews"},
			"", errors.New("denied"))
	})
	result, err := VerifyAuthorization(context.Background(), client, testNames(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped == "" {
		t.Fatal("verification was not reported as skipped")
	}
}
