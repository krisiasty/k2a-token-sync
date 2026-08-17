package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/krisiasty/k2a-token-sync/internal/hardening"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

func commandRestrictionNames() hardening.Names {
	return hardening.Names{
		Namespace:           "k2a-token-sync",
		ArgoCDNamespace:     "argocd",
		ServiceAccount:      "k2a-token-sync",
		BaselineRole:        "k2a-token-sync",
		BaselineRoleBinding: "k2a-token-sync",
		RestrictedRole:      "k2a-token-sync-restricted",
		RestrictedBinding:   "k2a-token-sync-restricted",
	}
}

func commandBaselineObjects(names hardening.Names) []runtime.Object {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "Helm",
		"app.kubernetes.io/name":       names.ServiceAccount,
	}
	return []runtime.Object{
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: names.ServiceAccount, Namespace: names.Namespace, Labels: labels},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: names.BaselineRole, Namespace: names.ArgoCDNamespace, Labels: labels},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "patch"},
			}},
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

func restrictionConnectionObject(namespace, name, endpoint, secretName string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "k2a-token-sync.io/v1alpha1",
		"kind":       "ClusterConnection",
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
		},
		"spec": map[string]any{
			"endpoint": endpoint,
		},
	}}
	if secretName != "" {
		_ = unstructured.SetNestedField(obj.Object, secretName, "spec", "secretName")
	}
	return obj
}

func TestRenderRestrictedManifestIsDeterministicAndEmptyIsDenyAll(t *testing.T) {
	names := commandRestrictionNames()
	first, err := renderRestrictedManifest(names, []string{"cluster-a", "cluster-b"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := renderRestrictedManifest(names, []string{"cluster-a", "cluster-b"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("identical input produced different YAML")
	}
	for _, want := range []string{
		"kind: Role\n", "kind: RoleBinding\n", "resourceNames:\n  - cluster-a\n  - cluster-b\n", "verbs:\n  - patch\n",
		"argocd.argoproj.io/sync-wave: \"-2\"", "argocd.argoproj.io/sync-wave: \"-1\"",
	} {
		if !strings.Contains(string(first), want) {
			t.Fatalf("manifest does not contain %q:\n%s", want, first)
		}
	}

	empty, err := renderRestrictedManifest(names, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(empty), "resourceNames:") || strings.Contains(string(empty), "- patch") {
		t.Fatalf("empty inventory rendered patch permission:\n%s", empty)
	}
	if !strings.Contains(string(empty), "rules: []") {
		t.Fatalf("empty inventory did not render an explicit empty rule list:\n%s", empty)
	}
}

func TestConfirmationRequiresExactYes(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{input: "yes\n", want: true},
		{input: "no\n", want: false},
		{input: "y\n", want: false},
		{input: "YES\n", want: false},
	} {
		var prompt bytes.Buffer
		got, err := confirmRestriction(strings.NewReader(tc.input), &prompt)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("confirmRestriction(%q) = %v, want %v", tc.input, got, tc.want)
		}
		if !strings.Contains(prompt.String(), "Type 'yes'") {
			t.Errorf("prompt = %q", prompt.String())
		}
	}
}

func TestPrintModeDiscoversInventoryWithoutWriting(t *testing.T) {
	names := commandRestrictionNames()
	client := kubefake.NewClientset(commandBaselineObjects(names)...)
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
		restrictionConnectionObject(names.Namespace, "beta", "beta.example.com", "cluster-custom"),
		restrictionConnectionObject(names.Namespace, "alpha", "alpha.example.com", ""),
	)
	var progress, manifest bytes.Buffer
	err := executeRestriction(t.Context(), client, dyn, restrictRBACParams{
		names: names, cluster: "test-context", printOnly: true,
		input: strings.NewReader(""), progress: &progress, manifestOut: &manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(manifest.String(), "- cluster-alpha\n  - cluster-custom") {
		t.Fatalf("manifest does not contain sorted default/custom names:\n%s", manifest.String())
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" || action.GetVerb() == "delete" {
			t.Fatalf("print mode performed write action: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
	if !strings.Contains(progress.String(), "No RBAC was changed") {
		t.Fatalf("progress = %q", progress.String())
	}
}

func TestDryRunAndRejectedConfirmationDoNotWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dryRun bool
		input  string
	}{
		{name: "dry-run", dryRun: true},
		{name: "confirmation refused", input: "no\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names := commandRestrictionNames()
			client := kubefake.NewClientset(commandBaselineObjects(names)...)
			dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
				map[schema.GroupVersionResource]string{inventory.GroupVersionResource: "ClusterConnectionList"},
				restrictionConnectionObject(names.Namespace, "alpha", "alpha.example.com", ""),
			)
			var progress bytes.Buffer
			err := executeRestriction(t.Context(), client, dyn, restrictRBACParams{
				names: names, cluster: "test", dryRun: tc.dryRun,
				input: strings.NewReader(tc.input), progress: &progress, manifestOut: &bytes.Buffer{},
			})
			if !tc.dryRun && err == nil {
				t.Fatal("rejected confirmation returned nil error")
			}
			for _, action := range client.Actions() {
				if slices.Contains([]string{"create", "update", "patch", "delete"}, action.GetVerb()) {
					t.Fatalf("preview performed write action: %s", action.GetVerb())
				}
			}
		})
	}
}
