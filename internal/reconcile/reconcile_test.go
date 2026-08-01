package reconcile

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krisiasty/k2a-token-sync/internal/config"
)

func TestResultSoonestTokenExpiry(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		result Result
		want   time.Time
	}{
		{
			name:   "no clusters",
			result: Result{},
		},
		{
			name: "clusters without recorded expiry",
			result: Result{Clusters: []ClusterStatus{
				{Name: "a"}, {Name: "b"},
			}},
		},
		{
			name: "picks the earliest",
			result: Result{Clusters: []ClusterStatus{
				{Name: "a", TokenExpiresAt: base.Add(720 * time.Hour)},
				{Name: "b", TokenExpiresAt: base.Add(2 * time.Hour)},
				{Name: "c", TokenExpiresAt: base.Add(48 * time.Hour)},
			}},
			want: base.Add(2 * time.Hour),
		},
		{
			// One capped cluster must pull the whole cadence in, otherwise its
			// credential dies while the others are still comfortable.
			name: "ignores clusters with no expiry but honours those that have one",
			result: Result{Clusters: []ClusterStatus{
				{Name: "a"},
				{Name: "b", TokenExpiresAt: base.Add(time.Hour)},
			}},
			want: base.Add(time.Hour),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.result.SoonestTokenExpiry()
			if !got.Equal(tc.want) {
				t.Fatalf("SoonestTokenExpiry() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResultAllSyncedAndFailures(t *testing.T) {
	t.Parallel()

	result := Result{Clusters: []ClusterStatus{
		{Name: "a", Synced: true},
		{Name: "b", Synced: false},
		{Name: "c", Synced: true},
	}}

	if result.AllSynced() {
		t.Error("AllSynced() = true with a failed cluster")
	}
	if got := result.Failures(); got != 1 {
		t.Errorf("Failures() = %d, want 1", got)
	}

	clean := Result{Clusters: []ClusterStatus{{Name: "a", Synced: true}}}
	if !clean.AllSynced() {
		t.Error("AllSynced() = false with every cluster synced")
	}
	if got := clean.Failures(); got != 0 {
		t.Errorf("Failures() = %d, want 0", got)
	}

	// An empty pass must not read as healthy progress by accident.
	empty := Result{}
	if got := empty.Failures(); got != 0 {
		t.Errorf("Failures() on an empty result = %d, want 0", got)
	}
}

func TestArgoCDSecretError(t *testing.T) {
	t.Parallel()

	r := &Reconciler{cfg: &config.Config{ArgoCDNamespace: "argocd"}}
	cluster := config.Cluster{Name: "standalone-1", SecretName: "cluster-standalone-1"}

	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "secrets"}, cluster.SecretName, errors.New("no access"))

	t.Run("nil passes through", func(t *testing.T) {
		t.Parallel()
		if err := r.argocdSecretError(cluster, nil); err != nil {
			t.Fatalf("argocdSecretError(nil) = %v, want nil", err)
		}
	})

	t.Run("other errors are untouched", func(t *testing.T) {
		t.Parallel()
		original := errors.New("connection refused")
		got := r.argocdSecretError(cluster, original)
		if !errors.Is(got, original) || got.Error() != original.Error() {
			t.Fatalf("argocdSecretError() = %q, want it returned verbatim", got)
		}
	})

	// The hint must survive the wrapping the apply helpers add, which is how the
	// error actually arrives here.
	t.Run("wrapped forbidden gains the remedy", func(t *testing.T) {
		t.Parallel()
		wrapped := fmt.Errorf("applying cluster secret argocd/%s: %w", cluster.SecretName, forbidden)
		got := r.argocdSecretError(cluster, wrapped)

		if !errors.Is(got, forbidden) {
			t.Error("the original API error is no longer unwrappable")
		}
		for _, want := range []string{"create and patch", "argocd", cluster.SecretName} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("error %q does not mention %q", got, want)
			}
		}
	})
}
