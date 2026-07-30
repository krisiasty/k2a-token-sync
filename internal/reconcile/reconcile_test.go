package reconcile

import (
	"testing"
	"time"
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
