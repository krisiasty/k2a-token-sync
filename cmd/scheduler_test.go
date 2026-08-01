package main

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

func TestNextInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		expiresIn time.Duration
		noExpiry  bool
		want      time.Duration
	}{
		{
			// Nothing published yet, so there is no expiry to derive from.
			name:     "no recorded expiry falls back to the cap",
			noExpiry: true,
			want:     maxPassInterval,
		},
		{
			// Half of 30 days is 15, so the cap wins. This is every ordinary
			// cluster: the interval is the cap, and a deleted Secret is noticed
			// within it.
			name:      "long-lived token is capped",
			expiresIn: 720 * time.Hour,
			want:      maxPassInterval,
		},
		{
			// A severely capped token is the case the derivation exists for. Half
			// the remaining lifetime keeps a whole refresh in hand, and it only
			// takes effect once it is shorter than the cap.
			name:      "short-lived token pulls the interval in",
			expiresIn: 6 * time.Minute,
			want:      3 * time.Minute,
		},
		{
			// A pathologically short cap must not turn the loop into a busy wait.
			name:      "very short token is floored",
			expiresIn: 30 * time.Second,
			want:      minPassInterval,
		},
		{
			// An already-expired token would derive a negative interval.
			name:      "expired token is floored rather than negative",
			expiresIn: -time.Hour,
			want:      minPassInterval,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var status v1alpha1.ClusterConnectionStatus
			if !tc.noExpiry {
				status.TokenExpiresAt = &metav1.Time{Time: now.Add(tc.expiresIn)}
			}

			got := nextInterval(status, now)
			if got != tc.want {
				t.Fatalf("nextInterval(+%v) = %v, want %v", tc.expiresIn, got, tc.want)
			}
			if got <= 0 {
				t.Fatalf("nextInterval returned %v; a non-positive interval would spin", got)
			}
		})
	}
}

// A pass is scheduled from when it started, not from when it finished.
//
// Measuring from the end added the pass's duration to every interval, and because
// that always carried the next due time a few milliseconds past the poll tick that
// would have caught it, each interval quietly became one whole tick longer: five
// and a half minutes for a configured five, observed live as 262 passes a day
// instead of 288.
func TestPassesAreScheduledFromWhenTheyStarted(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	var status v1alpha1.ClusterConnectionStatus
	status.TokenExpiresAt = &metav1.Time{Time: startedAt.Add(720 * time.Hour)}

	got := dueAfterPass(startedAt, status)
	if want := startedAt.Add(maxPassInterval); !got.Equal(want) {
		t.Errorf("next due at %v, want %v — exactly one interval after the pass began", got, want)
	}
}

func TestHealthReadinessTracksEveryCluster(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	if state.isReady() {
		t.Error("a freshly created state reports ready before anything reconciled")
	}

	state.record([]clusterReport{{Name: "a", Synced: true}, {Name: "b", Synced: false}}, time.Minute)
	if state.isReady() {
		t.Error("ready with one cluster unsynced; a partial failure must be visible")
	}

	state.record([]clusterReport{{Name: "a", Synced: true}, {Name: "b", Synced: true}}, time.Minute)
	if !state.isReady() {
		t.Error("not ready although every cluster synced")
	}

	// An empty inventory is ready: there is nothing to fail, and holding the pod
	// unready would make a cluster-less install look broken.
	empty := newHealthState()
	empty.record(nil, time.Minute)
	if !empty.isReady() {
		t.Error("an empty inventory reports unready")
	}
}

func TestHealthLivenessSurvivesAPassInProgress(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	if !state.isLive() {
		t.Error("not live at startup")
	}

	state.recordAttempt()
	if !state.isLive() {
		t.Error("not live while a pass is in progress")
	}

	state.record([]clusterReport{{Name: "a", Synced: true}}, time.Minute)
	if !state.isLive() {
		t.Error("not live immediately after a pass")
	}
}
