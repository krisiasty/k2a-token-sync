package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/inventory"
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

// The scheduling arithmetic is exact, but the poll it lands on is not: dueAt is
// computed from a clock read that happens a little after a tick, since the
// inventory list comes first. Without slack, a cluster due a few milliseconds
// past a tick waits for the following one, and every interval comes out one whole
// tick longer than configured — five and a half minutes for a configured five,
// which is what ran in production.
func TestAClusterDueJustPastATickIsNotHeldForAWholeExtraPoll(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		dueIn time.Duration
		want  bool
	}{
		{"overdue", -time.Minute, true},
		{"due exactly now", 0, true},
		{"due a moment after this tick", 40 * time.Millisecond, true},
		{"due within half a tick", dueSlack - time.Second, true},
		{"due beyond half a tick waits", dueSlack + time.Second, false},
		{"due a full interval away waits", maxPassInterval, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &scheduler{
				state: map[string]*clusterState{"c": {dueAt: now.Add(tc.dueIn)}},
				now:   func() time.Time { return now },
			}
			got := len(s.claimDue()) == 1
			if got != tc.want {
				t.Errorf("due=%v for a cluster due in %v, want %v", got, tc.dueIn, tc.want)
			}
		})
	}
}

// An unresolvable spec is never reconciled, however overdue it looks.
func TestAnInvalidClusterIsNeverDue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	s := &scheduler{
		state: map[string]*clusterState{
			"broken": {dueAt: now.Add(-time.Hour), invalidReason: "endpoint is not a host:port"},
		},
		now: func() time.Time { return now },
	}
	if due := s.claimDue(); len(due) != 0 {
		t.Errorf("claimDue returned %d clusters, want none", len(due))
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

// stoppedClock builds a health state whose sense of time the test controls.
func stoppedClock(t *testing.T, at time.Time) (*healthState, func(time.Duration)) {
	t.Helper()

	now := at
	state := newHealthState()
	state.now = func() time.Time { return now }
	state.polledAt = now
	return state, func(d time.Duration) { now = now.Add(d) }
}

func TestHealthLivenessTracksCompletedPolls(t *testing.T) {
	t.Parallel()

	state, advance := stoppedClock(t, time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC))
	if !state.isLive() {
		t.Error("not live at startup, before the first poll has had a chance to run")
	}

	advance(maxProgressAge - time.Second)
	if !state.isLive() {
		t.Error("a loop within its window is reported as stalled")
	}

	// The window is deliberately several polls wide: a control plane that is
	// briefly unreachable must not be answered by restarting a healthy pod.
	advance(2 * time.Second)
	if state.isLive() {
		t.Errorf("live %v after the last completed poll, want the probe to fail past %v",
			maxProgressAge+time.Second, maxProgressAge)
	}

	state.recordPoll()
	if !state.isLive() {
		t.Error("a completed poll did not clear the stall")
	}
}

// The failure this replaced: while a pass was in progress liveness returned true
// unconditionally, so an inventory call that hung — the one thing a restart would
// actually fix — kept the probe green for as long as the process lived.
func TestHealthLivenessDoesNotExcuseAPassThatNeverEnds(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	state, advance := stoppedClock(t, start)

	state.passStarted("slow", start)
	advance(clusterTimeout)
	state.recordPoll() // the loop is still polling; only this cluster is stuck
	if !state.isLive() {
		t.Error("a pass that is merely slow tripped the liveness probe")
	}

	// Past its own timeout the pass is not slow, it is wedged: every pass runs
	// under a context that should have ended it.
	advance(passGrace + time.Second)
	state.recordPoll()
	if state.isLive() {
		t.Error("a pass that outlived its timeout was still reported as a healthy loop")
	}

	state.passFinished("slow")
	if !state.isLive() {
		t.Error("still stalled after the pass ended")
	}
}

// The condition is the primary report, because it is the only one attached to
// the thing an operator is looking at. Somebody who set clusterRole and saw
// nothing happen opens the connection, not the process — and a pruned field
// leaves no other trace on it, being indistinguishable from one nobody set.
func TestAStaleSchemaIsReportedOnEveryConnection(t *testing.T) {
	t.Parallel()

	s := &scheduler{health: newHealthState()}
	s.health.recordSchema(inventory.SchemaCheck{
		MissingSpecFields: []string{"clusterRole", "namespaces"},
	})

	var status v1alpha1.ClusterConnectionStatus
	s.setSchemaCondition(&status)

	cond := meta.FindStatusCondition(status.Conditions, v1alpha1.ConditionSchemaCurrent)
	if cond == nil {
		t.Fatal("no SchemaCurrent condition was written")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Status = %v, want False", cond.Status)
	}
	if cond.Reason != v1alpha1.ReasonSchemaOutdated {
		t.Errorf("Reason = %q, want %q", cond.Reason, v1alpha1.ReasonSchemaOutdated)
	}
	// Naming the fields is the whole value: "something is stale" sends the
	// reader looking, "these are discarded" tells them what to fix.
	for _, want := range []string{"spec.clusterRole", "spec.namespaces", "k2a-token-sync-crds"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("message does not mention %q: %s", want, cond.Message)
		}
	}
}

// Being unable to read the CRD is Unknown, not False. An operator who upgraded
// the image before the chart lands here with nothing actually wrong, and
// reporting it as a stale schema would send them to fix the wrong thing.
func TestAnUnreadableSchemaIsUnknownRatherThanFalse(t *testing.T) {
	t.Parallel()

	s := &scheduler{health: newHealthState()}
	s.health.recordSchema(inventory.SchemaCheck{Unverifiable: errors.New("forbidden")})

	var status v1alpha1.ClusterConnectionStatus
	s.setSchemaCondition(&status)

	cond := meta.FindStatusCondition(status.Conditions, v1alpha1.ConditionSchemaCurrent)
	if cond == nil {
		t.Fatal("no SchemaCurrent condition was written")
	}
	if cond.Status != metav1.ConditionUnknown {
		t.Errorf("Status = %v, want Unknown for a schema that could not be read", cond.Status)
	}
	if cond.Reason != v1alpha1.ReasonSchemaUnverified {
		t.Errorf("Reason = %q, want %q", cond.Reason, v1alpha1.ReasonSchemaUnverified)
	}
}

// Before the first check completes there is no verdict, and inventing one would
// claim the schema is current on the strength of nothing having looked. That is
// worse than silence: it is a healthy-looking answer to a question nobody asked.
func TestNoConditionIsWrittenBeforeTheFirstCheck(t *testing.T) {
	t.Parallel()

	s := &scheduler{health: newHealthState()}

	var status v1alpha1.ClusterConnectionStatus
	s.setSchemaCondition(&status)

	if cond := meta.FindStatusCondition(status.Conditions, v1alpha1.ConditionSchemaCurrent); cond != nil {
		t.Errorf("a condition was written before any check ran: %+v", cond)
	}
}

// The check is far slower than the poll on purpose. Running it every tick would
// be a request every thirty seconds to answer a question whose answer changes
// about once a release.
func TestTheSchemaCheckIsRateLimited(t *testing.T) {
	t.Parallel()

	var calls int
	now := time.Now()
	s := &scheduler{
		health: newHealthState(),
		logger: slog.New(slog.DiscardHandler),
		now:    func() time.Time { return now },
		schema: func(context.Context) inventory.SchemaCheck {
			calls++
			return inventory.SchemaCheck{}
		},
	}

	s.checkSchema(t.Context())
	s.checkSchema(t.Context())
	if calls != 1 {
		t.Errorf("calls = %d, want 1: a second check inside the interval should be skipped", calls)
	}

	now = now.Add(schemaCheckInterval + time.Second)
	s.checkSchema(t.Context())
	if calls != 2 {
		t.Errorf("calls = %d, want 2: the check should run again once the interval elapsed", calls)
	}
}
