package main

import (
	"testing"
	"time"
)

func TestNextPassInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name            string
		refreshInterval time.Duration
		expiresIn       time.Duration
		noExpiry        bool
		want            time.Duration
	}{
		{
			// The common case: a 30-day token means half life is far beyond the
			// configured interval, so the interval governs.
			name:            "long lived token leaves the interval intact",
			refreshInterval: 24 * time.Hour,
			expiresIn:       720 * time.Hour,
			want:            24 * time.Hour,
		},
		{
			// The bug this guards: an API server capping tokens at 1h would
			// otherwise leave ArgoCD holding a dead credential for 23 hours.
			name:            "capped token shortens the interval",
			refreshInterval: 24 * time.Hour,
			expiresIn:       time.Hour,
			want:            30 * time.Minute,
		},
		{
			name:            "pathologically short cap is floored",
			refreshInterval: 24 * time.Hour,
			expiresIn:       10 * time.Second,
			want:            minPassInterval,
		},
		{
			name:            "already expired is floored rather than negative",
			refreshInterval: 24 * time.Hour,
			expiresIn:       -time.Hour,
			want:            minPassInterval,
		},
		{
			// Nothing was reissued this pass, so there is no expiry to derive from.
			name:            "no known expiry falls back to the interval",
			refreshInterval: 6 * time.Hour,
			noExpiry:        true,
			want:            6 * time.Hour,
		},
		{
			name:            "interval still wins when it is the shorter of the two",
			refreshInterval: 5 * time.Minute,
			expiresIn:       720 * time.Hour,
			want:            5 * time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var expiry time.Time
			if !tc.noExpiry {
				expiry = now.Add(tc.expiresIn)
			}

			got := nextPassInterval(tc.refreshInterval, expiry, now)
			if got != tc.want {
				t.Fatalf("nextPassInterval(%v, +%v) = %v, want %v",
					tc.refreshInterval, tc.expiresIn, got, tc.want)
			}
			if got <= 0 {
				t.Fatalf("nextPassInterval returned %v; a non-positive sleep would spin", got)
			}
		})
	}
}
